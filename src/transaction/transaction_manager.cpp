#include "transaction/transaction_manager.h"

#include <thread>

#include "common/exception/transaction_manager.h"
#include "storage/wal_replayer.h"

using namespace kuzu::common;
using namespace kuzu::main;

namespace kuzu {
namespace transaction {

Transaction& TransactionManager::beginTransaction(
    ClientContext& clientContext, TransactionType type) {
    // We obtain the lock for starting new transactions. In case this cannot be obtained this
    // ensures calls to other public functions is not restricted.
    std::unique_lock<std::mutex> txnLock{transactionIDMtx};
    std::unique_lock<std::mutex> txnMgrLock{transactionManagerMtx};
    switch (type) {
    case TransactionType::WRITE: {
        return beginWriteTransactionNoLock(clientContext);
    } break;
    case TransactionType::READ_ONLY: {
        return beginReadOnlyTransactionNoLock(clientContext);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

Transaction& TransactionManager::beginWriteTransactionNoLock(ClientContext& clientContext) {
    if (hasActiveWriteTransactionNoLock()) {
        throw TransactionManagerException(
            "Cannot start a new write transaction in the system. Only one write transaction at a "
            "time is allowed in the system.");
    }
    activeWriteTransaction = std::make_unique<Transaction>(
        clientContext, TransactionType::WRITE, ++currentTransactionID);
    return *activeWriteTransaction;
}

Transaction& TransactionManager::beginReadOnlyTransactionNoLock(ClientContext& clientContext) {
    auto transaction = std::make_unique<Transaction>(
        clientContext, TransactionType::READ_ONLY, ++currentTransactionID);
    auto& ref = *transaction;
    activeReadOnlyTransactions.insert(std::move(transaction));
    return ref;
}

// void TransactionManager::commitButKeepActiveWriteTransaction(Transaction* transaction) {
//    std::unique_lock<std::mutex> lck{transactionManagerMtx};
//    if (transaction->isReadOnly()) {
//        for (auto& txn : activeReadOnlyTransactions) {
//            if (txn.get() == transaction) {
//                activeReadOnlyTransactions.erase(txn);
//                return;
//            }
//        }
//        return;
//    }
//    checkActiveWriteTransactionNoLock(transaction);
//    wal.logCommit(transaction->getID());
//}

// void TransactionManager::manuallyClearActiveWriteTransaction(Transaction* transaction) {
//    std::unique_lock<std::mutex> lck{transactionManagerMtx};
//    checkActiveWriteTransactionNoLock(transaction);
//    clearActiveWriteTransactionIfWriteTransactionNoLock(transaction);
//}

// void TransactionManager::commitOrRollbackNoLock(Transaction* transaction, bool isCommit) {
//    if (transaction->isReadOnly()) {
//        clearActiveReadTransaction(transaction);
//        return;
//    }
//    checkActiveWriteTransactionNoLock(transaction);
//    if (isCommit) {
//        wal.logCommit(transaction->getID());
//    }
//}

void TransactionManager::checkActiveWriteTransactionNoLock(Transaction* transaction) const {
    if (activeWriteTransaction.get() != transaction) {
        throw TransactionManagerException(
            "The ID of the committing write transaction " + std::to_string(transaction->getID()) +
            " is not equal to the ID of the activeWriteTransaction: " +
            std::to_string(activeWriteTransaction->getID()));
    }
}

void TransactionManager::clearActiveReadTransaction(Transaction* transaction) {
    for (auto& txn : activeReadOnlyTransactions) {
        if (txn.get() == transaction) {
            activeReadOnlyTransactions.erase(txn);
            return;
        }
    }
}

void TransactionManager::commitTransaction(Transaction* transaction, bool autoCheckpoint) {
    std::unique_lock<std::mutex> lck{transactionManagerMtx};
    if (transaction->isReadOnly()) {
        clearActiveReadTransaction(transaction);
        return;
    }
    transaction->commit();
    checkActiveWriteTransactionNoLock(transaction);
    wal.logCommit(transaction->getID());
    wal.flushAllPages();
    clearActiveWriteTransactionNoLock();
    if (autoCheckpoint) {
        checkpoint(transaction->getClientContext(), storage::WALReplayMode::COMMIT_CHECKPOINT);
    }
}

void TransactionManager::rollbackTransaction(Transaction* transaction, bool autoCheckpoint) {
    std::unique_lock<std::mutex> lck{transactionManagerMtx};
    if (transaction->isReadOnly()) {
        clearActiveReadTransaction(transaction);
        return;
    }
    transaction->rollback();
    checkActiveWriteTransactionNoLock(transaction);
    if (autoCheckpoint) {
        checkpoint(transaction->getClientContext(), storage::WALReplayMode::ROLLBACK);
    }
    clearActiveWriteTransactionNoLock();
}

void TransactionManager::checkpoint(
    ClientContext& clientContext, storage::WALReplayMode replayMode) {
    stopNewTransactionsAndWaitUntilAllReadTransactionsLeave();
    auto walReplayer = std::make_unique<storage::WALReplayer>(&wal,
        clientContext.getStorageManager(), clientContext.getMemoryManager()->getBufferManager(),
        clientContext.getCatalog(), replayMode, clientContext.getVFSUnsafe());
    walReplayer->replay();
    wal.clearWAL();
    allowReceivingNewTransactions();
}

void TransactionManager::allowReceivingNewTransactions() {
    transactionIDMtx.unlock();
}

void TransactionManager::stopNewTransactionsAndWaitUntilAllReadTransactionsLeave() {
    transactionIDMtx.lock();
    std::unique_lock<std::mutex> lck{transactionManagerMtx};
    uint64_t numTimesWaited = 0;
    while (true) {
        if (!activeReadOnlyTransactions.empty()) {
            numTimesWaited++;
            if (numTimesWaited * THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS >
                checkpointWaitTimeoutInMicros) {
                transactionIDMtx.unlock();
                throw TransactionManagerException(
                    "Timeout waiting for read transactions to leave the system before committing "
                    "and checkpointing a write transaction. If you have an open read transaction "
                    "close and try again.");
            }
            std::this_thread::sleep_for(
                std::chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
        } else {
            break;
        }
    }
}

} // namespace transaction
} // namespace kuzu
