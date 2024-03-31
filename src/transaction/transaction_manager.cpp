#include "transaction/transaction_manager.h"

#include <thread>

#include "common/exception/transaction_manager.h"
#include "main/client_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace transaction {

std::unique_ptr<Transaction> TransactionManager::beginWriteTransaction(
    main::ClientContext& clientContext) {
    // We obtain the lock for starting new transactions. In case this cannot be obtained this
    // ensures calls to other public functions is not restricted.
    lock_t newTransactionLck{mtxForStartingNewTransactions};
    lock_t publicFunctionLck{mtxForSerializingPublicFunctionCalls};
    //    if (hasActiveWriteTransactionNoLock()) {
    //        throw TransactionManagerException(
    //            "Cannot start a new write transaction in the system. "
    //            "Only one write transaction at a time is allowed in the system.");
    //    }
    auto transaction = std::make_unique<Transaction>(
        clientContext, TransactionType::WRITE, ++lastTransactionID, lastTimestamp);
    activeWriteTransactionID.insert(transaction->getID());
    return transaction;
}

std::unique_ptr<Transaction> TransactionManager::beginReadOnlyTransaction(
    main::ClientContext& clientContext) {
    // We obtain the lock for starting new transactions. In case this cannot be obtained this
    // ensures calls to other public functions is not restricted.
    lock_t newTransactionLck{mtxForStartingNewTransactions};
    lock_t publicFunctionLck{mtxForSerializingPublicFunctionCalls};
    auto transaction = std::make_unique<Transaction>(
        clientContext, TransactionType::READ_ONLY, ++lastTransactionID, lastTimestamp);
    activeReadOnlyTransactionIDs.insert(transaction->getID());
    return transaction;
}

void TransactionManager::commitButKeepActiveWriteTransaction(Transaction* transaction) {
    lock_t lck{mtxForSerializingPublicFunctionCalls};
    commitOrRollbackNoLock(transaction, true /* is commit */);
}

void TransactionManager::manuallyClearActiveWriteTransaction(Transaction* transaction) {
    lock_t lck{mtxForSerializingPublicFunctionCalls};
    clearActiveWriteTransactionIfWriteTransactionNoLock(transaction);
}

void TransactionManager::commitOrRollbackNoLock(Transaction* transaction, bool isCommit) {
    if (transaction->isReadOnly()) {
        activeReadOnlyTransactionIDs.erase(transaction->getID());
        return;
    }
    if (isCommit) {
        lastTimestamp++;
        transaction->commitTS = lastTimestamp;
        transaction->commit(&wal);
        lastCommitID++;
    } else {
        transaction->rollback();
    }
}

void TransactionManager::commit(Transaction* transaction) {
    lock_t lck{mtxForSerializingPublicFunctionCalls};
    commitOrRollbackNoLock(transaction, true /* is commit */);
    clearActiveWriteTransactionIfWriteTransactionNoLock(transaction);
}

void TransactionManager::rollback(Transaction* transaction) {
    lock_t lck{mtxForSerializingPublicFunctionCalls};
    commitOrRollbackNoLock(transaction, false /* is rollback */);
    clearActiveWriteTransactionIfWriteTransactionNoLock(transaction);
}

void TransactionManager::allowReceivingNewTransactions() {
    mtxForStartingNewTransactions.unlock();
}

void TransactionManager::stopNewTransactionsAndWaitUntilAllReadTransactionsLeave() {
    mtxForStartingNewTransactions.lock();
    lock_t lck{mtxForSerializingPublicFunctionCalls};
    uint64_t numTimesWaited = 0;
    while (true) {
        if (!activeReadOnlyTransactionIDs.empty()) {
            numTimesWaited++;
            if (numTimesWaited * THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS >
                checkPointWaitTimeoutForTransactionsToLeaveInMicros) {
                mtxForStartingNewTransactions.unlock();
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
