#pragma once

#include <memory>
#include <mutex>
#include <unordered_set>

#include "storage/wal/wal.h"
#include "transaction.h"

using lock_t = std::unique_lock<std::mutex>;

namespace kuzu {
namespace main {
class ClientContext;
} // namespace main
namespace transaction {

class TransactionManager {
public:
    // Timestamp starts from 1. 0 is reserved for the dummy system transaction.
    explicit TransactionManager(storage::WAL& wal)
        : wal{wal}, lastTransactionID{Transaction::START_TRANSACTION_ID}, lastTimestamp{1},
          lastCommitID{0} {};
    std::unique_ptr<Transaction> beginWriteTransaction(main::ClientContext& clientContext);
    std::unique_ptr<Transaction> beginReadOnlyTransaction(main::ClientContext& clientContext);
    void commit(Transaction* transaction);
    void commitButKeepActiveWriteTransaction(Transaction* transaction);
    void manuallyClearActiveWriteTransaction(Transaction* transaction);
    void rollback(Transaction* transaction);
    // This functions locks the mutex to start new transactions. This lock needs to be manually
    // unlocked later by calling allowReceivingNewTransactions() by the thread that called
    // stopNewTransactionsAndWaitUntilAllReadTransactionsLeave().
    void stopNewTransactionsAndWaitUntilAllReadTransactionsLeave();
    void allowReceivingNewTransactions();

    // Warning: Below public functions are for tests only
    inline std::unordered_set<uint64_t>& getActiveReadOnlyTransactionIDs() {
        lock_t lck{mtxForSerializingPublicFunctionCalls};
        return activeReadOnlyTransactionIDs;
    }
    inline common::transaction_t getActiveWriteTransactionID() {
        lock_t lck{mtxForSerializingPublicFunctionCalls};
        KU_ASSERT(activeWriteTransactionID.size() == 1);
        return *activeWriteTransactionID.begin();
    }
    inline bool hasActiveWriteTransactionID() {
        lock_t lck{mtxForSerializingPublicFunctionCalls};
        return !activeWriteTransactionID.empty();
    }
    inline void setCheckPointWaitTimeoutForTransactionsToLeaveInMicros(uint64_t waitTimeInMicros) {
        checkPointWaitTimeoutForTransactionsToLeaveInMicros = waitTimeInMicros;
    }

private:
    inline bool hasActiveWriteTransactionNoLock() const {
        return !activeWriteTransactionID.empty();
    }
    inline void clearActiveWriteTransactionIfWriteTransactionNoLock(Transaction* transaction) {
        if (transaction->isWriteTransaction()) {
            for (auto& id : activeWriteTransactionID) {
                if (id == transaction->getID()) {
                    activeWriteTransactionID.erase(id);
                    return;
                }
            }
        }
    }
    void commitOrRollbackNoLock(Transaction* transaction, bool isCommit);

private:
    storage::WAL& wal;

    std::unordered_set<common::transaction_t> activeWriteTransactionID;
    std::unordered_set<uint64_t> activeReadOnlyTransactionIDs;

    common::transaction_t lastTransactionID;
    common::transaction_t lastTimestamp;
    // ID of the last committed write transaction. This is currently used primarily for
    // debugging purposes during development and is not written to disk in a db file.
    // In particular, transactions do not use this to perform reads. Our current transaction design
    // supports a concurrency model that requires on 2 versions, one for the read-only transactions
    // and the for the writer transaction, so we can read correct version by looking at the type of
    // the transaction.
    // TODO: Should remove this field.
    common::transaction_t lastCommitID;
    // This mutex is used to ensure thread safety and letting only one public function to be called
    // at any time except the stopNewTransactionsAndWaitUntilAllReadTransactionsLeave
    // function, which needs to let calls to comming and rollback.
    std::mutex mtxForSerializingPublicFunctionCalls;
    std::mutex mtxForStartingNewTransactions;
    uint64_t checkPointWaitTimeoutForTransactionsToLeaveInMicros =
        common::DEFAULT_CHECKPOINT_WAIT_TIMEOUT_FOR_TRANSACTIONS_TO_LEAVE_IN_MICROS;
};
} // namespace transaction
} // namespace kuzu
