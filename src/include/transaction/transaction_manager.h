#pragma once

#include <memory>
#include <mutex>
#include <unordered_set>

#include "storage/wal/wal.h"
#include "transaction.h"

namespace kuzu {
namespace main {
class ClientContext;
} // namespace main
namespace storage {
enum class WALReplayMode : uint8_t;
}
namespace transaction {

class TransactionManager {
public:
    static constexpr common::transaction_t MIN_TRANSACTION_ID = (common::transaction_t)1 << 63;

    explicit TransactionManager(storage::WAL& wal)
        : wal{wal}, currentTransactionID{MIN_TRANSACTION_ID},
          checkpointWaitTimeoutInMicros{common::DEFAULT_CHECKPOINT_WAIT_TIMEOUT_IN_MICROS} {};

    Transaction& beginTransaction(main::ClientContext& clientContext, TransactionType type);
    void commitTransaction(Transaction* transaction, bool autoCheckpoint = true);
    void rollbackTransaction(Transaction* transaction, bool autoCheckpoint = true);

    //    void commitButKeepActiveWriteTransaction(Transaction* transaction);
    //    void manuallyClearActiveWriteTransaction(Transaction* transaction);
    // This functions locks the mutex to start new transactions. This lock needs to be manually
    // unlocked later by calling allowReceivingNewTransactions() by the thread that called
    // stopNewTransactionsAndWaitUntilAllReadTransactionsLeave().
    void stopNewTransactionsAndWaitUntilAllReadTransactionsLeave();
    void allowReceivingNewTransactions();

    // TODO: Should be moved to database config.
    inline void setCheckpointWaitTimeoutInMicros(uint64_t waitTimeInMicros) {
        checkpointWaitTimeoutInMicros = waitTimeInMicros;
    }

private:
    Transaction& beginWriteTransactionNoLock(main::ClientContext& clientContext);
    Transaction& beginReadOnlyTransactionNoLock(main::ClientContext& clientContext);
    void checkpoint(main::ClientContext& clientContext, storage::WALReplayMode replayMode);

    inline bool hasActiveWriteTransactionNoLock() const { return !activeWriteTransaction; }
    //    void commitOrRollbackNoLock(Transaction* transaction, bool isCommit);
    void checkActiveWriteTransactionNoLock(Transaction* transaction) const;
    void clearActiveReadTransaction(Transaction* transaction);
    inline void clearActiveWriteTransactionNoLock() { activeWriteTransaction.reset(); }

private:
    storage::WAL& wal;

    // This mutex is used to ensure thread safety and letting only one public function to be called
    // at any time except the stopNewTransactionsAndWaitUntilAllReadTransactionsLeave
    // function, which needs to let calls to comming and rollback.
    std::mutex transactionManagerMtx;
    //    common::transaction_t activeWriteTransactionID;
    //    std::unordered_set<common::transaction_t> activeReadOnlyTransactionIDs;
    common::transaction_t currentTransactionID;
    common::transaction_t currentTransactionTS;
    std::mutex transactionIDMtx;
    uint64_t checkpointWaitTimeoutInMicros;
    std::unique_ptr<Transaction> activeWriteTransaction;
    std::unordered_set<std::unique_ptr<Transaction>> activeReadOnlyTransactions;
};

} // namespace transaction
} // namespace kuzu
