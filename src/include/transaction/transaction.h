#pragma once

#include <memory>

#include "storage/local_storage/local_storage.h"

namespace kuzu {
namespace main {
class ClientContext;
} // namespace main
namespace storage {
class LocalStorage;
} // namespace storage
namespace transaction {

enum class TransactionType : uint8_t { READ_ONLY, WRITE };

class Transaction {
public:
    Transaction(
        main::ClientContext& clientContext, TransactionType transactionType, uint64_t transactionID)
        : clientContext{&clientContext}, type{transactionType}, ID{transactionID} {
        localStorage = std::make_unique<storage::LocalStorage>(clientContext);
    }

    // Should only be used for static dummy transactions.
    // We have to include `local_storage.h` for now because of the `LocalStorage` member.
    // Once we clear dummy transactions, should be able to remove this include.
    constexpr explicit Transaction(TransactionType transactionType) noexcept
        : type{transactionType}, ID{common::INVALID_TRANSACTION_ID} {}

    void commit();
    void rollback();

public:
    inline TransactionType getType() const { return type; }
    inline bool isReadOnly() const { return TransactionType::READ_ONLY == type; }
    inline bool isWriteTransaction() const { return TransactionType::WRITE == type; }
    inline uint64_t getID() const { return ID; }
    inline storage::LocalStorage* getLocalStorage() { return localStorage.get(); }
    main::ClientContext& getClientContext() {
        KU_ASSERT(clientContext);
        return *clientContext; }

private:
    // We have to keep a pointer for now, as dummy transactions are not supposed to refer to a
    // clientContext.
    main::ClientContext* clientContext;
    TransactionType type;
    common::transaction_t ID;
    std::unique_ptr<storage::LocalStorage> localStorage;
};

static Transaction DUMMY_READ_TRANSACTION = Transaction(TransactionType::READ_ONLY);
static Transaction DUMMY_WRITE_TRANSACTION = Transaction(TransactionType::WRITE);

} // namespace transaction
} // namespace kuzu
