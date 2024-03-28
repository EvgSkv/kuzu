#include "transaction/transaction.h"

#include "catalog/catalog.h"
#include "main/client_context.h"
#include "transaction/transaction_action.h"
#include "storage/storage_manager.h"

namespace kuzu {
namespace transaction {

void Transaction::commit() {
    KU_ASSERT(clientContext);
    clientContext->getCatalog()->prepareCommitOrRollback(TransactionAction::COMMIT);
    localStorage->prepareCommit();
    clientContext->getStorageManager()->prepareCommit(this);
}

void Transaction::rollback() {
    KU_ASSERT(clientContext);
    clientContext->getCatalog()->prepareCommitOrRollback(TransactionAction::ROLLBACK);
    localStorage->prepareRollback();
    clientContext->getStorageManager()->prepareRollback(this);
}

} // namespace transaction
} // namespace kuzu
