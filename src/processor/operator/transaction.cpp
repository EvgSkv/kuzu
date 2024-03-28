#include "processor/operator/transaction.h"

#include "transaction/transaction_context.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace processor {

bool Transaction::getNextTuplesInternal(ExecutionContext* context) {
    if (hasExecuted) {
        return false;
    }
    hasExecuted = true;
    auto transactionContext = context->clientContext->getTransactionContext();
    switch (transactionAction) {
    case transaction::TransactionAction::BEGIN_READ: {
        transactionContext->beginTransaction(TransactionType::READ_ONLY);
    } break;
    case transaction::TransactionAction::BEGIN_WRITE: {
        transactionContext->beginTransaction(TransactionType::WRITE);
    } break;
    case transaction::TransactionAction::COMMIT: {
        transactionContext->commit();
    } break;
    case transaction::TransactionAction::COMMIT_SKIP_CHECKPOINTING: {
        transactionContext->commitSkipCheckPointing();
    } break;
    case transaction::TransactionAction::ROLLBACK: {
        transactionContext->rollback();
    } break;
    case transaction::TransactionAction::ROLLBACK_SKIP_CHECKPOINTING: {
        transactionContext->rollbackSkipCheckPointing();
    } break;
    default:
        KU_UNREACHABLE;
    }
    return true;
}

} // namespace processor
} // namespace kuzu
