#pragma once

#include "bound_statement.h"
#include "catalog/catalog.h"

namespace kuzu {
namespace binder {

// Perform semantic rewrite over bound statement.
class BoundStatementRewriter {
public:
    static void rewrite(BoundStatement& boundStatement, const catalog::Catalog& catalog,
        const main::ClientContext& clientContext);
};

} // namespace binder
} // namespace kuzu
