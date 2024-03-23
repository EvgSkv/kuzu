#include "planner/operator/logical_distinct.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void Planner::appendDistinct(const expression_vector& exprs, LogicalPlan& plan) {
    KU_ASSERT(!exprs.empty());
    auto distinct = make_shared<LogicalDistinct>(exprs, plan.getLastOperator());
    appendFlattens(distinct->getGroupsPosToFlatten(), plan);
    distinct->setChild(0, plan.getLastOperator());
    distinct->computeFactorizedSchema();
    plan.setLastOperator(std::move(distinct));
}

} // namespace planner
} // namespace kuzu
