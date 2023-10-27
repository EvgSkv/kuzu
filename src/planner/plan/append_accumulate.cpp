#include <memory>
#include <utility>

#include "binder/expression/expression.h"
#include "common/join_type.h"
#include "planner/operator/logical_accumulate.h"
#include "planner/operator/logical_plan.h"
#include "planner/query_planner.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void QueryPlanner::appendAccumulate(AccumulateType accumulateType,
    const expression_vector& expressionsToFlatten, LogicalPlan& plan) {
    auto op = make_shared<LogicalAccumulate>(
        accumulateType, expressionsToFlatten, plan.getLastOperator());
    appendFlattens(op->getGroupPositionsToFlatten(), plan);
    op->computeFactorizedSchema();
    plan.setLastOperator(std::move(op));
}

} // namespace planner
} // namespace kuzu
