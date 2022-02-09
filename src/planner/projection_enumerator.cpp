#include "src/planner/include/projection_enumerator.h"

#include "src/binder/include/expression/function_expression.h"
#include "src/planner/include/enumerator.h"
#include "src/planner/include/logical_plan/operator/aggregate/logical_aggregate.h"
#include "src/planner/include/logical_plan/operator/distinct/logical_distinct.h"
#include "src/planner/include/logical_plan/operator/limit/logical_limit.h"
#include "src/planner/include/logical_plan/operator/multiplicity_reducer/logical_multiplcity_reducer.h"
#include "src/planner/include/logical_plan/operator/order_by/logical_order_by.h"
#include "src/planner/include/logical_plan/operator/projection/logical_projection.h"
#include "src/planner/include/logical_plan/operator/skip/logical_skip.h"

namespace graphflow {
namespace planner {

void ProjectionEnumerator::enumerateProjectionBody(const BoundProjectionBody& projectionBody,
    const vector<unique_ptr<LogicalPlan>>& plans, bool isFinalReturn) {
    for (auto& plan : plans) {
        enumerateAggregate(projectionBody, *plan);
        enumerateOrderBy(projectionBody, *plan);
        // TODO: return expression rewrite should be moved out of enumerator
        auto expressionsToProject =
            getExpressionsToProject(projectionBody, *plan->schema, isFinalReturn);
        enumerateProjection(expressionsToProject, projectionBody.isProjectionDistinct(), *plan);
        enumerateSkipAndLimit(projectionBody, *plan);
        if (isFinalReturn) {
            plan->setExpressionsToCollect(expressionsToProject);
        }
    }
}

void ProjectionEnumerator::enumerateAggregate(
    const BoundProjectionBody& projectionBody, LogicalPlan& plan) {
    auto expressionsToAggregate = getExpressionsToAggregate(projectionBody, *plan.schema);
    if (expressionsToAggregate.empty()) {
        return;
    }
    vector<shared_ptr<Expression>> expressionsToProject;
    for (auto& expressionToAggregate : expressionsToAggregate) {
        if (expressionToAggregate->getChildren().empty()) { // skip COUNT(*)
            continue;
        }
        expressionsToProject.push_back(expressionToAggregate->getChild(0));
    }
    auto expressionsToGroupBy = getExpressionToGroupBy(projectionBody, *plan.schema);
    for (auto& expressionToGroupBy : expressionsToGroupBy) {
        expressionsToProject.push_back(expressionToGroupBy);
    }
    appendProjection(expressionsToProject, plan);
    appendAggregate(expressionsToGroupBy, expressionsToAggregate, plan);
}

void ProjectionEnumerator::enumerateOrderBy(
    const BoundProjectionBody& projectionBody, LogicalPlan& plan) {
    if (!projectionBody.hasOrderByExpressions()) {
        return;
    }
    appendOrderBy(projectionBody.getOrderByExpressions(), projectionBody.getSortingOrders(), plan);
}

void ProjectionEnumerator::enumerateProjection(
    const vector<shared_ptr<Expression>>& expressionsToProject, bool isDistinct,
    LogicalPlan& plan) {
    appendProjection(expressionsToProject, plan);
    if (isDistinct) {
        appendDistinct(expressionsToProject, plan);
    }
}

void ProjectionEnumerator::enumerateSkipAndLimit(
    const BoundProjectionBody& projectionBody, LogicalPlan& plan) {
    if (!projectionBody.hasSkip() && !projectionBody.hasLimit()) {
        return;
    }
    appendMultiplicityReducer(plan);
    if (projectionBody.hasSkip()) {
        appendSkip(projectionBody.getSkipNumber(), plan);
    }
    if (projectionBody.hasLimit()) {
        appendLimit(projectionBody.getLimitNumber(), plan);
    }
}

void ProjectionEnumerator::appendProjection(
    const vector<shared_ptr<Expression>>& expressionsToProject, LogicalPlan& plan) {
    vector<uint32_t> groupsPosToWrite;
    for (auto& expression : expressionsToProject) {
        enumerator->appendScanPropertiesAndPlanSubqueryIfNecessary(expression, plan);
        auto dependentGroupsPos = Enumerator::getDependentGroupsPos(expression, *plan.schema);
        groupsPosToWrite.push_back(enumerator->appendFlattensButOne(dependentGroupsPos, plan));
    }
    auto groupsPosInScopeBeforeProjection = plan.schema->getGroupsPosInScope();
    plan.schema->clearExpressionsInScope();
    for (auto i = 0u; i < expressionsToProject.size(); ++i) {
        plan.schema->insertToGroupAndScope(
            expressionsToProject[i]->getUniqueName(), groupsPosToWrite[i]);
    }
    auto groupsPosInScopeAfterProjection = plan.schema->getGroupsPosInScope();
    unordered_set<uint32_t> discardGroupsPos;
    for (auto i = 0; i < plan.schema->getNumGroups(); ++i) {
        if (groupsPosInScopeBeforeProjection.contains(i) &&
            !groupsPosInScopeAfterProjection.contains(i)) {
            discardGroupsPos.insert(i);
        }
    }
    auto projection = make_shared<LogicalProjection>(
        expressionsToProject, move(discardGroupsPos), plan.lastOperator);
    plan.appendOperator(move(projection));
}

void ProjectionEnumerator::appendDistinct(
    const vector<shared_ptr<Expression>>& expressionsToDistinct, LogicalPlan& plan) {
    for (auto& expression : expressionsToDistinct) {
        auto dependentGroupsPos = Enumerator::getDependentGroupsPos(expression, *plan.schema);
        Enumerator::appendFlattens(dependentGroupsPos, plan);
    }
    auto distinct =
        make_shared<LogicalDistinct>(expressionsToDistinct, plan.schema->copy(), plan.lastOperator);
    plan.schema->clear();
    auto groupPos = plan.schema->createGroup();
    for (auto& expression : expressionsToDistinct) {
        plan.schema->insertToGroupAndScope(expression->getUniqueName(), groupPos);
    }
    plan.appendOperator(move(distinct));
}

void ProjectionEnumerator::appendAggregate(
    const vector<shared_ptr<Expression>>& expressionsToGroupBy,
    const vector<shared_ptr<Expression>>& expressionsToAggregate, LogicalPlan& plan) {
    for (auto& expressionToGroupBy : expressionsToGroupBy) {
        auto dependentGroupsPos =
            Enumerator::getDependentGroupsPos(expressionToGroupBy, *plan.schema);
        enumerator->appendFlattens(dependentGroupsPos, plan);
    }
    for (auto& expressionToAggregate : expressionsToAggregate) {
        assert(isExpressionAggregate(expressionToAggregate->expressionType));
        auto& functionExpression = (FunctionExpression&)*expressionToAggregate;
        if (functionExpression.isFunctionDistinct()) {
            auto dependentGroupsPos =
                Enumerator::getDependentGroupsPos(expressionToAggregate, *plan.schema);
            enumerator->appendFlattens(dependentGroupsPos, plan);
        }
    }
    auto aggregate = make_shared<LogicalAggregate>(
        expressionsToGroupBy, expressionsToAggregate, plan.schema->copy(), plan.lastOperator);
    plan.schema->clear();
    auto groupPos = plan.schema->createGroup();
    for (auto& expression : expressionsToGroupBy) {
        plan.schema->insertToGroupAndScope(expression->getUniqueName(), groupPos);
    }
    for (auto& expression : expressionsToAggregate) {
        plan.schema->insertToGroupAndScope(expression->getUniqueName(), groupPos);
    }
    plan.appendOperator(move(aggregate));
}

void ProjectionEnumerator::appendOrderBy(const vector<shared_ptr<Expression>>& expressions,
    const vector<bool>& isAscOrders, LogicalPlan& plan) {
    vector<string> orderByExpressionNames;
    for (auto& expression : expressions) {
        enumerator->appendScanPropertiesAndPlanSubqueryIfNecessary(expression, plan);
        // We only allow orderby key(s) to be unflat, if they are all part of the same factorization
        // group and there is no other factorized group in the schema, so any payload is also unflat
        // and part of the same factorization group. The rationale for this limitation is this: (1)
        // to keep both the frontend and orderby operators simpler, we want order by to not change
        // the schema, so the input and output of order by should have the same factorization
        // structure. (2) Because orderby needs to flatten the keys to sort, if a key column that is
        // unflat is the input, we need to somehow flatten it in the factorized table. However
        // whenever we can we want to avoid adding an explicit flatten operator as this makes us
        // fall back to tuple-at-a-time processing. However in the specified limited case, we can
        // give factorized table a set of unflat vectors (all in the same datachunk/factorization
        // group), sort the table, and scan into unflat vectors, so the schema remains the same. In
        // more complicated cases, e.g., when there are 2 factorization groups, FactorizedTable
        // cannot read back a flat column into an unflat vector.
        if (plan.schema->groups.size() > 1) {
            auto dependentGroupsPos = Enumerator::getDependentGroupsPos(expression, *plan.schema);
            enumerator->appendFlattens(dependentGroupsPos, plan);
        }
        orderByExpressionNames.push_back(expression->getUniqueName());
    }
    auto schemaBeforeOrderBy = plan.schema->copy();
    plan.schema->clear();
    Enumerator::computeSchemaForHashJoinAndOrderByAndUnionAll(
        schemaBeforeOrderBy->getGroupsPosInScope(), *schemaBeforeOrderBy, *plan.schema);
    auto orderBy = make_shared<LogicalOrderBy>(orderByExpressionNames, isAscOrders,
        schemaBeforeOrderBy->copy(), schemaBeforeOrderBy->getExpressionNamesInScope(),
        plan.lastOperator);
    plan.appendOperator(move(orderBy));
}

void ProjectionEnumerator::appendMultiplicityReducer(LogicalPlan& plan) {
    plan.appendOperator(make_shared<LogicalMultiplicityReducer>(plan.lastOperator));
}

void ProjectionEnumerator::appendLimit(uint64_t limitNumber, LogicalPlan& plan) {
    auto groupPosToSelect =
        enumerator->appendFlattensButOne(plan.schema->getGroupsPosInScope(), plan);
    auto limit = make_shared<LogicalLimit>(
        limitNumber, groupPosToSelect, plan.schema->getGroupsPosInScope(), plan.lastOperator);
    for (auto& group : plan.schema->groups) {
        group->estimatedCardinality = limitNumber;
    }
    plan.appendOperator(move(limit));
}

void ProjectionEnumerator::appendSkip(uint64_t skipNumber, LogicalPlan& plan) {
    auto groupPosToSelect =
        enumerator->appendFlattensButOne(plan.schema->getGroupsPosInScope(), plan);
    auto skip = make_shared<LogicalSkip>(
        skipNumber, groupPosToSelect, plan.schema->getGroupsPosInScope(), plan.lastOperator);
    for (auto& group : plan.schema->groups) {
        group->estimatedCardinality -= skipNumber;
    }
    plan.appendOperator(move(skip));
}

vector<shared_ptr<Expression>> ProjectionEnumerator::getExpressionToGroupBy(
    const BoundProjectionBody& projectionBody, const Schema& schema) {
    vector<shared_ptr<Expression>> expressionsToGroupBy;
    for (auto& expression : projectionBody.getProjectionExpressions()) {
        if (Enumerator::getAggregationExpressionsNotInSchema(expression, schema).empty()) {
            expressionsToGroupBy.push_back(expression);
        }
    }
    return expressionsToGroupBy;
}

vector<shared_ptr<Expression>> ProjectionEnumerator::getExpressionsToAggregate(
    const BoundProjectionBody& projectionBody, const Schema& schema) {
    vector<shared_ptr<Expression>> expressionsToAggregate;
    for (auto& expression : projectionBody.getProjectionExpressions()) {
        for (auto& aggExpression :
            Enumerator::getAggregationExpressionsNotInSchema(expression, schema)) {
            expressionsToAggregate.push_back(aggExpression);
        }
    }
    return expressionsToAggregate;
}

vector<shared_ptr<Expression>> ProjectionEnumerator::getExpressionsToProject(
    const BoundProjectionBody& projectionBody, const Schema& schema,
    bool isRewritingAllProperties) {
    vector<shared_ptr<Expression>> expressionsToProject;
    for (auto& expression : projectionBody.getProjectionExpressions()) {
        if (expression->expressionType == VARIABLE) {
            for (auto& property :
                rewriteVariableExpression(expression, schema, isRewritingAllProperties)) {
                expressionsToProject.push_back(property);
            }
        } else {
            expressionsToProject.push_back(expression);
        }
    }
    return expressionsToProject;
}

vector<shared_ptr<Expression>> ProjectionEnumerator::rewriteVariableExpression(
    const shared_ptr<Expression>& variable, const Schema& schema, bool isRewritingAllProperties) {
    GF_ASSERT(VARIABLE == variable->expressionType);
    vector<shared_ptr<Expression>> result;
    vector<shared_ptr<Expression>> allProperties;
    if (NODE == variable->dataType) {
        auto node = static_pointer_cast<NodeExpression>(variable);
        allProperties = rewriteNodeExpressionAsAllProperties(node);
        if (!isRewritingAllProperties) {
            allProperties.push_back(node->getNodeIDPropertyExpression());
        }
    } else if (REL == variable->dataType) {
        auto rel = static_pointer_cast<RelExpression>(variable);
        allProperties = rewriteRelExpressionAsAllProperties(rel);
    }
    for (auto& property : allProperties) {
        if (isRewritingAllProperties || schema.expressionInScope(property->getUniqueName())) {
            result.push_back(property);
        }
    }
    return result;
}

vector<shared_ptr<Expression>> ProjectionEnumerator::rewriteNodeExpressionAsAllProperties(
    const shared_ptr<NodeExpression>& node) {
    vector<shared_ptr<Expression>> result;
    for (auto& property :
        createPropertyExpressions(node, catalog.getAllNodeProperties(node->label))) {
        result.push_back(property);
    }
    return result;
}

vector<shared_ptr<Expression>> ProjectionEnumerator::rewriteRelExpressionAsAllProperties(
    const shared_ptr<RelExpression>& rel) {
    vector<shared_ptr<Expression>> result;
    for (auto& property : createPropertyExpressions(rel, catalog.getRelProperties(rel->label))) {
        result.push_back(property);
    }
    return result;
}

vector<shared_ptr<Expression>> ProjectionEnumerator::createPropertyExpressions(
    const shared_ptr<Expression>& variable, const vector<PropertyDefinition>& properties) {
    vector<shared_ptr<Expression>> result;
    for (auto& property : properties) {
        result.emplace_back(make_shared<PropertyExpression>(
            property.dataType, property.name, property.id, variable));
    }
    return result;
}

} // namespace planner
} // namespace graphflow
