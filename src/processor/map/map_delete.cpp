#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "binder/expression/node_expression.h"
#include "binder/expression/rel_expression.h"
#include "common/types/internal_id_t.h"
#include "planner/operator/logical_operator.h"
#include "planner/operator/persistent/logical_delete.h"
#include "planner/operator/schema.h"
#include "processor/data_pos.h"
#include "processor/operator/persistent/delete.h"
#include "processor/operator/persistent/delete_executor.h"
#include "processor/operator/physical_operator.h"
#include "processor/plan_mapper.h"
#include "storage/stats/rels_store_statistics.h"
#include "storage/store/node_table.h"
#include "storage/store/nodes_store.h"
#include "storage/store/rel_table.h"
#include "storage/store/rels_store.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

static std::unique_ptr<NodeDeleteExecutor> getNodeDeleteExecutor(
    NodesStore* store, const NodeExpression& node, const Schema& inSchema) {
    auto nodeIDPos = DataPos(inSchema.getExpressionPos(*node.getInternalID()));
    if (node.isMultiLabeled()) {
        std::unordered_map<table_id_t, NodeTable*> tableIDToTableMap;
        for (auto tableID : node.getTableIDs()) {
            auto table = store->getNodeTable(tableID);
            tableIDToTableMap.insert({tableID, table});
        }
        return std::make_unique<MultiLabelNodeDeleteExecutor>(
            std::move(tableIDToTableMap), nodeIDPos);
    } else {
        auto table = store->getNodeTable(node.getSingleTableID());
        return std::make_unique<SingleLabelNodeDeleteExecutor>(table, nodeIDPos);
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDeleteNode(LogicalOperator* logicalOperator) {
    auto logicalDeleteNode = (LogicalDeleteNode*)logicalOperator;
    auto inSchema = logicalDeleteNode->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto nodesStore = &storageManager.getNodesStore();
    std::vector<std::unique_ptr<NodeDeleteExecutor>> executors;
    for (auto& node : logicalDeleteNode->getNodesRef()) {
        executors.push_back(getNodeDeleteExecutor(nodesStore, *node, *inSchema));
    }
    return std::make_unique<DeleteNode>(std::move(executors), std::move(prevOperator),
        getOperatorID(), logicalDeleteNode->getExpressionsForPrinting());
}

static std::unique_ptr<RelDeleteExecutor> getRelDeleteExecutor(
    storage::RelsStore* store, const binder::RelExpression& rel, const Schema& inSchema) {
    auto srcNodePos = DataPos(inSchema.getExpressionPos(*rel.getSrcNode()->getInternalID()));
    auto dstNodePos = DataPos(inSchema.getExpressionPos(*rel.getDstNode()->getInternalID()));
    auto relIDPos = DataPos(inSchema.getExpressionPos(*rel.getInternalIDProperty()));
    auto statistics = store->getRelsStatistics();
    if (rel.isMultiLabeled()) {
        std::unordered_map<table_id_t, std::pair<storage::RelTable*, storage::RelsStoreStats*>>
            tableIDToTableMap;
        for (auto tableID : rel.getTableIDs()) {
            auto table = store->getRelTable(tableID);
            tableIDToTableMap.insert({tableID, std::make_pair(table, statistics)});
        }
        return std::make_unique<MultiLabelRelDeleteExecutor>(
            std::move(tableIDToTableMap), srcNodePos, dstNodePos, relIDPos);
    } else {
        auto table = store->getRelTable(rel.getSingleTableID());
        return std::make_unique<SingleLabelRelDeleteExecutor>(
            statistics, table, srcNodePos, dstNodePos, relIDPos);
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapDeleteRel(LogicalOperator* logicalOperator) {
    auto logicalDeleteRel = (LogicalDeleteRel*)logicalOperator;
    auto inSchema = logicalDeleteRel->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto relStore = &storageManager.getRelsStore();
    std::vector<std::unique_ptr<RelDeleteExecutor>> Executors;
    for (auto& rel : logicalDeleteRel->getRelsRef()) {
        Executors.push_back(getRelDeleteExecutor(relStore, *rel, *inSchema));
    }
    return std::make_unique<DeleteRel>(std::move(Executors), std::move(prevOperator),
        getOperatorID(), logicalOperator->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
