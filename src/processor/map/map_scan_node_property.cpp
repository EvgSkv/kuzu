#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "binder/expression/property_expression.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "planner/operator/logical_operator.h"
#include "planner/operator/scan/logical_scan_node_property.h"
#include "processor/data_pos.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/scan/scan_node_table.h"
#include "processor/plan_mapper.h"
#include "storage/store/node_table.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapScanNodeProperty(
    LogicalOperator* logicalOperator) {
    auto& scanProperty = (const LogicalScanNodeProperty&)*logicalOperator;
    auto outSchema = scanProperty.getSchema();
    auto inSchema = scanProperty.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto inputNodeIDVectorPos = DataPos(inSchema->getExpressionPos(*scanProperty.getNodeID()));
    auto& nodeStore = storageManager.getNodesStore();
    std::vector<DataPos> outVectorsPos;
    for (auto& expression : scanProperty.getProperties()) {
        outVectorsPos.emplace_back(outSchema->getExpressionPos(*expression));
    }
    auto tableIDs = scanProperty.getTableIDs();
    if (tableIDs.size() > 1) {
        std::unordered_map<table_id_t, std::vector<column_id_t>> tableIDToColumns;
        std::unordered_map<table_id_t, storage::NodeTable*> tables;
        for (auto& tableID : tableIDs) {
            tables.insert({tableID, nodeStore.getNodeTable(tableID)});
            std::vector<uint32_t> columns;
            for (auto& expression : scanProperty.getProperties()) {
                auto property = static_pointer_cast<PropertyExpression>(expression);
                if (!property->hasPropertyID(tableID)) {
                    columns.push_back(UINT32_MAX);
                } else {
                    columns.push_back(
                        catalog->getReadOnlyVersion()->getTableSchema(tableID)->getColumnID(
                            property->getPropertyID(tableID)));
                }
            }
            tableIDToColumns.insert({tableID, std::move(columns)});
        }
        return std::make_unique<ScanMultiNodeTables>(inputNodeIDVectorPos, std::move(outVectorsPos),
            std::move(tables), std::move(tableIDToColumns), std::move(prevOperator),
            getOperatorID(), scanProperty.getExpressionsForPrinting());
    } else {
        auto tableID = tableIDs[0];
        auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(tableID);
        std::vector<column_id_t> columnIDs;
        for (auto& expression : scanProperty.getProperties()) {
            auto property = static_pointer_cast<PropertyExpression>(expression);
            if (property->hasPropertyID(tableID)) {
                columnIDs.push_back(tableSchema->getColumnID(property->getPropertyID(tableID)));
            } else {
                columnIDs.push_back(UINT32_MAX);
            }
        }
        return std::make_unique<ScanSingleNodeTable>(inputNodeIDVectorPos, std::move(outVectorsPos),
            nodeStore.getNodeTable(tableID), std::move(columnIDs), std::move(prevOperator),
            getOperatorID(), scanProperty.getExpressionsForPrinting());
    }
}

} // namespace processor
} // namespace kuzu
