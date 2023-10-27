#include "processor/operator/scan/scan_node_table.h"

#include "common/types/internal_id_t.h"
#include "processor/execution_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

bool ScanSingleNodeTable::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    for (auto& outputVector : outPropertyVectors) {
        outputVector->resetAuxiliaryBuffer();
    }
    table->read(transaction, inputNodeIDVector, columnIDs, outPropertyVectors);
    return true;
}

bool ScanMultiNodeTables::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto tableID =
        inputNodeIDVector
            ->getValue<nodeID_t>(inputNodeIDVector->state->selVector->selectedPositions[0])
            .tableID;
    tables.at(tableID)->read(
        transaction, inputNodeIDVector, tableIDToScanColumnIds.at(tableID), outPropertyVectors);
    return true;
}

} // namespace processor
} // namespace kuzu
