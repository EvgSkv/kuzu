#include "storage/store/rel_table.h"

#include "common/cast.h"
#include "common/exception/message.h"
#include "storage/local_storage/local_rel_table.h"
#include "storage/stats/rels_store_statistics.h"
#include "storage/store/rel_table_data.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

RelDetachDeleteState::RelDetachDeleteState() {
    auto tempSharedState = std::make_shared<DataChunkState>();
    dstNodeIDVector = std::make_unique<ValueVector>(LogicalType{LogicalTypeID::INTERNAL_ID});
    relIDVector = std::make_unique<ValueVector>(LogicalType{LogicalTypeID::INTERNAL_ID});
    dstNodeIDVector->setState(tempSharedState);
    relIDVector->setState(tempSharedState);
}

RelTable::RelTable(BMFileHandle* dataFH, BMFileHandle* metadataFH, RelsStoreStats* relsStoreStats,
    MemoryManager* memoryManager, RelTableCatalogEntry* relTableEntry, WAL* wal,
    bool enableCompression)
    : Table{relTableEntry, relsStoreStats, memoryManager, wal} {
    fwdRelTableData = std::make_unique<RelTableData>(dataFH, metadataFH, bufferManager, wal,
        relTableEntry, relsStoreStats, RelDataDirection::FWD, enableCompression);
    bwdRelTableData = std::make_unique<RelTableData>(dataFH, metadataFH, bufferManager, wal,
        relTableEntry, relsStoreStats, RelDataDirection::BWD, enableCompression);
}

void RelTable::read(Transaction* transaction, TableReadState& readState) {
    auto& relReadState = ku_dynamic_cast<TableReadState&, RelTableReadState&>(readState);
    if (relReadState.readFromLocalStorage) {
        auto& localReadState =
            ku_dynamic_cast<LocalReadState&, LocalRelReadState&>(*relReadState.localState);
        localReadState.localNodeGroup->scan(readState);
        return;
    }
    scan(transaction, relReadState);
}

void RelTable::insert(Transaction* transaction, TableInsertState& insertState) {
    auto localTable =
        transaction->getLocalStorage()->getLocalTable(tableID, NotExistAction::CREATE);
    if (localTable->insert(insertState)) {
        auto relsStats = ku_dynamic_cast<TablesStatistics*, RelsStoreStats*>(tablesStatistics);
        relsStats->updateNumTuplesByValue(tableID, 1);
    }
}

void RelTable::update(Transaction* transaction, TableUpdateState& updateState) {
    auto localTable =
        transaction->getLocalStorage()->getLocalTable(tableID, NotExistAction::CREATE);
    localTable->update(updateState);
}

void RelTable::delete_(Transaction* transaction, TableDeleteState& deleteState) {
    auto localTable =
        transaction->getLocalStorage()->getLocalTable(tableID, NotExistAction::CREATE);
    if (localTable->delete_(deleteState)) {
        auto relsStats = ku_dynamic_cast<TablesStatistics*, RelsStoreStats*>(tablesStatistics);
        relsStats->updateNumTuplesByValue(tableID, -1);
    }
}

void RelTable::detachDelete(Transaction* transaction, RelDataDirection direction,
    ValueVector* srcNodeIDVector, RelDetachDeleteState* deleteState) {
    KU_ASSERT(srcNodeIDVector->state->selVector->selectedSize == 1);
    std::vector<column_id_t> relIDColumns = {REL_ID_COLUMN_ID};
    auto readOutputVectors = std::vector<ValueVector*>{
        deleteState->dstNodeIDVector.get(), deleteState->relIDVector.get()};
    auto relReadState = std::make_unique<RelTableReadState>(
        *srcNodeIDVector, relIDColumns, readOutputVectors, direction);
    initializeReadState(transaction, direction, relIDColumns, *srcNodeIDVector, *relReadState);
    row_idx_t numRelsDeleted =
        detachDeleteForCSRRels(transaction, srcNodeIDVector, relReadState.get(), deleteState);
    auto relsStats = ku_dynamic_cast<TablesStatistics*, RelsStoreStats*>(tablesStatistics);
    relsStats->updateNumTuplesByValue(tableID, -numRelsDeleted);
}

void RelTable::checkIfNodeHasRels(
    Transaction* transaction, RelDataDirection direction, ValueVector* srcNodeIDVector) {
    KU_ASSERT(srcNodeIDVector->state->isFlat());
    auto nodeIDPos = srcNodeIDVector->state->selVector->selectedPositions[0];
    auto nodeOffset = srcNodeIDVector->getValue<nodeID_t>(nodeIDPos).offset;
    auto res = direction == common::RelDataDirection::FWD ?
                   fwdRelTableData->checkIfNodeHasRels(transaction, nodeOffset) :
                   bwdRelTableData->checkIfNodeHasRels(transaction, nodeOffset);
    if (res) {
        throw RuntimeException(ExceptionMessage::violateDeleteNodeWithConnectedEdgesConstraint(
            tableName, std::to_string(nodeOffset),
            RelDataDirectionUtils::relDirectionToString(direction)));
    }
}

row_idx_t RelTable::detachDeleteForCSRRels(Transaction* transaction, ValueVector* srcNodeIDVector,
    RelTableReadState* relDataReadState, RelDetachDeleteState* detachDeleteState) {
    row_idx_t numRelsDeleted = 0;
    auto tempState = detachDeleteState->dstNodeIDVector->state.get();
    auto deleteState = std::make_unique<RelTableDeleteState>(
        *srcNodeIDVector, *detachDeleteState->dstNodeIDVector, *detachDeleteState->relIDVector);
    auto localTable =
        transaction->getLocalStorage()->getLocalTable(tableID, NotExistAction::CREATE);
    while (relDataReadState->hasMoreToRead(transaction)) {
        scan(transaction, *relDataReadState);
        auto numRelsScanned = tempState->selVector->selectedSize;
        tempState->selVector->resetSelectorToValuePosBufferWithSize(1);
        for (auto i = 0u; i < numRelsScanned; i++) {
            tempState->selVector->selectedPositions[0] = i;
            numRelsDeleted += localTable->delete_(*deleteState);
        }
        tempState->selVector->resetSelectorToUnselectedWithSize(DEFAULT_VECTOR_CAPACITY);
    }
    return numRelsDeleted;
}

void RelTable::scan(Transaction* transaction, RelTableReadState& scanState) {
    auto tableData = getDirectedTableData(scanState.direction);
    tableData->scan(
        transaction, *scanState.dataReadState, scanState.nodeIDVector, scanState.outputVectors);
}

void RelTable::addColumn(
    Transaction* transaction, const Property& property, ValueVector* defaultValueVector) {
    auto relsStats = ku_dynamic_cast<TablesStatistics*, RelsStoreStats*>(tablesStatistics);
    relsStats->setPropertyStatisticsForTable(tableID, property.getPropertyID(),
        PropertyStatistics{!defaultValueVector->hasNoNullsGuarantee()});
    relsStats->addMetadataDAHInfo(tableID, *property.getDataType());
    fwdRelTableData->addColumn(transaction,
        RelDataDirectionUtils::relDirectionToString(RelDataDirection::FWD),
        fwdRelTableData->getNbrIDColumn()->getMetadataDA(),
        *relsStats->getColumnMetadataDAHInfo(
            transaction, tableID, fwdRelTableData->getNumColumns(), RelDataDirection::FWD),
        property, defaultValueVector, relsStats);
    bwdRelTableData->addColumn(transaction,
        RelDataDirectionUtils::relDirectionToString(RelDataDirection::BWD),
        bwdRelTableData->getNbrIDColumn()->getMetadataDA(),
        *relsStats->getColumnMetadataDAHInfo(
            transaction, tableID, bwdRelTableData->getNumColumns(), RelDataDirection::BWD),
        property, defaultValueVector, relsStats);
    // TODO(Guodong): addColumn is not going through localStorage design for now. So it needs to add
    // tableID into the wal's updated table set separately, as it won't trigger prepareCommit.
    wal->addToUpdatedTables(tableID);
}

// TODO: Should remove.
void RelTable::prepareCommit(Transaction*, LocalTable*) {}

void RelTable::prepareRollback(LocalTable*) {}

void RelTable::checkpointInMemory() {
    fwdRelTableData->checkpointInMemory();
    bwdRelTableData->checkpointInMemory();
}

void RelTable::rollbackInMemory() {
    fwdRelTableData->rollbackInMemory();
    bwdRelTableData->rollbackInMemory();
}

} // namespace storage
} // namespace kuzu
