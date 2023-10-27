#include "processor/operator/persistent/copy_rel.h"

#include <memory>
#include <mutex>
#include <utility>

#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "processor/execution_context.h"
#include "processor/result/factorized_table.h"
#include "processor/result/result_set.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"
#include "storage/stats/rels_store_statistics.h"
#include "storage/wal/wal.h"

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

CopyRelSharedState::CopyRelSharedState(table_id_t tableID, RelsStoreStats* relsStatistics,
    std::unique_ptr<DirectedInMemRelData> fwdRelData,
    std::unique_ptr<DirectedInMemRelData> bwdRelData, MemoryManager* memoryManager)
    : tableID{tableID}, relsStatistics{relsStatistics}, fwdRelData{std::move(fwdRelData)},
      bwdRelData{std::move(bwdRelData)}, hasLoggedWAL{false}, numRows{0} {
    auto ftTableSchema = std::make_unique<FactorizedTableSchema>();
    ftTableSchema->appendColumn(
        std::make_unique<ColumnSchema>(false /* flat */, 0 /* dataChunkPos */,
            LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::STRING})));
    fTable = std::make_shared<FactorizedTable>(memoryManager, std::move(ftTableSchema));
}

void CopyRelSharedState::logCopyRelWALRecord(WAL* wal) {
    std::unique_lock xLck{mtx};
    if (!hasLoggedWAL) {
        wal->logCopyRelRecord(tableID);
        wal->flushAllPages();
        hasLoggedWAL = true;
    }
}

void CopyRel::initLocalStateInternal(ResultSet* /*resultSet_*/, ExecutionContext* /*context*/) {
    localState = std::make_unique<CopyRelLocalState>();
    localState->fwdCopyStates.resize(this->info.schema->getNumProperties());
    for (auto i = 0u; i < this->info.schema->getNumProperties(); i++) {
        localState->fwdCopyStates[i] = std::make_unique<PropertyCopyState>();
    }
    localState->bwdCopyStates.resize(this->info.schema->getNumProperties());
    for (auto i = 0u; i < this->info.schema->getNumProperties(); i++) {
        localState->bwdCopyStates[i] = std::make_unique<PropertyCopyState>();
    }
}

void CopyRel::initGlobalStateInternal(ExecutionContext* /*context*/) {
    if (!isCopyAllowed()) {
        throw CopyException(ExceptionMessage::notAllowCopyOnNonEmptyTableException());
    }
}

} // namespace processor
} // namespace kuzu
