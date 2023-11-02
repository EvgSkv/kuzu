#include "graph_test/graph_test.h"

#include "binder/binder.h"
#include "parser/parser.h"
#include "spdlog/spdlog.h"
#include "storage/storage_manager.h"

using ::testing::Test;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace testing {

void PrivateGraphTest::validateQueryBestPlanJoinOrder(
    std::string query, std::string expectedJoinOrder) {
    auto catalog = getCatalog(*database);
    auto statement = parser::Parser::parseQuery(query);
    auto parsedQuery = (parser::RegularQuery*)statement.get();
    auto boundQuery = Binder(*catalog, database->memoryManager.get(),
        database->storageManager.get(), conn->clientContext.get())
                          .bind(*parsedQuery);
    auto plan = Planner::getBestPlan(*catalog,
        *getStorageManager(*database)->getNodesStore().getNodesStatisticsAndDeletedIDs(),
        *getStorageManager(*database)->getRelsStore().getRelsStatistics(), *boundQuery);
    ASSERT_STREQ(LogicalPlanUtil::encodeJoin(*plan).c_str(), expectedJoinOrder.c_str());
}

void DBTest::createDB(uint64_t checkpointWaitTimeout) {
    if (database != nullptr) {
        database.reset();
    }
    database = std::make_unique<main::Database>(databasePath, *systemConfig);
    getTransactionManager(*database)->setCheckPointWaitTimeoutForTransactionsToLeaveInMicros(
        checkpointWaitTimeout /* 10ms */);
    spdlog::set_level(spdlog::level::info);
}

} // namespace testing
} // namespace kuzu
