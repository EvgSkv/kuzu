#include "catalog/catalog.h"

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog_content.h"
#include "catalog/node_table_schema.h"
#include "catalog/rel_table_group_schema.h"
#include "catalog/rel_table_schema.h"
#include "catalog/table_schema.h"
#include "common/expression_type.h"
#include "common/table_type.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "function/scalar_macro_function.h"
#include "function/vector_functions.h"
#include "storage/wal/wal.h"
#include "transaction/transaction_action.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace catalog {

Catalog::Catalog() : wal{nullptr} {
    catalogContentForReadOnlyTrx = std::make_unique<CatalogContent>();
}

Catalog::Catalog(WAL* wal) : wal{wal} {
    catalogContentForReadOnlyTrx = std::make_unique<CatalogContent>(wal->getDirectory());
}

void Catalog::prepareCommitOrRollback(TransactionAction action) {
    if (hasUpdates()) {
        wal->logCatalogRecord();
        if (action == TransactionAction::COMMIT) {
            catalogContentForWriteTrx->saveToFile(wal->getDirectory(), DBFileType::WAL_VERSION);
        }
    }
}

void Catalog::checkpointInMemory() {
    if (hasUpdates()) {
        catalogContentForReadOnlyTrx = std::move(catalogContentForWriteTrx);
    }
}

ExpressionType Catalog::getFunctionType(const std::string& name) const {
    return catalogContentForReadOnlyTrx->getFunctionType(name);
}

table_id_t Catalog::addNodeTableSchema(const binder::BoundCreateTableInfo& info) {
    initCatalogContentForWriteTrxIfNecessary();
    return catalogContentForWriteTrx->addNodeTableSchema(info);
}

table_id_t Catalog::addRelTableSchema(const binder::BoundCreateTableInfo& info) {
    initCatalogContentForWriteTrxIfNecessary();
    auto tableID = catalogContentForWriteTrx->addRelTableSchema(info);
    wal->logRelTableRecord(tableID);
    return tableID;
}

common::table_id_t Catalog::addRelTableGroupSchema(const binder::BoundCreateTableInfo& info) {
    initCatalogContentForWriteTrxIfNecessary();
    auto tableID = catalogContentForWriteTrx->addRelTableGroupSchema(info);
    auto relTableGroupSchema =
        (RelTableGroupSchema*)catalogContentForWriteTrx->getTableSchema(tableID);
    for (auto relTableID : relTableGroupSchema->getRelTableIDs()) {
        wal->logRelTableRecord(relTableID);
    }
    return tableID;
}

table_id_t Catalog::addRdfGraphSchema(const binder::BoundCreateTableInfo& info) {
    initCatalogContentForWriteTrxIfNecessary();
    return catalogContentForWriteTrx->addRdfGraphSchema(info);
}

void Catalog::dropTableSchema(table_id_t tableID) {
    initCatalogContentForWriteTrxIfNecessary();
    auto tableSchema = catalogContentForWriteTrx->getTableSchema(tableID);
    switch (tableSchema->tableType) {
    case TableType::REL_GROUP: {
        auto relTableGroupSchema = reinterpret_cast<RelTableGroupSchema*>(tableSchema);
        auto relTableIDs = relTableGroupSchema->getRelTableIDs();
        catalogContentForWriteTrx->dropTableSchema(tableID);
        for (auto relTableID : relTableIDs) {
            wal->logDropTableRecord(relTableID);
        }
    } break;
    default: {
        catalogContentForWriteTrx->dropTableSchema(tableID);
        wal->logDropTableRecord(tableID);
    }
    }
}

void Catalog::renameTable(table_id_t tableID, const std::string& newName) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->renameTable(tableID, newName);
}

void Catalog::addNodeProperty(
    table_id_t tableID, const std::string& propertyName, std::unique_ptr<LogicalType> dataType) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->addNodeProperty(
        propertyName, std::move(dataType));
}

void Catalog::addRelProperty(
    table_id_t tableID, const std::string& propertyName, std::unique_ptr<LogicalType> dataType) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->addRelProperty(
        propertyName, std::move(dataType));
    wal->logAddPropertyRecord(
        tableID, catalogContentForWriteTrx->getTableSchema(tableID)->getPropertyID(propertyName));
}

void Catalog::dropProperty(table_id_t tableID, property_id_t propertyID) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->dropProperty(propertyID);
    wal->logDropPropertyRecord(tableID, propertyID);
}

void Catalog::renameProperty(
    table_id_t tableID, property_id_t propertyID, const std::string& newName) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->renameProperty(propertyID, newName);
}

std::unordered_set<TableSchema*> Catalog::getAllRelTableSchemasContainBoundTable(
    table_id_t boundTableID) const {
    std::unordered_set<TableSchema*> relTableSchemas;
    auto nodeTableSchema =
        reinterpret_cast<NodeTableSchema*>(getReadOnlyVersion()->getTableSchema(boundTableID));
    for (auto& fwdRelTableID : nodeTableSchema->getFwdRelTableIDSet()) {
        relTableSchemas.insert(
            reinterpret_cast<RelTableSchema*>(getReadOnlyVersion()->getTableSchema(fwdRelTableID)));
    }
    for (auto& bwdRelTableID : nodeTableSchema->getBwdRelTableIDSet()) {
        relTableSchemas.insert(
            reinterpret_cast<RelTableSchema*>(getReadOnlyVersion()->getTableSchema(bwdRelTableID)));
    }
    return relTableSchemas;
}

void Catalog::addVectorFunction(
    std::string name, function::vector_function_definitions definitions) {
    catalogContentForReadOnlyTrx->addVectorFunction(std::move(name), std::move(definitions));
}

void Catalog::addScalarMacroFunction(
    std::string name, std::unique_ptr<function::ScalarMacroFunction> macro) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->addScalarMacroFunction(std::move(name), std::move(macro));
}

void Catalog::setTableComment(table_id_t tableID, const std::string& comment) {
    initCatalogContentForWriteTrxIfNecessary();
    catalogContentForWriteTrx->getTableSchema(tableID)->setComment(comment);
}

} // namespace catalog
} // namespace kuzu
