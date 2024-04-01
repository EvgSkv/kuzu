#include "catalog/catalog.h"

#include "binder/ddl/bound_alter_info.h"
#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "catalog/catalog_entry/scalar_macro_catalog_entry.h"
#include "common/exception/catalog.h"
#include "storage/wal/wal.h"
#include "transaction/transaction.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;

namespace kuzu {
namespace catalog {

Catalog::Catalog(VirtualFileSystem* vfs) : wal{nullptr} {
    content = std::make_unique<CatalogContent>(vfs);
}

Catalog::Catalog(WAL* wal, VirtualFileSystem* vfs) : wal{wal} {
    content = std::make_unique<CatalogContent>(wal->getDirectory(), vfs);
}

uint64_t Catalog::getTableCount(Transaction* tx) const {
    return content->getNumTables(tx);
}

bool Catalog::containsNodeTable(Transaction* tx) const {
    return content->containsTable(tx, CatalogEntryType::NODE_TABLE_ENTRY);
}

bool Catalog::containsRelTable(Transaction* tx) const {
    return content->containsTable(tx, CatalogEntryType::REL_TABLE_ENTRY);
}

bool Catalog::containsTable(Transaction* tx, const std::string& tableName) const {
    return content->containsTable(tx, tableName);
}

table_id_t Catalog::getTableID(Transaction* tx, const std::string& tableName) const {
    return content->getTableID(tx, tableName);
}

std::vector<common::table_id_t> Catalog::getNodeTableIDs(Transaction* tx) const {
    return content->getTableIDs(tx, CatalogEntryType::NODE_TABLE_ENTRY);
}

std::vector<common::table_id_t> Catalog::getRelTableIDs(Transaction* tx) const {
    return content->getTableIDs(tx, CatalogEntryType::REL_TABLE_ENTRY);
}

std::string Catalog::getTableName(Transaction* tx, table_id_t tableID) const {
    return content->getTableName(tx, tableID);
}

TableCatalogEntry* Catalog::getTableCatalogEntry(Transaction* tx, table_id_t tableID) const {
    return ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(
        content->getTableCatalogEntry(tx, tableID));
}

std::vector<NodeTableCatalogEntry*> Catalog::getNodeTableEntries(Transaction* tx) const {
    return content->getTableCatalogEntries<NodeTableCatalogEntry*>(
        tx, CatalogEntryType::NODE_TABLE_ENTRY);
}

std::vector<RelTableCatalogEntry*> Catalog::getRelTableEntries(transaction::Transaction* tx) const {
    return content->getTableCatalogEntries<RelTableCatalogEntry*>(
        tx, CatalogEntryType::REL_TABLE_ENTRY);
}

std::vector<RelGroupCatalogEntry*> Catalog::getRelTableGroupEntries(
    transaction::Transaction* tx) const {
    return content->getTableCatalogEntries<RelGroupCatalogEntry*>(
        tx, CatalogEntryType::REL_GROUP_ENTRY);
}

std::vector<RDFGraphCatalogEntry*> Catalog::getRdfGraphEntries(transaction::Transaction* tx) const {
    return content->getTableCatalogEntries<RDFGraphCatalogEntry*>(
        tx, CatalogEntryType::RDF_GRAPH_ENTRY);
}

std::vector<TableCatalogEntry*> Catalog::getTableEntries(Transaction* tx) const {
    std::vector<TableCatalogEntry*> result;
    for (auto& [_, entry] : content->tables->getEntries(tx)) {
        result.push_back(ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(entry));
    }
    return result;
}

std::vector<TableCatalogEntry*> Catalog::getTableSchemas(
    Transaction* tx, const table_id_vector_t& tableIDs) const {
    std::vector<TableCatalogEntry*> result;
    for (auto tableID : tableIDs) {
        result.push_back(ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(
            content->getTableCatalogEntry(tx, tableID)));
    }
    return result;
}

table_id_t Catalog::createTableSchema(
    transaction::Transaction* transaction, const BoundCreateTableInfo& info) {
    return content->createTable(transaction, info);
}

void Catalog::dropTableSchema(transaction::Transaction* tx, table_id_t tableID) {
    content->dropTable(tx, tableID);
}

void Catalog::alterTableSchema(transaction::Transaction* transaction, const BoundAlterInfo& info) {
    content->alterTable(transaction, info);
}

void Catalog::addFunction(std::string name, function::function_set functionSet) {
    KU_ASSERT(content != nullptr);
    content->addFunction(std::move(name), std::move(functionSet));
}

void Catalog::addBuiltInFunction(std::string name, function::function_set functionSet) {
    content->addFunction(std::move(name), std::move(functionSet));
}

CatalogSet* Catalog::getFunctions(Transaction*) const {
    return content->functions.get();
}

CatalogEntry* Catalog::getFunctionEntry(Transaction* tx, const std::string& name) {
    auto catalogSet = content->functions.get();
    if (!catalogSet->containsEntry(tx, name)) {
        throw CatalogException(stringFormat("function {} does not exist.", name));
    }
    return catalogSet->getEntry(tx, name);
}

bool Catalog::containsMacro(Transaction*, const std::string& macroName) const {
    return content->containMacro(macroName);
}

void Catalog::addScalarMacroFunction(
    std::string name, std::unique_ptr<function::ScalarMacroFunction> macro) {
    KU_ASSERT(content != nullptr);
    auto scalarMacroCatalogEntry =
        std::make_unique<ScalarMacroCatalogEntry>(std::move(name), std::move(macro));
    content->functions->createEntry(&DUMMY_WRITE_TRANSACTION, std::move(scalarMacroCatalogEntry));
}

std::vector<std::string> Catalog::getMacroNames(Transaction* tx) const {
    std::vector<std::string> macroNames;
    for (auto& [_, function] : content->functions->getEntries(tx)) {
        if (function->getType() == CatalogEntryType::SCALAR_MACRO_ENTRY) {
            macroNames.push_back(function->getName());
        }
    }
    return macroNames;
}

void Catalog::setTableComment(
    transaction::Transaction* tx, table_id_t tableID, const std::string& comment) {
    KU_ASSERT(content != nullptr);
    auto tableEntry = content->getTableCatalogEntry(tx, tableID);
    ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(tableEntry)->setComment(comment);
}

} // namespace catalog
} // namespace kuzu
