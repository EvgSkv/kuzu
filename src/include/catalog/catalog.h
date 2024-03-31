#pragma once

#include <memory>

#include "catalog/catalog_content.h"

namespace kuzu {
namespace storage {
class WAL;
}
namespace transaction {
class Transaction;
} // namespace transaction
namespace catalog {
class TableCatalogEntry;
class NodeTableCatalogEntry;
class RelTableCatalogEntry;
class RelGroupCatalogEntry;
class RDFGraphCatalogEntry;

class Catalog {
public:
    explicit Catalog(common::VirtualFileSystem* vfs);

    Catalog(storage::WAL* wal, common::VirtualFileSystem* vfs);

    // TODO(Guodong): Get rid of the following.
    inline CatalogContent* getContent() const { return content.get(); }

    // ----------------------------- Table Schemas ----------------------------
    uint64_t getTableCount(transaction::Transaction* tx) const;

    bool containsNodeTable(transaction::Transaction* tx) const;
    bool containsRelTable(transaction::Transaction* tx) const;
    bool containsTable(transaction::Transaction* tx, const std::string& tableName) const;

    common::table_id_t getTableID(transaction::Transaction* tx, const std::string& tableName) const;
    std::vector<common::table_id_t> getNodeTableIDs(transaction::Transaction* tx) const;
    std::vector<common::table_id_t> getRelTableIDs(transaction::Transaction* tx) const;

    std::string getTableName(transaction::Transaction* tx, common::table_id_t tableID) const;
    TableCatalogEntry* getTableCatalogEntry(
        transaction::Transaction* tx, common::table_id_t tableID) const;
    std::vector<NodeTableCatalogEntry*> getNodeTableEntries(transaction::Transaction* tx) const;
    std::vector<RelTableCatalogEntry*> getRelTableEntries(transaction::Transaction* tx) const;
    std::vector<RelGroupCatalogEntry*> getRelTableGroupEntries(transaction::Transaction* tx) const;
    std::vector<RDFGraphCatalogEntry*> getRdfGraphEntries(transaction::Transaction* tx) const;
    std::vector<TableCatalogEntry*> getTableEntries(transaction::Transaction* tx) const;
    std::vector<TableCatalogEntry*> getTableSchemas(
        transaction::Transaction* tx, const common::table_id_vector_t& tableIDs) const;

    common::table_id_t createTableSchema(
        transaction::Transaction* tx, const binder::BoundCreateTableInfo& info);
    void dropTableSchema(transaction::Transaction* tx, common::table_id_t tableID);
    void alterTableSchema(transaction::Transaction* tx, const binder::BoundAlterInfo& info);
    void renameTable(
        transaction::Transaction* tx, common::table_id_t tableID, const std::string& newName);

    void setTableComment(
        transaction::Transaction* tx, common::table_id_t tableID, const std::string& comment);

    // ----------------------------- Functions ----------------------------
    void addFunction(std::string name, function::function_set functionSet);
    void addBuiltInFunction(std::string name, function::function_set functionSet);
    CatalogSet* getFunctions(transaction::Transaction* tx) const;
    CatalogEntry* getFunctionEntry(transaction::Transaction* tx, const std::string& name);

    bool containsMacro(transaction::Transaction* tx, const std::string& macroName) const;
    void addScalarMacroFunction(
        std::string name, std::unique_ptr<function::ScalarMacroFunction> macro);
    // TODO(Ziyi): pass transaction pointer here.
    function::ScalarMacroFunction* getScalarMacroFunction(const std::string& name) const {
        return content->getScalarMacroFunction(name);
    }

    std::vector<std::string> getMacroNames(transaction::Transaction* tx) const;

    // TODO(Guodong): Should be removed.
    static void saveInitialCatalogToFile(
        const std::string& directory, common::VirtualFileSystem* vfs) {
        std::make_unique<Catalog>(vfs)->content->saveToFile(
            directory, common::FileVersionType::ORIGINAL);
    }

protected:
    std::unique_ptr<CatalogContent> content;
    storage::WAL* wal;
};

} // namespace catalog
} // namespace kuzu
