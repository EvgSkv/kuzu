#pragma once

#include "catalog/catalog_set.h"
#include "common/cast.h"

namespace kuzu {
namespace binder {
struct BoundAlterInfo;
struct BoundCreateTableInfo;
} // namespace binder
namespace common {
class Serializer;
class Deserializer;
class VirtualFileSystem;
} // namespace common
namespace function {
struct ScalarMacroFunction;
} // namespace function

namespace catalog {

// TODO(Guodong): Shoule be merged with Catalog.
class CatalogContent {
    friend class Catalog;

public:
    KUZU_API explicit CatalogContent(common::VirtualFileSystem* vfs);

    virtual ~CatalogContent() = default;

    CatalogContent(const std::string& directory, common::VirtualFileSystem* vfs);

    CatalogContent(std::unique_ptr<CatalogSet> tables, common::table_id_t nextTableID,
        std::unique_ptr<CatalogSet> functions, common::VirtualFileSystem* vfs)
        : tables{std::move(tables)}, nextTableID{nextTableID}, vfs{vfs}, functions{std::move(
                                                                             functions)} {}

    common::table_id_t getTableID(
        transaction::Transaction* transaction, const std::string& tableName) const;
    CatalogEntry* getTableCatalogEntry(
        transaction::Transaction* transaction, common::table_id_t tableID) const;

    void saveToFile(const std::string& directory, common::FileVersionType dbFileType);
    void readFromFile(const std::string& directory, common::FileVersionType dbFileType);

protected:
    common::table_id_t assignNextTableID() { return nextTableID++; }

private:
    // ----------------------------- Functions ----------------------------
    void registerBuiltInFunctions();

    bool containMacro(const std::string& macroName) const {
        return functions->containsEntry(&transaction::DUMMY_READ_TRANSACTION, macroName);
    }
    void addFunction(std::string name, function::function_set definitions);

    function::ScalarMacroFunction* getScalarMacroFunction(const std::string& name) const;

    // ----------------------------- Table entries ----------------------------
    uint64_t getNumTables(transaction::Transaction* transaction) const {
        return tables->getEntries(transaction).size();
    }

    bool containsTable(transaction::Transaction* transaction, const std::string& tableName) const;
    bool containsTable(transaction::Transaction* transaction, CatalogEntryType catalogType) const;

    std::string getTableName(
        transaction::Transaction* transaction, common::table_id_t tableID) const;

    template<typename T>
    std::vector<T> getTableCatalogEntries(
        transaction::Transaction* transaction, CatalogEntryType catalogType) const {
        std::vector<T> result;
        for (auto& [_, entry] : tables->getEntries(transaction)) {
            if (entry->getType() == catalogType) {
                result.push_back(common::ku_dynamic_cast<CatalogEntry*, T>(entry));
            }
        }
        return result;
    }

    std::vector<common::table_id_t> getTableIDs(
        transaction::Transaction* transaction, CatalogEntryType catalogType) const;

    common::table_id_t createTable(
        transaction::Transaction* transaction, const binder::BoundCreateTableInfo& info);
    void dropTable(transaction::Transaction* transaction, common::table_id_t tableID);
    void alterTable(transaction::Transaction* transaction, const binder::BoundAlterInfo& info);
    void renameTable(transaction::Transaction* transaction, common::table_id_t tableID,
        const std::string& newName);

private:
    std::unique_ptr<CatalogEntry> createNodeTableEntry(transaction::Transaction* transaction,
        common::table_id_t tableID, const binder::BoundCreateTableInfo& info) const;
    std::unique_ptr<CatalogEntry> createRelTableEntry(transaction::Transaction* transaction,
        common::table_id_t tableID, const binder::BoundCreateTableInfo& info) const;
    std::unique_ptr<CatalogEntry> createRelTableGroupEntry(transaction::Transaction* transaction,
        common::table_id_t tableID, const binder::BoundCreateTableInfo& info);
    std::unique_ptr<CatalogEntry> createRdfGraphEntry(transaction::Transaction* transaction,
        common::table_id_t tableID, const binder::BoundCreateTableInfo& info);

protected:
    std::unique_ptr<CatalogSet> tables;

private:
    common::table_id_t nextTableID;
    common::VirtualFileSystem* vfs;
    std::unique_ptr<CatalogSet> functions;
};

} // namespace catalog
} // namespace kuzu
