#pragma once

#include "catalog_entry/catalog_entry.h"
#include "common/case_insensitive_map.h"

namespace kuzu {
namespace storage {
class UndoBuffer;
} // namespace storage

namespace catalog {

class CatalogSet {
    friend class storage::UndoBuffer;

public:
    //===--------------------------------------------------------------------===//
    // getters & setters
    //===--------------------------------------------------------------------===//
    bool containsEntry(transaction::Transaction* transaction, const std::string& name) const;
    CatalogEntry* getEntry(transaction::Transaction* transaction, const std::string& name);
    KUZU_API void createEntry(
        transaction::Transaction* transaction, std::unique_ptr<CatalogEntry> entry);
    void dropEntry(transaction::Transaction* transaction, const std::string& name);
    void renameEntry(transaction::Transaction* transaction, const std::string& oldName,
        const std::string& newName);
    common::case_insensitive_map_t<CatalogEntry*> getEntries(transaction::Transaction* transaction);

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer serializer) const;
    static std::unique_ptr<CatalogSet> deserialize(common::Deserializer& deserializer);

private:
    void emplace(std::unique_ptr<CatalogEntry> entry);
    void erase(const std::string& name);

    std::unique_ptr<CatalogEntry> createDummyEntry(std::string name) const;

    CatalogEntry* traverseVersionChainsForTransaction(
        transaction::Transaction* transaction, CatalogEntry* currentEntry) const;
    bool checkWWConflict(transaction::Transaction* transaction, CatalogEntry* entry) const;

private:
    common::case_insensitive_map_t<std::unique_ptr<CatalogEntry>> entries;
};

} // namespace catalog
} // namespace kuzu
