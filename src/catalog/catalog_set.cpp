#include "catalog/catalog_set.h"

#include "binder/ddl/bound_alter_info.h"
#include "catalog/catalog_entry/dummy_catalog_entry.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/assert.h"
#include "common/exception/catalog.h"
#include "common/string_format.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace catalog {

// TODO(Guodong): Remove this function.
bool CatalogSet::containsEntry(Transaction* transaction, const std::string& name) const {
    if (!entries.contains(name)) {
        return false;
    }
    // Check versions.
    auto entry = traverseVersionChainsForTransaction(transaction, entries.at(name).get());
    return !entry->isDeleted();
}

// TODO(Guodong): Should refactor this function to support different handling of not found entry.
CatalogEntry* CatalogSet::getEntry(Transaction* transaction, const std::string& name) {
    // LCOV_EXCL_START
    // We should not trigger the following check. If so, we should throw more informative error
    // message at catalog level.
    if (!containsEntry(transaction, name)) {
        throw CatalogException(stringFormat("Cannot find catalog entry with name {}.", name));
    }
    // LCOV_EXCL_STOP
    auto entry = traverseVersionChainsForTransaction(transaction, entries.at(name).get());
    KU_ASSERT(entry != nullptr && !entry->isDeleted());
    return entry;
}

void CatalogSet::createEntry(Transaction* transaction, std::unique_ptr<CatalogEntry> entry) {
    entry->setTimestamp(transaction->getID());
    if (entries.contains(entry->getName())) {
        auto existingEntry = entries.at(entry->getName()).get();
        if (checkWWConflict(transaction, existingEntry)) {
            throw CatalogException(stringFormat(
                "Write-write conflict on creating catalog entry with name {}.", entry->getName()));
        }
        if (!existingEntry->isDeleted()) {
            throw CatalogException(
                stringFormat("Catalog entry with name {} already exists.", entry->getName()));
        }
    }
    auto dummyEntry = createDummyEntry(entry->getName());
    entries.emplace(entry->getName(), std::move(dummyEntry));
    auto entryPtr = entry.get();
    emplace(std::move(entry));
    KU_ASSERT(transaction);
    if (transaction->getStartTS() > 0) {
        KU_ASSERT(transaction->getID() != common::INVALID_TRANSACTION);
        transaction->addCatalogEntry(this, entryPtr->getPrev());
    }
}

void CatalogSet::emplace(std::unique_ptr<CatalogEntry> entry) {
    if (entries.contains(entry->getName())) {
        entry->setPrev(std::move(entries.at(entry->getName())));
        entries.erase(entry->getName());
    }
    entries.emplace(entry->getName(), std::move(entry));
}

void CatalogSet::erase(const std::string& name) {
    entries.erase(name);
}

std::unique_ptr<CatalogEntry> CatalogSet::createDummyEntry(std::string name) const {
    return std::make_unique<DummyCatalogEntry>(std::move(name));
}

CatalogEntry* CatalogSet::traverseVersionChainsForTransaction(
    Transaction* transaction, CatalogEntry* currentEntry) const {
    while (currentEntry) {
        if (currentEntry->getTimestamp() == transaction->getID()) {
            // This entry is created by the current transaction.
            break;
        }
        if (currentEntry->getTimestamp() <= transaction->getStartTS()) {
            // This entry was committed before the current transaction starts.
            break;
        }
        currentEntry = currentEntry->getPrev();
    }
    return currentEntry;
}

bool CatalogSet::checkWWConflict(Transaction* transaction, CatalogEntry* entry) const {
    return (entry->getTimestamp() >= Transaction::START_TRANSACTION_ID &&
               entry->getTimestamp() != transaction->getID()) ||
           (entry->getTimestamp() < Transaction::START_TRANSACTION_ID &&
               entry->getTimestamp() > transaction->getStartTS());
}

void CatalogSet::dropEntry(Transaction* transaction, const std::string& name) {
    KU_ASSERT(containsEntry(transaction, name));
    auto entry = getEntry(transaction, name);
    auto tombstone = createDummyEntry(name);
    tombstone->setTimestamp(transaction->getID());
    auto tombstonePtr = tombstone.get();
    emplace(std::move(tombstone));
    if (transaction->getStartTS() > 0) {
        KU_ASSERT(transaction->getID() != INVALID_TRANSACTION);
        transaction->addCatalogEntry(this, tombstonePtr->getPrev());
    }
}

void CatalogSet::alterEntry(Transaction* transaction, const binder::BoundAlterInfo& alterInfo) {
    KU_ASSERT(containsEntry(transaction, alterInfo.tableName));
    auto entry = getEntry(transaction, alterInfo.tableName);
    KU_ASSERT(entry->getType() == CatalogEntryType::NODE_TABLE_ENTRY ||
              entry->getType() == CatalogEntryType::REL_TABLE_ENTRY ||
              entry->getType() == CatalogEntryType::REL_GROUP_ENTRY ||
              entry->getType() == CatalogEntryType::RDF_GRAPH_ENTRY);
    auto tableEntry = ku_dynamic_cast<CatalogEntry*, TableCatalogEntry*>(entry);
    auto newEntry = tableEntry->alter(alterInfo);
    newEntry->setTimestamp(transaction->getID());
    if (alterInfo.alterType == AlterType::RENAME_TABLE) {
        // We treat rename table as drop and create.
        dropEntry(transaction, alterInfo.tableName);
        createEntry(transaction, std::move(newEntry));
    } else {
        emplace(std::move(newEntry));
    }
    if (transaction->getStartTS() > 0) {
        KU_ASSERT(transaction->getID() != INVALID_TRANSACTION);
        transaction->addCatalogEntry(this, entry);
    }
}

case_insensitive_map_t<CatalogEntry*> CatalogSet::getEntries(Transaction* transaction) {
    case_insensitive_map_t<CatalogEntry*> result;
    for (auto& [name, entry] : entries) {
        auto currentEntry = traverseVersionChainsForTransaction(transaction, entry.get());
        if (currentEntry->isDeleted()) {
            continue;
        }
        result.emplace(name, currentEntry);
    }
    return result;
}

void CatalogSet::serialize(common::Serializer serializer) const {
    uint64_t numEntries = 0;
    for (auto& [name, entry] : entries) {
        switch (entry->getType()) {
        case CatalogEntryType::SCALAR_FUNCTION_ENTRY:
        case CatalogEntryType::REWRITE_FUNCTION_ENTRY:
        case CatalogEntryType::AGGREGATE_FUNCTION_ENTRY:
        case CatalogEntryType::TABLE_FUNCTION_ENTRY:
            continue;
        default:
            numEntries++;
        }
    }
    serializer.serializeValue(numEntries);
    for (auto& [name, entry] : entries) {
        entry->serialize(serializer);
    }
}

std::unique_ptr<CatalogSet> CatalogSet::deserialize(common::Deserializer& deserializer) {
    std::unique_ptr<CatalogSet> catalogSet = std::make_unique<CatalogSet>();
    uint64_t numEntries;
    deserializer.deserializeValue(numEntries);
    for (uint64_t i = 0; i < numEntries; i++) {
        auto entry = CatalogEntry::deserialize(deserializer);
        if (entry != nullptr) {
            catalogSet->emplace(std::move(entry));
        }
    }
    return catalogSet;
}

} // namespace catalog
} // namespace kuzu
