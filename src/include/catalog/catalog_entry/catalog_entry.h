#pragma once

#include <string>

#include "catalog_entry_type.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "main/client_context.h"

namespace kuzu {
namespace catalog {

class KUZU_API CatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructor & destructor
    //===--------------------------------------------------------------------===//
    CatalogEntry() = default;
    CatalogEntry(CatalogEntryType type, std::string name)
        : type{type}, name{std::move(name)}, timestamp{common::INVALID_TRANSACTION} {}
    virtual ~CatalogEntry() = default;

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    CatalogEntryType getType() const { return type; }
    void rename(std::string name_) { this->name = std::move(name_); }
    std::string getName() const { return name; }
    common::transaction_t getTimestamp() const { return timestamp; }
    void setTimestamp(common::transaction_t timestamp_) { this->timestamp = timestamp_; }
    bool isDeleted() const { return deleted; }
    void setDeleted(bool deleted_) { this->deleted = deleted_; }
    CatalogEntry* getPrev() const {
        KU_ASSERT(prev);
        return prev.get();
    }
    std::unique_ptr<CatalogEntry> movePrev() {
        if (this->prev) {
            this->prev->setNext(nullptr);
        }
        return std::move(prev);
    }
    void setPrev(std::unique_ptr<CatalogEntry> prev_) {
        this->prev = std::move(prev_);
        if (this->prev) {
            this->prev->setNext(this);
        }
    }
    CatalogEntry* getNext() const { return next; }
    void setNext(CatalogEntry* next_) { this->next = next_; }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    virtual void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<CatalogEntry> deserialize(common::Deserializer& deserializer);
    virtual std::string toCypher(main::ClientContext* /*clientContext*/) const { KU_UNREACHABLE; }

protected:
    virtual void copy(CatalogEntry& other) const;

protected:
    CatalogEntryType type;
    std::string name;
    common::transaction_t timestamp;
    bool deleted = false;
    // Older versions.
    std::unique_ptr<CatalogEntry> prev;
    // Newer versions.
    CatalogEntry* next = nullptr;
};

} // namespace catalog
} // namespace kuzu
