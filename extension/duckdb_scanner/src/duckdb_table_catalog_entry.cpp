#include "duckdb_table_catalog_entry.h"

namespace kuzu {
namespace catalog {

DuckDBTableCatalogEntry::DuckDBTableCatalogEntry(
    std::string name, common::table_id_t tableID, function::TableFunction scanFunction)
    : TableCatalogEntry{CatalogEntryType::FOREIGN_TABLE_ENTRY, std::move(name), tableID},
      scanFunction{std::move(scanFunction)} {}

common::TableType DuckDBTableCatalogEntry::getTableType() const {
    return common::TableType::FOREIGN;
}

std::unique_ptr<TableCatalogEntry> DuckDBTableCatalogEntry::copy() const {
    auto other = std::make_unique<DuckDBTableCatalogEntry>(name, tableID, scanFunction);
    TableCatalogEntry::copy(*other);
    return other;
}

} // namespace catalog
} // namespace kuzu
