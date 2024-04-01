#pragma once

#include "function_catalog_entry.h"

namespace kuzu {
namespace catalog {

class TableFunctionCatalogEntry : public FunctionCatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    TableFunctionCatalogEntry() = default;
    TableFunctionCatalogEntry(std::string name, function::function_set functionSet);
};

} // namespace catalog
} // namespace kuzu
