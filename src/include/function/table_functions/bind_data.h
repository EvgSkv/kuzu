#pragma once

#include "common/copier_config/copier_config.h"
#include "common/types/types.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace function {

struct TableFuncBindData {
    std::vector<std::unique_ptr<common::LogicalType>> columnTypes;
    std::vector<std::string> columnNames;

    TableFuncBindData(std::vector<std::unique_ptr<common::LogicalType>> columnTypes,
        std::vector<std::string> columnNames)
        : columnTypes{std::move(columnTypes)}, columnNames{std::move(columnNames)} {}

    virtual ~TableFuncBindData() = default;

    virtual std::unique_ptr<TableFuncBindData> copy() = 0;
};

struct ScanBindData : public TableFuncBindData {
    storage::MemoryManager* mm;
    common::ReaderConfig config;

    ScanBindData(std::vector<std::unique_ptr<common::LogicalType>> columnTypes,
        std::vector<std::string> columnNames, storage::MemoryManager* mm,
        const common::ReaderConfig& config)
        : TableFuncBindData{std::move(columnTypes), std::move(columnNames)}, mm{mm}, config{
                                                                                         config} {}

    std::unique_ptr<TableFuncBindData> copy() override {
        return std::make_unique<ScanBindData>(
            common::LogicalType::copy(columnTypes), columnNames, mm, config);
    }
};

enum class ExternalDependenciesType : uint8_t { PYTHON_DEPENDENCY };

class ExternalDependency {
public:
    explicit ExternalDependency(ExternalDependenciesType type) : type(std::move(type)){};
    virtual ~ExternalDependency(){};

    ExternalDependenciesType type;
};

struct PyTableFunctionData : public TableFuncBindData {
    PyTableFunctionData(std::vector<std::unique_ptr<common::LogicalType>> columnTypes,
        std::vector<std::string> columnNames)
        : TableFuncBindData{std::move(columnTypes), std::move(columnNames)} {}

    //! External dependencies of this table function
    std::unique_ptr<ExternalDependency> externalDependency;
};

} // namespace function
} // namespace kuzu
