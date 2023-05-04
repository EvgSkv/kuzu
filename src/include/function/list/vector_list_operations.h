#pragma once

#include "function/vector_operations.h"

namespace kuzu {
namespace function {

struct VectorListOperations : public VectorOperations {

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryListExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 3);
        TernaryOperationExecutor::executeList<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], *params[2], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryListExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 2);
        BinaryOperationExecutor::executeList<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], result);
    }

    template<typename OPERATION, typename RESULT_TYPE>
    static std::vector<std::unique_ptr<VectorOperationDefinition>>
    getBinaryListOperationDefinitions(std::string funcName, common::DataTypeID resultTypeID) {
        std::vector<std::unique_ptr<VectorOperationDefinition>> result;
        scalar_exec_func execFunc;
        for (auto& rightTypeID : std::vector<common::DataTypeID>{common::BOOL, common::INT64,
                 common::DOUBLE, common::STRING, common::DATE, common::TIMESTAMP, common::INTERVAL,
                 common::VAR_LIST}) {
            switch (rightTypeID) {
            case common::BOOL: {
                execFunc =
                    BinaryListExecFunction<common::list_entry_t, uint8_t, RESULT_TYPE, OPERATION>;
            } break;
            case common::INT64: {
                execFunc =
                    BinaryListExecFunction<common::list_entry_t, int64_t, RESULT_TYPE, OPERATION>;
            } break;
            case common::DOUBLE: {
                execFunc =
                    BinaryListExecFunction<common::list_entry_t, double_t, RESULT_TYPE, OPERATION>;
            } break;
            case common::STRING: {
                execFunc = BinaryListExecFunction<common::list_entry_t, common::ku_string_t,
                    RESULT_TYPE, OPERATION>;
            } break;
            case common::DATE: {
                execFunc = BinaryListExecFunction<common::list_entry_t, common::date_t, RESULT_TYPE,
                    OPERATION>;
            } break;
            case common::TIMESTAMP: {
                execFunc = BinaryListExecFunction<common::list_entry_t, common::timestamp_t,
                    RESULT_TYPE, OPERATION>;
            } break;
            case common::INTERVAL: {
                execFunc = BinaryListExecFunction<common::list_entry_t, common::interval_t,
                    RESULT_TYPE, OPERATION>;
            } break;
            case common::VAR_LIST: {
                execFunc = BinaryListExecFunction<common::list_entry_t, common::list_entry_t,
                    RESULT_TYPE, OPERATION>;
            } break;
            default: {
                assert(false);
            }
            }
            result.push_back(make_unique<VectorOperationDefinition>(funcName,
                std::vector<common::DataTypeID>{common::VAR_LIST, rightTypeID}, resultTypeID,
                execFunc, nullptr, false /* isVarlength*/));
        }
        return result;
    }
};

struct ListCreationVectorOperation : public VectorListOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result);
};

struct ListLenVectorOperation : public VectorListOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct ListExtractVectorOperation : public VectorListOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListConcatVectorOperation : public VectorListOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListAppendVectorOperation : public VectorListOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListPrependVectorOperation : public VectorListOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

struct ListPositionVectorOperation : public VectorListOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct ListContainsVectorOperation : public VectorListOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
};

struct ListSliceVectorOperation : public VectorListOperations {
    static std::vector<std::unique_ptr<VectorOperationDefinition>> getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

} // namespace function
} // namespace kuzu
