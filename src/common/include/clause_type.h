#pragma once

#include <cstdint>

namespace graphflow {
namespace common {

enum class ClauseType : uint8_t {
    // updating clause
    SET = 0,
    DELETE = 1,
};

} // namespace common
} // namespace graphflow
