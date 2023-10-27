#include "common/data_chunk/data_chunk.h"

#include <cassert>
#include <cstdint>
#include <memory>
#include <utility>

#include "common/vector/value_vector.h"

namespace kuzu {
namespace common {

void DataChunk::insert(uint32_t pos, std::shared_ptr<ValueVector> valueVector) {
    valueVector->setState(state);
    assert(valueVectors.size() > pos);
    valueVectors[pos] = std::move(valueVector);
}

void DataChunk::resetAuxiliaryBuffer() {
    for (auto& valueVector : valueVectors) {
        valueVector->resetAuxiliaryBuffer();
    }
}

} // namespace common
} // namespace kuzu
