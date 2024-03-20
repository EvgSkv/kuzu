#pragma once

#include "common/static_vector.h"
#include "common/type_utils.h"
#include "common/types/internal_id_t.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "storage/index/hash_index_header.h"
#include "storage/index/hash_index_slot.h"
#include "storage/index/hash_index_utils.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/storage_structure/overflow_file.h"

namespace kuzu {
namespace storage {

constexpr size_t BUFFER_SIZE = 1024;
template<typename T>
using IndexBuffer = common::StaticVector<std::pair<T, common::offset_t>, BUFFER_SIZE>;
/**
 * Basic index file consists of three disk arrays: indexHeader, primary slots (pSlots), and overflow
 * slots (oSlots).
 *
 * 1. HashIndexHeader contains the current state of the hash tables (level and split information:
 * currentLevel, levelHashMask, higherLevelHashMask, nextSplitSlotId;  key data type).
 *
 * 2. Given a key, it is mapped to one of the pSlots based on its hash value and the level and
 * splitting info. The actual key and value are either stored in the pSlot, or in a chained overflow
 * slots (oSlots) of the pSlot.
 *
 * The slot data structure:
 * Each slot (p/oSlot) consists of a slot header and several entries. The max number of entries in
 * slot is given by HashIndexConstants::SLOT_CAPACITY. The size of the slot is given by
 * (sizeof(SlotHeader) + (SLOT_CAPACITY * sizeof(Entry)).
 *
 * SlotHeader: [numEntries, validityMask, nextOvfSlotId]
 * Entry: [key (fixed sized part), node_offset]
 *
 * 3. oSlots are used to store entries that comes to the designated primary slot that has already
 * been filled to the capacity. Several overflow slots can be chained after the single primary slot
 * as a singly linked link-list. Each slot's SlotHeader has information about the next overflow slot
 * in the chain and also the number of filled entries in that slot.
 *
 *  */

// T is the key type stored in the slots.
// For strings this is different than the type used when inserting/searching
// (see BufferKeyType and Key)
template<typename T>
class HashIndexBuilder final {
    static_assert(getSlotCapacity<T>() <= SlotHeader::FINGERPRINT_CAPACITY);
    // Size of the validity mask
    static_assert(getSlotCapacity<T>() <= sizeof(SlotHeader().validityMask) * 8);
    static_assert(getSlotCapacity<T>() <= std::numeric_limits<entry_pos_t>::max() + 1);

public:
    explicit HashIndexBuilder(OverflowFileHandle* overflowFileHandle);

    static void createEmptyIndexFiles(uint64_t indexPos, FileHandle& fileHandle);

public:
    // Reserves space for at least the specified number of elements.
    // This reserves space for numEntries in total, regardless of existing entries in the builder
    void bulkReserve(uint32_t numEntries);

    using BufferKeyType =
        typename std::conditional<std::same_as<T, common::ku_string_t>, std::string, T>::type;
    // Appends the buffer to the index. Returns the number of values successfully inserted.
    // I.e. if a key fails to insert, its index will be the return value
    size_t append(const IndexBuffer<BufferKeyType>& buffer);
    using Key =
        typename std::conditional<std::same_as<T, common::ku_string_t>, std::string_view, T>::type;
    bool lookup(Key key, common::offset_t& result);

    uint64_t size() { return this->indexHeader->numEntries; }

    void forEach(std::function<void(slot_id_t, uint8_t, SlotEntry<T>)> func);
    std::string toString();

    // Assumes that space has already been allocated for the entry
    bool appendInternal(Key key, common::offset_t value, common::hash_t hash);
    Slot<T>* getSlot(const SlotInfo& slotInfo);

    uint32_t allocatePSlots(uint32_t numSlotsToAllocate);
    uint32_t allocateAOSlot();
    /*
     * When a slot is split, we add a new slot, which ends up with an
     * id equal to the slot to split's ID + (1 << header.currentLevel).
     * Values are then rehashed using a hash index which is one bit wider than before,
     * meaning they either stay in the existing slot, or move into the new one.
     */
    void splitSlot(HashIndexHeader& header);

    inline bool equals(Key keyToLookup, const T& keyInEntry) const {
        return keyToLookup == keyInEntry;
    }

    inline void insert(
        Key key, Slot<T>* slot, uint8_t entryPos, common::offset_t value, uint8_t fingerprint) {
        auto& entry = slot->entries[entryPos];
        entry.key = key;
        entry.value = value;
        slot->header.setEntryValid(entryPos, fingerprint);
        KU_ASSERT(HashIndexUtils::getFingerprintForHash(HashIndexUtils::hash(key)) == fingerprint);
    }
    void copy(const SlotEntry<T>& oldEntry, slot_id_t newSlotId, uint8_t fingerprint);
    void insertToNewOvfSlot(
        Key key, Slot<T>* previousSlot, common::offset_t offset, uint8_t fingerprint);
    common::hash_t hashStored(const T& key) const;

    struct SlotIterator {
        explicit SlotIterator(slot_id_t newSlotId, HashIndexBuilder<T>* builder)
            : slotInfo{newSlotId, SlotType::PRIMARY}, slot(builder->getSlot(slotInfo)) {}
        SlotInfo slotInfo;
        Slot<T>* slot;
    };

    inline bool nextChainedSlot(SlotIterator& iter) {
        if (iter.slot->header.nextOvfSlotId != 0) {
            iter.slotInfo.slotId = iter.slot->header.nextOvfSlotId;
            iter.slotInfo.slotType = SlotType::OVF;
            iter.slot = getSlot(iter.slotInfo);
            return true;
        }
        return false;
    }

    // TODO: this will use almost as much memory as the entire index including already written
    // values and new values Replacing with a vector<vector<tuple<S, offset_t, hash_t>>> will be
    // much more memory efficient (24 bytes per primary slot instead of 256), though I'm not sure if
    // it will be faster Alternatively, what if we don't reserve the same number of slots on disk?
    // We can still benefit from the fact that all entries for a single slot in the destination will
    // be sourced from the same slot in-memory, so for each entry in slot x, find destination slot y
    // and then scan the rest of slot x for other slots that also go to y before continuing. It's
    // worst case quadratic in the number of entries to insert, but in practice shouldn't be very
    // bad since it only writes to each page once and there are a limited number of entries per
    // primary slot (though it's still possible for collisions to result in them all ending up in
    // one, but all having different destination slots, in which case the performance will be bad).
    // TODO: vector is not memory stable, so adding overflow slots will cause the iterator to become
    // invalid But the iterator could just store the slot info; access into vector is fast
    OverflowFileHandle* overflowFileHandle;
    FileHandle dummy;
    InMemDiskArrayBuilder<Slot<T>> pSlots;
    InMemDiskArrayBuilder<Slot<T>> oSlots;
    std::unique_ptr<HashIndexHeader> indexHeader;
};

template<>
bool HashIndexBuilder<common::ku_string_t>::equals(
    std::string_view keyToLookup, const common::ku_string_t& keyInEntry) const;
} // namespace storage
} // namespace kuzu
