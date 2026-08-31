// Continuous-region bitmap allocator for FalconFS KV Cache.
//
// Each Store region owned by a DN is represented as a fixed-size bitmap of
// blocks. The allocator hands out absolute Store DRAM offsets and tracks
// occupancy to support recovery (rebuild-from-metadata-scan).
//
// In the production code path this allocator will live in PostgreSQL shared
// memory protected by an LWLock, mirroring `ShardTableShmemInit`. For unit
// testing and reference parity, the standalone implementation here uses a
// std::mutex but exposes the same API surface.
#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <vector>

#include "vllm_kv_cache/src/common/kv_types.h"

namespace falconfs::kv {

struct BitmapAllocateResult {
    ItemResult result;
    int64_t pool_offset = 0;
};

class BitmapAllocator {
public:
    BitmapAllocator(int32_t store_node_id,
                    int32_t owner_dn_id,
                    int64_t base_offset,
                    int64_t region_bytes,
                    int64_t block_size,
                    int64_t store_epoch);

    int32_t StoreNodeId() const { return store_node_id_; }
    int32_t OwnerDnId() const { return owner_dn_id_; }
    int64_t BaseOffset() const { return base_offset_; }
    int64_t RegionBytes() const { return region_bytes_; }
    int64_t BlockSize() const { return block_size_; }
    int64_t StoreEpoch() const { return store_epoch_; }

    int64_t TotalBlocks() const;
    int64_t FreeBlocks() const;

    // Returns an absolute pool offset on success, THROTTLED on exhaustion.
    BitmapAllocateResult Allocate();

    // Frees a previously allocated offset. Returns INVALID_ARGUMENT if the
    // offset is outside the region or unaligned, INTERNAL_ERROR on double-free.
    ItemResult Free(int64_t pool_offset);

    // Recovery: forcibly mark a slot occupied. Used when scanning persisted
    // metadata rows that already reference offsets in this region. Returns
    // INTERNAL_ERROR on double-mark (corruption signal).
    ItemResult MarkOccupied(int64_t pool_offset);

    bool Contains(int64_t pool_offset) const;

private:
    int32_t store_node_id_;
    int32_t owner_dn_id_;
    int64_t base_offset_;
    int64_t region_bytes_;
    int64_t block_size_;
    int64_t store_epoch_;
    int64_t total_blocks_;
    mutable std::mutex mu_;
    std::vector<uint64_t> words_;  // bitset, 1 = occupied
    int64_t free_blocks_;
    int64_t next_hint_;
};

}  // namespace falconfs::kv
