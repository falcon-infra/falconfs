#include "vllm_kv_cache/src/metadata/bitmap_allocator.h"

namespace falconfs::kv {

namespace {
constexpr int64_t kBitsPerWord = 64;

inline int64_t WordIndex(int64_t bit) { return bit / kBitsPerWord; }
inline int64_t BitOffset(int64_t bit) { return bit % kBitsPerWord; }

inline bool TestBit(const std::vector<uint64_t>& words, int64_t bit) {
    return (words[WordIndex(bit)] >> BitOffset(bit)) & 1ULL;
}

inline void SetBit(std::vector<uint64_t>& words, int64_t bit) {
    words[WordIndex(bit)] |= (1ULL << BitOffset(bit));
}

inline void ClearBit(std::vector<uint64_t>& words, int64_t bit) {
    words[WordIndex(bit)] &= ~(1ULL << BitOffset(bit));
}
}  // namespace

BitmapAllocator::BitmapAllocator(int32_t store_node_id,
                                 int32_t owner_dn_id,
                                 int64_t base_offset,
                                 int64_t region_bytes,
                                 int64_t block_size,
                                 int64_t store_epoch)
    : store_node_id_(store_node_id),
      owner_dn_id_(owner_dn_id),
      base_offset_(base_offset),
      region_bytes_(region_bytes),
      block_size_(block_size),
      store_epoch_(store_epoch),
      total_blocks_(block_size > 0 ? region_bytes / block_size : 0),
      words_((total_blocks_ + kBitsPerWord - 1) / kBitsPerWord, 0ULL),
      free_blocks_(total_blocks_),
      next_hint_(0) {}

int64_t BitmapAllocator::TotalBlocks() const { return total_blocks_; }

int64_t BitmapAllocator::FreeBlocks() const {
    std::lock_guard<std::mutex> lock(mu_);
    return free_blocks_;
}

bool BitmapAllocator::Contains(int64_t pool_offset) const {
    return pool_offset >= base_offset_ &&
           pool_offset < base_offset_ + region_bytes_;
}

BitmapAllocateResult BitmapAllocator::Allocate() {
    BitmapAllocateResult out;
    if (total_blocks_ <= 0) {
        out.result = ItemResult::Err(ErrorCode::INVALID_ARGUMENT, false, "empty region");
        return out;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (free_blocks_ <= 0) {
        out.result = ItemResult::Err(ErrorCode::THROTTLED, true, "region exhausted");
        return out;
    }
    for (int64_t step = 0; step < total_blocks_; ++step) {
        int64_t idx = (next_hint_ + step) % total_blocks_;
        if (!TestBit(words_, idx)) {
            SetBit(words_, idx);
            next_hint_ = (idx + 1) % total_blocks_;
            free_blocks_ -= 1;
            out.result = ItemResult::Ok();
            out.pool_offset = base_offset_ + idx * block_size_;
            return out;
        }
    }
    // Unreachable when free_blocks_ > 0 but kept defensively.
    out.result = ItemResult::Err(ErrorCode::THROTTLED, true, "region exhausted");
    return out;
}

ItemResult BitmapAllocator::Free(int64_t pool_offset) {
    if (!Contains(pool_offset)) {
        return ItemResult::Err(ErrorCode::INVALID_ARGUMENT, false, "offset outside region");
    }
    int64_t relative = pool_offset - base_offset_;
    if (block_size_ <= 0 || relative % block_size_ != 0) {
        return ItemResult::Err(ErrorCode::INVALID_ARGUMENT, false, "unaligned offset");
    }
    int64_t idx = relative / block_size_;
    if (idx < 0 || idx >= total_blocks_) {
        return ItemResult::Err(ErrorCode::INVALID_ARGUMENT, false, "offset outside region");
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (!TestBit(words_, idx)) {
        return ItemResult::Err(ErrorCode::INTERNAL_ERROR, false, "double free");
    }
    ClearBit(words_, idx);
    if (idx < next_hint_) {
        next_hint_ = idx;
    }
    free_blocks_ += 1;
    return ItemResult::Ok();
}

ItemResult BitmapAllocator::MarkOccupied(int64_t pool_offset) {
    if (!Contains(pool_offset)) {
        return ItemResult::Err(ErrorCode::INVALID_ARGUMENT, false, "offset outside region");
    }
    int64_t relative = pool_offset - base_offset_;
    if (block_size_ <= 0 || relative % block_size_ != 0) {
        return ItemResult::Err(ErrorCode::INVALID_ARGUMENT, false, "unaligned offset");
    }
    int64_t idx = relative / block_size_;
    if (idx < 0 || idx >= total_blocks_) {
        return ItemResult::Err(ErrorCode::INVALID_ARGUMENT, false, "offset outside region");
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (TestBit(words_, idx)) {
        return ItemResult::Err(ErrorCode::INTERNAL_ERROR, false, "duplicate occupied");
    }
    SetBit(words_, idx);
    free_blocks_ -= 1;
    return ItemResult::Ok();
}

}  // namespace falconfs::kv
