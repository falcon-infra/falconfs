// mmap-backed DRAM pool for FalconFS KV Cache (v6 §12.0).
//
// Default: a contiguous mmap region (PROT_READ|PROT_WRITE,
// MAP_PRIVATE|MAP_ANONYMOUS, optionally MAP_HUGETLB).
//
// When ``posix_shm_segment_name`` is non-empty (standalone ``falcon_kv_store`` passes
// ``FALCON_KV_STORE_SHM_NAME``), the pool is backed by ``shm_open`` + ``mmap(MAP_SHARED)``
// so same-host Clients can attach the same segment (``KVStoreFacadeRegistry`` colocation path).
// Block-aligned writes and reads index into the region by `pool_offset` relative to the pool
// base address; this matches the Store data plane in the v6 design.
//
// Per-stripe locking (v6.6.1): a fixed array of std::shared_mutex stripes
// indexes by (pool_offset / block_size) % N so disjoint slots rarely contend.
// Writers take exclusive locks; readers take shared locks on the same stripe.
//
// Stripe count: environment variable FALCON_KV_STORE_DRAM_STRIPES (default 64,
// clamped to [1, 4096]).
//
// Per-key metadata (version, crc, compression, original size) lives in
// `KVStoreEngine`.
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>

namespace falconfs::kv {

struct DramPoolWriteResult {
    bool ok = false;
    int32_t bytes_written = 0;
};

class DramPool {
public:
    // Constructs an mmap-backed pool sized to `region_bytes`. If
    // `posix_shm_segment_name` is non-empty, uses POSIX shared memory (``MAP_SHARED``)
    // so colocated Clients can ``shm_open`` the same name from CN catalog; ``try_huge_pages``
    // is ignored in that mode. Otherwise anonymous ``mmap``; if ``try_huge_pages=true``,
    // MAP_HUGETLB is attempted first and the constructor falls back to standard pages.
    DramPool(std::size_t region_bytes,
             std::size_t block_size,
             bool try_huge_pages = false,
             const std::string& posix_shm_segment_name = std::string());
    ~DramPool();

    DramPool(const DramPool&) = delete;
    DramPool& operator=(const DramPool&) = delete;

    std::size_t RegionBytes() const { return region_bytes_; }
    std::size_t BlockSize() const { return block_size_; }
    bool UsedHugePages() const { return used_huge_pages_; }
    std::size_t NumStripes() const { return num_stripes_; }

    // True if `pool_offset` is in-range and aligned to `block_size_`.
    bool ValidOffset(int64_t pool_offset) const;

    // Writes `payload` into the slot starting at `pool_offset`. Returns
    // `ok=false` if the offset is invalid or the payload exceeds `block_size_`.
    DramPoolWriteResult Write(int64_t pool_offset, const std::string& payload);

    // Reads `size` bytes (must be <= `block_size_`) starting at `pool_offset`.
    bool Read(int64_t pool_offset, int32_t size, std::string* out) const;

private:
    void Unmap();
    std::size_t StripeIndex(int64_t pool_offset) const;

    void* base_ = nullptr;
    int shm_fd_ = -1;
    std::size_t region_bytes_ = 0;
    std::size_t block_size_ = 0;
    std::size_t num_stripes_ = 64;
    bool used_huge_pages_ = false;
    // C++17+: std::shared_mutex is neither movable nor copyable; use unique_ptr to array.
    mutable std::unique_ptr<std::shared_mutex[]> stripe_locks_;
};

}  // namespace falconfs::kv
