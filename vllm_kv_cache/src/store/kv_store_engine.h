#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace falconfs::kv {

class DramPool;
class SSDSpillManager;
class StoreRegionRegistry;

struct StoreResultMeta {
    bool success = false;
    int32_t error_code = 0;
    bool retryable = false;
    std::string error_message;
};

struct StoreWriteResult {
    StoreResultMeta result;
    int32_t bytes_written = 0;
    uint32_t crc32 = 0;
};

struct StoreReadResult {
    StoreResultMeta result;
    std::string payload;
    uint32_t crc32 = 0;
    int32_t compression = 0;
    int32_t original_size = 0;
};

class KVStoreEngine {
public:
    struct HeartbeatTarget {
        int32_t owner_dn_id = 0;
        int64_t store_epoch = 0;
    };
    using HeartbeatSender = std::function<bool(int32_t owner_dn_id,
                                               int32_t store_node_id,
                                               int64_t store_epoch,
                                               int64_t now_ms)>;

    KVStoreEngine(int32_t store_node_id = 1,
                  int64_t base_offset = 0,
                  int64_t region_bytes = 64LL * 65536LL,
                  int32_t block_size = 65536,
                  int64_t store_epoch = 1,
                  std::string posix_shm_segment_name = {});
    ~KVStoreEngine();

    KVStoreEngine(const KVStoreEngine&) = delete;
    KVStoreEngine& operator=(const KVStoreEngine&) = delete;
    KVStoreEngine(KVStoreEngine&&) noexcept;
    KVStoreEngine& operator=(KVStoreEngine&&) noexcept;

    // Optional integrations. Setting either is safe at any time.
    void SetSSDSpillManager(std::shared_ptr<SSDSpillManager> spill_manager);
    void SetRegionRegistry(std::shared_ptr<StoreRegionRegistry> registry, int32_t owner_dn_id);
    void SetHeartbeatSender(HeartbeatSender sender);

    // True when SetSSDSpillManager has been called with a non-null manager.
    // Used by the in-plugin eviction worker to skip cycles that would
    // otherwise always fail (and roll back via catalog CAS, churning
    // versions for no good reason).
    bool HasSpillManager() const;

    int32_t StoreNodeId() const;
    int32_t BlockSize() const;

    StoreWriteResult Write(const std::string& block_hash,
                           int64_t pool_offset,
                           const std::string& payload,
                           int32_t compression,
                           int32_t original_size,
                           int32_t block_size,
                           int64_t expected_version,
                           int64_t expected_store_epoch,
                           bool verify_checksum,
                           uint32_t checksum_hint);

    StoreReadResult Read(const std::string& block_hash,
                         int64_t pool_offset,
                         int32_t block_size,
                         int64_t expected_version,
                         int64_t expected_store_epoch) const;

    StoreReadResult ReadFromSSD(const std::string& block_hash,
                                const std::string& evicted_path,
                                int64_t expected_version) const;

    StoreResultMeta ValidateEvictedPath(const std::string& block_hash,
                                        const std::string& evicted_path) const;

    // Spills DRAM bytes at `pool_offset` to SSD using the configured
    // SSDSpillManager. `dram_read_size` is the number of bytes to read from the
    // slot (must be <= BlockSize()); 0 means read the full configured block size.
    StoreWriteResult SpillBlockToSSD(const std::string& block_hash,
                                     int64_t pool_offset,
                                     int64_t expected_version,
                                     int64_t expected_store_epoch,
                                     int32_t dram_read_size,
                                     std::string* out_evicted_path);

    // Sends heartbeats for all known owner-DN regions through the configured
    // sender. Successful sends refresh StoreRegionRegistry heartbeat timestamps.
    // Returns number of successful sends.
    int SendHeartbeats(int64_t now_ms);

    // Exposes currently tracked heartbeat targets for tests/diagnostics.
    std::vector<HeartbeatTarget> HeartbeatTargets() const;

    // In-memory SSD test hook used when no SSDSpillManager is configured.
    void PutSSDForTest(const std::string& block_hash,
                       const std::string& evicted_path,
                       const std::string& payload,
                       int32_t compression,
                       int32_t original_size,
                       int64_t version);

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace falconfs::kv
