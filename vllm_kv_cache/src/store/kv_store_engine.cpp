#include "vllm_kv_cache/src/store/kv_store_engine.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "kv_common.pb.h"
#include "vllm_kv_cache/src/store/dram_pool.h"
#include "vllm_kv_cache/src/store/ssd_spill_manager.h"
#include "vllm_kv_cache/src/store/store_region_registry.h"

namespace falconfs::kv {

namespace {

bool BlockHashForcedToFailSpill(const std::string& block_hash) {
    const char* env = std::getenv("FALCON_KV_FORCED_SPILL_FAIL_HASHES");
    if (env == nullptr || env[0] == '\0') {
        return false;
    }
    std::string hay(env);
    std::size_t pos = 0;
    while (pos < hay.size()) {
        std::size_t comma = hay.find(',', pos);
        std::string tok = (comma == std::string::npos) ? hay.substr(pos) : hay.substr(pos, comma - pos);
        while (!tok.empty() && (tok.front() == ' ' || tok.front() == '\t')) {
            tok.erase(0, 1);
        }
        while (!tok.empty() && (tok.back() == ' ' || tok.back() == '\t')) {
            tok.pop_back();
        }
        if (tok == block_hash) {
            return true;
        }
        if (comma == std::string::npos) {
            break;
        }
        pos = comma + 1;
    }
    return false;
}

StoreResultMeta MakeResult(bool success, int32_t error_code, bool retryable, const char* message) {
    StoreResultMeta out;
    out.success = success;
    out.error_code = error_code;
    out.retryable = retryable;
    out.error_message = message;
    return out;
}

uint32_t ComputeChecksum(const std::string& payload) {
    // Lightweight deterministic checksum for native scaffolding. The production
    // path will use crc32 once we wire zlib; this matches the reference Python
    // FNV-1a-style hash so unit-test parity is preserved.
    uint32_t h = 2166136261u;
    for (unsigned char c : payload) {
        h ^= c;
        h *= 16777619u;
    }
    return h;
}

bool EnvFlagEnabled(const char* key, bool default_value) {
    const char* v = std::getenv(key);
    if (v == nullptr || v[0] == '\0') return default_value;
    if (std::strcmp(v, "1") == 0 || std::strcmp(v, "true") == 0 ||
        std::strcmp(v, "TRUE") == 0 || std::strcmp(v, "yes") == 0 ||
        std::strcmp(v, "YES") == 0 || std::strcmp(v, "on") == 0 ||
        std::strcmp(v, "ON") == 0) {
        return true;
    }
    if (std::strcmp(v, "0") == 0 || std::strcmp(v, "false") == 0 ||
        std::strcmp(v, "FALSE") == 0 || std::strcmp(v, "no") == 0 ||
        std::strcmp(v, "NO") == 0 || std::strcmp(v, "off") == 0 ||
        std::strcmp(v, "OFF") == 0) {
        return false;
    }
    return default_value;
}

bool HotPathChecksumsEnabled() {
    static const bool enabled = EnvFlagEnabled("FALCON_KV_STORE_COMPUTE_CHECKSUMS", false);
    return enabled;
}

}  // namespace

class KVStoreEngine::Impl {
public:
    Impl(int32_t store_node_id,
         int64_t base_offset,
         int64_t region_bytes,
         int32_t block_size,
         int64_t store_epoch,
         std::string posix_shm_segment_name)
        : store_node_id_(store_node_id),
          base_offset_(base_offset),
          region_bytes_(region_bytes),
          block_size_(block_size),
          store_epoch_(store_epoch),
          dram_pool_(std::make_unique<DramPool>(static_cast<std::size_t>(region_bytes),
                                                static_cast<std::size_t>(block_size),
                                                /*try_huge_pages=*/false,
                                                posix_shm_segment_name)) {}

    void SetSSDSpillManager(std::shared_ptr<SSDSpillManager> spill_manager) {
        std::shared_lock<std::shared_mutex> lock(engine_mu_);
        spill_manager_ = std::move(spill_manager);
    }

    bool HasSpillManager() const {
        std::shared_lock<std::shared_mutex> lock(engine_mu_);
        return spill_manager_ != nullptr;
    }

    void SetRegionRegistry(std::shared_ptr<StoreRegionRegistry> registry, int32_t owner_dn_id) {
        std::unique_lock<std::shared_mutex> lock(engine_mu_);
        registry_ = std::move(registry);
        owner_dn_id_ = owner_dn_id;
    }

    void SetHeartbeatSender(HeartbeatSender sender) {
        std::unique_lock<std::shared_mutex> lock(engine_mu_);
        heartbeat_sender_ = std::move(sender);
    }

    int32_t StoreNodeId() const { return store_node_id_; }
    int32_t BlockSize() const { return block_size_; }

    StoreWriteResult Write(const std::string& block_hash,
                           int64_t pool_offset,
                           const std::string& payload,
                           int32_t compression,
                           int32_t original_size,
                           int32_t block_size,
                           int64_t expected_version,
                           int64_t expected_store_epoch,
                           bool verify_checksum,
                           uint32_t checksum_hint) {
        (void)block_hash;
        (void)expected_version;
        (void)compression;
        (void)original_size;
        std::shared_lock<std::shared_mutex> lock(engine_mu_);
        StoreWriteResult out;
        if (expected_store_epoch != store_epoch_) {
            out.result = MakeResult(false, ErrorCode::STALE_EPOCH, true, "stale store epoch");
            return out;
        }
        if (registry_ != nullptr && !registry_->CanAllocate(store_node_id_, owner_dn_id_)) {
            out.result = MakeResult(false, ErrorCode::STORE_WRITE_FAILED, true, "region not healthy");
            return out;
        }
        if (!ValidOffset(pool_offset) || block_size <= 0 || block_size > block_size_) {
            out.result = MakeResult(false, ErrorCode::INVALID_ARGUMENT, false, "invalid offset or block_size");
            return out;
        }
        if (static_cast<int32_t>(payload.size()) > block_size) {
            out.result = MakeResult(false, ErrorCode::INVALID_ARGUMENT, false, "payload too large");
            return out;
        }
        uint32_t crc = 0;
        if (verify_checksum || HotPathChecksumsEnabled()) {
            crc = ComputeChecksum(payload);
        }
        if (verify_checksum && checksum_hint != crc) {
            out.result = MakeResult(false, ErrorCode::CHECKSUM_MISMATCH, false, "checksum mismatch");
            return out;
        }

        const int64_t pool_relative = pool_offset - base_offset_;
        DramPoolWriteResult dpr = dram_pool_->Write(pool_relative, payload);
        if (!dpr.ok) {
            out.result = MakeResult(false, ErrorCode::INVALID_ARGUMENT, false, "dram write failed");
            return out;
        }

        out.result = MakeResult(true, ErrorCode::OK, false, "");
        out.bytes_written = dpr.bytes_written;
        out.crc32 = crc;
        return out;
    }

    StoreReadResult Read(const std::string& block_hash,
                         int64_t pool_offset,
                         int32_t block_size,
                         int64_t expected_version,
                         int64_t expected_store_epoch) const {
        (void)block_hash;
        (void)expected_version;
        std::shared_lock<std::shared_mutex> lock(engine_mu_);
        StoreReadResult out;
        if (expected_store_epoch != store_epoch_) {
            out.result = MakeResult(false, ErrorCode::STALE_EPOCH, true, "stale store epoch");
            return out;
        }
        if (registry_ != nullptr && !registry_->CanRead(store_node_id_, owner_dn_id_)) {
            out.result = MakeResult(false, ErrorCode::STORE_WRITE_FAILED, true, "region not readable");
            return out;
        }
        if (!ValidOffset(pool_offset) || block_size <= 0 || block_size > block_size_) {
            out.result = MakeResult(false, ErrorCode::INVALID_ARGUMENT, false, "invalid offset or block_size");
            return out;
        }

        std::string payload;
        const int64_t pool_relative = pool_offset - base_offset_;
        if (!dram_pool_->Read(pool_relative, block_size, &payload)) {
            out.result = MakeResult(false, ErrorCode::INTERNAL_ERROR, false, "dram read failed");
            return out;
        }
        out.result = MakeResult(true, ErrorCode::OK, false, "");
        out.payload = std::move(payload);
        out.crc32 = HotPathChecksumsEnabled() ? ComputeChecksum(out.payload) : 0;
        out.compression = 0;
        out.original_size = block_size;
        return out;
    }

    StoreReadResult ReadFromSSD(const std::string& block_hash,
                                const std::string& evicted_path,
                                int64_t expected_version) const {
        std::shared_lock<std::shared_mutex> lock(engine_mu_);
        StoreReadResult out;
        if (spill_manager_ != nullptr) {
            if (!spill_manager_->ValidatePath(evicted_path)) {
                out.result = MakeResult(false, ErrorCode::INVALID_ARGUMENT, false, "invalid evicted_path");
                return out;
            }
            auto it = ssd_meta_.find(block_hash);
            if (it == ssd_meta_.end() || it->second.path != evicted_path) {
                out.result = MakeResult(false, ErrorCode::NOT_FOUND, false, "ssd payload missing");
                return out;
            }
            if (expected_version > 0 && expected_version != it->second.version) {
                out.result = MakeResult(false, ErrorCode::CAS_CONFLICT, true, "version mismatch");
                return out;
            }
            SSDSpillReadResult sr = spill_manager_->Read(evicted_path);
            if (!sr.ok) {
                out.result = MakeResult(false, ErrorCode::NOT_FOUND, false, sr.error_message.c_str());
                return out;
            }
            out.result = MakeResult(true, ErrorCode::OK, false, "");
            out.payload = std::move(sr.payload);
            out.crc32 = it->second.crc32;
            out.compression = it->second.compression;
            out.original_size = it->second.original_size;
            return out;
        }

        // In-memory test fallback when no real spill manager is configured.
        if (!ValidEvictedPathInMemory(evicted_path)) {
            out.result = MakeResult(false, ErrorCode::INVALID_ARGUMENT, false, "invalid evicted_path");
            return out;
        }
        auto it = ssd_data_.find(block_hash);
        if (it == ssd_data_.end() || it->second.path != evicted_path) {
            out.result = MakeResult(false, ErrorCode::NOT_FOUND, false, "ssd payload missing");
            return out;
        }
        if (expected_version > 0 && expected_version != it->second.version) {
            out.result = MakeResult(false, ErrorCode::CAS_CONFLICT, true, "version mismatch");
            return out;
        }
        out.result = MakeResult(true, ErrorCode::OK, false, "");
        out.payload = it->second.payload;
        out.crc32 = it->second.crc32;
        out.compression = it->second.compression;
        out.original_size = it->second.original_size;
        return out;
    }

    StoreResultMeta ValidateEvictedPath(const std::string& block_hash,
                                        const std::string& evicted_path) const {
        std::shared_lock<std::shared_mutex> lock(engine_mu_);
        if (spill_manager_ != nullptr) {
            std::string err;
            if (!spill_manager_->ValidateExistingFile(evicted_path, &err)) {
                return MakeResult(false, ErrorCode::NOT_FOUND, false, err.c_str());
            }
            auto it = ssd_meta_.find(block_hash);
            if (it != ssd_meta_.end() && it->second.path != evicted_path) {
                return MakeResult(false, ErrorCode::CAS_CONFLICT, true, "ssd metadata path mismatch");
            }
            return MakeResult(true, ErrorCode::OK, false, "");
        }
        if (!ValidEvictedPathInMemory(evicted_path)) {
            return MakeResult(false, ErrorCode::INVALID_ARGUMENT, false, "invalid evicted_path");
        }
        auto it = ssd_data_.find(block_hash);
        if (it == ssd_data_.end() || it->second.path != evicted_path) {
            return MakeResult(false, ErrorCode::NOT_FOUND, false, "ssd payload missing");
        }
        return MakeResult(true, ErrorCode::OK, false, "");
    }

    StoreWriteResult SpillBlockToSSD(const std::string& block_hash,
                                     int64_t pool_offset,
                                     int64_t expected_version,
                                     int64_t expected_store_epoch,
                                     int32_t dram_read_size,
                                     std::string* out_evicted_path) {
        std::unique_lock<std::shared_mutex> lock(engine_mu_);
        StoreWriteResult out;
        if (BlockHashForcedToFailSpill(block_hash)) {
            out.result = MakeResult(false, ErrorCode::STORE_WRITE_FAILED, true, "forced spill failure (FALCON_KV_FORCED_SPILL_FAIL_HASHES)");
            return out;
        }
        if (expected_store_epoch != store_epoch_) {
            out.result = MakeResult(false, ErrorCode::STALE_EPOCH, true, "stale store epoch");
            return out;
        }
        if (spill_manager_ == nullptr) {
            out.result = MakeResult(false, ErrorCode::INTERNAL_ERROR, false, "spill manager not configured");
            return out;
        }
        if (!ValidOffset(pool_offset)) {
            out.result = MakeResult(false, ErrorCode::INVALID_ARGUMENT, false, "invalid pool_offset for spill");
            return out;
        }
        int32_t read_len = dram_read_size > 0 ? dram_read_size : block_size_;
        if (read_len > block_size_) {
            out.result = MakeResult(false, ErrorCode::INVALID_ARGUMENT, false, "dram_read_size exceeds block_size");
            return out;
        }
        std::string payload;
        const int64_t pool_relative = pool_offset - base_offset_;
        if (!dram_pool_->Read(pool_relative, read_len, &payload)) {
            out.result = MakeResult(false, ErrorCode::INTERNAL_ERROR, false, "dram read failed");
            return out;
        }
        const uint32_t crc = ComputeChecksum(payload);
        SSDSpillResult sr = spill_manager_->Spill(store_node_id_, block_hash, expected_version, payload);
        if (!sr.ok) {
            out.result = MakeResult(false, ErrorCode::STORE_WRITE_FAILED, true, sr.error_message.c_str());
            return out;
        }
        SSDMetadata m;
        m.path = sr.evicted_path;
        m.crc32 = crc;
        m.compression = 0;
        m.original_size = read_len;
        m.version = expected_version;
        ssd_meta_[block_hash] = m;
        if (out_evicted_path) *out_evicted_path = sr.evicted_path;
        out.result = MakeResult(true, ErrorCode::OK, false, "");
        out.bytes_written = static_cast<int32_t>(payload.size());
        out.crc32 = crc;
        return out;
    }

    int SendHeartbeats(int64_t now_ms) {
        std::unique_lock<std::shared_mutex> lock(engine_mu_);
        if (heartbeat_sender_ == nullptr || registry_ == nullptr) {
            return 0;
        }
        const auto regions = registry_->List();
        int success = 0;
        for (const auto& region : regions) {
            if (region.store_node_id != store_node_id_) {
                continue;
            }
            if (region.owner_dn_id <= 0) {
                continue;
            }
            const bool ok = heartbeat_sender_(region.owner_dn_id,
                                              store_node_id_,
                                              store_epoch_,
                                              now_ms);
            if (!ok) {
                continue;
            }
            registry_->Heartbeat(store_node_id_, region.owner_dn_id, store_epoch_, now_ms);
            ++success;
        }
        return success;
    }

    std::vector<HeartbeatTarget> HeartbeatTargets() const {
        std::shared_lock<std::shared_mutex> lock(engine_mu_);
        std::vector<HeartbeatTarget> out;
        if (registry_ == nullptr) {
            return out;
        }
        const auto regions = registry_->List();
        out.reserve(regions.size());
        for (const auto& region : regions) {
            if (region.store_node_id != store_node_id_) {
                continue;
            }
            HeartbeatTarget t;
            t.owner_dn_id = region.owner_dn_id;
            t.store_epoch = store_epoch_;
            out.push_back(t);
        }
        std::sort(out.begin(), out.end(),
                  [](const HeartbeatTarget& a, const HeartbeatTarget& b) {
                      return a.owner_dn_id < b.owner_dn_id;
                  });
        out.erase(std::unique(out.begin(), out.end(),
                              [](const HeartbeatTarget& a, const HeartbeatTarget& b) {
                                  return a.owner_dn_id == b.owner_dn_id;
                              }),
                  out.end());
        return out;
    }

    void PutSSDForTest(const std::string& block_hash,
                       const std::string& evicted_path,
                       const std::string& payload,
                       int32_t compression,
                       int32_t original_size,
                       int64_t version) {
        std::unique_lock<std::shared_mutex> lock(engine_mu_);
        SSDEntry e;
        e.path = evicted_path;
        e.payload = payload;
        e.crc32 = ComputeChecksum(payload);
        e.compression = compression;
        e.original_size = original_size;
        e.version = version;
        ssd_data_[block_hash] = std::move(e);
    }

private:
    bool ValidOffset(int64_t pool_offset) const {
        if (pool_offset < base_offset_ || pool_offset >= base_offset_ + region_bytes_) {
            return false;
        }
        return ((pool_offset - base_offset_) % block_size_) == 0;
    }

    static bool ValidEvictedPathInMemory(const std::string& evicted_path) {
        if (evicted_path.empty() || evicted_path[0] != '/') {
            return false;
        }
        return evicted_path.find("..") == std::string::npos;
    }

    struct SSDEntry {
        std::string path;
        std::string payload;
        uint32_t crc32 = 0;
        int32_t compression = 0;
        int32_t original_size = 0;
        int64_t version = 0;
    };

    struct SSDMetadata {
        std::string path;
        uint32_t crc32 = 0;
        int32_t compression = 0;
        int32_t original_size = 0;
        int64_t version = 0;
    };

    int32_t store_node_id_;
    int64_t base_offset_;
    int64_t region_bytes_;
    int32_t block_size_;
    int64_t store_epoch_;
    int32_t owner_dn_id_ = 0;

    mutable std::shared_mutex engine_mu_;
    std::unique_ptr<DramPool> dram_pool_;
    std::shared_ptr<SSDSpillManager> spill_manager_;
    std::shared_ptr<StoreRegionRegistry> registry_;
    HeartbeatSender heartbeat_sender_;
    std::unordered_map<std::string, SSDEntry> ssd_data_;       // legacy in-memory SSD test hook
    std::unordered_map<std::string, SSDMetadata> ssd_meta_;    // metadata for real SSDSpillManager-backed entries
};

KVStoreEngine::KVStoreEngine(int32_t store_node_id,
                             int64_t base_offset,
                             int64_t region_bytes,
                             int32_t block_size,
                             int64_t store_epoch,
                             std::string posix_shm_segment_name)
    : impl_(std::make_unique<Impl>(store_node_id,
                                    base_offset,
                                    region_bytes,
                                    block_size,
                                    store_epoch,
                                    std::move(posix_shm_segment_name))) {}

KVStoreEngine::~KVStoreEngine() = default;
KVStoreEngine::KVStoreEngine(KVStoreEngine&&) noexcept = default;
KVStoreEngine& KVStoreEngine::operator=(KVStoreEngine&&) noexcept = default;

void KVStoreEngine::SetSSDSpillManager(std::shared_ptr<SSDSpillManager> spill_manager) {
    impl_->SetSSDSpillManager(std::move(spill_manager));
}
bool KVStoreEngine::HasSpillManager() const {
    return impl_->HasSpillManager();
}

void KVStoreEngine::SetRegionRegistry(std::shared_ptr<StoreRegionRegistry> registry, int32_t owner_dn_id) {
    impl_->SetRegionRegistry(std::move(registry), owner_dn_id);
}

void KVStoreEngine::SetHeartbeatSender(HeartbeatSender sender) {
    impl_->SetHeartbeatSender(std::move(sender));
}

int32_t KVStoreEngine::StoreNodeId() const { return impl_->StoreNodeId(); }
int32_t KVStoreEngine::BlockSize() const { return impl_->BlockSize(); }

StoreWriteResult KVStoreEngine::Write(const std::string& block_hash,
                                      int64_t pool_offset,
                                      const std::string& payload,
                                      int32_t compression,
                                      int32_t original_size,
                                      int32_t block_size,
                                      int64_t expected_version,
                                      int64_t expected_store_epoch,
                                      bool verify_checksum,
                                      uint32_t checksum_hint) {
    return impl_->Write(block_hash,
                        pool_offset,
                        payload,
                        compression,
                        original_size,
                        block_size,
                        expected_version,
                        expected_store_epoch,
                        verify_checksum,
                        checksum_hint);
}

StoreReadResult KVStoreEngine::Read(const std::string& block_hash,
                                    int64_t pool_offset,
                                    int32_t block_size,
                                    int64_t expected_version,
                                    int64_t expected_store_epoch) const {
    return impl_->Read(block_hash, pool_offset, block_size, expected_version, expected_store_epoch);
}

StoreReadResult KVStoreEngine::ReadFromSSD(const std::string& block_hash,
                                           const std::string& evicted_path,
                                           int64_t expected_version) const {
    return impl_->ReadFromSSD(block_hash, evicted_path, expected_version);
}

StoreResultMeta KVStoreEngine::ValidateEvictedPath(const std::string& block_hash,
                                                   const std::string& evicted_path) const {
    return impl_->ValidateEvictedPath(block_hash, evicted_path);
}

StoreWriteResult KVStoreEngine::SpillBlockToSSD(const std::string& block_hash,
                                                int64_t pool_offset,
                                                int64_t expected_version,
                                                int64_t expected_store_epoch,
                                                int32_t dram_read_size,
                                                std::string* out_evicted_path) {
    return impl_->SpillBlockToSSD(block_hash,
                                 pool_offset,
                                 expected_version,
                                 expected_store_epoch,
                                 dram_read_size,
                                 out_evicted_path);
}

int KVStoreEngine::SendHeartbeats(int64_t now_ms) {
    return impl_->SendHeartbeats(now_ms);
}

std::vector<KVStoreEngine::HeartbeatTarget> KVStoreEngine::HeartbeatTargets() const {
    return impl_->HeartbeatTargets();
}

void KVStoreEngine::PutSSDForTest(const std::string& block_hash,
                                  const std::string& evicted_path,
                                  const std::string& payload,
                                  int32_t compression,
                                  int32_t original_size,
                                  int64_t version) {
    impl_->PutSSDForTest(block_hash, evicted_path, payload, compression, original_size, version);
}

}  // namespace falconfs::kv
