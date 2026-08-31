#include "vllm_kv_cache/src/metadata/kv_shmem_runtime.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"

#include <algorithm>
#include <cstring>
#include <mutex>

#if defined(__has_include)
#if __has_include("postgres.h")
#define FALCON_KV_HAVE_POSTGRES_HEADERS 1
#endif
#endif

#if FALCON_KV_HAVE_POSTGRES_HEADERS
extern "C" {
#include "postgres.h"
#include "varatt.h"
#include "falcon/include/connection_pool/connection_pool_config.h"
#include "falcon/include/metadb/kv_shmem.h"
#include "utils/builtins.h"
}
#endif

namespace falconfs::kv {
namespace {

std::mutex g_ops_mu;
std::optional<KVShmemRuntimeOps> g_ops;

#if FALCON_KV_HAVE_POSTGRES_HEADERS
struct DramMetaCandidateCtx {
    std::function<void(const std::string&)> cb;
};

void DramMetaCandidateCallback(void* arg, const char* block_hash, int16_t block_hash_len) {
    if (arg == nullptr || block_hash == nullptr || block_hash_len <= 0) return;
    auto* ctx = static_cast<DramMetaCandidateCtx*>(arg);
    if (ctx->cb) ctx->cb(std::string(block_hash, static_cast<std::size_t>(block_hash_len)));
}

#endif

}  // namespace

void InstallKVShmemRuntimeOps(KVShmemRuntimeOps ops) {
    std::lock_guard<std::mutex> lock(g_ops_mu);
    g_ops = std::move(ops);
}

void ClearKVShmemRuntimeOps() {
    std::lock_guard<std::mutex> lock(g_ops_mu);
    g_ops.reset();
}

std::optional<KVShmemRuntimeOps> GetKVShmemRuntimeOps() {
    std::lock_guard<std::mutex> lock(g_ops_mu);
    return g_ops;
}

void InstallKVShmemRuntimeOpsFromPg() {
#if !FALCON_KV_HAVE_POSTGRES_HEADERS
    // Standalone/test binaries do not link PostgreSQL headers/shmem runtime.
    return;
#else
    KVShmemRuntimeOps ops;

    // ── Bitmap allocator ────────────────────────────────────────────────
    ops.register_region = [](const EngineStoreRegion& region) {
        return KVBitmapShmemRegisterRegion(region.store_node_id,
                                           /*owner_dn_id=*/0,
                                           static_cast<uint64>(region.base_offset),
                                           static_cast<uint64>(region.region_bytes),
                                           static_cast<uint64>(region.block_size));
    };
    ops.bitmap_allocate = [](int32_t store_node_id) {
        KVRuntimeBitmapAllocateResult out;
        uint64 pool_offset = 0;
        if (!KVBitmapShmemAllocate(store_node_id, &pool_offset)) return out;
        out.ok = true;
        out.pool_offset = static_cast<int64_t>(pool_offset);
        return out;
    };
    ops.bitmap_free = [](int32_t store_node_id, int64_t pool_offset) {
        return KVBitmapShmemFree(store_node_id, static_cast<uint64>(pool_offset));
    };
    ops.bitmap_mark_occupied = [](int32_t store_node_id, int64_t pool_offset) {
        return KVBitmapShmemMarkOccupied(store_node_id, static_cast<uint64>(pool_offset));
    };
    ops.bitmap_stats = [](int32_t store_node_id) -> std::optional<std::pair<int64_t, int64_t>> {
        uint64 total = 0, free = 0;
        if (!KVBitmapShmemGetStats(store_node_id, &total, &free)) return std::nullopt;
        return std::make_pair(static_cast<int64_t>(total), static_cast<int64_t>(free));
    };

    // ── DRAM block meta shmem (primary: lease + CLOCK LRU) ──────────────
    ops.dram_meta_insert = [](const std::string& block_hash,
                               int32_t store_node_id, int64_t pool_offset,
                               int64_t store_epoch, int64_t dn_epoch, int32_t status) {
        return KVDramMetaInsert(block_hash.data(),
                                static_cast<int16>(block_hash.size()),
                                store_node_id, pool_offset,
                                store_epoch, dn_epoch,
                                static_cast<int16>(status));
    };
    ops.dram_meta_update_status = [](const std::string& block_hash, int32_t new_status) {
        return KVDramMetaUpdateStatus(block_hash.data(),
                                      static_cast<int16>(block_hash.size()),
                                      static_cast<int16>(new_status));
    };
    ops.dram_meta_grant_lease = [](const std::string& block_hash,
                                    int64_t store_epoch, int64_t now_ms, int64_t ttl_ms) {
        KVRuntimeLeaseGrantResult out;
        int64 lease_token = 0, lease_expire_ms = 0;
        out.ok = KVDramMetaGrantLease(block_hash.data(),
                                      static_cast<int16>(block_hash.size()),
                                      store_epoch, now_ms, ttl_ms,
                                      &lease_token, &lease_expire_ms);
        out.lease_token    = lease_token;
        out.lease_expire_ms = lease_expire_ms;
        return out;
    };
    ops.dram_meta_renew_lease = [](const std::string& block_hash,
                                    int64_t lease_token,
                                    int64_t expected_dn_epoch, int64_t current_dn_epoch,
                                    int64_t expected_store_epoch,
                                    int64_t now_ms, int64_t ttl_ms) {
        KVRuntimeLeaseRenewResult out;
        int64 lease_expire_ms = 0;
        out.ok = KVDramMetaRenewLease(block_hash.data(),
                                      static_cast<int16>(block_hash.size()),
                                      lease_token,
                                      expected_dn_epoch, current_dn_epoch,
                                      expected_store_epoch,
                                      now_ms, ttl_ms,
                                      &lease_expire_ms);
        out.lease_expire_ms = lease_expire_ms;
        return out;
    };
    ops.dram_meta_can_evict = [](const std::string& block_hash, int64_t now_ms) {
        return KVDramMetaCanEvict(block_hash.data(),
                                  static_cast<int16>(block_hash.size()),
                                  now_ms);
    };
    ops.dram_meta_drop = [](const std::string& block_hash) {
        (void) KVDramMetaDrop(block_hash.data(), static_cast<int16>(block_hash.size()));
    };
    ops.dram_meta_remove = [](const std::string& block_hash) {
        return KVDramMetaRemove(block_hash.data(), static_cast<int16>(block_hash.size()));
    };
    ops.dram_meta_clear = []() { KVDramMetaClear(); };
    ops.dram_meta_cold_candidates =
        [](int64_t now_ms,
           int32_t limit,
           const std::function<void(const std::string&)>& cb) {
            if (!cb) return;
            DramMetaCandidateCtx ctx;
            ctx.cb = cb;
            KVDramMetaColdCandidates(now_ms, limit, DramMetaCandidateCallback, &ctx);
        };

    // ── Legacy lease shmem (kept as fallback; dram_meta_* takes priority) ─
    ops.lease_grant = [](const std::string& block_hash,
                          int64_t store_epoch, int64_t now_ms, int64_t ttl_ms) {
        KVRuntimeLeaseGrantResult out;
        int64 lease_token = 0, lease_expire_ms = now_ms + ttl_ms;
        out.ok = KVLeaseShmemGrant(block_hash.data(),
                                   static_cast<int16>(block_hash.size()),
                                   store_epoch, now_ms, ttl_ms,
                                   &lease_token, &lease_expire_ms);
        out.lease_token    = lease_token;
        out.lease_expire_ms = lease_expire_ms;
        return out;
    };
    ops.lease_renew = [](const std::string& block_hash,
                          int64_t lease_token,
                          int64_t expected_dn_epoch, int64_t current_dn_epoch,
                          int64_t expected_store_epoch,
                          int64_t now_ms, int64_t ttl_ms) {
        KVRuntimeLeaseRenewResult out;
        int64 lease_expire_ms = now_ms + ttl_ms;
        out.ok = KVLeaseShmemRenew(block_hash.data(),
                                   static_cast<int16>(block_hash.size()),
                                   lease_token,
                                   expected_dn_epoch, current_dn_epoch,
                                   expected_store_epoch,
                                   now_ms, ttl_ms,
                                   &lease_expire_ms);
        out.lease_expire_ms = lease_expire_ms;
        return out;
    };
    ops.lease_can_evict = [](const std::string& block_hash, int64_t now_ms) {
        return KVLeaseShmemCanEvict(block_hash.data(),
                                   static_cast<int16>(block_hash.size()), now_ms);
    };
    ops.lease_drop = [](const std::string& block_hash) {
        (void) KVLeaseShmemDrop(block_hash.data(), static_cast<int16>(block_hash.size()));
    };
    ops.lease_clear = []() { KVLeaseShmemClear(); };

    InstallKVShmemRuntimeOps(std::move(ops));
#endif
}

}  // namespace falconfs::kv
