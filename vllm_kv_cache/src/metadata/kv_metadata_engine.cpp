// v6.4 metadata engine: in-process DRAM Runtime Cache (bitmap + meta array +
// per-shard hash index) with write-through to persistent catalog.
//
// Architecture:
//   - KVRegion: per-Store-region atomic bitmap + DramMetaSlot array + CLOCK hand.
//   - ShardHashIndex: striped concurrent hash map block_hash->(region,slot_idx).
//   - Catalog bridge: shmem_ops_ (production libpq) or fallback_meta_ (unit tests).
//   - No PG shared-memory for runtime state; process-local state only.

#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"

#include <cassert>
#include <algorithm>
#include <array>
#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "vllm_kv_cache/src/common/kv_types.h"
#include "vllm_kv_cache/src/metadata/bitmap_allocator.h"
#include "vllm_kv_cache/src/metadata/kv_meta_table_accessor.h"
#include "vllm_kv_cache/src/metadata/kv_shmem_runtime.h"
#include "vllm_kv_cache/src/metadata/lease_manager.h"

namespace falconfs::kv {

namespace {

// Sentinel for v6.4 §8 "AllocatedPendingStore": no loadable pool_offset / lease yet.
constexpr int64_t kPendingStorePoolOffset = -1;

// ──────────────────────────────────────────────────────────────────────────────
// Result helpers (unchanged from prior versions)
// ──────────────────────────────────────────────────────────────────────────────

EngineResultMeta ToEngineResult(const ItemResult& result) {
    EngineResultMeta out;
    out.success = result.success;
    out.error_code = static_cast<int32_t>(result.error_code);
    out.retryable = result.retryable;
    out.error_message = result.error_message;
    return out;
}

EngineBlockLocation ToEngineLocation(const BlockLocation& location) {
    EngineBlockLocation out;
    out.store_node_id = location.store_node_id;
    out.pool_offset = location.pool_offset;
    out.evicted_path = location.evicted_path;
    out.store_epoch = location.store_epoch;
    return out;
}

EngineLeaseInfo ToEngineLease(const LeaseInfo& lease) {
    EngineLeaseInfo out;
    out.lease_token = lease.lease_token;
    out.lease_expire_ms = lease.lease_expire_ms;
    out.dn_epoch = lease.dn_epoch;
    out.store_epoch = lease.store_epoch;
    return out;
}

constexpr int64_t kDefaultLeaseTtlMs = 5000;

// ──────────────────────────────────────────────────────────────────────────────
// v6.4 in-process DRAM cache data structures
// ──────────────────────────────────────────────────────────────────────────────

// Per-DRAM-block runtime state stored in the region's dense meta array.
// Access is protected by the ShardHashIndex stripe lock for the block's hash.
struct DramMetaSlot {
    uint8_t  state{0};           // BlockStatus as uint8; 0 = FREE/UNSPECIFIED
    uint8_t  ref_bit{0};         // CLOCK second-chance (1 = recently used)
    uint16_t shard_id{0};        // catalog shard owning this block
    int32_t  store_node_id{0};   // which Store region holds this block
    int64_t  lease_token{0};     // 0 = no lease
    int64_t  lease_expire_ms{0}; // 0 = no expiry
    int64_t  version{0};         // mirrors catalog version
    int64_t  store_epoch{0};     // epoch of Store-region at allocation time
    std::string block_hash;      // reverse-lookup key for CLOCK scan
};

// Reference into (region_id=store_node_id, slot_idx).
struct SlotRef {
    int32_t store_node_id{0};
    int64_t slot_idx{0};
};

struct PendingRestoreRow {
    std::string block_hash;
    int32_t status{0};
    EngineBlockLocation location;
    int64_t version{0};
    int64_t now_ms{0};
};

// Per-Store-region owner: atomic bitmap + fixed meta array + CLOCK state.
struct KVRegion {
    int32_t store_node_id{0};
    int32_t owner_dn_id{0};
    int64_t base_offset{0};
    int64_t region_bytes{0};
    int32_t block_size{0};
    int64_t total_blocks{0};
    int64_t store_epoch{0};
    EngineRegionState state{EngineRegionState::HEALTHY};

    // Bitmap: bit i = 1 means slot i is occupied.
    // (Declared before free_blocks so initializer order matches declaration.)
    std::vector<std::atomic<uint64_t>> bitmap_words;
    // Dense meta array: meta[i] is the runtime state of bitmap slot i.
    std::unique_ptr<DramMetaSlot[]> meta;
    // CLOCK / allocation hints.
    mutable std::atomic<uint64_t> alloc_hint{0};
    mutable std::atomic<uint64_t> clock_hand{0};
    std::atomic<int64_t> free_blocks{0};

    KVRegion(int32_t sid, int32_t dn, int64_t base, int64_t bytes,
             int32_t bs, int64_t epoch)
        : store_node_id(sid), owner_dn_id(dn), base_offset(base),
          region_bytes(bytes), block_size(bs),
          total_blocks(bs > 0 && bytes > 0 ? bytes / bs : 0),
          store_epoch(epoch),
          bitmap_words(
              static_cast<std::size_t>(
                  bs > 0 && bytes > 0 ? ((bytes / bs) + 63) / 64 : 0)),
          free_blocks(bs > 0 && bytes > 0 ? bytes / bs : 0) {
        // bitmap_words are zero-initialised by the vector constructor.
        if (total_blocks > 0)
            meta = std::make_unique<DramMetaSlot[]>(
                static_cast<std::size_t>(total_blocks));
    }

    KVRegion(const KVRegion&) = delete;
    KVRegion& operator=(const KVRegion&) = delete;

    int64_t SlotIdxFromOffset(int64_t offset) const {
        if (block_size <= 0 || offset < base_offset) return -1;
        return (offset - base_offset) / block_size;
    }
    int64_t OffsetFromSlot(int64_t slot_idx) const {
        return base_offset + slot_idx * block_size;
    }
};

// Per-shard striped concurrent hash map: block_hash → SlotRef.
constexpr int kHashStripes = 64;
struct ShardHashIndex {
    struct Stripe {
        mutable std::shared_mutex mu;
        std::unordered_map<std::string, SlotRef> map;
    };
    std::array<Stripe, kHashStripes> stripes;

    int StripeIdx(const std::string& key) const {
        return static_cast<int>(std::hash<std::string>{}(key) % kHashStripes);
    }
    Stripe& GetStripe(const std::string& key) { return stripes[StripeIdx(key)]; }
    const Stripe& GetStripe(const std::string& key) const { return stripes[StripeIdx(key)]; }

    std::optional<SlotRef> Find(const std::string& key) const {
        const Stripe& s = GetStripe(key);
        std::shared_lock<std::shared_mutex> lk(s.mu);
        auto it = s.map.find(key);
        if (it == s.map.end()) return std::nullopt;
        return it->second;
    }
    // Returns true if newly inserted (false = already existed).
    bool TryInsert(const std::string& key, SlotRef ref) {
        Stripe& s = GetStripe(key);
        std::unique_lock<std::shared_mutex> lk(s.mu);
        return s.map.emplace(key, ref).second;
    }
    void Upsert(const std::string& key, SlotRef ref) {
        Stripe& s = GetStripe(key);
        std::unique_lock<std::shared_mutex> lk(s.mu);
        s.map[key] = ref;
    }
    bool Erase(const std::string& key) {
        Stripe& s = GetStripe(key);
        std::unique_lock<std::shared_mutex> lk(s.mu);
        return s.map.erase(key) > 0;
    }

    // v6.4 §6: caller holds exclusive stripe lock while mutating DramMetaSlot
    // fields or while reading slot state together with hash-map membership.
    template <typename Fn>
    void WithExclusiveStripe(const std::string& key, Fn&& fn) {
        Stripe& s = GetStripe(key);
        std::unique_lock<std::shared_mutex> lk(s.mu);
        auto it = s.map.find(key);
        std::optional<SlotRef> ref;
        if (it != s.map.end()) ref = it->second;
        std::forward<Fn>(fn)(ref);
    }

    void EraseStore(int32_t store_node_id) {
        for (auto& s : stripes) {
            std::unique_lock<std::shared_mutex> lk(s.mu);
            for (auto it = s.map.begin(); it != s.map.end();) {
                if (it->second.store_node_id == store_node_id) {
                    it = s.map.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }

    void Clear() {
        for (auto& s : stripes) {
            std::unique_lock<std::shared_mutex> lk(s.mu);
            s.map.clear();
        }
    }
};

}  // namespace

// ──────────────────────────────────────────────────────────────────────────────
// KVMetadataEngine::Impl
// ──────────────────────────────────────────────────────────────────────────────

class KVMetadataEngine::Impl {
public:
    Impl(int32_t store_node_id,
         int32_t dn_id,
         int64_t region_bytes,
         int32_t block_size,
         int64_t dn_epoch,
         int64_t store_epoch,
         int32_t kvblock_shard_id)
        : dn_id_(dn_id),
          block_size_(block_size),
          dn_epoch_(dn_epoch),
          shard_id_(kvblock_shard_id),
          next_lease_token_(1),
          lease_manager_(dn_epoch) {
        shmem_ops_ = GetKVShmemRuntimeOps();
        if (region_bytes > 0 && block_size > 0) {
            EngineStoreRegion r;
            r.store_node_id = store_node_id;
            r.base_offset   = 0;
            r.region_bytes  = region_bytes;
            r.block_size    = block_size;
            r.store_epoch   = store_epoch;
            RegisterStoreRegionInternal(r);
        }
    }

    int32_t DnId()      const { return dn_id_; }
    int64_t DnEpoch()   const { return dn_epoch_; }
    int32_t BlockSize() const { return block_size_; }

    void SetAllowInlineMetaTableLookup(bool v) {
        allow_inline_meta_lookup_ = v;
        catalog_tier_ = v ? EngineCatalogTier::LOCAL_FALLBACK : EngineCatalogTier::REMOTE_LIBPQ;
    }
    bool AllowInlineMetaTableLookup() const { return allow_inline_meta_lookup_; }

    void SetCatalogTier(EngineCatalogTier tier) {
        catalog_tier_ = tier;
        allow_inline_meta_lookup_ = (tier == EngineCatalogTier::LOCAL_FALLBACK);
    }
    EngineCatalogTier CatalogTier() const { return catalog_tier_; }

    void SetDnEpochFromCatalog(int64_t epoch) {
        dn_epoch_ = epoch;
        lease_manager_.SetDnEpoch(dn_epoch_);
        lease_manager_.Clear();
    }

    // ── Region management ────────────────────────────────────────────────────

    EngineResultMeta RegisterStoreRegion(const EngineStoreRegion& region) {
        if (region.block_size != block_size_)
            return ToEngineResult(ItemResult::Err(ErrorCode::INVALID_ARGUMENT, false,
                                                  "region block_size mismatch"));
        EngineResultMeta out = RegisterStoreRegionInternal(region);
        if (out.success) {
            ApplyPendingRestoresForStore(region.store_node_id);
        }
        return out;
    }

    EngineResultMeta SetRegionState(int32_t store_node_id, EngineRegionState state) {
        std::unique_lock<std::shared_mutex> lk(regions_mu_);
        auto it = regions_.find(store_node_id);
        if (it == regions_.end())
            return ToEngineResult(ItemResult::Err(ErrorCode::INVALID_ARGUMENT, false,
                                                  "unknown region"));
        it->second->state = state;
        return ToEngineResult(ItemResult::Ok());
    }

    bool HasRegion(int32_t store_node_id) const {
        std::shared_lock<std::shared_mutex> lk(regions_mu_);
        return regions_.find(store_node_id) != regions_.end();
    }

    int64_t RegionStoreEpoch(int32_t store_node_id) const {
        std::shared_lock<std::shared_mutex> lk(regions_mu_);
        auto it = regions_.find(store_node_id);
        return it == regions_.end() ? 0 : it->second->store_epoch;
    }

    // ── Recovery helpers ─────────────────────────────────────────────────────

    EngineResultMeta MarkBitmapOccupied(int32_t store_node_id, int64_t pool_offset) {
        std::shared_lock<std::shared_mutex> lk(regions_mu_);
        auto it = regions_.find(store_node_id);
        if (it == regions_.end())
            return ToEngineResult(ItemResult::Err(ErrorCode::INVALID_ARGUMENT, false,
                                                  "unknown region"));
        KVRegion& re = *it->second;
        int64_t slot_idx = re.SlotIdxFromOffset(pool_offset);
        if (slot_idx < 0 || slot_idx >= re.total_blocks)
            return ToEngineResult(ItemResult::Err(ErrorCode::INVALID_ARGUMENT, false,
                                                  "pool_offset out of range"));
        TryMarkBitOccupied(re, slot_idx);
        return ToEngineResult(ItemResult::Ok());
    }

    EngineResultMeta RestoreRow(const std::string& block_hash,
                                int32_t status,
                                const EngineBlockLocation& location,
                                int64_t version,
                                int64_t now_ms) {
        return RestoreRowInternal(block_hash, status, location, version, now_ms,
                                  /*park_if_region_missing=*/true);
    }

    EngineResultMeta RestoreRowInternal(const std::string& block_hash,
                                        int32_t status,
                                        const EngineBlockLocation& location,
                                        int64_t version,
                                        int64_t now_ms,
                                        bool park_if_region_missing) {
        // Persist to in-memory catalog only in unit-test / LOCAL_FALLBACK mode.
        if (catalog_tier_ == EngineCatalogTier::LOCAL_FALLBACK) {
            AccessorRow arow;
            arow.status         = status;
            arow.store_node_id  = location.store_node_id;
            arow.pool_offset    = location.pool_offset;
            arow.evicted_path   = location.evicted_path;
            arow.version        = version;
            arow.updated_at_ms  = now_ms;
            if (!fallback_meta_.UpsertForRecovery(shard_id_, block_hash, arow))
                return ToEngineResult(ItemResult::Err(ErrorCode::INTERNAL_ERROR, false,
                                                      "restore upsert failed"));
        }

        BlockStatus bs = static_cast<BlockStatus>(status);
        // Only DRAM-resident statuses get a meta entry + hash-index entry.
        if (bs == BlockStatus::ALLOCATED || bs == BlockStatus::STORED ||
            bs == BlockStatus::EVICTING  || bs == BlockStatus::FAILED) {
            std::shared_lock<std::shared_mutex> lk(regions_mu_);
            auto it = regions_.find(location.store_node_id);
            if (it == regions_.end()) {
                if (park_if_region_missing) {
                    ParkRestoreRow(block_hash, status, location, version, now_ms);
                }
                return ToEngineResult(ItemResult::Ok());  // region not registered yet
            }
            KVRegion& re = *it->second;
            int64_t slot_idx = re.SlotIdxFromOffset(location.pool_offset);
            if (slot_idx < 0 || slot_idx >= re.total_blocks)
                return ToEngineResult(ItemResult::Ok());
            TryMarkBitOccupied(re, slot_idx);
            // Initialise meta slot. v6 §15.4: a grace lease prevents the
            // CLOCK eviction worker from immediately consuming the entire
            // reseeded working set on the first pass after recovery, and
            // also blocks spurious STORED -> EVICTING -> STORED CAS cycles
            // that would bump the version unnecessarily before any client
            // ever talks to this DN.
            DramMetaSlot& slot = re.meta[slot_idx];
            slot.state         = static_cast<uint8_t>(bs);
            slot.ref_bit       = 0;
            slot.shard_id      = static_cast<uint16_t>(shard_id_);
            slot.store_node_id = location.store_node_id;
            slot.lease_token   = NewLeaseToken();
            slot.lease_expire_ms = now_ms + kDefaultLeaseTtlMs;
            slot.version       = version;
            slot.store_epoch   = re.store_epoch;
            slot.block_hash    = block_hash;
            // Publish to hash index (upsert for idempotent recovery).
            shard_index_.Upsert(block_hash,
                                SlotRef{location.store_node_id, slot_idx});
        }
        return ToEngineResult(ItemResult::Ok());
    }

    void ParkRestoreRow(const std::string& block_hash,
                        int32_t status,
                        const EngineBlockLocation& location,
                        int64_t version,
                        int64_t now_ms) {
        std::lock_guard<std::mutex> lk(pending_restore_mu_);
        auto& rows = pending_restores_by_store_[location.store_node_id];
        for (auto& row : rows) {
            if (row.block_hash == block_hash) {
                row.status = status;
                row.location = location;
                row.version = version;
                row.now_ms = now_ms;
                return;
            }
        }
        rows.push_back(PendingRestoreRow{block_hash, status, location, version, now_ms});
    }

    void DropPendingRestoresForStore(int32_t store_node_id) {
        std::lock_guard<std::mutex> lk(pending_restore_mu_);
        pending_restores_by_store_.erase(store_node_id);
    }

    void ApplyPendingRestoresForStore(int32_t store_node_id) {
        std::vector<PendingRestoreRow> rows;
        {
            std::lock_guard<std::mutex> lk(pending_restore_mu_);
            auto it = pending_restores_by_store_.find(store_node_id);
            if (it == pending_restores_by_store_.end()) return;
            rows.swap(it->second);
            pending_restores_by_store_.erase(it);
        }
        for (const auto& row : rows) {
            (void)RestoreRowInternal(row.block_hash, row.status, row.location,
                                     row.version, row.now_ms,
                                     /*park_if_region_missing=*/false);
        }
    }

    static void ResetRegionRuntime(KVRegion& region) {
        for (auto& word : region.bitmap_words) {
            word.store(0, std::memory_order_release);
        }
        for (int64_t idx = 0; idx < region.total_blocks; ++idx) {
            region.meta[static_cast<std::size_t>(idx)] = DramMetaSlot{};
        }
        region.free_blocks.store(region.total_blocks, std::memory_order_relaxed);
        region.alloc_hint.store(0, std::memory_order_relaxed);
        region.clock_hand.store(0, std::memory_order_relaxed);
    }

    void ClearRuntimeForStoreRestart(int32_t store_node_id) {
        // Match lookup/drop lock order: shard stripes first, then regions_mu_.
        shard_index_.EraseStore(store_node_id);
        {
            std::unique_lock<std::shared_mutex> lk(regions_mu_);
            auto it = regions_.find(store_node_id);
            if (it != regions_.end()) {
                ResetRegionRuntime(*it->second);
            }
        }
        DropPendingRestoresForStore(store_node_id);
    }

    int64_t BumpDnEpoch() {
        ++dn_epoch_;
        // All lease tokens become stale: epoch check in RenewLease validates
        // expected_dn_epoch against the engine's current dn_epoch_.
        // Also update the fallback lease_manager so catalog-path leases are fenced.
        lease_manager_.SetDnEpoch(dn_epoch_);
        lease_manager_.Clear();
        return dn_epoch_;
    }

    // ── Hot-path operations ──────────────────────────────────────────────────

    EngineLookupResult LookupDramCacheOnly(const std::string& block_hash,
                                           bool renew_lease_on_hit,
                                           int64_t now_ms) {
        EngineLookupResult pending;
        std::optional<EngineLookupResult> dram_out;

        // v6.4 §6: exclusive stripe lock for DRAM hits (slot fields + lease/ref_bit).
        shard_index_.WithExclusiveStripe(block_hash, [&](const std::optional<SlotRef>& slot_ref) {
            if (!slot_ref.has_value()) return;
            std::shared_lock<std::shared_mutex> rl(regions_mu_);
            auto it = regions_.find(slot_ref->store_node_id);
            if (it == regions_.end()) return;
            KVRegion& re = *it->second;
            if (slot_ref->slot_idx < 0 || slot_ref->slot_idx >= re.total_blocks) return;
            DramMetaSlot& slot = re.meta[slot_ref->slot_idx];
            const BlockStatus st = static_cast<BlockStatus>(slot.state);

            EngineLookupResult local;
            if (st == BlockStatus::EVICTING) {
                local.result = ToEngineResult(
                    ItemResult::Err(ErrorCode::CAS_CONFLICT, true, "evicting"));
                dram_out = std::move(local);
                return;
            }
            // v6.4 §8 / §13: ALLOCATED in DRAM — load path omits pool_offset and lease.
            if (st == BlockStatus::ALLOCATED) {
                local.result = ToEngineResult(ItemResult::Ok());
                EngineBlockMeta meta;
                meta.status = static_cast<int32_t>(BlockStatus::ALLOCATED);
                meta.location.store_node_id = slot.store_node_id;
                meta.location.pool_offset   = kPendingStorePoolOffset;
                meta.location.store_epoch   = slot.store_epoch;
                meta.version                = slot.version;
                local.row                   = meta;
                local.cacheable             = false;
                local.allocated_pending_store = true;
                dram_out = std::move(local);
                return;
            }
            if (st == BlockStatus::STORED) {
                if (renew_lease_on_hit) {
                    GrantOrExtendLeaseInSlot(slot, now_ms);
                }
                slot.ref_bit = 1;
                BlockLocation loc;
                loc.store_node_id = slot.store_node_id;
                loc.pool_offset   = re.OffsetFromSlot(slot_ref->slot_idx);
                loc.store_epoch   = slot.store_epoch;
                EngineBlockMeta meta;
                meta.status   = static_cast<int32_t>(BlockStatus::STORED);
                meta.location = ToEngineLocation(loc);
                meta.version  = slot.version;
                local.row = meta;
                local.cacheable = true;
                if (renew_lease_on_hit) {
                    LeaseInfo li;
                    li.lease_token     = slot.lease_token;
                    li.lease_expire_ms = slot.lease_expire_ms;
                    li.dn_epoch        = dn_epoch_;
                    li.store_epoch     = slot.store_epoch;
                    local.lease = ToEngineLease(li);
                }
                local.result = ToEngineResult(ItemResult::Ok());
                dram_out = std::move(local);
                return;
            }
            // EVICTED must not appear in the DRAM shard index (v6.4 §13).
        });

        if (dram_out.has_value()) return *dram_out;

        pending.needs_catalog = true;
        pending.result = ToEngineResult(ItemResult::Ok());
        return pending;
    }

    EngineLookupResult Lookup(const std::string& block_hash,
                              bool renew_lease_on_hit,
                              int64_t now_ms) {
        EngineLookupResult pre = LookupDramCacheOnly(block_hash, renew_lease_on_hit, now_ms);
        if (!pre.needs_catalog) return pre;
        if (catalog_tier_ == EngineCatalogTier::REMOTE_LIBPQ || !allow_inline_meta_lookup_) {
            EngineLookupResult out;
            out.result = ToEngineResult(
                ItemResult::Err(ErrorCode::NOT_FOUND, false, "catalog path required"));
            return out;
        }

        EngineLookupResult out;
        // ── Catalog path (cache miss or catalog-only rows such as EVICTED) ──
        Row row;
        if (!LookupMetaRow(block_hash, &row)) {
            out.result = ToEngineResult(ItemResult::Err(ErrorCode::NOT_FOUND, false, "missing"));
            return out;
        }
        if (row.status == BlockStatus::EVICTING) {
            out.result = ToEngineResult(
                ItemResult::Err(ErrorCode::CAS_CONFLICT, true, "evicting"));
            return out;
        }
        if (row.status == BlockStatus::EVICTED) {
            out.result = ToEngineResult(ItemResult::Ok());
            out.row = ToEngineMeta(row);
            out.cacheable = true;
            out.evicted_catalog_hit = true;
            return out;
        }
        if (row.status == BlockStatus::FAILED) {
            out.result = ToEngineResult(ItemResult::Err(ErrorCode::NOT_FOUND, false, "failed"));
            return out;
        }
        if (row.status == BlockStatus::ALLOCATED) {
            out.result = ToEngineResult(ItemResult::Ok());
            EngineBlockMeta meta;
            meta.status = static_cast<int32_t>(BlockStatus::ALLOCATED);
            meta.location.store_node_id = row.location.store_node_id;
            meta.location.pool_offset   = kPendingStorePoolOffset;
            meta.location.store_epoch   = row.location.store_epoch;
            meta.version                = row.version;
            out.row                     = meta;
            out.cacheable               = false;
            out.allocated_pending_store = true;
            return out;
        }
        if (row.status == BlockStatus::STORED) {
            out.result    = ToEngineResult(ItemResult::Ok());
            out.row       = ToEngineMeta(row);
            out.cacheable = true;
            // v6.4: no lease_manager_ grant on catalog-only STORED without a DRAM slot.
            (void)renew_lease_on_hit;
            return out;
        }
        out.result = ToEngineResult(ItemResult::Err(ErrorCode::NOT_FOUND, false, "not found"));
        return out;
    }

    EngineAllocateResult Allocate(const std::string& block_hash,
                                  int32_t block_size,
                                  int64_t now_ms,
                                  int32_t preferred_store_id = 0,
                                  bool allow_fallback_store = true) {
        EngineAllocateResult out;
        if (block_size <= 0 || block_size != block_size_) {
            out.result = ToEngineResult(
                ItemResult::Err(ErrorCode::INVALID_ARGUMENT, false, "block_size mismatch"));
            return out;
        }

        // ── Cache pre-step: reuse existing slot if already allocated ──
        {
            auto slot_ref = shard_index_.Find(block_hash);
            if (slot_ref.has_value()) {
                std::shared_lock<std::shared_mutex> rl(regions_mu_);
                auto it = regions_.find(slot_ref->store_node_id);
                if (it != regions_.end()) {
                    KVRegion& re = *it->second;
                    if (slot_ref->slot_idx >= 0 && slot_ref->slot_idx < re.total_blocks) {
                        DramMetaSlot& slot = re.meta[slot_ref->slot_idx];
                        BlockStatus st = static_cast<BlockStatus>(slot.state);
                        if (st == BlockStatus::ALLOCATED || st == BlockStatus::STORED) {
                            GrantOrExtendLeaseInSlot(slot, now_ms);
                            BlockLocation loc;
                            loc.store_node_id = slot.store_node_id;
                            loc.pool_offset   = re.OffsetFromSlot(slot_ref->slot_idx);
                            loc.store_epoch   = slot.store_epoch;
                            EngineBlockMeta meta;
                            meta.status   = static_cast<int32_t>(st);
                            meta.location = ToEngineLocation(loc);
                            meta.version  = slot.version;
                            LeaseInfo li;
                            li.lease_token    = slot.lease_token;
                            li.lease_expire_ms = slot.lease_expire_ms;
                            li.dn_epoch       = dn_epoch_;
                            li.store_epoch    = slot.store_epoch;
                            out.result = ToEngineResult(ItemResult::Ok());
                            out.row    = meta;
                            out.lease  = ToEngineLease(li);
                            out.reused_existing_allocation = true;
                            return out;
                        }
                        if (st == BlockStatus::EVICTING) {
                            out.result = ToEngineResult(
                                ItemResult::Err(ErrorCode::CAS_CONFLICT, true, "evicting"));
                            return out;
                        }
                    }
                }
            }
        }

        // ── Pick region and reserve bitmap slot ──
        std::vector<KVRegion*> candidates = PickRegions(preferred_store_id, allow_fallback_store);
        if (candidates.empty()) {
            out.result = ToEngineResult(
                ItemResult::Err(ErrorCode::THROTTLED, true, "no healthy region"));
            return out;
        }

        if (catalog_tier_ == EngineCatalogTier::REMOTE_LIBPQ) {
            out.result = ToEngineResult(
                ItemResult::Err(ErrorCode::INTERNAL_ERROR, false,
                                "catalog path required; use AllocatePass1ReserveBitmap + libpq"));
            return out;
        }

        ItemResult last_err = ItemResult::Err(ErrorCode::THROTTLED, true, "no healthy region");
        for (KVRegion* re : candidates) {
            auto slot_res = AllocateBitmapSlot(*re);
            if (!slot_res.ok) {
                last_err = ItemResult::Err(ErrorCode::THROTTLED, true, "bitmap shmem exhausted");
                continue;
            }
            const int64_t slot_idx   = slot_res.slot_idx;
            const int64_t pool_offset = slot_res.pool_offset;

            // ── Catalog write-through (INSERT) ──
            if (!InsertAllocatedMetaRow(block_hash, re->store_node_id, pool_offset, now_ms)) {
                // Catalog rejected (duplicate key or error); rollback bitmap.
                FreeBitmapSlot(*re, slot_idx);
                last_err = ItemResult::Err(ErrorCode::CAS_CONFLICT, true,
                                           "metadata insert conflict");
                continue;
            }

            // ── Commit cache: initialise meta slot and publish to hash index ──
            // v6.4 §3.1 / §7.4: newly allocated rows start at version=1 on both
            // tiers. The DRAM mirror MUST match the catalog INSERT version so
            // the immediate CAS UpdateStatus(ALLOCATED→STORED) reads the same
            // expected_version on either tier.
            DramMetaSlot& slot = re->meta[slot_idx];
            slot.state         = static_cast<uint8_t>(BlockStatus::ALLOCATED);
            slot.ref_bit       = 1;
            slot.shard_id      = static_cast<uint16_t>(shard_id_);
            slot.store_node_id = re->store_node_id;
            slot.version       = 1;
            slot.store_epoch   = re->store_epoch;
            slot.block_hash    = block_hash;
            // Grant initial lease.
            slot.lease_token    = NewLeaseToken();
            slot.lease_expire_ms = now_ms + kDefaultLeaseTtlMs;

            if (!shard_index_.TryInsert(block_hash, SlotRef{re->store_node_id, slot_idx})) {
                // A concurrent allocate won the shard-index race; roll back.
                slot = DramMetaSlot{};
                FreeBitmapSlot(*re, slot_idx);
                // Refetch the winner.
                auto winner = shard_index_.Find(block_hash);
                if (winner.has_value()) {
                    std::shared_lock<std::shared_mutex> rl(regions_mu_);
                    auto wit = regions_.find(winner->store_node_id);
                    if (wit != regions_.end()) {
                        KVRegion& wre = *wit->second;
                        if (winner->slot_idx >= 0 && winner->slot_idx < wre.total_blocks) {
                            DramMetaSlot& ws = wre.meta[winner->slot_idx];
                            BlockLocation loc;
                            loc.store_node_id = ws.store_node_id;
                            loc.pool_offset   = wre.OffsetFromSlot(winner->slot_idx);
                            loc.store_epoch   = ws.store_epoch;
                            EngineBlockMeta meta;
                            meta.status   = static_cast<int32_t>(
                                static_cast<BlockStatus>(ws.state));
                            meta.location = ToEngineLocation(loc);
                            meta.version  = ws.version;
                            LeaseInfo li;
                            li.lease_token    = ws.lease_token;
                            li.lease_expire_ms = ws.lease_expire_ms;
                            li.dn_epoch       = dn_epoch_;
                            li.store_epoch    = ws.store_epoch;
                            out.result = ToEngineResult(ItemResult::Ok());
                            out.row    = meta;
                            out.lease  = ToEngineLease(li);
                            out.reused_existing_allocation = true;
                            return out;
                        }
                    }
                }
                last_err = ItemResult::Err(ErrorCode::CAS_CONFLICT, true,
                                           "concurrent allocate");
                continue;
            }

            BlockLocation loc;
            loc.store_node_id = re->store_node_id;
            loc.pool_offset   = pool_offset;
            loc.store_epoch   = re->store_epoch;
            EngineBlockMeta meta;
            meta.status   = static_cast<int32_t>(BlockStatus::ALLOCATED);
            meta.location = ToEngineLocation(loc);
            meta.version  = 1;
            LeaseInfo li;
            li.lease_token    = slot.lease_token;
            li.lease_expire_ms = slot.lease_expire_ms;
            li.dn_epoch       = dn_epoch_;
            li.store_epoch    = re->store_epoch;
            out.result = ToEngineResult(ItemResult::Ok());
            out.row    = meta;
            out.lease  = ToEngineLease(li);
            out.reused_existing_allocation = false;
            return out;
        }
        out.result = ToEngineResult(last_err);
        return out;
    }

    EngineAllocatePass1Outcome AllocatePass1ReserveBitmap(const std::string& block_hash,
                                                          int32_t block_size,
                                                          int64_t now_ms,
                                                          int32_t preferred_store_id,
                                                          bool allow_fallback_store) {
        EngineAllocatePass1Outcome out;
        if (block_size <= 0 || block_size != block_size_) {
            out.kind = EngineAllocatePass1Outcome::Kind::Failed;
            out.fail_meta = ToEngineResult(
                ItemResult::Err(ErrorCode::INVALID_ARGUMENT, false, "block_size mismatch"));
            return out;
        }

        {
            auto slot_ref = shard_index_.Find(block_hash);
            if (slot_ref.has_value()) {
                std::shared_lock<std::shared_mutex> rl(regions_mu_);
                auto it = regions_.find(slot_ref->store_node_id);
                if (it != regions_.end()) {
                    KVRegion& re = *it->second;
                    if (slot_ref->slot_idx >= 0 && slot_ref->slot_idx < re.total_blocks) {
                        DramMetaSlot& slot = re.meta[slot_ref->slot_idx];
                        BlockStatus st = static_cast<BlockStatus>(slot.state);
                        if (st == BlockStatus::ALLOCATED || st == BlockStatus::STORED) {
                            GrantOrExtendLeaseInSlot(slot, now_ms);
                            BlockLocation loc;
                            loc.store_node_id = slot.store_node_id;
                            loc.pool_offset   = re.OffsetFromSlot(slot_ref->slot_idx);
                            loc.store_epoch   = slot.store_epoch;
                            EngineBlockMeta meta;
                            meta.status   = static_cast<int32_t>(st);
                            meta.location = ToEngineLocation(loc);
                            meta.version  = slot.version;
                            LeaseInfo li;
                            li.lease_token     = slot.lease_token;
                            li.lease_expire_ms = slot.lease_expire_ms;
                            li.dn_epoch        = dn_epoch_;
                            li.store_epoch     = slot.store_epoch;
                            out.kind = EngineAllocatePass1Outcome::Kind::ReusedDram;
                            out.reused_dram.result = ToEngineResult(ItemResult::Ok());
                            out.reused_dram.row    = meta;
                            out.reused_dram.lease  = ToEngineLease(li);
                            out.reused_dram.reused_existing_allocation = true;
                            return out;
                        }
                        if (st == BlockStatus::EVICTING) {
                            out.kind = EngineAllocatePass1Outcome::Kind::Failed;
                            out.fail_meta = ToEngineResult(
                                ItemResult::Err(ErrorCode::CAS_CONFLICT, true, "evicting"));
                            return out;
                        }
                    }
                }
            }
        }

        std::vector<KVRegion*> candidates = PickRegions(preferred_store_id, allow_fallback_store);
        if (candidates.empty()) {
            out.kind = EngineAllocatePass1Outcome::Kind::Failed;
            out.fail_meta = ToEngineResult(
                ItemResult::Err(ErrorCode::THROTTLED, true, "no healthy region"));
            return out;
        }

        ItemResult last_err = ItemResult::Err(ErrorCode::THROTTLED, true, "no healthy region");
        for (KVRegion* re : candidates) {
            auto slot_res = AllocateBitmapSlot(*re);
            if (!slot_res.ok) {
                last_err = ItemResult::Err(ErrorCode::THROTTLED, true, "bitmap shmem exhausted");
                continue;
            }
            const int64_t slot_idx   = slot_res.slot_idx;
            const int64_t pool_offset = slot_res.pool_offset;
            re->meta[slot_idx]       = DramMetaSlot{};

            out.kind             = EngineAllocatePass1Outcome::Kind::ReservedBitmap;
            out.store_node_id    = re->store_node_id;
            out.pool_offset      = pool_offset;
            out.slot_idx         = slot_idx;
            return out;
        }
        out.kind = EngineAllocatePass1Outcome::Kind::Failed;
        out.fail_meta = ToEngineResult(last_err);
        return out;
    }

    void RollbackAllocatePass1Reservation(int32_t store_node_id, int64_t slot_idx) {
        std::shared_lock<std::shared_mutex> lk(regions_mu_);
        auto it = regions_.find(store_node_id);
        if (it == regions_.end()) return;
        KVRegion& re = *it->second;
        if (slot_idx >= 0 && slot_idx < re.total_blocks) {
            re.meta[slot_idx] = DramMetaSlot{};
        }
        FreeBitmapSlot(re, slot_idx);
    }

    EngineAllocateResult CommitAllocatePass1AfterCatalogInsert(const std::string& block_hash,
                                                               int32_t store_node_id,
                                                               int64_t slot_idx,
                                                               int64_t now_ms) {
        EngineAllocateResult out;
        std::shared_lock<std::shared_mutex> rl(regions_mu_);
        auto it = regions_.find(store_node_id);
        if (it == regions_.end()) {
            out.result = ToEngineResult(
                ItemResult::Err(ErrorCode::INTERNAL_ERROR, false, "region gone"));
            return out;
        }
        KVRegion& re = *it->second;
        if (slot_idx < 0 || slot_idx >= re.total_blocks) {
            out.result = ToEngineResult(
                ItemResult::Err(ErrorCode::INTERNAL_ERROR, false, "slot out of range"));
            return out;
        }

        DramMetaSlot& slot = re.meta[slot_idx];
        slot.state         = static_cast<uint8_t>(BlockStatus::ALLOCATED);
        slot.ref_bit       = 1;
        slot.shard_id      = static_cast<uint16_t>(shard_id_);
        slot.store_node_id = re.store_node_id;
        // v6.4 §3.1 / §7.4: catalog INSERT writes version=1, DRAM mirror MUST
        // match so the immediate ALLOCATED→STORED CAS succeeds on either tier.
        slot.version       = 1;
        slot.store_epoch   = re.store_epoch;
        slot.block_hash    = block_hash;
        slot.lease_token   = NewLeaseToken();
        slot.lease_expire_ms = now_ms + kDefaultLeaseTtlMs;

        if (!shard_index_.TryInsert(block_hash, SlotRef{re.store_node_id, slot_idx})) {
            slot = DramMetaSlot{};
            FreeBitmapSlot(re, slot_idx);
            auto winner = shard_index_.Find(block_hash);
            if (winner.has_value()) {
                auto wit = regions_.find(winner->store_node_id);
                if (wit != regions_.end()) {
                    KVRegion& wre = *wit->second;
                    if (winner->slot_idx >= 0 && winner->slot_idx < wre.total_blocks) {
                        DramMetaSlot& ws = wre.meta[winner->slot_idx];
                        BlockLocation loc;
                        loc.store_node_id = ws.store_node_id;
                        loc.pool_offset   = wre.OffsetFromSlot(winner->slot_idx);
                        loc.store_epoch   = ws.store_epoch;
                        EngineBlockMeta meta;
                        meta.status   = static_cast<int32_t>(static_cast<BlockStatus>(ws.state));
                        meta.location = ToEngineLocation(loc);
                        meta.version  = ws.version;
                        LeaseInfo li;
                        li.lease_token     = ws.lease_token;
                        li.lease_expire_ms = ws.lease_expire_ms;
                        li.dn_epoch        = dn_epoch_;
                        li.store_epoch     = ws.store_epoch;
                        out.result = ToEngineResult(ItemResult::Ok());
                        out.row    = meta;
                        out.lease  = ToEngineLease(li);
                        out.reused_existing_allocation = true;
                        return out;
                    }
                }
            }
            out.result = ToEngineResult(
                ItemResult::Err(ErrorCode::CAS_CONFLICT, true, "concurrent allocate"));
            return out;
        }

        BlockLocation loc;
        loc.store_node_id = re.store_node_id;
        loc.pool_offset   = re.OffsetFromSlot(slot_idx);
        loc.store_epoch   = re.store_epoch;
        EngineBlockMeta meta;
        meta.status   = static_cast<int32_t>(BlockStatus::ALLOCATED);
        meta.location = ToEngineLocation(loc);
        meta.version  = 1;
        LeaseInfo li;
        li.lease_token     = slot.lease_token;
        li.lease_expire_ms = slot.lease_expire_ms;
        li.dn_epoch        = dn_epoch_;
        li.store_epoch     = re.store_epoch;
        out.result = ToEngineResult(ItemResult::Ok());
        out.row    = meta;
        out.lease  = ToEngineLease(li);
        out.reused_existing_allocation = false;
        return out;
    }

    EngineRenewLeaseResult RenewLease(const std::string& block_hash,
                                      int64_t lease_token,
                                      int64_t expected_dn_epoch,
                                      int64_t expected_store_epoch,
                                      int32_t requested_ttl_ms,
                                      int64_t now_ms) {
        EngineRenewLeaseResult out;
        // Epoch fence: stale DN epoch is always rejected.
        if (expected_dn_epoch != dn_epoch_) {
            out.result = ToEngineResult(
                ItemResult::Err(ErrorCode::STALE_EPOCH, true, "dn_epoch mismatch"));
            return out;
        }

        // v6.4 §6: renew mutates slot fields under the shard stripe exclusive lock.
        shard_index_.WithExclusiveStripe(block_hash, [&](const std::optional<SlotRef>& slot_ref) {
            if (!slot_ref.has_value()) {
                // v6.4 §11.3: no DRAM slot — do not consult catalog from RenewLease; client
                // refreshes via BatchLookupWithLease (which reads EVICTED / NOT_FOUND).
                out.result = ToEngineResult(ItemResult::Err(
                    ErrorCode::LEASE_EXPIRED,
                    true,
                    "no DRAM slot for renew; retry after BatchLookupWithLease"));
                return;
            }
            std::shared_lock<std::shared_mutex> rl(regions_mu_);
            auto it = regions_.find(slot_ref->store_node_id);
            if (it == regions_.end()) {
                out.result = ToEngineResult(
                    ItemResult::Err(ErrorCode::NOT_FOUND, false, "region gone"));
                return;
            }
            KVRegion& re = *it->second;
            if (slot_ref->slot_idx < 0 || slot_ref->slot_idx >= re.total_blocks) {
                out.result = ToEngineResult(
                    ItemResult::Err(ErrorCode::INTERNAL_ERROR, false, "slot out of range"));
                return;
            }
            DramMetaSlot& slot = re.meta[slot_ref->slot_idx];

            if (slot.store_epoch != expected_store_epoch) {
                out.result = ToEngineResult(
                    ItemResult::Err(ErrorCode::STALE_EPOCH, true, "store_epoch mismatch"));
                return;
            }
            if (slot.lease_token != lease_token) {
                out.result = ToEngineResult(
                    ItemResult::Err(ErrorCode::LEASE_TOKEN_MISMATCH, true,
                                    "lease_token mismatch"));
                return;
            }
            if (slot.lease_expire_ms > 0 && slot.lease_expire_ms <= now_ms) {
                out.result = ToEngineResult(
                    ItemResult::Err(ErrorCode::LEASE_EXPIRED, true, "lease expired"));
                return;
            }

            const int64_t ttl = requested_ttl_ms > 0 ? requested_ttl_ms : kDefaultLeaseTtlMs;
            slot.lease_expire_ms = now_ms + ttl;
            slot.ref_bit         = 1;

            LeaseInfo li;
            li.lease_token     = slot.lease_token;
            li.lease_expire_ms = slot.lease_expire_ms;
            li.dn_epoch        = dn_epoch_;
            li.store_epoch     = slot.store_epoch;
            out.result         = ToEngineResult(ItemResult::Ok());
            out.lease          = ToEngineLease(li);
        });
        return out;
    }

    KVMetadataEngine::RenewLeasePass1Outcome RenewLeasePass1(const std::string& block_hash,
                                                             int64_t lease_token,
                                                             int64_t expected_dn_epoch,
                                                             int64_t expected_store_epoch,
                                                             int32_t requested_ttl_ms,
                                                             int64_t now_ms) {
        KVMetadataEngine::RenewLeasePass1Outcome o;
        o.dram = RenewLease(block_hash,
                            lease_token,
                            expected_dn_epoch,
                            expected_store_epoch,
                            requested_ttl_ms,
                            now_ms);
        return o;
    }

    std::optional<EngineUpdateStatusResult> UpdateStatusPass1OrCatalog(
        const std::string& block_hash,
        int32_t expected_from_status,
        int32_t to_status,
        int64_t expected_version,
        const std::string& evicted_path,
        bool allow_noop_if_already_target,
        int64_t now_ms) {
        BlockStatus expected = static_cast<BlockStatus>(expected_from_status);
        BlockStatus target   = static_cast<BlockStatus>(to_status);

        if (target == BlockStatus::EVICTED && evicted_path.empty()) {
            EngineUpdateStatusResult out;
            out.result = ToEngineResult(
                ItemResult::Err(ErrorCode::INVALID_ARGUMENT, false, "evicted_path required"));
            out.new_version    = expected_version;
            out.current_status = expected_from_status;
            return out;
        }

        auto slot_ref = shard_index_.Find(block_hash);
        if (slot_ref.has_value()) {
            std::shared_lock<std::shared_mutex> rl(regions_mu_);
            auto it = regions_.find(slot_ref->store_node_id);
            if (it != regions_.end()) {
                KVRegion& re = *it->second;
                if (slot_ref->slot_idx >= 0 && slot_ref->slot_idx < re.total_blocks) {
                    DramMetaSlot& slot = re.meta[slot_ref->slot_idx];
                    BlockStatus cur_st = static_cast<BlockStatus>(slot.state);
                    if (slot.version != expected_version) {
                        EngineUpdateStatusResult out;
                        out.result = ToEngineResult(
                            ItemResult::Err(ErrorCode::CAS_CONFLICT, true, "version mismatch"));
                        out.new_version    = slot.version;
                        out.current_status = static_cast<int32_t>(cur_st);
                        return out;
                    }
                    if (cur_st != expected) {
                        if (allow_noop_if_already_target && cur_st == target) {
                            EngineUpdateStatusResult out;
                            out.result         = ToEngineResult(ItemResult::Ok());
                            out.new_version    = slot.version;
                            out.current_status = static_cast<int32_t>(cur_st);
                            return out;
                        }
                        EngineUpdateStatusResult out;
                        out.result = ToEngineResult(
                            ItemResult::Err(ErrorCode::CAS_CONFLICT, true, "status mismatch"));
                        out.new_version    = slot.version;
                        out.current_status = static_cast<int32_t>(cur_st);
                        return out;
                    }
                }
            }
        }
        (void)now_ms;
        return std::nullopt;
    }

    void ApplyUpdateStatusAfterCatalogSuccess(const std::string& block_hash,
                                              int32_t to_status,
                                              int64_t new_version,
                                              int32_t /*previous_status_in_dram*/) {
        BlockStatus target = static_cast<BlockStatus>(to_status);
        auto slot_ref = shard_index_.Find(block_hash);
        if (target == BlockStatus::EVICTED) {
            if (slot_ref.has_value()) DropSlot(*slot_ref, block_hash);
            return;
        }
        shard_index_.WithExclusiveStripe(block_hash, [&](const std::optional<SlotRef>& locked_ref) {
            if (!locked_ref.has_value()) return;
            std::shared_lock<std::shared_mutex> rl(regions_mu_);
            auto it = regions_.find(locked_ref->store_node_id);
            if (it == regions_.end()) return;
            KVRegion& re = *it->second;
            if (locked_ref->slot_idx < 0 || locked_ref->slot_idx >= re.total_blocks) return;
            DramMetaSlot& slot = re.meta[locked_ref->slot_idx];
            slot.state   = static_cast<uint8_t>(target);
            slot.version = new_version;
        });
    }

    std::optional<EngineFreeAllocatedResult> FreeAllocatedPass1OrCatalog(const std::string& block_hash,
                                                                         int64_t expected_version,
                                                                         bool force,
                                                                         int64_t /*now_ms*/) {
        auto slot_ref = shard_index_.Find(block_hash);
        if (slot_ref.has_value()) {
            std::shared_lock<std::shared_mutex> rl(regions_mu_);
            auto it = regions_.find(slot_ref->store_node_id);
            if (it != regions_.end()) {
                KVRegion& re = *it->second;
                if (slot_ref->slot_idx >= 0 && slot_ref->slot_idx < re.total_blocks) {
                    DramMetaSlot& slot = re.meta[slot_ref->slot_idx];
                    if (slot.version != expected_version) {
                        EngineFreeAllocatedResult out;
                        out.result = ToEngineResult(
                            ItemResult::Err(ErrorCode::CAS_CONFLICT, true, "version mismatch"));
                        out.new_version = slot.version;
                        return out;
                    }
                    BlockStatus st = static_cast<BlockStatus>(slot.state);
                    if (!force && st != BlockStatus::ALLOCATED && st != BlockStatus::FAILED) {
                        EngineFreeAllocatedResult out;
                        out.result = ToEngineResult(
                            ItemResult::Err(ErrorCode::INVALID_ARGUMENT, false, "row not freeable"));
                        out.new_version = slot.version;
                        return out;
                    }
                }
            }
        }
        return std::nullopt;
    }

    void ApplyFreeAfterCatalogDeleteSuccess(const std::string& block_hash,
                                            int64_t /*deleted_version*/) {
        auto slot_ref = shard_index_.Find(block_hash);
        if (slot_ref.has_value()) DropSlot(*slot_ref, block_hash);
    }

    EngineUpdateStatusResult UpdateStatus(const std::string& block_hash,
                                          int32_t expected_from_status,
                                          int32_t to_status,
                                          int64_t expected_version,
                                          const std::string& evicted_path,
                                          bool allow_noop_if_already_target,
                                          int64_t now_ms) {
        EngineUpdateStatusResult out;
        BlockStatus expected = static_cast<BlockStatus>(expected_from_status);
        BlockStatus target   = static_cast<BlockStatus>(to_status);

        if (target == BlockStatus::EVICTED && evicted_path.empty()) {
            out.result = ToEngineResult(
                ItemResult::Err(ErrorCode::INVALID_ARGUMENT, false,
                                "evicted_path required"));
            out.new_version    = expected_version;
            out.current_status = expected_from_status;
            return out;
        }

        // ── Cache pre-validation ──
        // For the EVICTED transition we need the old row to free the bitmap slot.
        // For other transitions we validate against the in-process meta.
        auto slot_ref = shard_index_.Find(block_hash);
        if (slot_ref.has_value()) {
            std::shared_lock<std::shared_mutex> rl(regions_mu_);
            auto it = regions_.find(slot_ref->store_node_id);
            if (it != regions_.end()) {
                KVRegion& re = *it->second;
                if (slot_ref->slot_idx >= 0 && slot_ref->slot_idx < re.total_blocks) {
                    DramMetaSlot& slot = re.meta[slot_ref->slot_idx];
                    BlockStatus cur_st = static_cast<BlockStatus>(slot.state);
                    if (slot.version != expected_version) {
                        out.result = ToEngineResult(
                            ItemResult::Err(ErrorCode::CAS_CONFLICT, true,
                                            "version mismatch"));
                        out.new_version    = slot.version;
                        out.current_status = static_cast<int32_t>(cur_st);
                        return out;
                    }
                    if (cur_st != expected) {
                        if (allow_noop_if_already_target && cur_st == target) {
                            out.result = ToEngineResult(ItemResult::Ok());
                            out.new_version    = slot.version;
                            out.current_status = static_cast<int32_t>(cur_st);
                            return out;
                        }
                        out.result = ToEngineResult(
                            ItemResult::Err(ErrorCode::CAS_CONFLICT, true,
                                            "status mismatch"));
                        out.new_version    = slot.version;
                        out.current_status = static_cast<int32_t>(cur_st);
                        return out;
                    }
                }
            }
        } else {
            // Not in DRAM cache: might be an EVICTED block in catalog.
            // Let the catalog CAS decide.
        }

        if (catalog_tier_ == EngineCatalogTier::REMOTE_LIBPQ) {
            out.result = ToEngineResult(
                ItemResult::Err(ErrorCode::INTERNAL_ERROR, false,
                                "catalog path required; use UpdateStatusPass1OrCatalog + libpq"));
            out.new_version    = expected_version;
            out.current_status = expected_from_status;
            return out;
        }

        // ── Catalog CAS ──
        KVRuntimeMetaCASResult cas = CASStatusUpdateMetaRow(block_hash,
                                                            expected_from_status,
                                                            to_status,
                                                            expected_version,
                                                            evicted_path,
                                                            now_ms);
        if (!cas.success) {
            out.current_status = cas.current_status;
            out.new_version    = cas.current_version;
            if (allow_noop_if_already_target &&
                cas.current_status == static_cast<int32_t>(target) &&
                cas.current_version == expected_version) {
                out.result = ToEngineResult(ItemResult::Ok());
                return out;
            }
            out.result = ToEngineResult(
                ItemResult::Err(cas.not_found ? ErrorCode::NOT_FOUND
                                              : ErrorCode::CAS_CONFLICT,
                                cas.retryable,
                                cas.not_found ? "missing" : "status/version mismatch"));
            return out;
        }

        // ── Mirror catalog result into DRAM cache ──
        if (target == BlockStatus::EVICTED) {
            // Release DRAM resources: erase hash, wipe meta, clear bitmap.
            if (slot_ref.has_value()) DropSlot(*slot_ref, block_hash);
        } else {
            // Update status mirror in meta slot (v6.4 §6: under exclusive stripe lock).
            shard_index_.WithExclusiveStripe(block_hash, [&](const std::optional<SlotRef>& locked_ref) {
                if (!locked_ref.has_value()) return;
                std::shared_lock<std::shared_mutex> rl(regions_mu_);
                auto it = regions_.find(locked_ref->store_node_id);
                if (it == regions_.end()) return;
                KVRegion& re = *it->second;
                if (locked_ref->slot_idx < 0 || locked_ref->slot_idx >= re.total_blocks) return;
                DramMetaSlot& slot = re.meta[locked_ref->slot_idx];
                slot.state   = static_cast<uint8_t>(target);
                slot.version = expected_version + 1;
            });
        }

        out.result         = ToEngineResult(ItemResult::Ok());
        out.new_version    = expected_version + 1;
        out.current_status = static_cast<int32_t>(target);
        return out;
    }

    EngineFreeAllocatedResult FreeAllocated(const std::string& block_hash,
                                             int64_t expected_version,
                                             bool force,
                                             int64_t /*now_ms*/) {
        EngineFreeAllocatedResult out;

        // ── Cache pre-validation ──
        auto slot_ref = shard_index_.Find(block_hash);
        if (slot_ref.has_value()) {
            std::shared_lock<std::shared_mutex> rl(regions_mu_);
            auto it = regions_.find(slot_ref->store_node_id);
            if (it != regions_.end()) {
                KVRegion& re = *it->second;
                if (slot_ref->slot_idx >= 0 && slot_ref->slot_idx < re.total_blocks) {
                    DramMetaSlot& slot = re.meta[slot_ref->slot_idx];
                    if (slot.version != expected_version) {
                        out.result = ToEngineResult(
                            ItemResult::Err(ErrorCode::CAS_CONFLICT, true,
                                            "version mismatch"));
                        out.new_version = slot.version;
                        return out;
                    }
                    BlockStatus st = static_cast<BlockStatus>(slot.state);
                    if (!force &&
                        st != BlockStatus::ALLOCATED &&
                        st != BlockStatus::FAILED) {
                        out.result = ToEngineResult(
                            ItemResult::Err(ErrorCode::INVALID_ARGUMENT, false,
                                            "row not freeable"));
                        out.new_version = slot.version;
                        return out;
                    }
                }
            }
        }

        if (catalog_tier_ == EngineCatalogTier::REMOTE_LIBPQ) {
            out.result = ToEngineResult(
                ItemResult::Err(ErrorCode::INTERNAL_ERROR, false,
                                "catalog path required; use FreeAllocatedPass1OrCatalog + libpq"));
            out.new_version = expected_version;
            return out;
        }

        // ── Catalog delete ──
        if (!DeleteMetaRow(block_hash, expected_version)) {
            out.result = ToEngineResult(
                ItemResult::Err(ErrorCode::CAS_CONFLICT, true, "delete conflict"));
            return out;
        }

        // ── Release DRAM slot ──
        if (slot_ref.has_value()) DropSlot(*slot_ref, block_hash);

        out.result      = ToEngineResult(ItemResult::Ok());
        out.new_version = expected_version + 1;
        return out;
    }

    std::vector<std::string> ColdCandidates(std::size_t limit, int64_t /*now_ms*/) const {
        if (limit == 0) return {};
        std::vector<std::string> result;
        result.reserve(limit);

        std::shared_lock<std::shared_mutex> rl(regions_mu_);
        for (auto& kv : regions_) {
            const KVRegion& re = *kv.second;
            if (re.total_blocks == 0 || !re.meta) continue;
            const int64_t n = re.total_blocks;
            int64_t start = static_cast<int64_t>(
                re.clock_hand.load(std::memory_order_relaxed)) % n;
            for (int64_t i = 0; i < n && result.size() < limit; ++i) {
                int64_t idx = (start + i) % n;
                const DramMetaSlot& slot = re.meta[idx];
                if (slot.state ==
                        static_cast<uint8_t>(BlockStatus::STORED) &&
                    !slot.block_hash.empty()) {
                    result.push_back(slot.block_hash);
                }
            }
            if (!result.empty()) {
                re.clock_hand.store(
                    static_cast<uint64_t>(
                        (start + static_cast<int64_t>(result.size())) % n),
                    std::memory_order_relaxed);
            }
            if (result.size() >= limit) break;
        }
        return result;
    }

    bool CanEvict(const std::string& block_hash, int64_t now_ms) const {
        auto slot_ref = shard_index_.Find(block_hash);
        if (!slot_ref.has_value()) return false;
        std::shared_lock<std::shared_mutex> rl(regions_mu_);
        auto it = regions_.find(slot_ref->store_node_id);
        if (it == regions_.end()) return false;
        const KVRegion& re = *it->second;
        if (slot_ref->slot_idx < 0 || slot_ref->slot_idx >= re.total_blocks) return false;
        const DramMetaSlot& slot = re.meta[slot_ref->slot_idx];
        if (slot.lease_token == 0) return true;
        return slot.lease_expire_ms <= now_ms;
    }

    // v6 §14.1 low-watermark probe. Returns 1.0 (no pressure) when no regions
    // are registered.
    double MinRegionFreeRatio() const {
        std::shared_lock<std::shared_mutex> rl(regions_mu_);
        double min_ratio = 1.0;
        bool any = false;
        for (const auto& kv : regions_) {
            const KVRegion& re = *kv.second;
            if (re.total_blocks <= 0) continue;
            const int64_t free_blocks = re.free_blocks.load(std::memory_order_relaxed);
            const double ratio = static_cast<double>(free_blocks) /
                                 static_cast<double>(re.total_blocks);
            if (!any || ratio < min_ratio) {
                min_ratio = ratio;
                any = true;
            }
        }
        return any ? min_ratio : 1.0;
    }

private:
    // ── Internal row type ────────────────────────────────────────────────────

    struct Row {
        BlockStatus   status = BlockStatus::UNSPECIFIED;
        BlockLocation location;
        int64_t       version = 0;
        int64_t       updated_at_ms = 0;
    };

    // ── Bitmap helpers ───────────────────────────────────────────────────────

    struct BitmapSlotResult {
        bool    ok{false};
        int64_t slot_idx{0};
        int64_t pool_offset{0};
    };

    static BitmapSlotResult AllocateBitmapSlot(KVRegion& re) {
        BitmapSlotResult out;
        if (re.free_blocks.load(std::memory_order_relaxed) <= 0) return out;
        const int64_t n_words = static_cast<int64_t>(re.bitmap_words.size());
        if (n_words == 0) return out;

        int64_t hint       = static_cast<int64_t>(re.alloc_hint.load(std::memory_order_relaxed));
        int64_t start_word = (hint / 64) % n_words;

        for (int64_t w = 0; w < n_words; ++w) {
            int64_t word_idx = (start_word + w) % n_words;
            while (true) {
                uint64_t word = re.bitmap_words[word_idx].load(std::memory_order_acquire);
                if (word == ~0ULL) break;  // all occupied in this word
                int      bit_pos = __builtin_ctzll(~word);
                uint64_t bit     = 1ULL << bit_pos;
                uint64_t prev    = re.bitmap_words[word_idx].fetch_or(bit,
                                       std::memory_order_acq_rel);
                if (prev & bit) continue;  // race; retry with fresh word value
                int64_t slot_idx = word_idx * 64 + bit_pos;
                if (slot_idx >= re.total_blocks) {
                    re.bitmap_words[word_idx].fetch_and(~bit,
                        std::memory_order_acq_rel);
                    return out;  // past end of region
                }
                re.free_blocks.fetch_sub(1, std::memory_order_relaxed);
                re.alloc_hint.store(static_cast<uint64_t>(slot_idx + 1),
                                    std::memory_order_relaxed);
                out.ok         = true;
                out.slot_idx   = slot_idx;
                out.pool_offset = re.OffsetFromSlot(slot_idx);
                return out;
            }
        }
        return out;
    }

    static void FreeBitmapSlot(KVRegion& re, int64_t slot_idx) {
        if (slot_idx < 0 || slot_idx >= re.total_blocks) return;
        const int64_t word_idx = slot_idx / 64;
        const uint64_t bit    = 1ULL << (slot_idx % 64);
        uint64_t prev = re.bitmap_words[word_idx].fetch_and(~bit,
                            std::memory_order_acq_rel);
        if (prev & bit)  // was occupied → now freed
            re.free_blocks.fetch_add(1, std::memory_order_relaxed);
    }

    // Try to mark a bit occupied; returns true if newly set.
    static bool TryMarkBitOccupied(KVRegion& re, int64_t slot_idx) {
        if (slot_idx < 0 || slot_idx >= re.total_blocks) return false;
        const int64_t word_idx = slot_idx / 64;
        const uint64_t bit    = 1ULL << (slot_idx % 64);
        uint64_t prev = re.bitmap_words[word_idx].fetch_or(bit,
                            std::memory_order_acq_rel);
        if (!(prev & bit)) {
            re.free_blocks.fetch_sub(1, std::memory_order_relaxed);
            return true;
        }
        return false;
    }

    // ── DropSlot: ordered release of cache state (§7.6) ─────────────────────
    // 1. Erase hash index entry.
    // 2. Wipe meta slot.
    // 3. Clear bitmap bit.
    // 4. Increment free_blocks.

    void DropSlot(const SlotRef& ref, const std::string& block_hash) {
        // Step 1: erase hash index (under exclusive stripe lock).
        shard_index_.Erase(block_hash);

        // Steps 2-4: wipe meta + release bitmap.
        std::shared_lock<std::shared_mutex> rl(regions_mu_);
        auto it = regions_.find(ref.store_node_id);
        if (it == regions_.end()) return;
        KVRegion& re = *it->second;
        if (ref.slot_idx < 0 || ref.slot_idx >= re.total_blocks || !re.meta) return;
        re.meta[ref.slot_idx] = DramMetaSlot{};   // step 2: wipe
        FreeBitmapSlot(re, ref.slot_idx);           // steps 3+4
    }

    // ── Lease helpers ────────────────────────────────────────────────────────

    int64_t NewLeaseToken() {
        return next_lease_token_.fetch_add(1, std::memory_order_relaxed);
    }

    void GrantOrExtendLeaseInSlot(DramMetaSlot& slot, int64_t now_ms) {
        const int64_t expire = now_ms + kDefaultLeaseTtlMs;
        if (slot.lease_token == 0 || slot.lease_expire_ms <= now_ms) {
            slot.lease_token    = NewLeaseToken();
            slot.lease_expire_ms = expire;
        } else {
            slot.lease_expire_ms = expire;
        }
        slot.ref_bit = 1;
    }

    // ── Region selection (affinity policy §5.3) ──────────────────────────────

    std::vector<KVRegion*> PickRegions(int32_t preferred_store_id,
                                       bool allow_fallback_store) {
        std::shared_lock<std::shared_mutex> rl(regions_mu_);
        if (preferred_store_id == 0) {
            std::vector<KVRegion*> healthy;
            for (auto& kv : regions_) {
                if (kv.second->state != EngineRegionState::HEALTHY) continue;
                healthy.push_back(kv.second.get());
            }
            std::sort(healthy.begin(), healthy.end(),
                      [](KVRegion* a, KVRegion* b) {
                          return a->free_blocks.load(std::memory_order_relaxed) >
                                 b->free_blocks.load(std::memory_order_relaxed);
                      });
            return healthy;
        }
        std::vector<KVRegion*> picks;
        auto it = regions_.find(preferred_store_id);
        if (it != regions_.end() && it->second->state == EngineRegionState::HEALTHY)
            picks.push_back(it->second.get());
        if (!allow_fallback_store) return picks;
        std::vector<KVRegion*> fallbacks;
        for (auto& kv : regions_) {
            if (kv.first == preferred_store_id) continue;
            if (kv.second->state != EngineRegionState::HEALTHY) continue;
            fallbacks.push_back(kv.second.get());
        }
        std::sort(fallbacks.begin(), fallbacks.end(),
                  [](KVRegion* a, KVRegion* b) {
                      return a->free_blocks.load(std::memory_order_relaxed) >
                             b->free_blocks.load(std::memory_order_relaxed);
                  });
        for (auto* r : fallbacks) picks.push_back(r);
        return picks;
    }

    // ── Catalog bridge (LOCAL_FALLBACK only; v6.4 REMOTE_LIBPQ uses libpq) ───

    bool LookupMetaRow(const std::string& block_hash, Row* out) {
        if (catalog_tier_ == EngineCatalogTier::REMOTE_LIBPQ) {
            assert(false && "in-process LookupMetaRow forbidden in REMOTE_LIBPQ");
            return false;
        }
        AccessorRow arow;
        if (!fallback_meta_.Lookup(shard_id_, block_hash, &arow)) return false;
        out->status            = static_cast<BlockStatus>(arow.status);
        out->location.store_node_id = arow.store_node_id;
        out->location.pool_offset   = arow.pool_offset;
        out->location.evicted_path  = arow.evicted_path;
        out->location.store_epoch   = LookupStoreEpoch(arow.store_node_id);
        out->version               = arow.version;
        out->updated_at_ms         = arow.updated_at_ms;
        return true;
    }

    bool InsertAllocatedMetaRow(const std::string& block_hash,
                                int32_t store_node_id, int64_t pool_offset,
                                int64_t now_ms) {
        if (catalog_tier_ == EngineCatalogTier::REMOTE_LIBPQ) {
            assert(false && "in-process InsertAllocatedMetaRow forbidden in REMOTE_LIBPQ");
            return false;
        }
        AccessorInsertSpec spec;
        spec.store_node_id = store_node_id;
        spec.pool_offset   = pool_offset;
        return fallback_meta_.InsertAllocated(shard_id_, block_hash, spec, now_ms);
    }

    KVRuntimeMetaCASResult CASStatusUpdateMetaRow(const std::string& block_hash,
                                                   int32_t expected_from_status,
                                                   int32_t to_status,
                                                   int64_t expected_version,
                                                   const std::string& evicted_path,
                                                   int64_t now_ms) {
        if (catalog_tier_ == EngineCatalogTier::REMOTE_LIBPQ) {
            assert(false && "in-process CASStatusUpdateMetaRow forbidden in REMOTE_LIBPQ");
            KVRuntimeMetaCASResult bad{};
            return bad;
        }
        AccessorCASResult cas = fallback_meta_.CASStatusUpdate(
            shard_id_, block_hash, expected_from_status, to_status,
            expected_version, evicted_path, now_ms);
        KVRuntimeMetaCASResult out;
        out.success        = cas.success;
        out.not_found      = cas.not_found;
        out.conflict       = cas.conflict;
        out.retryable      = cas.retryable;
        out.current_status = cas.current_status;
        out.current_version = cas.current_version;
        return out;
    }

    bool DeleteMetaRow(const std::string& block_hash, int64_t expected_version) {
        if (catalog_tier_ == EngineCatalogTier::REMOTE_LIBPQ) {
            assert(false && "in-process DeleteMetaRow forbidden in REMOTE_LIBPQ");
            return false;
        }
        bool conflict = false;
        return fallback_meta_.Delete(shard_id_, block_hash, expected_version, &conflict)
               && !conflict;
    }

    int64_t LookupStoreEpoch(int32_t store_node_id) const {
        auto it = regions_.find(store_node_id);
        return it != regions_.end() ? it->second->store_epoch : 0;
    }

    // ── RegisterStoreRegion (internal) ───────────────────────────────────────

    EngineResultMeta RegisterStoreRegionInternal(const EngineStoreRegion& region) {
        bool clear_runtime_for_restart = false;
        {
            std::unique_lock<std::shared_mutex> lk(regions_mu_);
            auto it = regions_.find(region.store_node_id);
            if (it != regions_.end()) {
                KVRegion& existing = *it->second;
                if (existing.base_offset == region.base_offset &&
                    existing.region_bytes == region.region_bytes &&
                    existing.block_size == region.block_size) {
                    // Same geometry: keep the KVRegion allocation stable because
                    // shard_index_ stores SlotRefs into meta[]. If the Store epoch
                    // advances, the Store's DRAM contents are gone; quarantine the
                    // region, clear stale runtime slots outside regions_mu_, then
                    // publish the new epoch.
                    if (region.store_epoch < existing.store_epoch) {
                        return ToEngineResult(ItemResult::Err(ErrorCode::STALE_EPOCH, true,
                                                              "stale store epoch"));
                    }
                    clear_runtime_for_restart = region.store_epoch > existing.store_epoch;
                    if (clear_runtime_for_restart) {
                        existing.state = EngineRegionState::QUARANTINED;
                    } else {
                        existing.store_epoch = region.store_epoch;
                        existing.state = EngineRegionState::HEALTHY;
                        return ToEngineResult(ItemResult::Ok());
                    }
                } else {
                    regions_[region.store_node_id] = std::make_unique<KVRegion>(
                        region.store_node_id, dn_id_, region.base_offset,
                        region.region_bytes, block_size_, region.store_epoch);
                    return ToEngineResult(ItemResult::Ok());
                }
            } else {
                regions_[region.store_node_id] = std::make_unique<KVRegion>(
                    region.store_node_id, dn_id_, region.base_offset,
                    region.region_bytes, block_size_, region.store_epoch);
                return ToEngineResult(ItemResult::Ok());
            }
        }

        if (clear_runtime_for_restart) {
            ClearRuntimeForStoreRestart(region.store_node_id);
            std::unique_lock<std::shared_mutex> lk(regions_mu_);
            auto it = regions_.find(region.store_node_id);
            if (it == regions_.end()) {
                return ToEngineResult(ItemResult::Err(ErrorCode::INTERNAL_ERROR, true,
                                                      "region disappeared during restart"));
            }
            it->second->store_epoch = region.store_epoch;
            it->second->state = EngineRegionState::HEALTHY;
        }
        return ToEngineResult(ItemResult::Ok());
    }

    // ── Row ↔ EngineBlockMeta conversion ────────────────────────────────────

    static EngineBlockMeta ToEngineMeta(const Row& row) {
        EngineBlockMeta out;
        out.status   = static_cast<int32_t>(row.status);
        out.location = ToEngineLocation(row.location);
        out.version  = row.version;
        return out;
    }

    // ── Private state ────────────────────────────────────────────────────────

    int32_t dn_id_;
    int32_t block_size_;
    int64_t dn_epoch_;
    int32_t shard_id_;
    std::atomic<int64_t> next_lease_token_;
    LeaseManager lease_manager_;   // in-process lease table for DRAM-resident slots

    std::optional<KVShmemRuntimeOps> shmem_ops_;
    mutable InMemoryKVMetaTableAccessor fallback_meta_;

    mutable std::shared_mutex regions_mu_;
    std::unordered_map<int32_t, std::unique_ptr<KVRegion>> regions_;
    std::mutex pending_restore_mu_;
    std::unordered_map<int32_t, std::vector<PendingRestoreRow>> pending_restores_by_store_;

    mutable ShardHashIndex shard_index_;

    // When false (DN BackgroundPoolManager), Lookup() does not consult in-process catalog hooks
    // after a DRAM miss — only `BatchLookupWithLeaseSplitForPoolWorker` + libpq may read catalog.
    bool allow_inline_meta_lookup_{true};
    EngineCatalogTier catalog_tier_{EngineCatalogTier::LOCAL_FALLBACK};
};

// ──────────────────────────────────────────────────────────────────────────────
// Public KVMetadataEngine forwarding wrappers
// ──────────────────────────────────────────────────────────────────────────────

KVMetadataEngine::KVMetadataEngine(int32_t store_node_id,
                                   int32_t dn_id,
                                   int64_t region_bytes,
                                   int32_t block_size,
                                   int64_t dn_epoch,
                                   int64_t store_epoch,
                                   int32_t kvblock_shard_id)
    : impl_(std::make_unique<Impl>(
          store_node_id, dn_id, region_bytes, block_size, dn_epoch, store_epoch,
          kvblock_shard_id)) {}

KVMetadataEngine::~KVMetadataEngine()                                    = default;
KVMetadataEngine::KVMetadataEngine(KVMetadataEngine&&) noexcept          = default;
KVMetadataEngine& KVMetadataEngine::operator=(KVMetadataEngine&&) noexcept = default;

int32_t KVMetadataEngine::DnId()       const { return impl_->DnId(); }
int64_t KVMetadataEngine::DnEpoch()    const { return impl_->DnEpoch(); }
int32_t KVMetadataEngine::BlockSize()  const { return impl_->BlockSize(); }

EngineResultMeta KVMetadataEngine::RegisterStoreRegion(const EngineStoreRegion& r) {
    return impl_->RegisterStoreRegion(r);
}
EngineResultMeta KVMetadataEngine::SetRegionState(int32_t sid, EngineRegionState st) {
    return impl_->SetRegionState(sid, st);
}
bool KVMetadataEngine::HasRegion(int32_t sid) const {
    return impl_->HasRegion(sid);
}
int64_t KVMetadataEngine::RegionStoreEpoch(int32_t sid) const {
    return impl_->RegionStoreEpoch(sid);
}
EngineResultMeta KVMetadataEngine::MarkBitmapOccupied(int32_t sid, int64_t offset) {
    return impl_->MarkBitmapOccupied(sid, offset);
}
EngineResultMeta KVMetadataEngine::RestoreRow(const std::string& block_hash,
                                              int32_t status,
                                              const EngineBlockLocation& location,
                                              int64_t version,
                                              int64_t now_ms) {
    return impl_->RestoreRow(block_hash, status, location, version, now_ms);
}
int64_t KVMetadataEngine::BumpDnEpoch() { return impl_->BumpDnEpoch(); }

void KVMetadataEngine::SetDnEpochFromCatalog(int64_t epoch) {
    impl_->SetDnEpochFromCatalog(epoch);
}

EngineLookupResult KVMetadataEngine::Lookup(const std::string& block_hash,
                                            bool renew_lease_on_hit,
                                            int64_t now_ms) {
    return impl_->Lookup(block_hash, renew_lease_on_hit, now_ms);
}
EngineLookupResult KVMetadataEngine::LookupDramCacheOnly(const std::string& block_hash,
                                                         bool renew_lease_on_hit,
                                                         int64_t now_ms) {
    return impl_->LookupDramCacheOnly(block_hash, renew_lease_on_hit, now_ms);
}
EngineAllocateResult KVMetadataEngine::Allocate(const std::string& block_hash,
                                                int32_t block_size,
                                                int64_t now_ms,
                                                int32_t preferred_store_id,
                                                bool allow_fallback_store) {
    return impl_->Allocate(block_hash, block_size, now_ms,
                           preferred_store_id, allow_fallback_store);
}
EngineRenewLeaseResult KVMetadataEngine::RenewLease(const std::string& block_hash,
                                                    int64_t lease_token,
                                                    int64_t expected_dn_epoch,
                                                    int64_t expected_store_epoch,
                                                    int32_t requested_ttl_ms,
                                                    int64_t now_ms) {
    return impl_->RenewLease(block_hash, lease_token, expected_dn_epoch,
                             expected_store_epoch, requested_ttl_ms, now_ms);
}
EngineUpdateStatusResult KVMetadataEngine::UpdateStatus(const std::string& block_hash,
                                                        int32_t expected_from_status,
                                                        int32_t to_status,
                                                        int64_t expected_version,
                                                        const std::string& evicted_path,
                                                        bool allow_noop_if_already_target,
                                                        int64_t now_ms) {
    return impl_->UpdateStatus(block_hash, expected_from_status, to_status,
                               expected_version, evicted_path,
                               allow_noop_if_already_target, now_ms);
}
EngineFreeAllocatedResult KVMetadataEngine::FreeAllocated(const std::string& block_hash,
                                                          int64_t expected_version,
                                                          bool force,
                                                          int64_t now_ms) {
    return impl_->FreeAllocated(block_hash, expected_version, force, now_ms);
}
std::vector<std::string> KVMetadataEngine::ColdCandidates(std::size_t limit,
                                                          int64_t now_ms) const {
    return impl_->ColdCandidates(limit, now_ms);
}
bool KVMetadataEngine::CanEvict(const std::string& block_hash, int64_t now_ms) const {
    return impl_->CanEvict(block_hash, now_ms);
}
double KVMetadataEngine::MinRegionFreeRatio() const {
    return impl_->MinRegionFreeRatio();
}

void KVMetadataEngine::SetAllowInlineMetaTableLookup(bool allow) {
    impl_->SetAllowInlineMetaTableLookup(allow);
}
bool KVMetadataEngine::AllowInlineMetaTableLookup() const {
    return impl_->AllowInlineMetaTableLookup();
}

void KVMetadataEngine::SetCatalogTier(EngineCatalogTier tier) {
    impl_->SetCatalogTier(tier);
}

EngineCatalogTier KVMetadataEngine::CatalogTier() const {
    return impl_->CatalogTier();
}

EngineAllocatePass1Outcome KVMetadataEngine::AllocatePass1ReserveBitmap(
    const std::string& block_hash,
    int32_t block_size,
    int64_t now_ms,
    int32_t preferred_store_id,
    bool allow_fallback_store) {
    return impl_->AllocatePass1ReserveBitmap(block_hash, block_size, now_ms,
                                               preferred_store_id, allow_fallback_store);
}
void KVMetadataEngine::RollbackAllocatePass1Reservation(int32_t store_node_id, int64_t slot_idx) {
    impl_->RollbackAllocatePass1Reservation(store_node_id, slot_idx);
}
EngineAllocateResult KVMetadataEngine::CommitAllocatePass1AfterCatalogInsert(
    const std::string& block_hash,
    int32_t store_node_id,
    int64_t slot_idx,
    int64_t now_ms) {
    return impl_->CommitAllocatePass1AfterCatalogInsert(block_hash, store_node_id, slot_idx, now_ms);
}

KVMetadataEngine::RenewLeasePass1Outcome KVMetadataEngine::RenewLeasePass1(
    const std::string& block_hash,
    int64_t lease_token,
    int64_t expected_dn_epoch,
    int64_t expected_store_epoch,
    int32_t requested_ttl_ms,
    int64_t now_ms) {
    return impl_->RenewLeasePass1(block_hash,
                                  lease_token,
                                  expected_dn_epoch,
                                  expected_store_epoch,
                                  requested_ttl_ms,
                                  now_ms);
}
std::optional<EngineUpdateStatusResult> KVMetadataEngine::UpdateStatusPass1OrCatalog(
    const std::string& block_hash,
    int32_t expected_from_status,
    int32_t to_status,
    int64_t expected_version,
    const std::string& evicted_path,
    bool allow_noop_if_already_target,
    int64_t now_ms) {
    return impl_->UpdateStatusPass1OrCatalog(block_hash,
                                             expected_from_status,
                                             to_status,
                                             expected_version,
                                             evicted_path,
                                             allow_noop_if_already_target,
                                             now_ms);
}
void KVMetadataEngine::ApplyUpdateStatusAfterCatalogSuccess(const std::string& block_hash,
                                                            int32_t to_status,
                                                            int64_t new_version,
                                                            int32_t previous_status_in_dram) {
    impl_->ApplyUpdateStatusAfterCatalogSuccess(block_hash, to_status, new_version,
                                                previous_status_in_dram);
}

std::optional<EngineFreeAllocatedResult> KVMetadataEngine::FreeAllocatedPass1OrCatalog(
    const std::string& block_hash,
    int64_t expected_version,
    bool force,
    int64_t now_ms) {
    return impl_->FreeAllocatedPass1OrCatalog(block_hash, expected_version, force, now_ms);
}
void KVMetadataEngine::ApplyFreeAfterCatalogDeleteSuccess(const std::string& block_hash,
                                                          int64_t deleted_version) {
    impl_->ApplyFreeAfterCatalogDeleteSuccess(block_hash, deleted_version);
}

}  // namespace falconfs::kv
