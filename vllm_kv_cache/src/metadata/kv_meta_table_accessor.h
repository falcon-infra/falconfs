// Abstraction that mirrors v6 §4.2 `KVMetaTableAccessor`. Provides the row-
// level operations the metadata engine needs against `falcon_kvblock_table`,
// without coupling to the PG headers. The PG-backed implementation lives in
// the falcon extension and forwards to the C functions in
// `falcon/metadb/kvblock_accessor.c`. The in-memory implementation supplied
// here is for unit tests.
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace falconfs::kv {

struct AccessorRow {
    int32_t kv_group_idx = 0;
    int32_t layer_mask = 0;
    int32_t status = 0;       // BlockStatus int (1=ALLOCATED, 2=STORED, ...)
    int32_t store_node_id = 0;
    int64_t pool_offset = 0;
    std::string evicted_path;
    int64_t version = 0;
    int64_t updated_at_ms = 0;
};

struct AccessorInsertSpec {
    int32_t kv_group_idx = 0;
    int32_t layer_mask = 0;
    int32_t store_node_id = 0;
    int64_t pool_offset = 0;
};

struct AccessorCASResult {
    bool success = false;
    bool not_found = false;
    bool conflict = false;
    bool retryable = false;
    int32_t current_status = 0;
    int64_t current_version = 0;
};

class IKVMetaTableAccessor {
public:
    virtual ~IKVMetaTableAccessor() = default;

    virtual bool Lookup(int32_t shard_id, const std::string& block_hash, AccessorRow* out) = 0;
    virtual bool InsertAllocated(int32_t shard_id,
                                 const std::string& block_hash,
                                 const AccessorInsertSpec& spec,
                                 int64_t now_ms) = 0;
    virtual AccessorCASResult CASStatusUpdate(int32_t shard_id,
                                              const std::string& block_hash,
                                              int32_t expected_from_status,
                                              int32_t to_status,
                                              int64_t expected_version,
                                              const std::string& evicted_path,
                                              int64_t now_ms) = 0;
    virtual bool Delete(int32_t shard_id,
                        const std::string& block_hash,
                        int64_t expected_version,
                        bool* out_conflict) = 0;

    // Recovery scan: invoke `cb` for every row whose status is in
    // `wanted_status` (treated as a set). Implementations decide ordering.
    using RowCallback = std::function<void(const std::string& /*block_hash*/, const AccessorRow& /*row*/)>;
    virtual void ScanForRecovery(int32_t shard_id,
                                 const std::vector<int32_t>& wanted_status,
                                 const RowCallback& cb) = 0;

    // Persisted DN epoch row access (v6 recovery fencing). `LoadDnEpoch`
    // returns 1 when absent, and `BumpDnEpoch` atomically increments and
    // returns the new epoch.
    virtual int64_t LoadDnEpoch(int32_t shard_id) = 0;
    virtual int64_t BumpDnEpoch(int32_t shard_id) = 0;

    // Recovery/test-only upsert used to seed metadata rows with explicit
    // status/version values. PG-backed paths should use normal insert/CAS APIs.
    virtual bool UpsertForRecovery(int32_t shard_id,
                                   const std::string& block_hash,
                                   const AccessorRow& row) = 0;
};

// Thread-safe in-memory implementation backed by a per-shard hashmap. Used
// by gtests and the reference flow.
class InMemoryKVMetaTableAccessor : public IKVMetaTableAccessor {
public:
    bool Lookup(int32_t shard_id, const std::string& block_hash, AccessorRow* out) override;
    bool InsertAllocated(int32_t shard_id,
                         const std::string& block_hash,
                         const AccessorInsertSpec& spec,
                         int64_t now_ms) override;
    AccessorCASResult CASStatusUpdate(int32_t shard_id,
                                      const std::string& block_hash,
                                      int32_t expected_from_status,
                                      int32_t to_status,
                                      int64_t expected_version,
                                      const std::string& evicted_path,
                                      int64_t now_ms) override;
    bool Delete(int32_t shard_id,
                const std::string& block_hash,
                int64_t expected_version,
                bool* out_conflict) override;
    void ScanForRecovery(int32_t shard_id,
                         const std::vector<int32_t>& wanted_status,
                         const RowCallback& cb) override;
    int64_t LoadDnEpoch(int32_t shard_id) override;
    int64_t BumpDnEpoch(int32_t shard_id) override;
    bool UpsertForRecovery(int32_t shard_id,
                           const std::string& block_hash,
                           const AccessorRow& row) override;

private:
    using ShardMap = std::unordered_map<std::string, AccessorRow>;
    mutable std::mutex mu_;
    std::unordered_map<int32_t, ShardMap> shards_;
    std::unordered_map<int32_t, int64_t> dn_epochs_;
};

}  // namespace falconfs::kv
