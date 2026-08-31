// v6.5 §12.6 client-side KV store facade — registry + local SHM / remote BRPC
// facades (P4). `KVStoreFacadeRegistry` polls membership from CN libpq,
// optionally LISTEN/NOTIFY when `FALCON_KV_MEMBERSHIP_NOTIFY_ENABLED=1`, and
// resolves `store_node_id` to `LocalKVStoreShmFacade` or `RemoteKVStoreFacade`.
//
// Tunables (env, mirroring planned GUCs on clients):
//   FALCON_KV_MEMBERSHIP_REFRESH_PERIOD_MS (default 5000)
//   FALCON_KV_MEMBERSHIP_REFRESH_MIN_INTERVAL_MS (default 1000)
//   FALCON_KV_MEMBERSHIP_NOTIFY_ENABLED (0/1, default 0)
//   FALCON_KV_FACADE_BRPC_TIMEOUT_MS (default 30000)
//
// Cross-references:
//   - Membership refresh model and three triggers: design §3.4.7.
//   - Diff-and-apply table: design §3.4.7.4.
//   - Local fast-path semantics: design §12.6.3.
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

// Forward-declare the proto request/response types so this header doesn't
// drag the protobuf headers into anybody who only needs the resolver type.
namespace falconfs::kv {

class BatchWriteBlockRequest;
class BatchWriteBlockResponse;
class BatchReadBlockRequest;
class BatchReadBlockResponse;
class BatchReadFromSSDRequest;
class BatchReadFromSSDResponse;
class WriteBlockRequest;
class WriteBlockResponse;
class ReadBlockRequest;
class ReadBlockResponse;
class ReadFromSSDRequest;
class ReadFromSSDResponse;

// Trip kind reported by RPC call sites that observed a transport-level
// failure. The refresh loop uses this to decide whether to refresh the DN
// list, the Store list, or both.
enum class RpcFailureKind : uint8_t {
    Unknown = 0,
    Unavailable,           // brpc UNAVAILABLE / connection refused / RST.
    BackendDown,           // observed via channel ping or BRPC error.
    StaleEpoch,            // server returned ErrorCode::STALE_EPOCH.
    StoreNotRegistered,    // DN told us to forget this store_node_id.
    NotFoundAtDn,          // shard route stale; refresh shard table.
};

struct KVStoreReadPayloadView {
    bool ok = false;
    const char* data = nullptr;
    size_t size = 0;
    std::shared_ptr<void> owner;
    std::string owned_payload;
    std::string kind;
};

struct KVStoreWritePayloadView {
    const char* data = nullptr;
    size_t size = 0;
};

// A pluggable callable that performs the byte-level work for a single Store.
// Two concrete implementations (P4):
//   - LocalKVStoreShmFacade: shm_open + mmap(MAP_SHARED) into the
//     falcon_kv_store daemon's DRAM segment.
//   - RemoteKVStoreFacade:   pooled brpc::Channel against the daemon's
//     KVDataService BRPC server.
//
// Both have identical observable surface modulo latency. A third
// "Unhealthy" instance is returned by Resolve() when the row is
// healthy=false (per §3.4.5): it short-circuits reads to cache-miss and
// refuses writes.
class IKVStoreFacade {
public:
    virtual ~IKVStoreFacade() = default;
    virtual int32_t StoreNodeId() const = 0;
    virtual bool    IsLocal()      const = 0;
    virtual bool    IsHealthy()    const = 0;

    virtual void WriteBlock(const WriteBlockRequest&, WriteBlockResponse*)     = 0;
    virtual void WriteBlockPayload(const WriteBlockRequest&,
                                   const char* payload,
                                   size_t payload_len,
                                   WriteBlockResponse*) = 0;
    virtual void BatchWriteBlockPayloads(const BatchWriteBlockRequest&,
                                         const std::vector<std::string>& payloads,
                                         BatchWriteBlockResponse*) = 0;
    virtual void BatchWriteBlockPayloadViews(const BatchWriteBlockRequest&,
                                            const std::vector<KVStoreWritePayloadView>& payloads,
                                            BatchWriteBlockResponse*);
    virtual void ReadBlock(const ReadBlockRequest&, ReadBlockResponse*)       = 0;
    virtual void ReadBlockPayload(const ReadBlockRequest&,
                                  ReadBlockResponse*,
                                  std::string* payload) = 0;
    virtual void BatchReadBlockPayloads(const BatchReadBlockRequest&,
                                        BatchReadBlockResponse*,
                                        std::vector<std::string>* payloads) = 0;
    virtual void BatchReadBlockPayloadViews(const BatchReadBlockRequest& req,
                                            BatchReadBlockResponse* resp,
                                            std::vector<KVStoreReadPayloadView>* views);
    virtual void ReadFromSSD(const ReadFromSSDRequest&, ReadFromSSDResponse*) = 0;

    virtual void BatchWriteBlock (const BatchWriteBlockRequest&,
                                  BatchWriteBlockResponse*)  = 0;
    virtual void BatchReadBlock  (const BatchReadBlockRequest&,
                                  BatchReadBlockResponse*)   = 0;
    virtual void BatchReadFromSSD(const BatchReadFromSSDRequest&,
                                  BatchReadFromSSDResponse*) = 0;
};

// Implementation-defined handle for a same-host SHM mapping (v6.5 P4).
struct LocalShmHandle {
    int     fd          = -1;
    void*   base        = nullptr;
    size_t  len         = 0;
    int32_t block_size  = 65536;
};

// Result returned by RefreshNow().
struct RefreshResult {
    int      num_dns;
    int      num_stores;
    uint64_t generation;
};

// Per-process registry that owns:
//   - the membership view of falcon_dn_node + falcon_store_node + the
//     shard table,
//   - one IKVStoreFacade per known store_node_id,
//   - the membership refresh loop (periodic + reactive + on-demand) and
//     optional LISTEN/NOTIFY consumer, all per design §3.4.7.
//
// The whole class is one ELF unit: shared by the C++ test binaries and by
// the pybind11 module that the Python OffloadingManager loads.
class KVStoreFacadeRegistry {
public:
    KVStoreFacadeRegistry();
    ~KVStoreFacadeRegistry();

    // Bootstrap. Idempotent. After return:
    //   - the periodic refresh thread is running;
    //   - the LISTEN/NOTIFY consumer is connected (if
    //     falcon_kv.membership_notify_enabled);
    //   - one initial refresh cycle has completed.
    // local_host_node_name typically comes from getenv("NODE_NAME")
    // or gethostname() (P4 picks the strategy).
    void Start(const std::string& local_host_node_name,
               const std::string& cn_libpq_conninfo);
    void Stop();

    // Application-callable on-demand refresh (§3.4.7.1). Posts a refresh
    // request to the loop and blocks up to timeout_ms for the next cycle
    // to complete. Subject to the membership_refresh_min_interval_ms rate
    // limit shared with reactive refreshes.
    RefreshResult RefreshNow(int32_t timeout_ms);

    // Posted by RPC call sites on transport-level failures so the loop
    // wakes immediately (§3.4.7.1 trigger #2). Coalesced. Cheap to call
    // from the hot path.
    void NotifyRpcFailure(int32_t store_or_dn_id, RpcFailureKind kind);

    // Generation counter; ticks after every successful refresh (§3.4.7.1).
    // Application code may snapshot it before an operation and compare
    // afterwards to detect "the view shifted under me" without parsing
    // the diff itself.
    uint64_t Generation() const noexcept;

    // Internal entry points used by the refresh loop's diff-and-apply
    // phase (§3.4.7.4). Not part of the public API.
    void RegisterLocalShm(int32_t store_id, LocalShmHandle* handle);
    void RegisterRemote  (int32_t store_id, const std::string& brpc_endpoint);
    std::shared_ptr<IKVStoreFacade> Resolve(int32_t store_id);
    std::map<int32_t, std::string> SnapshotDnEndpoints();
    std::map<int32_t, bool> SnapshotStoreLocality();

private:
    // P4 fills this in; the inner state is intentionally invisible at the
    // header level so that the implementation can swap data structures
    // (e.g. lock-free vs mutex-protected map) without API churn.
    struct State;
    std::unique_ptr<State> state_;
};

}  // namespace falconfs::kv
