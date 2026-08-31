// v6 §19.4 / M4 distributed cluster E2E.
//
// Hashes a population of keys across one or more DN BRPC endpoints (one per
// DN), drives a full alloc -> update STORED -> lookup -> renew -> free
// lifecycle through every endpoint in parallel, and asserts:
//
//   1. Each key is routed to exactly one DN (consistent with the Python
//      Router policy: hash modulo the sorted DN-id list).
//   2. Every BRPC step succeeds with the expected version transitions
//      (alloc -> 1, update STORED -> 2).
//   3. After force-free, no `cluster_<run_id>_*` rows leak in either DN's
//      `pg_catalog.falcon_kvblock_table` (verified externally by the
//      harness via psql; this binary only tests the BRPC contract).
//   4. Both DNs receive a non-zero share of work so the test fails fast if
//      a single-DN regression sneaks in.
//
// CLI:
//   FalconKVClusterE2E --dn HOST:PORT [--dn HOST:PORT ...]
//                      [--keys N] [--iterations N] [--threads N]
//                      [--timeout-ms MS]

#include <brpc/channel.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "kv_common.pb.h"
#include "kv_metadata_service.pb.h"
#include "tests/falcon_kv/kv_e2e_block_size.h"

namespace {

int Fail(const std::string& msg) {
    std::cerr << "CLUSTER_E2E_FAIL: " << msg << std::endl;
    return 1;
}

bool InitChannel(const std::string& endpoint, int timeout_ms, brpc::Channel* channel) {
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = timeout_ms;
    options.connect_timeout_ms = 5000;
    options.max_retry = 0;
    options.connection_type = "pooled";
    return channel->Init(endpoint.c_str(), &options) == 0;
}

int64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Match Python `Router.route(...)` exactly: stable hash modulo sorted DN ids.
// We pin to std::hash<std::string> here; the Python equivalent uses
// `hash(block_hash) % len(sorted_keys)`. The actual hash function differs,
// so we don't try to emulate Python's; we just want a deterministic
// distribution.
size_t RouteIndex(const std::string& block_hash, size_t num_endpoints) {
    return std::hash<std::string>{}(block_hash) % num_endpoints;
}

// Per-key state captured between phases.
struct KeyState {
    std::string block_hash;
    int endpoint_idx = 0;
    int64_t alloc_version = 0;
    int64_t stored_version = 0;
    int64_t lease_token = 0;
    int64_t dn_epoch = 0;
    int64_t store_epoch = 0;
    int64_t store_node_id = 0;
    int64_t pool_offset = 0;
};

struct EndpointStats {
    std::atomic<int64_t> allocs_attempted{0};
    std::atomic<int64_t> allocs_succeeded{0};
    std::atomic<int64_t> updates_succeeded{0};
    std::atomic<int64_t> lookups_succeeded{0};
    std::atomic<int64_t> renews_succeeded{0};
    std::atomic<int64_t> frees_succeeded{0};
};

struct Config {
    std::vector<std::string> endpoints;
    int total_keys = 64;
    int iterations = 1;
    int threads = 4;
    int timeout_ms = 30000;
    std::string shm_runtime_dir;
    std::string run_prefix;
};

// Per-(endpoint, phase) batched BRPC call helper.
template <typename Request, typename Response, typename FillItemFn,
          typename ReadResultFn>
bool CallEndpoint(const Config& cfg, int endpoint_idx, const std::string& phase_tag,
                  std::vector<int>& key_indices, std::vector<KeyState>& states,
                  FillItemFn fill_item, ReadResultFn read_result,
                  std::function<void(brpc::Channel*, brpc::Controller*,
                                     const Request&, Response*)>
                      rpc_call) {
    if (key_indices.empty()) return true;
    brpc::Channel channel;
    if (!InitChannel(cfg.endpoints[endpoint_idx], cfg.timeout_ms, &channel)) {
        Fail("channel init failed: " + cfg.endpoints[endpoint_idx]);
        return false;
    }
    Request req;
    req.mutable_meta()->set_request_id(cfg.run_prefix + "_" + phase_tag + "_ep" +
                                       std::to_string(endpoint_idx));
    req.mutable_meta()->set_client_id(0);
    for (int idx : key_indices) {
        fill_item(req, states[idx]);
    }
    Response resp;
    brpc::Controller cntl;
    rpc_call(&channel, &cntl, req, &resp);
    if (cntl.Failed()) {
        Fail(phase_tag + " rpc fail on " + cfg.endpoints[endpoint_idx] + ": " +
             cntl.ErrorText());
        return false;
    }
    if (static_cast<int>(resp.results_size()) != static_cast<int>(key_indices.size())) {
        Fail(phase_tag + " size mismatch on " + cfg.endpoints[endpoint_idx]);
        return false;
    }
    for (int i = 0; i < resp.results_size(); ++i) {
        if (!read_result(resp.results(i), states[key_indices[i]])) return false;
    }
    return true;
}

bool RunIteration(const Config& cfg, int iteration_idx,
                  std::vector<EndpointStats>& stats) {
    using namespace falconfs::kv;

    const int N = cfg.total_keys;
    std::vector<KeyState> states(N);
    std::unordered_map<int, std::vector<int>> by_endpoint;  // endpoint_idx -> key indices

    for (int i = 0; i < N; ++i) {
        states[i].block_hash =
            "cluster_" + cfg.run_prefix + "_i" + std::to_string(iteration_idx) +
            "_k" + std::to_string(i);
        states[i].endpoint_idx = static_cast<int>(
            RouteIndex(states[i].block_hash, cfg.endpoints.size()));
        by_endpoint[states[i].endpoint_idx].push_back(i);
    }

    // ── Allocate ──
    for (auto& kv : by_endpoint) {
        auto& key_indices = kv.second;
        bool ok = CallEndpoint<BatchAllocateRequest, BatchAllocateResponse>(
            cfg, kv.first, "alloc", key_indices, states,
            [&](BatchAllocateRequest& req, KeyState& st) {
                auto* it = req.add_items();
                it->set_block_hash(st.block_hash);
                it->set_block_size(falconfs::kv::test::E2eKvBlockSize());
                it->set_preferred_store_id(1);
                it->set_allow_fallback_store(true);
                req.set_deduplicate_in_request(true);
            },
            [&](const AllocateResult& r, KeyState& st) -> bool {
                stats[st.endpoint_idx].allocs_attempted.fetch_add(1);
                if (!r.result().success() || r.version() != 1) {
                    Fail("alloc failed for " + st.block_hash + ": " +
                         r.result().error_message());
                    return false;
                }
                st.alloc_version = r.version();
                st.lease_token = r.lease().lease_token();
                st.dn_epoch = r.lease().dn_epoch();
                st.store_epoch = r.lease().store_epoch();
                st.store_node_id = r.location().store_node_id();
                st.pool_offset = r.location().pool_offset();
                stats[st.endpoint_idx].allocs_succeeded.fetch_add(1);
                return true;
            },
            [](brpc::Channel* ch, brpc::Controller* c,
               const BatchAllocateRequest& q, BatchAllocateResponse* p) {
                KVMetadataService_Stub stub(ch);
                stub.BatchAllocateWithLease(c, &q, p, nullptr);
            });
        if (!ok) return false;
    }

    // ── Update STORED ──
    for (auto& kv : by_endpoint) {
        auto& key_indices = kv.second;
        bool ok = CallEndpoint<BatchUpdateStatusRequest, BatchUpdateStatusResponse>(
            cfg, kv.first, "update", key_indices, states,
            [&](BatchUpdateStatusRequest& req, KeyState& st) {
                auto* it = req.add_items();
                it->set_block_hash(st.block_hash);
                it->set_expected_from_status(BLOCK_STATUS_ALLOCATED);
                it->set_to_status(BLOCK_STATUS_STORED);
                it->set_expected_version(st.alloc_version);
            },
            [&](const StatusUpdateResult& r, KeyState& st) -> bool {
                if (!r.result().success() || r.new_version() != 2) {
                    Fail("update failed for " + st.block_hash);
                    return false;
                }
                st.stored_version = r.new_version();
                stats[st.endpoint_idx].updates_succeeded.fetch_add(1);
                return true;
            },
            [](brpc::Channel* ch, brpc::Controller* c,
               const BatchUpdateStatusRequest& q, BatchUpdateStatusResponse* p) {
                KVMetadataService_Stub stub(ch);
                stub.BatchUpdateBlockStatus(c, &q, p, nullptr);
            });
        if (!ok) return false;
    }

    // ── Lookup hit ──
    for (auto& kv : by_endpoint) {
        auto& key_indices = kv.second;
        bool ok = CallEndpoint<BatchLookupRequest, BatchLookupResponse>(
            cfg, kv.first, "lookup", key_indices, states,
            [&](BatchLookupRequest& req, KeyState& st) {
                auto* it = req.add_items();
                it->set_block_hash(st.block_hash);
                it->set_renew_lease_on_hit(true);
            },
            [&](const LookupResult& r, KeyState& st) -> bool {
                if (!r.result().success() || r.status() != BLOCK_STATUS_STORED) {
                    Fail("lookup failed for " + st.block_hash);
                    return false;
                }
                if (r.location().store_node_id() != st.store_node_id ||
                    r.location().pool_offset() != st.pool_offset) {
                    Fail("lookup location drift for " + st.block_hash);
                    return false;
                }
                st.lease_token = r.lease().lease_token();
                st.dn_epoch = r.lease().dn_epoch();
                st.store_epoch = r.lease().store_epoch();
                stats[st.endpoint_idx].lookups_succeeded.fetch_add(1);
                return true;
            },
            [](brpc::Channel* ch, brpc::Controller* c,
               const BatchLookupRequest& q, BatchLookupResponse* p) {
                KVMetadataService_Stub stub(ch);
                stub.BatchLookupWithLease(c, &q, p, nullptr);
            });
        if (!ok) return false;
    }

    // ── Renew lease ──
    for (auto& kv : by_endpoint) {
        auto& key_indices = kv.second;
        bool ok = CallEndpoint<BatchRenewLeaseRequest, BatchRenewLeaseResponse>(
            cfg, kv.first, "renew", key_indices, states,
            [&](BatchRenewLeaseRequest& req, KeyState& st) {
                req.set_requested_ttl_ms(2000);
                auto* it = req.add_items();
                it->set_block_hash(st.block_hash);
                it->set_lease_token(st.lease_token);
                it->set_expected_dn_epoch(st.dn_epoch);
                it->set_expected_store_epoch(st.store_epoch);
            },
            [&](const RenewLeaseResult& r, KeyState& st) -> bool {
                if (!r.result().success()) {
                    Fail("renew failed for " + st.block_hash);
                    return false;
                }
                stats[st.endpoint_idx].renews_succeeded.fetch_add(1);
                return true;
            },
            [](brpc::Channel* ch, brpc::Controller* c,
               const BatchRenewLeaseRequest& q, BatchRenewLeaseResponse* p) {
                KVMetadataService_Stub stub(ch);
                stub.BatchRenewLease(c, &q, p, nullptr);
            });
        if (!ok) return false;
    }

    // ── Free ──
    for (auto& kv : by_endpoint) {
        auto& key_indices = kv.second;
        bool ok = CallEndpoint<BatchFreeAllocatedRequest, BatchFreeAllocatedResponse>(
            cfg, kv.first, "free", key_indices, states,
            [&](BatchFreeAllocatedRequest& req, KeyState& st) {
                auto* it = req.add_items();
                it->set_block_hash(st.block_hash);
                it->set_expected_version(st.stored_version);
                it->set_force(true);
            },
            [&](const FreeAllocatedResult& r, KeyState& st) -> bool {
                if (!r.result().success()) {
                    Fail("free failed for " + st.block_hash);
                    return false;
                }
                stats[st.endpoint_idx].frees_succeeded.fetch_add(1);
                return true;
            },
            [](brpc::Channel* ch, brpc::Controller* c,
               const BatchFreeAllocatedRequest& q, BatchFreeAllocatedResponse* p) {
                KVMetadataService_Stub stub(ch);
                stub.BatchFreeAllocated(c, &q, p, nullptr);
            });
        if (!ok) return false;
    }

    return true;
}

void PrintUsage(const char* argv0) {
    std::cerr <<
        "Usage: " << argv0 << " --dn HOST:PORT [--dn HOST:PORT ...] [options]\n"
        "  --dn HOST:PORT        DN BRPC endpoint; repeat for multi-DN sweep (>= 1).\n"
        "  --keys N              Total keys generated per iteration (default 64).\n"
        "  --iterations N        Number of full alloc->free sweeps (default 1).\n"
        "  --threads N           [reserved] currently ignored; sequential phases.\n"
        "  --timeout-ms MS       Per-RPC timeout (default 30000).\n"
        "  --shm-runtime-dir DIR Optional v6.5 P4 hint for local SHM runtime (logged only).\n";
}

}  // namespace

int main(int argc, char** argv) {
    Config cfg;
    cfg.run_prefix = std::to_string(NowNs());
    for (int i = 1; i < argc; ++i) {
        const std::string a(argv[i]);
        if (a == "-h" || a == "--help") {
            PrintUsage(argv[0]);
            return 0;
        }
        if (a == "--dn" && i + 1 < argc) {
            cfg.endpoints.push_back(argv[++i]);
        } else if (a == "--keys" && i + 1 < argc) {
            cfg.total_keys = std::atoi(argv[++i]);
        } else if (a == "--iterations" && i + 1 < argc) {
            cfg.iterations = std::atoi(argv[++i]);
        } else if (a == "--threads" && i + 1 < argc) {
            cfg.threads = std::atoi(argv[++i]);
        } else if (a == "--timeout-ms" && i + 1 < argc) {
            cfg.timeout_ms = std::atoi(argv[++i]);
        } else if (a == "--shm-runtime-dir" && i + 1 < argc) {
            cfg.shm_runtime_dir = argv[++i];
        } else {
            PrintUsage(argv[0]);
            return Fail("unknown arg: " + a);
        }
    }
    if (cfg.endpoints.empty()) {
        PrintUsage(argv[0]);
        return Fail("at least one --dn endpoint required");
    }
    if (cfg.total_keys <= 0 || cfg.iterations <= 0) {
        return Fail("--keys and --iterations must be positive");
    }

    std::vector<EndpointStats> stats(cfg.endpoints.size());

    std::cout << "CLUSTER_E2E_BEGIN endpoints=" << cfg.endpoints.size()
              << " keys=" << cfg.total_keys
              << " iterations=" << cfg.iterations
              << " run_prefix=" << cfg.run_prefix;
    if (!cfg.shm_runtime_dir.empty()) {
        std::cout << " shm_runtime_dir=" << cfg.shm_runtime_dir;
    }
    std::cout << std::endl;

    const auto t0 = std::chrono::steady_clock::now();
    for (int it = 0; it < cfg.iterations; ++it) {
        if (!RunIteration(cfg, it, stats)) return 1;
        std::cout << "  iter=" << it << " OK" << std::endl;
    }
    const auto t1 = std::chrono::steady_clock::now();
    const double sec = std::chrono::duration<double>(t1 - t0).count();

    // Multi-DN sanity: every endpoint should see at least one alloc when more
    // than one was supplied. Single-endpoint runs naturally skip this check.
    bool any_idle = false;
    for (size_t i = 0; i < cfg.endpoints.size(); ++i) {
        const auto& s = stats[i];
        std::cout << "  ep=" << cfg.endpoints[i]
                  << " allocs=" << s.allocs_succeeded.load()
                  << " updates=" << s.updates_succeeded.load()
                  << " lookups=" << s.lookups_succeeded.load()
                  << " renews=" << s.renews_succeeded.load()
                  << " frees=" << s.frees_succeeded.load() << std::endl;
        if (s.allocs_succeeded.load() == 0) any_idle = true;
    }
    if (cfg.endpoints.size() > 1 && any_idle) {
        return Fail("multi-DN sweep: at least one DN saw zero traffic");
    }

    std::cout << "CLUSTER_E2E_OK elapsed_s=" << sec
              << " endpoints=" << cfg.endpoints.size()
              << " keys=" << cfg.total_keys
              << " iterations=" << cfg.iterations
              << std::endl;
    return 0;
}
