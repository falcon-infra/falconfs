// v6.5 §19.4.1 drill #4 — Reference mixed colocation (affinity + fallback).
//
// Models four logical Clients on four distinct NODE_NAME identities (v65mix0..3),
// each paired with Store store_node_id = phase+1 (same host_node_name as the
// Store processes started by the harness). For each phase we:
//   1) set NODE_NAME to v65mix{phase}
//   2) refresh membership from CN
//   3) issue many BatchAllocateWithLease calls on the shard-owning DN with
//      preferred_store_id = phase+1
//   4) measure allocator affinity: returned store_node_id == preferred_store_id
//
// Assertions (design intent):
//   - Per-phase affinity rate >= 72% (relax 75% slightly for tiny bitmaps).
//   - At least one cross-store fallback observed globally (returned store !=
//     preferred).
//   - With 3+ DNs, every DN endpoint receives at least one allocation across all
//     keys (same invariant as FalconKVClusterE2E).

#include <brpc/channel.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "kv_common.pb.h"
#include "kv_metadata_service.pb.h"
#include "tests/falcon_kv/kv_e2e_block_size.h"
#include "vllm_kv_cache/src/store/kv_store_facade.h"

namespace {

int Fail(const std::string& msg) {
    std::cerr << "MIXED_COLOCATION_FAIL: " << msg << std::endl;
    return 1;
}

bool InitChannel(const std::string& endpoint, int timeout_ms, brpc::Channel* channel) {
    brpc::ChannelOptions options;
    options.protocol            = "baidu_std";
    options.timeout_ms          = timeout_ms;
    options.connect_timeout_ms  = 5000;
    options.max_retry           = 0;
    options.connection_type     = "pooled";
    return channel->Init(endpoint.c_str(), &options) == 0;
}

size_t RouteIndex(const std::string& block_hash, size_t num_dns) {
    return std::hash<std::string>{}(block_hash) % num_dns;
}

int64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

bool AllocateOne(const std::string& dn_ep,
                 int timeout_ms,
                 const std::string& run_id,
                 int phase,
                 int key_idx,
                 int32_t preferred_store_id,
                 int32_t* out_store) {
    using namespace falconfs::kv;
    brpc::Channel ch;
    if (!InitChannel(dn_ep, timeout_ms, &ch)) {
        return false;
    }
    KVMetadataService_Stub stub(&ch);
    BatchAllocateRequest req;
    req.mutable_meta()->set_request_id(run_id + "_p" + std::to_string(phase) + "_k" +
                                       std::to_string(key_idx));
    req.mutable_meta()->set_client_id(static_cast<uint32_t>(100 + phase));
    auto* it = req.add_items();
    it->set_block_hash("mix_" + run_id + "_p" + std::to_string(phase) + "_k" +
                       std::to_string(key_idx));
    it->set_block_size(falconfs::kv::test::E2eKvBlockSize());
    it->set_preferred_store_id(preferred_store_id);
    it->set_allow_fallback_store(true);
    req.set_deduplicate_in_request(true);
    BatchAllocateResponse resp;
    brpc::Controller cntl;
    stub.BatchAllocateWithLease(&cntl, &req, &resp, nullptr);
    if (cntl.Failed() || resp.results_size() != 1 || !resp.results(0).result().success()) {
        return false;
    }
    *out_store = resp.results(0).location().store_node_id();
    return true;
}

bool FreeOne(const std::string& dn_ep,
             int timeout_ms,
             const std::string& run_id,
             int phase,
             const std::string& block_hash,
             int64_t version) {
    using namespace falconfs::kv;
    brpc::Channel ch;
    if (!InitChannel(dn_ep, timeout_ms, &ch)) {
        return false;
    }
    KVMetadataService_Stub stub(&ch);
    BatchFreeAllocatedRequest req;
    req.mutable_meta()->set_request_id(run_id + "_free_" + block_hash);
    req.mutable_meta()->set_client_id(static_cast<uint32_t>(100 + phase));
    auto* it = req.add_items();
    it->set_block_hash(block_hash);
    it->set_expected_version(version);
    it->set_force(true);
    BatchFreeAllocatedResponse resp;
    brpc::Controller cntl;
    stub.BatchFreeAllocated(&cntl, &req, &resp, nullptr);
    return !cntl.Failed() && resp.results_size() == 1 && resp.results(0).result().success();
}

}  // namespace

int main(int argc, char** argv) {
    std::string cn_conninfo;
    std::vector<std::string> dn_endpoints;
    int keys_per_phase = 180;
    int timeout_ms     = 30000;
    for (int i = 1; i < argc; ++i) {
        const std::string a(argv[i]);
        if (a == "--cn-conninfo" && i + 1 < argc) {
            cn_conninfo = argv[++i];
        } else if (a == "--dn" && i + 1 < argc) {
            dn_endpoints.push_back(argv[++i]);
        } else if (a == "--keys-per-phase" && i + 1 < argc) {
            keys_per_phase = std::atoi(argv[++i]);
        } else if (a == "--timeout-ms" && i + 1 < argc) {
            timeout_ms = std::atoi(argv[++i]);
        } else if (a == "-h" || a == "--help") {
            std::cerr << "Usage: " << argv[0]
                      << " --cn-conninfo CONN --dn HOST:PORT [--dn ...]\n"
                         "  [--keys-per-phase N] [--timeout-ms MS]\n";
            return 0;
        } else {
            return Fail("unknown arg: " + a);
        }
    }
    if (cn_conninfo.empty() || dn_endpoints.size() < 2) {
        return Fail("--cn-conninfo and at least two --dn endpoints are required");
    }
    if (keys_per_phase <= 0) {
        return Fail("--keys-per-phase must be positive");
    }

    const std::string run_id = std::to_string(NowNs());
    std::unordered_map<size_t, int64_t> dn_alloc_counts;

    int64_t global_total   = 0;
    int64_t global_hits    = 0;
    int64_t global_fallback = 0;

    for (int phase = 0; phase < 4; ++phase) {
        const std::string node = "v65mix" + std::to_string(phase);
        if (setenv("NODE_NAME", node.c_str(), 1) != 0) {
            return Fail("setenv NODE_NAME failed");
        }
        falconfs::kv::KVStoreFacadeRegistry reg;
        reg.Start(node, cn_conninfo);
        (void)reg.RefreshNow(8000);

        const int32_t preferred = static_cast<int32_t>(phase + 1);
        int64_t phase_total = 0;
        int64_t phase_hits  = 0;

        for (int k = 0; k < keys_per_phase; ++k) {
            const std::string h =
                "mix_" + run_id + "_p" + std::to_string(phase) + "_k" + std::to_string(k);
            const size_t ep_idx = RouteIndex(h, dn_endpoints.size());
            int32_t sid = 0;
            if (!AllocateOne(dn_endpoints[ep_idx], timeout_ms, run_id, phase, k, preferred, &sid)) {
                return Fail("allocate failed phase=" + std::to_string(phase) + " k=" + std::to_string(k));
            }
            ++dn_alloc_counts[ep_idx];
            ++phase_total;
            ++global_total;
            if (sid == preferred) {
                ++phase_hits;
                ++global_hits;
            } else {
                ++global_fallback;
            }
            // Free immediately so the next regression suite sees a clean catalog.
            if (!FreeOne(dn_endpoints[ep_idx], timeout_ms, run_id, phase, h, 1)) {
                return Fail("free failed phase=" + std::to_string(phase) + " k=" + std::to_string(k));
            }
        }

        const double rate =
            phase_total > 0 ? static_cast<double>(phase_hits) / static_cast<double>(phase_total) : 0.0;
        std::cout << "  phase=" << phase << " NODE_NAME=" << node
                  << " preferred_store=" << preferred << " affinity=" << rate << " (hits=" << phase_hits
                  << "/" << phase_total << ")" << std::endl;
        if (rate < 0.72) {
            return Fail("phase " + std::to_string(phase) + " affinity " + std::to_string(rate) +
                        " below 0.72 threshold");
        }
        reg.Stop();
    }

    if (global_fallback < 1) {
        // When every phase hits its preferred store (common with 4 stores + healthy
        // regions), still prove the allocator can leave the preferred hint: prefer a
        // store id that is not registered on the DN so PickRegions orders fallbacks.
        const std::string fb_node = "v65mix_fb";
        if (setenv("NODE_NAME", fb_node.c_str(), 1) != 0) {
            return Fail("setenv NODE_NAME failed (fallback probe)");
        }
        falconfs::kv::KVStoreFacadeRegistry reg;
        reg.Start(fb_node, cn_conninfo);
        (void)reg.RefreshNow(8000);
        constexpr int32_t kAbsentPreferredStore = 2147483647;
        const std::string h = "mix_" + run_id + "_p4_k0";
        const size_t ep_idx = RouteIndex(h, dn_endpoints.size());
        int32_t sid = 0;
        if (!AllocateOne(dn_endpoints[ep_idx], timeout_ms, run_id, 4, 0, kAbsentPreferredStore, &sid)) {
            reg.Stop();
            return Fail("forced fallback allocate failed");
        }
        if (sid == kAbsentPreferredStore) {
            reg.Stop();
            return Fail("forced fallback returned absent preferred store id");
        }
        ++global_fallback;
        if (!FreeOne(dn_endpoints[ep_idx], timeout_ms, run_id, 4, h, 1)) {
            reg.Stop();
            return Fail("forced fallback free failed");
        }
        reg.Stop();
    }

    if (global_fallback < 1) {
        return Fail("expected at least one allocator fallback (preferred != chosen store)");
    }
    if (dn_endpoints.size() > 1) {
        for (size_t i = 0; i < dn_endpoints.size(); ++i) {
            if (dn_alloc_counts[i] == 0) {
                return Fail("DN index " + std::to_string(i) + " saw zero allocations");
            }
        }
    }

    const double g_rate =
        global_total > 0 ? static_cast<double>(global_hits) / static_cast<double>(global_total) : 0.0;
    std::cout << "MIXED_COLOCATION_OK global_affinity=" << g_rate
              << " global_fallbacks=" << global_fallback << " dns=" << dn_endpoints.size()
              << std::endl;
    return 0;
}
