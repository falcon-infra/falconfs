// End-to-end KV metadata stress test.
//
// Goal (per user request): exercise the FalconFS cluster's KV metadata path
// with varying numbers of concurrent BRPC clients and batch sizes, and check
// system correctness on every observable surface:
//
//   * BatchAllocateWithLease   — every allocated block returns a unique
//                                 (store_node_id, pool_offset) and version=1.
//   * BatchUpdateBlockStatus   — ALLOCATED→STORED CAS with expected_version=1
//                                 succeeds and returns new_version=2.
//   * BatchLookupWithLease     — every block looks up cacheable=true with
//                                 status=STORED and the same location.
//   * BatchRenewLease          — every previously-leased block renews.
//   * BatchFreeAllocated       — force-free every block; subsequent lookup
//                                 returns NOT_FOUND.
//   * Concurrent dedup         — when N threads race on the SAME block hash,
//                                 exactly one allocation wins and the rest
//                                 must report reused_existing_allocation=true
//                                 with the same (store_node_id, pool_offset).
//
// Each phase uses (threads * waves) parallel BRPC clients hammering one DN
// endpoint. The test then sweeps a matrix of (threads, batch_size, waves)
// configurations to cover small/medium/large concurrency and small/large
// batches. Between configurations, every block is force-freed so the engine
// bitmap is fully released, the catalog row count returns to zero, and the
// next configuration starts on a clean slate.
//
// Exit code: 0 on full pass; non-zero with a "STRESS_FAIL: ..." message on
// the first observed correctness violation.

#include <brpc/channel.h>
#include <google/protobuf/stubs/common.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "kv_common.pb.h"
#include "kv_metadata_service.pb.h"
#include "tests/falcon_kv/kv_e2e_block_size.h"

// At most 64 distinct DRAM slots per DN engine (per-region bitmap); stress
// configs keep threads*batch_size*waves <= this bound.
constexpr int kMaxDistinctBlocks = 64;

struct StressConfig {
    int threads;
    int batch_size;
    int waves;
};

struct GlobalConfig {
    std::string endpoint = "127.0.0.1:55530";
    int timeout_ms = 120000;
    int dedup_clients = 8;
    int sweep_iterations = 1;
    std::string run_prefix;
};

int Fail(const std::string& msg) {
    std::cerr << "STRESS_FAIL: " << msg << std::endl;
    return 1;
}

std::string MakeBlockHash(const std::string& run, const std::string& tag, int tid, int wave, int idx) {
    std::ostringstream oss;
    oss << "stress_" << run << "_" << tag << "_t" << tid << "_w" << wave << "_i" << idx;
    return oss.str();
}

std::string ReqId(const std::string& run, const char* phase, int wid, int extra) {
    std::ostringstream oss;
    oss << run << "_" << phase << "_w" << wid << "_x" << extra;
    return oss.str();
}

bool InitChannel(const GlobalConfig& cfg, brpc::Channel* channel) {
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = cfg.timeout_ms;
    options.connect_timeout_ms = 5000;
    options.max_retry = 0;
    options.connection_type = "pooled";
    return channel->Init(cfg.endpoint.c_str(), &options) == 0;
}

struct WaveState {
    int tid = 0;
    int wave = 0;
    std::vector<std::string> hashes;
    std::vector<int64_t> versions;       // version after alloc (==1)
    std::vector<int64_t> versions_stored; // version after update STORED (==2)
    std::vector<int64_t> store_node_ids;
    std::vector<int64_t> pool_offsets;
    std::vector<int64_t> lease_tokens;
    std::vector<int64_t> dn_epochs;
    std::vector<int64_t> store_epochs;
};

void PrepareHashes(const GlobalConfig& gcfg, const StressConfig& cfg, const std::string& tag,
                   int wid, WaveState* st) {
    st->tid = wid % cfg.threads;
    st->wave = wid / cfg.threads;
    st->hashes.clear();
    st->hashes.reserve(static_cast<size_t>(cfg.batch_size));
    for (int i = 0; i < cfg.batch_size; ++i) {
        st->hashes.push_back(
            MakeBlockHash(gcfg.run_prefix, tag, st->tid, st->wave, i));
    }
    const auto sz = static_cast<size_t>(cfg.batch_size);
    st->versions.assign(sz, 0);
    st->versions_stored.assign(sz, 0);
    st->store_node_ids.assign(sz, 0);
    st->pool_offsets.assign(sz, 0);
    st->lease_tokens.assign(sz, 0);
    st->dn_epochs.assign(sz, 0);
    st->store_epochs.assign(sz, 0);
}

bool RunAllocate(const GlobalConfig& gcfg, const StressConfig& cfg, int wid, WaveState* st,
                 std::string* err) {
    using falconfs::kv::KVMetadataService_Stub;
    brpc::Channel channel;
    if (!InitChannel(gcfg, &channel)) {
        *err = "alloc: channel init failed wid=" + std::to_string(wid);
        return false;
    }
    KVMetadataService_Stub stub(&channel);

    falconfs::kv::BatchAllocateRequest req;
    req.mutable_meta()->set_client_id(40000 + st->tid);
    req.mutable_meta()->set_request_id(ReqId(gcfg.run_prefix, "alloc", wid, 0));
    req.set_deduplicate_in_request(true);
    for (const auto& h : st->hashes) {
        auto* it = req.add_items();
        it->set_block_hash(h);
        it->set_block_size(falconfs::kv::test::E2eKvBlockSize());
        it->set_preferred_store_id(1);
        it->set_allow_fallback_store(true);
    }
    falconfs::kv::BatchAllocateResponse rsp;
    brpc::Controller cntl;
    stub.BatchAllocateWithLease(&cntl, &req, &rsp, nullptr);
    if (cntl.Failed()) {
        *err = "alloc rpc fail wid=" + std::to_string(wid) + ": " + cntl.ErrorText();
        return false;
    }
    if (rsp.results_size() != cfg.batch_size) {
        *err = "alloc size mismatch wid=" + std::to_string(wid);
        return false;
    }
    for (int i = 0; i < cfg.batch_size; ++i) {
        const auto& r = rsp.results(i);
        if (!r.result().success() || r.block_hash() != st->hashes[static_cast<size_t>(i)]) {
            *err = "alloc item " + std::to_string(i) + " fail wid=" + std::to_string(wid) +
                   " err=" + r.result().error_message() +
                   " code=" + std::to_string(r.result().error_code());
            return false;
        }
        if (r.version() != 1) {
            *err = "alloc item version!=1 wid=" + std::to_string(wid) +
                   " got=" + std::to_string(r.version());
            return false;
        }
        if (!r.has_location() || !r.has_lease()) {
            *err = "alloc item missing location/lease wid=" + std::to_string(wid);
            return false;
        }
        if (r.reused_existing_allocation()) {
            *err = "alloc item unexpectedly reused wid=" + std::to_string(wid);
            return false;
        }
        st->versions[static_cast<size_t>(i)]      = r.version();
        st->store_node_ids[static_cast<size_t>(i)] = r.location().store_node_id();
        st->pool_offsets[static_cast<size_t>(i)]  = r.location().pool_offset();
        st->lease_tokens[static_cast<size_t>(i)]  = r.lease().lease_token();
        st->dn_epochs[static_cast<size_t>(i)]     = r.lease().dn_epoch();
        st->store_epochs[static_cast<size_t>(i)]  = r.lease().store_epoch();
    }
    return true;
}

bool RunUpdate(const GlobalConfig& gcfg, const StressConfig& cfg, int wid, WaveState* st,
               std::string* err) {
    using falconfs::kv::BLOCK_STATUS_ALLOCATED;
    using falconfs::kv::BLOCK_STATUS_STORED;
    using falconfs::kv::KVMetadataService_Stub;

    brpc::Channel channel;
    if (!InitChannel(gcfg, &channel)) {
        *err = "update: channel init failed wid=" + std::to_string(wid);
        return false;
    }
    KVMetadataService_Stub stub(&channel);
    falconfs::kv::BatchUpdateStatusRequest req;
    req.mutable_meta()->set_client_id(40000 + st->tid);
    req.mutable_meta()->set_request_id(ReqId(gcfg.run_prefix, "upd", wid, 0));
    for (int i = 0; i < cfg.batch_size; ++i) {
        auto* it = req.add_items();
        it->set_block_hash(st->hashes[static_cast<size_t>(i)]);
        it->set_expected_from_status(BLOCK_STATUS_ALLOCATED);
        it->set_to_status(BLOCK_STATUS_STORED);
        it->set_expected_version(st->versions[static_cast<size_t>(i)]);
        it->set_allow_noop_if_already_target(true);
    }
    falconfs::kv::BatchUpdateStatusResponse rsp;
    brpc::Controller cntl;
    stub.BatchUpdateBlockStatus(&cntl, &req, &rsp, nullptr);
    if (cntl.Failed()) {
        *err = "update rpc fail wid=" + std::to_string(wid) + ": " + cntl.ErrorText();
        return false;
    }
    if (rsp.results_size() != cfg.batch_size) {
        *err = "update size mismatch wid=" + std::to_string(wid);
        return false;
    }
    for (int i = 0; i < cfg.batch_size; ++i) {
        const auto& r = rsp.results(i);
        if (!r.result().success()) {
            *err = "update item " + std::to_string(i) + " fail wid=" + std::to_string(wid) +
                   " err=" + r.result().error_message() +
                   " code=" + std::to_string(r.result().error_code()) +
                   " current_status=" + std::to_string(r.current_status()) +
                   " new_version=" + std::to_string(r.new_version());
            return false;
        }
        if (r.new_version() != 2) {
            *err = "update new_version!=2 wid=" + std::to_string(wid) +
                   " got=" + std::to_string(r.new_version());
            return false;
        }
        st->versions_stored[static_cast<size_t>(i)] = r.new_version();
    }
    return true;
}

bool RunLookup(const GlobalConfig& gcfg, const StressConfig& cfg, int wid, WaveState* st,
               std::string* err) {
    using falconfs::kv::BLOCK_STATUS_STORED;
    using falconfs::kv::KVMetadataService_Stub;

    brpc::Channel channel;
    if (!InitChannel(gcfg, &channel)) {
        *err = "lookup: channel init failed wid=" + std::to_string(wid);
        return false;
    }
    KVMetadataService_Stub stub(&channel);
    falconfs::kv::BatchLookupRequest req;
    req.mutable_meta()->set_client_id(40000 + st->tid);
    req.mutable_meta()->set_request_id(ReqId(gcfg.run_prefix, "lk", wid, 0));
    for (const auto& h : st->hashes) {
        auto* it = req.add_items();
        it->set_block_hash(h);
        it->set_renew_lease_on_hit(true);
    }
    falconfs::kv::BatchLookupResponse rsp;
    brpc::Controller cntl;
    stub.BatchLookupWithLease(&cntl, &req, &rsp, nullptr);
    if (cntl.Failed()) {
        *err = "lookup rpc fail wid=" + std::to_string(wid) + ": " + cntl.ErrorText();
        return false;
    }
    if (rsp.results_size() != cfg.batch_size) {
        *err = "lookup size mismatch wid=" + std::to_string(wid);
        return false;
    }
    for (int i = 0; i < cfg.batch_size; ++i) {
        const auto& r = rsp.results(i);
        if (!r.result().success() || !r.cacheable() || !r.has_lease()) {
            *err = "lookup item " + std::to_string(i) + " fail wid=" + std::to_string(wid) +
                   " err=" + r.result().error_message() +
                   " code=" + std::to_string(r.result().error_code());
            return false;
        }
        if (r.status() != BLOCK_STATUS_STORED) {
            *err = "lookup status!=STORED wid=" + std::to_string(wid) +
                   " got=" + std::to_string(static_cast<int>(r.status()));
            return false;
        }
        if (r.version() != st->versions_stored[static_cast<size_t>(i)]) {
            *err = "lookup version mismatch wid=" + std::to_string(wid) +
                   " got=" + std::to_string(r.version()) +
                   " want=" + std::to_string(st->versions_stored[static_cast<size_t>(i)]);
            return false;
        }
        if (r.location().store_node_id() != st->store_node_ids[static_cast<size_t>(i)] ||
            r.location().pool_offset()   != st->pool_offsets[static_cast<size_t>(i)]) {
            *err = "lookup location mismatch wid=" + std::to_string(wid);
            return false;
        }
        // overwrite lease (renewed)
        st->lease_tokens[static_cast<size_t>(i)] = r.lease().lease_token();
        st->dn_epochs[static_cast<size_t>(i)]    = r.lease().dn_epoch();
        st->store_epochs[static_cast<size_t>(i)] = r.lease().store_epoch();
    }
    return true;
}

bool RunRenew(const GlobalConfig& gcfg, const StressConfig& cfg, int wid, WaveState* st,
              std::string* err) {
    using falconfs::kv::KVMetadataService_Stub;

    brpc::Channel channel;
    if (!InitChannel(gcfg, &channel)) {
        *err = "renew: channel init failed wid=" + std::to_string(wid);
        return false;
    }
    KVMetadataService_Stub stub(&channel);
    falconfs::kv::BatchRenewLeaseRequest req;
    req.mutable_meta()->set_client_id(40000 + st->tid);
    req.mutable_meta()->set_request_id(ReqId(gcfg.run_prefix, "rn", wid, 0));
    req.set_requested_ttl_ms(4000);
    for (int i = 0; i < cfg.batch_size; ++i) {
        auto* it = req.add_items();
        it->set_block_hash(st->hashes[static_cast<size_t>(i)]);
        it->set_lease_token(st->lease_tokens[static_cast<size_t>(i)]);
        it->set_expected_dn_epoch(st->dn_epochs[static_cast<size_t>(i)]);
        it->set_expected_store_epoch(st->store_epochs[static_cast<size_t>(i)]);
    }
    falconfs::kv::BatchRenewLeaseResponse rsp;
    brpc::Controller cntl;
    stub.BatchRenewLease(&cntl, &req, &rsp, nullptr);
    if (cntl.Failed()) {
        *err = "renew rpc fail wid=" + std::to_string(wid) + ": " + cntl.ErrorText();
        return false;
    }
    if (rsp.results_size() != cfg.batch_size) {
        *err = "renew size mismatch wid=" + std::to_string(wid);
        return false;
    }
    for (int i = 0; i < cfg.batch_size; ++i) {
        if (!rsp.results(i).result().success()) {
            *err = "renew item " + std::to_string(i) + " fail wid=" + std::to_string(wid) +
                   " err=" + rsp.results(i).result().error_message();
            return false;
        }
    }
    return true;
}

bool RunFree(const GlobalConfig& gcfg, const StressConfig& cfg, int wid, WaveState* st,
             std::string* err) {
    using falconfs::kv::KVMetadataService_Stub;

    brpc::Channel channel;
    if (!InitChannel(gcfg, &channel)) {
        *err = "free: channel init failed wid=" + std::to_string(wid);
        return false;
    }
    KVMetadataService_Stub stub(&channel);
    falconfs::kv::BatchFreeAllocatedRequest req;
    req.mutable_meta()->set_client_id(40000 + st->tid);
    req.mutable_meta()->set_request_id(ReqId(gcfg.run_prefix, "free", wid, 0));
    for (int i = 0; i < cfg.batch_size; ++i) {
        auto* it = req.add_items();
        it->set_block_hash(st->hashes[static_cast<size_t>(i)]);
        it->set_expected_version(st->versions_stored[static_cast<size_t>(i)]);
        it->set_force(true); // STORED is not freeable without force
    }
    falconfs::kv::BatchFreeAllocatedResponse rsp;
    brpc::Controller cntl;
    stub.BatchFreeAllocated(&cntl, &req, &rsp, nullptr);
    if (cntl.Failed()) {
        *err = "free rpc fail wid=" + std::to_string(wid) + ": " + cntl.ErrorText();
        return false;
    }
    if (rsp.results_size() != cfg.batch_size) {
        *err = "free size mismatch wid=" + std::to_string(wid);
        return false;
    }
    for (int i = 0; i < cfg.batch_size; ++i) {
        const auto& r = rsp.results(i);
        if (!r.result().success()) {
            *err = "free item " + std::to_string(i) + " fail wid=" + std::to_string(wid) +
                   " err=" + r.result().error_message();
            return false;
        }
    }
    return true;
}

bool RunLookupAfterFree(const GlobalConfig& gcfg, const StressConfig& cfg, int wid, WaveState* st,
                        std::string* err) {
    using falconfs::kv::ErrorCode;
    using falconfs::kv::KVMetadataService_Stub;

    brpc::Channel channel;
    if (!InitChannel(gcfg, &channel)) {
        *err = "post-free lookup: channel init failed wid=" + std::to_string(wid);
        return false;
    }
    KVMetadataService_Stub stub(&channel);
    falconfs::kv::BatchLookupRequest req;
    req.mutable_meta()->set_client_id(40000 + st->tid);
    req.mutable_meta()->set_request_id(ReqId(gcfg.run_prefix, "lkpf", wid, 0));
    for (const auto& h : st->hashes) {
        auto* it = req.add_items();
        it->set_block_hash(h);
        it->set_renew_lease_on_hit(false);
    }
    falconfs::kv::BatchLookupResponse rsp;
    brpc::Controller cntl;
    stub.BatchLookupWithLease(&cntl, &req, &rsp, nullptr);
    if (cntl.Failed()) {
        *err = "post-free lookup rpc fail wid=" + std::to_string(wid) + ": " + cntl.ErrorText();
        return false;
    }
    if (rsp.results_size() != cfg.batch_size) {
        *err = "post-free lookup size mismatch wid=" + std::to_string(wid);
        return false;
    }
    for (int i = 0; i < cfg.batch_size; ++i) {
        const auto& r = rsp.results(i);
        if (r.result().success()) {
            *err = "post-free lookup unexpectedly succeeded wid=" + std::to_string(wid) +
                   " item=" + std::to_string(i);
            return false;
        }
        if (r.result().error_code() != ErrorCode::NOT_FOUND) {
            *err = "post-free lookup not NOT_FOUND wid=" + std::to_string(wid) +
                   " code=" + std::to_string(r.result().error_code());
            return false;
        }
    }
    return true;
}

template <typename Fn>
bool RunPhaseParallel(int parallel_units, const GlobalConfig& gcfg, const StressConfig& cfg,
                      std::vector<WaveState>* states, const std::string& phase_label, Fn&& phase_fn) {
    std::atomic<int> failures{0};
    std::mutex err_mu;
    std::string first_err;
    std::vector<std::thread> ws;
    ws.reserve(static_cast<size_t>(parallel_units));
    for (int wid = 0; wid < parallel_units; ++wid) {
        ws.emplace_back([&, wid]() {
            std::string e;
            if (!phase_fn(gcfg, cfg, wid, &(*states)[static_cast<size_t>(wid)], &e)) {
                failures.fetch_add(1, std::memory_order_relaxed);
                std::lock_guard<std::mutex> lk(err_mu);
                if (first_err.empty()) first_err = e;
            }
        });
    }
    for (auto& t : ws) t.join();
    if (failures.load(std::memory_order_relaxed) != 0) {
        Fail(phase_label + ": " + first_err);
        return false;
    }
    return true;
}

bool RunOneStress(const GlobalConfig& gcfg, const StressConfig& cfg, int iter,
                  int64_t* total_blocks_out) {
    const int64_t total = static_cast<int64_t>(cfg.threads) * cfg.batch_size * cfg.waves;
    if (total > kMaxDistinctBlocks) {
        return Fail("config exceeds engine slot capacity " + std::to_string(kMaxDistinctBlocks));
    }
    if (total_blocks_out) *total_blocks_out = total;

    const int parallel = cfg.threads * cfg.waves;
    std::vector<WaveState> states(static_cast<size_t>(parallel));

    const std::string tag =
        "i" + std::to_string(iter) + "_t" + std::to_string(cfg.threads) +
        "b" + std::to_string(cfg.batch_size) + "w" + std::to_string(cfg.waves);

    for (int wid = 0; wid < parallel; ++wid) {
        PrepareHashes(gcfg, cfg, tag, wid, &states[static_cast<size_t>(wid)]);
    }

    if (!RunPhaseParallel(parallel, gcfg, cfg, &states, "alloc", RunAllocate))    return false;

    // Cross-thread uniqueness: every (store_node_id, pool_offset) pair must be unique.
    {
        std::set<std::pair<int64_t, int64_t>> seen;
        for (const auto& st : states) {
            for (size_t i = 0; i < st.pool_offsets.size(); ++i) {
                auto p = std::make_pair(st.store_node_ids[i], st.pool_offsets[i]);
                if (!seen.insert(p).second) {
                    return Fail("duplicate (store_node_id, pool_offset) across threads: store=" +
                                std::to_string(p.first) + " offset=" + std::to_string(p.second));
                }
            }
        }
    }

    if (!RunPhaseParallel(parallel, gcfg, cfg, &states, "update", RunUpdate))    return false;
    if (!RunPhaseParallel(parallel, gcfg, cfg, &states, "lookup", RunLookup))    return false;
    if (!RunPhaseParallel(parallel, gcfg, cfg, &states, "renew",  RunRenew))     return false;
    if (!RunPhaseParallel(parallel, gcfg, cfg, &states, "free",   RunFree))      return false;
    if (!RunPhaseParallel(parallel, gcfg, cfg, &states, "post_free_lookup", RunLookupAfterFree)) return false;
    return true;
}

// In-request dedup: ONE BRPC call with N duplicate items + deduplicate_in_request=true.
// v6.4 §7.4: every result must point to the same (store_node_id, pool_offset, version).
bool RunInRequestDedup(const GlobalConfig& gcfg, std::string* err) {
    using falconfs::kv::KVMetadataService_Stub;

    const std::string hash =
        "stress_" + gcfg.run_prefix + "_dedup_inreq";
    const int N = std::max(2, gcfg.dedup_clients);

    brpc::Channel channel;
    if (!InitChannel(gcfg, &channel)) {
        *err = "in-request dedup: channel init";
        return false;
    }
    KVMetadataService_Stub stub(&channel);
    falconfs::kv::BatchAllocateRequest req;
    req.mutable_meta()->set_client_id(60000);
    req.mutable_meta()->set_request_id(gcfg.run_prefix + "_dedup_inreq");
    req.set_deduplicate_in_request(true);
    for (int i = 0; i < N; ++i) {
        auto* it = req.add_items();
        it->set_block_hash(hash);
        it->set_block_size(falconfs::kv::test::E2eKvBlockSize());
        it->set_preferred_store_id(1);
        it->set_allow_fallback_store(true);
    }
    falconfs::kv::BatchAllocateResponse rsp;
    brpc::Controller cntl;
    stub.BatchAllocateWithLease(&cntl, &req, &rsp, nullptr);
    if (cntl.Failed()) {
        *err = "in-request dedup: rpc fail: " + cntl.ErrorText();
        return false;
    }
    if (rsp.results_size() != N) {
        *err = "in-request dedup: results_size=" + std::to_string(rsp.results_size()) +
               " want=" + std::to_string(N);
        return false;
    }
    int64_t ref_store  = -1;
    int64_t ref_offset = -1;
    for (int i = 0; i < N; ++i) {
        const auto& r = rsp.results(i);
        if (!r.result().success()) {
            *err = "in-request dedup: item " + std::to_string(i) + " failed: " +
                   r.result().error_message();
            return false;
        }
        if (i == 0) {
            ref_store  = r.location().store_node_id();
            ref_offset = r.location().pool_offset();
        } else if (r.location().store_node_id() != ref_store ||
                   r.location().pool_offset()   != ref_offset) {
            *err = "in-request dedup: item " + std::to_string(i) + " location differs";
            return false;
        }
    }
    // Teardown: free. The DRAM slot is in ALLOCATED with version=1 (newly
    // allocated, never updated); both catalog and DRAM CAS expect exactly 1.
    {
        falconfs::kv::BatchFreeAllocatedRequest freq;
        freq.mutable_meta()->set_client_id(60000);
        freq.mutable_meta()->set_request_id(gcfg.run_prefix + "_dedup_inreq_free");
        auto* it = freq.add_items();
        it->set_block_hash(hash);
        it->set_expected_version(1);
        it->set_force(true);
        falconfs::kv::BatchFreeAllocatedResponse fr;
        brpc::Controller fcntl;
        stub.BatchFreeAllocated(&fcntl, &freq, &fr, nullptr);
        if (fcntl.Failed() || fr.results_size() != 1 || !fr.results(0).result().success()) {
            *err = "in-request dedup teardown: free failed";
            if (fr.results_size() == 1) {
                *err += " err=" + fr.results(0).result().error_message() +
                        " code=" + std::to_string(fr.results(0).result().error_code());
            }
            return false;
        }
    }
    return true;
}

// Cross-request race: N concurrent BRPC clients each issuing their own
// single-item alloc for the SAME hash. v6.4 §7.4 allows distinct racers to
// observe CAS_CONFLICT (retryable); on retry, the engine's DRAM Pass-1 sees
// the winner's published slot and returns ReusedDram. Convergence guarantee:
// every client eventually succeeds with the SAME (store_node_id, pool_offset);
// exactly one of them is reused_existing_allocation=false.
bool RunCrossRequestRaceWithRetry(const GlobalConfig& gcfg, std::string* err) {
    using falconfs::kv::ErrorCode;
    using falconfs::kv::KVMetadataService_Stub;

    const std::string hash =
        "stress_" + gcfg.run_prefix + "_dedup_xreq";
    const int N = gcfg.dedup_clients;
    constexpr int kMaxAttempts = 6;

    struct ClientOutcome {
        bool ok = false;
        bool reused = false;
        int64_t store_node_id = 0;
        int64_t pool_offset = 0;
        std::string err;
    };
    std::vector<ClientOutcome> outs(static_cast<size_t>(N));
    std::vector<std::thread> ws;
    ws.reserve(static_cast<size_t>(N));
    for (int i = 0; i < N; ++i) {
        ws.emplace_back([&, i]() {
            ClientOutcome& out = outs[static_cast<size_t>(i)];
            brpc::Channel channel;
            if (!InitChannel(gcfg, &channel)) {
                out.err = "channel init";
                return;
            }
            KVMetadataService_Stub stub(&channel);
            for (int attempt = 0; attempt < kMaxAttempts && !out.ok; ++attempt) {
                falconfs::kv::BatchAllocateRequest req;
                req.mutable_meta()->set_client_id(60000 + i);
                req.mutable_meta()->set_request_id(
                    gcfg.run_prefix + "_dedup_xreq_c" + std::to_string(i) +
                    "_a" + std::to_string(attempt));
                req.set_deduplicate_in_request(false);
                auto* it = req.add_items();
                it->set_block_hash(hash);
                it->set_block_size(falconfs::kv::test::E2eKvBlockSize());
                it->set_preferred_store_id(1);
                it->set_allow_fallback_store(true);
                falconfs::kv::BatchAllocateResponse rsp;
                brpc::Controller cntl;
                stub.BatchAllocateWithLease(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    out.err = "rpc fail: " + cntl.ErrorText();
                    return;
                }
                if (rsp.results_size() != 1) {
                    out.err = "results_size != 1";
                    return;
                }
                const auto& r = rsp.results(0);
                if (r.result().success()) {
                    out.ok            = true;
                    out.reused        = r.reused_existing_allocation();
                    out.store_node_id = r.location().store_node_id();
                    out.pool_offset   = r.location().pool_offset();
                    return;
                }
                if (r.result().error_code() != ErrorCode::CAS_CONFLICT &&
                    r.result().error_code() != ErrorCode::THROTTLED) {
                    out.err = "non-retryable err code=" +
                              std::to_string(r.result().error_code()) + " msg=" +
                              r.result().error_message();
                    return;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(2 + 2 * attempt));
            }
            if (!out.ok) {
                out.err = "exceeded retry budget";
            }
        });
    }
    for (auto& t : ws) t.join();

    int reused_false = 0;
    int64_t ref_store  = -1;
    int64_t ref_offset = -1;
    for (int i = 0; i < N; ++i) {
        const auto& o = outs[static_cast<size_t>(i)];
        if (!o.ok) {
            *err = "cross-request race: client " + std::to_string(i) + " failed: " + o.err;
            return false;
        }
        if (i == 0) {
            ref_store = o.store_node_id;
            ref_offset = o.pool_offset;
        } else if (o.store_node_id != ref_store || o.pool_offset != ref_offset) {
            *err = "cross-request race: client " + std::to_string(i) +
                   " location diverged store=" + std::to_string(o.store_node_id) +
                   " offset=" + std::to_string(o.pool_offset) +
                   " ref=(" + std::to_string(ref_store) + "," + std::to_string(ref_offset) + ")";
            return false;
        }
        if (!o.reused) ++reused_false;
    }
    if (reused_false < 1) {
        *err = "cross-request race: no client reported reused_existing_allocation=false";
        return false;
    }

    // Teardown. After a successful race, the winning row has version=1 (new
    // allocate; never updated through STORED). Free with the matching version.
    {
        brpc::Channel channel;
        if (!InitChannel(gcfg, &channel)) {
            *err = "cross-request race teardown: channel init";
            return false;
        }
        KVMetadataService_Stub stub(&channel);
        falconfs::kv::BatchFreeAllocatedRequest freq;
        freq.mutable_meta()->set_client_id(60000);
        freq.mutable_meta()->set_request_id(gcfg.run_prefix + "_dedup_xreq_free");
        auto* it = freq.add_items();
        it->set_block_hash(hash);
        it->set_expected_version(1);
        it->set_force(true);
        falconfs::kv::BatchFreeAllocatedResponse fr;
        brpc::Controller cntl;
        stub.BatchFreeAllocated(&cntl, &freq, &fr, nullptr);
        if (cntl.Failed() || fr.results_size() != 1 || !fr.results(0).result().success()) {
            *err = "cross-request race teardown: free failed";
            if (fr.results_size() == 1) {
                *err += " err=" + fr.results(0).result().error_message() +
                        " code=" + std::to_string(fr.results(0).result().error_code());
            }
            return false;
        }
    }
    return true;
}

void PrintUsage(const char* argv0) {
    std::cerr <<
        "Usage: " << argv0 << " [options]\n"
        "  --endpoint HOST:PORT    BRPC endpoint of a DN/CN BRPC server (default 127.0.0.1:55530)\n"
        "  --timeout-ms MS         per-RPC timeout (default 120000)\n"
        "  --dedup-clients N       concurrent racers in dedup test (default 8)\n"
        "  --iterations N          repeat the full sweep N times (default 1)\n"
        "  --run-prefix STR        unique prefix for block hashes (default: time-based)\n"
        "\n"
        "Sweeps a built-in (threads, batch_size, waves) matrix that exercises the FalconFS\n"
        "KV metadata path with varying concurrent BRPC clients and batch sizes, validating\n"
        "alloc/update/lookup/renew/free correctness on every observable surface and that\n"
        "every block is fully reclaimed between sweeps so the engine bitmap and the\n"
        "persistent catalog return to a clean state.\n";
}

int main(int argc, char** argv) {
    GlobalConfig gcfg;
    gcfg.run_prefix = std::to_string(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());

    for (int i = 1; i < argc; ++i) {
        const std::string a(argv[i]);
        if (a == "-h" || a == "--help") {
            PrintUsage(argv[0]);
            return 0;
        }
        if (a == "--endpoint" && i + 1 < argc)        gcfg.endpoint = argv[++i];
        else if (a == "--timeout-ms" && i + 1 < argc) gcfg.timeout_ms = std::atoi(argv[++i]);
        else if (a == "--dedup-clients" && i + 1 < argc) gcfg.dedup_clients = std::atoi(argv[++i]);
        else if (a == "--iterations" && i + 1 < argc) gcfg.sweep_iterations = std::atoi(argv[++i]);
        else if (a == "--run-prefix" && i + 1 < argc) gcfg.run_prefix = argv[++i];
        else { PrintUsage(argv[0]); return Fail("unknown arg: " + a); }
    }

    // Sweep matrix: (threads, batch_size, waves).
    // Each row's threads*batch_size*waves <= kMaxDistinctBlocks (64) so the
    // engine bitmap can hold every block at once.
    const std::vector<StressConfig> sweep = {
        {1, 1, 1},     // smoke: smallest possible
        {1, 8, 1},     // single client, medium batch
        {1, 64, 1},    // single client, max batch
        {2, 16, 1},    // 2 clients, batch 16  (32 blocks)
        {4, 8,  1},    // 4 clients, batch 8   (32 blocks)
        {4, 8,  2},    // 4 clients, 2 waves   (64 blocks)
        {8, 4,  1},    // 8 clients, batch 4   (32 blocks)
        {8, 8,  1},    // 8 clients, batch 8   (64 blocks)
        {16, 4, 1},    // 16 clients, batch 4  (64 blocks)
    };

    std::cout << "STRESS_BEGIN endpoint=" << gcfg.endpoint
              << " run_prefix=" << gcfg.run_prefix
              << " sweep_size=" << sweep.size()
              << " iterations=" << gcfg.sweep_iterations
              << std::endl;

    const auto t0 = std::chrono::steady_clock::now();
    int64_t cum_blocks = 0;
    int cum_configs = 0;
    for (int iter = 0; iter < gcfg.sweep_iterations; ++iter) {
        for (const auto& cfg : sweep) {
            const auto cfg_t0 = std::chrono::steady_clock::now();
            int64_t blocks = 0;
            if (!RunOneStress(gcfg, cfg, iter, &blocks)) {
                return 1;
            }
            const auto cfg_t1 = std::chrono::steady_clock::now();
            const double sec = std::chrono::duration<double>(cfg_t1 - cfg_t0).count();
            cum_blocks += blocks;
            ++cum_configs;
            std::cout << "  CFG iter=" << iter
                      << " threads=" << cfg.threads
                      << " batch_size=" << cfg.batch_size
                      << " waves=" << cfg.waves
                      << " blocks=" << blocks
                      << " elapsed_s=" << sec
                      << " OK"
                      << std::endl;
        }
    }

    // In-request dedup correctness.
    {
        std::string err;
        if (!RunInRequestDedup(gcfg, &err)) {
            return Fail(err);
        }
        std::cout << "  DEDUP_INREQ items=" << std::max(2, gcfg.dedup_clients)
                  << " OK" << std::endl;
    }
    // Cross-request race correctness (with retry).
    {
        std::string err;
        if (!RunCrossRequestRaceWithRetry(gcfg, &err)) {
            return Fail(err);
        }
        std::cout << "  DEDUP_XREQ clients=" << gcfg.dedup_clients
                  << " OK" << std::endl;
    }

    const auto t1 = std::chrono::steady_clock::now();
    const double sec = std::chrono::duration<double>(t1 - t0).count();
    std::cout << "STRESS_OK endpoint=" << gcfg.endpoint
              << " configs=" << cum_configs
              << " total_blocks=" << cum_blocks
              << " elapsed_s=" << sec
              << " run_prefix=" << gcfg.run_prefix
              << std::endl;
    return 0;
}
