#include <brpc/channel.h>
#include <google/protobuf/stubs/common.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "kv_common.pb.h"
#include "kv_metadata_service.pb.h"
#include "tests/falcon_kv/kv_e2e_block_size.h"

namespace {

constexpr int kMaxDistinctBlocks = 64;

struct Config {
    std::string endpoint = "127.0.0.1:55530";
    int threads = 4;
    int batch_size = 4;
    int waves = 2;
    int timeout_ms = 120000;
    std::string run_id;
};

struct WaveWorkState {
    int tid = 0;
    int wave = 0;
    std::vector<std::string> hashes;
    std::vector<int64_t> versions_after_alloc;
    std::vector<int64_t> lease_tokens;
    std::vector<int64_t> dn_epochs;
    std::vector<int64_t> store_epochs;
};

int Fail(const std::string &msg)
{
    std::cerr << "META_CONCURRENCY_E2E_FAIL: " << msg << std::endl;
    return 1;
}

std::string BlockHash(const std::string &run_id, int tid, int wave, int idx)
{
    std::ostringstream oss;
    oss << "mce2e_" << run_id << "_t" << tid << "_w" << wave << "_i" << idx;
    return oss.str();
}

std::string RequestId(const std::string &run_id, const char *phase, int wid, int seq)
{
    std::ostringstream oss;
    oss << run_id << "_" << phase << "_w" << wid << "_s" << seq;
    return oss.str();
}

static void TidWaveFromWid(int wid, const Config &cfg, int *tid, int *wave)
{
    *tid = wid % cfg.threads;
    *wave = wid / cfg.threads;
}

void PrepareHashes(WaveWorkState *st, const Config &cfg, int wid)
{
    TidWaveFromWid(wid, cfg, &st->tid, &st->wave);
    st->hashes.clear();
    st->hashes.reserve(static_cast<size_t>(cfg.batch_size));
    for (int i = 0; i < cfg.batch_size; ++i) {
        st->hashes.push_back(BlockHash(cfg.run_id, st->tid, st->wave, i));
    }
    st->versions_after_alloc.resize(static_cast<size_t>(cfg.batch_size));
    st->lease_tokens.resize(static_cast<size_t>(cfg.batch_size));
    st->dn_epochs.resize(static_cast<size_t>(cfg.batch_size));
    st->store_epochs.resize(static_cast<size_t>(cfg.batch_size));
}

void RunAllocatePhase(int wid, const Config &cfg, WaveWorkState *st, std::atomic<int> *failures)
{
    using falconfs::kv::KVMetadataService_Stub;

    PrepareHashes(st, cfg, wid);

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = cfg.timeout_ms;
    options.connect_timeout_ms = 5000;
    options.max_retry = 0;
    options.connection_type = "pooled";
    if (channel.Init(cfg.endpoint.c_str(), &options) != 0) {
        std::cerr << "allocate phase: channel Init failed wid=" << wid << std::endl;
        failures->fetch_add(1, std::memory_order_relaxed);
        return;
    }

    KVMetadataService_Stub stub(&channel);
    falconfs::kv::BatchAllocateRequest alloc_req;
    alloc_req.mutable_meta()->set_client_id(8000 + st->tid);
    alloc_req.mutable_meta()->set_request_id(RequestId(cfg.run_id, "alloc", wid, 0));
    alloc_req.set_deduplicate_in_request(true);
    for (const auto &h : st->hashes) {
        auto *it = alloc_req.add_items();
        it->set_block_hash(h);
        it->set_block_size(falconfs::kv::test::E2eKvBlockSize());
        it->set_preferred_store_id(1);
        it->set_allow_fallback_store(true);
    }

    falconfs::kv::BatchAllocateResponse alloc_rsp;
    brpc::Controller cntl;
    stub.BatchAllocateWithLease(&cntl, &alloc_req, &alloc_rsp, nullptr);
    if (cntl.Failed()) {
        std::cerr << "allocate phase wid=" << wid << " err=" << cntl.ErrorText() << std::endl;
        failures->fetch_add(1, std::memory_order_relaxed);
        return;
    }
    if (alloc_rsp.results_size() != cfg.batch_size) {
        std::cerr << "allocate phase wid=" << wid << " size mismatch" << std::endl;
        failures->fetch_add(1, std::memory_order_relaxed);
        return;
    }
    for (int i = 0; i < cfg.batch_size; ++i) {
        const auto &r = alloc_rsp.results(i);
        if (r.block_hash() != st->hashes[static_cast<size_t>(i)] || !r.result().success()) {
            std::cerr << "allocate phase wid=" << wid << " item " << i
                      << " err=" << r.result().error_message() << " code=" << r.result().error_code()
                      << std::endl;
            failures->fetch_add(1, std::memory_order_relaxed);
            return;
        }
        st->versions_after_alloc[static_cast<size_t>(i)] = r.version();
    }
}

void RunUpdatePhase(int wid, const Config &cfg, WaveWorkState *st, std::atomic<int> *failures)
{
    using falconfs::kv::BLOCK_STATUS_ALLOCATED;
    using falconfs::kv::BLOCK_STATUS_STORED;
    using falconfs::kv::KVMetadataService_Stub;

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = cfg.timeout_ms;
    options.connect_timeout_ms = 5000;
    options.max_retry = 0;
    options.connection_type = "pooled";
    if (channel.Init(cfg.endpoint.c_str(), &options) != 0) {
        std::cerr << "update phase: channel Init failed wid=" << wid << std::endl;
        failures->fetch_add(1, std::memory_order_relaxed);
        return;
    }

    KVMetadataService_Stub stub(&channel);
    falconfs::kv::BatchUpdateStatusRequest upd_req;
    upd_req.mutable_meta()->set_client_id(8000 + st->tid);
    upd_req.mutable_meta()->set_request_id(RequestId(cfg.run_id, "upd", wid, 0));
    for (int i = 0; i < cfg.batch_size; ++i) {
        auto *it = upd_req.add_items();
        it->set_block_hash(st->hashes[static_cast<size_t>(i)]);
        it->set_expected_from_status(BLOCK_STATUS_ALLOCATED);
        it->set_to_status(BLOCK_STATUS_STORED);
        it->set_expected_version(st->versions_after_alloc[static_cast<size_t>(i)]);
        it->set_allow_noop_if_already_target(true);
    }
    falconfs::kv::BatchUpdateStatusResponse upd_rsp;
    brpc::Controller cntl;
    stub.BatchUpdateBlockStatus(&cntl, &upd_req, &upd_rsp, nullptr);
    if (cntl.Failed()) {
        std::cerr << "update phase wid=" << wid << " err=" << cntl.ErrorText() << std::endl;
        failures->fetch_add(1, std::memory_order_relaxed);
        return;
    }
    if (upd_rsp.results_size() != cfg.batch_size) {
        std::cerr << "update phase wid=" << wid << " size mismatch" << std::endl;
        failures->fetch_add(1, std::memory_order_relaxed);
        return;
    }
    for (int i = 0; i < cfg.batch_size; ++i) {
        if (!upd_rsp.results(i).result().success()) {
            std::cerr << "update phase wid=" << wid << " item " << i << " failed" << std::endl;
            failures->fetch_add(1, std::memory_order_relaxed);
            return;
        }
    }
}

void RunLookupPhase(int wid, const Config &cfg, WaveWorkState *st, std::atomic<int> *failures)
{
    using falconfs::kv::KVMetadataService_Stub;

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = cfg.timeout_ms;
    options.connect_timeout_ms = 5000;
    options.max_retry = 0;
    options.connection_type = "pooled";
    if (channel.Init(cfg.endpoint.c_str(), &options) != 0) {
        std::cerr << "lookup phase: channel Init failed wid=" << wid << std::endl;
        failures->fetch_add(1, std::memory_order_relaxed);
        return;
    }

    KVMetadataService_Stub stub(&channel);
    falconfs::kv::BatchLookupRequest lk_req;
    lk_req.mutable_meta()->set_client_id(8000 + st->tid);
    lk_req.mutable_meta()->set_request_id(RequestId(cfg.run_id, "lk", wid, 0));
    for (const auto &h : st->hashes) {
        auto *it = lk_req.add_items();
        it->set_block_hash(h);
        it->set_renew_lease_on_hit(true);
    }
    falconfs::kv::BatchLookupResponse lk_rsp;
    brpc::Controller cntl;
    stub.BatchLookupWithLease(&cntl, &lk_req, &lk_rsp, nullptr);
    if (cntl.Failed()) {
        std::cerr << "lookup phase wid=" << wid << " err=" << cntl.ErrorText() << std::endl;
        failures->fetch_add(1, std::memory_order_relaxed);
        return;
    }
    if (lk_rsp.results_size() != cfg.batch_size) {
        std::cerr << "lookup phase wid=" << wid << " size mismatch" << std::endl;
        failures->fetch_add(1, std::memory_order_relaxed);
        return;
    }
    for (int i = 0; i < cfg.batch_size; ++i) {
        const auto &r = lk_rsp.results(i);
        if (!r.result().success() || !r.cacheable() || !r.has_lease()) {
            std::cerr << "lookup phase wid=" << wid << " item " << i
                      << " err=" << r.result().error_message()
                      << " code=" << r.result().error_code()
                      << std::endl;
            failures->fetch_add(1, std::memory_order_relaxed);
            return;
        }
        st->lease_tokens[static_cast<size_t>(i)] = r.lease().lease_token();
        st->dn_epochs[static_cast<size_t>(i)] = r.lease().dn_epoch();
        st->store_epochs[static_cast<size_t>(i)] = r.lease().store_epoch();
    }
}

void RunRenewPhase(int wid, const Config &cfg, WaveWorkState *st, std::atomic<int> *failures)
{
    using falconfs::kv::KVMetadataService_Stub;

    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = cfg.timeout_ms;
    options.connect_timeout_ms = 5000;
    options.max_retry = 0;
    options.connection_type = "pooled";
    if (channel.Init(cfg.endpoint.c_str(), &options) != 0) {
        std::cerr << "renew phase: channel Init failed wid=" << wid << std::endl;
        failures->fetch_add(1, std::memory_order_relaxed);
        return;
    }

    KVMetadataService_Stub stub(&channel);
    falconfs::kv::BatchRenewLeaseRequest rn_req;
    rn_req.mutable_meta()->set_client_id(8000 + st->tid);
    rn_req.mutable_meta()->set_request_id(RequestId(cfg.run_id, "rn", wid, 0));
    rn_req.set_requested_ttl_ms(4000);
    for (int i = 0; i < cfg.batch_size; ++i) {
        auto *it = rn_req.add_items();
        it->set_block_hash(st->hashes[static_cast<size_t>(i)]);
        it->set_lease_token(st->lease_tokens[static_cast<size_t>(i)]);
        it->set_expected_dn_epoch(st->dn_epochs[static_cast<size_t>(i)]);
        it->set_expected_store_epoch(st->store_epochs[static_cast<size_t>(i)]);
    }
    falconfs::kv::BatchRenewLeaseResponse rn_rsp;
    brpc::Controller cntl;
    stub.BatchRenewLease(&cntl, &rn_req, &rn_rsp, nullptr);
    if (cntl.Failed()) {
        std::cerr << "renew phase wid=" << wid << " err=" << cntl.ErrorText() << std::endl;
        failures->fetch_add(1, std::memory_order_relaxed);
        return;
    }
    if (rn_rsp.results_size() != cfg.batch_size) {
        std::cerr << "renew phase wid=" << wid << " size mismatch" << std::endl;
        failures->fetch_add(1, std::memory_order_relaxed);
        return;
    }
    for (int i = 0; i < cfg.batch_size; ++i) {
        if (!rn_rsp.results(i).result().success()) {
            std::cerr << "renew phase wid=" << wid << " item " << i
                      << " err=" << rn_rsp.results(i).result().error_message()
                      << " code=" << rn_rsp.results(i).result().error_code()
                      << std::endl;
            failures->fetch_add(1, std::memory_order_relaxed);
            return;
        }
    }
}

template <typename Fn>
void RunPhaseParallel(int parallel_units, const Config &cfg, std::vector<WaveWorkState> *states, std::atomic<int> *failures, Fn &&phase_fn)
{
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(parallel_units));
    for (int wid = 0; wid < parallel_units; ++wid) {
        workers.emplace_back(phase_fn, wid, std::cref(cfg), &(*states)[static_cast<size_t>(wid)], failures);
    }
    for (auto &w : workers) {
        w.join();
    }
}

void PrintUsage(const char *argv0)
{
    std::cerr
        << "Usage: " << argv0
        << " [--endpoint HOST:PORT] [--threads N] [--batch-size|--batch_size B] [--waves W] [--timeout-ms MS]\n"
        << "  Phased BRPC stress: for each RPC type, spawns (threads*waves) concurrent clients so all\n"
        << "  BatchAllocate / BatchUpdate / BatchLookup / BatchRenew calls hit the server together.\n"
        << "  Constraint: threads * batch_size * waves <= " << kMaxDistinctBlocks
        << " (single-region engine slot limit in current PG backend).\n";
}

bool ParseInt(const char *s, int *out)
{
    char *end = nullptr;
    long v = std::strtol(s, &end, 10);
    if (end == s || *end != '\0' || v < 1 || v > 256) {
        return false;
    }
    *out = static_cast<int>(v);
    return true;
}

} // namespace

int main(int argc, char **argv)
{
    Config cfg;
    cfg.run_id = std::to_string(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());

    for (int i = 1; i < argc; ++i) {
        const std::string arg(argv[i]);
        if (arg == "--help" || arg == "-h") {
            PrintUsage(argv[0]);
            return 0;
        }
        if (arg == "--endpoint" && i + 1 < argc) {
            cfg.endpoint = argv[++i];
        } else if (arg == "--threads" && i + 1 < argc) {
            if (!ParseInt(argv[++i], &cfg.threads)) {
                return Fail("invalid --threads (use 1..256)");
            }
        } else if ((arg == "--batch-size" || arg == "--batch_size") && i + 1 < argc) {
            if (!ParseInt(argv[++i], &cfg.batch_size)) {
                return Fail("invalid --batch-size (use 1..256)");
            }
        } else if (arg == "--waves" && i + 1 < argc) {
            if (!ParseInt(argv[++i], &cfg.waves)) {
                return Fail("invalid --waves (use 1..256)");
            }
        } else if (arg == "--timeout-ms" && i + 1 < argc) {
            const char *s = argv[++i];
            char *end = nullptr;
            const long t = std::strtol(s, &end, 10);
            if (end == s || *end != '\0' || t < 3000L || t > 600000L) {
                return Fail("invalid --timeout-ms (use 3000..600000)");
            }
            cfg.timeout_ms = static_cast<int>(t);
        } else {
            return Fail("unknown arg: " + arg);
        }
    }

    const int64_t total_blocks = static_cast<int64_t>(cfg.threads) * cfg.batch_size * cfg.waves;
    if (total_blocks > kMaxDistinctBlocks) {
        std::ostringstream oss;
        oss << "threads*batch_size*waves=" << total_blocks << " exceeds engine capacity " << kMaxDistinctBlocks
            << " (lower concurrency, batch size, or waves)";
        return Fail(oss.str());
    }
    if (total_blocks < 1) {
        return Fail("no work (threads/batch_size/waves)");
    }

    const int parallel_units = cfg.threads * cfg.waves;
    std::vector<WaveWorkState> states(static_cast<size_t>(parallel_units));
    std::atomic<int> failures{0};

    const auto t0 = std::chrono::steady_clock::now();

    RunPhaseParallel(parallel_units, cfg, &states, &failures, RunAllocatePhase);
    if (failures.load(std::memory_order_relaxed) != 0) {
        return Fail("allocate phase: one or more parallel workers failed");
    }
    RunPhaseParallel(parallel_units, cfg, &states, &failures, RunUpdatePhase);
    if (failures.load(std::memory_order_relaxed) != 0) {
        return Fail("update phase: one or more parallel workers failed");
    }
    RunPhaseParallel(parallel_units, cfg, &states, &failures, RunLookupPhase);
    if (failures.load(std::memory_order_relaxed) != 0) {
        return Fail("lookup phase: one or more parallel workers failed");
    }
    RunPhaseParallel(parallel_units, cfg, &states, &failures, RunRenewPhase);
    if (failures.load(std::memory_order_relaxed) != 0) {
        return Fail("renew phase: one or more parallel workers failed");
    }

    const auto t1 = std::chrono::steady_clock::now();
    const double sec = std::chrono::duration<double>(t1 - t0).count();

    const int64_t rpc_batches = static_cast<int64_t>(parallel_units) * 4;
    std::cout << "META_CONCURRENCY_E2E_OK"
              << " endpoint=" << cfg.endpoint << " threads=" << cfg.threads << " batch_size=" << cfg.batch_size
              << " waves=" << cfg.waves << " parallel_units=" << parallel_units << " total_blocks=" << total_blocks
              << " rpc_batches=" << rpc_batches << " phased_parallel=1 elapsed_s=" << sec << " run_id=" << cfg.run_id
              << std::endl;
    return 0;
}
