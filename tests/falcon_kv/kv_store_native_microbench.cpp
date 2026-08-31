#include <brpc/channel.h>
#include <brpc/controller.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "kv_data_service.pb.h"

namespace {

struct Config {
    std::vector<std::string> endpoints{"127.0.0.1:18765"};
    int blocks = 1024;
    int block_bytes = 1024 * 1024;
    int threads = 16;
    int timeout_ms = 30000;
    int64_t epoch = 1;
    bool verify = true;
};

int Fail(const std::string& msg) {
    std::cerr << "NATIVE_KV_STORE_MICROBENCH_FAIL: " << msg << std::endl;
    return 1;
}

std::vector<std::string> Split(const std::string& s, char delim) {
    std::vector<std::string> out;
    std::size_t start = 0;
    while (start <= s.size()) {
        std::size_t pos = s.find(delim, start);
        std::string part = (pos == std::string::npos) ? s.substr(start) : s.substr(start, pos - start);
        if (!part.empty()) out.push_back(part);
        if (pos == std::string::npos) break;
        start = pos + 1;
    }
    return out;
}

bool ParseInt(const char* s, int* out) {
    char* end = nullptr;
    long v = std::strtol(s, &end, 10);
    if (end == s || *end != '\0' || v <= 0 || v > 1000000000L) return false;
    *out = static_cast<int>(v);
    return true;
}

bool ParseArgs(int argc, char** argv, Config* cfg) {
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if ((arg == "--endpoints" || arg == "--stores") && i + 1 < argc) {
            cfg->endpoints = Split(argv[++i], ',');
            if (cfg->endpoints.empty()) return false;
        } else if (arg == "--blocks" && i + 1 < argc) {
            if (!ParseInt(argv[++i], &cfg->blocks)) return false;
        } else if ((arg == "--block-bytes" || arg == "--block_bytes") && i + 1 < argc) {
            if (!ParseInt(argv[++i], &cfg->block_bytes)) return false;
        } else if (arg == "--threads" && i + 1 < argc) {
            if (!ParseInt(argv[++i], &cfg->threads)) return false;
        } else if (arg == "--timeout-ms" && i + 1 < argc) {
            if (!ParseInt(argv[++i], &cfg->timeout_ms)) return false;
        } else if (arg == "--no-verify") {
            cfg->verify = false;
        } else {
            return false;
        }
    }
    return true;
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

std::string Payload(int n) {
    std::string p;
    p.resize(static_cast<std::size_t>(n));
    for (int i = 0; i < n; ++i) p[static_cast<std::size_t>(i)] = static_cast<char>(i & 0xff);
    return p;
}

struct Assignment {
    int endpoint_idx = 0;
    int64_t offset = 0;
    std::string hash;
};

int64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

struct Summary {
    double wall_s = 0;
    double avg_ms = 0;
    double p50_ms = 0;
    double p95_ms = 0;
    double p99_ms = 0;
    double wall_mb_s = 0;
    double instr_mb_s = 0;
};

Summary Summarize(std::vector<double> lat_s, int64_t bytes, double wall_s) {
    std::sort(lat_s.begin(), lat_s.end());
    auto pct = [&](double p) {
        if (lat_s.empty()) return 0.0;
        std::size_t idx = static_cast<std::size_t>(std::llround((p / 100.0) * (lat_s.size() - 1)));
        if (idx >= lat_s.size()) idx = lat_s.size() - 1;
        return lat_s[idx] * 1000.0;
    };
    double sum = std::accumulate(lat_s.begin(), lat_s.end(), 0.0);
    Summary s;
    s.wall_s = wall_s;
    s.avg_ms = lat_s.empty() ? 0.0 : (sum / lat_s.size()) * 1000.0;
    s.p50_ms = pct(50);
    s.p95_ms = pct(95);
    s.p99_ms = pct(99);
    s.wall_mb_s = (bytes / 1.0e6) / std::max(wall_s, 1e-12);
    s.instr_mb_s = (bytes / 1.0e6) / std::max(sum, 1e-12);
    return s;
}

void PrintSummary(const char* phase, const Summary& s) {
    std::cout << phase << ": wall=" << s.wall_s << "s wall_MB/s=" << s.wall_mb_s
              << " avg=" << s.avg_ms << "ms p50=" << s.p50_ms << "ms p95=" << s.p95_ms
              << "ms p99=" << s.p99_ms << "ms instr_MB/s=" << s.instr_mb_s << std::endl;
}

template <typename Fn>
int RunPhase(const Config& cfg, const std::vector<Assignment>& work, Fn&& fn,
             std::vector<double>* latencies, double* wall_s) {
    latencies->assign(work.size(), 0.0);
    std::atomic<int> next{0};
    std::atomic<int> failures{0};
    int64_t t0 = NowNs();
    std::vector<std::thread> threads;
    for (int tid = 0; tid < cfg.threads; ++tid) {
        threads.emplace_back([&, tid]() {
            std::vector<std::unique_ptr<brpc::Channel>> channels;
            channels.reserve(cfg.endpoints.size());
            for (const auto& ep : cfg.endpoints) {
                auto ch = std::make_unique<brpc::Channel>();
                if (!InitChannel(ep, cfg.timeout_ms, ch.get())) {
                    failures.fetch_add(1);
                    return;
                }
                channels.emplace_back(std::move(ch));
            }
            while (true) {
                int i = next.fetch_add(1);
                if (i >= static_cast<int>(work.size())) break;
                int64_t a = NowNs();
                if (!fn(*channels[work[static_cast<std::size_t>(i)].endpoint_idx], work[static_cast<std::size_t>(i)])) {
                    failures.fetch_add(1);
                    break;
                }
                int64_t b = NowNs();
                (*latencies)[static_cast<std::size_t>(i)] = static_cast<double>(b - a) / 1.0e9;
            }
        });
    }
    for (auto& th : threads) th.join();
    int64_t t1 = NowNs();
    *wall_s = static_cast<double>(t1 - t0) / 1.0e9;
    return failures.load() == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    Config cfg;
    if (!ParseArgs(argc, argv, &cfg)) {
        return Fail("usage: FalconKVStoreNativeMicrobench --endpoints host:port[,host:port...] --blocks N --block-bytes N --threads N [--no-verify]");
    }
    std::vector<Assignment> work;
    work.reserve(static_cast<std::size_t>(cfg.blocks));
    std::vector<int> per_ep(cfg.endpoints.size(), 0);
    for (int i = 0; i < cfg.blocks; ++i) {
        int ep = i % static_cast<int>(cfg.endpoints.size());
        int slot = per_ep[static_cast<std::size_t>(ep)]++;
        Assignment a;
        a.endpoint_idx = ep;
        a.offset = static_cast<int64_t>(slot) * cfg.block_bytes;
        a.hash = "native-micro-" + std::to_string(i);
        work.push_back(std::move(a));
    }
    const std::string payload = Payload(cfg.block_bytes);
    std::vector<double> lat;
    double wall = 0.0;

    auto write_fn = [&](brpc::Channel& ch, const Assignment& a) {
        falconfs::kv::KVDataService_Stub stub(&ch);
        falconfs::kv::WriteBlockRequest req;
        req.mutable_meta()->set_request_id("native_write_" + a.hash);
        auto* it = req.mutable_item();
        it->set_block_hash(a.hash);
        it->set_pool_offset(a.offset);
        it->set_block_size(cfg.block_bytes);
        it->set_expected_store_epoch(cfg.epoch);
        brpc::Controller cntl;
        cntl.request_attachment().append(payload.data(), payload.size());
        falconfs::kv::WriteBlockResponse rsp;
        stub.WriteBlock(&cntl, &req, &rsp, nullptr);
        return !cntl.Failed() && rsp.has_result() && rsp.result().result().success();
    };
    if (RunPhase(cfg, work, write_fn, &lat, &wall) != 0) return Fail("write phase failed");
    Summary ws = Summarize(lat, static_cast<int64_t>(cfg.blocks) * cfg.block_bytes, wall);

    auto read_fn = [&](brpc::Channel& ch, const Assignment& a) {
        falconfs::kv::KVDataService_Stub stub(&ch);
        falconfs::kv::ReadBlockRequest req;
        req.mutable_meta()->set_request_id("native_read_" + a.hash);
        auto* it = req.mutable_item();
        it->set_block_hash(a.hash);
        it->set_pool_offset(a.offset);
        it->set_block_size(cfg.block_bytes);
        it->set_expected_store_epoch(cfg.epoch);
        brpc::Controller cntl;
        falconfs::kv::ReadBlockResponse rsp;
        stub.ReadBlock(&cntl, &req, &rsp, nullptr);
        if (cntl.Failed() || !rsp.has_result() || !rsp.result().result().success()) return false;
        if (!cfg.verify) return true;
        std::string got;
        if (cntl.response_attachment().size() > 0) {
            cntl.response_attachment().copy_to(&got);
        } else {
            got = rsp.result().payload();
        }
        return got == payload;
    };
    if (RunPhase(cfg, work, read_fn, &lat, &wall) != 0) return Fail("read phase failed");
    Summary rs = Summarize(lat, static_cast<int64_t>(cfg.blocks) * cfg.block_bytes, wall);

    std::cout << "--- NATIVE_KV_STORE_MICROBENCH ---" << std::endl;
    std::cout << "endpoints=" << cfg.endpoints.size() << " blocks=" << cfg.blocks
              << " block_bytes=" << cfg.block_bytes << " threads=" << cfg.threads << std::endl;
    PrintSummary("write", ws);
    PrintSummary("read", rs);
    std::cout << "--- end NATIVE_KV_STORE_MICROBENCH ---" << std::endl;
    return 0;
}
