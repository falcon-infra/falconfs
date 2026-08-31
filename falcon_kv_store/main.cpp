/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * Standalone `falcon_kv_store` daemon (v6.5 P2): hosts KVDataService on a
 * dedicated BRPC port, registers the DRAM region with each DN pooler via
 * KVStoreAdminService::RegisterStoreRegion, and mirrors membership in
 * pg_catalog.falcon_store_node on the CN.
 */

#include <brpc/channel.h>
#include <brpc/protocol.h>
#include <brpc/server.h>
#include <libpq-fe.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "kv_store_admin_service.pb.h"
#include "vllm_kv_cache/src/service/kv_data_brpc_service.h"
#include "vllm_kv_cache/src/service/kv_store_admin_brpc_service.h"
#include "vllm_kv_cache/src/store/kv_data_service_impl.h"
#include "vllm_kv_cache/src/store/kv_store_engine.h"

namespace {

std::atomic<bool> g_stop{false};
brpc::Server* g_server = nullptr;

void OnSignal(int /*sig*/) {
    g_stop.store(true, std::memory_order_relaxed);
    if (g_server != nullptr) {
        g_server->Stop(0);
    }
}

int64_t WallClockMillis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::string GetEnvStr(const char* key, const char* fallback) {
    const char* v = std::getenv(key);
    if (v != nullptr && v[0] != '\0') {
        return std::string(v);
    }
    return std::string(fallback);
}

int GetEnvInt(const char* key, int fallback) {
    const char* v = std::getenv(key);
    if (v == nullptr || v[0] == '\0') {
        return fallback;
    }
    return static_cast<int>(std::strtol(v, nullptr, 10));
}

int64_t GetEnvInt64(const char* key, int64_t fallback) {
    const char* v = std::getenv(key);
    if (v == nullptr || v[0] == '\0') {
        return fallback;
    }
    return static_cast<int64_t>(std::strtoll(v, nullptr, 10));
}

std::string ResolveHostNodeName() {
    const char* env = std::getenv("NODE_NAME");
    if (env != nullptr && env[0] != '\0') {
        return std::string(env);
    }
    char buf[256];
    if (gethostname(buf, sizeof(buf)) == 0) {
        buf[sizeof(buf) - 1] = '\0';
        return std::string(buf);
    }
    return "unknown_host";
}

std::string GetCurrentUserName() {
    const char* u = std::getenv("USER");
    if (u != nullptr && u[0] != '\0') {
        return std::string(u);
    }
    u = std::getenv("PGUSER");
    if (u != nullptr && u[0] != '\0') {
        return std::string(u);
    }
    return "postgres";
}

PGconn* OpenCnConn(int cn_pg_port) {
    if (cn_pg_port <= 0) {
        return nullptr;
    }
    std::ostringstream conninfo;
    conninfo << "hostaddr=127.0.0.1 port=" << cn_pg_port << " user=" << GetCurrentUserName()
             << " dbname=postgres application_name=falcon_kv_store";
    PGconn* conn = PQconnectdb(conninfo.str().c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "[falcon_kv_store] CN connect failed: %s\n", PQerrorMessage(conn));
        fflush(stderr);
        PQfinish(conn);
        return nullptr;
    }
    return conn;
}

std::string SqlEscapeLiteral(PGconn* conn, const std::string& s) {
    char* lit = PQescapeLiteral(conn, s.c_str(), s.size());
    if (lit == nullptr) {
        return "''";
    }
    std::string out(lit);
    PQfreemem(lit);
    return out;
}

bool ExecStoreRegister(PGconn* conn,
                       int32_t store_node_id,
                       const std::string& host_node,
                       const std::string& host,
                       int brpc_port,
                       const std::string& runtime_dir,
                       const std::string& shm_name,
                       int64_t dram_pool_bytes,
                       int32_t block_size,
                       int64_t store_epoch) {
    /* Use a single SQL string so PostgreSQL can coerce string literals to the
     * extension's cstring-typed parameters (PQexecParams with text OIDs does
     * not resolve to falcon_store_node_register). */
    std::ostringstream q;
    q << "SELECT pg_catalog.falcon_store_node_register(" << store_node_id << ", "
      << SqlEscapeLiteral(conn, host_node) << ", " << SqlEscapeLiteral(conn, host) << ", "
      << brpc_port << ", " << SqlEscapeLiteral(conn, runtime_dir) << ", "
      << SqlEscapeLiteral(conn, shm_name) << ", " << static_cast<long long>(dram_pool_bytes) << "::bigint, "
      << block_size << "::int, " << static_cast<long long>(store_epoch) << "::bigint);";
    PGresult* res = PQexec(conn, q.str().c_str());
    const bool ok = (PQresultStatus(res) == PGRES_TUPLES_OK);
    if (!ok) {
        fprintf(stderr, "[falcon_kv_store] falcon_store_node_register failed: %s\n",
                PQresultErrorMessage(res));
        fflush(stderr);
    }
    PQclear(res);
    return ok;
}

bool ExecStoreHeartbeat(PGconn* conn, int32_t store_node_id, int64_t store_epoch, int64_t now_ms) {
    std::ostringstream q;
    q << "SELECT pg_catalog.falcon_store_node_heartbeat(" << store_node_id << ", "
      << static_cast<long long>(store_epoch) << "::bigint, " << static_cast<long long>(now_ms)
      << "::bigint);";
    PGresult* res = PQexec(conn, q.str().c_str());
    const bool ok = (PQresultStatus(res) == PGRES_TUPLES_OK);
    PQclear(res);
    return ok;
}

bool ExecStoreUnregister(PGconn* conn, int32_t store_node_id) {
    std::ostringstream q;
    q << "SELECT pg_catalog.falcon_store_node_unregister(" << store_node_id << ");";
    PGresult* res = PQexec(conn, q.str().c_str());
    const bool ok = (PQresultStatus(res) == PGRES_TUPLES_OK);
    PQclear(res);
    return ok;
}

std::vector<int> ParseDnPoolerPorts(const std::string& csv) {
    std::vector<int> out;
    std::size_t pos = 0;
    while (pos < csv.size()) {
        std::size_t comma = csv.find(',', pos);
        std::string tok =
            (comma == std::string::npos) ? csv.substr(pos) : csv.substr(pos, comma - pos);
        while (!tok.empty() && (tok.front() == ' ' || tok.front() == '\t')) {
            tok.erase(0, 1);
        }
        while (!tok.empty() && (tok.back() == ' ' || tok.back() == '\t')) {
            tok.pop_back();
        }
        if (!tok.empty()) {
            out.push_back(static_cast<int>(std::strtol(tok.c_str(), nullptr, 10)));
        }
        if (comma == std::string::npos) {
            break;
        }
        pos = comma + 1;
    }
    return out;
}

bool InitBrpcChannel(const std::string& endpoint, brpc::Channel* ch) {
    brpc::ChannelOptions opt;
    opt.protocol            = "baidu_std";
    opt.timeout_ms          = 30000;
    opt.connect_timeout_ms  = 5000;
    opt.max_retry           = 0;
    opt.connection_type     = "pooled";
    return ch->Init(endpoint.c_str(), &opt) == 0;
}

bool RegisterRegionOnDn(const std::string& dn_endpoint,
                        const std::string& store_brpc_endpoint,
                        int32_t store_node_id,
                        int region_index,
                        int64_t base_offset,
                        int64_t region_bytes,
                        int32_t block_size,
                        int64_t store_epoch,
                        const std::string& shm_name,
                        const std::string& runtime_dir) {
    brpc::Channel ch;
    if (!InitBrpcChannel(dn_endpoint, &ch)) {
        fprintf(stderr, "[falcon_kv_store] channel init failed: %s\n", dn_endpoint.c_str());
        fflush(stderr);
        return false;
    }
    falconfs::kv::KVStoreAdminService_Stub stub(&ch);
    falconfs::kv::RegisterStoreRegionRequest req;
    req.mutable_meta()->set_request_id("falcon_kv_store_register");
    falconfs::kv::StoreRegionInfo* r = req.mutable_region();
    r->set_store_node_id(store_node_id);
    r->set_region_index(region_index);
    r->set_base_offset(base_offset);
    r->set_region_bytes(region_bytes);
    r->set_block_size(block_size);
    r->set_store_epoch(store_epoch);
    r->set_shm_name(shm_name);
    r->set_runtime_dir(runtime_dir);
    req.set_store_brpc_endpoint(store_brpc_endpoint);
    falconfs::kv::RegisterStoreRegionResponse resp;
    brpc::Controller cntl;
    stub.RegisterStoreRegion(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        fprintf(stderr, "[falcon_kv_store] RegisterStoreRegion RPC failed ep=%s err=%s\n",
                dn_endpoint.c_str(), cntl.ErrorText().c_str());
        fflush(stderr);
        return false;
    }
    if (!resp.result().success()) {
        fprintf(stderr, "[falcon_kv_store] RegisterStoreRegion rejected ep=%s msg=%s\n",
                dn_endpoint.c_str(), resp.result().error_message().c_str());
        fflush(stderr);
        return false;
    }
    return true;
}

bool HeartbeatDn(const std::string& dn_endpoint,
                 int32_t store_node_id,
                 int64_t store_epoch,
                 int64_t now_ms) {
    brpc::Channel ch;
    if (!InitBrpcChannel(dn_endpoint, &ch)) {
        return false;
    }
    falconfs::kv::KVStoreAdminService_Stub stub(&ch);
    falconfs::kv::HeartbeatRequest req;
    req.mutable_meta()->set_request_id("falcon_kv_store_hb");
    req.set_store_node_id(store_node_id);
    req.set_store_epoch(store_epoch);
    req.set_now_ms(now_ms);
    falconfs::kv::HeartbeatResponse resp;
    brpc::Controller cntl;
    stub.Heartbeat(&cntl, &req, &resp, nullptr);
    return !cntl.Failed() && resp.result().success();
}

}  // namespace

int main(int argc, char** argv) {
    (void) argc;
    (void) argv;

    /* Large vLLM KV blocks in a single BatchWrite/BatchRead need a bigger cap
     * than brpc's default (~64 MiB). Applies to this process only. */
    constexpr uint64_t kMinBrpcMaxBody = 512ULL * 1024 * 1024;
    if (brpc::FLAGS_max_body_size < kMinBrpcMaxBody) {
        brpc::FLAGS_max_body_size = kMinBrpcMaxBody;
    }

    const int cn_port = GetEnvInt("FALCON_KV_STORE_CN_PGPORT", 55500);
    const int store_brpc_port = GetEnvInt("FALCON_KV_STORE_BRPC_PORT", 18765);
    const std::string dn_csv = GetEnvStr("FALCON_KV_STORE_DN_POOLERS", "55530,55550");
    const int32_t store_node_id = static_cast<int32_t>(GetEnvInt("FALCON_KV_STORE_NODE_ID", 1));
    const int64_t store_epoch = GetEnvInt64("FALCON_KV_STORE_EPOCH", 1);
    /* Default matches vLLM-style logical KV block bytes (fp16 Llama-class slice):
     * 2 * layers * kv_heads * head_dim * gpu_block_tokens * sizeof(fp16). */
    constexpr int kVllmDefaultBlockBytes = 2 * 32 * 8 * 128 * 16 * 2;
    const int32_t block_size =
        static_cast<int32_t>(GetEnvInt("FALCON_KV_STORE_BLOCK_SIZE", kVllmDefaultBlockBytes));
    const std::string bind_ip = GetEnvStr("FALCON_KV_STORE_BIND_IP", "0.0.0.0");

    const std::vector<int> dn_ports = ParseDnPoolerPorts(dn_csv);
    if (dn_ports.empty()) {
        fprintf(stderr, "[falcon_kv_store] FALCON_KV_STORE_DN_POOLERS is empty\n");
        return 2;
    }

    constexpr int64_t kBytesPerDnRegion = 64LL * 65536LL;
    const int64_t kLegacyTotalBytes = kBytesPerDnRegion * static_cast<int64_t>(dn_ports.size());
    const int64_t dram_env            = GetEnvInt64("FALCON_KV_STORE_DRAM_BYTES", 0);
    const int64_t dn_n                = static_cast<int64_t>(std::max<std::size_t>(1, dn_ports.size()));
    const int64_t bs                  = static_cast<int64_t>(block_size);
    /* At least 64 logical blocks per DN slice so multi-DN routing + batched tests
     * fit; round ``region_bytes`` so each DN slice is an integer multiple of
     * ``block_size`` (RegisterStoreRegion / pool offsets rely on alignment). */
    const int64_t min_slots_total = bs * 64LL * dn_n;
    int64_t region_bytes =
        (dram_env > 0) ? dram_env : std::max(kLegacyTotalBytes, min_slots_total);
    const int64_t align = bs * dn_n;
    if (align > 0) {
        region_bytes = (region_bytes + align - 1) / align * align;
    }
    const int64_t slice_bytes = region_bytes / dn_n;

    const std::string host_node = ResolveHostNodeName();
    const std::string advertise_host = GetEnvStr("FALCON_KV_STORE_ADVERTISE_HOST", "127.0.0.1");
    const std::string store_brpc_endpoint =
        advertise_host + ":" + std::to_string(store_brpc_port);
    const std::string runtime_dir =
        GetEnvStr("FALCON_KV_STORE_RUNTIME_DIR", "/tmp/falcon_kv_store_runtime");
    const std::string shm_name = GetEnvStr("FALCON_KV_STORE_SHM_NAME", "heap_dram_p2");

    auto store_engine = std::make_shared<falconfs::kv::KVStoreEngine>(
        store_node_id,
        /*base_offset=*/0,
        /*region_bytes=*/region_bytes,
        block_size,
        store_epoch,
        shm_name);

    auto data_impl    = std::make_shared<falconfs::kv::KVDataServiceImpl>(store_engine);
    auto data_adapter = std::make_shared<falconfs::kv::KVDataBrpcServiceAdapter>(data_impl);
    auto admin_impl   = falconfs::kv::CreateKVStoreAdminBrpcServiceImplForStore(store_engine);
    auto admin_adapter = std::make_shared<falconfs::kv::KVStoreAdminBrpcServiceAdapter>(admin_impl);

    brpc::Server server;
    g_server = &server;
    std::signal(SIGINT, OnSignal);
    std::signal(SIGTERM, OnSignal);

    if (server.AddService(data_adapter.get(), brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        fprintf(stderr, "[falcon_kv_store] AddService(KVData) failed\n");
        return 3;
    }
    if (server.AddService(admin_adapter.get(), brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        fprintf(stderr, "[falcon_kv_store] AddService(KVStoreAdmin) failed\n");
        return 3;
    }

    butil::EndPoint point;
    if (butil::str2endpoint(bind_ip.c_str(), store_brpc_port, &point) != 0) {
        fprintf(stderr, "[falcon_kv_store] invalid bind %s:%d\n", bind_ip.c_str(), store_brpc_port);
        return 3;
    }
    brpc::ServerOptions options;
    if (server.Start(point, &options) != 0) {
        fprintf(stderr, "[falcon_kv_store] brpc Start failed on port %d\n", store_brpc_port);
        return 3;
    }

    PGconn* cn = OpenCnConn(cn_port);
    if (cn == nullptr) {
        fprintf(stderr, "[falcon_kv_store] could not connect to CN postgres\n");
        server.Stop(0);
        server.Join();
        return 4;
    }
    if (!ExecStoreRegister(cn, store_node_id, host_node, advertise_host, store_brpc_port, runtime_dir,
                            shm_name, region_bytes, block_size, store_epoch)) {
        fprintf(stderr, "[falcon_kv_store] falcon_store_node_register failed\n");
        PQfinish(cn);
        server.Stop(0);
        server.Join();
        return 4;
    }

    for (std::size_t i = 0; i < dn_ports.size(); ++i) {
        const std::string ep = "127.0.0.1:" + std::to_string(dn_ports[i]);
        const int64_t base = static_cast<int64_t>(i) * slice_bytes;
        bool registered = false;
        for (int attempt = 0; attempt < 120 && !registered; ++attempt) {
            registered = RegisterRegionOnDn(ep, store_brpc_endpoint, store_node_id,
                                            static_cast<int>(i), base, slice_bytes,
                                            block_size, store_epoch, shm_name, runtime_dir);
            if (!registered) {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        }
        if (!registered) {
            fprintf(stderr, "[falcon_kv_store] RegisterStoreRegion failed for %s\n", ep.c_str());
            (void) ExecStoreUnregister(cn, store_node_id);
            PQfinish(cn);
            server.Stop(0);
            server.Join();
            return 5;
        }
    }

    std::thread hb_thread([&]() {
        while (!g_stop.load(std::memory_order_relaxed)) {
            for (int i = 0; i < 50 && !g_stop.load(std::memory_order_relaxed); ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            if (g_stop.load(std::memory_order_relaxed)) {
                break;
            }
            PGconn* c = OpenCnConn(cn_port);
            if (c != nullptr) {
                (void) ExecStoreHeartbeat(c, store_node_id, store_epoch, WallClockMillis());
                PQfinish(c);
            }
            const int64_t now_ms = WallClockMillis();
            for (std::size_t i = 0; i < dn_ports.size(); ++i) {
                const int p = dn_ports[i];
                const int64_t base = static_cast<int64_t>(i) * slice_bytes;
                (void) RegisterRegionOnDn("127.0.0.1:" + std::to_string(p),
                                          store_brpc_endpoint,
                                          store_node_id,
                                          static_cast<int>(i),
                                          base,
                                          slice_bytes,
                                          block_size,
                                          store_epoch,
                                          shm_name,
                                          runtime_dir);
                (void) HeartbeatDn("127.0.0.1:" + std::to_string(p), store_node_id, store_epoch, now_ms);
            }
        }
    });

    printf("[falcon_kv_store] ready store_node_id=%d brpc=%s dram_bytes=%lld\n", store_node_id,
           store_brpc_endpoint.c_str(), static_cast<long long>(region_bytes));
    fflush(stdout);

    server.RunUntilAskedToQuit();
    g_stop.store(true, std::memory_order_relaxed);
    if (hb_thread.joinable()) {
        hb_thread.join();
    }

    (void) ExecStoreUnregister(cn, store_node_id);
    PQfinish(cn);

    server.Stop(0);
    server.Join();
    g_server = nullptr;
    return 0;
}
