/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "brpc_comm_adapter/falcon_brpc_server.h"

#include <brpc/server.h>
#include <libpq-fe.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <pwd.h>
#include <sstream>
#include <thread>
#include <unistd.h>

#include "base_comm_adapter/base_meta_service_job.h"
#include "brpc_comm_adapter/brpc_kv_service_imp.h"
#include "brpc_comm_adapter/brpc_meta_service_imp.h"
#include "brpc_comm_adapter/kv_eviction_worker.h"
#include "brpc_comm_adapter/kv_recovery_runner.h"
#include "brpc_comm_adapter/kv_runtime_register.h"
#include "connection_pool/falcon_kv_config.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_service_impl.h"
#include "vllm_kv_cache/src/service/kv_store_admin_brpc_service.h"

extern "C" {
extern int FalconPGPort;
}

namespace {

constexpr int32_t kKvShardId = 1;

/* Match ``falcon_kv_store`` default (``FALCON_KV_STORE_BLOCK_SIZE``): vLLM logical
 * KV block bytes for a representative fp16 slice (2 * L * Hkv * D * tokens * 2). */
constexpr int32_t kVllmDefaultKvBlockBytes = 2 * 32 * 8 * 128 * 16 * 2;

int32_t ResolveDnKvBlockSize()
{
    const char *v = std::getenv("FALCON_KV_STORE_BLOCK_SIZE");
    if (v == nullptr || v[0] == '\0') {
        return kVllmDefaultKvBlockBytes;
    }
    char *end = nullptr;
    long x = std::strtol(v, &end, 10);
    if (end == v || x <= 0 || x > 2147483647L) {
        return kVllmDefaultKvBlockBytes;
    }
    return static_cast<int32_t>(x);
}

std::string GetCurrentUserName()
{
    const char *u = std::getenv("USER");
    if (u != nullptr && u[0] != '\0') return u;
    u = std::getenv("PGUSER");
    if (u != nullptr && u[0] != '\0') return u;
    struct passwd *pw = getpwuid(getuid());
    if (pw != nullptr && pw->pw_name != nullptr) return pw->pw_name;
    return "postgres";
}

PGconn *OpenPostgresConn(int pg_port)
{
    if (pg_port <= 0) return nullptr;
    std::ostringstream conninfo;
    conninfo << "hostaddr=127.0.0.1 port=" << pg_port << " user=" << GetCurrentUserName()
             << " dbname=postgres application_name=falcon_kv_dn_bgworker";
    PGconn *conn = PQconnectdb(conninfo.str().c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        PQfinish(conn);
        return nullptr;
    }
    return conn;
}

/* One-time bootstrap: open a temporary libpq connection from the bgworker and
 * call `pg_catalog.falcon_create_kvblock_table()` so the schema exists before
 * any pool-worker accepts a KV catalog round-trip. The connection is closed
 * immediately afterwards; per-worker libpq connections in PGConnectionPool
 * handle the actual hot path. */
void EnsureKvblockTableOnce(int pg_port)
{
    PGconn *conn = OpenPostgresConn(pg_port);
    if (conn == nullptr) return;
    PGresult *res = PQexec(conn, "SELECT pg_catalog.falcon_create_kvblock_table()");
    PQclear(res);
    PQfinish(conn);
}

int64_t WallClockMillis()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::string ResolveHostNodeName()
{
    const char *env = std::getenv("NODE_NAME");
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

std::string ResolvePgHostForCatalog(const std::string& serverIp)
{
    if (serverIp.empty()) return "127.0.0.1";
    butil::ip_t ip{};
    if (butil::str2ip(serverIp.c_str(), &ip) == 0) {
        butil::IPStr s = butil::ip2str(ip);
        return std::string(s.c_str());
    }
    return "127.0.0.1";
}

int32_t QueryLocalForeignServerId(PGconn *conn)
{
    PGresult *res = PQexec(
        conn,
        "SELECT server_id FROM pg_catalog.falcon_foreign_server "
        "WHERE is_local IS TRUE ORDER BY server_id LIMIT 1;");
    if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1) {
        PQclear(res);
        res = PQexec(conn,
                     "SELECT server_id FROM pg_catalog.falcon_foreign_server ORDER BY server_id LIMIT 1;");
    }
    int32_t sid = -1;
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) >= 1 && !PQgetisnull(res, 0, 0)) {
        sid = static_cast<int32_t>(std::strtol(PQgetvalue(res, 0, 0), nullptr, 10));
    }
    PQclear(res);
    return sid;
}

bool ExecFalconDnNodeRegister(PGconn *conn,
                             int32_t server_id,
                             const std::string& host_node,
                             const std::string& pg_host,
                             int pg_port,
                             int kv_brpc_port,
                             int64_t dn_epoch)
{
    const char *sql =
        "SELECT pg_catalog.falcon_dn_node_register($1::int, $2::text, $3::text, $4::int, $5::int, $6::bigint);";
    const char *vals[6];
    std::string s_id  = std::to_string(server_id);
    std::string s_pgp = std::to_string(pg_port);
    std::string s_brp = std::to_string(kv_brpc_port);
    std::string s_ep  = std::to_string(static_cast<long long>(dn_epoch));
    vals[0]           = s_id.c_str();
    vals[1]           = host_node.c_str();
    vals[2]           = pg_host.c_str();
    vals[3]           = s_pgp.c_str();
    vals[4]           = s_brp.c_str();
    vals[5]           = s_ep.c_str();
    const int lens[6] = {
        static_cast<int>(s_id.size()),
        static_cast<int>(host_node.size()),
        static_cast<int>(pg_host.size()),
        static_cast<int>(s_pgp.size()),
        static_cast<int>(s_brp.size()),
        static_cast<int>(s_ep.size()),
    };
    const int fmts[6] = {0, 0, 0, 0, 0, 0};
    Oid types[6]      = {23, 25, 25, 23, 23, 20};

    PGresult *res = PQexecParams(conn, sql, 6, types, vals, lens, fmts, 0);
    const bool ok = (PQresultStatus(res) == PGRES_TUPLES_OK);
    if (!ok) {
        fprintf(stderr, "[FalconBrpcServer] falcon_dn_node_register failed: %s\n", PQerrorMessage(conn));
        fflush(stderr);
    }
    PQclear(res);
    return ok;
}

bool ExecFalconDnNodeHeartbeat(PGconn *conn, int32_t server_id, int64_t now_ms)
{
    const char *sql = "SELECT pg_catalog.falcon_dn_node_heartbeat($1::int, $2::bigint);";
    std::string s_id = std::to_string(server_id);
    std::string s_ms = std::to_string(static_cast<long long>(now_ms));
    const char *vals[2]  = {s_id.c_str(), s_ms.c_str()};
    const int lens[2]    = {static_cast<int>(s_id.size()), static_cast<int>(s_ms.size())};
    const int fmts[2]    = {0, 0};
    Oid types[2]       = {23, 20};
    PGresult *res      = PQexecParams(conn, sql, 2, types, vals, lens, fmts, 0);
    const bool ok      = (PQresultStatus(res) == PGRES_TUPLES_OK);
    PQclear(res);
    return ok;
}

bool ExecFalconKvMembershipWatchdogTick(PGconn *conn, int64_t now_ms, int64_t skew_ms)
{
    const char *sql =
        "SELECT pg_catalog.falcon_kv_membership_watchdog_tick($1::bigint, $2::bigint);";
    std::string s_now  = std::to_string(static_cast<long long>(now_ms));
    std::string s_skew = std::to_string(static_cast<long long>(skew_ms));
    const char *vals[2]  = {s_now.c_str(), s_skew.c_str()};
    const int lens[2]    = {static_cast<int>(s_now.size()), static_cast<int>(s_skew.size())};
    const int fmts[2]    = {0, 0};
    Oid types[2]         = {20, 20};
    PGresult *res        = PQexecParams(conn, sql, 2, types, vals, lens, fmts, 0);
    const bool ok        = (PQresultStatus(res) == PGRES_TUPLES_OK);
    if (!ok) {
        fprintf(stderr, "[FalconBrpcServer] falcon_kv_membership_watchdog_tick failed: %s\n",
                PQerrorMessage(conn));
        fflush(stderr);
    }
    PQclear(res);
    return ok;
}

void KvMembershipWatchdogThreadMain(int pg_port, std::atomic<bool> *stop_flag)
{
    while (!stop_flag->load(std::memory_order_relaxed)) {
        const int period_ms =
            FalconKvWatchdogPeriodMs > 0 ? FalconKvWatchdogPeriodMs : 1000;
        for (int slept = 0; slept < period_ms && !stop_flag->load(std::memory_order_relaxed);
             slept += 100) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (stop_flag->load(std::memory_order_relaxed)) break;
        const int64_t skew_ms =
            FalconKvWatchdogSkewMs > 0 ? static_cast<int64_t>(FalconKvWatchdogSkewMs) : 30000LL;
        PGconn *conn = OpenPostgresConn(pg_port);
        if (conn == nullptr) continue;
        (void) ExecFalconKvMembershipWatchdogTick(conn, WallClockMillis(), skew_ms);
        PQfinish(conn);
    }
}

bool ExecFalconDnNodeUnregister(PGconn *conn, int32_t server_id)
{
    const char *sql   = "SELECT pg_catalog.falcon_dn_node_unregister($1::int);";
    std::string s_id  = std::to_string(server_id);
    const char *vals[1]  = {s_id.c_str()};
    const int lens[1]    = {static_cast<int>(s_id.size())};
    const int fmts[1]    = {0};
    Oid types[1]         = {23};
    PGresult *res        = PQexecParams(conn, sql, 1, types, vals, lens, fmts, 0);
    const bool ok        = (PQresultStatus(res) == PGRES_TUPLES_OK);
    PQclear(res);
    return ok;
}

void DnMembershipHeartbeatThreadMain(int32_t server_id,
                                     int pg_port,
                                     std::atomic<bool> *stop_flag)
{
    while (!stop_flag->load(std::memory_order_relaxed)) {
        for (int i = 0; i < 50 && !stop_flag->load(std::memory_order_relaxed); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        if (stop_flag->load(std::memory_order_relaxed)) break;
        PGconn *conn = OpenPostgresConn(pg_port);
        if (conn == nullptr) continue;
        (void) ExecFalconDnNodeHeartbeat(conn, server_id, WallClockMillis());
        PQfinish(conn);
    }
}

}  // namespace

class FalconBrpcServer {
  public:
    FalconBrpcServer(falcon_meta_job_dispatch_func dispatchFun, const char *serverIp, int port)
        : m_jobDispatchFunc(dispatchFun),
          m_serverIp(serverIp ? serverIp : ""),
          m_port(port)
    {
    }

    void Run()
    {
        falcon::meta_proto::BrpcMetaServiceImpl BrpcMetaServiceImpl(m_jobDispatchFunc);
        if (m_server.AddService(&BrpcMetaServiceImpl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            throw std::runtime_error("FalconBrpcServer: brpc server AddService failed");
        }

        /* v6.5 P1/P2: start with no registered Store region; falcon_kv_store
         * registers slices via KVStoreAdminService::RegisterStoreRegion and the
         * recovery scan replays parked rows on each successful registration. */
        m_kvMetadataEngine = std::make_shared<falconfs::kv::KVMetadataEngine>(
            /*store_node_id=*/1,
            /*dn_id=*/1,
            /*region_bytes=*/0,
            /*block_size=*/ResolveDnKvBlockSize(),
            /*dn_epoch=*/1,
            /*store_epoch=*/1,
            /*kvblock_shard_id=*/kKvShardId);
        m_kvMetadataEngine->SetCatalogTier(falconfs::kv::EngineCatalogTier::REMOTE_LIBPQ);
        m_kvMetadataImpl = std::make_shared<falconfs::kv::KVMetadataServiceImpl>(m_kvMetadataEngine);

        EnsureKvblockTableOnce(FalconPGPort);

        falcon::kv_proto::KVRecoveryRunner recoveryRunner(m_kvMetadataEngine, FalconPGPort,
                                                          kKvShardId);
        (void) recoveryRunner.Run();

        /* v6.5: CN catalog membership row for this DN (libpq, not SPI). */
        if (FalconPGPort > 0) {
            PGconn *regConn = OpenPostgresConn(FalconPGPort);
            if (regConn != nullptr) {
                m_localServerId = QueryLocalForeignServerId(regConn);
                if (m_localServerId >= 0) {
                    const std::string node = ResolveHostNodeName();
                    const std::string pgHost = ResolvePgHostForCatalog(m_serverIp);
                    if (ExecFalconDnNodeRegister(regConn, m_localServerId, node, pgHost, FalconPGPort,
                                               m_port, m_kvMetadataEngine->DnEpoch())) {
                        m_dnCatalogRegistered = true;
                    }
                } else {
                    fprintf(stderr, "[FalconBrpcServer] could not resolve local falcon_foreign_server id\n");
                    fflush(stderr);
                }
                PQfinish(regConn);
            }
        }

        if (m_dnCatalogRegistered) {
            m_dnHbStop.store(false, std::memory_order_relaxed);
            m_dnHbThread = std::thread(DnMembershipHeartbeatThreadMain, m_localServerId, FalconPGPort,
                                       &m_dnHbStop);
        }

        if (FalconPGPort > 0) {
            m_kvWatchdogStop.store(false, std::memory_order_relaxed);
            m_kvWatchdogThread =
                std::thread(KvMembershipWatchdogThreadMain, FalconPGPort, &m_kvWatchdogStop);
        }

        falcon::kv_proto::KVRuntimeRegister::Install(m_kvMetadataImpl);

        /* v6.5 P2: KV data lives on falcon_kv_store; DN is metadata + admin only.
         * Eviction spill is wired to the Store over BRPC in P3 — keep the worker
         * thread alive with a null store engine so catalog CAS paths stay warm. */
        m_kvEvictionWorker = std::make_unique<falcon::kv_proto::KVEvictionWorker>(
            m_kvMetadataEngine, m_kvMetadataImpl, /*store_engine=*/nullptr, FalconPGPort);
        m_kvEvictionWorker->Start();

        falcon::kv_proto::BrpcKVMetadataServiceImpl kvMetadataBrpcService(m_jobDispatchFunc);
        auto kvStoreAdminImpl = falconfs::kv::CreateKVStoreAdminBrpcServiceImplForDn(
            m_kvMetadataEngine,
            [&recoveryRunner]() { (void) recoveryRunner.ReplayRecoverOnly(); },
            [&recoveryRunner](int32_t store_node_id, const std::string& store_endpoint) {
                auto stats = recoveryRunner.ReconcileStoreRestart(store_node_id, store_endpoint);
                fprintf(stderr,
                        "[FalconBrpcServer] Store restart reconcile store=%d ok=%d scanned=%ld deleted=%ld preserved=%ld validated=%ld invalid_deleted=%ld validation_failed=%ld\n",
                        store_node_id,
                        stats.ok ? 1 : 0,
                        (long) stats.scanned_rows,
                        (long) stats.deleted_rows,
                        (long) stats.preserved_evicted_rows,
                        (long) stats.validated_evicted_rows,
                        (long) stats.invalid_deleted_rows,
                        (long) stats.validation_failed_rows);
                fflush(stderr);
            });
        falconfs::kv::KVStoreAdminBrpcServiceAdapter kvStoreAdminBrpcService(kvStoreAdminImpl);

        if (m_server.AddService(&kvMetadataBrpcService, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            throw std::runtime_error("FalconBrpcServer: brpc server AddService(KV metadata) failed");
        }
        if (m_server.AddService(&kvStoreAdminBrpcService, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            throw std::runtime_error("FalconBrpcServer: brpc server AddService(KV store admin) failed");
        }

        butil::ip_t brpcServerIp;
        int ret = butil::str2ip(m_serverIp.c_str(), &brpcServerIp);
        if (ret != 0) {
            printf("FalconBrpcServer: failed to convert %s to brpc ip_t type, using wildcard.\n",
                   m_serverIp.c_str());
            brpcServerIp = butil::IP_ANY;
        } else {
            printf("FalconBrpcServer: convert %s to brpc ip_t type success, using it as server ip.\n",
                   m_serverIp.c_str());
        }

        butil::EndPoint point;
        point = butil::EndPoint(brpcServerIp, m_port);
        brpc::ServerOptions options;
        if (m_server.Start(point, &options) != 0) {
            throw std::runtime_error("FalconBrpcServer: failed to start server.");
        }

        m_server.RunUntilAskedToQuit();
    }

    void Shutdown()
    {
        if (m_kvEvictionWorker) {
            m_kvEvictionWorker->Stop();
            m_kvEvictionWorker.reset();
        }

        m_kvWatchdogStop.store(true, std::memory_order_relaxed);
        if (m_kvWatchdogThread.joinable()) {
            m_kvWatchdogThread.join();
        }

        m_dnHbStop.store(true, std::memory_order_relaxed);
        if (m_dnHbThread.joinable()) {
            m_dnHbThread.join();
        }

        if (m_dnCatalogRegistered && m_localServerId >= 0 && FalconPGPort > 0) {
            PGconn *c = OpenPostgresConn(FalconPGPort);
            if (c != nullptr) {
                (void) ExecFalconDnNodeUnregister(c, m_localServerId);
                PQfinish(c);
            }
            m_dnCatalogRegistered = false;
        }

        m_server.Stop(0);
        m_server.Join();
        falcon::kv_proto::KVRuntimeRegister::Uninstall();
    }

  private:
    falcon_meta_job_dispatch_func m_jobDispatchFunc;
    std::string m_serverIp;
    int m_port{};
    brpc::Server m_server;

    std::shared_ptr<falconfs::kv::KVMetadataEngine> m_kvMetadataEngine;
    std::shared_ptr<falconfs::kv::KVMetadataServiceImpl> m_kvMetadataImpl;
    std::unique_ptr<falcon::kv_proto::KVEvictionWorker> m_kvEvictionWorker;

    std::atomic<bool> m_dnHbStop{false};
    std::thread m_dnHbThread;
    std::atomic<bool> m_kvWatchdogStop{false};
    std::thread m_kvWatchdogThread;
    bool m_dnCatalogRegistered{false};
    int32_t m_localServerId{-1};
};

static std::unique_ptr<FalconBrpcServer> g_falconBrpcServerInstance = NULL;
extern "C" {
void __gcov_exit(void) __attribute__((weak));
void __gcov_dump(void) __attribute__((weak));
}

int StartFalconCommunicationServer(falcon_meta_job_dispatch_func dispatchFunc,
                                   const char *serverIp,
                                   int serverListenPort)
{
    try {
        if (g_falconBrpcServerInstance == NULL) {
            g_falconBrpcServerInstance = std::make_unique<FalconBrpcServer>(dispatchFunc, serverIp, serverListenPort);
            g_falconBrpcServerInstance->Run();
            return true;
        }
    } catch (const std::runtime_error &e) {
        printf("%s", e.what());
        fflush(stdout);
        return 1;
    }
    return 0;
}

int StopFalconCommunicationServer()
{
    try {
        if (g_falconBrpcServerInstance != NULL) {
            g_falconBrpcServerInstance->Shutdown();
            g_falconBrpcServerInstance = NULL;
            return 0;
        }
    } catch (const std::exception &e) {
        printf("%s", e.what());
        fflush(stdout);
        return 1;
    }
    return 1;
}

void FlushFalconCommunicationCoverageData(void)
{
    if (__gcov_exit != NULL) {
        __gcov_exit();
    }
    if (__gcov_dump != NULL) {
        __gcov_dump();
    }
}
