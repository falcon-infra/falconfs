/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "brpc_comm_adapter/kv_recovery_runner.h"

#include <arpa/inet.h>
#include <brpc/channel.h>
#include <libpq-fe.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <pwd.h>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include "connection_pool/kv_catalog_wire.h"
#include "kv_store_admin_service.pb.h"
#include "vllm_kv_cache/src/metadata/kv_meta_table_accessor.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_recovery.h"

namespace falcon::kv_proto {

namespace {

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

PGconn *OpenRecoveryConnection(int pg_port)
{
    if (pg_port <= 0) return nullptr;
    std::ostringstream conninfo;
    conninfo << "hostaddr=127.0.0.1 port=" << pg_port << " user=" << GetCurrentUserName()
             << " dbname=postgres application_name=falcon_kv_recovery";
    PGconn *conn = PQconnectdb(conninfo.str().c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        PQfinish(conn);
        return nullptr;
    }
    return conn;
}

int64_t NowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Calls `pg_catalog.falcon_kv_metadata_recovery_call(method, payload)` over
// `conn` and returns the response bytes (empty on error).
std::string CallRecovery(PGconn *conn, int method, const std::string &payload)
{
    std::string out;
    if (conn == nullptr) return out;

    int32_t method_be = htonl(method);
    const char *param_values[2];
    int param_lengths[2];
    int param_formats[2];
    Oid param_types[2];

    param_values[0] = reinterpret_cast<const char *>(&method_be);
    param_lengths[0] = sizeof(int32_t);
    param_formats[0] = 1; /* binary */
    param_types[0] = 23;  /* int4 */

    param_values[1] = payload.data();
    param_lengths[1] = static_cast<int>(payload.size());
    param_formats[1] = 1; /* binary */
    param_types[1] = 17;  /* bytea */

    PGresult *res = PQexecParams(
        conn,
        "SELECT pg_catalog.falcon_kv_metadata_recovery_call($1, $2)",
        2, param_types, param_values, param_lengths, param_formats, 1);
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1 &&
        PQnfields(res) == 1 && !PQgetisnull(res, 0, 0)) {
        out.assign(PQgetvalue(res, 0, 0), PQgetlength(res, 0, 0));
    }
    PQclear(res);
    return out;
}

// Minimal libpq-backed IKVMetaTableAccessor that only implements the methods
// `RecoverMetadataFromAccessor` actually invokes: LoadDnEpoch, BumpDnEpoch,
// and ScanForRecovery. Other operations return defaults so a misuse does not
// silently corrupt persistent state.
class LibpqRecoveryAccessor : public ::falconfs::kv::IKVMetaTableAccessor {
public:
    LibpqRecoveryAccessor(PGconn *conn, int32_t shard_id) : conn_(conn), shard_id_(shard_id) {}

    bool Lookup(int32_t, const std::string &, ::falconfs::kv::AccessorRow *) override
    {
        return false;
    }
    bool InsertAllocated(int32_t, const std::string &,
                         const ::falconfs::kv::AccessorInsertSpec &, int64_t) override
    {
        return false;
    }
    ::falconfs::kv::AccessorCASResult CASStatusUpdate(int32_t, const std::string &,
                                                       int32_t, int32_t, int64_t,
                                                       const std::string &, int64_t) override
    {
        ::falconfs::kv::AccessorCASResult r;
        r.success = false;
        return r;
    }
    bool Delete(int32_t, const std::string &, int64_t, bool *) override
    {
        return false;
    }
    bool UpsertForRecovery(int32_t, const std::string &,
                           const ::falconfs::kv::AccessorRow &) override
    {
        return false;
    }

    void ScanForRecovery(int32_t /*shard_id*/, const std::vector<int32_t> &wanted,
                         const RowCallback &cb) override
    {
        std::string resp = CallRecovery(conn_, KV_CATALOG_METHOD_SCAN_FOR_RECOVERY,
                                        std::string());
        if (resp.size() < sizeof(uint32_t)) return;
        uint32_t row_count = 0;
        std::memcpy(&row_count, resp.data(), sizeof(uint32_t));
        const char *cursor = resp.data() + sizeof(uint32_t);
        const char *end = resp.data() + resp.size();
        const std::size_t row_size = sizeof(KVCatalogRecoveryRow);

        for (uint32_t i = 0; i < row_count; ++i) {
            if (cursor + row_size > end) break;
            KVCatalogRecoveryRow w;
            std::memcpy(&w, cursor, sizeof(w));
            cursor += row_size;

            // Status filter: skip any row whose status is not in `wanted`.
            // (RecoverMetadataFromAccessor passes ALLOCATED/STORED/EVICTING/EVICTED.)
            const int32_t status = static_cast<int32_t>(w.status);
            bool match = false;
            for (int32_t wnt : wanted) {
                if (wnt == status) { match = true; break; }
            }
            if (!match) continue;

            ::falconfs::kv::AccessorRow row;
            row.kv_group_idx = 0;
            row.layer_mask = 0;
            row.status = status;
            row.store_node_id = w.store_node_id;
            row.pool_offset = w.pool_offset;
            if (w.evicted_path_len > 0) {
                row.evicted_path.assign(w.evicted_path,
                                        static_cast<size_t>(w.evicted_path_len));
            }
            row.version = w.version;
            row.updated_at_ms = w.updated_at_ms;

            std::string block_hash(reinterpret_cast<const char *>(w.block_hash),
                                   static_cast<size_t>(w.block_hash_len));
            cb(block_hash, row);
        }
    }

    int64_t LoadDnEpoch(int32_t /*shard_id*/) override
    {
        KVCatalogDnEpochRequest req{};
        req.shard_id = shard_id_;
        std::string payload(reinterpret_cast<const char *>(&req), sizeof(req));
        std::string resp = CallRecovery(conn_, KV_CATALOG_METHOD_LOAD_DN_EPOCH, payload);
        if (resp.size() < sizeof(KVCatalogDnEpochResponse)) return 1;
        KVCatalogDnEpochResponse r{};
        std::memcpy(&r, resp.data(), sizeof(r));
        return r.dn_epoch;
    }

    int64_t BumpDnEpoch(int32_t /*shard_id*/) override
    {
        KVCatalogDnEpochRequest req{};
        req.shard_id = shard_id_;
        std::string payload(reinterpret_cast<const char *>(&req), sizeof(req));
        std::string resp = CallRecovery(conn_, KV_CATALOG_METHOD_BUMP_DN_EPOCH, payload);
        if (resp.size() < sizeof(KVCatalogDnEpochResponse)) return 1;
        KVCatalogDnEpochResponse r{};
        std::memcpy(&r, resp.data(), sizeof(r));
        return r.dn_epoch;
    }

private:
    PGconn *conn_;
    int32_t shard_id_;
};

}  // namespace

KVRecoveryRunner::KVRecoveryRunner(std::shared_ptr<::falconfs::kv::KVMetadataEngine> engine,
                                   int pg_port, int32_t shard_id)
    : engine_(std::move(engine)), pg_port_(pg_port), shard_id_(shard_id)
{
}

KVRecoveryStats KVRecoveryRunner::Run()
{
    KVRecoveryStats stats;
    if (engine_ == nullptr) return stats;

    PGconn *conn = OpenRecoveryConnection(pg_port_);
    if (conn == nullptr) {
        // Could not even connect; on a fresh cluster this is fine (the catalog
        // is empty and the engine starts clean), but we cannot persistently
        // bump dn_epoch. Leave stats.ok = false so the caller can warn.
        return stats;
    }

    LibpqRecoveryAccessor accessor(conn, shard_id_);
    ::falconfs::kv::MetadataRecoveryStats r =
        ::falconfs::kv::RecoverMetadataFromAccessor(&accessor, engine_.get(), shard_id_, NowMs());

    PQfinish(conn);

    stats.ok = true;
    stats.bumped_dn_epoch = r.bumped_dn_epoch;
    stats.recovered_rows = r.recovered_rows;
    stats.reconciled_evicting = r.reconciled_evicting_rows;
    return stats;
}

KVRecoveryStats KVRecoveryRunner::ReplayRecoverOnly()
{
    KVRecoveryStats stats;
    if (engine_ == nullptr) {
        return stats;
    }

    PGconn *conn = OpenRecoveryConnection(pg_port_);
    if (conn == nullptr) {
        return stats;
    }

    LibpqRecoveryAccessor accessor(conn, shard_id_);
    ::falconfs::kv::MetadataRecoveryStats r =
        ::falconfs::kv::ReplayRecoverMetadataFromAccessor(&accessor, engine_.get(), shard_id_, NowMs());
    PQfinish(conn);

    stats.ok                = true;
    stats.recovered_rows    = r.recovered_rows;
    stats.reconciled_evicting = r.reconciled_evicting_rows;
    return stats;
}

KVStoreRestartReconcileStats KVRecoveryRunner::ReconcileStoreRestart(int32_t store_node_id,
                                                                       const std::string& store_endpoint)
{
    KVStoreRestartReconcileStats stats;
    PGconn *conn = OpenRecoveryConnection(pg_port_);
    if (conn == nullptr) {
        return stats;
    }

    KVCatalogStoreRestartRequest req{};
    req.store_node_id = store_node_id;
    req.now_ms = NowMs();
    std::string payload(reinterpret_cast<const char *>(&req), sizeof(req));
    std::string resp = CallRecovery(conn, KV_CATALOG_METHOD_RECONCILE_STORE_RESTART, payload);

    if (resp.size() < sizeof(KVCatalogStoreRestartResponse)) {
        PQfinish(conn);
        return stats;
    }
    KVCatalogStoreRestartResponse wire{};
    std::memcpy(&wire, resp.data(), sizeof(wire));
    stats.ok = true;
    stats.scanned_rows = wire.scanned_rows;
    stats.deleted_rows = wire.deleted_rows;
    stats.preserved_evicted_rows = wire.preserved_evicted_rows;

    if (wire.preserved_evicted_rows <= 0 || store_endpoint.empty()) {
        PQfinish(conn);
        return stats;
    }

    KVCatalogStoreEvictedScanRequest scan_req{};
    scan_req.store_node_id = store_node_id;
    scan_req.max_rows = 1024;
    std::string scan_payload(reinterpret_cast<const char *>(&scan_req), sizeof(scan_req));
    std::string scan_resp = CallRecovery(conn, KV_CATALOG_METHOD_SCAN_STORE_EVICTED, scan_payload);
    if (scan_resp.size() < sizeof(uint32_t)) {
        stats.validation_failed_rows += wire.preserved_evicted_rows;
        PQfinish(conn);
        return stats;
    }
    uint32_t row_count = 0;
    std::memcpy(&row_count, scan_resp.data(), sizeof(uint32_t));
    const char *cursor = scan_resp.data() + sizeof(uint32_t);
    const char *end = scan_resp.data() + scan_resp.size();

    brpc::Channel channel;
    brpc::ChannelOptions opts;
    opts.protocol = "baidu_std";
    opts.connection_type = "pooled";
    opts.timeout_ms = 5000;
    opts.connect_timeout_ms = 1000;
    opts.max_retry = 0;
    if (channel.Init(store_endpoint.c_str(), &opts) != 0) {
        stats.validation_failed_rows += row_count;
        PQfinish(conn);
        return stats;
    }
    falconfs::kv::KVStoreAdminService_Stub stub(&channel);
    falconfs::kv::ValidateEvictedPathsRequest vreq;
    vreq.mutable_meta()->set_request_id("store_restart_validate:" + std::to_string(store_node_id));
    vreq.set_store_node_id(store_node_id);

    std::vector<KVCatalogStoreEvictedRow> rows;
    rows.reserve(row_count);
    for (uint32_t i = 0; i < row_count; ++i) {
        if (cursor + sizeof(KVCatalogStoreEvictedRow) > end) break;
        KVCatalogStoreEvictedRow row{};
        std::memcpy(&row, cursor, sizeof(row));
        cursor += sizeof(row);
        rows.push_back(row);
        auto *item = vreq.add_items();
        item->set_block_hash(row.block_hash, row.block_hash_len);
        item->set_evicted_path(row.evicted_path, row.evicted_path_len);
    }
    if (rows.empty()) {
        PQfinish(conn);
        return stats;
    }

    falconfs::kv::ValidateEvictedPathsResponse vresp;
    brpc::Controller cntl;
    stub.ValidateEvictedPaths(&cntl, &vreq, &vresp, nullptr);
    if (cntl.Failed() || !vresp.result().success() ||
        vresp.results_size() != static_cast<int>(rows.size())) {
        stats.validation_failed_rows += static_cast<int64_t>(rows.size());
        PQfinish(conn);
        return stats;
    }

    std::vector<KVCatalogDeleteInvalidEvictedItem> invalid;
    for (int i = 0; i < vresp.results_size(); ++i) {
        if (vresp.results(i).valid()) {
            ++stats.validated_evicted_rows;
            continue;
        }
        KVCatalogDeleteInvalidEvictedItem item{};
        item.block_hash_len = rows[static_cast<size_t>(i)].block_hash_len;
        std::memcpy(item.block_hash, rows[static_cast<size_t>(i)].block_hash, item.block_hash_len);
        item.expected_version = rows[static_cast<size_t>(i)].version;
        invalid.push_back(item);
    }
    if (!invalid.empty()) {
        std::string del_payload;
        uint32_t count = static_cast<uint32_t>(invalid.size());
        del_payload.resize(sizeof(uint32_t) + invalid.size() * sizeof(KVCatalogDeleteInvalidEvictedItem));
        std::memcpy(del_payload.data(), &count, sizeof(uint32_t));
        std::memcpy(del_payload.data() + sizeof(uint32_t), invalid.data(),
                    invalid.size() * sizeof(KVCatalogDeleteInvalidEvictedItem));
        std::string del_resp = CallRecovery(conn, KV_CATALOG_METHOD_DELETE_STORE_EVICTED_INVALID,
                                            del_payload);
        if (del_resp.size() >= sizeof(uint32_t)) {
            uint32_t del_count = 0;
            std::memcpy(&del_count, del_resp.data(), sizeof(uint32_t));
            const auto *results = reinterpret_cast<const KVCatalogDeleteInvalidEvictedResult *>(
                del_resp.data() + sizeof(uint32_t));
            for (uint32_t i = 0; i < del_count && i < invalid.size(); ++i) {
                if (results[i].deleted) ++stats.invalid_deleted_rows;
            }
        }
    }
    PQfinish(conn);
    return stats;
}

}  // namespace falcon::kv_proto
