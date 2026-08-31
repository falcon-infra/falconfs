/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "brpc_comm_adapter/kv_runtime_register.h"

#include <arpa/inet.h>
#include <libpq-fe.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>

#include "base_comm_adapter/base_kv_cache_service_job.h"
#include "connection_pool/falcon_kv_runtime_bridge.h"
#include "connection_pool/kv_catalog_wire.h"
#include "kv_metadata_service.pb.h"

namespace falcon::kv_proto {

namespace {

/* Process-shared engine impl. Set by Install() at plugin startup, read by the
 * per-job callback that runs on connection-pool worker threads. shared_ptr
 * copy is thread-safe; we never replace it after Install. */
std::shared_ptr<::falconfs::kv::KVMetadataServiceImpl> g_kv_impl;
std::mutex g_kv_impl_mu;

std::shared_ptr<::falconfs::kv::KVMetadataServiceImpl> AcquireImpl()
{
    std::lock_guard<std::mutex> lk(g_kv_impl_mu);
    return g_kv_impl;
}

/* One libpq round-trip per sub-batch. The pool worker hands us its own
 * dedicated PGconn so concurrent KV BRPC calls fan out across many PG
 * backends in parallel rather than serializing through one connection
 * (v6.4 \u00a74.1.1, mirroring FalconFS BatchWorkerTask::DoWork). */
std::string CallCatalog(PGconn *conn, int catalog_method, const std::string &payload)
{
    std::string out;
    if (conn == nullptr) {
        return out;
    }
    int32_t method_be = htonl(catalog_method);
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

    PGresult *res =
        PQexecParams(conn, "SELECT pg_catalog.falcon_kv_metadata_catalog_call($1, $2)", 2,
                     param_types, param_values, param_lengths, param_formats, 1);
    if (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) == 1 && PQnfields(res) == 1 &&
        !PQgetisnull(res, 0, 0)) {
        const char *p = PQgetvalue(res, 0, 0);
        int n = PQgetlength(res, 0, 0);
        out.assign(p, n);
    }
    PQclear(res);
    return out;
}

/* Wire-format packers / unpackers (POD <-> protobuf). */

void CopyBlockHashIn(const std::string &src, uint8_t *dst, uint16_t *dst_len)
{
    uint16_t n = static_cast<uint16_t>(
        src.size() <= KV_CATALOG_BLOCK_HASH_MAX_LEN ? src.size() : KV_CATALOG_BLOCK_HASH_MAX_LEN);
    std::memset(dst, 0, KV_CATALOG_BLOCK_HASH_MAX_LEN);
    if (n > 0) {
        std::memcpy(dst, src.data(), n);
    }
    *dst_len = n;
}

void CopyEvictedPathIn(const std::string &src, char *dst, uint16_t *dst_len)
{
    size_t n = src.size();
    if (n >= KV_CATALOG_EVICTED_PATH_MAX_LEN) {
        n = KV_CATALOG_EVICTED_PATH_MAX_LEN - 1;
    }
    if (n > 0) {
        std::memcpy(dst, src.data(), n);
    }
    dst[n] = '\0';
    *dst_len = static_cast<uint16_t>(n);
}

void FillOk(::falconfs::kv::ItemResultMeta *m)
{
    m->set_success(true);
    m->set_error_code(::falconfs::kv::ErrorCode::OK);
    m->set_retryable(false);
    m->set_error_message("");
}

void FillErr(::falconfs::kv::ItemResultMeta *m, ::falconfs::kv::ErrorCode code, bool retryable,
             const char *msg)
{
    m->set_success(false);
    m->set_error_code(code);
    m->set_retryable(retryable);
    m->set_error_message(msg ? msg : "");
}

/* The four catalog callbacks. Each is invoked by `KVMetadataServiceImpl::Batch*SplitForPoolWorker`
 * with a serialized sub-batch; we pack to POD, send one libpq round-trip,
 * unpack back to a serialized protobuf response. */

std::string CatalogLookup(PGconn *conn, const std::string &sub_payload)
{
    using namespace ::falconfs::kv;
    BatchLookupRequest req;
    if (!req.ParseFromString(sub_payload)) return {};
    const uint32_t count = static_cast<uint32_t>(req.items_size());
    std::string buf;
    buf.resize(sizeof(uint32_t) + count * sizeof(KVCatalogLookupItem));
    std::memcpy(buf.data(), &count, sizeof(uint32_t));
    KVCatalogLookupItem *items =
        reinterpret_cast<KVCatalogLookupItem *>(buf.data() + sizeof(uint32_t));
    for (uint32_t i = 0; i < count; ++i) {
        std::memset(&items[i], 0, sizeof(KVCatalogLookupItem));
        CopyBlockHashIn(req.items(i).block_hash(), items[i].block_hash, &items[i].block_hash_len);
    }
    std::string resp_bytes = CallCatalog(conn, ::KV_CATALOG_METHOD_LOOKUP, buf);

    BatchLookupResponse rsp;
    if (resp_bytes.size() < sizeof(uint32_t)) return {};
    uint32_t resp_count = 0;
    std::memcpy(&resp_count, resp_bytes.data(), sizeof(uint32_t));
    if (resp_count != count ||
        resp_bytes.size() < sizeof(uint32_t) + count * sizeof(KVCatalogLookupResult)) {
        return {};
    }
    const KVCatalogLookupResult *results = reinterpret_cast<const KVCatalogLookupResult *>(
        resp_bytes.data() + sizeof(uint32_t));
    for (uint32_t i = 0; i < count; ++i) {
        auto *r = rsp.add_results();
        r->set_block_hash(req.items(i).block_hash());
        if (!results[i].found) {
            FillErr(r->mutable_result(), ErrorCode::NOT_FOUND, false, "");
            r->set_cacheable(false);
            continue;
        }
        FillOk(r->mutable_result());
        r->set_status(static_cast<BlockStatus>(results[i].status));
        auto *loc = r->mutable_location();
        loc->set_store_node_id(results[i].store_node_id);
        loc->set_pool_offset(results[i].pool_offset);
        if (results[i].evicted_path_len > 0) {
            loc->set_evicted_path(std::string(results[i].evicted_path,
                                              results[i].evicted_path_len));
        }
        loc->set_store_epoch(0);
        r->set_version(results[i].version);
        r->set_cacheable(results[i].status == KV_CATALOG_STATUS_STORED ||
                         results[i].status == KV_CATALOG_STATUS_EVICTED);
        r->set_evicted_catalog_hit(results[i].status == KV_CATALOG_STATUS_EVICTED);
        r->set_allocated_pending_store(results[i].status == KV_CATALOG_STATUS_ALLOCATED);
    }
    return rsp.SerializeAsString();
}

std::string CatalogInsertAllocated(PGconn *conn, const std::string &sub_payload)
{
    using namespace ::falconfs::kv;
    BatchAllocateRequest req;
    if (!req.ParseFromString(sub_payload)) return {};
    const uint32_t count = static_cast<uint32_t>(req.items_size());
    const bool promote_mode = (count > 0 &&
        req.items(0).allocate_hint() == AllocateHint::ALLOCATE_HINT_PROMOTE_FROM_EVICTED);
    int32_t method = ::KV_CATALOG_METHOD_INSERT_ALLOCATED;
    std::string buf;
    if (promote_mode) {
        method = ::KV_CATALOG_METHOD_PROMOTE_FROM_EVICTED;
        buf.resize(sizeof(uint32_t) + count * sizeof(KVCatalogPromoteItem));
        std::memcpy(buf.data(), &count, sizeof(uint32_t));
        KVCatalogPromoteItem *items =
            reinterpret_cast<KVCatalogPromoteItem *>(buf.data() + sizeof(uint32_t));
        for (uint32_t i = 0; i < count; ++i) {
            std::memset(&items[i], 0, sizeof(KVCatalogPromoteItem));
            CopyBlockHashIn(req.items(i).block_hash(), items[i].block_hash, &items[i].block_hash_len);
            items[i].kv_group_idx = req.items(i).kv_group_idx();
            items[i].layer_mask = req.items(i).layer_mask();
            items[i].store_node_id = req.items(i).worker_reserved_store_node_id();
            items[i].pool_offset = req.items(i).worker_reserved_pool_offset();
            items[i].expected_version = 0;
            items[i].now_ms = 0;
        }
    } else {
        buf.resize(sizeof(uint32_t) + count * sizeof(KVCatalogInsertItem));
        std::memcpy(buf.data(), &count, sizeof(uint32_t));
        KVCatalogInsertItem *items =
            reinterpret_cast<KVCatalogInsertItem *>(buf.data() + sizeof(uint32_t));
        int64_t now_ms = 0; /* now_ms is a server-side timestamp; falcon.so writes one. */
        for (uint32_t i = 0; i < count; ++i) {
            std::memset(&items[i], 0, sizeof(KVCatalogInsertItem));
            CopyBlockHashIn(req.items(i).block_hash(), items[i].block_hash, &items[i].block_hash_len);
            items[i].kv_group_idx = req.items(i).kv_group_idx();
            items[i].layer_mask = req.items(i).layer_mask();
            items[i].store_node_id = req.items(i).worker_reserved_store_node_id();
            items[i].pool_offset = req.items(i).worker_reserved_pool_offset();
            items[i].now_ms = now_ms;
        }
    }
    std::string resp_bytes = CallCatalog(conn, method, buf);

    BatchAllocateResponse rsp;
    if (resp_bytes.size() < sizeof(uint32_t)) return {};
    uint32_t resp_count = 0;
    std::memcpy(&resp_count, resp_bytes.data(), sizeof(uint32_t));
    if (resp_count != count) {
        return {};
    }
    const KVCatalogInsertResult *results = nullptr;
    const KVCatalogPromoteResult *promote_results = nullptr;
    if (promote_mode) {
        if (resp_bytes.size() < sizeof(uint32_t) + count * sizeof(KVCatalogPromoteResult)) {
            return {};
        }
        promote_results = reinterpret_cast<const KVCatalogPromoteResult *>(
            resp_bytes.data() + sizeof(uint32_t));
    } else {
        if (resp_bytes.size() < sizeof(uint32_t) + count * sizeof(KVCatalogInsertResult)) {
            return {};
        }
        results = reinterpret_cast<const KVCatalogInsertResult *>(
            resp_bytes.data() + sizeof(uint32_t));
    }
    for (uint32_t i = 0; i < count; ++i) {
        auto *r = rsp.add_results();
        r->set_block_hash(req.items(i).block_hash());
        const bool ok = promote_mode ? (promote_results[i].promoted != 0) : (results[i].inserted != 0);
        if (!ok) {
            FillErr(r->mutable_result(), ErrorCode::CAS_CONFLICT, true,
                    "catalog insert lost (duplicate or conflict)");
            r->set_reused_existing_allocation(false);
            continue;
        }
        FillOk(r->mutable_result());
        auto *loc = r->mutable_location();
        loc->set_store_node_id(req.items(i).worker_reserved_store_node_id());
        loc->set_pool_offset(req.items(i).worker_reserved_pool_offset());
        loc->set_evicted_path("");
        loc->set_store_epoch(0);
        r->set_version(promote_mode ? promote_results[i].new_version : 1);
        r->set_reused_existing_allocation(false);
    }
    return rsp.SerializeAsString();
}

std::string CatalogCASStatusUpdate(PGconn *conn, const std::string &sub_payload)
{
    using namespace ::falconfs::kv;
    BatchUpdateStatusRequest req;
    if (!req.ParseFromString(sub_payload)) return {};
    const uint32_t count = static_cast<uint32_t>(req.items_size());
    std::string buf;
    buf.resize(sizeof(uint32_t) + count * sizeof(KVCatalogCASItem));
    std::memcpy(buf.data(), &count, sizeof(uint32_t));
    KVCatalogCASItem *items =
        reinterpret_cast<KVCatalogCASItem *>(buf.data() + sizeof(uint32_t));
    for (uint32_t i = 0; i < count; ++i) {
        std::memset(&items[i], 0, sizeof(KVCatalogCASItem));
        CopyBlockHashIn(req.items(i).block_hash(), items[i].block_hash, &items[i].block_hash_len);
        items[i].expected_from_status = static_cast<int32_t>(req.items(i).expected_from_status());
        items[i].to_status = static_cast<int32_t>(req.items(i).to_status());
        items[i].expected_version = req.items(i).expected_version();
        items[i].now_ms = 0;
        const std::string &ep = req.items(i).evicted_path();
        if (!ep.empty()) {
            items[i].has_evicted_path = 1;
            CopyEvictedPathIn(ep, items[i].evicted_path, &items[i].evicted_path_len);
        }
    }
    std::string resp_bytes = CallCatalog(conn, ::KV_CATALOG_METHOD_CAS_STATUS_UPDATE, buf);

    BatchUpdateStatusResponse rsp;
    if (resp_bytes.size() < sizeof(uint32_t)) return {};
    uint32_t resp_count = 0;
    std::memcpy(&resp_count, resp_bytes.data(), sizeof(uint32_t));
    if (resp_count != count ||
        resp_bytes.size() < sizeof(uint32_t) + count * sizeof(KVCatalogCASResult)) {
        return {};
    }
    const KVCatalogCASResult *results = reinterpret_cast<const KVCatalogCASResult *>(
        resp_bytes.data() + sizeof(uint32_t));
    for (uint32_t i = 0; i < count; ++i) {
        auto *r = rsp.add_results();
        r->set_block_hash(req.items(i).block_hash());
        r->set_new_version(results[i].current_version);
        r->set_current_status(static_cast<BlockStatus>(results[i].current_status));
        if (results[i].success) {
            FillOk(r->mutable_result());
            continue;
        }
        if (results[i].not_found) {
            FillErr(r->mutable_result(), ErrorCode::NOT_FOUND, false, "row missing");
            continue;
        }
        if (results[i].conflict && req.items(i).allow_noop_if_already_target() &&
            results[i].current_status == static_cast<int32_t>(req.items(i).to_status())) {
            FillOk(r->mutable_result());
            continue;
        }
        FillErr(r->mutable_result(), ErrorCode::CAS_CONFLICT, true, "CAS conflict");
    }
    return rsp.SerializeAsString();
}

std::string CatalogDelete(PGconn *conn, const std::string &sub_payload)
{
    using namespace ::falconfs::kv;
    BatchFreeAllocatedRequest req;
    if (!req.ParseFromString(sub_payload)) return {};
    const uint32_t count = static_cast<uint32_t>(req.items_size());
    std::string buf;
    buf.resize(sizeof(uint32_t) + count * sizeof(KVCatalogDeleteItem));
    std::memcpy(buf.data(), &count, sizeof(uint32_t));
    KVCatalogDeleteItem *items =
        reinterpret_cast<KVCatalogDeleteItem *>(buf.data() + sizeof(uint32_t));
    for (uint32_t i = 0; i < count; ++i) {
        std::memset(&items[i], 0, sizeof(KVCatalogDeleteItem));
        CopyBlockHashIn(req.items(i).block_hash(), items[i].block_hash, &items[i].block_hash_len);
        items[i].expected_version = req.items(i).expected_version();
    }
    std::string resp_bytes = CallCatalog(conn, ::KV_CATALOG_METHOD_DELETE, buf);

    BatchFreeAllocatedResponse rsp;
    if (resp_bytes.size() < sizeof(uint32_t)) return {};
    uint32_t resp_count = 0;
    std::memcpy(&resp_count, resp_bytes.data(), sizeof(uint32_t));
    if (resp_count != count ||
        resp_bytes.size() < sizeof(uint32_t) + count * sizeof(KVCatalogDeleteResult)) {
        return {};
    }
    const KVCatalogDeleteResult *results = reinterpret_cast<const KVCatalogDeleteResult *>(
        resp_bytes.data() + sizeof(uint32_t));
    for (uint32_t i = 0; i < count; ++i) {
        auto *r = rsp.add_results();
        r->set_block_hash(req.items(i).block_hash());
        r->set_new_version(req.items(i).expected_version());
        if (results[i].success) {
            FillOk(r->mutable_result());
            continue;
        }
        if (results[i].conflict) {
            FillErr(r->mutable_result(), ErrorCode::CAS_CONFLICT, true, "delete CAS conflict");
            r->set_new_version(0);
            continue;
        }
        FillErr(r->mutable_result(), ErrorCode::NOT_FOUND, false, "row missing");
        r->set_new_version(0);
    }
    return rsp.SerializeAsString();
}

/* Single C entry point registered with falcon.so. Runs on a pool worker thread.
 * `pg_conn_opaque` is the worker's libpq connection. */
extern "C" int FalconKVProcessJobImpl(int method, const char *req_buf, int req_size,
                                      char **out_resp, int *out_resp_size, void *pg_conn_opaque)
{
    if (out_resp == nullptr || out_resp_size == nullptr) return 1;
    *out_resp = nullptr;
    *out_resp_size = 0;

    auto impl = AcquireImpl();
    if (impl == nullptr) {
        return 1;
    }
    PGconn *conn = static_cast<PGconn *>(pg_conn_opaque);

    using namespace ::falconfs::kv;
    std::string req_str(req_buf, static_cast<size_t>(req_size));
    std::string resp_str;

    switch (static_cast<FalconKVServiceMethod>(method)) {
    case FalconKVServiceMethod::BATCH_LOOKUP_WITH_LEASE: {
        BatchLookupRequest req;
        if (!req.ParseFromString(req_str)) return 1;
        BatchLookupResponse resp;
        impl->BatchLookupWithLeaseSplitForPoolWorker(
            req, &resp,
            [conn](const std::string &payload) { return CatalogLookup(conn, payload); });
        resp_str = resp.SerializeAsString();
        break;
    }
    case FalconKVServiceMethod::BATCH_ALLOCATE_WITH_LEASE: {
        BatchAllocateRequest req;
        if (!req.ParseFromString(req_str)) return 1;
        BatchAllocateResponse resp;
        impl->BatchAllocateWithLeaseSplitForPoolWorker(
            req, &resp,
            [conn](const std::string &payload) { return CatalogInsertAllocated(conn, payload); });
        resp_str = resp.SerializeAsString();
        break;
    }
    case FalconKVServiceMethod::BATCH_RENEW_LEASE: {
        BatchRenewLeaseRequest req;
        if (!req.ParseFromString(req_str)) return 1;
        BatchRenewLeaseResponse resp;
        impl->BatchRenewLeaseSplitForPoolWorker(req, &resp,
                                                [](const std::string &) { return std::string{}; });
        resp_str = resp.SerializeAsString();
        break;
    }
    case FalconKVServiceMethod::BATCH_UPDATE_BLOCK_STATUS: {
        BatchUpdateStatusRequest req;
        if (!req.ParseFromString(req_str)) return 1;
        BatchUpdateStatusResponse resp;
        impl->BatchUpdateBlockStatusSplitForPoolWorker(
            req, &resp,
            [conn](const std::string &payload) { return CatalogCASStatusUpdate(conn, payload); });
        resp_str = resp.SerializeAsString();
        break;
    }
    case FalconKVServiceMethod::BATCH_FREE_ALLOCATED: {
        BatchFreeAllocatedRequest req;
        if (!req.ParseFromString(req_str)) return 1;
        BatchFreeAllocatedResponse resp;
        impl->BatchFreeAllocatedSplitForPoolWorker(
            req, &resp,
            [conn](const std::string &payload) { return CatalogDelete(conn, payload); });
        resp_str = resp.SerializeAsString();
        break;
    }
    default:
        return 1;
    }

    if (resp_str.empty()) {
        return 0; /* empty response \u2014 worker turns this into per-item INTERNAL_ERROR */
    }
    *out_resp = static_cast<char *>(std::malloc(resp_str.size()));
    if (*out_resp == nullptr) return 1;
    std::memcpy(*out_resp, resp_str.data(), resp_str.size());
    *out_resp_size = static_cast<int>(resp_str.size());
    return 0;
}

}  // namespace

void KVRuntimeRegister::Install(std::shared_ptr<::falconfs::kv::KVMetadataServiceImpl> impl)
{
    {
        std::lock_guard<std::mutex> lk(g_kv_impl_mu);
        g_kv_impl = std::move(impl);
    }
    FalconKVSetProcessJob(&FalconKVProcessJobImpl);
}

void KVRuntimeRegister::Uninstall()
{
    FalconKVSetProcessJob(nullptr);
    std::lock_guard<std::mutex> lk(g_kv_impl_mu);
    g_kv_impl.reset();
}

std::shared_ptr<::falconfs::kv::KVMetadataServiceImpl> KVRuntimeRegister::GetImpl()
{
    return AcquireImpl();
}

bool KVRuntimeRegister::EnsureKvblockTableOnConn(void *pg_conn_opaque)
{
    PGconn *conn = static_cast<PGconn *>(pg_conn_opaque);
    if (conn == nullptr) return false;
    PGresult *res = PQexec(conn, "SELECT pg_catalog.falcon_create_kvblock_table()");
    bool ok = (PQresultStatus(res) == PGRES_TUPLES_OK);
    PQclear(res);
    return ok;
}

std::string KVRuntimeRegister::CatalogCASStatusUpdateOnConn(void *pg_conn_opaque,
                                                            const std::string &serialized_sub_payload)
{
    PGconn *conn = static_cast<PGconn *>(pg_conn_opaque);
    return CatalogCASStatusUpdate(conn, serialized_sub_payload);
}

}  // namespace falcon::kv_proto
