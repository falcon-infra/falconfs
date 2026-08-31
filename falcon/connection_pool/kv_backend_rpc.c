/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * v6.4 \u00a74.1.1 catalog SQL entry point. The BRPC plugin in libbrpcplugin.so
 * sends one libpq query per sub-batch:
 *
 *   SELECT falcon_kv_metadata_catalog_call(method int, payload bytea) -> bytea
 *
 * `payload` is the v6.4 wire-format request defined in
 * `falcon/include/connection_pool/kv_catalog_wire.h`. We dispatch by `method`
 * to the matching `FalconKVBlock*` C accessor (which uses PG internal APIs
 * only \u2014 v6.4 \u00a74.1, no raw SQL), wrap the call in a sub-transaction so a
 * per-item PostgreSQL error converts into the per-item `ItemResultMeta`
 * fields described in the response struct, and return the response buffer as
 * a `bytea` value.
 */

#include "postgres.h"

#include "access/xact.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "varatt.h"

#include "connection_pool/kv_catalog_wire.h"
#include "metadb/kvblock_table.h"
#include "utils/error_log.h"

PG_FUNCTION_INFO_V1(falcon_kv_metadata_catalog_call);

Datum falcon_kv_metadata_catalog_call(PG_FUNCTION_ARGS)
{
    int32_t method = PG_GETARG_INT32(0);
    bytea  *payload = PG_GETARG_BYTEA_P(1);

    const char *req_buf = (const char *) VARDATA_ANY(payload);
    uint64_t req_size = (uint64_t) VARSIZE_ANY_EXHDR(payload);
    if (req_size < sizeof(uint32_t))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "kv catalog call: payload too small");

    uint32_t count = 0;
    memcpy(&count, req_buf, sizeof(uint32_t));
    uint64_t resp_size = KVCatalogResponseSize(method, count);
    if (resp_size == 0)
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "kv catalog call: unknown method");

    bytea *reply = (bytea *) palloc0(VARHDRSZ + resp_size);
    SET_VARSIZE(reply, VARHDRSZ + resp_size);
    char *resp_buf = (char *) VARDATA(reply);

    /* v6.4 \u00a74.1.2: wrap the entire sub-batch in one sub-transaction so a
     * PostgreSQL ERROR cannot tear out of the C handler. Per-item errors are
     * surfaced via the per-result struct, never as a SQL error. */
    BeginInternalSubTransaction(NULL);
    MemoryContext oldcontext = CurrentMemoryContext;
    PG_TRY();
    {
        switch (method) {
        case KV_CATALOG_METHOD_LOOKUP:
            FalconKVBlockBatchLookup(req_buf, req_size, resp_buf, resp_size);
            break;
        case KV_CATALOG_METHOD_INSERT_ALLOCATED:
            FalconKVBlockBatchInsertAllocated(req_buf, req_size, resp_buf, resp_size);
            break;
        case KV_CATALOG_METHOD_CAS_STATUS_UPDATE:
            FalconKVBlockBatchCASStatusUpdate(req_buf, req_size, resp_buf, resp_size);
            break;
        case KV_CATALOG_METHOD_DELETE:
            FalconKVBlockBatchDelete(req_buf, req_size, resp_buf, resp_size);
            break;
        case KV_CATALOG_METHOD_PROMOTE_FROM_EVICTED:
            FalconKVBlockBatchPromoteFromEvicted(req_buf, req_size, resp_buf, resp_size);
            break;
        default:
            FALCON_ELOG_ERROR(ARGUMENT_ERROR, "kv catalog call: unsupported method");
        }
        ReleaseCurrentSubTransaction();
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcontext);
        FlushErrorState();
        RollbackAndReleaseCurrentSubTransaction();
        /* On total batch failure: zero out the response so per-item fields
         * remain at their default (success=0/found=0/...) values; the BRPC
         * plugin treats this as a retryable INTERNAL_ERROR per item. */
        memset(resp_buf, 0, resp_size);
        memcpy(resp_buf, &count, sizeof(uint32_t));
    }
    PG_END_TRY();

    PG_RETURN_BYTEA_P(reply);
}
