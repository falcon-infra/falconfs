/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * v6 §15.1 / §15.4 catalog SQL entry point for DN-restart recovery.
 *
 *   SELECT falcon_kv_metadata_recovery_call(method int, payload bytea) -> bytea
 *
 * Methods (`KVCatalogMethod`):
 *   - SCAN_FOR_RECOVERY: returns every row in `pg_catalog.falcon_kvblock_table`
 *     packed as `[uint32_t row_count][KVCatalogRecoveryRow * row_count]`. The
 *     plugin's libpq recovery accessor calls this once at DN startup before
 *     the BRPC server begins serving KV traffic so the in-process DRAM cache
 *     is fully populated.
 *   - LOAD_DN_EPOCH / BUMP_DN_EPOCH: read or atomically increment the
 *     persisted dn_epoch row in `pg_catalog.falcon_kvblock_dn_epoch`. Old
 *     lease tokens carrying a stale dn_epoch are rejected by the engine on
 *     renew (v6 §11.3).
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

PG_FUNCTION_INFO_V1(falcon_kv_metadata_recovery_call);

/* The recovery scan response is variable-size. Choose a max capacity that
 * comfortably covers single-shard test deployments; production sizing will
 * page through this once the cluster grows beyond 64K rows per shard. */
#define KV_RECOVERY_MAX_ROWS 65536

Datum falcon_kv_metadata_recovery_call(PG_FUNCTION_ARGS)
{
    int32_t method = PG_GETARG_INT32(0);
    bytea  *payload = PG_GETARG_BYTEA_P(1);
    const char *req_buf = (const char *) VARDATA_ANY(payload);
    uint64_t req_size = (uint64_t) VARSIZE_ANY_EXHDR(payload);

    /* Per-method response shape. */
    bytea *reply = NULL;

    BeginInternalSubTransaction(NULL);
    MemoryContext oldcontext = CurrentMemoryContext;
    PG_TRY();
    {
        switch (method) {
        case KV_CATALOG_METHOD_SCAN_FOR_RECOVERY: {
            /* Allocate worst-case response buffer; trim to actual count. */
            uint64_t max_size = sizeof(uint32_t) +
                                (uint64_t) KV_RECOVERY_MAX_ROWS * sizeof(KVCatalogRecoveryRow);
            reply = (bytea *) palloc0(VARHDRSZ + max_size);
            char *resp_buf = (char *) VARDATA(reply);
            KVCatalogRecoveryRow *rows =
                (KVCatalogRecoveryRow *)(resp_buf + sizeof(uint32_t));
            uint32_t produced = FalconKVBlockScanForRecovery(rows, KV_RECOVERY_MAX_ROWS);
            memcpy(resp_buf, &produced, sizeof(uint32_t));
            uint64_t actual_size = sizeof(uint32_t) +
                                   (uint64_t) produced * sizeof(KVCatalogRecoveryRow);
            SET_VARSIZE(reply, VARHDRSZ + actual_size);
            break;
        }
        case KV_CATALOG_METHOD_RECONCILE_STORE_RESTART: {
            if (req_size < sizeof(KVCatalogStoreRestartRequest))
                FALCON_ELOG_ERROR(ARGUMENT_ERROR,
                                  "kv recovery store restart: payload too small");
            KVCatalogStoreRestartRequest req;
            memcpy(&req, req_buf, sizeof(req));
            uint64_t resp_size = sizeof(KVCatalogStoreRestartResponse);
            reply = (bytea *) palloc0(VARHDRSZ + resp_size);
            SET_VARSIZE(reply, VARHDRSZ + resp_size);
            KVCatalogStoreRestartResponse resp;
            FalconKVBlockReconcileStoreRestart(req.store_node_id, req.now_ms, &resp);
            memcpy(VARDATA(reply), &resp, sizeof(resp));
            break;
        }
        case KV_CATALOG_METHOD_SCAN_STORE_EVICTED: {
            if (req_size < sizeof(KVCatalogStoreEvictedScanRequest))
                FALCON_ELOG_ERROR(ARGUMENT_ERROR,
                                  "kv recovery scan store evicted: payload too small");
            KVCatalogStoreEvictedScanRequest req;
            memcpy(&req, req_buf, sizeof(req));
            uint32_t max_rows = req.max_rows == 0 ? 1024 : req.max_rows;
            if (max_rows > KV_RECOVERY_MAX_ROWS) max_rows = KV_RECOVERY_MAX_ROWS;
            uint64_t max_size = sizeof(uint32_t) +
                                (uint64_t) max_rows * sizeof(KVCatalogStoreEvictedRow);
            reply = (bytea *) palloc0(VARHDRSZ + max_size);
            char *resp_buf = (char *) VARDATA(reply);
            KVCatalogStoreEvictedRow *rows =
                (KVCatalogStoreEvictedRow *)(resp_buf + sizeof(uint32_t));
            uint32_t produced = FalconKVBlockScanStoreEvicted(req.store_node_id, rows, max_rows);
            memcpy(resp_buf, &produced, sizeof(uint32_t));
            uint64_t actual_size = sizeof(uint32_t) +
                                   (uint64_t) produced * sizeof(KVCatalogStoreEvictedRow);
            SET_VARSIZE(reply, VARHDRSZ + actual_size);
            break;
        }
        case KV_CATALOG_METHOD_DELETE_STORE_EVICTED_INVALID: {
            if (req_size < sizeof(uint32_t))
                FALCON_ELOG_ERROR(ARGUMENT_ERROR,
                                  "kv recovery delete invalid evicted: payload too small");
            uint32_t count = 0;
            memcpy(&count, req_buf, sizeof(uint32_t));
            uint64_t resp_size = KVCatalogResponseSize(method, count);
            reply = (bytea *) palloc0(VARHDRSZ + resp_size);
            SET_VARSIZE(reply, VARHDRSZ + resp_size);
            FalconKVBlockDeleteInvalidEvicted(req_buf, req_size, VARDATA(reply), resp_size);
            break;
        }
        case KV_CATALOG_METHOD_LOAD_DN_EPOCH:
        case KV_CATALOG_METHOD_BUMP_DN_EPOCH: {
            if (req_size < sizeof(KVCatalogDnEpochRequest))
                FALCON_ELOG_ERROR(ARGUMENT_ERROR,
                                  "kv recovery dn_epoch: payload too small");
            KVCatalogDnEpochRequest req;
            memcpy(&req, req_buf, sizeof(req));
            int64_t epoch = (method == KV_CATALOG_METHOD_BUMP_DN_EPOCH)
                                ? FalconKVBlockBumpDnEpoch(req.shard_id)
                                : FalconKVBlockLoadDnEpoch(req.shard_id);
            uint64_t resp_size = sizeof(KVCatalogDnEpochResponse);
            reply = (bytea *) palloc0(VARHDRSZ + resp_size);
            SET_VARSIZE(reply, VARHDRSZ + resp_size);
            KVCatalogDnEpochResponse resp;
            resp.dn_epoch = epoch;
            memcpy(VARDATA(reply), &resp, sizeof(resp));
            break;
        }
        default:
            FALCON_ELOG_ERROR(ARGUMENT_ERROR,
                              "kv recovery call: unsupported method");
        }
        ReleaseCurrentSubTransaction();
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(oldcontext);
        FlushErrorState();
        RollbackAndReleaseCurrentSubTransaction();
        /* On total failure: empty bytea so the plugin can detect "no rows". */
        if (reply == NULL) {
            reply = (bytea *) palloc0(VARHDRSZ + sizeof(uint32_t));
            SET_VARSIZE(reply, VARHDRSZ + sizeof(uint32_t));
        } else {
            uint32_t zero = 0;
            memcpy(VARDATA(reply), &zero, sizeof(zero));
            SET_VARSIZE(reply, VARHDRSZ + sizeof(uint32_t));
        }
    }
    PG_END_TRY();

    PG_RETURN_BYTEA_P(reply);
}
