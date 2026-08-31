/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * v6.4 \u00a73.1 / \u00a74.2 catalog accessor for `falcon_kvblock_table`. Implementation
 * uses PG internal APIs only (no raw SQL on the hot path).
 */

#include "metadb/kvblock_table.h"

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "executor/spi.h"
#include "utils/lsyscache.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "storage/lockdefs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "varatt.h"

#include "utils/error_log.h"
#include "utils/utils.h"

#include <string.h>

const char *KvblockTableName      = "falcon_kvblock_table";
const char *KvblockTableIndexName = "falcon_kvblock_table_pkey";
static const char *KvblockTableStatusIndexName = "falcon_kvblock_status_time_idx";

void ConstructCreateKvblockTableCommand(StringInfo command, const char *name)
{
    appendStringInfo(command,
                     "CREATE TABLE falcon.%s("
                     "block_hash    BYTEA  PRIMARY KEY,"
                     "kv_group_idx  INT    NOT NULL DEFAULT 0,"
                     "layer_mask    INT    NOT NULL DEFAULT 0,"
                     "status        SMALLINT NOT NULL,"
                     "store_node_id INT    NOT NULL,"
                     "pool_offset   BIGINT NOT NULL,"
                     "evicted_path  TEXT,"
                     "version       BIGINT NOT NULL,"
                     "updated_at_ms BIGINT NOT NULL);"
                     "CREATE INDEX %s ON falcon.%s USING btree(status, updated_at_ms);"
                     "ALTER TABLE falcon.%s SET SCHEMA pg_catalog;"
                     "GRANT SELECT ON pg_catalog.%s TO public;"
                     "ALTER EXTENSION falcon ADD TABLE %s;"
                     /* v6 \u00a715.1 persisted dn_epoch row used for restart fencing. */
                     "CREATE TABLE falcon.falcon_kvblock_dn_epoch("
                     "shard_id INT PRIMARY KEY,"
                     "dn_epoch BIGINT NOT NULL);"
                     "ALTER TABLE falcon.falcon_kvblock_dn_epoch SET SCHEMA pg_catalog;"
                     "GRANT SELECT ON pg_catalog.falcon_kvblock_dn_epoch TO public;"
                     "ALTER EXTENSION falcon ADD TABLE falcon_kvblock_dn_epoch;",
                     name,
                     KvblockTableStatusIndexName, name,
                     name,
                     name,
                     name);
}

static Oid g_kvblockRelationId      = InvalidOid;
static Oid g_kvblockRelationIndexId = InvalidOid;

Oid KvblockRelationId(void)
{
    if (g_kvblockRelationId != InvalidOid)
        return g_kvblockRelationId;
    Oid oid = RelnameGetRelid(KvblockTableName);
    if (oid == InvalidOid)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_kvblock_table is not created");
    g_kvblockRelationId = oid;
    return oid;
}

Oid KvblockRelationIndexId(void)
{
    if (g_kvblockRelationIndexId != InvalidOid)
        return g_kvblockRelationIndexId;
    /* The PK index name is the canonical PostgreSQL one ("<tbl>_pkey"). */
    Oid oid = RelnameGetRelid(KvblockTableIndexName);
    if (oid == InvalidOid)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "falcon_kvblock_table primary key index is missing");
    g_kvblockRelationIndexId = oid;
    return oid;
}

/* ------------------------------------------------------------------------ */
/*  Helpers                                                                 */
/* ------------------------------------------------------------------------ */

static bytea *MakeByteaFromBlockHash(const uint8_t *block_hash, uint16_t len)
{
    bytea *out = (bytea *) palloc(VARHDRSZ + len);
    SET_VARSIZE(out, VARHDRSZ + len);
    memcpy(VARDATA(out), block_hash, len);
    return out;
}

/* Fetch the current row by block_hash. Returns true if found, false otherwise.
 * Caller passes a Datum/null array sized to Natts_falcon_kvblock_table.
 * The caller MUST hold an open transaction or sub-transaction. */
static bool FetchRowByHash(Relation rel, const uint8_t *block_hash, uint16_t len,
                           HeapTuple *out_tuple, Datum *datums, bool *isnulls)
{
    ScanKeyData k[1];
    bytea *key = MakeByteaFromBlockHash(block_hash, len);
    ScanKeyInit(&k[0],
                Anum_falcon_kvblock_table_block_hash,
                BTEqualStrategyNumber,
                F_BYTEAEQ,
                PointerGetDatum(key));
    SysScanDesc scan = systable_beginscan(rel, KvblockRelationIndexId(), true /*indexOK*/,
                                          GetActiveSnapshot(), 1, k);
    HeapTuple t = systable_getnext(scan);
    bool found = HeapTupleIsValid(t);
    if (found) {
        TupleDesc tupdesc = RelationGetDescr(rel);
        heap_deform_tuple(t, tupdesc, datums, isnulls);
        if (out_tuple)
            *out_tuple = heap_copytuple(t);
    }
    systable_endscan(scan);
    pfree(key);
    return found;
}

static void CopyEvictedPathField(const Datum *datums, const bool *isnulls,
                                 char *out, uint16_t *out_len)
{
    *out_len = 0;
    out[0] = '\0';
    if (isnulls[Anum_falcon_kvblock_table_evicted_path - 1])
        return;
    text *t = DatumGetTextP(datums[Anum_falcon_kvblock_table_evicted_path - 1]);
    int sz = VARSIZE_ANY_EXHDR(t);
    if (sz < 0)
        sz = 0;
    if (sz > KV_CATALOG_EVICTED_PATH_MAX_LEN - 1)
        sz = KV_CATALOG_EVICTED_PATH_MAX_LEN - 1;
    memcpy(out, VARDATA_ANY(t), sz);
    out[sz] = '\0';
    *out_len = (uint16_t) sz;
}

/* ------------------------------------------------------------------------ */
/*  Lookup                                                                  */
/* ------------------------------------------------------------------------ */

void FalconKVBlockBatchLookup(const char *req_buf, uint64_t req_size,
                              char *resp_buf, uint64_t resp_size)
{
    if (req_size < sizeof(uint32_t) || resp_size < sizeof(uint32_t))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "kvblock lookup: buffer too small");

    uint32_t count = 0;
    memcpy(&count, req_buf, sizeof(uint32_t));
    const uint64_t expect_req = sizeof(uint32_t) + (uint64_t) count * sizeof(KVCatalogLookupItem);
    const uint64_t expect_resp = KVCatalogResponseSize(KV_CATALOG_METHOD_LOOKUP, count);
    if (req_size < expect_req || resp_size < expect_resp)
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "kvblock lookup: count/size mismatch");
    memcpy(resp_buf, &count, sizeof(uint32_t));

    if (count == 0)
        return;

    const KVCatalogLookupItem *items = (const KVCatalogLookupItem *)(req_buf + sizeof(uint32_t));
    KVCatalogLookupResult *results =
        (KVCatalogLookupResult *)(resp_buf + sizeof(uint32_t));
    memset(results, 0, (size_t) count * sizeof(KVCatalogLookupResult));

    Relation rel = table_open(KvblockRelationId(), AccessShareLock);
    for (uint32_t i = 0; i < count; ++i) {
        Datum datums[Natts_falcon_kvblock_table];
        bool  isnulls[Natts_falcon_kvblock_table];
        memset(isnulls, 0, sizeof(isnulls));
        bool found = FetchRowByHash(rel, items[i].block_hash, items[i].block_hash_len,
                                    NULL, datums, isnulls);
        results[i].found = found ? 1 : 0;
        if (!found)
            continue;
        results[i].status = (uint8_t) DatumGetInt16(datums[Anum_falcon_kvblock_table_status - 1]);
        results[i].store_node_id = DatumGetInt32(datums[Anum_falcon_kvblock_table_store_node_id - 1]);
        results[i].pool_offset = DatumGetInt64(datums[Anum_falcon_kvblock_table_pool_offset - 1]);
        results[i].version = DatumGetInt64(datums[Anum_falcon_kvblock_table_version - 1]);
        results[i].updated_at_ms = DatumGetInt64(datums[Anum_falcon_kvblock_table_updated_at_ms - 1]);
        CopyEvictedPathField(datums, isnulls, results[i].evicted_path,
                             &results[i].evicted_path_len);
    }
    table_close(rel, AccessShareLock);
}

/* ------------------------------------------------------------------------ */
/*  Insert allocated                                                        */
/* ------------------------------------------------------------------------ */

void FalconKVBlockBatchInsertAllocated(const char *req_buf, uint64_t req_size,
                                       char *resp_buf, uint64_t resp_size)
{
    if (req_size < sizeof(uint32_t) || resp_size < sizeof(uint32_t))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "kvblock insert: buffer too small");
    uint32_t count = 0;
    memcpy(&count, req_buf, sizeof(uint32_t));
    const uint64_t expect_req = sizeof(uint32_t) + (uint64_t) count * sizeof(KVCatalogInsertItem);
    const uint64_t expect_resp = KVCatalogResponseSize(KV_CATALOG_METHOD_INSERT_ALLOCATED, count);
    if (req_size < expect_req || resp_size < expect_resp)
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "kvblock insert: count/size mismatch");
    memcpy(resp_buf, &count, sizeof(uint32_t));
    if (count == 0)
        return;

    const KVCatalogInsertItem *items = (const KVCatalogInsertItem *)(req_buf + sizeof(uint32_t));
    KVCatalogInsertResult *results =
        (KVCatalogInsertResult *)(resp_buf + sizeof(uint32_t));
    memset(results, 0, (size_t) count * sizeof(KVCatalogInsertResult));

    Relation rel = table_open(KvblockRelationId(), RowExclusiveLock);
    CatalogIndexState istate = CatalogOpenIndexes(rel);
    TupleDesc tupdesc = RelationGetDescr(rel);

    for (uint32_t i = 0; i < count; ++i) {
        /* Pre-check: if the row already exists, return inserted=0 (CAS_CONFLICT
         * at the engine layer). This avoids relying on a raised duplicate-key
         * error which would tear the whole sub-transaction. */
        Datum dummy_d[Natts_falcon_kvblock_table];
        bool  dummy_n[Natts_falcon_kvblock_table];
        if (FetchRowByHash(rel, items[i].block_hash, items[i].block_hash_len,
                           NULL, dummy_d, dummy_n)) {
            results[i].inserted = 0;
            continue;
        }
        bytea *key = MakeByteaFromBlockHash(items[i].block_hash, items[i].block_hash_len);
        Datum datums[Natts_falcon_kvblock_table];
        bool  isnulls[Natts_falcon_kvblock_table];
        memset(isnulls, 0, sizeof(isnulls));
        datums[Anum_falcon_kvblock_table_block_hash - 1]    = PointerGetDatum(key);
        datums[Anum_falcon_kvblock_table_kv_group_idx - 1]  = Int32GetDatum(items[i].kv_group_idx);
        datums[Anum_falcon_kvblock_table_layer_mask - 1]    = Int32GetDatum(items[i].layer_mask);
        datums[Anum_falcon_kvblock_table_status - 1]        = Int16GetDatum((int16) KV_CATALOG_STATUS_ALLOCATED);
        datums[Anum_falcon_kvblock_table_store_node_id - 1] = Int32GetDatum(items[i].store_node_id);
        datums[Anum_falcon_kvblock_table_pool_offset - 1]   = Int64GetDatum(items[i].pool_offset);
        isnulls[Anum_falcon_kvblock_table_evicted_path - 1] = true;
        datums[Anum_falcon_kvblock_table_version - 1]       = Int64GetDatum(1);
        datums[Anum_falcon_kvblock_table_updated_at_ms - 1] = Int64GetDatum(items[i].now_ms);
        HeapTuple t = heap_form_tuple(tupdesc, datums, isnulls);
        CatalogTupleInsertWithInfo(rel, t, istate);
        heap_freetuple(t);
        pfree(key);
        results[i].inserted = 1;
    }
    CommandCounterIncrement();
    CatalogCloseIndexes(istate);
    table_close(rel, RowExclusiveLock);
}

/* ------------------------------------------------------------------------ */
/*  CAS Status Update                                                       */
/* ------------------------------------------------------------------------ */

void FalconKVBlockBatchCASStatusUpdate(const char *req_buf, uint64_t req_size,
                                       char *resp_buf, uint64_t resp_size)
{
    if (req_size < sizeof(uint32_t) || resp_size < sizeof(uint32_t))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "kvblock cas: buffer too small");
    uint32_t count = 0;
    memcpy(&count, req_buf, sizeof(uint32_t));
    const uint64_t expect_req = sizeof(uint32_t) + (uint64_t) count * sizeof(KVCatalogCASItem);
    const uint64_t expect_resp = KVCatalogResponseSize(KV_CATALOG_METHOD_CAS_STATUS_UPDATE, count);
    if (req_size < expect_req || resp_size < expect_resp)
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "kvblock cas: count/size mismatch");
    memcpy(resp_buf, &count, sizeof(uint32_t));
    if (count == 0)
        return;

    const KVCatalogCASItem *items = (const KVCatalogCASItem *)(req_buf + sizeof(uint32_t));
    KVCatalogCASResult *results =
        (KVCatalogCASResult *)(resp_buf + sizeof(uint32_t));
    memset(results, 0, (size_t) count * sizeof(KVCatalogCASResult));

    Relation rel = table_open(KvblockRelationId(), RowExclusiveLock);
    CatalogIndexState istate = CatalogOpenIndexes(rel);
    TupleDesc tupdesc = RelationGetDescr(rel);

    for (uint32_t i = 0; i < count; ++i) {
        Datum datums[Natts_falcon_kvblock_table];
        bool  isnulls[Natts_falcon_kvblock_table];
        memset(isnulls, 0, sizeof(isnulls));
        HeapTuple cur = NULL;
        bool found = FetchRowByHash(rel, items[i].block_hash, items[i].block_hash_len,
                                    &cur, datums, isnulls);
        if (!found) {
            results[i].not_found = 1;
            continue;
        }
        int32 cur_status = (int32) DatumGetInt16(datums[Anum_falcon_kvblock_table_status - 1]);
        int64 cur_version = DatumGetInt64(datums[Anum_falcon_kvblock_table_version - 1]);
        results[i].current_status = cur_status;
        results[i].current_version = cur_version;
        if (cur_status != items[i].expected_from_status ||
            cur_version != items[i].expected_version) {
            results[i].conflict = 1;
            heap_freetuple(cur);
            continue;
        }
        Datum new_d[Natts_falcon_kvblock_table];
        bool  new_n[Natts_falcon_kvblock_table];
        bool  rep[Natts_falcon_kvblock_table];
        memset(rep, 0, sizeof(rep));
        memset(new_n, 0, sizeof(new_n));
        new_d[Anum_falcon_kvblock_table_status - 1]        = Int16GetDatum((int16) items[i].to_status);
        rep[Anum_falcon_kvblock_table_status - 1]          = true;
        new_d[Anum_falcon_kvblock_table_version - 1]       = Int64GetDatum(cur_version + 1);
        rep[Anum_falcon_kvblock_table_version - 1]         = true;
        new_d[Anum_falcon_kvblock_table_updated_at_ms - 1] = Int64GetDatum(items[i].now_ms);
        rep[Anum_falcon_kvblock_table_updated_at_ms - 1]   = true;
        if (items[i].has_evicted_path) {
            uint16_t plen = items[i].evicted_path_len;
            if (plen > KV_CATALOG_EVICTED_PATH_MAX_LEN - 1)
                plen = KV_CATALOG_EVICTED_PATH_MAX_LEN - 1;
            new_d[Anum_falcon_kvblock_table_evicted_path - 1] =
                CStringGetTextDatum(items[i].evicted_path);
            rep[Anum_falcon_kvblock_table_evicted_path - 1] = true;
            (void) plen;
        }
        HeapTuple updated = heap_modify_tuple(cur, tupdesc, new_d, new_n, rep);
        CatalogTupleUpdateWithInfo(rel, &cur->t_self, updated, istate);
        heap_freetuple(updated);
        heap_freetuple(cur);
        results[i].success = 1;
        results[i].current_status = items[i].to_status;
        results[i].current_version = cur_version + 1;
    }
    CommandCounterIncrement();
    CatalogCloseIndexes(istate);
    table_close(rel, RowExclusiveLock);
}

/* ------------------------------------------------------------------------ */
/*  Delete (with optional CAS on version)                                   */
/* ------------------------------------------------------------------------ */

void FalconKVBlockBatchDelete(const char *req_buf, uint64_t req_size,
                              char *resp_buf, uint64_t resp_size)
{
    if (req_size < sizeof(uint32_t) || resp_size < sizeof(uint32_t))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "kvblock delete: buffer too small");
    uint32_t count = 0;
    memcpy(&count, req_buf, sizeof(uint32_t));
    const uint64_t expect_req = sizeof(uint32_t) + (uint64_t) count * sizeof(KVCatalogDeleteItem);
    const uint64_t expect_resp = KVCatalogResponseSize(KV_CATALOG_METHOD_DELETE, count);
    if (req_size < expect_req || resp_size < expect_resp)
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "kvblock delete: count/size mismatch");
    memcpy(resp_buf, &count, sizeof(uint32_t));
    if (count == 0)
        return;

    const KVCatalogDeleteItem *items = (const KVCatalogDeleteItem *)(req_buf + sizeof(uint32_t));
    KVCatalogDeleteResult *results =
        (KVCatalogDeleteResult *)(resp_buf + sizeof(uint32_t));
    memset(results, 0, (size_t) count * sizeof(KVCatalogDeleteResult));

    Relation rel = table_open(KvblockRelationId(), RowExclusiveLock);

    for (uint32_t i = 0; i < count; ++i) {
        Datum datums[Natts_falcon_kvblock_table];
        bool  isnulls[Natts_falcon_kvblock_table];
        memset(isnulls, 0, sizeof(isnulls));
        HeapTuple cur = NULL;
        bool found = FetchRowByHash(rel, items[i].block_hash, items[i].block_hash_len,
                                    &cur, datums, isnulls);
        if (!found) {
            /* Treat absent row as success for free idempotency unless CAS asked. */
            if (items[i].expected_version > 0) {
                results[i].conflict = 1;
            } else {
                results[i].success = 1;
            }
            continue;
        }
        int64 cur_version = DatumGetInt64(datums[Anum_falcon_kvblock_table_version - 1]);
        if (items[i].expected_version > 0 && cur_version != items[i].expected_version) {
            results[i].conflict = 1;
            heap_freetuple(cur);
            continue;
        }
        simple_heap_delete(rel, &cur->t_self);
        heap_freetuple(cur);
        results[i].success = 1;
    }
    CommandCounterIncrement();
    table_close(rel, RowExclusiveLock);
}

/* ------------------------------------------------------------------------ */
/*  Recovery scan + dn_epoch (v6 \u00a715.1, \u00a715.4)                                */
/* ------------------------------------------------------------------------ */

void FalconKVBlockBatchPromoteFromEvicted(const char *req_buf, uint64_t req_size,
                                          char *resp_buf, uint64_t resp_size)
{
    if (req_size < sizeof(uint32_t) || resp_size < sizeof(uint32_t))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "kvblock promote: buffer too small");
    uint32_t count = 0;
    memcpy(&count, req_buf, sizeof(uint32_t));
    const uint64_t expect_req = sizeof(uint32_t) + (uint64_t) count * sizeof(KVCatalogPromoteItem);
    const uint64_t expect_resp = KVCatalogResponseSize(KV_CATALOG_METHOD_PROMOTE_FROM_EVICTED, count);
    if (req_size < expect_req || resp_size < expect_resp)
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "kvblock promote: count/size mismatch");
    memcpy(resp_buf, &count, sizeof(uint32_t));
    if (count == 0)
        return;

    const KVCatalogPromoteItem *items =
        (const KVCatalogPromoteItem *)(req_buf + sizeof(uint32_t));
    KVCatalogPromoteResult *results =
        (KVCatalogPromoteResult *)(resp_buf + sizeof(uint32_t));
    memset(results, 0, (size_t) count * sizeof(KVCatalogPromoteResult));

    Relation rel = table_open(KvblockRelationId(), RowExclusiveLock);
    CatalogIndexState istate = CatalogOpenIndexes(rel);
    TupleDesc tupdesc = RelationGetDescr(rel);

    for (uint32_t i = 0; i < count; ++i) {
        Datum datums[Natts_falcon_kvblock_table];
        bool  isnulls[Natts_falcon_kvblock_table];
        memset(isnulls, 0, sizeof(isnulls));
        HeapTuple cur = NULL;
        bool found = FetchRowByHash(rel, items[i].block_hash, items[i].block_hash_len,
                                    &cur, datums, isnulls);
        if (!found) {
            continue;
        }
        int32 cur_status = (int32) DatumGetInt16(datums[Anum_falcon_kvblock_table_status - 1]);
        int64 cur_version = DatumGetInt64(datums[Anum_falcon_kvblock_table_version - 1]);
        if (cur_status != KV_CATALOG_STATUS_EVICTED ||
            (items[i].expected_version > 0 && cur_version != items[i].expected_version)) {
            heap_freetuple(cur);
            continue;
        }

        Datum new_d[Natts_falcon_kvblock_table];
        bool  new_n[Natts_falcon_kvblock_table];
        bool  rep[Natts_falcon_kvblock_table];
        memset(rep, 0, sizeof(rep));
        memset(new_n, 0, sizeof(new_n));
        new_d[Anum_falcon_kvblock_table_status - 1]        = Int16GetDatum((int16) KV_CATALOG_STATUS_ALLOCATED);
        rep[Anum_falcon_kvblock_table_status - 1]          = true;
        new_d[Anum_falcon_kvblock_table_store_node_id - 1] = Int32GetDatum(items[i].store_node_id);
        rep[Anum_falcon_kvblock_table_store_node_id - 1]   = true;
        new_d[Anum_falcon_kvblock_table_pool_offset - 1]   = Int64GetDatum(items[i].pool_offset);
        rep[Anum_falcon_kvblock_table_pool_offset - 1]     = true;
        rep[Anum_falcon_kvblock_table_evicted_path - 1]    = true;
        new_n[Anum_falcon_kvblock_table_evicted_path - 1]  = true;
        new_d[Anum_falcon_kvblock_table_version - 1]       = Int64GetDatum(cur_version + 1);
        rep[Anum_falcon_kvblock_table_version - 1]         = true;
        new_d[Anum_falcon_kvblock_table_updated_at_ms - 1] = Int64GetDatum(items[i].now_ms);
        rep[Anum_falcon_kvblock_table_updated_at_ms - 1]   = true;
        HeapTuple updated = heap_modify_tuple(cur, tupdesc, new_d, new_n, rep);
        CatalogTupleUpdateWithInfo(rel, &cur->t_self, updated, istate);
        heap_freetuple(updated);
        heap_freetuple(cur);
        results[i].promoted = 1;
        results[i].new_version = cur_version + 1;
    }
    CommandCounterIncrement();
    CatalogCloseIndexes(istate);
    table_close(rel, RowExclusiveLock);
}

uint32_t FalconKVBlockScanForRecovery(KVCatalogRecoveryRow *out_rows,
                                      uint32_t out_rows_capacity)
{
    if (out_rows == NULL || out_rows_capacity == 0)
        return 0;

    Relation rel = table_open(KvblockRelationId(), AccessShareLock);
    SysScanDesc scan = systable_beginscan(rel, InvalidOid, false /*indexOK*/,
                                          GetActiveSnapshot(), 0, NULL);
    TupleDesc tupdesc = RelationGetDescr(rel);
    uint32_t produced = 0;
    HeapTuple t;
    while ((t = systable_getnext(scan)) != NULL) {
        if (produced >= out_rows_capacity)
            break;
        Datum datums[Natts_falcon_kvblock_table];
        bool  isnulls[Natts_falcon_kvblock_table];
        heap_deform_tuple(t, tupdesc, datums, isnulls);

        KVCatalogRecoveryRow *row = &out_rows[produced];
        memset(row, 0, sizeof(*row));

        if (!isnulls[Anum_falcon_kvblock_table_block_hash - 1]) {
            bytea *bh = DatumGetByteaP(datums[Anum_falcon_kvblock_table_block_hash - 1]);
            int sz = VARSIZE_ANY_EXHDR(bh);
            if (sz < 0) sz = 0;
            if (sz > KV_CATALOG_BLOCK_HASH_MAX_LEN) sz = KV_CATALOG_BLOCK_HASH_MAX_LEN;
            memcpy(row->block_hash, VARDATA_ANY(bh), sz);
            row->block_hash_len = (uint16_t) sz;
        }
        row->status = (uint8_t) DatumGetInt16(datums[Anum_falcon_kvblock_table_status - 1]);
        row->store_node_id = DatumGetInt32(datums[Anum_falcon_kvblock_table_store_node_id - 1]);
        row->pool_offset = DatumGetInt64(datums[Anum_falcon_kvblock_table_pool_offset - 1]);
        row->version = DatumGetInt64(datums[Anum_falcon_kvblock_table_version - 1]);
        row->updated_at_ms = DatumGetInt64(datums[Anum_falcon_kvblock_table_updated_at_ms - 1]);
        CopyEvictedPathField(datums, isnulls, row->evicted_path, &row->evicted_path_len);

        ++produced;
    }
    systable_endscan(scan);
    table_close(rel, AccessShareLock);
    return produced;
}

void FalconKVBlockReconcileStoreRestart(int32_t store_node_id,
                                        int64_t now_ms,
                                        KVCatalogStoreRestartResponse *out)
{
    if (out == NULL)
        return;
    memset(out, 0, sizeof(*out));

    Relation rel = table_open(KvblockRelationId(), RowExclusiveLock);
    CatalogIndexState istate = CatalogOpenIndexes(rel);
    TupleDesc tupdesc = RelationGetDescr(rel);
    SysScanDesc scan = systable_beginscan(rel, InvalidOid, false /*indexOK*/,
                                          GetActiveSnapshot(), 0, NULL);
    HeapTuple t;
    while ((t = systable_getnext(scan)) != NULL) {
        Datum datums[Natts_falcon_kvblock_table];
        bool  isnulls[Natts_falcon_kvblock_table];
        heap_deform_tuple(t, tupdesc, datums, isnulls);
        int32 row_store = DatumGetInt32(datums[Anum_falcon_kvblock_table_store_node_id - 1]);
        if (row_store != store_node_id)
            continue;

        ++out->scanned_rows;
        int32 status = (int32) DatumGetInt16(datums[Anum_falcon_kvblock_table_status - 1]);
        bool has_evicted_path = false;
        if (!isnulls[Anum_falcon_kvblock_table_evicted_path - 1]) {
            text *path = DatumGetTextP(datums[Anum_falcon_kvblock_table_evicted_path - 1]);
            has_evicted_path = VARSIZE_ANY_EXHDR(path) > 0;
        }

        if (status == KV_CATALOG_STATUS_EVICTED && has_evicted_path) {
            ++out->preserved_evicted_rows;
            continue;
        }

        simple_heap_delete(rel, &t->t_self);
        ++out->deleted_rows;
    }
    systable_endscan(scan);
    CommandCounterIncrement();
    CatalogCloseIndexes(istate);
    table_close(rel, RowExclusiveLock);
    (void) now_ms;
}


uint32_t FalconKVBlockScanStoreEvicted(int32_t store_node_id,
                                       KVCatalogStoreEvictedRow *out_rows,
                                       uint32_t max_rows)
{
    if (out_rows == NULL || max_rows == 0)
        return 0;
    uint32_t produced = 0;
    Relation rel = table_open(KvblockRelationId(), AccessShareLock);
    TupleDesc tupdesc = RelationGetDescr(rel);
    SysScanDesc scan = systable_beginscan(rel, InvalidOid, false /*indexOK*/,
                                          GetActiveSnapshot(), 0, NULL);
    HeapTuple t;
    while (produced < max_rows && (t = systable_getnext(scan)) != NULL) {
        Datum datums[Natts_falcon_kvblock_table];
        bool  isnulls[Natts_falcon_kvblock_table];
        heap_deform_tuple(t, tupdesc, datums, isnulls);
        int32 row_store = DatumGetInt32(datums[Anum_falcon_kvblock_table_store_node_id - 1]);
        int32 status = (int32) DatumGetInt16(datums[Anum_falcon_kvblock_table_status - 1]);
        if (row_store != store_node_id || status != KV_CATALOG_STATUS_EVICTED)
            continue;
        if (isnulls[Anum_falcon_kvblock_table_evicted_path - 1])
            continue;
        text *path = DatumGetTextP(datums[Anum_falcon_kvblock_table_evicted_path - 1]);
        if (VARSIZE_ANY_EXHDR(path) <= 0)
            continue;

        Datum hash_d = datums[Anum_falcon_kvblock_table_block_hash - 1];
        bytea *hash = DatumGetByteaP(hash_d);
        int hash_len = VARSIZE_ANY_EXHDR(hash);
        if (hash_len > KV_CATALOG_BLOCK_HASH_MAX_LEN)
            hash_len = KV_CATALOG_BLOCK_HASH_MAX_LEN;
        KVCatalogStoreEvictedRow *row = &out_rows[produced];
        memset(row, 0, sizeof(*row));
        row->block_hash_len = (uint16_t) hash_len;
        memcpy(row->block_hash, VARDATA_ANY(hash), hash_len);
        row->version = DatumGetInt64(datums[Anum_falcon_kvblock_table_version - 1]);
        CopyEvictedPathField(datums, isnulls, row->evicted_path, &row->evicted_path_len);
        ++produced;
    }
    systable_endscan(scan);
    table_close(rel, AccessShareLock);
    return produced;
}

void FalconKVBlockDeleteInvalidEvicted(const char *req_buf, uint64_t req_size,
                                       char *resp_buf, uint64_t resp_size)
{
    if (req_size < sizeof(uint32_t) || resp_size < sizeof(uint32_t))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "kvblock invalid evicted delete: buffer too small");
    uint32_t count = 0;
    memcpy(&count, req_buf, sizeof(uint32_t));
    const uint64_t expect_req = sizeof(uint32_t) +
        (uint64_t) count * sizeof(KVCatalogDeleteInvalidEvictedItem);
    const uint64_t expect_resp = KVCatalogResponseSize(KV_CATALOG_METHOD_DELETE_STORE_EVICTED_INVALID, count);
    if (req_size < expect_req || resp_size < expect_resp)
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "kvblock invalid evicted delete: count/size mismatch");
    memcpy(resp_buf, &count, sizeof(uint32_t));
    if (count == 0)
        return;

    const KVCatalogDeleteInvalidEvictedItem *items =
        (const KVCatalogDeleteInvalidEvictedItem *)(req_buf + sizeof(uint32_t));
    KVCatalogDeleteInvalidEvictedResult *results =
        (KVCatalogDeleteInvalidEvictedResult *)(resp_buf + sizeof(uint32_t));
    memset(results, 0, (size_t) count * sizeof(KVCatalogDeleteInvalidEvictedResult));

    Relation rel = table_open(KvblockRelationId(), RowExclusiveLock);
    for (uint32_t i = 0; i < count; ++i) {
        Datum datums[Natts_falcon_kvblock_table];
        bool  isnulls[Natts_falcon_kvblock_table];
        memset(isnulls, 0, sizeof(isnulls));
        HeapTuple cur = NULL;
        bool found = FetchRowByHash(rel, items[i].block_hash, items[i].block_hash_len,
                                    &cur, datums, isnulls);
        if (!found) {
            results[i].deleted = 1;
            continue;
        }
        int32 status = (int32) DatumGetInt16(datums[Anum_falcon_kvblock_table_status - 1]);
        int64 version = DatumGetInt64(datums[Anum_falcon_kvblock_table_version - 1]);
        if (status != KV_CATALOG_STATUS_EVICTED ||
            (items[i].expected_version > 0 && version != items[i].expected_version)) {
            results[i].conflict = 1;
            heap_freetuple(cur);
            continue;
        }
        simple_heap_delete(rel, &cur->t_self);
        heap_freetuple(cur);
        results[i].deleted = 1;
    }
    CommandCounterIncrement();
    table_close(rel, RowExclusiveLock);
}

/* The dn_epoch row is keyed by shard_id. Scan sequentially since the table is
 * tiny (one row per shard). Returns false if the table is missing
 * (pre-migration deployments). */
static bool DnEpochTableExists(void)
{
    /* The schema creator places the table in pg_catalog. */
    Oid nsp = get_namespace_oid("pg_catalog", true);
    if (nsp == InvalidOid) return false;
    return get_relname_relid("falcon_kvblock_dn_epoch", nsp) != InvalidOid;
}

static Oid DnEpochRelOid(void)
{
    Oid nsp = get_namespace_oid("pg_catalog", true);
    if (nsp == InvalidOid) return InvalidOid;
    return get_relname_relid("falcon_kvblock_dn_epoch", nsp);
}

#define DN_EPOCH_NATTS 2
#define DN_EPOCH_ANUM_SHARD 1
#define DN_EPOCH_ANUM_EPOCH 2

static HeapTuple DnEpochFetchRow(Relation rel, int32 shard_id, Datum *datums, bool *isnulls)
{
    SysScanDesc scan = systable_beginscan(rel, InvalidOid, false /*indexOK*/,
                                          GetActiveSnapshot(), 0, NULL);
    HeapTuple t;
    HeapTuple match = NULL;
    TupleDesc tupdesc = RelationGetDescr(rel);
    while ((t = systable_getnext(scan)) != NULL) {
        Datum cur_datums[DN_EPOCH_NATTS];
        bool  cur_nulls[DN_EPOCH_NATTS];
        heap_deform_tuple(t, tupdesc, cur_datums, cur_nulls);
        if (!cur_nulls[DN_EPOCH_ANUM_SHARD - 1] &&
            DatumGetInt32(cur_datums[DN_EPOCH_ANUM_SHARD - 1]) == shard_id) {
            match = heap_copytuple(t);
            if (datums != NULL && isnulls != NULL) {
                memcpy(datums, cur_datums, sizeof(cur_datums));
                memcpy(isnulls, cur_nulls, sizeof(cur_nulls));
            }
            break;
        }
    }
    systable_endscan(scan);
    return match;
}

int64_t FalconKVBlockLoadDnEpoch(int32_t shard_id)
{
    if (!DnEpochTableExists())
        return 1;
    Oid relid = DnEpochRelOid();
    if (relid == InvalidOid) return 1;

    int64_t epoch = 1;
    Relation rel = table_open(relid, AccessShareLock);
    Datum datums[DN_EPOCH_NATTS];
    bool  isnulls[DN_EPOCH_NATTS];
    HeapTuple t = DnEpochFetchRow(rel, shard_id, datums, isnulls);
    if (t != NULL) {
        if (!isnulls[DN_EPOCH_ANUM_EPOCH - 1])
            epoch = DatumGetInt64(datums[DN_EPOCH_ANUM_EPOCH - 1]);
        heap_freetuple(t);
    }
    table_close(rel, AccessShareLock);
    return epoch;
}

int64_t FalconKVBlockBumpDnEpoch(int32_t shard_id)
{
    if (!DnEpochTableExists())
        return 1;
    Oid relid = DnEpochRelOid();
    if (relid == InvalidOid) return 1;

    int64_t new_epoch = 1;
    Relation rel = table_open(relid, RowExclusiveLock);
    CatalogIndexState istate = CatalogOpenIndexes(rel);
    TupleDesc tupdesc = RelationGetDescr(rel);
    Datum datums[DN_EPOCH_NATTS];
    bool  isnulls[DN_EPOCH_NATTS];
    HeapTuple cur = DnEpochFetchRow(rel, shard_id, datums, isnulls);

    if (cur == NULL) {
        /* First bump: insert (shard_id, 2). LoadDnEpoch returns 1 when absent,
         * so the first bump must produce 2 to remain monotonically increasing. */
        new_epoch = 2;
        Datum new_d[DN_EPOCH_NATTS];
        bool  new_n[DN_EPOCH_NATTS] = {false, false};
        new_d[DN_EPOCH_ANUM_SHARD - 1] = Int32GetDatum(shard_id);
        new_d[DN_EPOCH_ANUM_EPOCH - 1] = Int64GetDatum(new_epoch);
        HeapTuple t = heap_form_tuple(tupdesc, new_d, new_n);
        CatalogTupleInsertWithInfo(rel, t, istate);
        heap_freetuple(t);
    } else {
        int64_t cur_epoch = isnulls[DN_EPOCH_ANUM_EPOCH - 1]
                                ? 1
                                : DatumGetInt64(datums[DN_EPOCH_ANUM_EPOCH - 1]);
        new_epoch = cur_epoch + 1;
        Datum new_d[DN_EPOCH_NATTS];
        bool  new_n[DN_EPOCH_NATTS] = {false, false};
        bool  rep[DN_EPOCH_NATTS] = {false, true};
        new_d[DN_EPOCH_ANUM_EPOCH - 1] = Int64GetDatum(new_epoch);
        HeapTuple updated = heap_modify_tuple(cur, tupdesc, new_d, new_n, rep);
        CatalogTupleUpdateWithInfo(rel, &cur->t_self, updated, istate);
        heap_freetuple(updated);
        heap_freetuple(cur);
    }
    CommandCounterIncrement();
    CatalogCloseIndexes(istate);
    table_close(rel, RowExclusiveLock);
    return new_epoch;
}
