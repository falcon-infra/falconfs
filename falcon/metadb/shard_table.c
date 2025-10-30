/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "metadb/shard_table.h"

#include "postgres.h"

#include "access/genam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "storage/shmem.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "metadb/foreign_server.h"
#include "utils/error_log.h"
#include "utils/shmem_control.h"
#include "utils/utils.h"

static ShmemControlData *ShardTableShmemControl = NULL;
static FormData_falcon_shard_table *ShardTableShmemCache = NULL;
static int32_t *ShardTableShmemCacheCount = NULL;
static pg_atomic_uint32 *ShardTableShmemCacheInvalid = NULL;

PG_FUNCTION_INFO_V1(falcon_build_shard_table);
PG_FUNCTION_INFO_V1(falcon_update_shard_table);
PG_FUNCTION_INFO_V1(falcon_reload_shard_table_cache);
PG_FUNCTION_INFO_V1(falcon_renew_shard_table);

Datum falcon_build_shard_table(PG_FUNCTION_ARGS)
{
    int32_t shardCount = PG_GETARG_INT32(0);

    if (shardCount <= 0 || shardCount > SHARD_COUNT_MAX)
        FALCON_ELOG_ERROR_EXTENDED(ARGUMENT_ERROR, "shard count must be in range (%d, %d]", 0, SHARD_COUNT_MAX);

    List *serverIdList = GetAllForeignServerId(false, true);
    if (list_length(serverIdList) == 0)
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "no worker.");

    int serverRound = 0;
    Relation rel = table_open(ShardRelationId(), RowExclusiveLock);
    CatalogIndexState indstate = CatalogOpenIndexes(rel);
    TupleDesc tupleDesc = RelationGetDescr(rel);
    for (int i = 0; i < shardCount; ++i) {
        int32_t rangePoint;
        if (i == shardCount - 1)
            rangePoint = SHARD_TABLE_RANGE_MAX;
        else
            rangePoint =
                ((int64_t)SHARD_TABLE_RANGE_MAX - SHARD_TABLE_RANGE_MIN) * (i + 1) / shardCount + SHARD_TABLE_RANGE_MIN;

        Datum datumArray[Natts_falcon_shard_table];
        bool isNullArray[Natts_falcon_shard_table];
        memset(isNullArray, 0, sizeof(isNullArray));
        datumArray[Anum_falcon_shard_table_range_point - 1] = Int32GetDatum(rangePoint);
        datumArray[Anum_falcon_shard_table_server_id - 1] = Int32GetDatum(list_nth_int(serverIdList, serverRound));
        HeapTuple heapTuple = heap_form_tuple(tupleDesc, datumArray, isNullArray);
        CatalogTupleInsertWithInfo(rel, heapTuple, indstate);

        serverRound = (serverRound + 1) % list_length(serverIdList);
    }
    CommandCounterIncrement();
    CatalogCloseIndexes(indstate);
    table_close(rel, RowExclusiveLock);
    InvalidateShardTableShmemCache();

    PG_RETURN_INT16(0);
}

Datum falcon_update_shard_table(PG_FUNCTION_ARGS)
{
    ArrayType *rangePointArrayType = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *serverIdArrayType = PG_GETARG_ARRAYTYPE_P(1);

    int rangePointCount;
    Datum *rangePointArray;
    ArrayTypeArrayToDatumArrayAndSize(rangePointArrayType, &rangePointArray, &rangePointCount);
    int serverIdCount;
    Datum *serverIdArray;
    ArrayTypeArrayToDatumArrayAndSize(serverIdArrayType, &serverIdArray, &serverIdCount);
    if (rangePointCount != serverIdCount)
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "range_point array must be as long as server_id array.");
    int changeCount = rangePointCount;

    Relation rel = table_open(ShardRelationId(), RowExclusiveLock);
    CatalogIndexState indstate = CatalogOpenIndexes(rel);
    TupleDesc tupleDesc = RelationGetDescr(rel);
    Datum datumArray[Natts_falcon_shard_table];
    bool isNullArray[Natts_falcon_shard_table];
    memset(isNullArray, 0, sizeof(isNullArray));
    bool doUpdateArray[Natts_falcon_shard_table];
    memset(doUpdateArray, 0, sizeof(doUpdateArray));
    doUpdateArray[Anum_falcon_shard_table_server_id - 1] = true;
    for (int i = 0; i < changeCount; i++) {
        ScanKeyData scanKey[1];
        ScanKeyInit(&scanKey[0],
                    Anum_falcon_shard_table_range_point,
                    BTEqualStrategyNumber,
                    F_INT4EQ,
                    rangePointArray[i]);
        SysScanDesc scanDesc =
            systable_beginscan(rel, ShardRelationIndexId(), true, GetTransactionSnapshot(), 1, scanKey);
        HeapTuple heapTuple = systable_getnext(scanDesc);
        if (!HeapTupleIsValid(heapTuple)) {
            // insert
            datumArray[Anum_falcon_shard_table_range_point - 1] = rangePointArray[i];
            datumArray[Anum_falcon_shard_table_server_id - 1] = serverIdArray[i];
            CatalogTupleInsertWithInfo(rel, heap_form_tuple(tupleDesc, datumArray, isNullArray), indstate);
        } else {
            // update
            datumArray[Anum_falcon_shard_table_server_id - 1] = serverIdArray[i];
            HeapTuple updatedTuple = heap_modify_tuple(heapTuple, tupleDesc, datumArray, isNullArray, doUpdateArray);
            CatalogTupleUpdateWithInfo(rel, &updatedTuple->t_self, updatedTuple, indstate);
        }
        systable_endscan(scanDesc);

        CommandCounterIncrement();
    }

    CatalogCloseIndexes(indstate);
    table_close(rel, RowExclusiveLock);
    InvalidateShardTableShmemCache();

    PG_RETURN_INT16(0);
}

Datum falcon_reload_shard_table_cache(PG_FUNCTION_ARGS)
{
    InvalidateShardTableShmemCache();
    ReloadShardTableShmemCache();

    PG_RETURN_INT16(0);
}

Datum falcon_renew_shard_table(PG_FUNCTION_ARGS)
{
    FuncCallContext *functionContext = NULL;
    List *returnInfoList = NIL;
    uint32_t d_off;
    ShardInfo *shardInfo;
    Datum values[5];
    bool resNulls[5];
    HeapTuple heapTupleRes;
    TupleDesc tupleDescriptor;

    if (SRF_IS_FIRSTCALL()) {
        functionContext = SRF_FIRSTCALL_INIT();

        MemoryContext oldContext = MemoryContextSwitchTo(functionContext->multi_call_memory_ctx);
        if (get_call_result_type(fcinfo, NULL, &tupleDescriptor) != TYPEFUNC_COMPOSITE) {
            FALCON_ELOG_ERROR(PROGRAM_ERROR, "return type must be a row type");
        }
        functionContext->tuple_desc = BlessTupleDesc(tupleDescriptor);

        while (pg_atomic_read_u32(ShardTableShmemCacheInvalid)) {
            ReloadShardTableShmemCache();
        }
        LWLockAcquire(&ShardTableShmemControl->lock, AccessShareLock);

        List *shardIdList = NIL;
        int32_t rangeLow = SHARD_TABLE_RANGE_MIN;
        for (int i = 0; i < *ShardTableShmemCacheCount; i++) {
            FormData_falcon_shard_table *shardTableRow = ShardTableShmemCache + i;

            shardInfo = (ShardInfo *)palloc(sizeof(ShardInfo));

            shardInfo->rangeMin = rangeLow;
            shardInfo->rangeMax = shardTableRow->range_point;
            shardInfo->serverId = shardTableRow->server_id;
            rangeLow = shardTableRow->range_point + 1;

            shardIdList = lappend_int(shardIdList, shardTableRow->server_id);
            returnInfoList = lappend(returnInfoList, shardInfo);
        }

        LWLockRelease(&ShardTableShmemControl->lock);

        List *connectionInfoList = GetForeignServerConnectionInfo(shardIdList);
        for (int i = 0; i < list_length(returnInfoList); ++i) {
            ShardInfo *shardInfo = list_nth(returnInfoList, i);
            ForeignServerConnectionInfo *connectionInfo = list_nth(connectionInfoList, i);

            strcpy(shardInfo->host, connectionInfo->host);
            shardInfo->port = connectionInfo->port;
        }

        functionContext->user_fctx = returnInfoList;
        functionContext->max_calls = list_length(returnInfoList);
        MemoryContextSwitchTo(oldContext);
    }

    functionContext = SRF_PERCALL_SETUP();
    returnInfoList = functionContext->user_fctx;
    d_off = functionContext->call_cntr;

    if (d_off < functionContext->max_calls) {
        shardInfo = (ShardInfo *)list_nth(returnInfoList, d_off);
        memset(resNulls, false, sizeof(resNulls));
        values[0] = Int32GetDatum(shardInfo->rangeMin);
        values[1] = Int32GetDatum(shardInfo->rangeMax);
        values[2] = CStringGetTextDatum(shardInfo->host);
        values[3] = UInt32GetDatum(shardInfo->port);
        values[4] = UInt32GetDatum(shardInfo->serverId);
        heapTupleRes = heap_form_tuple(functionContext->tuple_desc, values, resNulls);
        SRF_RETURN_NEXT(functionContext, HeapTupleGetDatum(heapTupleRes));
    }

    SRF_RETURN_DONE(functionContext);
}

Oid ShardRelationId(void)
{
    GetRelationOid("falcon_shard_table", &CachedRelationOid[CACHED_RELATION_SHARD_TABLE]);
    return CachedRelationOid[CACHED_RELATION_SHARD_TABLE];
}

Oid ShardRelationIndexId(void)
{
    GetRelationOid("falcon_shard_table_index", &CachedRelationOid[CACHED_RELATION_SHARD_TABLE_INDEX]);
    return CachedRelationOid[CACHED_RELATION_SHARD_TABLE_INDEX];
}

void SearchShardInfoByHashValue(int32_t hashValue, int32_t *rangePoint, int32_t *serverId)
{
    while (pg_atomic_read_u32(ShardTableShmemCacheInvalid)) {
        ReloadShardTableShmemCache();
    }
    LWLockAcquire(&ShardTableShmemControl->lock, LW_SHARED);
    int l = 0;
    int r = *ShardTableShmemCacheCount;
    while (l < r) {
        int mid = (l + r) / 2;
        if (ShardTableShmemCache[mid].range_point < hashValue)
            l = mid + 1;
        else
            r = mid;
    }
    if (l == *ShardTableShmemCacheCount) {
        FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR,
                                   "shard value %d out of range, max support %d",
                                   hashValue,
                                   ShardTableShmemCache[*ShardTableShmemCacheCount - 1].range_point);
    }
    *rangePoint = ShardTableShmemCache[l].range_point;
    *serverId = ShardTableShmemCache[l].server_id;
    LWLockRelease(&ShardTableShmemControl->lock);
}
void SearchShardInfoByShardValue(uint64_t shardColValue, int32_t *rangePoint, int32_t *serverId)
{
    // Block shard map read only when transfer operations acquire AccessExclusiveLock
    int32 hashValue = HashShard(shardColValue);
    SearchShardInfoByHashValue(hashValue, rangePoint, serverId);
}

List *GetShardTableData()
{
    while (pg_atomic_read_u32(ShardTableShmemCacheInvalid)) {
        ReloadShardTableShmemCache();
    }
    LWLockAcquire(&ShardTableShmemControl->lock, AccessShareLock);

    List *result = NIL;
    for (int i = 0; i < *ShardTableShmemCacheCount; i++) {
        FormData_falcon_shard_table *shardTableRow = ShardTableShmemCache + i;

        FormData_falcon_shard_table *data = palloc(sizeof(FormData_falcon_shard_table));
        data->range_point = shardTableRow->range_point;
        data->server_id = shardTableRow->server_id;
        result = lappend(result, data);
    }
    LWLockRelease(&ShardTableShmemControl->lock);
    return result;
}

int32_t GetShardTableSize()
{
    while (pg_atomic_read_u32(ShardTableShmemCacheInvalid)) {
        ReloadShardTableShmemCache();
    }
    LWLockAcquire(&ShardTableShmemControl->lock, AccessShareLock);

    int32_t result = *ShardTableShmemCacheCount;

    LWLockRelease(&ShardTableShmemControl->lock);
    return result;
}

void InvalidateShardTableShmemCacheCallback(Datum argument, Oid relationId)
{
    if (relationId == InvalidOid || relationId == ShardRelationId()) {
        InvalidateShardTableShmemCache();
    }
}

size_t ShardTableShmemsize()
{
    return sizeof(ShmemControlData) + sizeof(int32_t) + sizeof(pg_atomic_uint32) +
           sizeof(FormData_falcon_shard_table) * SHARD_COUNT_MAX;
}
void ShardTableShmemInit()
{
    bool initialized;
    ShardTableShmemControl = ShmemInitStruct("Shard Table Control", ShardTableShmemsize(), &initialized);
    ShardTableShmemCacheCount = (int32_t *)(ShardTableShmemControl + 1);
    ShardTableShmemCacheInvalid = (pg_atomic_uint32 *)(ShardTableShmemCacheCount + 1);
    ShardTableShmemCache = (FormData_falcon_shard_table *)(ShardTableShmemCacheInvalid + 1);
    if (!initialized) {
        ShardTableShmemControl->trancheId = LWLockNewTrancheId();
        ShardTableShmemControl->lockTrancheName = "Falcon Shard Table Shmem Control";
        LWLockRegisterTranche(ShardTableShmemControl->trancheId, ShardTableShmemControl->lockTrancheName);
        LWLockInitialize(&ShardTableShmemControl->lock, ShardTableShmemControl->trancheId);

        *ShardTableShmemCacheCount = 0;
        pg_atomic_init_u32(ShardTableShmemCacheInvalid, 1);
    }
}

void InvalidateShardTableShmemCache() { pg_atomic_exchange_u32(ShardTableShmemCacheInvalid, 1); }

void ReloadShardTableShmemCache()
{
    LWLockAcquire(&ShardTableShmemControl->lock, LW_EXCLUSIVE);
    if (!pg_atomic_exchange_u32(ShardTableShmemCacheInvalid, 0)) {
        LWLockRelease(&ShardTableShmemControl->lock);
        return;
    }

    *ShardTableShmemCacheCount = 0;
    bool exceedMaxNumOfShardTable = false;
    Relation rel = table_open(ShardRelationId(), AccessShareLock);
    Relation relIndex = index_open(ShardRelationIndexId(), AccessShareLock);
    SysScanDesc scanDesc = systable_beginscan_ordered(rel, relIndex, NULL, 0, NULL);
    TupleDesc tupleDesc = RelationGetDescr(rel);

    Datum datumArray[Natts_falcon_shard_table];
    bool isNullArray[Natts_falcon_shard_table];
    HeapTuple heapTuple;
    while (HeapTupleIsValid(heapTuple = systable_getnext(scanDesc))) {
        if (*ShardTableShmemCacheCount >= SHARD_COUNT_MAX) {
            exceedMaxNumOfShardTable = true;
            break;
        }

        heap_deform_tuple(heapTuple, tupleDesc, datumArray, isNullArray);

        ShardTableShmemCache[*ShardTableShmemCacheCount].range_point =
            DatumGetInt32(datumArray[Anum_falcon_shard_table_range_point - 1]);
        ShardTableShmemCache[*ShardTableShmemCacheCount].server_id =
            DatumGetInt32(datumArray[Anum_falcon_shard_table_server_id - 1]);

        ++*ShardTableShmemCacheCount;
    }
    systable_endscan_ordered(scanDesc);
    index_close(relIndex, AccessShareLock);
    table_close(rel, AccessShareLock);

    if (exceedMaxNumOfShardTable) {
        InvalidateShardTableShmemCache();
        FALCON_ELOG_ERROR_EXTENDED(
            PROGRAM_ERROR,
            "shard table exceed max size %d, tables whose range_point are larger than %d are ignored",
            SHARD_COUNT_MAX,
            ShardTableShmemCache[*ShardTableShmemCacheCount - 1].range_point);
    }

    LWLockRelease(&ShardTableShmemControl->lock);
}
