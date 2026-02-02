/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "perf_counter/falcon_perf_latency_shmem.h"
#include "postgres.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/falcon_meta_service_def.h"

/* Global shared memory pointer */
FalconPerfLatencyShmem *g_FalconPerfLatencyShmem = NULL;

/* GUC variable for enabling/disabling performance monitoring */
bool falcon_perf_enabled = true;

/* LWLock tranche ID for performance counters */
static int falcon_perf_lwlock_tranche_id = 0;

/* Helper function to initialize LWLocks for OpcodeLatencyBreakdown */
static void OpcodeLatencyBreakdownLWLockInit(OpcodeLatencyBreakdown *op, int tranche_id)
{
    LWLockInitialize(&op->total.lock, tranche_id);
    LWLockInitialize(&op->e2eLatency.lock, tranche_id);
    LWLockInitialize(&op->pathVerify.lock, tranche_id);
    LWLockInitialize(&op->tableOpen.lock, tranche_id);
    LWLockInitialize(&op->pathParse.lock, tranche_id);
    LWLockInitialize(&op->indexScan.lock, tranche_id);
    LWLockInitialize(&op->tableModify.lock, tranche_id);
    LWLockInitialize(&op->remoteCall.lock, tranche_id);
    /* New fields */
    LWLockInitialize(&op->tableClose.lock, tranche_id);
    LWLockInitialize(&op->indexOpen.lock, tranche_id);
    LWLockInitialize(&op->indexClose.lock, tranche_id);
    LWLockInitialize(&op->commit.lock, tranche_id);
}

/*
 * Calculate shared memory size required for performance counters
 */
Size FalconPerfLatencyShmemSize(void) { return MAXALIGN(sizeof(FalconPerfLatencyShmem)); }

/*
 * Initialize shared memory for performance counters
 */
void FalconPerfLatencyShmemInit(void)
{
    bool found;
    Size size = FalconPerfLatencyShmemSize();

    g_FalconPerfLatencyShmem = (FalconPerfLatencyShmem *)ShmemInitStruct("FalconPerfLatency", size, &found);

    if (!found) {
        /* First time initialization - zero out all data */
        memset(g_FalconPerfLatencyShmem, 0, size);

        /* Initialize enabled from GUC variable (may have been set from postgresql.conf) */
        g_FalconPerfLatencyShmem->enabled = falcon_perf_enabled;

        /* Initialize all LatencyData structures */
        /* Connection Pool Layer - Common */
        LatencyDataInit(&g_FalconPerfLatencyShmem->queueWaitLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->enqueueDelayLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->inQueueLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->connWaitLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->workerWaitLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->totalRequestLatency);
        /* Connection Pool Layer - Batch path */
        LatencyDataInit(&g_FalconPerfLatencyShmem->shmemCopyLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->pgExecTotalLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->resultProcLatency);
        /* Connection Pool Layer - Single path */
        LatencyDataInit(&g_FalconPerfLatencyShmem->shmemCopySingleLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->pgExecSingleLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->resultProcSingleLatency);
        /* Serialization Layer */
        LatencyDataInit(&g_FalconPerfLatencyShmem->paramDecodeLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->responseEncodeLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->shmemAllocLatency);

        /* File Semantic Operations - Per-opcode breakdown */
        OpcodeLatencyBreakdownInit(&g_FalconPerfLatencyShmem->createOp);
        OpcodeLatencyBreakdownInit(&g_FalconPerfLatencyShmem->mkdirOp);
        OpcodeLatencyBreakdownInit(&g_FalconPerfLatencyShmem->statOp);
        OpcodeLatencyBreakdownInit(&g_FalconPerfLatencyShmem->openOp);
        OpcodeLatencyBreakdownInit(&g_FalconPerfLatencyShmem->closeOp);
        OpcodeLatencyBreakdownInit(&g_FalconPerfLatencyShmem->unlinkOp);
        OpcodeLatencyBreakdownInit(&g_FalconPerfLatencyShmem->rmdirOp);
        OpcodeLatencyBreakdownInit(&g_FalconPerfLatencyShmem->renameOp);
        OpcodeLatencyBreakdownInit(&g_FalconPerfLatencyShmem->readdirOp);
        OpcodeLatencyBreakdownInit(&g_FalconPerfLatencyShmem->opendirOp);

        /* KV Operations - Per-opcode breakdown */
        OpcodeLatencyBreakdownInit(&g_FalconPerfLatencyShmem->kvPutOp);
        OpcodeLatencyBreakdownInit(&g_FalconPerfLatencyShmem->kvGetOp);
        OpcodeLatencyBreakdownInit(&g_FalconPerfLatencyShmem->kvDelOp);

        /* Slice Operations */
        LatencyDataInit(&g_FalconPerfLatencyShmem->slicePutLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->sliceGetLatency);

        /* Legacy global counters */
        LatencyDataInit(&g_FalconPerfLatencyShmem->pathVerifyLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->pathParseLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->tableOpenLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->indexScanLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->tableInsertLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->tableUpdateLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->tableDeleteLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->commitLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->dirInsertLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->dirSearchLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->dirDeleteLatency);
        LatencyDataInit(&g_FalconPerfLatencyShmem->remoteCallLatency);

        /* Initialize LWLocks */
        falcon_perf_lwlock_tranche_id = LWLockNewTrancheId();
        LWLockRegisterTranche(falcon_perf_lwlock_tranche_id, "falcon_perf_latency");

        /* Initialize locks for Connection Pool Layer */
        LWLockInitialize(&g_FalconPerfLatencyShmem->queueWaitLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->enqueueDelayLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->inQueueLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->connWaitLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->workerWaitLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->totalRequestLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->shmemCopyLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->pgExecTotalLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->resultProcLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->shmemCopySingleLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->pgExecSingleLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->resultProcSingleLatency.lock, falcon_perf_lwlock_tranche_id);
        /* Serialization Layer */
        LWLockInitialize(&g_FalconPerfLatencyShmem->paramDecodeLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->responseEncodeLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->shmemAllocLatency.lock, falcon_perf_lwlock_tranche_id);

        /* Initialize locks for File Semantic Operations (per-opcode breakdown) */
        OpcodeLatencyBreakdownLWLockInit(&g_FalconPerfLatencyShmem->createOp, falcon_perf_lwlock_tranche_id);
        OpcodeLatencyBreakdownLWLockInit(&g_FalconPerfLatencyShmem->mkdirOp, falcon_perf_lwlock_tranche_id);
        OpcodeLatencyBreakdownLWLockInit(&g_FalconPerfLatencyShmem->statOp, falcon_perf_lwlock_tranche_id);
        OpcodeLatencyBreakdownLWLockInit(&g_FalconPerfLatencyShmem->openOp, falcon_perf_lwlock_tranche_id);
        OpcodeLatencyBreakdownLWLockInit(&g_FalconPerfLatencyShmem->closeOp, falcon_perf_lwlock_tranche_id);
        OpcodeLatencyBreakdownLWLockInit(&g_FalconPerfLatencyShmem->unlinkOp, falcon_perf_lwlock_tranche_id);
        OpcodeLatencyBreakdownLWLockInit(&g_FalconPerfLatencyShmem->rmdirOp, falcon_perf_lwlock_tranche_id);
        OpcodeLatencyBreakdownLWLockInit(&g_FalconPerfLatencyShmem->renameOp, falcon_perf_lwlock_tranche_id);
        OpcodeLatencyBreakdownLWLockInit(&g_FalconPerfLatencyShmem->readdirOp, falcon_perf_lwlock_tranche_id);
        OpcodeLatencyBreakdownLWLockInit(&g_FalconPerfLatencyShmem->opendirOp, falcon_perf_lwlock_tranche_id);

        /* Initialize locks for KV Operations (per-opcode breakdown) */
        OpcodeLatencyBreakdownLWLockInit(&g_FalconPerfLatencyShmem->kvPutOp, falcon_perf_lwlock_tranche_id);
        OpcodeLatencyBreakdownLWLockInit(&g_FalconPerfLatencyShmem->kvGetOp, falcon_perf_lwlock_tranche_id);
        OpcodeLatencyBreakdownLWLockInit(&g_FalconPerfLatencyShmem->kvDelOp, falcon_perf_lwlock_tranche_id);

        /* Initialize locks for Slice Operations */
        LWLockInitialize(&g_FalconPerfLatencyShmem->slicePutLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->sliceGetLatency.lock, falcon_perf_lwlock_tranche_id);

        /* Initialize locks for Legacy global counters */
        LWLockInitialize(&g_FalconPerfLatencyShmem->pathVerifyLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->pathParseLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->tableOpenLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->indexScanLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->tableInsertLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->tableUpdateLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->tableDeleteLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->commitLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->dirInsertLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->dirSearchLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->dirDeleteLatency.lock, falcon_perf_lwlock_tranche_id);
        LWLockInitialize(&g_FalconPerfLatencyShmem->remoteCallLatency.lock, falcon_perf_lwlock_tranche_id);
    }
}

/*
 * Report latency to shared memory (PostgreSQL backend only - uses LWLock)
 */
void ReportLatencyToShmem(LatencyData *data, uint64 latencyNs)
{
    if (data == NULL)
        return;
    if (g_FalconPerfLatencyShmem == NULL || !g_FalconPerfLatencyShmem->enabled)
        return;

    LWLockAcquire(&data->lock, LW_EXCLUSIVE);
    data->sum_ns += latencyNs;
    data->count++;
    if (latencyNs < data->min_ns)
        data->min_ns = latencyNs;
    if (latencyNs > data->max_ns)
        data->max_ns = latencyNs;
    LWLockRelease(&data->lock);
}

/*
 * Report latency to shared memory using atomic operations.
 * Safe to call from non-PostgreSQL threads (C++ std::thread).
 *
 * Uses "best-effort" min/max tracking: only attempts CAS once without retry.
 * This avoids contention while still tracking approximate min/max:
 * - If CAS fails, another thread likely set a better value
 * - Similar extreme values will eventually succeed
 * - After warmup, min/max stabilize and rarely need updates
 */
__attribute__((visibility("default")))
void ReportLatencyToShmemAtomic(LatencyData *data, uint64 latencyNs)
{
    if (data == NULL)
        return;
    if (g_FalconPerfLatencyShmem == NULL || !g_FalconPerfLatencyShmem->enabled)
        return;

    /* Atomic add for sum and count - contention-free */
    __atomic_add_fetch(&data->sum_ns, latencyNs, __ATOMIC_RELAXED);
    __atomic_add_fetch(&data->count, 1, __ATOMIC_RELAXED);

    /* Best-effort min update: check first, then single CAS attempt (no loop) */
    uint64 old_min = __atomic_load_n(&data->min_ns, __ATOMIC_RELAXED);
    if (latencyNs < old_min) {
        __atomic_compare_exchange_n(&data->min_ns, &old_min, latencyNs, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
        /* Don't retry if failed - another thread set a value, which is fine */
    }

    /* Best-effort max update: check first, then single CAS attempt (no loop) */
    uint64 old_max = __atomic_load_n(&data->max_ns, __ATOMIC_RELAXED);
    if (latencyNs > old_max) {
        __atomic_compare_exchange_n(&data->max_ns, &old_max, latencyNs, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
        /* Don't retry if failed - another thread set a value, which is fine */
    }
}

/*
 * Getter functions for LatencyData pointers
 * These allow C++ code to access specific latency counters without
 * including PostgreSQL headers (which conflict with glog/brpc).
 */
LatencyData *GetTotalRequestLatencyData(void)
{
    return g_FalconPerfLatencyShmem ? &g_FalconPerfLatencyShmem->totalRequestLatency : NULL;
}

LatencyData *GetQueueWaitLatencyData(void)
{
    return g_FalconPerfLatencyShmem ? &g_FalconPerfLatencyShmem->queueWaitLatency : NULL;
}

LatencyData *GetShmemCopyLatencyData(void)
{
    return g_FalconPerfLatencyShmem ? &g_FalconPerfLatencyShmem->shmemCopyLatency : NULL;
}

LatencyData *GetPgExecTotalLatencyData(void)
{
    return g_FalconPerfLatencyShmem ? &g_FalconPerfLatencyShmem->pgExecTotalLatency : NULL;
}

LatencyData *GetResultProcLatencyData(void)
{
    return g_FalconPerfLatencyShmem ? &g_FalconPerfLatencyShmem->resultProcLatency : NULL;
}

LatencyData *GetEnqueueDelayLatencyData(void)
{
    return g_FalconPerfLatencyShmem ? &g_FalconPerfLatencyShmem->enqueueDelayLatency : NULL;
}

LatencyData *GetInQueueLatencyData(void)
{
    return g_FalconPerfLatencyShmem ? &g_FalconPerfLatencyShmem->inQueueLatency : NULL;
}

LatencyData *GetConnWaitLatencyData(void)
{
    return g_FalconPerfLatencyShmem ? &g_FalconPerfLatencyShmem->connWaitLatency : NULL;
}

LatencyData *GetWorkerWaitLatencyData(void)
{
    return g_FalconPerfLatencyShmem ? &g_FalconPerfLatencyShmem->workerWaitLatency : NULL;
}

/* Single (non-batch) path getters */
LatencyData *GetShmemCopySingleLatencyData(void)
{
    return g_FalconPerfLatencyShmem ? &g_FalconPerfLatencyShmem->shmemCopySingleLatency : NULL;
}

LatencyData *GetPgExecSingleLatencyData(void)
{
    return g_FalconPerfLatencyShmem ? &g_FalconPerfLatencyShmem->pgExecSingleLatency : NULL;
}

LatencyData *GetResultProcSingleLatencyData(void)
{
    return g_FalconPerfLatencyShmem ? &g_FalconPerfLatencyShmem->resultProcSingleLatency : NULL;
}

/* Serialization layer getters */
LatencyData *GetParamDecodeLatencyData(void)
{
    return g_FalconPerfLatencyShmem ? &g_FalconPerfLatencyShmem->paramDecodeLatency : NULL;
}

LatencyData *GetResponseEncodeLatencyData(void)
{
    return g_FalconPerfLatencyShmem ? &g_FalconPerfLatencyShmem->responseEncodeLatency : NULL;
}

LatencyData *GetShmemAllocLatencyData(void)
{
    return g_FalconPerfLatencyShmem ? &g_FalconPerfLatencyShmem->shmemAllocLatency : NULL;
}

__attribute__((visibility("default")))
LatencyData *GetOpcodeE2ELatencyData(int opcode)
{
    if (g_FalconPerfLatencyShmem == NULL)
        return NULL;

    switch ((FalconMetaServiceType)opcode) {
        case CREATE:
            return &g_FalconPerfLatencyShmem->createOp.e2eLatency;
        case MKDIR:
        case MKDIR_SUB_MKDIR:
        case MKDIR_SUB_CREATE:
            return &g_FalconPerfLatencyShmem->mkdirOp.e2eLatency;
        case STAT:
            return &g_FalconPerfLatencyShmem->statOp.e2eLatency;
        case OPEN:
            return &g_FalconPerfLatencyShmem->openOp.e2eLatency;
        case CLOSE:
            return &g_FalconPerfLatencyShmem->closeOp.e2eLatency;
        case UNLINK:
            return &g_FalconPerfLatencyShmem->unlinkOp.e2eLatency;
        case RMDIR:
        case RMDIR_SUB_RMDIR:
        case RMDIR_SUB_UNLINK:
            return &g_FalconPerfLatencyShmem->rmdirOp.e2eLatency;
        case RENAME:
        case RENAME_SUB_RENAME_LOCALLY:
        case RENAME_SUB_CREATE:
            return &g_FalconPerfLatencyShmem->renameOp.e2eLatency;
        case READDIR:
            return &g_FalconPerfLatencyShmem->readdirOp.e2eLatency;
        case OPENDIR:
            return &g_FalconPerfLatencyShmem->opendirOp.e2eLatency;
        case KV_PUT:
            return &g_FalconPerfLatencyShmem->kvPutOp.e2eLatency;
        case KV_GET:
            return &g_FalconPerfLatencyShmem->kvGetOp.e2eLatency;
        case KV_DEL:
            return &g_FalconPerfLatencyShmem->kvDelOp.e2eLatency;
        default:
            return NULL;
    }
}
