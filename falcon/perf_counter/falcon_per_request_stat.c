/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "perf_counter/falcon_per_request_stat.h"
#include "postgres.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include <limits.h>

/* Global shared memory pointer */
FalconPerRequestStatShmem *g_FalconPerRequestStatShmem = NULL;

bool FalconPerfEnabled = true;

/*
 * Checkpoint name registration table.
 *
 * Common checkpoint names (indices 0-8) are identical for all opcodes.
 * Per-opcode handler checkpoints start at index CKPT_HANDLER_START (9).
 * Tail checkpoints (respEncode, shmemAlloc, pqResult, resultProc) are appended
 * after the last per-opcode checkpoint.
 * NULL terminates the list for each opcode.
 *
 * NOTE: The tail checkpoint positions vary per opcode because different opcodes
 * have different numbers of handler checkpoints.  The aggregation code uses
 * checkpointCount from each RequestStat to determine the actual extent.
 */

/* Common prefix names (shared by all opcodes, indices 0-8) */
#define COMMON_PREFIX \
    "start",         /* 0: stat slot allocated */ \
    "dispatch",      /* 1: brpc received */ \
    "enqueue",       /* 2: job enqueued */ \
    "dequeue",       /* 3: job dequeued */ \
    "connAcquired",  /* 4: PG connection obtained */ \
    "shmemCopy",     /* 5: data copied to shmem */ \
    "pqSend",        /* 6: PQsendQuery called */ \
    "pgEntry",       /* 7: PG function entry */ \
    "paramDecode"    /* 8: param deserialization done */

/* Common tail names (appended after per-opcode handler checkpoints) */
#define COMMON_TAIL \
    "respEncode",    /* handler done, response encoded */ \
    "shmemAlloc",    /* response shmem allocated */ \
    "pqResult",      /* PQgetResult returned */ \
    "resultProc"     /* response processed, job done */

const char *g_checkpointNames[NOT_SUPPORTED][STAT_MAX_CHECKPOINTS] = {
    /* PLAIN_COMMAND (0) - not instrumented */
    {NULL},

    /* MKDIR (1) */
    {COMMON_PREFIX,
     "handlerEntry", "pathVerify", "tableOpen", "indexOpen",
     "pathParse", "dirInsert", "indexClose", "remoteDone",
     COMMON_TAIL, NULL},

    /* MKDIR_SUB_MKDIR (2) */
    {COMMON_PREFIX,
     "handlerStart", "beforeOpen", "indexReady",
     "beforeInsert", "insertDone",
     "indexClose", "tableClose",
     COMMON_TAIL, NULL},

    /* MKDIR_SUB_CREATE (3) */
    {COMMON_PREFIX,
     "handlerStart", "beforeOpen", "shardGroup",
     "indexReady", "insertDone", "done",
     COMMON_TAIL, NULL},

    /* CREATE (4) */
    {COMMON_PREFIX,
     "handlerEntry", "pathVerify", "batchSetup", "pathParse",
     "dirTableClose", "subTxnBegin", "indexReady",
     "beforeInsert", "insertDone",
     "batchDone", "shardClose",
     COMMON_TAIL, NULL},

    /* STAT (5) */
    {COMMON_PREFIX,
     "handlerEntry", "pathVerify", "batchSetup", "pathParse",
     "tableClose", "shardOpen", "fetchDone",
     COMMON_TAIL, NULL},

    /* OPEN (6) */
    {COMMON_PREFIX,
     "handlerEntry", "pathVerify", "batchSetup", "pathParse",
     "tableClose", "shardOpen", "fetchDone",
     COMMON_TAIL, NULL},

    /* CLOSE (7) */
    {COMMON_PREFIX,
     "handlerEntry", "pathVerify", "batchSetup", "pathParse",
     "tableClose", "shardOpen", "closeModify",
     COMMON_TAIL, NULL},

    /* UNLINK (8) */
    {COMMON_PREFIX,
     "handlerEntry", "pathVerify", "batchSetup", "pathParse",
     "tableClose", "shardOpen", "unlinkModify",
     COMMON_TAIL, NULL},

    /* READDIR (9) */
    {COMMON_PREFIX,
     "handlerEntry", "pathVerify", "pathResolve", "shardScan", "resultBuild",
     COMMON_TAIL, NULL},

    /* OPENDIR (10) */
    {COMMON_PREFIX,
     "handlerEntry", "pathVerify", "tableOpen", "pathParse",
     COMMON_TAIL, NULL},

    /* RMDIR (11) */
    {COMMON_PREFIX,
     "handlerEntry", "pathVerify", "tableOpen",
     "pathParse", "dirDelete", "remoteDone",
     COMMON_TAIL, NULL},

    /* RMDIR_SUB_RMDIR (12) */
    {COMMON_PREFIX,
     "handlerEntry", "setup", "tableOpen",
     "dirSearch", "dirDelete", "emptyCheck",
     COMMON_TAIL, NULL},

    /* RMDIR_SUB_UNLINK (13) */
    {COMMON_PREFIX,
     "handlerEntry", "setup",
     "shardLookup", "inodeModify",
     COMMON_TAIL, NULL},

    /* RENAME (14) */
    {COMMON_PREFIX,
     "handlerEntry", "pathVerify", "cnCheck", "pathParse",
     "dirModify", "tableClose", "shardLookup",
     "sendRename", "remoteWait", "responseProc",
     "crossShardMove", "renameDone",
     COMMON_TAIL, NULL},

    /* RENAME_SUB_RENAME_LOCALLY (15) */
    {COMMON_PREFIX,
     "handlerEntry", "targetCheck", "dirTableOpen", "dirModify",
     "dirTableClose", "shardLookup", "srcInodeOpen",
     "srcTupleDelete", "dstTupleInsert", "dstInodeClose",
     "renameDone",
     COMMON_TAIL, NULL},

    /* RENAME_SUB_CREATE (16) */
    {COMMON_PREFIX,
     "handlerEntry", "setup", "shardLookup",
     "tableOpen", "tupleInsert", "done",
     COMMON_TAIL, NULL},

    /* UTIMENS (17) */
    {COMMON_PREFIX,
     "handlerEntry", "pathVerify", "beforeOpen", "pathParse",
     "shardLookup", "utimeModify", "done",
     COMMON_TAIL, NULL},

    /* CHOWN (18) */
    {COMMON_PREFIX,
     "handlerEntry", "pathVerify", "beforeOpen", "pathParse",
     "shardLookup", "chownModify", "done",
     COMMON_TAIL, NULL},

    /* CHMOD (19) */
    {COMMON_PREFIX,
     "handlerEntry", "pathVerify", "beforeOpen", "pathParse",
     "shardLookup", "chmodModify", "done",
     COMMON_TAIL, NULL},

    /* KV_PUT (20) */
    {COMMON_PREFIX,
     "handlerEntry", "hashPartId", "shardResolve",
     "kvTableOpen", "arrayBuild", "tupleInsert",
     "done",
     COMMON_TAIL, NULL},

    /* KV_GET (21) */
    {COMMON_PREFIX,
     "handlerEntry", "hashPartId",
     "scanSetup", "indexScan", "arrayDecode", "done",
     COMMON_TAIL, NULL},

    /* KV_DEL (22) */
    {COMMON_PREFIX,
     "handlerEntry", "hashPartId",
     "scanSetup", "tupleDelete",
     "done",
     COMMON_TAIL, NULL},

    /* SLICE_PUT (23) */
    {COMMON_PREFIX,
     "handlerStart", "beforeShard", "shardResolve",
     "tableOpen", "insertLoop", "done",
     COMMON_TAIL, NULL},

    /* SLICE_GET (24) */
    {COMMON_PREFIX,
     "handlerStart", "beforeShard",
     "shardResolve", "scanDone", "done",
     COMMON_TAIL, NULL},

    /* SLICE_DEL (25) */
    {COMMON_PREFIX,
     "handlerStart", "beforeShard",
     "shardResolve", "deleteDone", "done",
     COMMON_TAIL, NULL},

    /* FETCH_SLICE_ID (26) */
    {COMMON_PREFIX,
     "handlerEntry", "scanReady", "done",
     COMMON_TAIL, NULL},
};

/*
 * Calculate shared memory size required for per-request stats.
 */
Size FalconPerRequestStatShmemSize(void)
{
    return MAXALIGN(sizeof(FalconPerRequestStatShmem));
}

/*
 * Initialize shared memory for per-request stats.
 */
void FalconPerRequestStatShmemInit(void)
{
    bool found;
    Size size = FalconPerRequestStatShmemSize();

    g_FalconPerRequestStatShmem =
        (FalconPerRequestStatShmem *)ShmemInitStruct("FalconPerRequestStat", size, &found);

    if (!found) {
        memset(g_FalconPerRequestStatShmem, 0, size);
        g_FalconPerRequestStatShmem->enabled = FalconPerfEnabled;
        g_FalconPerRequestStatShmem->nextIndex = 0;

        for (int op = 0; op < NOT_SUPPORTED; op++) {
            for (int g = 0; g < STAT_MAX_CHECKPOINTS - 1; g++) {
                g_FalconPerRequestStatShmem->accum[op].gaps[g].min_ns = INT64_MAX;
            }
        }
    }
}

/*
 * Allocate a slot in the stat array.
 * Returns the slot index, or -1 if the array is full.
 * Uses atomic increment for lock-free allocation.
 */
int32_t PerRequestStatAllocIndex(void)
{
    if (g_FalconPerRequestStatShmem == NULL || !g_FalconPerRequestStatShmem->enabled)
        return -1;

    int64_t idx = __atomic_fetch_add(&g_FalconPerRequestStatShmem->nextIndex, 1, __ATOMIC_RELAXED);
    int32_t slot = (int32_t)(idx % STAT_ARRAY_SIZE);

    RequestStat *rs = &g_FalconPerRequestStatShmem->statArray[slot];

    if (rs->inUse) {
        __atomic_fetch_add(&g_FalconPerRequestStatShmem->allocDropCount, 1, __ATOMIC_RELAXED);
        return -1;
    }

    rs->completed = false;
    rs->inUse = true;
    rs->opcode = 0;
    rs->checkpointCount = 0;
    memset(rs->timestamps, 0, sizeof(rs->timestamps));
    StatCheckpoint(slot, CKPT_START);

    return slot;
}

void PerRequestStatComplete(int32_t index, int32_t opcode)
{
    if (index < 0 || g_FalconPerRequestStatShmem == NULL)
        return;
    if (opcode < 0 || opcode >= NOT_SUPPORTED)
        goto release;

    RequestStat *rs = &g_FalconPerRequestStatShmem->statArray[index];
    int ckptCount = rs->checkpointCount;
    if (ckptCount < 2)
        goto release;

    OpcodeAccum *accum = &g_FalconPerRequestStatShmem->accum[opcode];

    int64_t t_first = rs->timestamps[0];
    int64_t t_last = rs->timestamps[ckptCount - 1];
    if (t_first > 0 && t_last > t_first)
        __atomic_fetch_add(&accum->e2eSumNs, t_last - t_first, __ATOMIC_RELAXED);

    for (int g = 0; g < ckptCount - 1; g++) {
        int64_t t0 = rs->timestamps[g];
        int64_t t1 = rs->timestamps[g + 1];
        int64_t gap_ns = (t0 > 0 && t1 > t0) ? (t1 - t0) : 0;
        GapAccum *ga = &accum->gaps[g];
        __atomic_fetch_add(&ga->sum_ns, gap_ns, __ATOMIC_RELAXED);
        __atomic_fetch_add(&ga->count, 1, __ATOMIC_RELAXED);
        atomic_min_i64(&ga->min_ns, gap_ns);
        atomic_max_i64(&ga->max_ns, gap_ns);
    }

    atomic_max_i64(&accum->maxCheckpointCount, ckptCount);
    __atomic_fetch_add(&accum->requestCount, 1, __ATOMIC_RELAXED);

release:
    {
        RequestStat *rs2 = &g_FalconPerRequestStatShmem->statArray[index];
        rs2->inUse = false;
        rs2->completed = false;
    }
}

void PerRequestStatAggregateAndOutput(void)
{
    if (g_FalconPerRequestStatShmem == NULL || !g_FalconPerRequestStatShmem->enabled)
        return;

    int64_t dropped = __atomic_exchange_n(&g_FalconPerRequestStatShmem->allocDropCount,
                                          0, __ATOMIC_RELAXED);
    int64_t statIdxDropped = __atomic_exchange_n(&g_FalconPerRequestStatShmem->statIndicesAllocDropCount,
                                                  0, __ATOMIC_RELAXED);
    bool hasData = false;

    OpcodeAccum snapshot[NOT_SUPPORTED];
    for (int op = 0; op < NOT_SUPPORTED; op++) {
        OpcodeAccum *src = &g_FalconPerRequestStatShmem->accum[op];
        snapshot[op].requestCount = __atomic_exchange_n(&src->requestCount, 0, __ATOMIC_RELAXED);
        snapshot[op].e2eSumNs = __atomic_exchange_n(&src->e2eSumNs, 0, __ATOMIC_RELAXED);
        snapshot[op].maxCheckpointCount = __atomic_exchange_n(&src->maxCheckpointCount, 0, __ATOMIC_RELAXED);

        for (int g = 0; g < STAT_MAX_CHECKPOINTS - 1; g++) {
            GapAccum *ga = &src->gaps[g];
            snapshot[op].gaps[g].sum_ns = __atomic_exchange_n(&ga->sum_ns, 0, __ATOMIC_RELAXED);
            snapshot[op].gaps[g].count = __atomic_exchange_n(&ga->count, 0, __ATOMIC_RELAXED);
            snapshot[op].gaps[g].min_ns = __atomic_exchange_n(&ga->min_ns, INT64_MAX, __ATOMIC_RELAXED);
            snapshot[op].gaps[g].max_ns = __atomic_exchange_n(&ga->max_ns, 0, __ATOMIC_RELAXED);
        }

        if (snapshot[op].requestCount > 0)
            hasData = true;
    }

    if (!hasData && dropped <= 0 && statIdxDropped <= 0)
        return;

    ereport(LOG, (errmsg("========== Falcon Per-Request Perf (60s) ==========")));

    if (dropped > 0) {
        ereport(WARNING, (errmsg("Falcon Perf: %lld samples dropped (no free slot in statArray[%d])",
                                 (long long)dropped, STAT_ARRAY_SIZE)));
    }

    if (statIdxDropped > 0) {
        ereport(WARNING, (errmsg("Falcon Perf: %lld stat-indices alloc failures (PG-side checkpoints lost)",
                                 (long long)statIdxDropped)));
    }

    for (int op = 1; op < NOT_SUPPORTED; op++) {
        OpcodeAccum *os = &snapshot[op];
        if (os->requestCount == 0)
            continue;

        const char *opName = "UNKNOWN";
        switch ((FalconMetaServiceType)op) {
            case MKDIR:                    opName = "MKDIR"; break;
            case MKDIR_SUB_MKDIR:          opName = "MKDIR_SUB_MKDIR"; break;
            case MKDIR_SUB_CREATE:         opName = "MKDIR_SUB_CREATE"; break;
            case CREATE:                   opName = "CREATE"; break;
            case STAT:                     opName = "STAT"; break;
            case OPEN:                     opName = "OPEN"; break;
            case CLOSE:                    opName = "CLOSE"; break;
            case UNLINK:                   opName = "UNLINK"; break;
            case READDIR:                  opName = "READDIR"; break;
            case OPENDIR:                  opName = "OPENDIR"; break;
            case RMDIR:                    opName = "RMDIR"; break;
            case RMDIR_SUB_RMDIR:          opName = "RMDIR_SUB_RMDIR"; break;
            case RMDIR_SUB_UNLINK:         opName = "RMDIR_SUB_UNLINK"; break;
            case RENAME:                   opName = "RENAME"; break;
            case RENAME_SUB_RENAME_LOCALLY: opName = "RENAME_SUB_RENAME"; break;
            case RENAME_SUB_CREATE:        opName = "RENAME_SUB_CREATE"; break;
            case UTIMENS:                  opName = "UTIMENS"; break;
            case CHOWN:                    opName = "CHOWN"; break;
            case CHMOD:                    opName = "CHMOD"; break;
            case KV_PUT:                   opName = "KV_PUT"; break;
            case KV_GET:                   opName = "KV_GET"; break;
            case KV_DEL:                   opName = "KV_DEL"; break;
            case SLICE_PUT:                opName = "SLICE_PUT"; break;
            case SLICE_GET:                opName = "SLICE_GET"; break;
            case SLICE_DEL:                opName = "SLICE_DEL"; break;
            case FETCH_SLICE_ID:           opName = "FETCH_SLICE_ID"; break;
            default:                       break;
        }

        double e2eAvgUs = (os->requestCount > 0 && os->e2eSumNs > 0)
                          ? (double)os->e2eSumNs / os->requestCount / 1000.0 : 0.0;
        double e2eSumUs = os->e2eSumNs / 1000.0;

        ereport(LOG, (errmsg("[%s] e2e: avg/sum=%.1f/%.1fus cnt=%llu",
                             opName, e2eAvgUs, e2eSumUs,
                             (unsigned long long)os->requestCount)));

        const char **names = g_checkpointNames[op];
        int maxCkpt = (int)os->maxCheckpointCount;
        for (int g = 0; g < maxCkpt - 1; g++) {
            GapAccum *ga = &os->gaps[g];
            if (ga->count == 0)
                continue;

            /*
             * Gap g measures timestamps[g + 1] - timestamps[g], so its stage
             * name must come from checkpoint g + 1 (the stage that just
             * completed).  Using names[g] shifts every printed stage by one and
             * makes tail labels such as shmemAlloc/pqResult incorrect.
             */
            const char *gapName = NULL;
            if (names != NULL && names[g + 1] != NULL)
                gapName = names[g + 1];

            char nameBuf[64];
            if (gapName == NULL) {
                snprintf(nameBuf, sizeof(nameBuf), "ckpt%d", g + 1);
                gapName = nameBuf;
            }

            double avgUs = (double)ga->sum_ns / ga->count / 1000.0;
            double minUs = (ga->min_ns == INT64_MAX) ? 0.0 : ga->min_ns / 1000.0;
            double maxUs = ga->max_ns / 1000.0;
            double sumUs = ga->sum_ns / 1000.0;
            double pct = (os->e2eSumNs > 0) ? (ga->sum_ns * 100.0 / os->e2eSumNs) : 0.0;

            if (ga->count != os->requestCount) {
                ereport(LOG, (errmsg("    %-20s avg/min/max/sum=%.1f/%.1f/%.1f/%.1fus cnt=%lld (%.1f%%) [%lld/%lld]",
                                     gapName,
                                     avgUs, minUs, maxUs, sumUs,
                                     (long long)ga->count,
                                     pct,
                                     (long long)ga->count,
                                     (long long)os->requestCount)));
            } else {
                ereport(LOG, (errmsg("    %-20s avg/min/max/sum=%.1f/%.1f/%.1f/%.1fus cnt=%lld (%.1f%%)",
                                     gapName,
                                     avgUs, minUs, maxUs, sumUs,
                                     (long long)ga->count,
                                     pct)));
            }
        }
    }
}
