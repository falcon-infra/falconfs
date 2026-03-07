/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "perf_counter/falcon_per_request_stat.h"
#include "postgres.h"
#include "storage/shmem.h"
#include "utils/elog.h"

/* Global shared memory pointer */
FalconPerRequestStatShmem *g_FalconPerRequestStatShmem = NULL;

/*
 * Checkpoint name registration table.
 *
 * Common checkpoint names (indices 0-7) are identical for all opcodes.
 * Per-opcode handler checkpoints start at index CKPT_HANDLER_START (8).
 * Tail checkpoints (respEncode, shmemAlloc, pqResult, resultProc) are appended
 * after the last per-opcode checkpoint.
 * NULL terminates the list for each opcode.
 *
 * NOTE: The tail checkpoint positions vary per opcode because different opcodes
 * have different numbers of handler checkpoints.  The aggregation code uses
 * checkpointCount from each RequestStat to determine the actual extent.
 */

/* Common prefix names (shared by all opcodes, indices 0-7) */
#define COMMON_PREFIX \
    "dispatch",      /* 0: brpc received */ \
    "enqueue",       /* 1: job enqueued */ \
    "dequeue",       /* 2: job dequeued */ \
    "connAcquired",  /* 3: PG connection obtained */ \
    "shmemCopy",     /* 4: data copied to shmem */ \
    "pqSend",        /* 5: PQsendQuery called */ \
    "pgEntry",       /* 6: PG function entry */ \
    "paramDecode"    /* 7: param deserialization done */

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
     "pathVerify", "tableOpen", "indexOpen", "pathParse",
     "dirInsert", "indexClose", "tableClose", "remoteCall",
     COMMON_TAIL, NULL},

    /* MKDIR_SUB_MKDIR (2) */
    {COMMON_PREFIX,
     "pathVerify", "tableOpen", "pathParse", "dirInsert",
     "tableClose",
     COMMON_TAIL, NULL},

    /* MKDIR_SUB_CREATE (3) */
    {COMMON_PREFIX,
     "tableOpen", "indexOpen", "tupleInsert",
     "indexClose", "tableClose", "commit",
     COMMON_TAIL, NULL},

    /* CREATE (4) */
    {COMMON_PREFIX,
     "pathVerify", "tableOpen", "pathParse", "tableClose",
     "inodeTableOpen", "indexOpen", "createInsert",
     "indexClose", "inodeTableClose", "commit",
     COMMON_TAIL, NULL},

    /* STAT (5) */
    {COMMON_PREFIX,
     "pathVerify", "tableOpen", "pathParse", "tableClose",
     "inodeTableOpen", "indexScan", "inodeTableClose",
     COMMON_TAIL, NULL},

    /* OPEN (6) */
    {COMMON_PREFIX,
     "pathVerify", "tableOpen", "pathParse", "tableClose",
     "inodeTableOpen", "indexScan", "inodeTableClose",
     COMMON_TAIL, NULL},

    /* CLOSE (7) */
    {COMMON_PREFIX,
     "pathVerify", "tableOpen", "pathParse", "tableClose",
     "inodeTableOpen", "closeModify", "inodeTableClose",
     COMMON_TAIL, NULL},

    /* UNLINK (8) */
    {COMMON_PREFIX,
     "pathVerify", "tableOpen", "pathParse", "tableClose",
     "inodeTableOpen", "unlinkModify", "inodeTableClose",
     COMMON_TAIL, NULL},

    /* READDIR (9) */
    {COMMON_PREFIX,
     "tableOpen", "indexScan", "tableClose",
     COMMON_TAIL, NULL},

    /* OPENDIR (10) */
    {COMMON_PREFIX,
     "pathVerify", "tableOpen", "pathParse", "tableClose",
     COMMON_TAIL, NULL},

    /* RMDIR (11) */
    {COMMON_PREFIX,
     "pathVerify", "tableOpen", "pathParse",
     "dirDelete", "tableClose", "remoteCall",
     COMMON_TAIL, NULL},

    /* RMDIR_SUB_RMDIR (12) */
    {COMMON_PREFIX,
     "pathVerify", "tableOpen", "pathParse",
     "dirDelete", "tableClose",
     COMMON_TAIL, NULL},

    /* RMDIR_SUB_UNLINK (13) */
    {COMMON_PREFIX,
     "inodeTableOpen", "indexScan",
     "tupleDelete", "inodeTableClose",
     COMMON_TAIL, NULL},

    /* RENAME (14) */
    {COMMON_PREFIX,
     "pathVerify", "srcTableOpen", "srcPathParse", "srcTableClose",
     "dstTableOpen", "dstPathParse", "dstTableClose",
     "inodeTableOpen", "renameLookup", "renameModify",
     "inodeTableClose", "remoteCall",
     COMMON_TAIL, NULL},

    /* RENAME_SUB_RENAME_LOCALLY (15) */
    {COMMON_PREFIX,
     "pathVerify", "srcTableOpen", "srcPathParse", "srcTableClose",
     "dstTableOpen", "dstPathParse", "dstTableClose",
     "inodeTableOpen", "renameLookup", "renameModify",
     "inodeTableClose",
     COMMON_TAIL, NULL},

    /* RENAME_SUB_CREATE (16) */
    {COMMON_PREFIX,
     "inodeTableOpen", "indexOpen", "tupleInsert",
     "indexClose", "inodeTableClose", "commit",
     COMMON_TAIL, NULL},

    /* UTIMENS (17) */
    {COMMON_PREFIX,
     "pathVerify", "tableOpen", "pathParse", "tableClose",
     "inodeTableOpen", "utimeModify", "inodeTableClose",
     COMMON_TAIL, NULL},

    /* CHOWN (18) */
    {COMMON_PREFIX,
     "pathVerify", "tableOpen", "pathParse", "tableClose",
     "inodeTableOpen", "chownModify", "inodeTableClose",
     COMMON_TAIL, NULL},

    /* CHMOD (19) */
    {COMMON_PREFIX,
     "pathVerify", "tableOpen", "pathParse", "tableClose",
     "inodeTableOpen", "chmodModify", "inodeTableClose",
     COMMON_TAIL, NULL},

    /* KV_PUT (20) */
    {COMMON_PREFIX,
     "shardLookup", "kvTableOpen", "indexOpen",
     "arrayBuild", "tupleInsert",
     "indexClose", "kvTableClose",
     COMMON_TAIL, NULL},

    /* KV_GET (21) */
    {COMMON_PREFIX,
     "shardLookup", "kvTableOpen",
     "indexScan", "arrayExtract",
     "kvTableClose",
     COMMON_TAIL, NULL},

    /* KV_DEL (22) */
    {COMMON_PREFIX,
     "shardLookup", "kvTableOpen",
     "indexScan", "tupleDelete",
     "kvTableClose",
     COMMON_TAIL, NULL},

    /* SLICE_PUT (23) */
    {COMMON_PREFIX,
     "shardLookup", "sliceTableOpen", "indexOpen",
     "tupleInsert", "indexClose", "sliceTableClose",
     COMMON_TAIL, NULL},

    /* SLICE_GET (24) */
    {COMMON_PREFIX,
     "shardLookup", "sliceTableOpen",
     "indexScan", "sliceTableClose",
     COMMON_TAIL, NULL},

    /* SLICE_DEL (25) */
    {COMMON_PREFIX,
     "shardLookup", "sliceTableOpen",
     "indexScan", "tupleDelete", "sliceTableClose",
     COMMON_TAIL, NULL},

    /* FETCH_SLICE_ID (26) */
    {COMMON_PREFIX,
     "rangeLookup", "idGenerate",
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
        g_FalconPerRequestStatShmem->enabled = true;
        g_FalconPerRequestStatShmem->nextIndex = 0;
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

    /* Reset slot for reuse */
    rs->completed = false;
    rs->inUse = true;
    rs->opcode = 0;
    rs->checkpointCount = 0;
    memset(rs->timestamps, 0, sizeof(rs->timestamps));

    return slot;
}

/*
 * Mark a stat slot as completed and ready for aggregation.
 */
void PerRequestStatComplete(int32_t index, int32_t opcode)
{
    if (index < 0 || g_FalconPerRequestStatShmem == NULL)
        return;

    RequestStat *rs = &g_FalconPerRequestStatShmem->statArray[index];
    rs->opcode = opcode;
    /* Memory barrier to ensure opcode/timestamps are visible before completed flag */
    __atomic_thread_fence(__ATOMIC_RELEASE);
    rs->completed = true;
}

/*
 * Aggregate completed per-request stats and output them.
 * Called periodically by the perf output worker (every 60s).
 *
 * Algorithm:
 * 1. Scan all slots, collect completed ones into per-opcode buckets
 * 2. For each opcode with data, compute gap statistics (min/max/avg/sum)
 * 3. Output in the agreed format
 * 4. Reset collected slots
 */
void PerRequestStatAggregateAndOutput(void)
{
    if (g_FalconPerRequestStatShmem == NULL || !g_FalconPerRequestStatShmem->enabled)
        return;

    /* Per-opcode aggregation accumulators */
    OpcodeGapStats opcodeStats[NOT_SUPPORTED];
    memset(opcodeStats, 0, sizeof(opcodeStats));

    /* Initialize min values */
    for (int op = 0; op < NOT_SUPPORTED; op++) {
        for (int g = 0; g < STAT_MAX_CHECKPOINTS - 1; g++) {
            opcodeStats[op].gaps[g].min_ns = UINT64_MAX;
        }
    }

    /* Phase 1: Scan and collect */
    int totalCollected = 0;
    for (int i = 0; i < STAT_ARRAY_SIZE; i++) {
        RequestStat *rs = &g_FalconPerRequestStatShmem->statArray[i];

        if (!rs->completed || !rs->inUse)
            continue;

        /* Memory barrier to ensure we see all writes */
        __atomic_thread_fence(__ATOMIC_ACQUIRE);

        int32_t opcode = rs->opcode;
        int ckptCount = rs->checkpointCount;

        if (opcode <= 0 || opcode >= NOT_SUPPORTED || ckptCount < 2) {
            /* Invalid or insufficient data, just reset */
            rs->inUse = false;
            rs->completed = false;
            continue;
        }

        OpcodeGapStats *os = &opcodeStats[opcode];
        os->requestCount++;
        if (ckptCount > os->maxCheckpointCount)
            os->maxCheckpointCount = ckptCount;

        /* Compute e2e (first to last checkpoint) */
        int64_t t_first = rs->timestamps[0];
        int64_t t_last = rs->timestamps[ckptCount - 1];
        if (t_first > 0 && t_last > t_first)
            os->e2eSumNs += (uint64_t)(t_last - t_first);

        /* Compute per-gap stats */
        for (int g = 0; g < ckptCount - 1; g++) {
            int64_t t0 = rs->timestamps[g];
            int64_t t1 = rs->timestamps[g + 1];

            if (t0 <= 0 || t1 <= 0)
                continue;

            /* Handle out-of-order (should be rare with CLOCK_MONOTONIC) */
            uint64_t gap_ns = (t1 > t0) ? (uint64_t)(t1 - t0) : 0;

            GapStat *gs = &os->gaps[g];
            gs->sum_ns += gap_ns;
            gs->count++;
            if (gap_ns < gs->min_ns)
                gs->min_ns = gap_ns;
            if (gap_ns > gs->max_ns)
                gs->max_ns = gap_ns;
        }

        /* Reset slot */
        rs->inUse = false;
        rs->completed = false;
        totalCollected++;
    }

    if (totalCollected == 0)
        return;

    /* Phase 2: Output */
    ereport(LOG, (errmsg("========== Falcon Per-Request Perf (60s) ==========")));

    for (int op = 1; op < NOT_SUPPORTED; op++) {
        OpcodeGapStats *os = &opcodeStats[op];
        if (os->requestCount == 0)
            continue;

        /* Get opcode name */
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

        /* e2e header line */
        uint64_t e2eAvgUs = 0;
        if (os->requestCount > 0 && os->e2eSumNs > 0)
            e2eAvgUs = os->e2eSumNs / os->requestCount / 1000;
        uint64_t e2eSumUs = os->e2eSumNs / 1000;

        ereport(LOG, (errmsg("[%s] e2e: avg/sum=%llu/%lluus cnt=%llu",
                             opName,
                             (unsigned long long)e2eAvgUs,
                             (unsigned long long)e2eSumUs,
                             (unsigned long long)os->requestCount)));

        /* Per-gap detail lines */
        const char **names = g_checkpointNames[op];
        for (int g = 0; g < os->maxCheckpointCount - 1; g++) {
            GapStat *gs = &os->gaps[g];
            if (gs->count == 0)
                continue;

            /* Get gap name: name is the label of the END checkpoint of this gap */
            const char *gapName = NULL;
            if (names != NULL && names[g + 1] != NULL)
                gapName = names[g + 1];

            char nameBuf[64];
            if (gapName == NULL) {
                snprintf(nameBuf, sizeof(nameBuf), "ckpt%d", g + 1);
                gapName = nameBuf;
            }

            uint64_t avgUs = gs->sum_ns / gs->count / 1000;
            uint64_t minUs = (gs->min_ns == UINT64_MAX) ? 0 : gs->min_ns / 1000;
            uint64_t maxUs = gs->max_ns / 1000;
            uint64_t sumUs = gs->sum_ns / 1000;
            double pct = (os->e2eSumNs > 0) ? (gs->sum_ns * 100.0 / os->e2eSumNs) : 0.0;

            if (gs->count != os->requestCount) {
                /* Count differs from header — show [actual/total] */
                ereport(LOG, (errmsg("    %-20s avg/min/max/sum=%llu/%llu/%llu/%lluus cnt=%llu (%.1f%%) [%llu/%llu]",
                                     gapName,
                                     (unsigned long long)avgUs,
                                     (unsigned long long)minUs,
                                     (unsigned long long)maxUs,
                                     (unsigned long long)sumUs,
                                     (unsigned long long)gs->count,
                                     pct,
                                     (unsigned long long)gs->count,
                                     (unsigned long long)os->requestCount)));
            } else {
                ereport(LOG, (errmsg("    %-20s avg/min/max/sum=%llu/%llu/%llu/%lluus cnt=%llu (%.1f%%)",
                                     gapName,
                                     (unsigned long long)avgUs,
                                     (unsigned long long)minUs,
                                     (unsigned long long)maxUs,
                                     (unsigned long long)sumUs,
                                     (unsigned long long)gs->count,
                                     pct)));
            }
        }
    }
}
