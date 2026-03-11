/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_PER_REQUEST_STAT_H
#define FALCON_PER_REQUEST_STAT_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include "utils/falcon_meta_service_def.h"

#define STAT_ARRAY_SIZE 10240
#define STAT_MAX_CHECKPOINTS 64
#define STAT_MAX_CHECKPOINT_NAME_LEN 32

/* Per-request stat entry in shared memory */
typedef struct RequestStat
{
    volatile bool completed; /* true when request finishes, ready for aggregation */
    volatile bool inUse;     /* true when slot is allocated */
    int32_t opcode;          /* FalconMetaServiceType */
    int32_t checkpointCount; /* Number of checkpoints written so far */
    int64_t timestamps[STAT_MAX_CHECKPOINTS]; /* CLOCK_MONOTONIC timestamps in ns */
} RequestStat;

/* Per-gap atomic accumulator (in shmem, updated at request completion) */
typedef struct GapAccum
{
    volatile int64_t sum_ns;
    volatile int64_t count;
    volatile int64_t min_ns;
    volatile int64_t max_ns;
} GapAccum;

/* Per-opcode atomic accumulator (in shmem) */
typedef struct OpcodeAccum
{
    volatile int64_t requestCount;
    volatile int64_t e2eSumNs;
    volatile int64_t maxCheckpointCount;
    GapAccum gaps[STAT_MAX_CHECKPOINTS - 1];
} OpcodeAccum;

/* Shared memory structure for per-request stats */
typedef struct FalconPerRequestStatShmem
{
    bool enabled;
    volatile int64_t nextIndex;        /* Atomic counter for slot allocation */
    volatile int64_t allocDropCount;   /* Requests dropped because no free slot */
    volatile int64_t statIndicesAllocDropCount; /* stat-indices shmem alloc failures (PG-side trace lost) */
    OpcodeAccum accum[NOT_SUPPORTED];
    RequestStat statArray[STAT_ARRAY_SIZE];
} FalconPerRequestStatShmem;

/*
 * Common connection pool checkpoint indices (same for all opcodes).
 * These are the first checkpoints written by the connection pool side.
 */
#define CKPT_DISPATCH       0 /* Job dispatched (brpc received) */
#define CKPT_ENQUEUE        1 /* Job enqueued to ConcurrentQueue */
#define CKPT_DEQUEUE        2 /* Job dequeued from queue */
#define CKPT_CONN_ACQUIRED  3 /* PG connection obtained */
#define CKPT_SHMEM_COPY     4 /* Data copied to shared memory */
#define CKPT_PQ_SEND        5 /* PQsendQuery called */

/*
 * Common PG envelope checkpoint indices.
 * Written by meta_serialize_interface.c around the handler call.
 */
#define CKPT_PG_ENTRY       6 /* PG function entry */
#define CKPT_PARAM_DECODE   7 /* Parameter deserialization done */

/* Per-opcode handler checkpoints start at this index */
#define CKPT_HANDLER_START  8

/*
 * Common PG tail checkpoints.
 * These are written AFTER the handler returns, using the handler's final
 * checkpoint index + 1, + 2.  The actual positions vary per opcode.
 * Use CKPT_TAIL_RESP_ENCODE(ci) etc. where ci is the current checkpoint counter.
 */

/*
 * Common connection pool tail checkpoints.
 * Written after PQgetResult returns.  Positions = handler checkpoints + PG tail + offset.
 */

/* Global shmem pointer */
extern FalconPerRequestStatShmem *g_FalconPerRequestStatShmem;

/* GUC: falcon.perf_enabled (PGC_POSTMASTER) */
extern bool FalconPerfEnabled;

/*
 * Checkpoint name registration table.
 * g_checkpointNames[opcode][i] = name for checkpoint i.
 * Common checkpoints (0-7) are the same for all opcodes.
 * Per-opcode checkpoints (8+) are defined per opcode.
 * Tail checkpoints are appended after per-opcode ones.
 * NULL terminates the list.
 */
extern const char *g_checkpointNames[NOT_SUPPORTED][STAT_MAX_CHECKPOINTS];

#ifdef __cplusplus
extern "C" {
#endif

/* Shmem lifecycle functions (called from falcon_init.c) */
size_t FalconPerRequestStatShmemSize(void);
void FalconPerRequestStatShmemInit(void);

/* Background worker registration (called from falcon_init.c) */
void RegisterFalconPerfOutputWorker(void);

/* Stat array slot management */
int32_t PerRequestStatAllocIndex(void);
void PerRequestStatComplete(int32_t index, int32_t opcode);

/* Aggregation and output (called from output worker) */
void PerRequestStatAggregateAndOutput(void);

#ifdef __cplusplus
}
#endif

static inline void atomic_min_i64(volatile int64_t *target, int64_t value)
{
    int64_t cur = __atomic_load_n(target, __ATOMIC_RELAXED);
    while (value < cur) {
        if (__atomic_compare_exchange_n(target, &cur, value, true,
                                        __ATOMIC_RELAXED, __ATOMIC_RELAXED))
            break;
    }
}

static inline void atomic_max_i64(volatile int64_t *target, int64_t value)
{
    int64_t cur = __atomic_load_n(target, __ATOMIC_RELAXED);
    while (value > cur) {
        if (__atomic_compare_exchange_n(target, &cur, value, true,
                                        __ATOMIC_RELAXED, __ATOMIC_RELAXED))
            break;
    }
}

/*
 * Inline timestamp write — this is the hot-path function.
 * Called from both C (PG backend) and C++ (connection pool).
 * Uses CLOCK_MONOTONIC for cross-thread/process consistency.
 */
static inline void StatCheckpoint(int32_t statIndex, int checkpointIdx)
{
    if (statIndex < 0 || statIndex >= STAT_ARRAY_SIZE ||
        g_FalconPerRequestStatShmem == NULL ||
        !g_FalconPerRequestStatShmem->enabled)
        return;
    if (checkpointIdx < 0 || checkpointIdx >= STAT_MAX_CHECKPOINTS)
        return;

    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    RequestStat *rs = &g_FalconPerRequestStatShmem->statArray[statIndex];
    rs->timestamps[checkpointIdx] = (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;

    /* Update checkpoint count (only increasing) */
    if (checkpointIdx >= rs->checkpointCount)
        rs->checkpointCount = checkpointIdx + 1;
}

/*
 * Broadcast a single timestamp to multiple stat slots.
 * Used for batch-level operations (tableOpen, tableClose, etc.)
 * that are shared across all jobs in a batch.
 */
static inline void StatCheckpointBroadcast(const int32_t *statIndices, int count,
                                           int checkpointIdx)
{
    if (g_FalconPerRequestStatShmem == NULL || !g_FalconPerRequestStatShmem->enabled)
        return;
    if (checkpointIdx < 0 || checkpointIdx >= STAT_MAX_CHECKPOINTS)
        return;

    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    int64_t now = (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;

    for (int i = 0; i < count; i++) {
        if (statIndices[i] < 0 || statIndices[i] >= STAT_ARRAY_SIZE)
            continue;
        RequestStat *rs = &g_FalconPerRequestStatShmem->statArray[statIndices[i]];
        rs->timestamps[checkpointIdx] = now;
        if (checkpointIdx >= rs->checkpointCount)
            rs->checkpointCount = checkpointIdx + 1;
    }
}

/* Convenience macro */
#define STAT_CKPT(idx, ckpt) StatCheckpoint((idx), (ckpt))

#endif /* FALCON_PER_REQUEST_STAT_H */
