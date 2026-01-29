/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_PERF_LATENCY_SHMEM_H
#define FALCON_PERF_LATENCY_SHMEM_H

#include "postgres.h"
#include "storage/lwlock.h"

#ifdef __cplusplus
extern "C" {
#endif

/* POD structure for latency statistics in shared memory */
typedef struct LatencyData
{
    LWLock lock;
    uint64 sum_ns; /* Total latency in nanoseconds */
    uint64 count;  /* Number of samples */
    uint64 min_ns; /* Minimum latency */
    uint64 max_ns; /* Maximum latency */
} LatencyData;

/*
 * Per-opcode latency breakdown structure.
 * Each file operation has its own set of sub-metrics for analysis:
 *   total = pathVerify + tableOpen + pathParse + indexScan + tableModify + remoteCall + other
 *
 * Not all fields are used by every opcode. Unused fields remain zero.
 */
typedef struct OpcodeLatencyBreakdown
{
    LatencyData total;       /* Total opcode latency (server-side execution time) */
    LatencyData e2eLatency;  /* End-to-end latency (from queue entry to response sent) */
    LatencyData pathVerify;  /* Path verification */
    LatencyData tableOpen;   /* Open table/relation */
    LatencyData pathParse;   /* Path parsing (directory table lookup/insert) */
    LatencyData indexScan;   /* Index scan for data lookup */
    LatencyData tableModify; /* Table insert/update/delete */
    LatencyData remoteCall;  /* Remote call to DN */
    LatencyData tableClose; /* Close table/relation */
    LatencyData indexOpen;  /* CatalogOpenIndexes() */
    LatencyData indexClose; /* CatalogCloseIndexes() */
    LatencyData commit;     /* Transaction commit (opcode-level) */
} OpcodeLatencyBreakdown;

/* Shared memory structure for latency statistics */
typedef struct FalconPerfLatencyShmem
{
    bool enabled; /* Performance monitoring enabled */

    /* ===== Connection Pool Layer (pg_connection.cpp) ===== */
    LatencyData queueWaitLatency;    /* Total queue wait time */
    LatencyData enqueueDelayLatency; /* Enqueue delay */
    LatencyData inQueueLatency;      /* Time in queue */
    LatencyData connWaitLatency;     /* Wait for connection */
    LatencyData workerWaitLatency;   /* Wait for worker */
    LatencyData totalRequestLatency; /* End-to-end request latency */

    /* ----- Batch path ----- */
    LatencyData shmemCopyLatency;   /* Shared memory copy (batch) */
    LatencyData pgExecTotalLatency; /* PG execution total (batch) */
    LatencyData resultProcLatency;  /* Result processing (batch) */

    /* ----- Single (non-batch) path ----- */
    LatencyData shmemCopySingleLatency;  /* Shared memory copy (single) */
    LatencyData pgExecSingleLatency;     /* PG execution (single) */
    LatencyData resultProcSingleLatency; /* Result processing (single) */

    /* ===== Serialization Layer (meta_serialize_interface.c) ===== */
    LatencyData paramDecodeLatency;    /* Parameter deserialization */
    LatencyData responseEncodeLatency; /* Response serialization */
    LatencyData shmemAllocLatency;     /* Shared memory allocation for response */

    /* ===== File Semantic Operations with per-opcode breakdown ===== */
    /*
     * Each opcode has breakdown: total = pathVerify + tableOpen + pathParse + indexScan + tableModify + remoteCall
     */
    OpcodeLatencyBreakdown createOp;  /* CREATE file */
    OpcodeLatencyBreakdown mkdirOp;   /* MKDIR */
    OpcodeLatencyBreakdown statOp;    /* STAT */
    OpcodeLatencyBreakdown openOp;    /* OPEN */
    OpcodeLatencyBreakdown closeOp;   /* CLOSE */
    OpcodeLatencyBreakdown unlinkOp;  /* UNLINK */
    OpcodeLatencyBreakdown rmdirOp;   /* RMDIR */
    OpcodeLatencyBreakdown renameOp;  /* RENAME */
    OpcodeLatencyBreakdown readdirOp; /* READDIR */
    OpcodeLatencyBreakdown opendirOp; /* OPENDIR */

    /* ===== KV Operations with breakdown ===== */
    /*
     * KV PUT: total = tableOpen + tableModify (insert)
     * KV GET: total = tableOpen + indexScan
     * KV DEL: total = tableModify (delete)
     */
    OpcodeLatencyBreakdown kvPutOp; /* KV PUT */
    OpcodeLatencyBreakdown kvGetOp; /* KV GET */
    OpcodeLatencyBreakdown kvDelOp; /* KV DEL */

    /* ===== Slice Operations ===== */
    LatencyData slicePutLatency; /* Slice PUT */
    LatencyData sliceGetLatency; /* Slice GET */

    /* ===== Legacy global counters (kept for backward compatibility) ===== */
    LatencyData pathVerifyLatency;  /* Path verification (global) */
    LatencyData pathParseLatency;   /* Path parsing (global) */
    LatencyData tableOpenLatency;   /* Open table (global) */
    LatencyData indexScanLatency;   /* Index scan (global) */
    LatencyData tableInsertLatency; /* Table insert (global) */
    LatencyData tableUpdateLatency; /* Table update (global) */
    LatencyData tableDeleteLatency; /* Table delete (global) */
    LatencyData commitLatency;      /* Transaction commit */
    LatencyData dirInsertLatency;   /* Directory table insert */
    LatencyData dirSearchLatency;   /* Directory table search */
    LatencyData dirDeleteLatency;   /* Directory table delete */
    LatencyData remoteCallLatency;  /* Remote call (global) */

} FalconPerfLatencyShmem;

/* Global shared memory pointer */
extern FalconPerfLatencyShmem *g_FalconPerfLatencyShmem;

/* GUC variable for enabling/disabling performance monitoring */
extern bool falcon_perf_enabled;

/* Shared memory functions */
extern Size FalconPerfLatencyShmemSize(void);
extern void FalconPerfLatencyShmemInit(void);

/* Background worker registration (defined in falcon_perf_output_worker.cpp) */
extern void RegisterFalconPerfOutputWorker(void);

/* Initialize a single LatencyData structure */
static inline void LatencyDataInit(LatencyData *data)
{
    data->sum_ns = 0;
    data->count = 0;
    data->min_ns = UINT64_MAX;
    data->max_ns = 0;
}

/* Initialize an OpcodeLatencyBreakdown structure */
static inline void OpcodeLatencyBreakdownInit(OpcodeLatencyBreakdown *op)
{
    LatencyDataInit(&op->total);
    LatencyDataInit(&op->e2eLatency);
    LatencyDataInit(&op->pathVerify);
    LatencyDataInit(&op->tableOpen);
    LatencyDataInit(&op->pathParse);
    LatencyDataInit(&op->indexScan);
    LatencyDataInit(&op->tableModify);
    LatencyDataInit(&op->remoteCall);
    LatencyDataInit(&op->tableClose);
    LatencyDataInit(&op->indexOpen);
    LatencyDataInit(&op->indexClose);
    LatencyDataInit(&op->commit);
}

/* Report latency to shared memory (PostgreSQL backend only - uses LWLock) */
extern void ReportLatencyToShmem(LatencyData *data, uint64 latencyNs);

/* Report latency using atomic operations (safe for C++ std::thread) */
extern void ReportLatencyToShmemAtomic(LatencyData *data, uint64 latencyNs);

/* Getter functions for LatencyData pointers (to avoid exposing struct layout) */
extern LatencyData *GetTotalRequestLatencyData(void);
extern LatencyData *GetQueueWaitLatencyData(void);
extern LatencyData *GetEnqueueDelayLatencyData(void);
extern LatencyData *GetInQueueLatencyData(void);
extern LatencyData *GetConnWaitLatencyData(void);
extern LatencyData *GetWorkerWaitLatencyData(void);
/* Batch path */
extern LatencyData *GetShmemCopyLatencyData(void);
extern LatencyData *GetPgExecTotalLatencyData(void);
extern LatencyData *GetResultProcLatencyData(void);
/* Single (non-batch) path */
extern LatencyData *GetShmemCopySingleLatencyData(void);
extern LatencyData *GetPgExecSingleLatencyData(void);
extern LatencyData *GetResultProcSingleLatencyData(void);
/* Serialization layer */
extern LatencyData *GetParamDecodeLatencyData(void);
extern LatencyData *GetResponseEncodeLatencyData(void);
extern LatencyData *GetShmemAllocLatencyData(void);

extern LatencyData *GetOpcodeE2ELatencyData(int opcode);

#ifdef __cplusplus
}
#endif

#endif /* FALCON_PERF_LATENCY_SHMEM_H */
