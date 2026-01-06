/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "utils/elog.h"
#include "perf_counter/falcon_perf_latency_shmem.h"
}

/* Signal handling */
static volatile sig_atomic_t got_sigterm = false;

static void falcon_perf_output_sigterm(SIGNAL_ARGS)
{
    int save_errno = errno;
    got_sigterm = true;
    SetLatch(MyLatch);
    errno = save_errno;
}

/* Output a single LatencyData field and reset it. Returns true if data was output. */
static bool OutputAndResetLatencyData(const char* name, LatencyData* data)
{
    if (data == NULL)
        return false;

    LWLockAcquire(&data->lock, LW_EXCLUSIVE);

    if (data->count == 0) {
        LWLockRelease(&data->lock);
        return false;
    }

    /* Read values */
    uint64 sumUs = data->sum_ns / 1000;
    uint64 count = data->count;
    double avgUs = (double)data->sum_ns / data->count / 1000.0;
    uint64 minNs = data->min_ns;
    uint64 maxNs = data->max_ns;

    /* Reset for next interval */
    data->sum_ns = 0;
    data->count = 0;
    data->min_ns = UINT64_MAX;
    data->max_ns = 0;

    LWLockRelease(&data->lock);

    /* Check if min/max were updated */
    if (minNs == UINT64_MAX && maxNs == 0) {
        /* No min/max data (very rare edge case) */
        ereport(LOG, (errmsg("        %s: avg/sum=%.0f/%lluus cnt=%llu",
                             name, avgUs,
                             (unsigned long long)sumUs,
                             (unsigned long long)count)));
    } else {
        /* Normal output with min/max/sum */
        uint64 minUs = (minNs == UINT64_MAX) ? 0 : minNs / 1000;
        uint64 maxUs = maxNs / 1000;
        ereport(LOG, (errmsg("        %s: avg/min/max/sum=%.0f/%llu/%llu/%lluus cnt=%llu",
                             name, avgUs,
                             (unsigned long long)minUs,
                             (unsigned long long)maxUs,
                             (unsigned long long)sumUs,
                             (unsigned long long)count)));
    }
    return true;
}

/* Check if LatencyData has data (without locking, for quick check) */
static bool HasLatencyData(LatencyData* data)
{
    return data != NULL && data->count > 0;
}

/* Check if OpcodeLatencyBreakdown has any data */
static bool HasOpcodeData(OpcodeLatencyBreakdown* op)
{
    return op != NULL && (op->total.count > 0 || op->e2eLatency.count > 0);
}

/* Read and reset a single LatencyData, returning sum_ns and count */
static void ReadAndResetLatencyData(LatencyData* data, uint64* out_sum_ns, uint64* out_count)
{
    *out_sum_ns = 0;
    *out_count = 0;
    if (data == NULL || data->count == 0)
        return;

    LWLockAcquire(&data->lock, LW_EXCLUSIVE);
    *out_sum_ns = data->sum_ns;
    *out_count = data->count;
    data->sum_ns = 0;
    data->count = 0;
    data->min_ns = UINT64_MAX;
    data->max_ns = 0;
    LWLockRelease(&data->lock);
}

/* Helper structure to hold sub-metric data for output */
typedef struct SubMetricData {
    uint64 sum_ns;
    uint64 count;
    uint64 min_ns;
    uint64 max_ns;
} SubMetricData;

/* Read sub-metric with full stats (sum, count, min, max) */
static void ReadAndResetLatencyDataFull(LatencyData* data, SubMetricData* out)
{
    out->sum_ns = 0;
    out->count = 0;
    out->min_ns = UINT64_MAX;
    out->max_ns = 0;
    if (data == NULL || data->count == 0)
        return;

    LWLockAcquire(&data->lock, LW_EXCLUSIVE);
    out->sum_ns = data->sum_ns;
    out->count = data->count;
    out->min_ns = data->min_ns;
    out->max_ns = data->max_ns;
    data->sum_ns = 0;
    data->count = 0;
    data->min_ns = UINT64_MAX;
    data->max_ns = 0;
    LWLockRelease(&data->lock);
}

/* Output a sub-metric with percentage */
static void OutputSubMetricWithPercent(const char* name, SubMetricData* data, uint64 totalSumNs)
{
    if (data->count == 0)
        return;

    double avgUs = (double)data->sum_ns / data->count / 1000.0;
    uint64 minUs = (data->min_ns == UINT64_MAX) ? 0 : data->min_ns / 1000;
    uint64 maxUs = data->max_ns / 1000;
    uint64 sumUs = data->sum_ns / 1000;
    int pct = (totalSumNs > 0) ? (int)(data->sum_ns * 100 / totalSumNs) : 0;

    ereport(LOG, (errmsg("        %s: avg/min/max/sum=%.0f/%llu/%llu/%lluus cnt=%llu (%d%%)",
                         name, avgUs,
                         (unsigned long long)minUs,
                         (unsigned long long)maxUs,
                         (unsigned long long)sumUs,
                         (unsigned long long)data->count,
                         pct)));
}

/*
 * Output opcode latency breakdown with percentage analysis.
 * Format:
 *   [OPCODE] e2e: avg/min/max/sum=2000/1800/2500/200000us cnt=100
 *       pgExec: avg/min/max/sum=1500/1200/2000/150000us cnt=100
 *       pathVerify: avg/min/max/sum=50/30/80/5000us cnt=100 (3%)
 *       tableOpen: avg/min/max/sum=100/80/150/10000us cnt=100 (7%)
 *       ...
 */
static void OutputOpcodeBreakdown(const char* opName, OpcodeLatencyBreakdown* op)
{
    if (op == NULL)
        return;

    SubMetricData e2eData;
    ReadAndResetLatencyDataFull(&op->e2eLatency, &e2eData);

    SubMetricData pgExecData;
    ReadAndResetLatencyDataFull(&op->total, &pgExecData);

    if (e2eData.count == 0 && pgExecData.count == 0) {
        return;
    }

    /* Read sub-metrics with full stats */
    SubMetricData pathVerify, tableOpen, pathParse, indexScan, tableModify, remoteCall;
    ReadAndResetLatencyDataFull(&op->pathVerify, &pathVerify);
    ReadAndResetLatencyDataFull(&op->tableOpen, &tableOpen);
    ReadAndResetLatencyDataFull(&op->pathParse, &pathParse);
    ReadAndResetLatencyDataFull(&op->indexScan, &indexScan);
    ReadAndResetLatencyDataFull(&op->tableModify, &tableModify);
    ReadAndResetLatencyDataFull(&op->remoteCall, &remoteCall);

    /* Calculate other (based on pgExec) */
    uint64 subTotalNs = pathVerify.sum_ns + tableOpen.sum_ns + pathParse.sum_ns +
                        indexScan.sum_ns + tableModify.sum_ns + remoteCall.sum_ns;
    uint64 otherNs = (pgExecData.sum_ns > subTotalNs) ? (pgExecData.sum_ns - subTotalNs) : 0;
    int otherPct = (pgExecData.sum_ns > 0) ? (int)(otherNs * 100 / pgExecData.sum_ns) : 0;

    /* Output e2e line first (end-to-end from queue entry to response sent) */
    if (e2eData.count > 0) {
        double e2eAvgUs = (double)e2eData.sum_ns / e2eData.count / 1000.0;
        uint64 e2eMinUs = (e2eData.min_ns == UINT64_MAX) ? 0 : e2eData.min_ns / 1000;
        uint64 e2eMaxUs = e2eData.max_ns / 1000;
        uint64 e2eSumUs = e2eData.sum_ns / 1000;
        ereport(LOG, (errmsg("    [%s] e2e: avg/min/max/sum=%.0f/%llu/%llu/%lluus cnt=%llu",
                             opName, e2eAvgUs,
                             (unsigned long long)e2eMinUs,
                             (unsigned long long)e2eMaxUs,
                             (unsigned long long)e2eSumUs,
                             (unsigned long long)e2eData.count)));
    }

    /* Output pgExec (PG backend execution time) */
    if (pgExecData.count > 0) {
        double pgExecAvgUs = (double)pgExecData.sum_ns / pgExecData.count / 1000.0;
        uint64 pgExecMinUs = (pgExecData.min_ns == UINT64_MAX) ? 0 : pgExecData.min_ns / 1000;
        uint64 pgExecMaxUs = pgExecData.max_ns / 1000;
        uint64 pgExecSumUs = pgExecData.sum_ns / 1000;
        ereport(LOG, (errmsg("        pgExec: avg/min/max/sum=%.0f/%llu/%llu/%lluus cnt=%llu",
                             pgExecAvgUs,
                             (unsigned long long)pgExecMinUs,
                             (unsigned long long)pgExecMaxUs,
                             (unsigned long long)pgExecSumUs,
                             (unsigned long long)pgExecData.count)));

        /* Output each sub-metric on its own line */
        OutputSubMetricWithPercent("pathVerify", &pathVerify, pgExecData.sum_ns);
        OutputSubMetricWithPercent("tableOpen", &tableOpen, pgExecData.sum_ns);
        OutputSubMetricWithPercent("pathParse", &pathParse, pgExecData.sum_ns);
        OutputSubMetricWithPercent("indexScan", &indexScan, pgExecData.sum_ns);
        OutputSubMetricWithPercent("tableModify", &tableModify, pgExecData.sum_ns);
        OutputSubMetricWithPercent("remoteCall", &remoteCall, pgExecData.sum_ns);

        /* Output other if significant */
        if (otherNs > 0 && otherPct > 0) {
            double otherAvgUs = (double)otherNs / pgExecData.count / 1000.0;
            uint64 otherSumUs = otherNs / 1000;
            ereport(LOG, (errmsg("        other: avg/sum=%.0f/%lluus cnt=%llu (%d%%)",
                                 otherAvgUs,
                                 (unsigned long long)otherSumUs,
                                 (unsigned long long)pgExecData.count,
                                 otherPct)));
        }
    }
}

/* Output all performance statistics and reset counters */
static void OutputAllStats(void)
{
    FalconPerfLatencyShmem* perf = g_FalconPerfLatencyShmem;
    if (perf == NULL)
        return;

    /* Skip output if performance monitoring is disabled */
    if (!perf->enabled)
        return;

    /* Quick check if there's any data to output */
    bool hasData = HasLatencyData(&perf->queueWaitLatency) ||
                   HasLatencyData(&perf->totalRequestLatency) ||
                   HasLatencyData(&perf->paramDecodeLatency) ||
                   HasLatencyData(&perf->responseEncodeLatency) ||
                   HasLatencyData(&perf->shmemAllocLatency) ||
                   HasOpcodeData(&perf->createOp) ||
                   HasOpcodeData(&perf->mkdirOp) ||
                   HasOpcodeData(&perf->statOp) ||
                   HasOpcodeData(&perf->openOp) ||
                   HasOpcodeData(&perf->closeOp) ||
                   HasOpcodeData(&perf->unlinkOp) ||
                   HasOpcodeData(&perf->rmdirOp) ||
                   HasOpcodeData(&perf->renameOp) ||
                   HasOpcodeData(&perf->readdirOp) ||
                   HasOpcodeData(&perf->opendirOp) ||
                   HasOpcodeData(&perf->kvPutOp) ||
                   HasOpcodeData(&perf->kvGetOp) ||
                   HasOpcodeData(&perf->kvDelOp) ||
                   HasLatencyData(&perf->slicePutLatency) ||
                   HasLatencyData(&perf->sliceGetLatency);

    if (!hasData)
        return;

    /* Print header */
    ereport(LOG, (errmsg("========== Falcon Perf (interval) ==========")));

    /* Connection Pool Layer */
    bool hasConnPoolData = HasLatencyData(&perf->queueWaitLatency) ||
                           HasLatencyData(&perf->enqueueDelayLatency) ||
                           HasLatencyData(&perf->inQueueLatency) ||
                           HasLatencyData(&perf->connWaitLatency) ||
                           HasLatencyData(&perf->workerWaitLatency) ||
                           HasLatencyData(&perf->totalRequestLatency) ||
                           HasLatencyData(&perf->shmemCopyLatency) ||
                           HasLatencyData(&perf->pgExecTotalLatency) ||
                           HasLatencyData(&perf->resultProcLatency) ||
                           HasLatencyData(&perf->shmemCopySingleLatency) ||
                           HasLatencyData(&perf->pgExecSingleLatency) ||
                           HasLatencyData(&perf->resultProcSingleLatency);
    if (hasConnPoolData) {
        ereport(LOG, (errmsg("  [Connection Pool]")));
        OutputAndResetLatencyData("queueWait",       &perf->queueWaitLatency);
        OutputAndResetLatencyData("enqueueDelay",    &perf->enqueueDelayLatency);
        OutputAndResetLatencyData("inQueue",         &perf->inQueueLatency);
        OutputAndResetLatencyData("connWait",        &perf->connWaitLatency);
        OutputAndResetLatencyData("workerWait",      &perf->workerWaitLatency);
        OutputAndResetLatencyData("totalRequest",    &perf->totalRequestLatency);
        OutputAndResetLatencyData("shmemCopyBatch",  &perf->shmemCopyLatency);
        OutputAndResetLatencyData("pgExecBatch",     &perf->pgExecTotalLatency);
        OutputAndResetLatencyData("resultProcBatch", &perf->resultProcLatency);
        OutputAndResetLatencyData("shmemCopySingle", &perf->shmemCopySingleLatency);
        OutputAndResetLatencyData("pgExecSingle",    &perf->pgExecSingleLatency);
        OutputAndResetLatencyData("resultProcSingle",&perf->resultProcSingleLatency);
    }

    /* Serialization Layer */
    bool hasSerializeData = HasLatencyData(&perf->paramDecodeLatency) ||
                            HasLatencyData(&perf->responseEncodeLatency) ||
                            HasLatencyData(&perf->shmemAllocLatency);
    if (hasSerializeData) {
        ereport(LOG, (errmsg("  [Serialization]")));
        OutputAndResetLatencyData("paramDecode",     &perf->paramDecodeLatency);
        OutputAndResetLatencyData("responseEncode",  &perf->responseEncodeLatency);
        OutputAndResetLatencyData("shmemAlloc",      &perf->shmemAllocLatency);
    }

    /* File Semantic Operations with breakdown */
    OutputOpcodeBreakdown("CREATE",  &perf->createOp);
    OutputOpcodeBreakdown("MKDIR",   &perf->mkdirOp);
    OutputOpcodeBreakdown("STAT",    &perf->statOp);
    OutputOpcodeBreakdown("OPEN",    &perf->openOp);
    OutputOpcodeBreakdown("CLOSE",   &perf->closeOp);
    OutputOpcodeBreakdown("UNLINK",  &perf->unlinkOp);
    OutputOpcodeBreakdown("RMDIR",   &perf->rmdirOp);
    OutputOpcodeBreakdown("RENAME",  &perf->renameOp);
    OutputOpcodeBreakdown("READDIR", &perf->readdirOp);
    OutputOpcodeBreakdown("OPENDIR", &perf->opendirOp);

    /* KV Operations with breakdown */
    OutputOpcodeBreakdown("KV_PUT", &perf->kvPutOp);
    OutputOpcodeBreakdown("KV_GET", &perf->kvGetOp);
    OutputOpcodeBreakdown("KV_DEL", &perf->kvDelOp);

    /* Slice Operations */
    OutputAndResetLatencyData("slicePut", &perf->slicePutLatency);
    OutputAndResetLatencyData("sliceGet", &perf->sliceGetLatency);
}

extern "C" __attribute__((visibility("default"))) void FalconPerfOutputWorkerMain(Datum main_arg)
{
    /* Signal handling setup */
    pqsignal(SIGTERM, falcon_perf_output_sigterm);
    BackgroundWorkerUnblockSignals();

    /* Check shared memory */
    if (g_FalconPerfLatencyShmem == NULL) {
        ereport(ERROR, (errmsg("Falcon perf shared memory not initialized")));
        proc_exit(1);
    }

    ereport(LOG, (errmsg("Falcon performance output worker started")));

    /* Main loop: output statistics every 60 seconds */
    int outputIntervalSec = 60;
    while (!got_sigterm) {
        int rc = WaitLatch(MyLatch,
                          WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
                          outputIntervalSec * 1000L,
                          PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);

        if (rc & WL_POSTMASTER_DEATH) {
            proc_exit(1);
        }

        if (got_sigterm) {
            break;
        }

        OutputAllStats();
    }

    ereport(LOG, (errmsg("Falcon performance output worker stopped")));
    proc_exit(0);
}

/* Register background worker */
extern "C" void RegisterFalconPerfOutputWorker(void)
{
    BackgroundWorker worker;

    memset(&worker, 0, sizeof(BackgroundWorker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
    worker.bgw_start_time = BgWorkerStart_PostmasterStart;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_notify_pid = 0;
    snprintf(worker.bgw_name, BGW_MAXLEN, "falcon_perf_output");
    snprintf(worker.bgw_type, BGW_MAXLEN, "falcon_perf_output");
    worker.bgw_main_arg = (Datum) 0;
    sprintf(worker.bgw_library_name, "falcon");
    sprintf(worker.bgw_function_name, "FalconPerfOutputWorkerMain");

    RegisterBackgroundWorker(&worker);
}
