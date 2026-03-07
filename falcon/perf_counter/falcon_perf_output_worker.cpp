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
#include "perf_counter/falcon_per_request_stat.h"
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

extern "C" __attribute__((visibility("default"))) void FalconPerfOutputWorkerMain(Datum main_arg)
{
    /* Signal handling setup */
    pqsignal(SIGTERM, falcon_perf_output_sigterm);
    BackgroundWorkerUnblockSignals();

    /* Check shared memory */
    if (g_FalconPerRequestStatShmem == NULL) {
        ereport(LOG, (errmsg("Falcon per-request stat shared memory not initialized, skipping")));
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

        PerRequestStatAggregateAndOutput();
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
