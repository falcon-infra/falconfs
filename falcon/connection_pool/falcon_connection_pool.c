/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "connection_pool/falcon_connection_pool.h"

#include <dlfcn.h>
#include <signal.h>
#include <threads.h>
#include <unistd.h>
#include "postgres.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "storage/shmem.h"
#include "utils/error_log.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/utils.h"

#include "access/xact.h"
#include "access/xlog.h"
#include "base_comm_adapter/comm_server_interface.h"
#include "brpc_comm_adapter/falcon_brpc_server.h"
#include "connection_pool/pg_connection_pool.h"
#include "control/control_flag.h"

int FalconPGPort = 0;
int FalconConnectionPoolPort = FALCON_CONNECTION_POOL_PORT_DEFAULT;
int FalconConnectionPoolSize = FALCON_CONNECTION_POOL_SIZE_DEFAULT;
int FalconConnectionPoolBatchSize = FALCON_CONNECTION_POOL_BATCH_SIZE_DEFAULT;
int FalconConnectionPoolWaitAdjust = FALCON_CONNECTION_POOL_WAIT_ADJUST_DEFAULT;
int FalconConnectionPoolWaitMin = FALCON_CONNECTION_POOL_WAIT_MIN_DEFAULT;
int FalconConnectionPoolWaitMax = FALCON_CONNECTION_POOL_WAIT_MAX_DEFAULT;
uint64_t FalconConnectionPoolShmemSize = FALCON_CONNECTION_POOL_SHMEM_SIZE_DEFAULT;

// communication plugin path, using global variable for shared to worker process
char *FalconCommunicationPluginPath;
// communication server IP, using global variable for shared to worker process
char *FalconCommunicationServerIp;

// variable used for falcon communication plugin
static falcon_plugin_start_comm_func_t comm_work_func = NULL;
static falcon_plugin_stop_comm_func_t comm_cleanup_func = NULL;
static void *falcon_comm_dl_handle = NULL;

static volatile bool got_SIGTERM = false;
static void FalconDaemonConnectionPoolProcessSigTermHandler(SIGNAL_ARGS);

void FalconDaemonConnectionPoolProcessMain(unsigned long int main_arg)
{
    pqsignal(SIGTERM, FalconDaemonConnectionPoolProcessSigTermHandler);
    BackgroundWorkerUnblockSignals();
    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    CurrentResourceOwner = ResourceOwnerCreate(NULL, "falcon connection pool");
    CurrentMemoryContext = AllocSetContextCreate(TopMemoryContext,
                                                 "falcon connection pool context",
                                                 ALLOCSET_DEFAULT_MINSIZE,
                                                 ALLOCSET_DEFAULT_INITSIZE,
                                                 ALLOCSET_DEFAULT_MAXSIZE);
    elog(LOG, "FalconDaemonConnectionPoolProcessMain: pid = %d", getpid());
    elog(LOG, "FalconDaemonConnectionPoolProcessMain: wait init.");
    bool falconHasBeenLoad = false;
    while (true) {
        StartTransactionCommand();
        falconHasBeenLoad = CheckFalconHasBeenLoaded();
        CommitTransactionCommand();
        if (falconHasBeenLoad) {
            break;
        }
        sleep(1);
    }
    do {
        sleep(1);
    } while (RecoveryInProgress());
    elog(LOG, "FalconDaemonConnectionPoolProcessMain: init finished.");

    // PostPortNumber need using both here and falcon_run_pooler_server_func, so set to global variable FalconPGPort
    FalconPGPort = PostPortNumber;
    RunConnectionPoolServer();
    elog(LOG, "FalconDaemonConnectionPoolProcessMain: connection pool server stopped.");
    return;
}

static void FalconDaemonConnectionPoolProcessSigTermHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    elog(LOG, "FalconDaemonConnectionPoolProcessSigTermHandler: get sigterm.");
    got_SIGTERM = true;

    DestroyPGConnectionPool();
    if (comm_cleanup_func != NULL) {
        comm_cleanup_func();
        comm_cleanup_func = NULL;
    }

    if (falcon_comm_dl_handle != NULL) {
        dlclose(falcon_comm_dl_handle);
        comm_work_func = NULL;
        falcon_comm_dl_handle = NULL;
    }

    errno = save_errno;
}

size_t FalconConnectionPoolShmemsize(void) { return FalconConnectionPoolShmemSize; }

void FalconConnectionPoolShmemInit(void)
{
    bool initialized;
    char *falconConnectionPoolShmemBuffer =
        ShmemInitStruct("Falcon Connection Pool Shmem", FalconConnectionPoolShmemsize(), &initialized);

    FalconShmemAllocator *allocator = GetFalconConnectionPoolShmemAllocator();
    if (FalconShmemAllocatorInit(allocator, falconConnectionPoolShmemBuffer, FalconConnectionPoolShmemsize()) != 0) {
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "FalconShmemAllocatorInit failed.");
    }

    if (!initialized) {
        memset(allocator->signatureCounter,
               0,
               sizeof(PaddedAtomic64) * (1 + FALCON_SHMEM_ALLOCATOR_FREE_LIST_COUNT + allocator->pageCount));
    }
}

static void StartCommunicationSever()
{
    /* Load communication plugin */
    if (FalconCommunicationPluginPath == NULL) {
        elog(
            ERROR,
            "Communication plugin not provide, please check the falcon_communication.plugin_path in postgresql.conf. ");
        return;
    }
    elog(LOG, "Using plugin %s start CommunicationSever.", FalconCommunicationPluginPath);

    falcon_comm_dl_handle = dlopen(FalconCommunicationPluginPath, RTLD_LAZY);
    if (!falcon_comm_dl_handle) {
        elog(ERROR,
             "Failed to load plugin in background worker: %s, error: %s",
             FalconCommunicationPluginPath,
             dlerror());
        return;
    }

    comm_work_func = (falcon_plugin_start_comm_func_t)dlsym(falcon_comm_dl_handle, FALCON_PLUGIN_START_COMM_FUNC_NAME);
    comm_cleanup_func = (falcon_plugin_stop_comm_func_t)dlsym(falcon_comm_dl_handle, FALCON_PLUGIN_STOP_COMM_FUNC_NAME);
    if (!comm_work_func || !comm_cleanup_func) {
        elog(ERROR, "Plugin %s missing required functions (work/cleanup)", FalconCommunicationPluginPath);
        dlclose(falcon_comm_dl_handle);
        return;
    }

    /* Execute plugin work */
    int ret =
        comm_work_func(FalconDispatchMetaJob2PGConnectionPool, FalconCommunicationServerIp, FalconConnectionPoolPort);
    if (ret != 0) {
        elog(ERROR, "Plugin work function returned %d: %s", ret, FalconCommunicationPluginPath);
        dlclose(falcon_comm_dl_handle);
        return;
    }
    /* Cleanup */
    elog(LOG, "Background worker stopping: %s", FalconCommunicationPluginPath);
    comm_cleanup_func();

    dlclose(falcon_comm_dl_handle);
}

void RunConnectionPoolServer(void)
{
    // start PGConnectionPool wait for dispatch jobs
    bool ret = StartPGConnectionPool();
    if (!ret) {
        elog(ERROR, "RunConnectionPoolServer: StartPGConnectionPool failed.");
    }

    // start Communication Server receive jobs and dispatch jobs to PGConnectionPool
    StartCommunicationSever();
}
