/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/lwlocknames.h"
#include "storage/shmem.h"
#include "tcop/utility.h"

#include "connection_pool/connection_pool.h"
#include "control/control_flag.h"
#include "control/hook.h"
#include "dir_path_shmem/dir_path_hash.h"
#include "metadb/foreign_server.h"
#include "metadb/metadata.h"
#include "metadb/shard_table.h"
#include "transaction/transaction.h"
#include "transaction/transaction_cleanup.h"
#include "utils/guc.h"
#include "utils/path_parse.h"
#include "utils/rwlock.h"
#include "utils/shmem_control.h"
#include "utils/falcon_plugin_guc.h"
#include "plugin/falcon_plugin_loader.h"

PG_MODULE_MAGIC;

/* Plugin system GUC variables */
char *falcon_plugin_directory = NULL;
char *falcon_plugin_custom_config = NULL;

/* Plugin system shared memory */
static FalconPluginSharedData *falcon_plugin_shmem = NULL;

void _PG_init(void);
static void FalconStart2PCCleanupWorker(void);
static void FalconStartConnectionPoolWorker(void);
static void InitializeFalconShmemStruct(void);
static void RegisterFalconConfigVariables(void);

void _PG_init(void)
{
    RegisterFalconConfigVariables();
    InitializeFalconShmemStruct();
    RegisterFalconTransactionCallback();
    ForeignServerCacheInit();
    PathParseMemoryContextInit();

    pre_ExecutorStart_hook = ExecutorStart_hook;
    ExecutorStart_hook = falcon_ExecutorStart;
    pre_ProcessUtility_hook = ProcessUtility_hook;
    ProcessUtility_hook = falcon_ProcessUtility;
    pre_object_access_hook = object_access_hook;
    object_access_hook = falcon_object_access;

    FalconStart2PCCleanupWorker();
    FalconStartConnectionPoolWorker();

    /* Load third-party plugins */
    if (falcon_plugin_directory) {
        FalconLoadPluginsFromDirectory(falcon_plugin_directory);
    }
}

/*
 * Start 2PC cleanup process.
 */
static void FalconStart2PCCleanupWorker(void)
{
    BackgroundWorker worker;
    BackgroundWorkerHandle *handle;
    BgwHandleStatus status;
    pid_t pid;

    MemSet(&worker, 0, sizeof(BackgroundWorker));
    strcpy(worker.bgw_name, "falcon_2pc_cleanup_process");
    strcpy(worker.bgw_type, "falcon_daemon_2pc_cleanup_process");
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 1;
    strcpy(worker.bgw_library_name, "falcon");
    strcpy(worker.bgw_function_name, "FalconDaemon2PCFailureCleanupProcessMain");

    if (process_shared_preload_libraries_in_progress) {
        RegisterBackgroundWorker(&worker);
        return;
    }

    /* must set notify PID to wait for startup */
    worker.bgw_notify_pid = MyProcPid;

    if (!RegisterDynamicBackgroundWorker(&worker, &handle))
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("could not register falcon background process"),
                 errhint("You may need to increase max_worker_processes.")));

    status = WaitForBackgroundWorkerStartup(handle, &pid);
    if (status != BGWH_STARTED)
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("could not start falcon background process"),
                 errhint("More detials may be available in the server log.")));
}

/*
 * Start falcon connection pool process.
 */
static void FalconStartConnectionPoolWorker(void)
{
    BackgroundWorker worker;
    BackgroundWorkerHandle *handle;
    BgwHandleStatus status;
    pid_t pid;

    MemSet(&worker, 0, sizeof(BackgroundWorker));
    strcpy(worker.bgw_name, "falcon_connection_pool_process");
    strcpy(worker.bgw_type, "falcon_daemon_connection_pool_process");
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = 1;
    strcpy(worker.bgw_library_name, "falcon");
    strcpy(worker.bgw_function_name, "FalconDaemonConnectionPoolProcessMain");

    if (process_shared_preload_libraries_in_progress) {
        RegisterBackgroundWorker(&worker);
        return;
    }

    /* must set notify PID to wait for startup */
    worker.bgw_notify_pid = MyProcPid;

    if (!RegisterDynamicBackgroundWorker(&worker, &handle))
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("could not register falcon background process"),
                 errhint("You may need to increase max_worker_processes.")));

    status = WaitForBackgroundWorkerStartup(handle, &pid);
    if (status != BGWH_STARTED)
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                 errmsg("could not start falcon background process"),
                 errhint("More detials may be available in the server log.")));
}

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static void FalconShmemRequest(void);
static void FalconShmemInit(void);
static void FalconShmemRequest(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

    RequestAddinShmemSpace(FalconControlShmemsize());
    RequestAddinShmemSpace(TransactionCleanupShmemsize());
    RequestAddinShmemSpace(ForeignServerShmemsize());
    RequestAddinShmemSpace(ShardTableShmemsize());
    RequestAddinShmemSpace(DirPathShmemsize());
    RequestAddinShmemSpace(FalconConnectionPoolShmemsize());
    RequestAddinShmemSpace(sizeof(FalconPluginSharedData));
}
static void FalconShmemInit(void)
{
    if (prev_shmem_startup_hook != NULL) {
        prev_shmem_startup_hook();
    }

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    FalconControlShmemInit();
    TransactionCleanupShmemInit();
    ForeignServerShmemInit();
    ShardTableShmemInit();
    DirPathShmemInit();
    FalconConnectionPoolShmemInit();

    /* Initialize plugin shared memory */
    bool found;
    falcon_plugin_shmem = ShmemInitStruct("FalconPluginData",
                                         sizeof(FalconPluginSharedData),
                                         &found);
    if (!found) {
        memset(falcon_plugin_shmem, 0, sizeof(FalconPluginSharedData));
        falcon_plugin_shmem->main_pid = getpid();
    }

    LWLockRelease(AddinShmemInitLock);
}
static void InitializeFalconShmemStruct(void)
{
    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = FalconShmemRequest;

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = FalconShmemInit;
}

/* Register Falcon configuration variables. */
static void RegisterFalconConfigVariables(void)
{
    DefineCustomIntVariable("falcon_connection_pool.port",
                            gettext_noop("Port of the pool manager."),
                            NULL,
                            &FalconConnectionPoolPort,
                            FALCON_CONNECTION_POOL_PORT_DEFAULT,
                            1,
                            65535,
                            PGC_POSTMASTER,
                            0,
                            NULL,
                            NULL,
                            NULL);

    DefineCustomIntVariable("falcon_connection_pool.pool_size",
                            gettext_noop("Pool size of the pool manager."),
                            NULL,
                            &FalconConnectionPoolSize,
                            FALCON_CONNECTION_POOL_SIZE_DEFAULT,
                            1,
                            256,
                            PGC_POSTMASTER,
                            0,
                            NULL,
                            NULL,
                            NULL);

    int FalconConnectionPoolShmemSizeInMB = FALCON_CONNECTION_POOL_SHMEM_SIZE_DEFAULT / 1024 / 1024;
    DefineCustomIntVariable(gettext_noop("falcon_connection_pool.shmem_size"),
                            "Shmem size of the pool manager, unit: MB.",
                            NULL,
                            &FalconConnectionPoolShmemSizeInMB,
                            FALCON_CONNECTION_POOL_SHMEM_SIZE_DEFAULT / 1024 / 1024,
                            32,
                            64 * 1024,
                            PGC_POSTMASTER,
                            0,
                            NULL,
                            NULL,
                            NULL);
    FalconConnectionPoolShmemSize = (uint64_t)FalconConnectionPoolShmemSizeInMB * 1024 * 1024;

    DefineCustomStringVariable("falcon_plugin.directory",
                              gettext_noop("Directory containing Falcon plugins."),
                              NULL,
                              &falcon_plugin_directory,
                              NULL,
                              PGC_POSTMASTER,
                              0,
                              NULL,
                              NULL,
                              NULL);

    DefineCustomStringVariable("falcon_plugin.custom_config",
                              gettext_noop("Custom configuration for plugins in JSON format."),
                              NULL,
                              &falcon_plugin_custom_config,
                              NULL,
                              PGC_SIGHUP,
                              0,
                              NULL,
                              NULL,
                              NULL);
}
