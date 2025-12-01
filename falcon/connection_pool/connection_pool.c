/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "connection_pool/connection_pool.h"

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
#include "access/xlog.h"
#include "access/xact.h"

#include "connection_pool/brpc_server.h"
#include "control/control_flag.h"
#include "utils/utils.h"

int FalconPGPort = 0;
int FalconConnectionPoolPort = FALCON_CONNECTION_POOL_PORT_DEFAULT;
int FalconConnectionPoolSize = FALCON_CONNECTION_POOL_SIZE_DEFAULT;
uint64_t FalconConnectionPoolShmemSize = FALCON_CONNECTION_POOL_SHMEM_SIZE_DEFAULT;
char *FalconNodeLocalIp = NULL;
static char *FalconConnectionPoolShmemBuffer = NULL;
FalconShmemAllocator FalconConnectionPoolShmemAllocator;

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
    while (true)
    {
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

    FalconPGPort = PostPortNumber;
    PG_RunConnectionPoolBrpcServer();

    elog(LOG, "FalconDaemonConnectionPoolProcessMain: connection pool server stopped.");
    return;
}

static void FalconDaemonConnectionPoolProcessSigTermHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    elog(LOG, "FalconDaemonConnectionPoolProcessSigTermHandler: get sigterm.");
    got_SIGTERM = true;
    PG_ShutdownConnectionPoolBrpcServer();

    errno = save_errno;
}

int FalconConnectionPoolGotSigTerm(void) { return got_SIGTERM; }

size_t FalconConnectionPoolShmemsize() { return FalconConnectionPoolShmemSize; }
void FalconConnectionPoolShmemInit()
{
    bool initialized;

    FalconConnectionPoolShmemBuffer =
        ShmemInitStruct("Falcon Connection Pool Shmem", FalconConnectionPoolShmemsize(), &initialized);
    if (FalconShmemAllocatorInit(&FalconConnectionPoolShmemAllocator,
                                 FalconConnectionPoolShmemBuffer,
                                 FalconConnectionPoolShmemsize()) != 0)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "FalconShmemAllocatorInit failed.");
    if (!initialized) {
        memset(FalconConnectionPoolShmemAllocator.signatureCounter,
               0,
               sizeof(PaddedAtomic64) *
                   (1 + FALCON_SHMEM_ALLOCATOR_FREE_LIST_COUNT + FalconConnectionPoolShmemAllocator.pageCount));
    }
}
