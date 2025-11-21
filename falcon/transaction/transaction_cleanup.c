/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "transaction/transaction_cleanup.h"

#include <unistd.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/xlog.h"
#include "catalog/indexing.h"
#include "catalog/pg_collation_d.h"
#include "executor/executor.h"
#include "postmaster/bgworker.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"

#include "control/control_flag.h"
#include "distributed_backend/remote_comm.h"
#include "distributed_backend/remote_comm_falcon.h"
#include "metadb/foreign_server.h"
#include "transaction/falcon_distributed_transaction.h"
#include "transaction/transaction_cleanup.h"
#include "utils/error_log.h"
#include "utils/shmem_control.h"
#include "utils/utils.h"

static Relation FalconDistributedTransactionRel = NULL;
static SysScanDesc FalconDistributedTransactionScanDescriptor = NULL;
static HASH_SEQ_STATUS *FalconDistributedTransactionHashSeqStatus = NULL;

static ShmemControlData *InprogressTransactionShmemControl;
static HTAB *InprogressTransactions = NULL;
static int32_t *InprogressTransactionCount = 0;

static volatile bool got_SIGTERM = false;
static void FalconDaemon2PCCleanupProcessSigTermHandler(SIGNAL_ARGS);
static int CleanupPreparedTransactions(int serverId);
static int Cleanup2PC(void);

PG_FUNCTION_INFO_V1(falcon_transaction_cleanup_trigger);
PG_FUNCTION_INFO_V1(falcon_transaction_cleanup_test);

Datum falcon_transaction_cleanup_trigger(PG_FUNCTION_ARGS)
{
    while (!CheckFalconBackgroundServiceStarted())
        sleep(1);

    int count = Cleanup2PC();

    PG_RETURN_INT16(count);
}

Datum falcon_transaction_cleanup_test(PG_FUNCTION_ARGS) { PG_RETURN_INT16(0); }

void FalconDaemon2PCFailureCleanupProcessMain(Datum main_arg)
{
    pqsignal(SIGTERM, FalconDaemon2PCCleanupProcessSigTermHandler);
    BackgroundWorkerUnblockSignals();

    BackgroundWorkerInitializeConnection("postgres", NULL, 0);

    ResourceOwner myOwner = ResourceOwnerCreate(NULL, "falcon background 2pc cleanup");
    MemoryContext myContext = AllocSetContextCreate(TopMemoryContext,
                                                    "falcon background 2pc cleanup",
                                                    ALLOCSET_DEFAULT_MINSIZE,
                                                    ALLOCSET_DEFAULT_INITSIZE,
                                                    ALLOCSET_DEFAULT_MAXSIZE);
    ResourceOwner oldOwner = CurrentResourceOwner;
    CurrentResourceOwner = myOwner;
    elog(LOG, "FalconDaemon2PCFailureCleanupProcessMain: wait init.");
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
    bool serviceStarted = false;
    do {
        sleep(1);
        serviceStarted = CheckFalconBackgroundServiceStarted();
    } while (!serviceStarted || RecoveryInProgress());
    elog(LOG, "FalconDaemon2PCFailureCleanupProcessMain: init finished.");
    int serverId = -1;
    while (true)
    {
        StartTransactionCommand();
        serverId = GetLocalServerId();
        CommitTransactionCommand();
        if (serverId != -1)
            break;
        
        // wait for shard table init
        sleep(1);
    }
    if (serverId == 0) {
        elog(LOG, "FalconDaemon2PCFailureCleanupProcessMain: Running.");
        while (!got_SIGTERM) {
            MemoryContext oldContext = MemoryContextSwitchTo(myContext);

            StartTransactionCommand();
            int count = Cleanup2PC();
            CommitTransactionCommand();

            if (count != 0) {
                elog(LOG, "2PCFailureCleanup: cleanup %d 2PC failure", count);
            }

            MemoryContextSwitchTo(oldContext);
            MemoryContextReset(myContext);
            sleep(60);
        }
    }

    elog(LOG, "FalconDaemon2PCFailureCleanupProcessMain: exit.");
    CurrentResourceOwner = oldOwner;
    ResourceOwnerRelease(myOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
    ResourceOwnerRelease(myOwner, RESOURCE_RELEASE_LOCKS, true, true);
    ResourceOwnerRelease(myOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
    ResourceOwnerDelete(myOwner);
    MemoryContextDelete(myContext);
    return;
}

void AddInprogressTransaction(const char *transactionName)
{
    LWLockAcquire(&InprogressTransactionShmemControl->lock, LW_EXCLUSIVE);

    bool found;
    hash_search(InprogressTransactions, transactionName, HASH_ENTER, &found);
    if (found)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "duplicate transaction name.");

    LWLockRelease(&InprogressTransactionShmemControl->lock);
}

void RemoveInprogressTransaction(const char *transactionName)
{
    LWLockAcquire(&InprogressTransactionShmemControl->lock, LW_EXCLUSIVE);

    bool found;
    hash_search(InprogressTransactions, transactionName, HASH_REMOVE, &found);
    if (!found)
        elog(WARNING, "%s, removing invalid transaction name.", FalconErrorCodeToString[PROGRAM_ERROR]);

    LWLockRelease(&InprogressTransactionShmemControl->lock);
}

void Write2PCRecord(int serverId, char *gid)
{
    Datum values[Natts_falcon_distributed_transaction];
    bool isNulls[Natts_falcon_distributed_transaction];

    /*form new transaction tuple */
    memset(values, 0, sizeof(values));
    memset(isNulls, false, sizeof(isNulls));

    values[Anum_falcon_distributed_transaction_nodeid - 1] = Int32GetDatum(serverId);
    values[Anum_falcon_distributed_transaction_gid - 1] = CStringGetTextDatum(gid);

    Relation rel = table_open(FalconDistributedTransactionRelationId(), RowExclusiveLock);

    TupleDesc tupleDescriptor = RelationGetDescr(rel);
    HeapTuple heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

    CatalogTupleInsert(rel, heapTuple);

    table_close(rel, RowExclusiveLock);
}

static int Cleanup2PC(void)
{
    LOCKTAG tag;
    SET_LOCKTAG_FALCON_OPERATION(tag, FALCON_LOCK_2PC_CLEANUP);
    (void)LockAcquire(&tag, ShareUpdateExclusiveLock, false, false);

    int count = 0;

    List *workerIdList = GetAllForeignServerId(true, false);
    for (int i = 0; i < list_length(workerIdList); i++) {
        PG_TRY();
        {
            count += CleanupPreparedTransactions(list_nth_int(workerIdList, i));
        }
        PG_CATCH();
        {
            if (FalconDistributedTransactionScanDescriptor) {
                systable_endscan(FalconDistributedTransactionScanDescriptor);
                FalconDistributedTransactionScanDescriptor = NULL;
            }
            if (FalconDistributedTransactionRel) {
                table_close(FalconDistributedTransactionRel, RowExclusiveLock);
                FalconDistributedTransactionRel = NULL;
            }
            if (FalconDistributedTransactionHashSeqStatus) {
                hash_seq_term(FalconDistributedTransactionHashSeqStatus);
                FalconDistributedTransactionHashSeqStatus = NULL;
            }
            FlushErrorState();
            elog(WARNING, "falcon: Transaction cleanup error on server %d!", list_nth_int(workerIdList, i));
        }
        PG_END_TRY();
    }

    (void)LockRelease(&tag, ShareUpdateExclusiveLock, false);

    return count;
}

static HTAB *AllocHashSet(void)
{
    HASHCTL hash_ctl;

    memset(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = MAX_TRANSACTION_GID_LENGTH;
    hash_ctl.entrysize = MAX_TRANSACTION_GID_LENGTH;
    hash_ctl.hcxt = CurrentMemoryContext;

    HTAB *set = hash_create("FalconHashSet", 8192, &hash_ctl, HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);

    return set;
}

static void HashSetErase(HTAB *set, const char *transaction)
{
    bool found = false;
    hash_search(set, transaction, HASH_REMOVE, &found);
}

static bool IsTransactionInSet(HTAB *set, const char *transaction)
{
    bool found = false;

    hash_search(set, transaction, HASH_FIND, &found);
    return found;
}

static HTAB *GetRemotePreparedTransactionSet(int serverId)
{
    int i;
    bool found = false;
    HTAB *transactionSet = AllocHashSet();

    StringInfo commandContainer = makeStringInfo();
    int localId = GetLocalServerId();
    appendStringInfo(commandContainer,
                     "SELECT gid FROM pg_prepared_xacts WHERE gid LIKE '%s%%:%d:%%';",
                     FALCON_TRANSACTION_2PC_HEAD,
                     localId);
    const char *command = commandContainer->data;

    FalconPlainCommandOnWorkerList(command, REMOTE_COMMAND_FLAG_NO_BEGIN, list_make1_int(serverId));
    MultipleServerRemoteCommandResult allResList = FalconSendCommandAndWaitForResult();
    RemoteCommandResultPerServerData *data = list_nth(allResList, 0);

    PGresult *res = list_nth(data->remoteCommandResult, 0);

    int rowCount = PQntuples(res);
    for (i = 0; i < rowCount; i++) {
        const char *transactionName = PQgetvalue(res, i, 0);

        hash_search(transactionSet, transactionName, HASH_ENTER, &found);
    }

    return transactionSet;
}

static HTAB *GetActiveTransactionSet(void)
{
    HTAB *activeTransactionSet = AllocHashSet();
    LWLockAcquire(&InprogressTransactionShmemControl->lock, LW_EXCLUSIVE);

    bool found;
    HASH_SEQ_STATUS scan;
    InprogressTransactionData *inprogressTransaction;
    hash_seq_init(&scan, InprogressTransactions);
    while ((inprogressTransaction = (InprogressTransactionData *)hash_seq_search(&scan))) {
        hash_search(activeTransactionSet, inprogressTransaction->gid, HASH_ENTER, &found);
    }

    LWLockRelease(&InprogressTransactionShmemControl->lock);
    return activeTransactionSet;
}

static bool CommitOrRollbackPreparedTransaction(int serverId, char *transactionGid, bool isCommit)
{
    char *command = palloc(MAX_TRANSACTION_GID_LENGTH + 24);
    if (isCommit)
        sprintf(command, "COMMIT PREPARED '%s';", transactionGid);
    else
        sprintf(command, "ROLLBACK PREPARED '%s';", transactionGid);

    FalconPlainCommandOnWorkerList(command, REMOTE_COMMAND_FLAG_NO_BEGIN, list_make1_int(serverId));
    FalconSendCommandAndWaitForResult();

    elog(LOG, "cleanup a prepared transaction on server: %d, command: %s", serverId, command);

    return true;
}

static int CleanupPreparedTransactions(int serverId)
{
    int cleanedTransactionCount = 0;

    MemoryContext localContext = AllocSetContextCreateInternal(CurrentMemoryContext,
                                                               "Cleanup2PC",
                                                               ALLOCSET_DEFAULT_MINSIZE,
                                                               ALLOCSET_DEFAULT_INITSIZE,
                                                               ALLOCSET_DEFAULT_MAXSIZE);
    MemoryContext oldContext = MemoryContextSwitchTo(localContext);

    /*********************** set A ******************************************/
    HTAB *setA = GetRemotePreparedTransactionSet(serverId);

    /*********************** set B ******************************************/
    HTAB *setB = GetActiveTransactionSet();

    /*********************** set C ******************************************/
    // We will traverse through this set, so it's not organized as a set
    FalconDistributedTransactionRel = table_open(FalconDistributedTransactionRelationId(), RowExclusiveLock);
    TupleDesc tupleDescriptor = RelationGetDescr(FalconDistributedTransactionRel);
    ScanKeyData scanKey[1];
    ScanKeyInit(&scanKey[0],
                Anum_falcon_distributed_transaction_nodeid,
                BTEqualStrategyNumber,
                F_INT4EQ,
                Int32GetDatum(serverId));
    FalconDistributedTransactionScanDescriptor = systable_beginscan(FalconDistributedTransactionRel,
                                                                    FalconDistributedTransactionRelationIndexId(),
                                                                    GetTransactionSnapshot(),
                                                                    NULL,
                                                                    1,
                                                                    scanKey);

    /*********************** set D ******************************************/
    HTAB *setD = GetRemotePreparedTransactionSet(serverId);

    /*
     * For each item x in C,
     * 1. if x ∈ B, x is ongoing transaction experiencing two-phase committing, nothing to do
     * 2. else if x ∈ A and x ∈ D, x is a residual prepared transaction after node crash or fault, should commit it
     * 3. else if x not ∈ A but x ∈ D, check its status next time
     * 4. else if x ∈ A but x not ∈ D, x has been committed
     * 5. else if x not ∈ A and x not ∈ D, x has been committed
     */
    HeapTuple heapTuple = NULL;
    while (HeapTupleIsValid(heapTuple = systable_getnext(FalconDistributedTransactionScanDescriptor))) {
        bool isNull = false;
        Datum transactionGidDatum =
            heap_getattr(heapTuple, Anum_falcon_distributed_transaction_gid, tupleDescriptor, &isNull);
        char *transactionGid = TextDatumGetCString(transactionGidDatum);

        bool foundInSetA = false;
        bool foundInSetB = false;
        bool foundInSetD = false;

        foundInSetB = IsTransactionInSet(setB, transactionGid);
        if (foundInSetB) {
            /* condition 1 meets */
            continue;
        }

        foundInSetA = IsTransactionInSet(setA, transactionGid);
        foundInSetD = IsTransactionInSet(setD, transactionGid);

        if (foundInSetA && foundInSetD) {
            /* condition 2 meets */
            CommitOrRollbackPreparedTransaction(serverId, transactionGid, true);
            HashSetErase(setA, transactionGid);
            cleanedTransactionCount++;
        } else if (!foundInSetA && foundInSetD) {
            /* condition 3 meets */
            continue;
        } else {
            /* condition 4 meets */
            /* condition 5 meets */
            CatalogTupleDelete(FalconDistributedTransactionRel, &heapTuple->t_self);
            if (foundInSetA) {
                HashSetErase(setA, transactionGid);
            }
        }
    }

    systable_endscan(FalconDistributedTransactionScanDescriptor);
    FalconDistributedTransactionScanDescriptor = NULL;
    table_close(FalconDistributedTransactionRel, RowExclusiveLock);
    FalconDistributedTransactionRel = NULL;

    /*
     * For each item y in A,
     * 1. if y ∈ B, y is ongoing transaction experiencing two-phase committing, nothing to do
     * 2. else since y not ∈ C, it is a residual prepared transaction that failed in the prepare phase, just roll back
     * it
     */
    char *transactionGid = NULL;
    HASH_SEQ_STATUS status;
    FalconDistributedTransactionHashSeqStatus = &status;
    hash_seq_init(FalconDistributedTransactionHashSeqStatus, setA);
    while ((transactionGid = hash_seq_search(FalconDistributedTransactionHashSeqStatus)) != NULL) {
        bool foundInSetB = IsTransactionInSet(setB, transactionGid);
        if (foundInSetB) {
            /* condition 1 meets */
            continue;
        } else {
            /* condition 2 meets */
            CommitOrRollbackPreparedTransaction(serverId, transactionGid, false);
            cleanedTransactionCount++;
        }
    }
    FalconDistributedTransactionHashSeqStatus = NULL;

    MemoryContextSwitchTo(oldContext);
    MemoryContextDelete(localContext);

    return cleanedTransactionCount;
}

static void FalconDaemon2PCCleanupProcessSigTermHandler(SIGNAL_ARGS)
{
    int save_errno = errno;

    elog(LOG, "FalconDaemon2PCCleanupProcessSigTermHandler: get sigterm.");
    got_SIGTERM = true;

    errno = save_errno;
}

size_t TransactionCleanupShmemsize()
{
    return sizeof(ShmemControlData) + sizeof(int32_t) +
           sizeof(InprogressTransactionData) * MAX_INPROGRESS_TRANSACTION_COUNT;
}
void TransactionCleanupShmemInit()
{
    bool initialized;

    InprogressTransactionShmemControl = ShmemInitStruct("Transaction Cleanup - Inprogress Transactions Control",
                                                        sizeof(ShmemControlData) + sizeof(int32_t),
                                                        &initialized);
    InprogressTransactionCount = (int32_t *)(InprogressTransactionShmemControl + 1);
    if (!initialized) {
        InprogressTransactionShmemControl->trancheId = LWLockNewTrancheId();
        InprogressTransactionShmemControl->lockTrancheName = "Falcon Inprogress Transactions";
        LWLockRegisterTranche(InprogressTransactionShmemControl->trancheId,
                              InprogressTransactionShmemControl->lockTrancheName);
        LWLockInitialize(&InprogressTransactionShmemControl->lock, InprogressTransactionShmemControl->trancheId);

        *InprogressTransactionCount = 0;
    }

    HASHCTL info;
    /* init hash table for nodeoid to dnDefs/coDefs lookup*/
    info.keysize = sizeof(InprogressTransactionData);
    info.entrysize = sizeof(InprogressTransactionData); //? maybe fix later
    InprogressTransactions = ShmemInitHash("Falcon Inprogress Transactions Hash Table",
                                           MAX_INPROGRESS_TRANSACTION_COUNT,
                                           MAX_INPROGRESS_TRANSACTION_COUNT,
                                           &info,
                                           HASH_ELEM | HASH_STRINGS);

    if (!InprogressTransactions) {
        elog(FATAL, "invalid shmem status when creating inprogressTransaction hashtable.");
    }
}
