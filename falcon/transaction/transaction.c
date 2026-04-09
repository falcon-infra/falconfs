/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "transaction/transaction.h"

#include "access/twophase.h"
#include "access/xact.h"
#include "catalog/pg_namespace_d.h"
#include "falcon_config.h"
#include "miscadmin.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

#include "control/control_flag.h"
#include "dir_path_shmem/dir_path_hash.h"
#include "distributed_backend/remote_comm.h"
#include "distributed_backend/remote_comm_falcon.h"
#include "metadb/foreign_server.h"
#include "transaction/transaction_cleanup.h"
#include "utils/error_log.h"
#include "utils/path_parse.h"
#include "utils/rwlock.h"
#include "utils/utils.h"

static FalconExplicitTransactionState falconExplicitTransactionState = FALCON_EXPLICIT_TRANSACTION_NONE;

static void FalconTransactionCallback(XactEvent event, void *args);
static void FalconSubTransactionCallback(SubXactEvent event, SubTransactionId mySubid,
                                          SubTransactionId parentSubid, void *arg);

char PreparedTransactionGid[MAX_TRANSACTION_GID_LENGTH + 1];
char RemoteTransactionGid[MAX_TRANSACTION_GID_LENGTH + 1];

/*
 * Sub-transaction RWLock state tracking
 *
 * We use a stack to track the number of held RWLocks at each sub-transaction
 * level. When a sub-transaction starts, we record RWLockGetHeldCount().
 * When it aborts, we release only locks acquired during that sub-transaction
 * by calling RWLockReleaseSince(savedCount, true).
 *
 * This preserves parent transaction's locks (e.g., path locks acquired during
 * PHASE 1 of CreateHandle) while cleaning up the sub-transaction's locks.
 *
 * Maximum nesting depth: 16 levels (should be sufficient for normal use cases).
 * If exceeded, FALCON_ELOG_ERROR will be raised in FalconSubTransactionCallback.
 */
#define MAX_SUB_XACT_DEPTH 16
static int subXactRWLockStack[MAX_SUB_XACT_DEPTH];
static int subXactRWLockStackTop = 0;

static void ClearRemoteTransactionGid()
{
    if (RemoteTransactionGid[0] != '\0') {
        RemoveInprogressTransaction(RemoteTransactionGid);
    }
    RemoteTransactionGid[0] = '\0';
}

StringInfo GetImplicitTransactionGid()
{
    StringInfo str = makeStringInfo();
    appendStringInfo(str,
                     "%s%u:%d:%c",
                     FALCON_TRANSACTION_2PC_HEAD,
                     GetTopTransactionId(),
                     GetLocalServerId(),
                     IsLocalWrite() ? 'T' : 'F');
    if (strlen(str->data) > MAX_TRANSACTION_GID_LENGTH)
        FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR,
                                   "length of transaction gid is expected to below %d",
                                   MAX_TRANSACTION_GID_LENGTH);
    return str;
}

void FalconExplicitTransactionBegin()
{
    if (falconExplicitTransactionState != FALCON_EXPLICIT_TRANSACTION_NONE)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "incorrect transaction state.");

    BeginTransactionBlock();
    CommitTransactionCommand();
    int setIsolationLevelRes = set_config_option("transaction_isolation",
                                                 "repeatable read",
                                                 (superuser() ? PGC_SUSET : PGC_USERSET),
                                                 PGC_S_SESSION,
                                                 GUC_ACTION_SET,
                                                 true,
                                                 0,
                                                 false);
    if (setIsolationLevelRes != 1)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "isolation level set error.");

    falconExplicitTransactionState = FALCON_EXPLICIT_TRANSACTION_BEGIN;
}

// may cause rollback
bool FalconExplicitTransactionCommit()
{
    if (falconExplicitTransactionState != FALCON_EXPLICIT_TRANSACTION_BEGIN)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "incorrect transaction state.");

    bool isCommit = EndTransactionBlock(false);
    CommitTransactionCommand();

    falconExplicitTransactionState = FALCON_EXPLICIT_TRANSACTION_NONE;
    return isCommit;
}

void FalconExplicitTransactionRollback()
{
    if (falconExplicitTransactionState != FALCON_EXPLICIT_TRANSACTION_BEGIN)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "incorrect transaction state.");

    UserAbortTransactionBlock(false);
    CommitTransactionCommand();

    falconExplicitTransactionState = FALCON_EXPLICIT_TRANSACTION_NONE;
}

extern bool FalconExplicitTransactionPrepare(const char *gid)
{
    if (falconExplicitTransactionState != FALCON_EXPLICIT_TRANSACTION_BEGIN)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "incorrect transaction state.");

    bool isPrepare = PrepareTransactionBlock(gid);
    CommitTransactionCommand();

    falconExplicitTransactionState = FALCON_EXPLICIT_TRANSACTION_NONE;
    return isPrepare;
}

extern void FalconExplicitTransactionCommitPrepared(const char *gid)
{
    if (falconExplicitTransactionState != FALCON_EXPLICIT_TRANSACTION_PREPARED)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "incorrect transaction state.");

    PreventInTransactionBlock(true, "COMMIT PREPARED");
    FinishPreparedTransaction(gid, true);
    CommitTransactionCommand();

    falconExplicitTransactionState = FALCON_EXPLICIT_TRANSACTION_NONE;
}
extern void FalconExplicitTransactionRollbackPrepared(const char *gid)
{
    if (falconExplicitTransactionState != FALCON_EXPLICIT_TRANSACTION_PREPARED)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "incorrect transaction state.");

    PreventInTransactionBlock(true, "ROLLBACK PREPARED");
    FinishPreparedTransaction(gid, false);
    CommitTransactionCommand();

    falconExplicitTransactionState = FALCON_EXPLICIT_TRANSACTION_NONE;
}

void RegisterFalconTransactionCallback(void)
{
    RegisterXactCallback(FalconTransactionCallback, NULL);
    RegisterSubXactCallback(FalconSubTransactionCallback, NULL);
}

static void FalconTransactionCallback(XactEvent event, void *args)
{
    switch (event) {
    case XACT_EVENT_PRE_COMMIT: {
        FalconRemoteCommandPrepare();
        break;
    }
    case XACT_EVENT_COMMIT: {
        FalconRemoteCommandCommit();

        TransactionLevelPathParseReset();
        CommitForDirPathHash();
        RWLockReleaseAll(false);
        ClearRemoteTransactionGid();
        ClearRemoteConnectionCommand();
        break;
    }
    case XACT_EVENT_ABORT: {
        FalconEnterAbortProgress();

        TransactionLevelPathParseReset();
        AbortForDirPathHash();
        RWLockReleaseAll(true);
        if (!FalconRemoteCommandAbort())
            FALCON_ELOG_WARNING(PROGRAM_ERROR, "Abort failed on some servers.");
        ClearRemoteTransactionGid();
        ClearRemoteConnectionCommand();
        if (falconExplicitTransactionState == FALCON_EXPLICIT_TRANSACTION_BEGIN)
            FalconExplicitTransactionRollback();

        FalconQuitAbortProgress();
        break;
    }
    case XACT_EVENT_PRE_PREPARE: {
        break;
    }
    case XACT_EVENT_PREPARE: {
        TransactionLevelPathParseReset();
        break;
    }
    case XACT_EVENT_PARALLEL_COMMIT:
    case XACT_EVENT_PARALLEL_PRE_COMMIT:
    case XACT_EVENT_PARALLEL_ABORT: {
        break;
    }
    }
}

/*
 * FalconSubTransactionCallback - Handle sub-transaction events for RWLock cleanup
 *
 * This callback is registered via RegisterSubXactCallback() and is invoked by
 * PostgreSQL at key sub-transaction lifecycle points.
 *
 * Problem being solved:
 *   When a sub-transaction aborts due to ERROR (e.g., unique key violation in
 *   InsertIntoInodeTable), PostgreSQL's errfinish() forcibly resets
 *   InterruptHoldoffCount to 0. However, Falcon's held_rwlocks array is not
 *   automatically cleaned up, causing two issues:
 *
 *   1. Stale lock entries remain in held_rwlocks, leading to incorrect lock
 *      release attempts later (possibly double-free or releasing wrong locks)
 *
 *   2. InterruptHoldoffCount mismatch: at main transaction commit,
 *      CommitTransaction() expects InterruptHoldoffCount > 0 but finds 0,
 *      causing assertion failure: "InterruptHoldoffCount > 0"
 *
 * Solution:
 *   Track the number of held locks at sub-transaction start, and on abort,
 *   release only locks acquired during that sub-transaction using
 *   RWLockReleaseSince(). This:
 *   - Cleans up stale lock entries
 *   - Preserves parent transaction's locks (e.g., path locks from PHASE 1)
 *   - Maintains InterruptHoldoffCount consistency via keepInterruptHoldoffCount=true
 *
 * Events handled:
 *   SUBXACT_EVENT_START_SUB:   Push current lock count onto stack
 *   SUBXACT_EVENT_ABORT_SUB:   Pop stack and release locks since saved count
 *   SUBXACT_EVENT_COMMIT_SUB:  Pop stack (locks become part of parent)
 *   SUBXACT_EVENT_PRE_COMMIT_SUB: No action needed
 */
static void FalconSubTransactionCallback(SubXactEvent event, SubTransactionId mySubid,
                                          SubTransactionId parentSubid, void *arg)
{
    switch (event) {
    case SUBXACT_EVENT_START_SUB: {
        /*
         * Record the current number of held RWLocks at sub-transaction start.
         *
         * This snapshot represents the "baseline" - locks held by the parent
         * transaction. Any locks acquired during this sub-transaction will
         * have indices >= this saved count.
         *
         * Example:
         *   Parent holds lock1 (num_held_rwlocks = 1)
         *   Sub-tx starts: savedCount = 1
         *   Sub-tx acquires lock2, lock3 (num_held_rwlocks = 3)
         *   Sub-tx aborts: release locks at indices [1, 2] (lock2, lock3)
         *   Result: lock1 (index 0) is preserved
         */
        if (subXactRWLockStackTop >= MAX_SUB_XACT_DEPTH)
            FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR,
                                       "sub-transaction nesting depth (%d) exceeds maximum (%d)",
                                       subXactRWLockStackTop,
                                       MAX_SUB_XACT_DEPTH);

        subXactRWLockStack[subXactRWLockStackTop++] = RWLockGetHeldCount();
        break;
    }

    case SUBXACT_EVENT_ABORT_SUB: {
        /*
         * Sub-transaction is aborting - release only locks acquired during
         * this sub-transaction.
         *
         * Context at this point:
         *   - An ERROR has been thrown (e.g., unique key violation)
         *   - errfinish() has reset InterruptHoldoffCount to 0
         *   - held_rwlocks array still contains all locks (parent + sub-tx)
         *   - We're in the PG_CATCH block or error recovery path
         *
         * Critical parameter: keepInterruptHoldoffCount = true
         *   This tells RWLockReleaseSince to preserve the current
         *   InterruptHoldoffCount value (0) instead of letting it decrement
         *   naturally. Without this, the count would go negative.
         *
         * Safety guarantee:
         *   Only locks at indices >= savedCount are released. Parent
         *   transaction's locks (indices < savedCount) remain untouched.
         *   This is crucial for operations like CreateHandle which acquire
         *   path locks in PHASE 1 (parent tx) and may abort a sub-tx in
         *   PHASE 2 without releasing those path locks.
         *
         * IMPORTANT: We must read savedCount BEFORE decrementing the stack top.
         *   If RWLockReleaseSince() throws an error, we want the stack to remain
         *   in a consistent state so that outer error handling can retry or
         *   handle the situation correctly. Decrementing the stack top first
         *   would cause a lock leak if RWLockReleaseSince() fails.
         */
        if (subXactRWLockStackTop <= 0)
            FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR,
                                       "sub-transaction RWLock stack underflow (top = %d)",
                                       subXactRWLockStackTop);

        int savedCount = subXactRWLockStack[subXactRWLockStackTop - 1];
        RWLockReleaseSince(savedCount, true);
        subXactRWLockStackTop--;
        break;
    }

    case SUBXACT_EVENT_COMMIT_SUB: {
        /*
         * Sub-transaction is committing successfully - locks acquired during
         * this sub-transaction become part of the parent transaction's locks.
         *
         * We simply pop the stack without releasing any locks. The locks will
         * be released when the parent transaction commits/aborts, or when a
         * parent-level sub-transaction aborts (in which case its savedCount
         * will be less than the current num_held_rwlocks).
         */
        if (subXactRWLockStackTop <= 0)
            FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR,
                                       "sub-transaction RWLock stack underflow on commit (top = %d)",
                                       subXactRWLockStackTop);

        subXactRWLockStackTop--;
        break;
    }

    case SUBXACT_EVENT_PRE_COMMIT_SUB: {
        /* No action needed before sub-transaction commit */
        break;
    }
    }
}
