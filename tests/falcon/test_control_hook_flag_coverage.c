#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "postgres.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "executor/executor.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "tcop/utility.h"

#include "transaction/transaction.h"
#include "utils/utils.h"

static int g_before_shmem_exit_count;
static int g_standard_process_utility_count;
static int g_standard_executor_start_count;
static int g_cleanup_connections_count;
static int g_release_all_count;
static int g_clear_dir_path_count;
static int g_invalidate_foreign_count;
static int g_invalidate_shard_count;
static bool g_shmem_found;
static char g_shmem[4096];

Oid CachedRelationOid[LAST_CACHED_RELATION_TYPE];
char PreparedTransactionGid[MAX_TRANSACTION_GID_LENGTH + 1];

static void ResetHookFlagHarness(void)
{
    g_before_shmem_exit_count = 0;
    g_standard_process_utility_count = 0;
    g_standard_executor_start_count = 0;
    g_cleanup_connections_count = 0;
    g_release_all_count = 0;
    g_clear_dir_path_count = 0;
    g_invalidate_foreign_count = 0;
    g_invalidate_shard_count = 0;
    g_shmem_found = false;
    memset(g_shmem, 0, sizeof(g_shmem));
    memset(CachedRelationOid, 0, sizeof(CachedRelationOid));
}

static int ExpectTrue(bool condition, const char *message)
{
    if (!condition) {
        (void)message;
        return 1;
    }
    return 0;
}

static List *AppendCell(List *list, NodeTag type, ListCell cell)
{
    if (list == NIL) {
        list = calloc(1, sizeof(List));
        list->type = type;
        list->max_length = 4;
        list->elements = calloc(list->max_length, sizeof(ListCell));
    } else if (list->length == list->max_length) {
        list->max_length *= 2;
        list->elements = realloc(list->elements, list->max_length * sizeof(ListCell));
    }

    list->elements[list->length++] = cell;
    return list;
}

List *lappend(List *list, void *datum)
{
    ListCell cell = {.ptr_value = datum};
    return AppendCell(list, T_List, cell);
}

void before_shmem_exit(pg_on_exit_callback function, Datum arg)
{
    (void)function;
    (void)arg;
    g_before_shmem_exit_count++;
}

void standard_ProcessUtility(PlannedStmt *pstmt,
                             const char *queryString,
                             bool readOnlyTree,
                             ProcessUtilityContext context,
                             ParamListInfo params,
                             QueryEnvironment *queryEnv,
                             DestReceiver *dest,
                             QueryCompletion *qc)
{
    (void)pstmt;
    (void)queryString;
    (void)readOnlyTree;
    (void)context;
    (void)params;
    (void)queryEnv;
    (void)dest;
    (void)qc;
    g_standard_process_utility_count++;
}

void standard_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
    (void)queryDesc;
    (void)eflags;
    g_standard_executor_start_count++;
}

void CleanupForeignServerConnections(void)
{
    g_cleanup_connections_count++;
}

void RWLockReleaseAll(bool isProcExit)
{
    (void)isProcExit;
    g_release_all_count++;
}

void ClearDirPathHash(void)
{
    g_clear_dir_path_count++;
}

void InvalidateForeignServerShmemCache(void)
{
    g_invalidate_foreign_count++;
}

void InvalidateShardTableShmemCache(void)
{
    g_invalidate_shard_count++;
}

void *ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
    (void)name;
    if (size > sizeof(g_shmem))
        abort();
    *foundPtr = g_shmem_found;
    return g_shmem;
}

int LWLockNewTrancheId(void)
{
    return 31;
}

void LWLockRegisterTranche(int tranche_id, const char *tranche_name)
{
    (void)tranche_id;
    (void)tranche_name;
}

void LWLockInitialize(LWLock *lock, int tranche_id)
{
    (void)lock;
    (void)tranche_id;
}

void ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
    (void)conditionName;
    (void)fileName;
    (void)lineNumber;
    abort();
}

#include "../../falcon/control/control_flag.c"
#include "../../falcon/control/hook.c"

static int TestControlFlagSharedStateLifecycle(void)
{
    ResetHookFlagHarness();

    /*
     * Initialize a fresh control shared-memory block, then verify the SQL
     * wrapper flips the background-service flag seen by the C checker.
     */
    FalconControlShmemInit();
    if (ExpectTrue(!CheckFalconBackgroundServiceStarted(), "background service starts cleared"))
        return 1;
    falcon_start_background_service(NULL);
    if (ExpectTrue(CheckFalconBackgroundServiceStarted(), "start wrapper sets the shared flag"))
        return 1;
    if (ExpectTrue(FalconControlShmemsize() == sizeof(ShmemControlData) + sizeof(pg_atomic_uint32),
                   "control shared-memory size matches stored fields"))
        return 1;

    /* Reinitialize as an existing segment to cover the branch that skips tranche setup. */
    g_shmem_found = true;
    FalconControlShmemInit();
    return ExpectTrue(CheckFalconBackgroundServiceStarted(), "existing shared flag remains readable");
}

static int TestAbortProgressFlagTransitions(void)
{
    ResetHookFlagHarness();

    /* The abort-progress helpers are plain process-local state transitions. */
    if (ExpectTrue(!FalconIsInAbortProgress(), "abort progress starts false"))
        return 1;
    FalconEnterAbortProgress();
    if (ExpectTrue(FalconIsInAbortProgress(), "enter abort progress sets flag"))
        return 1;
    FalconQuitAbortProgress();
    return ExpectTrue(!FalconIsInAbortProgress(), "quit abort progress clears flag");
}

static int TestHookRegistrationAndCleanup(void)
{
    TransactionStmt transactionStmt = {.type = T_TransactionStmt};
    PlannedStmt pstmt = {.type = T_PlannedStmt, .utilityStmt = (Node *)&transactionStmt};

    ResetHookFlagHarness();
    /*
     * The first utility hook call registers process-exit cleanup and still
     * delegates to PostgreSQL's standard utility executor.
     */
    falcon_ProcessUtility(&pstmt, "SELECT 1", false, PROCESS_UTILITY_TOPLEVEL, NULL, NULL, NULL, NULL);
    if (ExpectTrue(g_before_shmem_exit_count == 1 && g_standard_process_utility_count == 1,
                   "utility hook registers cleanup once and delegates"))
        return 1;

    /* A later executor hook call should reuse the registered callback state. */
    falcon_ExecutorStart(NULL, 0);
    if (ExpectTrue(g_before_shmem_exit_count == 1 && g_standard_executor_start_count == 1,
                   "executor hook delegates without registering twice"))
        return 1;

    FalconCleanupOnExit(0, 0);
    return ExpectTrue(g_release_all_count == 1 && g_cleanup_connections_count == 1,
                      "process cleanup releases locks and foreign connections");
}

static int TestDropFalconInvalidatesCachedState(void)
{
    PlannedStmt pstmt = {.type = T_PlannedStmt};
    DropStmt dropStmt = {.type = T_DropStmt};
    String falconName = {.type = T_String, .sval = "falcon"};
    String otherName = {.type = T_String, .sval = "other_extension"};

    ResetHookFlagHarness();
    for (int i = 0; i < LAST_CACHED_RELATION_TYPE; ++i)
        CachedRelationOid[i] = (Oid)(100 + i);

    /*
     * A DROP statement for a different object should pass through without
     * invalidating Falcon's local caches.
     */
    dropStmt.objects = lappend(NIL, &otherName);
    pstmt.utilityStmt = (Node *)&dropStmt;
    falcon_ProcessUtility(&pstmt, "DROP EXTENSION other_extension", false, PROCESS_UTILITY_TOPLEVEL,
                          NULL, NULL, NULL, NULL);
    if (ExpectTrue(g_clear_dir_path_count == 0 && CachedRelationOid[0] != InvalidOid,
                   "non-Falcon drop leaves caches intact"))
        return 1;

    /*
     * Dropping the falcon extension is the hook's cache-invalidation trigger:
     * relation OIDs are cleared and all shared metadata caches are invalidated.
     */
    dropStmt.objects = lappend(NIL, &falconName);
    falcon_ProcessUtility(&pstmt, "DROP EXTENSION falcon", false, PROCESS_UTILITY_TOPLEVEL,
                          NULL, NULL, NULL, NULL);
    if (ExpectTrue(g_clear_dir_path_count == 1 && g_invalidate_foreign_count == 1 &&
                       g_invalidate_shard_count == 1,
                   "Falcon drop invalidates every cache"))
        return 1;
    for (int i = 0; i < LAST_CACHED_RELATION_TYPE; ++i) {
        if (ExpectTrue(CachedRelationOid[i] == InvalidOid, "cached relation OID is cleared"))
            return 1;
    }
    return 0;
}

int main(void)
{
    if (TestControlFlagSharedStateLifecycle())
        return 1;
    if (TestAbortProgressFlagTransitions())
        return 1;
    if (TestHookRegistrationAndCleanup())
        return 1;
    if (TestDropFalconInvalidatesCachedState())
        return 1;
    return 0;
}
