#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "postgres.h"
#include "access/genam.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "executor/executor.h"
#include "nodes/pg_list.h"
#include "storage/lwlock.h"
#include "storage/lock.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/shmem_control.h"

#include "distributed_backend/remote_comm_falcon.h"
#include "transaction/falcon_distributed_transaction.h"
#include "transaction/transaction_cleanup.h"
#include "utils/utils.h"

#ifdef vsnprintf
#undef vsnprintf
#endif
#ifdef snprintf
#undef snprintf
#endif
#ifdef vsprintf
#undef vsprintf
#endif
#ifdef vfprintf
#undef vfprintf
#endif

typedef struct FakeHash
{
    char keys[32][MAX_TRANSACTION_GID_LENGTH];
    bool used[32];
} FakeHash;

static char g_shmem[8192];
static bool g_shmem_found;
static int g_lwlock_acquire_count;
static int g_lwlock_release_count;
static int g_remote_plain_count;
static int g_remote_wait_count;
static int g_last_remote_server_id;
static char g_last_remote_command[256];
static Oid g_next_relation_oid;
Oid CachedRelationOid[LAST_CACHED_RELATION_TYPE];
MemoryContext TopMemoryContext = NULL;
MemoryContext CurrentMemoryContext = NULL;
ResourceOwner CurrentResourceOwner = NULL;
Oid MyDatabaseId = 1;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;
const char *FalconErrorCodeToString[LAST_FALCON_ERROR_CODE + 1];

static int ExpectTrue(bool condition, const char *message)
{
    if (!condition) {
        fprintf(stderr, "EXPECT FAILED: %s\n", message);
        return 1;
    }
    return 0;
}

static void ResetCleanupHarness(void)
{
    memset(g_shmem, 0, sizeof(g_shmem));
    g_shmem_found = false;
    g_lwlock_acquire_count = 0;
    g_lwlock_release_count = 0;
    g_remote_plain_count = 0;
    g_remote_wait_count = 0;
    g_last_remote_server_id = -1;
    g_last_remote_command[0] = '\0';
    g_next_relation_oid = 700;
    memset(CachedRelationOid, 0, sizeof(CachedRelationOid));
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

List *lappend_int(List *list, int datum)
{
    ListCell cell = {.int_value = datum};
    return AppendCell(list, T_IntList, cell);
}

List *list_make1_impl(NodeTag type, ListCell datum1)
{
    return AppendCell(NIL, type, datum1);
}

void *palloc(Size size)
{
    return calloc(1, size);
}

void pfree(void *pointer)
{
    free(pointer);
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
    return 71;
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

bool LWLockAcquire(LWLock *lock, LWLockMode mode)
{
    (void)lock;
    (void)mode;
    g_lwlock_acquire_count++;
    return true;
}

void LWLockRelease(LWLock *lock)
{
    (void)lock;
    g_lwlock_release_count++;
}

HTAB *ShmemInitHash(const char *name, long init_size, long max_size, HASHCTL *infoP, int hash_flags)
{
    (void)name;
    (void)init_size;
    (void)max_size;
    (void)infoP;
    (void)hash_flags;
    return (HTAB *)calloc(1, sizeof(FakeHash));
}

HTAB *hash_create(const char *tabname, long nelem, const HASHCTL *info, int flags)
{
    (void)tabname;
    (void)nelem;
    (void)info;
    (void)flags;
    return (HTAB *)calloc(1, sizeof(FakeHash));
}

void *hash_search(HTAB *hashp, const void *keyPtr, HASHACTION action, bool *foundPtr)
{
    FakeHash *hash = (FakeHash *)hashp;
    const char *key = (const char *)keyPtr;
    int freeIndex = -1;
    for (int i = 0; i < 32; ++i) {
        if (hash->used[i] && strcmp(hash->keys[i], key) == 0) {
            if (foundPtr)
                *foundPtr = true;
            if (action == HASH_REMOVE)
                hash->used[i] = false;
            return hash->keys[i];
        }
        if (!hash->used[i] && freeIndex < 0)
            freeIndex = i;
    }
    if (foundPtr)
        *foundPtr = false;
    if (action != HASH_ENTER || freeIndex < 0)
        return NULL;
    hash->used[freeIndex] = true;
    snprintf(hash->keys[freeIndex], sizeof(hash->keys[freeIndex]), "%s", key);
    return hash->keys[freeIndex];
}

void hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp)
{
    status->hashp = hashp;
    status->curBucket = 0;
    status->curEntry = NULL;
}

void *hash_seq_search(HASH_SEQ_STATUS *status)
{
    FakeHash *hash = (FakeHash *)status->hashp;
    while (status->curBucket < 32) {
        int index = status->curBucket++;
        if (hash->used[index])
            return hash->keys[index];
    }
    return NULL;
}

void hash_seq_term(HASH_SEQ_STATUS *status)
{
    (void)status;
}

MemoryContext AllocSetContextCreateInternal(MemoryContext parent,
                                            const char *name,
                                            Size minContextSize,
                                            Size initBlockSize,
                                            Size maxBlockSize)
{
    (void)parent;
    (void)name;
    (void)minContextSize;
    (void)initBlockSize;
    (void)maxBlockSize;
    return calloc(1, 1);
}

void MemoryContextDelete(MemoryContext context)
{
    free(context);
}

void MemoryContextReset(MemoryContext context)
{
    (void)context;
}

StringInfo makeStringInfo(void)
{
    StringInfo str = calloc(1, sizeof(StringInfoData));
    str->maxlen = 512;
    str->data = calloc(1, str->maxlen);
    return str;
}

void appendStringInfo(StringInfo str, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    int len = vsnprintf(str->data + str->len, str->maxlen - str->len, fmt, args);
    va_end(args);
    if (len > 0)
        str->len += len;
}

void GetRelationOid(const char *relationName, Oid *relationOid)
{
    (void)relationName;
    if (*relationOid == InvalidOid)
        *relationOid = g_next_relation_oid++;
}

int32_t GetLocalServerId(void)
{
    return 9;
}

bool CheckFalconBackgroundServiceStarted(void)
{
    return true;
}

Snapshot GetTransactionSnapshot(void)
{
    return (Snapshot)1;
}

void PushActiveSnapshot(Snapshot snapshot)
{
    (void)snapshot;
}

void PopActiveSnapshot(void) {}

int FalconPlainCommandOnWorkerList(const char *command, uint32_t commandFlag, List *targetWorkerIdList)
{
    (void)commandFlag;
    g_remote_plain_count++;
    g_last_remote_server_id = list_nth_int(targetWorkerIdList, 0);
    snprintf(g_last_remote_command, sizeof(g_last_remote_command), "%s", command);
    return 0;
}

MultipleServerRemoteCommandResult FalconSendCommandAndWaitForResult(void)
{
    g_remote_wait_count++;
    return NIL;
}

bool errstart(int elevel, const char *domain)
{
    (void)elevel;
    (void)domain;
    return true;
}

bool errstart_cold(int elevel, const char *domain)
{
    return errstart(elevel, domain);
}

void errfinish(const char *filename, int lineno, const char *funcname)
{
    (void)filename;
    (void)lineno;
    (void)funcname;
}

int errmsg(const char *fmt, ...)
{
    (void)fmt;
    return 0;
}

int errmsg_internal(const char *fmt, ...)
{
    (void)fmt;
    return 0;
}

void elog_start(const char *filename, int lineno, const char *funcname)
{
    (void)filename;
    (void)lineno;
    (void)funcname;
}

void elog_finish(int elevel, const char *fmt, ...)
{
    (void)elevel;
    (void)fmt;
}

int pg_vsnprintf(char *str, size_t count, const char *fmt, va_list args)
{
    return vsnprintf(str, count, fmt, args);
}

int pg_snprintf(char *str, size_t count, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    int ret = vsnprintf(str, count, fmt, args);
    va_end(args);
    return ret;
}

int pg_sprintf(char *str, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    int ret = vsprintf(str, fmt, args);
    va_end(args);
    return ret;
}

int pg_vsprintf(char *str, const char *fmt, va_list args)
{
    return vsprintf(str, fmt, args);
}

int pg_fprintf(FILE *stream, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    int ret = vfprintf(stream, fmt, args);
    va_end(args);
    return ret;
}

void ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
    (void)conditionName;
    (void)fileName;
    (void)lineNumber;
    abort();
}

/* Unreached PostgreSQL/database hooks needed by transaction_cleanup.c. */
pqsigfunc pqsignal(int signum, pqsigfunc handler)
{
    (void)signum;
    return handler;
}
void BackgroundWorkerUnblockSignals(void) {}
void BackgroundWorkerInitializeConnection(const char *dbname, const char *username, uint32 flags)
{
    (void)dbname;
    (void)username;
    (void)flags;
}
ResourceOwner ResourceOwnerCreate(ResourceOwner parent, const char *name)
{
    (void)parent;
    (void)name;
    return (ResourceOwner)calloc(1, 1);
}
void ResourceOwnerRelease(ResourceOwner owner, ResourceReleasePhase phase, bool isCommit, bool isTopLevel)
{
    (void)owner;
    (void)phase;
    (void)isCommit;
    (void)isTopLevel;
}
void ResourceOwnerDelete(ResourceOwner owner)
{
    free(owner);
}
void StartTransactionCommand(void) {}
void CommitTransactionCommand(void) {}
bool CheckFalconHasBeenLoaded(void)
{
    return true;
}
bool RecoveryInProgress(void)
{
    return false;
}
List *GetAllForeignServerId(bool exceptSelf, bool exceptCn)
{
    (void)exceptSelf;
    (void)exceptCn;
    return NIL;
}
LockAcquireResult LockAcquire(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock, bool dontWait)
{
    (void)locktag;
    (void)lockmode;
    (void)sessionLock;
    (void)dontWait;
    return LOCKACQUIRE_OK;
}
bool LockRelease(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock)
{
    (void)locktag;
    (void)lockmode;
    (void)sessionLock;
    return true;
}
void FlushErrorState(void) {}
void pg_re_throw(void)
{
    abort();
}
Relation table_open(Oid relationId, LOCKMODE lockmode)
{
    (void)relationId;
    (void)lockmode;
    return (Relation)1;
}
void table_close(Relation relation, LOCKMODE lockmode)
{
    (void)relation;
    (void)lockmode;
}
void ScanKeyInit(ScanKey entry, AttrNumber attributeNumber, StrategyNumber strategy,
                 RegProcedure procedure, Datum argument)
{
    (void)entry;
    (void)attributeNumber;
    (void)strategy;
    (void)procedure;
    (void)argument;
}
SysScanDesc systable_beginscan(Relation heapRelation, Oid indexId, bool indexOK,
                               Snapshot snapshot, int nkeys, ScanKey key)
{
    (void)heapRelation;
    (void)indexId;
    (void)indexOK;
    (void)snapshot;
    (void)nkeys;
    (void)key;
    return (SysScanDesc)1;
}
HeapTuple systable_getnext(SysScanDesc sysscan)
{
    (void)sysscan;
    return NULL;
}
void systable_endscan(SysScanDesc sysscan)
{
    (void)sysscan;
}

Datum nocachegetattr(HeapTuple tup, int attnum, TupleDesc tupleDesc)
{
    (void)tup;
    (void)attnum;
    (void)tupleDesc;
    return (Datum)0;
}

Datum getmissingattr(TupleDesc tupleDesc, int attnum, bool *isnull)
{
    (void)tupleDesc;
    (void)attnum;
    *isnull = true;
    return (Datum)0;
}

Datum heap_getsysattr(HeapTuple tup, int attnum, TupleDesc tupleDesc, bool *isnull)
{
    (void)tup;
    (void)attnum;
    (void)tupleDesc;
    *isnull = true;
    return (Datum)0;
}

text *cstring_to_text(const char *s)
{
    return (text *)s;
}

char *text_to_cstring(const text *t)
{
    return (char *)t;
}

void CatalogTupleInsert(Relation heapRel, HeapTuple tup)
{
    (void)heapRel;
    (void)tup;
}
void CatalogTupleDelete(Relation heapRel, ItemPointer tid)
{
    (void)heapRel;
    (void)tid;
}
PGresult *PQgetResult(PGconn *conn)
{
    (void)conn;
    return NULL;
}
int PQntuples(const PGresult *res)
{
    (void)res;
    return 0;
}
char *PQgetvalue(const PGresult *res, int tup_num, int field_num)
{
    (void)res;
    (void)tup_num;
    (void)field_num;
    return "";
}
HeapTuple heap_form_tuple(TupleDesc tupleDescriptor, const Datum *values, const bool *isnull)
{
    (void)tupleDescriptor;
    (void)values;
    (void)isnull;
    return (HeapTuple)1;
}

#include "../../falcon/transaction/falcon_distributed_transaction.c"
#include "../../falcon/transaction/transaction_cleanup.c"

static int TestCleanupShmemAndInprogressLifecycle(void)
{
    /* Cover fresh/existing shared-memory initialization and in-progress transaction hash updates. */
    ResetCleanupHarness();
    TransactionCleanupShmemInit();
    if (ExpectTrue(TransactionCleanupShmemsize() > sizeof(ShmemControlData),
                   "cleanup shmem size includes control block and hash entries"))
        return 1;

    AddInprogressTransaction("gid_cleanup");
    RemoveInprogressTransaction("gid_cleanup");
    if (ExpectTrue(g_lwlock_acquire_count == 2 && g_lwlock_release_count == 2,
                   "in-progress add/remove take and release the shmem lock"))
        return 1;

    g_shmem_found = true;
    TransactionCleanupShmemInit();
    return 0;
}

static int TestRelationOidCacheAndPreparedCommandHelpers(void)
{
    /* Cover relation OID cache lookups plus commit/rollback prepared-transaction command construction. */
    ResetCleanupHarness();
    Oid rel = FalconDistributedTransactionRelationId();
    Oid relAgain = FalconDistributedTransactionRelationId();
    Oid index = FalconDistributedTransactionRelationIndexId();
    if (ExpectTrue(rel == relAgain && index != InvalidOid && index != rel,
                   "distributed transaction relation oid helpers cache lookups"))
        return 1;

    if (ExpectTrue(falcon_transaction_cleanup_test(NULL) == Int16GetDatum(0),
                   "SQL cleanup test wrapper returns zero"))
        return 1;
    if (ExpectTrue(falcon_transaction_cleanup_trigger(NULL) == Int16GetDatum(0),
                   "SQL cleanup trigger handles an empty worker list"))
        return 1;

    CommitOrRollbackPreparedTransaction(11, "gid_commit", true);
    if (ExpectTrue(g_remote_plain_count == 1 && g_remote_wait_count == 1 &&
                       g_last_remote_server_id == 11 &&
                       strstr(g_last_remote_command, "COMMIT PREPARED 'gid_commit'") != NULL,
                   "commit helper sends commit prepared command"))
        return 1;

    CommitOrRollbackPreparedTransaction(12, "gid_rollback", false);
    return ExpectTrue(g_remote_plain_count == 2 && g_remote_wait_count == 2 &&
                          g_last_remote_server_id == 12 &&
                          strstr(g_last_remote_command, "ROLLBACK PREPARED 'gid_rollback'") != NULL,
                      "rollback helper sends rollback prepared command");
}

int main(void)
{
    if (TestCleanupShmemAndInprogressLifecycle())
        return 1;
    if (TestRelationOidCacheAndPreparedCommandHelpers())
        return 1;
    return 0;
}
