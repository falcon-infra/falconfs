#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "postgres.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"

#include "metadb/foreign_server.h"
#include "remote_connection_utils/serialized_data.h"
#include "transaction/transaction.h"

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

typedef struct FakeConnState
{
    int32_t serverId;
    ForeignServerConnection connection;
    int results[64];
    int resultCount;
    int resultIndex;
    int sendCount;
    char lastCommand[256];
} FakeConnState;

static FakeConnState g_conn_states[8];
static int g_conn_count;
static int g_pqclear_count;
static int g_add_inprogress_count;
static int g_write_2pc_count;
static bool g_fail_pipeline_sync;
static int32_t g_null_conn_server_id;
MemoryContext TopMemoryContext = NULL;
MemoryContext CurrentMemoryContext = NULL;
char PreparedTransactionGid[MAX_TRANSACTION_GID_LENGTH + 1];
char RemoteTransactionGid[MAX_TRANSACTION_GID_LENGTH + 1];

static int ExpectTrue(bool condition, const char *message)
{
    if (!condition) {
        fprintf(stderr, "EXPECT FAILED: %s\n", message);
        return 1;
    }
    return 0;
}

static void ResetRemoteHarness(void)
{
    memset(g_conn_states, 0, sizeof(g_conn_states));
    g_conn_count = 0;
    g_pqclear_count = 0;
    g_add_inprogress_count = 0;
    g_write_2pc_count = 0;
    g_fail_pipeline_sync = false;
    g_null_conn_server_id = -1;
    PreparedTransactionGid[0] = '\0';
    RemoteTransactionGid[0] = '\0';
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

StringInfo makeStringInfo(void)
{
    StringInfo str = calloc(1, sizeof(StringInfoData));
    str->maxlen = 1024;
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

void *palloc(Size size)
{
    return calloc(1, size);
}

void pfree(void *pointer)
{
    free(pointer);
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

void MemoryContextReset(MemoryContext context)
{
    (void)context;
}

static FakeConnState *FindConnStateByServerId(int32_t serverId)
{
    for (int i = 0; i < g_conn_count; ++i) {
        if (g_conn_states[i].serverId == serverId)
            return &g_conn_states[i];
    }
    FakeConnState *state = &g_conn_states[g_conn_count++];
    state->serverId = serverId;
    state->connection.serverId = serverId;
    state->connection.conn = (PGconn *)(uintptr_t)(serverId + 1);
    state->connection.transactionState = FALCON_REMOTE_TRANSACTION_NONE;
    return state;
}

static FakeConnState *FindConnStateByConn(const PGconn *conn)
{
    uintptr_t id = (uintptr_t)conn;
    for (int i = 0; i < g_conn_count; ++i) {
        if ((uintptr_t)g_conn_states[i].connection.conn == id)
            return &g_conn_states[i];
    }
    return NULL;
}

static void QueueResult(FakeConnState *state, ExecStatusType status)
{
    state->results[state->resultCount++] = status;
    state->results[state->resultCount++] = -1;
}

List *GetForeignServerConnection(List *foreignServerIdList)
{
    List *connections = NIL;
    for (int i = 0; i < list_length(foreignServerIdList); ++i) {
        int32_t serverId = list_nth_int(foreignServerIdList, i);
        FakeConnState *state = FindConnStateByServerId(serverId);
        if (serverId == g_null_conn_server_id)
            state->connection.conn = NULL;
        connections = lappend(connections, &state->connection);
    }
    return connections;
}

int PQsendQueryParams(PGconn *conn, const char *command, int nParams, const Oid *paramTypes,
                      const char *const *paramValues, const int *paramLengths,
                      const int *paramFormats, int resultFormat)
{
    (void)nParams;
    (void)paramTypes;
    (void)paramValues;
    (void)paramLengths;
    (void)paramFormats;
    (void)resultFormat;
    FakeConnState *state = FindConnStateByConn(conn);
    state->sendCount++;
    snprintf(state->lastCommand, sizeof(state->lastCommand), "%s", command);
    QueueResult(state, strstr(command, "SELECT") ? PGRES_TUPLES_OK : PGRES_COMMAND_OK);
    return 1;
}

int PQsendQueryPrepared(PGconn *conn, const char *stmtName, int nParams, const char *const *paramValues,
                        const int *paramLengths, const int *paramFormats, int resultFormat)
{
    (void)stmtName;
    (void)nParams;
    (void)paramValues;
    (void)paramLengths;
    (void)paramFormats;
    (void)resultFormat;
    FakeConnState *state = FindConnStateByConn(conn);
    state->sendCount++;
    snprintf(state->lastCommand, sizeof(state->lastCommand), "prepared");
    QueueResult(state, PGRES_TUPLES_OK);
    return 1;
}

int PQpipelineSync(PGconn *conn)
{
    FakeConnState *state = FindConnStateByConn(conn);
    QueueResult(state, PGRES_PIPELINE_SYNC);
    return !g_fail_pipeline_sync;
}

PGresult *PQgetResult(PGconn *conn)
{
    FakeConnState *state = FindConnStateByConn(conn);
    if (state == NULL || state->resultIndex >= state->resultCount)
        return NULL;
    int status = state->results[state->resultIndex++];
    if (status < 0)
        return NULL;
    return (PGresult *)(uintptr_t)(status + 1);
}

ExecStatusType PQresultStatus(const PGresult *res)
{
    return (ExecStatusType)((uintptr_t)res - 1);
}

char *PQerrorMessage(const PGconn *conn)
{
    (void)conn;
    return "stub error";
}

char *PQresultErrorMessage(const PGresult *res)
{
    (void)res;
    return "stub result error";
}

void PQclear(PGresult *res)
{
    (void)res;
    g_pqclear_count++;
}

StringInfo GetImplicitTransactionGid(void)
{
    StringInfo gid = makeStringInfo();
    appendStringInfo(gid, "gid_test");
    return gid;
}

void AddInprogressTransaction(const char *transactionName)
{
    (void)transactionName;
    g_add_inprogress_count++;
}

void Write2PCRecord(int32 serverId, char *gid)
{
    (void)serverId;
    (void)gid;
    g_write_2pc_count++;
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

int errcode(int sqlerrcode)
{
    return sqlerrcode;
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

int pg_sprintf(char *str, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    int ret = vsprintf(str, fmt, args);
    va_end(args);
    return ret;
}

int pg_fprintf(FILE *stream, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    int ret = vfprintf(stream, fmt, args);
    va_end(args);
    return ret;
}

#include "../../falcon/distributed_backend/remote_comm_falcon.c"

typedef struct FakeRemoteHash
{
    int keys[16];
    RemoteConnectionCommandData entries[16];
    bool used[16];
} FakeRemoteHash;

HTAB *hash_create(const char *tabname, long nelem, const HASHCTL *info, int flags)
{
    (void)tabname;
    (void)nelem;
    (void)info;
    (void)flags;
    return (HTAB *)calloc(1, sizeof(FakeRemoteHash));
}

void *hash_search(HTAB *hashp, const void *keyPtr, HASHACTION action, bool *foundPtr)
{
    FakeRemoteHash *hash = (FakeRemoteHash *)hashp;
    int key = *(const int *)keyPtr;
    int freeIndex = -1;
    for (int i = 0; i < 16; ++i) {
        if (hash->used[i] && hash->keys[i] == key) {
            if (foundPtr)
                *foundPtr = true;
            return &hash->entries[i];
        }
        if (!hash->used[i] && freeIndex < 0)
            freeIndex = i;
    }
    if (foundPtr)
        *foundPtr = false;
    if (action != HASH_ENTER || freeIndex < 0)
        return NULL;
    hash->used[freeIndex] = true;
    hash->keys[freeIndex] = key;
    memset(&hash->entries[freeIndex], 0, sizeof(hash->entries[freeIndex]));
    return &hash->entries[freeIndex];
}

void hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp)
{
    status->hashp = hashp;
    status->curBucket = 0;
    status->curEntry = NULL;
}

void *hash_seq_search(HASH_SEQ_STATUS *status)
{
    FakeRemoteHash *hash = (FakeRemoteHash *)status->hashp;
    while (status->curBucket < 16) {
        int index = status->curBucket++;
        if (hash->used[index])
            return &hash->entries[index];
    }
    return NULL;
}

static int TestSendPrepareAndCommitRemoteCommands(void)
{
    ResetRemoteHarness();
    RegisterLocalProcessFlag(true);
    if (ExpectTrue(!IsLocalWrite(), "readonly local process does not mark local write"))
        return 1;
    RegisterLocalProcessFlag(false);
    if (ExpectTrue(IsLocalWrite(), "write local process marks local write"))
        return 1;

    List *writeWorker = list_make1_int(1);
    List *snapshotWorker = list_make1_int(2);
    char payload[] = "abc";
    SerializedData param = {.buffer = payload, .size = sizeof(payload), .capacity = sizeof(payload)};

    FalconPlainCommandOnWorkerList("SELECT 1", REMOTE_COMMAND_FLAG_WRITE, writeWorker);
    FalconMetaCallOnWorkerList(MKDIR, 2, param, REMOTE_COMMAND_FLAG_NEED_TRANSACTION_SNAPSHOT, snapshotWorker);
    MultipleServerRemoteCommandResult result = FalconSendCommandAndWaitForResult();
    if (ExpectTrue(list_length(result) == 2, "send collects results for both workers"))
        return 1;
    if (ExpectTrue(FindConnStateByServerId(1)->connection.transactionState ==
                       FALCON_REMOTE_TRANSACTION_BEGIN_FOR_WRITE,
                   "write command starts write transaction"))
        return 1;
    if (ExpectTrue(FindConnStateByServerId(2)->connection.transactionState ==
                       FALCON_REMOTE_TRANSACTION_BEGIN_FOR_SNAPSHOT,
                   "snapshot command starts snapshot transaction"))
        return 1;

    FalconRemoteCommandPrepare();
    if (ExpectTrue(strcmp(RemoteTransactionGid, "gid_test") == 0 && g_add_inprogress_count == 1 &&
                       g_write_2pc_count == 1,
                   "prepare promotes write worker to remote 2pc"))
        return 1;
    if (ExpectTrue(FindConnStateByServerId(1)->connection.transactionState ==
                       FALCON_REMOTE_TRANSACTION_PREPARE,
                   "write worker enters prepared state"))
        return 1;

    FalconRemoteCommandCommit();
    if (ExpectTrue(FindConnStateByServerId(1)->connection.transactionState ==
                       FALCON_REMOTE_TRANSACTION_NONE,
                   "commit clears prepared worker state"))
        return 1;
    if (ExpectTrue(FindConnStateByServerId(2)->connection.transactionState ==
                       FALCON_REMOTE_TRANSACTION_NONE,
                   "commit clears snapshot worker state"))
        return 1;

    ClearRemoteConnectionCommand();
    return ExpectTrue(g_pqclear_count > 0 && !IsLocalWrite() && RemoteTransactionGid[0] == '\0',
                      "clear releases marked results and resets transaction state");
}

static int TestAbortAndNoCommandPaths(void)
{
    ResetRemoteHarness();
    MarkPGresultToBeClearedLater(NULL);
    ClearRemoteConnectionCommand();
    FalconRemoteCommandPrepare();
    FalconRemoteCommandCommit();
    if (ExpectTrue(FalconRemoteCommandAbort(), "abort with no command succeeds"))
        return 1;

    List *workers = NIL;
    workers = lappend_int(workers, 3);
    workers = lappend_int(workers, 4);
    g_null_conn_server_id = 4;
    FalconPlainCommandOnWorkerList("UPDATE t SET x = 1", REMOTE_COMMAND_FLAG_WRITE, workers);
    FalconSendCommandAndWaitForResult();
    bool aborted = FalconRemoteCommandAbort();
    if (ExpectTrue(!aborted, "abort reports failed connection"))
        return 1;
    if (ExpectTrue(FindConnStateByServerId(3)->connection.transactionState ==
                       FALCON_REMOTE_TRANSACTION_NONE,
                   "abort clears live connection transaction state"))
        return 1;

    ClearRemoteConnectionCommand();
    return 0;
}

static int TestNoBeginPrepareNoopAndPreparedAbort(void)
{
    ResetRemoteHarness();
    List *worker = list_make1_int(5);
    char payload[] = "plain";
    SerializedData plainParam = {.buffer = payload, .size = sizeof(payload), .capacity = sizeof(payload)};
    FalconMetaCallOnWorkerList(PLAIN_COMMAND, 1, plainParam, REMOTE_COMMAND_FLAG_NO_BEGIN, NIL);

    g_fail_pipeline_sync = true;
    FalconPlainCommandOnWorkerList("SELECT 5", REMOTE_COMMAND_FLAG_NO_BEGIN, worker);
    MultipleServerRemoteCommandResult result = FalconSendCommandAndWaitForResult();
    if (ExpectTrue(list_length(result) == 1, "no-begin command still returns a server result"))
        return 1;
    if (ExpectTrue(FindConnStateByServerId(5)->connection.transactionState ==
                       FALCON_REMOTE_TRANSACTION_NONE,
                   "no-begin command leaves transaction state unchanged"))
        return 1;
    FalconRemoteCommandPrepare();
    if (ExpectTrue(RemoteTransactionGid[0] == '\0', "prepare is a no-op when 2pc is unnecessary"))
        return 1;
    FalconRemoteCommandCommit();
    ClearRemoteConnectionCommand();

    ResetRemoteHarness();
    FalconPlainCommandOnWorkerList("UPDATE t SET x = 7", REMOTE_COMMAND_FLAG_WRITE, list_make1_int(7));
    FakeConnState *dirty = FindConnStateByServerId(7);
    QueueResult(dirty, PGRES_COMMAND_OK);
    FalconRemoteCommandCommit();
    ClearRemoteConnectionCommand();

    ResetRemoteHarness();
    RegisterLocalProcessFlag(false);
    FalconPlainCommandOnWorkerList("UPDATE t SET x = 6", REMOTE_COMMAND_FLAG_WRITE, list_make1_int(6));
    FalconSendCommandAndWaitForResult();
    FalconRemoteCommandPrepare();
    if (ExpectTrue(FindConnStateByServerId(6)->connection.transactionState ==
                       FALCON_REMOTE_TRANSACTION_PREPARE,
                   "write worker is prepared before abort"))
        return 1;
    if (ExpectTrue(FalconRemoteCommandAbort(), "prepared abort succeeds"))
        return 1;
    if (ExpectTrue(FindConnStateByServerId(6)->connection.transactionState ==
                       FALCON_REMOTE_TRANSACTION_NONE,
                   "prepared abort clears transaction state"))
        return 1;
    ClearRemoteConnectionCommand();
    return 0;
}

int main(void)
{
    if (TestSendPrepareAndCommitRemoteCommands())
        return 1;
    if (TestAbortAndNoCommandPaths())
        return 1;
    if (TestNoBeginPrepareNoopAndPreparedAbort())
        return 1;
    return 0;
}
