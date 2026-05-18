#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "postgres.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"

#include "metadb/foreign_server.h"
#include "metadb/shard_table.h"
#include "distributed_backend/remote_comm_falcon.h"
#include "utils/utils.h"

#ifdef vsnprintf
#undef vsnprintf
#endif
#ifdef snprintf
#undef snprintf
#endif

static int g_spi_connect_result;
static int g_spi_execute_count;
static int g_last_spi_result;
static int g_spi_finish_count;
static int g_clear_dir_path_count;
static int g_invalidate_foreign_count;
static int g_invalidate_shard_count;
static int g_pooler_count;
static int32_t g_local_server_id;
static int g_plain_command_count;
static int g_pq_get_result_count;
static int g_search_range_point;
static int g_search_server_id;
static bool g_copy_data_once;
static char g_last_command[4096];
static char g_last_plain_command[4096];

uint64 SPI_processed;
SPITupleTable *SPI_tuptable;
Oid CachedRelationOid[LAST_CACHED_RELATION_TYPE];

static void ResetControlHarness(void)
{
    g_spi_connect_result = SPI_OK_CONNECT;
    g_spi_execute_count = 0;
    g_last_spi_result = SPI_OK_UTILITY;
    g_spi_finish_count = 0;
    g_clear_dir_path_count = 0;
    g_invalidate_foreign_count = 0;
    g_invalidate_shard_count = 0;
    g_pooler_count = 0;
    g_local_server_id = 7;
    g_plain_command_count = 0;
    g_pq_get_result_count = 0;
    g_search_range_point = 0;
    g_search_server_id = 1;
    g_copy_data_once = false;
    g_last_command[0] = '\0';
    g_last_plain_command[0] = '\0';
    SPI_processed = 0;
    SPI_tuptable = NULL;
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

List *lappend_int(List *list, int datum)
{
    ListCell cell = {.int_value = datum};
    return AppendCell(list, T_IntList, cell);
}

List *lappend_oid(List *list, Oid datum)
{
    ListCell cell = {.oid_value = datum};
    return AppendCell(list, T_OidList, cell);
}

List *list_make1_impl(NodeTag type, ListCell datum1)
{
    return AppendCell(NIL, type, datum1);
}

List *list_make2_impl(NodeTag type, ListCell datum1, ListCell datum2)
{
    return AppendCell(AppendCell(NIL, type, datum1), type, datum2);
}

StringInfo makeStringInfo(void)
{
    StringInfo str = calloc(1, sizeof(StringInfoData));
    str->maxlen = 4096;
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

int SPI_connect(void)
{
    return g_spi_connect_result;
}

int SPI_finish(void)
{
    g_spi_finish_count++;
    return 0;
}

int SPI_execute(const char *src, bool read_only, long tcount)
{
    (void)read_only;
    (void)tcount;
    g_spi_execute_count++;
    snprintf(g_last_command, sizeof(g_last_command), "%s", src);

    if (strstr(src, "SELECT tablename FROM pg_tables") != NULL)
        return SPI_OK_SELECT;
    return g_last_spi_result;
}

Datum SPI_getbinval(HeapTuple tuple, TupleDesc tupdesc, int fnumber, bool *isnull)
{
    (void)tupdesc;
    (void)fnumber;
    *isnull = false;
    return CStringGetDatum((char *)tuple);
}

List *GetShardTableData(void)
{
    static FormData_falcon_shard_table localShard = {.range_point = 11, .server_id = 7};
    static FormData_falcon_shard_table remoteShard = {.range_point = 19, .server_id = 8};

    List *data = NIL;
    data = lappend(data, &localShard);
    data = lappend(data, &remoteShard);
    return data;
}

int32_t GetLocalServerId(void)
{
    return g_local_server_id;
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

void RunConnectionPoolServer(void)
{
    g_pooler_count++;
}

void SearchShardInfoByHashValue(int32_t hashValue, int32_t *rangePoint, int32_t *serverId)
{
    *rangePoint = hashValue + g_search_range_point;
    *serverId = g_search_server_id;
}

List *GetAllForeignServerId(bool exceptSelf, bool exceptCn)
{
    (void)exceptSelf;
    (void)exceptCn;
    return NIL;
}

List *GetForeignServerConnection(List *foreignServerIdList)
{
    (void)foreignServerIdList;
    /*
     * falcon_move_shard expects source and target connection objects in list
     * order.  The pointers only need stable identities because libpq is stubbed.
     */
    static ForeignServerConnection sourceConn;
    static ForeignServerConnection targetConn;
    static int source;
    static int target;

    sourceConn.serverId = 1;
    sourceConn.conn = (PGconn *)&source;
    targetConn.serverId = 2;
    targetConn.conn = (PGconn *)&target;

    List *connections = NIL;
    connections = lappend(connections, &sourceConn);
    connections = lappend(connections, &targetConn);
    return connections;
}

int FalconPlainCommandOnWorkerList(const char *command, uint32_t commandFlag, List *targetWorkerIdList)
{
    (void)commandFlag;
    (void)targetWorkerIdList;
    g_plain_command_count++;
    snprintf(g_last_plain_command, sizeof(g_last_plain_command), "%s", command);
    return 0;
}

MultipleServerRemoteCommandResult FalconSendCommandAndWaitForResult(void)
{
    return NIL;
}

int PQsendQueryParams(PGconn *conn, const char *command, int nParams, const Oid *paramTypes,
                      const char *const *paramValues, const int *paramLengths,
                      const int *paramFormats, int resultFormat)
{
    (void)conn;
    (void)command;
    (void)nParams;
    (void)paramTypes;
    (void)paramValues;
    (void)paramLengths;
    (void)paramFormats;
    (void)resultFormat;
    return 1;
}

int PQpipelineSync(PGconn *conn)
{
    (void)conn;
    return 1;
}

PGresult *PQgetResult(PGconn *conn)
{
    (void)conn;
    g_pq_get_result_count++;
    /*
     * The fifth libpq result is the expected NULL separator before the final
     * pipeline-sync result; other calls use their ordinal as a fake PGresult.
     */
    if (g_pq_get_result_count == 5)
        return NULL;
    return (PGresult *)(uintptr_t)g_pq_get_result_count;
}

ExecStatusType PQresultStatus(const PGresult *res)
{
    uintptr_t step = (uintptr_t)res;
    /* Map the fake PGresult ordinals to the states falcon_move_shard validates. */
    if (step == 1)
        return PGRES_COPY_OUT;
    if (step == 2)
        return PGRES_COPY_IN;
    if (step == 6)
        return PGRES_PIPELINE_SYNC;
    return PGRES_COMMAND_OK;
}

void PQclear(PGresult *res)
{
    (void)res;
}

int PQgetCopyData(PGconn *conn, char **buffer, int async)
{
    (void)conn;
    (void)async;
    if (g_copy_data_once) {
        g_copy_data_once = false;
        *buffer = strdup("row");
        return 3;
    }
    return -1;
}

int PQputCopyData(PGconn *conn, const char *buffer, int nbytes)
{
    (void)conn;
    (void)buffer;
    (void)nbytes;
    return 1;
}

void PQfreemem(void *ptr)
{
    free(ptr);
}

int PQputCopyEnd(PGconn *conn, const char *errormsg)
{
    (void)conn;
    (void)errormsg;
    return 1;
}

char *PQerrorMessage(const PGconn *conn)
{
    (void)conn;
    return "stub";
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

void ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
    (void)conditionName;
    (void)fileName;
    (void)lineNumber;
    abort();
}

#include "../../falcon/control/control_func.c"

static int TestClearUserDataBuildsLocalShardCommand(void)
{
    ResetControlHarness();

    /* Use mixed local/remote shard metadata to verify only local shard tables are appended. */
    falcon_clear_user_data_func(NULL);
    if (ExpectTrue(g_spi_execute_count == 1, "clear user data executes one utility command"))
        return 1;
    if (ExpectTrue(strstr(g_last_command, "falcon_inode_table_11") != NULL,
                   "local shard inode table is truncated"))
        return 1;
    if (ExpectTrue(strstr(g_last_command, "falcon_xattr_table_11") != NULL,
                   "local shard xattr table is truncated"))
        return 1;
    if (ExpectTrue(strstr(g_last_command, "falcon_inode_table_19") == NULL,
                   "remote shard tables are skipped"))
        return 1;
    return ExpectTrue(g_clear_dir_path_count == 1 && g_spi_finish_count == 1,
                      "successful clear resets directory cache and finishes SPI");
}

static int TestClearAllDataDropsShardTablesAndInvalidatesCaches(void)
{
    /* Feed the SPI scan with sharded and non-sharded Falcon table names. */
    HeapTuple rows[] = {
        (HeapTuple)"falcon_inode_table_11",
        (HeapTuple)"falcon_xattr_table_11",
        (HeapTuple)"falcon_directory_table",
    };
    SPITupleTable table = {.tupdesc = NULL, .vals = rows, .numvals = 3};

    ResetControlHarness();
    SPI_processed = 3;
    SPI_tuptable = &table;

    /* The second SPI command should drop sharded tables and truncate the remaining table. */
    falcon_clear_all_data_func(NULL);
    if (ExpectTrue(g_spi_execute_count == 2, "clear all data selects tables then executes cleanup"))
        return 1;
    if (ExpectTrue(strstr(g_last_command, "ALTER EXTENSION falcon DROP TABLE falcon_inode_table_11") != NULL,
                   "sharded inode table is dropped before removal"))
        return 1;
    if (ExpectTrue(strstr(g_last_command, "ALTER EXTENSION falcon DROP TABLE falcon_xattr_table_11") != NULL,
                   "sharded xattr table is dropped before removal"))
        return 1;
    if (ExpectTrue(strstr(g_last_command, "TRUNCATE TABLE falcon_directory_table") != NULL,
                   "non-sharded falcon table is truncated"))
        return 1;
    return ExpectTrue(g_invalidate_foreign_count == 1 && g_invalidate_shard_count == 1 &&
                          g_clear_dir_path_count == 1,
                      "successful clear invalidates all metadata caches");
}

static int TestSmallControlFunctions(void)
{
    ResetControlHarness();

    /* Seed cached relation OIDs so the clear function must reset every slot. */
    for (int i = 0; i < LAST_CACHED_RELATION_TYPE; ++i)
        CachedRelationOid[i] = (Oid)(100 + i);

    falcon_clear_cached_relation_oid_func(NULL);
    for (int i = 0; i < LAST_CACHED_RELATION_TYPE; ++i) {
        if (ExpectTrue(CachedRelationOid[i] == InvalidOid, "cached relation oid entry is cleared"))
            return 1;
    }

    /* Exercise the SQL-callable wrapper without starting a real pooler process. */
    falcon_run_pooler_server_func(NULL);
    return ExpectTrue(g_pooler_count == 1, "pooler server startup is delegated");
}

static int TestMoveShardRunsCopyPipelineAndDropsSourceTable(void)
{
    ResetControlHarness();
    g_local_server_id = FALCON_CN_SERVER_ID;
    g_copy_data_once = true;

    /*
     * Build a minimal fmgr call frame because falcon_move_shard reads its range
     * point and target server through PG_GETARG_INT32 instead of normal C args.
     */
    LOCAL_FCINFO(fcinfo, 2);
    InitFunctionCallInfoData(*fcinfo, NULL, 2, InvalidOid, NULL, NULL);
    fcinfo->args[0].value = Int32GetDatum(11);
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = Int32GetDatum(2);
    fcinfo->args[1].isnull = false;

    /*
     * The libpq stubs walk through COPY_OUT, COPY_IN, COMMAND_OK, NULL,
     * and PIPELINE_SYNC so the shard move reaches its source cleanup command.
     */
    falcon_move_shard(fcinfo);
    if (ExpectTrue(g_plain_command_count == 3, "move shard sends update, create, and drop commands"))
        return 1;
    if (ExpectTrue(strstr(g_last_plain_command,
                          "falcon_drop_distributed_data_table_by_range_point(11)") != NULL,
                   "source shard table is dropped after copy finishes"))
        return 1;
    return ExpectTrue(g_pq_get_result_count == 6, "copy pipeline consumes all expected libpq results");
}

static int TestControlErrorBranchesReturnThroughHarness(void)
{
    ResetControlHarness();
    g_spi_connect_result = SPI_ERROR_CONNECT;
    falcon_clear_user_data_func(NULL);
    if (ExpectTrue(g_spi_finish_count == 2, "clear user data connect failure calls finish and returns"))
        return 1;

    ResetControlHarness();
    g_last_spi_result = SPI_ERROR_OPUNKNOWN;
    falcon_clear_user_data_func(NULL);
    if (ExpectTrue(g_spi_finish_count == 2 && g_clear_dir_path_count == 1,
                   "clear user data SPI failure path still reaches cleanup harness"))
        return 1;

    ResetControlHarness();
    g_last_spi_result = SPI_ERROR_OPUNKNOWN;
    falcon_clear_all_data_func(NULL);
    if (ExpectTrue(g_spi_execute_count == 2 && g_spi_finish_count == 2,
                   "clear all data utility failure branch is covered"))
        return 1;

    ResetControlHarness();
    g_local_server_id = FALCON_CN_SERVER_ID;
    g_search_range_point = 1;
    LOCAL_FCINFO(fcinfo, 2);
    InitFunctionCallInfoData(*fcinfo, NULL, 2, InvalidOid, NULL, NULL);
    fcinfo->args[0].value = Int32GetDatum(11);
    fcinfo->args[0].isnull = false;
    fcinfo->args[1].value = Int32GetDatum(2);
    fcinfo->args[1].isnull = false;
    falcon_move_shard(fcinfo);
    if (ExpectTrue(g_plain_command_count == 3, "range mismatch branch returns through error shim"))
        return 1;

    ResetControlHarness();
    g_local_server_id = FALCON_CN_SERVER_ID;
    g_search_server_id = 2;
    falcon_move_shard(fcinfo);
    return ExpectTrue(g_plain_command_count == 3, "same source and target branch returns through error shim");
}

int main(void)
{
    if (TestClearUserDataBuildsLocalShardCommand())
        return 1;
    if (TestClearAllDataDropsShardTablesAndInvalidatesCaches())
        return 1;
    if (TestSmallControlFunctions())
        return 1;
    if (TestMoveShardRunsCopyPipelineAndDropsSourceTable())
        return 1;
    if (TestControlErrorBranchesReturnThroughHarness())
        return 1;
    return 0;
}
