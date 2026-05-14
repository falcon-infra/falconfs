#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "postgres.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"

#include "metadb/shard_table.h"

#ifdef vsnprintf
#undef vsnprintf
#endif
#ifdef snprintf
#undef snprintf
#endif

const char *InodeTableName = "falcon_inode_table";
const char *XattrTableName = "falcon_xattr_table";
const char *SliceTableName = "falcon_slice_table";
const char *KvmetaTableName = "falcon_kvmeta_table";

static int g_spi_connect_result;
static int g_spi_execute_result;
static int g_spi_execute_count;
static int g_spi_finish_count;
static int32_t g_local_server_id;
static bool g_relation_exists;
static char g_last_command[8192];
static bool g_fail_kvmeta_command;

static void ResetDistributedHarness(void)
{
    g_spi_connect_result = SPI_OK_CONNECT;
    g_spi_execute_result = SPI_OK_UTILITY;
    g_spi_execute_count = 0;
    g_spi_finish_count = 0;
    g_local_server_id = 7;
    g_relation_exists = false;
    g_fail_kvmeta_command = false;
    g_last_command[0] = '\0';
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

StringInfo makeStringInfo(void)
{
    StringInfo str = calloc(1, sizeof(StringInfoData));
    str->maxlen = 8192;
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

void resetStringInfo(StringInfo str)
{
    str->len = 0;
    if (str->data != NULL)
        str->data[0] = '\0';
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
    if (g_fail_kvmeta_command && strstr(src, "CREATE KVMETA") != NULL)
        return SPI_ERROR_OPUNKNOWN;
    return g_spi_execute_result;
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

bool CheckIfRelationExists(const char *relationName, Oid relNamespace)
{
    (void)relationName;
    (void)relNamespace;
    return g_relation_exists;
}

void ConstructCreateInodeTableCommand(StringInfo command, const char *name)
{
    appendStringInfo(command, "CREATE INODE %s;", name);
}

void ConstructCreateXattrTableCommand(StringInfo command, const char *name)
{
    appendStringInfo(command, "CREATE XATTR %s;", name);
}

void ConstructCreateSliceTableCommand(StringInfo command, const char *name)
{
    appendStringInfo(command, "CREATE SLICE %s;", name);
}

void ConstructCreateKvmetaTableCommand(StringInfo command, const char *name)
{
    appendStringInfo(command, "CREATE KVMETA %s;", name);
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

#include "../../falcon/distributed_backend/distributed_backend_falcon.c"

static int TestCreateDistributedTablesFiltersLocalShards(void)
{
    ResetDistributedHarness();

    /* Mixed shard metadata should generate SQL only for the local server shard. */
    FalconCreateDistributedDataTable();
    if (ExpectTrue(g_spi_execute_count == 1, "distributed table creation executes one SPI command"))
        return 1;
    if (ExpectTrue(strstr(g_last_command, "CREATE INODE falcon_inode_table_11") != NULL,
                   "local inode shard table is created"))
        return 1;
    if (ExpectTrue(strstr(g_last_command, "CREATE XATTR falcon_xattr_table_11") != NULL,
                   "local xattr shard table is created"))
        return 1;
    return ExpectTrue(strstr(g_last_command, "falcon_inode_table_19") == NULL,
                      "remote shard table is skipped");
}

static int TestExistingRelationsAndLocalServerFilteringSkipSPI(void)
{
    ResetDistributedHarness();
    g_relation_exists = true;

    /*
     * Pretend the first local shard table already exists.  The production loop
     * should skip command construction entirely instead of issuing partial DDL.
     */
    FalconCreateDistributedDataTable();
    if (ExpectTrue(g_spi_execute_count == 0, "existing distributed data tables skip SPI"))
        return 1;

    ResetDistributedHarness();
    g_local_server_id = 99;
    /*
     * Move the local server id away from every shard returned by the harness.
     * This covers the filter branch where no local shard contributes SQL.
     */
    FalconCreateSliceTable();
    if (ExpectTrue(g_spi_execute_count == 0, "no local slice shard means no SPI command"))
        return 1;

    ResetDistributedHarness();
    g_relation_exists = true;
    /* Existing auxiliary relations should use the same early-skip path as data tables. */
    FalconCreateKvmetaTable();
    return ExpectTrue(g_spi_execute_count == 0, "existing kvmeta table skips SPI");
}

static int TestRangePointAndAuxiliaryTableCommands(void)
{
    ResetDistributedHarness();
    /*
     * Direct range-point helpers do not consult shard metadata, so a supplied
     * range point must appear in both inode and xattr table names.
     */
    FalconCreateDistributedDataTableByRangePoint(23);
    if (ExpectTrue(strstr(g_last_command, "falcon_inode_table_23") != NULL &&
                       strstr(g_last_command, "falcon_xattr_table_23") != NULL,
                   "range-point table creation uses the supplied shard id"))
        return 1;

    ResetDistributedHarness();
    /* Dropping a range point must detach the sharded tables from the extension before DROP TABLE. */
    FalconDropDistributedDataTableByRangePoint(29);
    if (ExpectTrue(strstr(g_last_command, "ALTER EXTENSION falcon DROP TABLE falcon_inode_table_29") != NULL &&
                       strstr(g_last_command, "DROP TABLE falcon_xattr_table_29") != NULL,
                   "range-point drop removes both inode and xattr tables"))
        return 1;

    ResetDistributedHarness();
    /* Slice table creation uses shard metadata and should keep only the local shard suffix. */
    FalconCreateSliceTable();
    if (ExpectTrue(strstr(g_last_command, "CREATE SLICE falcon_slice_table_11") != NULL,
                   "slice table creation targets the local shard"))
        return 1;

    ResetDistributedHarness();
    /* Kvmeta follows the same local-shard filtering rule but uses a different command builder. */
    FalconCreateKvmetaTable();
    return ExpectTrue(strstr(g_last_command, "CREATE KVMETA falcon_kvmeta_table_11") != NULL,
                      "kvmeta table creation targets the local shard");
}

static int TestSqlCallableWrappersDelegateToHelpers(void)
{
    LOCAL_FCINFO(fcinfo, 1);
    InitFunctionCallInfoData(*fcinfo, NULL, 1, InvalidOid, NULL, NULL);

    ResetDistributedHarness();
    /* SQL wrapper should delegate to local-shard table creation and return success. */
    if (ExpectTrue(falcon_create_distributed_data_table(NULL) == Int16GetDatum(SUCCESS),
                   "distributed table wrapper returns success"))
        return 1;
    if (ExpectTrue(strstr(g_last_command, "CREATE INODE falcon_inode_table_11") != NULL,
                   "distributed table wrapper builds local shard DDL"))
        return 1;

    ResetDistributedHarness();
    fcinfo->args[0].value = Int32GetDatum(41);
    fcinfo->args[0].isnull = false;
    /* Range-point create wrapper must pass its fmgr argument into the helper. */
    if (ExpectTrue(falcon_create_distributed_data_table_by_range_point(fcinfo) == Int16GetDatum(SUCCESS),
                   "range-point create wrapper returns success"))
        return 1;
    if (ExpectTrue(strstr(g_last_command, "falcon_inode_table_41") != NULL,
                   "range-point create wrapper uses fmgr argument"))
        return 1;

    ResetDistributedHarness();
    fcinfo->args[0].value = Int32GetDatum(43);
    /* Range-point drop wrapper must use the same fmgr argument extraction path. */
    if (ExpectTrue(falcon_drop_distributed_data_table_by_range_point(fcinfo) == Int16GetDatum(SUCCESS),
                   "range-point drop wrapper returns success"))
        return 1;
    if (ExpectTrue(strstr(g_last_command, "falcon_xattr_table_43") != NULL,
                   "range-point drop wrapper uses fmgr argument"))
        return 1;

    ResetDistributedHarness();
    if (ExpectTrue(falcon_create_slice_table(NULL) == Int16GetDatum(SUCCESS),
                   "slice wrapper returns success"))
        return 1;
    if (ExpectTrue(strstr(g_last_command, "CREATE SLICE falcon_slice_table_11") != NULL,
                   "slice wrapper delegates to helper"))
        return 1;

    ResetDistributedHarness();
    if (ExpectTrue(falcon_create_kvmeta_table(NULL) == Int16GetDatum(SUCCESS),
                   "kvmeta wrapper returns success"))
        return 1;
    if (ExpectTrue(strstr(g_last_command, "CREATE KVMETA falcon_kvmeta_table_11") != NULL,
                   "kvmeta wrapper delegates to helper"))
        return 1;

    ResetDistributedHarness();
    return ExpectTrue(falcon_prepare_commands(NULL) == Int16GetDatum(SUCCESS),
                      "prepare wrapper delegates to helper");
}

static int TestPrepareCommandsIsIdempotent(void)
{
    ResetDistributedHarness();

    /* The second call should return before SPI because the static prepared flag is set. */
    FalconPrepareCommands();
    if (ExpectTrue(g_spi_execute_count == 1 && strstr(g_last_command, "PREPARE cs_meta_call") != NULL,
                   "first prepare sends the prepared statement SQL"))
        return 1;
    FalconPrepareCommands();
    return ExpectTrue(g_spi_execute_count == 1, "second prepare call is a no-op");
}

static int TestSpiFailureBranchesContinueThroughHarness(void)
{
    ResetDistributedHarness();
    g_spi_connect_result = SPI_ERROR_CONNECT;

    /* The ereport shim lets this standalone test cover the SPI connect error path. */
    FalconCreateDistributedDataTableByRangePoint(31);
    if (ExpectTrue(g_spi_finish_count >= 1, "connect failure path finishes SPI before reporting"))
        return 1;

    ResetDistributedHarness();
    g_spi_execute_result = SPI_ERROR_OPUNKNOWN;
    /*
     * Force SPI_execute to fail after command construction.  The local ereport
     * shim records that cleanup happened without terminating the process.
     */
    FalconDropDistributedDataTableByRangePoint(37);
    if (ExpectTrue(g_spi_finish_count >= 1 && g_spi_execute_count == 1,
                   "SPI execution failure path reports after the utility command"))
        return 1;

    ResetDistributedHarness();
    g_spi_execute_result = SPI_ERROR_OPUNKNOWN;
    /* Repeat the SPI failure on an auxiliary-table path to cover its separate error branch. */
    FalconCreateSliceTable();
    if (ExpectTrue(g_spi_finish_count >= 1 && strstr(g_last_command, "CREATE SLICE") != NULL,
                   "slice table creation reports SPI utility failures"))
        return 1;

    ResetDistributedHarness();
    g_fail_kvmeta_command = true;
    /* Kvmeta has its own SPI failure branch after command construction. */
    FalconCreateKvmetaTable();
    return ExpectTrue(g_spi_finish_count >= 1 && strstr(g_last_command, "CREATE KVMETA") != NULL,
                      "kvmeta table creation reports SPI utility failures");
}

int main(void)
{
    if (TestCreateDistributedTablesFiltersLocalShards())
        return 1;
    if (TestExistingRelationsAndLocalServerFilteringSkipSPI())
        return 1;
    if (TestRangePointAndAuxiliaryTableCommands())
        return 1;
    if (TestSpiFailureBranchesContinueThroughHarness())
        return 1;
    if (TestPrepareCommandsIsIdempotent())
        return 1;
    if (TestSqlCallableWrappersDelegateToHelpers())
        return 1;
    return 0;
}
