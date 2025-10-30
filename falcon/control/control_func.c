/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "postgres.h"

#include "executor/spi.h"
#include "fmgr.h"
#include "nodes/pg_list.h"

#include "connection_pool/brpc_server.h"
#include "dir_path_shmem/dir_path_hash.h"
#include "distributed_backend/remote_comm_falcon.h"
#include "metadb/foreign_server.h"
#include "metadb/meta_process_info.h"
#include "metadb/shard_table.h"
#include "utils/error_log.h"
#include "utils/utils.h"

PG_FUNCTION_INFO_V1(falcon_clear_user_data_func);
PG_FUNCTION_INFO_V1(falcon_clear_all_data_func);
PG_FUNCTION_INFO_V1(falcon_clear_cached_relation_oid_func);
PG_FUNCTION_INFO_V1(falcon_run_pooler_server_func);
PG_FUNCTION_INFO_V1(falcon_move_shard);

Datum falcon_clear_user_data_func(PG_FUNCTION_ARGS)
{
    int spiConnectionResult = SPI_connect();
    if (spiConnectionResult != SPI_OK_CONNECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "could not connect to SPI manager.");
    }

    StringInfo command = makeStringInfo();
    List *shardTableData = GetShardTableData();
    for (int i = 0; i < list_length(shardTableData); ++i) {
        FormData_falcon_shard_table *data = list_nth(shardTableData, i);
        if (data->server_id != GetLocalServerId())
            continue;

        appendStringInfo(command, "TRUNCATE TABLE falcon_inode_table_" INT32_PRINT_SYMBOL ";", data->range_point);
        appendStringInfo(command, "TRUNCATE TABLE falcon_xattr_table_" INT32_PRINT_SYMBOL ";", data->range_point);
    }
    appendStringInfo(command, "TRUNCATE TABLE falcon_directory_table;");

    int spiQueryResult = SPI_execute(command->data, false, 0);
    if (spiQueryResult != SPI_OK_UTILITY) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "spi exec failed.");
    }
    ClearDirPathHash();

    SPI_finish();
    PG_RETURN_INT16(0);
}

Datum falcon_clear_all_data_func(PG_FUNCTION_ARGS)
{
    int spiConnectionResult = SPI_connect();
    if (spiConnectionResult != SPI_OK_CONNECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "could not connect to SPI manager.");
    }

    int spiQueryResult = SPI_execute("SELECT tablename FROM pg_tables WHERE tablename LIKE 'falcon%';", true, 0);
    if (spiQueryResult != SPI_OK_SELECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "spi exec failed.");
    }

    List *falconTableName = NIL;
    for (int i = 0; i < SPI_processed; ++i) {
        bool isNull;
        char *name = DatumGetCString(SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1, &isNull));
        falconTableName = lappend(falconTableName, name);
    }

    StringInfo command = makeStringInfo();
    for (int i = 0; i < list_length(falconTableName); ++i) {
        char *name = (char *)list_nth(falconTableName, i);

        if (strstr(name, "falcon_inode_table_") != NULL || strstr(name, "falcon_xattr_table_") != NULL) {
            appendStringInfo(command, "ALTER EXTENSION falcon DROP TABLE %s; DROP TABLE %s;", name, name);
        } else {
            appendStringInfo(command, "TRUNCATE TABLE %s;", name);
        }
    }
    appendStringInfo(command, "ALTER SEQUENCE pg_dfs_inodeid_seq RESTART;");

    spiQueryResult = SPI_execute(command->data, false, 0);
    if (spiQueryResult != SPI_OK_UTILITY) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "spi exec failed.");
    }
    InvalidateForeignServerShmemCache();
    InvalidateShardTableShmemCache();
    ClearDirPathHash();

    SPI_finish();
    PG_RETURN_INT16(0);
}

Datum falcon_clear_cached_relation_oid_func(PG_FUNCTION_ARGS)
{
    memset(CachedRelationOid, 0, sizeof(CachedRelationOid));

    PG_RETURN_INT16(0);
}

Datum falcon_run_pooler_server_func(PG_FUNCTION_ARGS)
{
    PG_RunConnectionPoolBrpcServer();
    PG_RETURN_INT16(0);
}

Datum falcon_move_shard(PG_FUNCTION_ARGS)
{
    // TODO: Need move xattr_table too, but this table is not used currently.

    int32_t rangePoint = PG_GETARG_INT32(0);
    int32_t targetServerId = PG_GETARG_INT32(1);

    int32_t rangePointCheck, sourceServerId;
    SearchShardInfoByHashValue(rangePoint, &rangePointCheck, &sourceServerId);
    if (rangePoint != rangePointCheck)
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "No shard matches input range point.");
    if (FALCON_CN_SERVER_ID != GetLocalServerId())
        FALCON_ELOG_ERROR(WRONG_WORKER, "falcon_move_shard can only be called on CN.");
    if (sourceServerId == targetServerId)
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "Target server is the same with source server.");

    List* foreignServerIdList = GetAllForeignServerId(false, false);
    
    StringInfo updateShardTableCommand = makeStringInfo();
    appendStringInfo(updateShardTableCommand, 
        "SELECT falcon_update_shard_table(ARRAY[%d], ARRAY[%d]);",
        rangePoint, targetServerId);
    FalconPlainCommandOnWorkerList(updateShardTableCommand->data,
        REMOTE_COMMAND_FLAG_WRITE, foreignServerIdList);
    
    StringInfo createNewTableCommand = makeStringInfo();
    appendStringInfo(createNewTableCommand, 
        "SELECT falcon_create_distributed_data_table_by_range_point(%d);",
        rangePoint);
    FalconPlainCommandOnWorkerList(createNewTableCommand->data,
        REMOTE_COMMAND_FLAG_WRITE, list_make1_int(targetServerId));
    
    FalconSendCommandAndWaitForResult();

    List* foreignServerConnList = GetForeignServerConnection(list_make2_int(sourceServerId, targetServerId));
    ForeignServerConnection* sourceServerConn = list_nth(foreignServerConnList, 0);
    ForeignServerConnection* targetServerConn = list_nth(foreignServerConnList, 1);

    StringInfo sourceCommand = makeStringInfo();
    appendStringInfo(sourceCommand, "COPY falcon_inode_table_%d TO STDOUT (FORMAT BINARY);",
        rangePoint);
    if (PQsendQueryParams(sourceServerConn->conn, sourceCommand->data,
            0, NULL, NULL, NULL, NULL, 1) != 1)
        FALCON_ELOG_ERROR(REMOTE_QUERY_FAILED, "Send COPY TO failed.");

    StringInfo targetCommand = makeStringInfo();
    appendStringInfo(targetCommand, "COPY falcon_inode_table_%d FROM STDIN (FORMAT BINARY);",
        rangePoint);
    if (PQsendQueryParams(targetServerConn->conn, targetCommand->data,
        0, NULL, NULL, NULL, NULL, 1) != 1) 
        FALCON_ELOG_ERROR(REMOTE_QUERY_FAILED, "Send COPY TO failed.");
    
    if (!PQpipelineSync(sourceServerConn->conn))
        FALCON_ELOG_ERROR_EXTENDED(REMOTE_QUERY_FAILED, "Source PQpipelineSync failed. ErrorMsg: %s",
            PQerrorMessage(sourceServerConn->conn));
    if (!PQpipelineSync(targetServerConn->conn))
        FALCON_ELOG_ERROR_EXTENDED(REMOTE_QUERY_FAILED, "Target PQpipelineSync failed. ErrorMsg: %s",
            PQerrorMessage(targetServerConn->conn));
    
    PGresult *sourceRes = PQgetResult(sourceServerConn->conn);
    if (PQresultStatus(sourceRes) != PGRES_COPY_OUT)
        FALCON_ELOG_ERROR_EXTENDED(REMOTE_QUERY_FAILED, "Source not in COPY_OUT state: %s", 
            PQerrorMessage(sourceServerConn->conn));
    PQclear(sourceRes);


    PGresult *targetRes = PQgetResult(targetServerConn->conn);
    if (PQresultStatus(targetRes) != PGRES_COPY_IN)
        FALCON_ELOG_ERROR_EXTENDED(REMOTE_QUERY_FAILED, "Target not in COPY_IN state: %s", 
            PQerrorMessage(targetServerConn->conn));
    PQclear(targetRes);
    
    while (true) 
    {
        char* buffer;
        int size = PQgetCopyData(sourceServerConn->conn, &buffer, 0);
        if (size == -2)
        {
            char* errorMsg = PQerrorMessage(sourceServerConn->conn);
            FALCON_ELOG_ERROR_EXTENDED(REMOTE_QUERY_FAILED, "PQgetCopyData failed. ErrorMsg: %s",
                errorMsg);
        }
        else if (size == -1)
        {
            break;
        }
        
        int ret = PQputCopyData(targetServerConn->conn, buffer, size);
        PQfreemem(buffer);
        if (ret != 1)
        {
            char* errorMsg = PQerrorMessage(targetServerConn->conn);
            FALCON_ELOG_ERROR_EXTENDED(REMOTE_QUERY_FAILED, "PQputCopyData failed. ErrorMsg: %s",
                errorMsg);
        }    
    }
    
    PGresult* res = PQgetResult(sourceServerConn->conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        FALCON_ELOG_ERROR(REMOTE_QUERY_FAILED, "copy from failed.");
    
    int ret = PQputCopyEnd(targetServerConn->conn, NULL);
    if (ret != 1)
        FALCON_ELOG_ERROR(REMOTE_QUERY_FAILED, "PQputCopyEnd(1) failed.");
    res = PQgetResult(targetServerConn->conn);
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
        FALCON_ELOG_ERROR(REMOTE_QUERY_FAILED, "PQputCopyEnd(2) failed.");
    res = PQgetResult(targetServerConn->conn);
    if (res != NULL)
        FALCON_ELOG_ERROR(REMOTE_QUERY_FAILED, "PQputCopyEnd(3) failed.");
    res = PQgetResult(targetServerConn->conn);
    if (PQresultStatus(res) != PGRES_PIPELINE_SYNC)
        FALCON_ELOG_ERROR(REMOTE_QUERY_FAILED, "PQputCopyEnd(4) failed.");

    StringInfo dropSourceCommand = makeStringInfo();
    appendStringInfo(dropSourceCommand, "SELECT falcon_drop_distributed_data_table_by_range_point(%d);",
        rangePoint);
    FalconPlainCommandOnWorkerList(dropSourceCommand->data, 
        REMOTE_COMMAND_FLAG_WRITE, list_make1_int(sourceServerId));
    FalconSendCommandAndWaitForResult();

    PG_RETURN_INT16(0);
}