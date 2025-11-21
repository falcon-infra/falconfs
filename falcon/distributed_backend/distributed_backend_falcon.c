/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "distributed_backend/distributed_backend_falcon.h"

#include "executor/spi.h"

#include "metadb/inode_table.h"
#include "metadb/shard_table.h"
#include "metadb/xattr_table.h"
#include "utils/error_log.h"
#include "utils/utils.h"

PG_FUNCTION_INFO_V1(falcon_create_distributed_data_table);
PG_FUNCTION_INFO_V1(falcon_create_distributed_data_table_by_range_point);
PG_FUNCTION_INFO_V1(falcon_drop_distributed_data_table_by_range_point);
PG_FUNCTION_INFO_V1(falcon_prepare_commands);

Datum falcon_create_distributed_data_table(PG_FUNCTION_ARGS)
{
    FalconCreateDistributedDataTable();

    PG_RETURN_INT16(SUCCESS);
}

Datum falcon_create_distributed_data_table_by_range_point(PG_FUNCTION_ARGS)
{
    int rangePoint = PG_GETARG_INT32(0);

    FalconCreateDistributedDataTableByRangePoint(rangePoint);

    PG_RETURN_INT16(SUCCESS);
}

Datum falcon_drop_distributed_data_table_by_range_point(PG_FUNCTION_ARGS)
{
    int rangePoint = PG_GETARG_INT32(0);

    FalconDropDistributedDataTableByRangePoint(rangePoint);

    PG_RETURN_INT16(SUCCESS);
}

Datum falcon_prepare_commands(PG_FUNCTION_ARGS)
{
    FalconPrepareCommands();

    PG_RETURN_INT16(SUCCESS);
}

void FalconCreateDistributedDataTable()
{
    List *shardTableData = GetShardTableData();

    StringInfo toExecCommand = makeStringInfo();
    StringInfo name = makeStringInfo();
    for (int i = 0; i < list_length(shardTableData); ++i) {
        Form_falcon_shard_table data = list_nth(shardTableData, i);
        if (data->server_id != GetLocalServerId())
            continue;

        resetStringInfo(name);
        appendStringInfo(name, "%s_%d", InodeTableName, data->range_point);
        if (CheckIfRelationExists(name->data, PG_CATALOG_NAMESPACE)) //
            continue;
        ConstructCreateInodeTableCommand(toExecCommand, name->data);

        resetStringInfo(name);
        appendStringInfo(name, "%s_%d", XattrTableName, data->range_point);
        ConstructCreateXattrTableCommand(toExecCommand, name->data);
    }
    if (toExecCommand->len == 0)
        return;

    int spiConnectionResult = SPI_connect();
    if (spiConnectionResult != SPI_OK_CONNECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "could not connect to SPI manager.");
    }

    int spiQueryResult = SPI_execute(toExecCommand->data, false, 0);
    if (spiQueryResult != SPI_OK_UTILITY) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "spi exec failed.");
    }
    SPI_finish();
}

void FalconCreateDistributedDataTableByRangePoint(int rangePoint)
{
    StringInfo toExecCommand = makeStringInfo();
    StringInfo name = makeStringInfo();
    appendStringInfo(name, "%s_%d", InodeTableName, rangePoint);
    ConstructCreateInodeTableCommand(toExecCommand, name->data);
    resetStringInfo(name);
    appendStringInfo(name, "%s_%d", XattrTableName, rangePoint);
    ConstructCreateXattrTableCommand(toExecCommand, name->data);

    int spiConnectionResult = SPI_connect();
    if (spiConnectionResult != SPI_OK_CONNECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "could not connect to SPI manager.");
    }

    int spiQueryResult = SPI_execute(toExecCommand->data, false, 0);
    if (spiQueryResult != SPI_OK_UTILITY) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "spi exec failed.");
    }
    SPI_finish();
}

void FalconDropDistributedDataTableByRangePoint(int rangePoint)
{
    StringInfo toExecCommand = makeStringInfo();
    StringInfo name = makeStringInfo();
    appendStringInfo(name, "%s_%d", InodeTableName, rangePoint);
    appendStringInfo(toExecCommand, "ALTER EXTENSION falcon DROP TABLE %s; DROP TABLE %s;",
        name->data, name->data);
    
    resetStringInfo(name);
    appendStringInfo(name, "%s_%d", XattrTableName, rangePoint);
    appendStringInfo(toExecCommand, "ALTER EXTENSION falcon DROP TABLE %s; DROP TABLE %s;",
        name->data, name->data);

    int spiConnectionResult = SPI_connect();
    if (spiConnectionResult != SPI_OK_CONNECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "could not connect to SPI manager.");
    }

    int spiQueryResult = SPI_execute(toExecCommand->data, false, 0);
    if (spiQueryResult != SPI_OK_UTILITY) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "spi exec failed.");
    }
    SPI_finish();
}

void FalconPrepareCommands()
{
    static bool Prepared = false;
    if (Prepared)
        return;
    int spiConnectionResult = SPI_connect();
    if (spiConnectionResult != SPI_OK_CONNECT) {
        SPI_finish();
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "could not connect to SPI manager.");
    }

    const char *commands[] = {
        "PREPARE cs_meta_call(int, int, bytea) AS SELECT falcon_meta_call_by_serialized_data($1, $2, $3);",
    };

    for (int i = 0; i < sizeof(commands) / sizeof(char *); ++i) {
        int spiQueryResult = SPI_execute(commands[i], false, 0);
        if (spiQueryResult != SPI_OK_UTILITY) {
            SPI_finish();
            FALCON_ELOG_ERROR(PROGRAM_ERROR, "spi exec failed.");
        }
    }
    SPI_finish();

    Prepared = true;
}
