/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "metadb/kvmeta_table.h"


const char *KvmetaTableName = "falcon_kvmeta_table";

void ConstructCreateKvmetaTableCommand(StringInfo command, const char *name)
{
    appendStringInfo(command,
                     "CREATE TABLE falcon.%s("
                     "user_key  text,"
                     "value_len int,"
                     "slice_num smallint,"
                     "value_key bigint[],"
                     "location  bigint[],"
                     "slice_len int[]);"
                     "CREATE UNIQUE INDEX %s_index ON falcon.%s USING btree(user_key);"
                     "ALTER TABLE falcon.%s SET SCHEMA pg_catalog;"
                     "GRANT SELECT ON pg_catalog.%s TO public;"
                     "ALTER EXTENSION falcon ADD TABLE %s;",
                     name,
                     name,
                     name,
                     name,
                     name,
                     name);
}