/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "metadb/slice_table.h"


const char *SliceTableName = "falcon_slice_table";

void ConstructCreateSliceTableCommand(StringInfo command, const char *name)
{
    appendStringInfo(command,
                     "CREATE TABLE falcon.%s("
                     "inodeid    bigint,"
                     "chunkid    int,"
                     "sliceid    bigint,"
                     "slicesize  int,"
                     "sliceoffset int,"
                     "slicelen   int,"
                     "sliceloc1  int,"
                     "sliceloc2  int);"
                     "CREATE INDEX %s_index ON falcon.%s USING btree(inodeid, chunkid);"
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