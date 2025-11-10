/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "metadb/kvsliceid_table.h"
#include "utils/utils.h"

const char *KvSliceIdTableName = "falcon_kvsliceid_table";
const char *KvSliceIdTableIndexName = "falcon_kvsliceid_table_index";

Oid KvSliceIdRelationId(void)
{
    GetRelationOid(KvSliceIdTableName, &CachedRelationOid[CACHED_RELATION_KVSLICEID_TABLE]);
    return CachedRelationOid[CACHED_RELATION_KVSLICEID_TABLE];
}

Oid KvSliceIdRelationIndexId(void)
{
    GetRelationOid(KvSliceIdTableIndexName, &CachedRelationOid[CACHED_RELATION_KVSLICEID_TABLE_INDEX]);
    return CachedRelationOid[CACHED_RELATION_KVSLICEID_TABLE_INDEX];
}


