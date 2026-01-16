/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "metadb/sliceid_table.h"
#include "utils/utils.h"

const char *KvSliceIdTableName = "falcon_kvsliceid_table";
const char *FileSliceIdTableName = "falcon_filesliceid_table";

Oid KvSliceIdRelationId(void)
{
    GetRelationOid(KvSliceIdTableName, &CachedRelationOid[CACHED_RELATION_KVSLICEID_TABLE]);
    return CachedRelationOid[CACHED_RELATION_KVSLICEID_TABLE];
}

Oid FileSliceIdRelationId(void)
{
    GetRelationOid(FileSliceIdTableName, &CachedRelationOid[CACHED_RELATION_FILESLICEID_TABLE]);
    return CachedRelationOid[CACHED_RELATION_FILESLICEID_TABLE];
}


