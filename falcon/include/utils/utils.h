/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_UTILS_H
#define FALCON_UTILS_H

#include <bits/wordsize.h>

#include "postgres.h"

#include "access/skey.h"
#include "catalog/pg_namespace_d.h"
#include "datatype/timestamp.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/plannodes.h"
#include "utils/array.h"
#include "utils/hsearch.h"

#include "metadb/directory_table.h"
#include "metadb/foreign_server.h"
#include "metadb/inode_table.h"
#include "metadb/shard_table.h"
#include "metadb/xattr_table.h"

#define INT32_PRINT_SYMBOL "%d"
#define UINT32_PRINT_SYMBOL "%u"
#if __WORDSIZE == 64
#define INT64_PRINT_SYMBOL "%ld"
#define UINT64_PRINT_SYMBOL "%lu"
#else
#define INT64_PRINT_SYMBOL "%lld"
#define UINT64_PRINT_SYMBOL "%llu"
#endif

uint64_t GenerateInodeIdBySeqAndNodeId(uint64_t seq, int nodeId);

int32 HashShard(uint64 parentId_partId);

bool CheckIfRelationExists(const char *relationName, Oid relNamespace);
typedef enum CachedRelationType {
    CACHED_RELATION_FOREIGN_SERVER = 0,
    CACHED_RELATION_FOREIGN_SERVER_INDEX,
    CACHED_RELATION_SHARD_TABLE,
    CACHED_RELATION_SHARD_TABLE_INDEX,
    CACHED_RELATION_DIRECTORY_TABLE,
    CACHED_RELATION_DIRECTORY_TABLE_INDEX,
    CACHED_RELATION_DISTRIBUTED_TRANSACTION_TABLE,
    CACHED_RELATION_DISTRIBUTED_TRANSACTION_TABLE_INDEX,
    LAST_CACHED_RELATION_TYPE
} CachedRelationType;
extern Oid CachedRelationOid[LAST_CACHED_RELATION_TYPE];
void GetRelationOid(const char *relationName, Oid *relOid);

bool CheckFalconHasBeenLoaded(void);
bool CheckWhetherTargetExistInIndex(Relation heap, Relation index, ScanKeyData *scanKeys);

void SetUpScanCaches(void);
extern ScanKeyData ForeignServerTableScanKey[LAST_FALCON_FOREIGN_SERVER_TABLE_SCANKEY_TYPE];
extern ScanKeyData DirectoryTableScanKey[LAST_FALCON_DIRECTORY_TABLE_SCANKEY_TYPE];
extern ScanKeyData InodeTableScanKey[LAST_FALCON_INODE_TABLE_SCANKEY_TYPE];
extern ScanKeyData
    InodeTableIndexParentIdPartIdNameScanKey[LAST_FALCON_INODE_TABLE_INDEX_PARENT_ID_PART_ID_NAME_SCANKEY_TYPE];
extern ScanKeyData XattrTableScanKey[LAST_FALCON_XATTR_TABLE_SCANKEY_TYPE];

bool ArrayTypeArrayToDatumArrayAndSize(ArrayType *arrayObject, Datum **datumArray, int *datumArrayLength);

void freeStringInfo(StringInfo s);

void hash_clear(HTAB *htab);

Oid FalconExtensionOwner(void);

typedef enum FALCON_LOCK_OPERATION { FALCON_LOCK_2PC_CLEANUP } FALCON_LOCK_OPERATION;

#define SET_LOCKTAG_FALCON_OPERATION(tag, operationId) \
    SET_LOCKTAG_ADVISORY(tag, MyDatabaseId, (uint32)0, (uint32)operationId, 569)

#endif
