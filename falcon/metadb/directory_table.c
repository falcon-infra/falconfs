/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "metadb/directory_table.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "utils/error_log.h"
#include "utils/utils.h"

const char *DirectoryTableName = "falcon_directory_table";
const char *DirectoryTableIndexName = "falcon_directory_table_index";

Oid DirectoryRelationId(void)
{
    GetRelationOid(DirectoryTableName, &CachedRelationOid[CACHED_RELATION_DIRECTORY_TABLE]);
    return CachedRelationOid[CACHED_RELATION_DIRECTORY_TABLE];
}

Oid DirectoryRelationIndexId(void)
{
    GetRelationOid(DirectoryTableIndexName, &CachedRelationOid[CACHED_RELATION_DIRECTORY_TABLE_INDEX]);
    return CachedRelationOid[CACHED_RELATION_DIRECTORY_TABLE_INDEX];
}

void SearchDirectoryTableInfo(Relation directoryRel, uint64_t parentId, const char *name, uint64_t *inodeId)
{
    ScanKeyData scanKey[2];
    int scanKeyCount = 2;
    SetUpScanCaches();
    scanKey[0] = DirectoryTableScanKey[DIRECTORY_TABLE_PARENT_ID_EQ];
    scanKey[0].sk_argument = UInt64GetDatum(parentId);
    scanKey[1] = DirectoryTableScanKey[DIRECTORY_TABLE_NAME_EQ];
    scanKey[1].sk_argument = CStringGetTextDatum(name);

    bool relControlledByCaller = (directoryRel != NULL);
    if (!relControlledByCaller)
        directoryRel = table_open(DirectoryRelationId(), AccessShareLock);
    SysScanDesc scanDescriptor = systable_beginscan(directoryRel,
                                                    DirectoryRelationIndexId(),
                                                    true,
                                                    GetTransactionSnapshot(),
                                                    scanKeyCount,
                                                    scanKey);
    TupleDesc tupleDesc = RelationGetDescr(directoryRel);
    HeapTuple heapTuple = systable_getnext(scanDescriptor);
    if (!HeapTupleIsValid(heapTuple)) {
        *inodeId = -1;
    } else {
        bool isNull = false;
        *inodeId = DatumGetUInt64(heap_getattr(heapTuple, Anum_falcon_directory_table_inode_id, tupleDesc, &isNull));
    }
    systable_endscan(scanDescriptor);
    if (!relControlledByCaller)
        table_close(directoryRel, AccessShareLock);
}

void InsertIntoDirectoryTable(Relation directoryRel,
                              CatalogIndexState indexState,
                              uint64_t parentId,
                              const char *name,
                              uint64_t inodeId)
{
    Datum values[Natts_falcon_directory_table];
    bool isNulls[Natts_falcon_directory_table];
    memset(values, 0, sizeof(values));
    memset(isNulls, false, sizeof(isNulls));
    values[Anum_falcon_directory_table_parent_id - 1] = UInt64GetDatum(parentId);
    values[Anum_falcon_directory_table_name - 1] = CStringGetTextDatum(name);
    values[Anum_falcon_directory_table_inode_id - 1] = UInt64GetDatum(inodeId);

    bool relControlledByCaller = (directoryRel != NULL);
    if (!relControlledByCaller)
        directoryRel = table_open(DirectoryRelationId(), RowExclusiveLock);
    HeapTuple heapTuple = heap_form_tuple(RelationGetDescr(directoryRel), values, isNulls);

    if (indexState == NULL)
        CatalogTupleInsert(directoryRel, heapTuple);
    else
        CatalogTupleInsertWithInfo(directoryRel, heapTuple, indexState);

    if (!relControlledByCaller)
        table_close(directoryRel, RowExclusiveLock);

    heap_freetuple(heapTuple);
}

void DeleteFromDirectoryTable(Relation directoryRel, uint64_t parentId, const char *name)
{
    ScanKeyData scanKey[2];
    int scanKeyCount = 2;
    SetUpScanCaches();
    scanKey[0] = DirectoryTableScanKey[DIRECTORY_TABLE_PARENT_ID_EQ];
    scanKey[0].sk_argument = UInt64GetDatum(parentId);
    scanKey[1] = DirectoryTableScanKey[DIRECTORY_TABLE_NAME_EQ];
    scanKey[1].sk_argument = CStringGetTextDatum(name);

    bool relControlledByCaller = (directoryRel != NULL);
    if (!relControlledByCaller)
        directoryRel = table_open(DirectoryRelationId(), RowExclusiveLock);
    SysScanDesc scanDescriptor = systable_beginscan(directoryRel,
                                                    DirectoryRelationIndexId(),
                                                    true,
                                                    GetTransactionSnapshot(),
                                                    scanKeyCount,
                                                    scanKey);
    HeapTuple heapTuple = systable_getnext(scanDescriptor);
    if (!HeapTupleIsValid(heapTuple)) {
        FALCON_ELOG_ERROR_EXTENDED(ARGUMENT_ERROR, "can not find " UINT64_PRINT_SYMBOL ":%s in disk.", parentId, name);
    } else {
        CatalogTupleDelete(directoryRel, &(heapTuple->t_self));
    }
    systable_endscan(scanDescriptor);
}
