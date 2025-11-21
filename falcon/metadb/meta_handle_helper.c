#include "metadb/meta_handle_helper.h"

#include "string.h"
#include "sys/stat.h"

#include "access/genam.h"
#include "access/table.h"
#include "access/xact.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "utils/error_log.h"
#include "utils/utils.h"

uint16_t HashPartId(const char *fileName)
{
    uint16_t hashValue = 0;
    for (int i = 0; i < strnlen(fileName, FILENAMELENGTH); ++i) {
        hashValue = hashValue * 31 + fileName[i];
    }
    return hashValue & PART_ID_MASK;
}

uint64_t CombineParentIdWithPartId(uint64_t parent_id, uint16_t part_id)
{
    return (parent_id << PART_ID_BIT_COUNT) | part_id;
}

Oid GetRelationOidByName_FALCON(const char *relationName)
{
    Oid res = InvalidOid;
    res = get_relname_relid(relationName, PG_CATALOG_NAMESPACE);
    if (res == InvalidOid) {
        FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR, "cannot find relation %s.", relationName);
    }
    return res;
}

bool SearchAndUpdateInodeTableInfo(const char *workerInodeRelationName,
                                   Relation workerInodeRelation,
                                   const char *workerInodeRelationIndexName,
                                   Oid workerInodeIndexOid,
                                   const uint64_t parentId_partId,
                                   const char *fileName,
                                   const bool doUpdate,
                                   uint64_t *inodeId,
                                   int64_t *size,
                                   const int64_t *newSize,
                                   uint64_t *updateVersion,
                                   uint64_t *nlink,
                                   const int nlinkChangeNum,
                                   mode_t *mode,
                                   const mode_t *newExecMode,
                                   int modeCheckType,
                                   uint32_t *newUid,
                                   uint32_t *newGid,
                                   const char *newEtag,
                                   TimestampTz *newAtime,
                                   TimestampTz *newMtime,
                                   int32_t *primaryNodeId,
                                   int32_t *newPrimaryNodeId,
                                   int32_t *backupNodeId)
{
    ScanKeyData scanKey[2];
    int scanKeyCount = 2;
    SysScanDesc scanDescriptor = NULL;
    HeapTuple heapTuple;
    TupleDesc tupleDesc;
    Relation workerInodeRel = workerInodeRelation;

    SetUpScanCaches();

    // set scan arguments
    scanKey[0] = InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_EQ];
    scanKey[0].sk_argument = UInt64GetDatum(parentId_partId);
    scanKey[1] = InodeTableScanKey[INODE_TABLE_NAME_EQ];
    scanKey[1].sk_argument = CStringGetTextDatum(fileName);

    bool needCatalogTupleUpdate = false;
    if (!workerInodeRelation) {
        workerInodeRel = table_open(GetRelationOidByName_FALCON(workerInodeRelationName),
                                    doUpdate ? RowExclusiveLock : AccessShareLock);
    }

    if (workerInodeIndexOid == InvalidOid)
        workerInodeIndexOid = GetRelationOidByName_FALCON(workerInodeRelationIndexName);
    scanDescriptor =
        systable_beginscan(workerInodeRel, workerInodeIndexOid, true, GetTransactionSnapshot(), scanKeyCount, scanKey);
    heapTuple = systable_getnext(scanDescriptor);
    tupleDesc = RelationGetDescr(workerInodeRel);

    if (!HeapTupleIsValid(heapTuple)) {
        systable_endscan(scanDescriptor);
        if (!workerInodeRelation) {
            table_close(workerInodeRel, doUpdate ? RowExclusiveLock : AccessShareLock);
        }
        return false;
    }

    Datum updateDatumArray[Natts_pg_dfs_inode_table];
    bool isNullArray[Natts_pg_dfs_inode_table];
    bool doUpdateArray[Natts_pg_dfs_inode_table];
    memset(doUpdateArray, false, sizeof(doUpdateArray));
    bool isNull;
    if (inodeId) {
        *inodeId = DatumGetUInt64(heap_getattr(heapTuple, Anum_pg_dfs_file_st_ino, tupleDesc, &isNull));
    }
    if (size) {
        *size = DatumGetInt64(heap_getattr(heapTuple, Anum_pg_dfs_file_st_size, tupleDesc, &isNull));
    }
    if (doUpdate && newSize) {
        updateDatumArray[Anum_pg_dfs_file_st_size - 1] = UInt64GetDatum(*newSize);
        isNullArray[Anum_pg_dfs_file_st_size - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_st_size - 1] = true;
        needCatalogTupleUpdate = true;
    }
    if (updateVersion) {
        *updateVersion = DatumGetUInt64(heap_getattr(heapTuple, Anum_pg_dfs_file_update_version, tupleDesc, &isNull));
        if (doUpdate) {
            updateDatumArray[Anum_pg_dfs_file_update_version - 1] = UInt64GetDatum((*updateVersion) + 1);
            isNullArray[Anum_pg_dfs_file_update_version - 1] = false;
            doUpdateArray[Anum_pg_dfs_file_update_version - 1] = true;
            needCatalogTupleUpdate = true;
        }
    }
    if (nlink) {
        *nlink = DatumGetUInt64(heap_getattr(heapTuple, Anum_pg_dfs_file_st_nlink, tupleDesc, &isNull));
        if (doUpdate) {
            if ((*nlink) + nlinkChangeNum == 0) // refcount changes to 0, need remove this inode row
            {
                CatalogTupleDelete(workerInodeRel, &heapTuple->t_self);
                CommandCounterIncrement();
            } else {
                updateDatumArray[Anum_pg_dfs_file_st_nlink - 1] = UInt64GetDatum((*nlink) + nlinkChangeNum);
                isNullArray[Anum_pg_dfs_file_st_nlink - 1] = false;
                doUpdateArray[Anum_pg_dfs_file_st_nlink - 1] = true;
                needCatalogTupleUpdate = true;
            }
        }
    }
    if (mode) {
        *mode = DatumGetUInt32(heap_getattr(heapTuple, Anum_pg_dfs_file_st_mode, tupleDesc, &isNull));
        if ((modeCheckType == MODE_CHECK_MUST_BE_FILE && !S_ISREG(*mode)) || 
            (modeCheckType == MODE_CHECK_MUST_BE_DIRECTORY && !S_ISDIR(*mode))) {
            systable_endscan(scanDescriptor);
            if (!workerInodeRelation) {
                table_close(workerInodeRel, doUpdate ? RowExclusiveLock : AccessShareLock);
            }
            // file exists, but is not the type expected
            return true;
        }
    }
    if (primaryNodeId) {
        *primaryNodeId = DatumGetUInt32(heap_getattr(heapTuple, Anum_pg_dfs_file_primary_nodeid, tupleDesc, &isNull));
    }
    if (backupNodeId) {
        *backupNodeId = DatumGetUInt32(heap_getattr(heapTuple, Anum_pg_dfs_file_backup_nodeid, tupleDesc, &isNull));
    }
    if (doUpdate && newUid) {
        updateDatumArray[Anum_pg_dfs_file_st_uid - 1] = UInt32GetDatum(*newUid);
        isNullArray[Anum_pg_dfs_file_st_uid - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_st_uid - 1] = true;
        needCatalogTupleUpdate = true;
    }
    if (doUpdate && newGid) {
        updateDatumArray[Anum_pg_dfs_file_st_gid - 1] = UInt32GetDatum(*newGid);
        isNullArray[Anum_pg_dfs_file_st_gid - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_st_gid - 1] = true;
        needCatalogTupleUpdate = true;
    }
    if (doUpdate && newExecMode) {
        mode_t oldMode =
            mode ? *mode : DatumGetUInt32(heap_getattr(heapTuple, Anum_pg_dfs_file_st_mode, tupleDesc, &isNull));
        mode_t newMode = (oldMode & ~0x1FFu) | *newExecMode;
        updateDatumArray[Anum_pg_dfs_file_st_mode - 1] = UInt32GetDatum(newMode);
        isNullArray[Anum_pg_dfs_file_st_mode - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_st_mode - 1] = true;
        needCatalogTupleUpdate = true;
    }
    if (doUpdate && newEtag) {
        updateDatumArray[Anum_pg_dfs_file_etag - 1] = CStringGetTextDatum(newEtag);
        isNullArray[Anum_pg_dfs_file_etag - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_etag - 1] = true;
        needCatalogTupleUpdate = true;
    }
    if (doUpdate && newAtime) {
        updateDatumArray[Anum_pg_dfs_file_st_atim - 1] = TimestampTzGetDatum(*newAtime);
        isNullArray[Anum_pg_dfs_file_st_atim - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_st_atim - 1] = true;
        needCatalogTupleUpdate = true;
    }
    if (doUpdate && newMtime) {
        updateDatumArray[Anum_pg_dfs_file_st_mtim - 1] = TimestampTzGetDatum(*newMtime);
        isNullArray[Anum_pg_dfs_file_st_mtim - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_st_mtim - 1] = true;
        needCatalogTupleUpdate = true;
    }
    if (doUpdate && newPrimaryNodeId) {
        updateDatumArray[Anum_pg_dfs_file_primary_nodeid - 1] = Int32GetDatum(*newPrimaryNodeId);
        isNullArray[Anum_pg_dfs_file_primary_nodeid - 1] = false;
        doUpdateArray[Anum_pg_dfs_file_primary_nodeid - 1] = true;
        needCatalogTupleUpdate = true;
    }

    if (doUpdate && needCatalogTupleUpdate) {
        HeapTuple updatedTuple = heap_modify_tuple(heapTuple, tupleDesc, updateDatumArray, isNullArray, doUpdateArray);
        CatalogTupleUpdate(workerInodeRel, &updatedTuple->t_self, updatedTuple);
        CommandCounterIncrement();
    }

    systable_endscan(scanDescriptor);
    if (!workerInodeRelation) {
        table_close(workerInodeRel, doUpdate ? RowExclusiveLock : AccessShareLock);
    }
    return true;
}

StringInfo GetInodeShardName(int shardId)
{
    StringInfo inodeShardName = makeStringInfo();
    appendStringInfo(inodeShardName, "%s_%d", InodeTableName, shardId);
    return inodeShardName;
}

StringInfo GetInodeIndexShardName(int shardId)
{
    StringInfo inodeIndexShardName = makeStringInfo();
    appendStringInfo(inodeIndexShardName, "%s_%d_%s", InodeTableName, shardId, "index");
    return inodeIndexShardName;
}

StringInfo __attribute__((unused)) GetXattrShardName(int shardId)
{
    StringInfo xattrShardName = makeStringInfo();
    appendStringInfo(xattrShardName, "%s_%d", XattrTableName, shardId);
    return xattrShardName;
}

StringInfo __attribute__((unused)) GetXattrIndexShardName(int shardId)
{
    StringInfo xattrIndexShardName = makeStringInfo();
    appendStringInfo(xattrIndexShardName, "%s_%d_%s", XattrTableName, shardId, "index");
    return xattrIndexShardName;
}