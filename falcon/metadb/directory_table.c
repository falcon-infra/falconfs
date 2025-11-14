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
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "control/meta_expiration.h"
#include "utils/error_log.h"
#include "utils/utils.h"
#include "utils/utils_standalone.h"

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

void SearchDirectoryTableInfo(Relation directoryRel, uint64_t parentId, const char *name, uint64_t *inodeId,
                              int64_t* accessTime, int64_t* updatedAccessTime)
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

    SysScanDesc scanDescriptor = NULL;
    SysScanDesc volatile scanDescriptorOnMemory = NULL;
    CatalogIndexState indstate;
    indstate = CatalogOpenIndexes(directoryRel);
	TupleDesc tupleDesc = RelationGetDescr(directoryRel);
	for(;;) {
        bool finishFlag = false;
        PG_TRY();
        {
            scanDescriptor = systable_beginscan(directoryRel, DirectoryRelationIndexId(),
                                                        true, GetTransactionSnapshot(), scanKeyCount, scanKey);
            scanDescriptorOnMemory = scanDescriptor;

            HeapTuple heapTuple = systable_getnext(scanDescriptor);
            if (!HeapTupleIsValid(heapTuple)) {
				*inodeId = -1;
				*accessTime = 0;
				*updatedAccessTime = 0;
                finishFlag = true;
            } else {
            	Datum datumArray[Natts_falcon_directory_table];
				bool isNullArray[Natts_falcon_directory_table];
				heap_deform_tuple(heapTuple, tupleDesc, datumArray, isNullArray);
				*inodeId = DatumGetUInt64(datumArray[Anum_falcon_directory_table_inode_id - 1]);
				*accessTime = DatumGetInt64(datumArray[Anum_falcon_directory_table_access_time - 1]);
				
				int64_t currentTime = GetCurrentTimeInUs();
				if (NeedRenewMetaAccessTime(*accessTime, currentTime))
				{
					LockRelation(directoryRel, RowExclusiveLock);

					bool doUpdateArray[Natts_falcon_directory_table];
					memset(doUpdateArray, 0, sizeof(doUpdateArray));
					datumArray[Anum_falcon_directory_table_access_time - 1] = Int64GetDatum(currentTime);
					isNullArray[Anum_falcon_directory_table_access_time - 1] = false;
					doUpdateArray[Anum_falcon_directory_table_access_time - 1] = true;
					HeapTuple updatedTuple = heap_modify_tuple(heapTuple, tupleDesc, datumArray, isNullArray, doUpdateArray);
					CatalogTupleUpdate(directoryRel, &updatedTuple->t_self, updatedTuple);
					heap_freetuple(updatedTuple);
					CommandCounterIncrement();

					UnlockRelation(directoryRel, RowExclusiveLock);

					*updatedAccessTime = currentTime;
				}
				else{
					*updatedAccessTime = 0;
				}
				finishFlag = true;
			}
        }
        PG_CATCH();
        {
            scanDescriptor = scanDescriptorOnMemory;
			if (scanDescriptor) {
				systable_endscan(scanDescriptor);
			}
            FlushErrorState();
            // error message must be "tuple concurrently updated"
        }
        PG_END_TRY();
        if (finishFlag) {
            break;
        }
    }
	systable_endscan(scanDescriptor);
    CatalogCloseIndexes(indstate);
    if (!relControlledByCaller)
	    table_close(directoryRel, AccessShareLock);
}

void InsertIntoDirectoryTable(Relation directoryRel,
                              CatalogIndexState indexState,
                              uint64_t parentId,
                              const char *name,
                              uint64_t inodeId,
                              int64_t accessTime)
{
    Datum values[Natts_falcon_directory_table];
    bool isNulls[Natts_falcon_directory_table];
    memset(values, 0, sizeof(values));
    memset(isNulls, false, sizeof(isNulls));
    values[Anum_falcon_directory_table_parent_id - 1] = UInt64GetDatum(parentId);
    values[Anum_falcon_directory_table_name - 1] = CStringGetTextDatum(name);
    values[Anum_falcon_directory_table_inode_id - 1] = UInt64GetDatum(inodeId);
    values[Anum_falcon_directory_table_access_time - 1] = Int64GetDatum(accessTime);

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

void DeleteFromDirectoryTable(Relation directoryRel, uint64_t parentId, const char *name, bool forExpired, uint64_t* inodeId)
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
    TupleDesc tupleDesc = RelationGetDescr(directoryRel);
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
        if (forExpired)	// check if this is an expired directory
		{
			Datum datumArray[Natts_falcon_directory_table];
			bool isNullArray[Natts_falcon_directory_table];
			heap_deform_tuple(heapTuple, tupleDesc, datumArray, isNullArray);
			if (inodeId)
				*inodeId = (uint64_t)DatumGetInt64(datumArray[Anum_falcon_directory_table_inode_id - 1]);
			int64_t accessTime = DatumGetInt64(datumArray[Anum_falcon_directory_table_access_time - 1]);
			int64_t currentTime = GetCurrentTimeInUs();
			if (!ExceedExpirationTimeInterval(accessTime, currentTime))
                FALCON_ELOG_ERROR(ARGUMENT_ERROR, "forExpired is true but target directory is not expired.");
		}
        CatalogTupleDelete(directoryRel, &(heapTuple->t_self));
    }
    systable_endscan(scanDescriptor);
}


void RenewDirectoryTableAccessTime(Relation directoryRel, uint64_t parentId, const char* name, int64_t currentTime)
{
	ScanKeyData scanKey[2];
	int 		scanKeyCount = 2;
    SetUpScanCaches();
	scanKey[0] = DirectoryTableScanKey[DIRECTORY_TABLE_PARENT_ID_EQ];
    scanKey[0].sk_argument = UInt64GetDatum(parentId);
    scanKey[1] = DirectoryTableScanKey[DIRECTORY_TABLE_NAME_EQ];
    scanKey[1].sk_argument = CStringGetTextDatum(name);
    
    bool relControlledByCaller = (directoryRel != NULL);
    if (!relControlledByCaller)
	    directoryRel = table_open(DirectoryRelationId(), RowExclusiveLock);
    SysScanDesc scanDescriptor = NULL;
	SysScanDesc volatile scanDescriptorOnMemory = NULL;

    CatalogIndexState indstate;
    indstate = CatalogOpenIndexes(directoryRel);

    for(;;) {
        bool finishFlag = false;
		scanDescriptor = NULL;
        
        PG_TRY();
		{ 
            scanDescriptor = systable_beginscan(directoryRel, DirectoryRelationIndexId(),
                true, GetTransactionSnapshot(), scanKeyCount, scanKey);
			scanDescriptorOnMemory = scanDescriptor;

            HeapTuple heapTuple = systable_getnext(scanDescriptor);
            if (!HeapTupleIsValid(heapTuple)) {
                finishFlag = true;
            } else {
            	TupleDesc tupleDesc = RelationGetDescr(directoryRel);

				Datum datumArray[Natts_falcon_directory_table];
				bool isNullArray[Natts_falcon_directory_table];
				heap_deform_tuple(heapTuple, tupleDesc, datumArray, isNullArray);
				int64_t oldAccessTime = DatumGetInt64(datumArray[Anum_falcon_directory_table_access_time - 1]);
				if (!NeedRenewMetaAccessTime(oldAccessTime, currentTime)) {
					finishFlag = true;
				} else {
					Datum updateDatumArray[Natts_falcon_directory_table];
					bool isNullArray[Natts_falcon_directory_table];
					bool doUpdateArray[Natts_falcon_directory_table];
					memset(doUpdateArray, 0, sizeof(doUpdateArray));
					doUpdateArray[Anum_falcon_directory_table_access_time - 1] = true;
					isNullArray[Anum_falcon_directory_table_access_time - 1] = false;
					updateDatumArray[Anum_falcon_directory_table_access_time - 1] = Int64GetDatum(currentTime);
					HeapTuple updatedTuple = heap_modify_tuple(heapTuple, tupleDesc,
						updateDatumArray, isNullArray, doUpdateArray);
					CatalogTupleUpdateWithInfo(directoryRel, &updatedTuple->t_self, updatedTuple, indstate);
					CommandCounterIncrement();
					finishFlag = true;
				}
			}
        }
        PG_CATCH();
        {
			scanDescriptor = scanDescriptorOnMemory;
			if (scanDescriptor) {
				systable_endscan(scanDescriptor);
			}
            FlushErrorState();
            // error message must be tuple concurrently updated
        }
        PG_END_TRY();
        if (finishFlag) {
            break;
        }
    }
	systable_endscan(scanDescriptor);
    CatalogCloseIndexes(indstate);
    if (!relControlledByCaller)
    	table_close(directoryRel, RowExclusiveLock);
}