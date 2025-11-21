#include "control/meta_expiration.h"

#include <unistd.h>
#include <sys/stat.h>

#include "access/genam.h"
#include "common/hashfn.h"
#include "utils/builtins.h"

#include "executor/spi.h"
#include "catalog/indexing.h"
#include "fmgr.h"
#include "funcapi.h"
#include "nodes/pg_list.h"
#include "postmaster/bgworker.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"

#include "control/control_flag.h"
#include "dir_path_shmem/dir_path_hash.h"
#include "distributed_backend/remote_comm_falcon.h"
#include "metadb/meta_handle_helper.h"
#include "metadb/shard_table.h"
#include "utils/error_log.h"
#include "utils/utils.h"
#include "utils/utils_standalone.h"

typedef struct ExpiredDirectoryKey
{
    uint64_t parentId;
    char name[FILENAMELENGTH];
} ExpiredDirectoryKey;
typedef struct ExpiredDirectoryEntry
{
    ExpiredDirectoryKey key;
    int64_t lastAccessTime;
} ExpiredDirectoryEntry;

const char* ExpiredInodeTableName = "falcon_expired_inodeid_table";
const char* ExpiredInodeTableIndexName = "falcon_expired_inodeid_table_index";

static Oid ExpiredInodeTableRelationId(void)
{
	GetRelationOid(ExpiredInodeTableName, &CachedRelationOid[CACHED_RELATION_EXPIRED_INODEID_TABLE]);
	return CachedRelationOid[CACHED_RELATION_EXPIRED_INODEID_TABLE];
}

static Oid ExpiredInodeIndexRelationId(void)
{
    GetRelationOid(ExpiredInodeTableIndexName, &CachedRelationOid[CACHED_RELATION_EXPIRED_INODEID_TABLE_INDEX]);
	return CachedRelationOid[CACHED_RELATION_EXPIRED_INODEID_TABLE_INDEX];
}

int32_t FalconMetaValidDuration;
bool ExceedExpirationTimeInterval(int64_t accessTime, int64_t currentTime)
{
    if (FalconMetaValidDuration < 0)
        return false;
    return currentTime - accessTime > (int64_t)FalconMetaValidDuration * 1000000ll * 2;
}
bool NeedRenewMetaAccessTime(int64_t accessTime, int64_t currentTime)
{
    if (FalconMetaValidDuration < 0)
        return false;
    return currentTime - accessTime > (int64_t)FalconMetaValidDuration * 1000000ll;
}

#define CLEAR_EXPIRED_DIRECTORY_MAX_COUNT_ONCE    1024

static int ClearExpiredDirectory(void);

PG_FUNCTION_INFO_V1(falcon_get_expired_directory);
PG_FUNCTION_INFO_V1(falcon_delete_expired_directory_internal);
PG_FUNCTION_INFO_V1(falcon_test_expiration);
PG_FUNCTION_INFO_V1(falcon_delete_expired_files_internal);
PG_FUNCTION_INFO_V1(falcon_test_renew_func);
Datum falcon_test_renew_func(PG_FUNCTION_ARGS)
{
    MemoryContext LocalMemoryContext = AllocSetContextCreate(
				CurrentMemoryContext,
				"LocalMemoryContext",
				ALLOCSET_DEFAULT_SIZES);
    MemoryContext oldMemoryContext = NULL;
    oldMemoryContext = MemoryContextSwitchTo(LocalMemoryContext);

    ScanKeyData scanKey[2];
    int         scanKeyCount = 2;
    uint64_t parentId = 0;
    const char* name = "/";
    int64_t accessTime = 1234;
    SetUpScanCaches();
    scanKey[0] = DirectoryTableScanKey[DIRECTORY_TABLE_PARENT_ID_EQ];
    scanKey[0].sk_argument = UInt64GetDatum(parentId);
    scanKey[1] = DirectoryTableScanKey[DIRECTORY_TABLE_NAME_EQ];
    scanKey[1].sk_argument = CStringGetTextDatum(name);
   
    Relation  directoryRel = table_open(DirectoryRelationId(), RowExclusiveLock);
    SysScanDesc scanDescriptor = NULL;

    CatalogIndexState indstate;
    indstate = CatalogOpenIndexes(directoryRel);

    for(;;) {
        bool finishFlag = false;
        PG_TRY();
        {
            *(volatile SysScanDesc *)(&scanDescriptor) = systable_beginscan(directoryRel, DirectoryRelationIndexId(),
                                                            true, GetTransactionSnapshot(), scanKeyCount, scanKey);
       
            HeapTuple heapTuple = systable_getnext(scanDescriptor);
            if (!HeapTupleIsValid(heapTuple)) {
                finishFlag = true;
            }
            TupleDesc tupleDesc = RelationGetDescr(directoryRel);
            Datum updateDatumArray[Natts_falcon_directory_table];
            bool isNullArray[Natts_falcon_directory_table];
            bool doUpdateArray[Natts_falcon_directory_table];
            memset(doUpdateArray, 0, sizeof(doUpdateArray));
            doUpdateArray[Anum_falcon_directory_table_access_time - 1] = true;
            isNullArray[Anum_falcon_directory_table_access_time - 1] = false;
            updateDatumArray[Anum_falcon_directory_table_access_time - 1] = Int64GetDatum(accessTime);
            HeapTuple updatedTuple = heap_modify_tuple(heapTuple, tupleDesc,
                updateDatumArray, isNullArray, doUpdateArray);
            CatalogTupleUpdateWithInfo(directoryRel, &updatedTuple->t_self, updatedTuple, indstate);
            CommandCounterIncrement();
            finishFlag = true;
        }
        PG_CATCH();
        {
            if (scanDescriptor) {
                systable_endscan(scanDescriptor);
            } 
            // EmitErrorReport();
            FlushErrorState();
            // error message must be  tuple concurrently updated
        }
        PG_END_TRY();
        if (finishFlag) {
            break;
        }
    }
    systable_endscan(scanDescriptor);
    CatalogCloseIndexes(indstate);
    // if (!relControlledByCaller)
    table_close(directoryRel, RowExclusiveLock);
    MemoryContextSwitchTo(oldMemoryContext);
    PG_RETURN_VOID();
}

Datum falcon_delete_expired_files_internal(PG_FUNCTION_ARGS)
{
	uint64_t inodeId = (uint64_t)PG_GETARG_INT64(0);

    SetUpScanCaches();
    List *shardTableData = GetShardTableData();
    int shardTableCount = list_length(shardTableData);
    
    int shardIndex = 0;
    while (shardIndex < shardTableCount)
    {
        int workerId = ((FormData_falcon_shard_table*)list_nth(shardTableData, shardIndex))->server_id;
        int shardId = ((FormData_falcon_shard_table*)list_nth(shardTableData, shardIndex))->range_point;

        if (workerId != GetLocalServerId())
        {
            ++shardIndex;
            continue;
        }

        uint64_t lowerId = CombineParentIdWithPartId(inodeId, 0);
        uint64_t upperId = CombineParentIdWithPartId(inodeId, 0x1FFF);

        StringInfo inodeShardName = GetInodeShardName(shardId);
        StringInfo inodeIndexShardName = GetInodeIndexShardName(shardId);

        ScanKeyData scanKey[2];
        int scanKeyCount = 2;

        scanKey[0] = InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_GE];
        scanKey[0].sk_argument = UInt64GetDatum(lowerId);
        scanKey[1] = InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_LE];
        scanKey[1].sk_argument = UInt64GetDatum(upperId);

        Relation workerInodeRel = table_open(GetRelationOidByName_FALCON(inodeShardName->data), AccessShareLock);
        SysScanDesc scanDescriptor = systable_beginscan(workerInodeRel, GetRelationOidByName_FALCON(inodeIndexShardName->data), 
                                                        true, GetTransactionSnapshot(), scanKeyCount, scanKey);
        TupleDesc tupleDescriptor = RelationGetDescr(workerInodeRel);

        Datum datumArray[Natts_pg_dfs_inode_table];
        bool isNullArray[Natts_pg_dfs_inode_table];
        HeapTuple heapTuple;
        while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
        {
            heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);
            int mode = DatumGetInt32(datumArray[Anum_pg_dfs_file_st_mode - 1]);
            if(S_ISREG(mode) || S_ISLNK(mode)) {
                CatalogTupleDelete(workerInodeRel, &heapTuple->t_self);
                CommandCounterIncrement();
            }
        }
        
        systable_endscan(scanDescriptor);
        table_close(workerInodeRel, AccessShareLock);
        shardIndex++;
    }
    PG_RETURN_INT16(SUCCESS);
}

Datum falcon_get_expired_directory(PG_FUNCTION_ARGS)
{
    FuncCallContext *functionContext = NULL;
	List			*resultList = NIL;
	if (SRF_IS_FIRSTCALL())
	{
		functionContext = SRF_FIRSTCALL_INIT();
		MemoryContext oldContext = MemoryContextSwitchTo(functionContext->multi_call_memory_ctx);

		TupleDesc tupleDesc;
		if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		{
            FALCON_ELOG_ERROR(PROGRAM_ERROR, "return type must be a row type.");
		}
		functionContext->tuple_desc = BlessTupleDesc(tupleDesc);

        Relation directoryRel = table_open(DirectoryRelationId(), AccessShareLock);
        SysScanDesc scanDesc = systable_beginscan(directoryRel, 
            DirectoryRelationIndexId(), true, GetTransactionSnapshot(), 0, NULL);
        TupleDesc directoryTupleDesc = RelationGetDescr(directoryRel);

        int64_t currentTime = GetCurrentTimeInUs();
        Datum datumArray[Natts_falcon_directory_table];
        bool isNullArray[Natts_falcon_directory_table];
        HeapTuple heapTuple;
        int count = 0;
        while (HeapTupleIsValid(heapTuple = systable_getnext(scanDesc)))
        {
            heap_deform_tuple(heapTuple, directoryTupleDesc, datumArray, isNullArray);

            int64_t parentId = DatumGetUInt64(datumArray[Anum_falcon_directory_table_parent_id - 1]);
            if (parentId == 0)  // '/' never expire
                continue;

            int64_t accessTime = DatumGetInt64(datumArray[Anum_falcon_directory_table_access_time - 1]);
            if (ExceedExpirationTimeInterval(accessTime, currentTime))
            {
                Datum resDatumArray[3];
                bool resIsNullArray[3];
                memset(resIsNullArray, 0, sizeof(resIsNullArray));
                resDatumArray[0] = datumArray[Anum_falcon_directory_table_parent_id - 1];
                resDatumArray[1] = datumArray[Anum_falcon_directory_table_name - 1];
                resDatumArray[2] = datumArray[Anum_falcon_directory_table_access_time - 1];
                HeapTuple res = heap_form_tuple(tupleDesc, resDatumArray, resIsNullArray);
                resultList = lappend(resultList, DatumGetPointer(heap_copy_tuple_as_datum(res, tupleDesc)));
                ++count;
            }

            if (count >= CLEAR_EXPIRED_DIRECTORY_MAX_COUNT_ONCE)
                break;
        }
        
        systable_endscan(scanDesc);
		table_close(directoryRel, AccessShareLock);

        functionContext->user_fctx = resultList;
		functionContext->max_calls = list_length(resultList);
		MemoryContextSwitchTo(oldContext);
    }

    functionContext = SRF_PERCALL_SETUP();
	resultList = functionContext->user_fctx;
	int d_off = functionContext->call_cntr;

	if (d_off < functionContext->max_calls)
	{
		Datum indoeDatum = (Datum)list_nth(resultList, d_off);
		SRF_RETURN_NEXT(functionContext, indoeDatum);
	}
	SRF_RETURN_DONE(functionContext);
}

Datum falcon_delete_expired_directory_internal(PG_FUNCTION_ARGS)
{
    uint64_t parentId = (uint64_t)PG_GETARG_INT64(0);
    const char* name = (const char*)PG_GETARG_CSTRING(1);

    Relation directoryRel = table_open(DirectoryRelationId(), RowExclusiveLock);
    DeleteDirectoryByDirectoryHashTable(directoryRel, parentId, name, DIR_LOCK_EXCLUSIVE, true, NULL);
	table_close(directoryRel, RowExclusiveLock);

    uint16_t partId = HashPartId(name);
	uint64_t parentId_partId = CombineParentIdWithPartId(parentId, partId);
    int32_t shardId, workerId;
	SearchShardInfoByShardValue(parentId_partId, &shardId, &workerId);
    if (workerId == GetLocalServerId())
    {
        StringInfo inodeShardName = GetInodeShardName(shardId);
        StringInfo inodeIndexShardName = GetInodeIndexShardName(shardId);
        uint64_t nlink;
	    bool fileExist = SearchAndUpdateInodeTableInfo(inodeShardName->data, NULL, inodeIndexShardName->data, InvalidOid,
												       parentId_partId, name, true,
                                      		    	   NULL, NULL, NULL, NULL, &nlink, -1, NULL, NULL, 
                                                       MODE_CHECK_MUST_BE_DIRECTORY, 
                                                       NULL, NULL, NULL, NULL,
                                                       NULL, NULL, NULL, NULL);
        if (!fileExist)
            FALCON_ELOG_ERROR(ARGUMENT_ERROR, "file not exist.");
    }

    PG_RETURN_INT32(0);
}

Datum falcon_test_expiration(PG_FUNCTION_ARGS)
{
    int clearCount = ClearExpiredDirectory();
    PG_RETURN_INT32(clearCount);
}

static volatile bool got_SIGTERM = false;
static void FalconDaemonMetaExpirationProcessSigTermHandler(SIGNAL_ARGS);

void FalconDaemonMetaExpirationProcessMain(Datum main_arg)
{
	pqsignal(SIGTERM, FalconDaemonMetaExpirationProcessSigTermHandler);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	ResourceOwner myOwner = ResourceOwnerCreate(NULL, "falcon background meta expiration");
	MemoryContext myContext = AllocSetContextCreate(TopMemoryContext,
												    "falcon background meta expiration memory context",
							 		  				ALLOCSET_DEFAULT_MINSIZE,
									  				ALLOCSET_DEFAULT_INITSIZE,
									 			 	ALLOCSET_DEFAULT_MAXSIZE);
	ResourceOwner oldOwner = CurrentResourceOwner;
	CurrentResourceOwner = myOwner;

	elog(LOG, "FalconDaemonMetaExpirationProcessMain: wait init.");
	bool serviceStarted = false;
	do
	{
		sleep(10);
		serviceStarted = CheckFalconBackgroundServiceStarted();
	} while (!serviceStarted);
	elog(LOG, "FalconDaemonMetaExpirationProcessMain: init finished.");
    StartTransactionCommand();
    int serverId = GetLocalServerId();
    CommitTransactionCommand();
    if (serverId == 0 && FalconMetaValidDuration >= 0)
    {
        while (!got_SIGTERM)
        {
            MemoryContext oldContext = MemoryContextSwitchTo(myContext);

            int totalClearCount = 0;
            for (;;)
            {
                StartTransactionCommand();
                int clearCount = ClearExpiredDirectory();
                CommitTransactionCommand();

                if (clearCount == 0)
                    break;
                if (clearCount > 0)
                    totalClearCount += clearCount;
                
                sleep(1);
            }

            if (totalClearCount != 0) {
                elog(LOG, "ClearExpiredDirectoryProcess: clear %d expired directories.", totalClearCount);
            }

            MemoryContextSwitchTo(oldContext);
            MemoryContextReset(myContext);

            int sleepTime = FalconMetaValidDuration / 2;
            if (sleepTime < 1)
                sleepTime = 1;
            sleep(sleepTime);
        }
    }
	
    elog(LOG, "FalconDaemonMetaExpirationProcessMain: exit.");
    CurrentResourceOwner = oldOwner;
	ResourceOwnerRelease(myOwner, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
	ResourceOwnerRelease(myOwner, RESOURCE_RELEASE_LOCKS, true, true);
	ResourceOwnerRelease(myOwner, RESOURCE_RELEASE_AFTER_LOCKS, true, true);
	ResourceOwnerDelete(myOwner);
	MemoryContextDelete(myContext);
	return;
}

static void FalconDaemonMetaExpirationProcessSigTermHandler(SIGNAL_ARGS)
{
	int save_errno = errno;

	elog(LOG, "FalconDaemonMetaExpirationProcessSigTermHandler: get sigterm.");
	got_SIGTERM = true;

	errno = save_errno;
}

static uint32 ExpiredDirectoryKeyHash(const void *key, Size keysize)
{
    ExpiredDirectoryKey* p = (ExpiredDirectoryKey*)key;
    return hash_bytes((const unsigned char*)p, strlen(p->name) + sizeof(p->parentId));
}
static int ExpiredDirectoryKeyCmp(const void *key1, const void *key2, Size keysize)
{
    ExpiredDirectoryKey* p1 = (ExpiredDirectoryKey*)key1;
    ExpiredDirectoryKey* p2 = (ExpiredDirectoryKey*)key2;
    if (p1->parentId != p2->parentId)
        return p1->parentId - p2->parentId;
    return strcmp(p1->name, p2->name);
}
static void* ExpiredDirectoryKeyCopy(void *dest, const void *src, Size keysize)
{
    ExpiredDirectoryKey* pDest = (ExpiredDirectoryKey*)dest;
    ExpiredDirectoryKey* pSrc = (ExpiredDirectoryKey*)src;
    pDest->parentId = pSrc->parentId;
    strcpy(pDest->name, pSrc->name);
    return dest;
}

static HTAB* CreateExpiredDirectoryHashTable(const char* name)
{
    HASHCTL info;
    memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ExpiredDirectoryKey);
	info.entrysize = sizeof(ExpiredDirectoryEntry);
    info.hcxt = CurrentMemoryContext;
    info.hash = ExpiredDirectoryKeyHash;
    info.match = ExpiredDirectoryKeyCmp;
    info.keycopy = ExpiredDirectoryKeyCopy;
    int flags = HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION | HASH_COMPARE | HASH_KEYCOPY;
    int capacity = CLEAR_EXPIRED_DIRECTORY_MAX_COUNT_ONCE * 2;
    return hash_create(name, capacity, &info, flags);
}

static HTAB* GetAllExpiredDirectory()
{
    HTAB* hashTable1 = CreateExpiredDirectoryHashTable("hashTable1");
    HTAB* hashTable2 = CreateExpiredDirectoryHashTable("hashTable2");

    HTAB* expiredDirectoryHashTable = hashTable1;
    HTAB* anotherExpiredDirectoryHashTable = hashTable2;

    // 1. get expired directory on local server
    int ret = 0;
    ret = SPI_connect();
    if (ret != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed: error code %d", ret);
    PushActiveSnapshot(GetTransactionSnapshot());

    ret = SPI_exec("SELECT * FROM falcon_get_expired_directory()", 0);
    if (ret < 0)
        elog(ERROR, "SPI_exec failed: error code %d", ret);
    TupleDesc tupleDesc = SPI_tuptable->tupdesc;
    SPITupleTable *tuptable = SPI_tuptable;
    for (uint64_t i = 0; i < SPI_processed; i++)
    {
        HeapTuple tuple = tuptable->vals[i];
        Datum datumArray[3];
        bool isNullArray[3];
        heap_deform_tuple(tuple, tupleDesc, datumArray, isNullArray);
        ExpiredDirectoryKey key;
        key.parentId = DatumGetUInt64(datumArray[0]);
        text* name = DatumGetTextP(datumArray[1]);
        int len = VARSIZE_ANY_EXHDR(name);
        strncpy(key.name, VARDATA_ANY(name), len);
        key.name[len] = 0;
        int64_t accessTime = DatumGetInt64(datumArray[2]);

        bool found;
        ExpiredDirectoryEntry* entry = (ExpiredDirectoryEntry*)hash_search(
            expiredDirectoryHashTable, &key, HASH_ENTER, &found);
        if (found)
            elog(ERROR, "duplicate expired directory table");
        entry->lastAccessTime = accessTime;
    }

    PopActiveSnapshot(); 
    ret = SPI_finish();
    if (ret != SPI_OK_FINISH)
        elog(ERROR, "SPI_finish failed: error code %d", ret);

    // 2. get expired directory on other server
    StringInfo command = makeStringInfo();
    appendStringInfo(command, "SELECT * FROM falcon_get_expired_directory();");
    List* workerIdList = GetAllForeignServerId(true, false);
    FalconPlainCommandOnWorkerList(command->data, REMOTE_COMMAND_FLAG_NO_BEGIN, workerIdList);
    MultipleServerRemoteCommandResult totalRemoteRes = FalconSendCommandAndWaitForResult();
    for (int i = 0; i < list_length(totalRemoteRes); ++i)
    {
        RemoteCommandResultPerServerData* remoteRes = list_nth(totalRemoteRes, i);
        if (list_length(remoteRes->remoteCommandResult) != 1)
            FALCON_ELOG_ERROR(PROGRAM_ERROR, "unexpected situation.");

        PGresult* res = list_nth(remoteRes->remoteCommandResult, 0);
        int rowCount = PQntuples(res);
        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            ExpiredDirectoryKey key;
            key.parentId = StringToUint64(PQgetvalue(res, rowIndex, 0));
            strcpy(key.name, PQgetvalue(res, rowIndex, 1));
            int64_t accessTime = StringToInt64(PQgetvalue(res, rowIndex, 2));

            bool found;
            ExpiredDirectoryEntry* entry = (ExpiredDirectoryEntry*)hash_search(
                expiredDirectoryHashTable, &key, HASH_ENTER, &found);
            if (!found) // an expired directory must be expired on all of the servers
                continue;
            int64_t lastAccessTime = Max(entry->lastAccessTime, accessTime);

            entry = (ExpiredDirectoryEntry*)hash_search(
                anotherExpiredDirectoryHashTable, &key, HASH_ENTER, &found);
            if (found)
                elog(ERROR, "duplicate expired directory.");
            entry->lastAccessTime = lastAccessTime;
        }

        // exchange two hash table
        if (expiredDirectoryHashTable == hashTable1)
        {
            expiredDirectoryHashTable = hashTable2;
            anotherExpiredDirectoryHashTable = hashTable1;
        }
        else
        {
            expiredDirectoryHashTable = hashTable1;
            anotherExpiredDirectoryHashTable = hashTable2;
        }
        hash_clear(anotherExpiredDirectoryHashTable);
    }

    // 3. return the target 
    return expiredDirectoryHashTable;
}

static void InsertInodeIdIntoExpiredTable(Relation expiredInodeRel, uint64_t inodeId)
{
    Datum values[1];
	bool isNulls[1];
	HeapTuple heapTuple;
	TupleDesc tupleDescriptor = RelationGetDescr(expiredInodeRel);

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));
	
	values[0] = UInt64GetDatum(inodeId);

	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);
	CatalogTupleInsert(expiredInodeRel, heapTuple);
	heap_freetuple(heapTuple);
	CommandCounterIncrement();
}

static void DeleteExpiredDirectory(uint64_t parentId, const char* name)
{
    if (GetLocalServerId() != 0)
        FALCON_ELOG_ERROR(WRONG_WORKER, "DeleteExpiredDirectory can only be called on CN.");

    // 1. delete target on cn
    uint64_t inodeId = 0;
    Relation directoryRel = table_open(DirectoryRelationId(), RowExclusiveLock);
    DeleteDirectoryByDirectoryHashTable(directoryRel, parentId, name, DIR_LOCK_EXCLUSIVE, true, &inodeId);
	table_close(directoryRel, RowExclusiveLock);

    // 2. delete target on all other workers
    StringInfo command = makeStringInfo();
	appendStringInfo(command, "select * from falcon_delete_expired_directory_internal("
        UINT64_PRINT_SYMBOL", '%s');", parentId, name);
	List* workerIdList = GetAllForeignServerId(true, false);
	(void) FalconPlainCommandOnWorkerList(command->data, REMOTE_COMMAND_FLAG_WRITE, workerIdList);

    (void) FalconSendCommandAndWaitForResult();

    // 3. insert the inodeid into the expired inodeid table;
    Relation expiredInodeRel = table_open(ExpiredInodeTableRelationId(), RowExclusiveLock);
    InsertInodeIdIntoExpiredTable(expiredInodeRel, inodeId);
    table_close(expiredInodeRel, RowExclusiveLock);
    // return inodeId;
}

static int DeleteFilesByExpiredInodeId()
{
    // 1. fetch expired inodeid from expired inodeid table;
    int successClearCount = 0;
    Relation expiredInodeRel = table_open(ExpiredInodeTableRelationId(), RowExclusiveLock);
    SysScanDesc scanDesc = systable_beginscan(expiredInodeRel, 
        ExpiredInodeIndexRelationId(), true, GetTransactionSnapshot(), 0, NULL);
    TupleDesc ExpiredInodeTupleDesc = RelationGetDescr(expiredInodeRel);
    Datum datumArray[1];
    bool isNullArray[1];
    HeapTuple heapTuple;
    while (HeapTupleIsValid(heapTuple = systable_getnext(scanDesc)))
    {
        heap_deform_tuple(heapTuple, ExpiredInodeTupleDesc, datumArray, isNullArray);
        int64_t inodeId = DatumGetUInt64(datumArray[0]);
        // 2. send command to delete files whose parentid is this one
        StringInfo command = makeStringInfo();
        appendStringInfo(command, "select * from falcon_delete_expired_files_internal("
            UINT64_PRINT_SYMBOL");", inodeId);
        List* workerIdList = GetAllForeignServerId(true, false);
        (void) FalconPlainCommandOnWorkerList(command->data, REMOTE_COMMAND_FLAG_NO_BEGIN, workerIdList);

        bool deleteSuccess = true;
        PG_TRY();
        {
            (void) FalconSendCommandAndWaitForResult();
        }
        PG_CATCH();
        {    
            deleteSuccess = false;
            elog(WARNING, "dn delete expired files failed!");
        }
        PG_END_TRY();
        if (deleteSuccess)
        {
            // 3. if delete successfully, delete this inodeid from the expired inode table
            CatalogTupleDelete(expiredInodeRel, &heapTuple->t_self);
            CommandCounterIncrement();
            successClearCount++;
        }
    }
    
    systable_endscan(scanDesc);
    table_close(expiredInodeRel, RowExclusiveLock);
    return successClearCount;
}

static int ClearExpiredDirectory()
{
    // 1. get candidate
    HTAB* expiredDirectoryHashTable = NULL;
    PG_TRY();
    {
        BeginInternalSubTransaction("get_all_expired_directory");
		expiredDirectoryHashTable = GetAllExpiredDirectory();
        FalconXEventBeforeCommit();
        ReleaseCurrentSubTransaction();
        FalconXEventAfterCommit();
	}
	PG_CATCH();
    {
        expiredDirectoryHashTable = NULL;
        FlushErrorState();
        RollbackAndReleaseCurrentSubTransaction();
        FalconXEventAfterAbort();
        elog(WARNING, "falcon: get expired directory failed.");
    }
    PG_END_TRY();
    if (!expiredDirectoryHashTable)
        return -1;

    // 2. use 2pc to delete one by one
    HASH_SEQ_STATUS scan;
    ExpiredDirectoryEntry* entry;
    hash_seq_init(&scan, expiredDirectoryHashTable);
    while ((entry = (ExpiredDirectoryEntry*)hash_seq_search(&scan)))
    {
        // sub transaction doesn't trigger XactCallback, so we should call them mannually
        PG_TRY();
        {
            BeginInternalSubTransaction("delete_expired_directory");
            DeleteExpiredDirectory(entry->key.parentId, entry->key.name);
            FalconXEventBeforeCommit();
            ReleaseCurrentSubTransaction();
            FalconXEventAfterCommit();
        }
        PG_CATCH();
        {
            FlushErrorState();
            RollbackAndReleaseCurrentSubTransaction();
            FalconXEventAfterAbort();
            elog(WARNING, "falcon: delete expired directory failed.");
        }
        PG_END_TRY();
    }
    
    // 3. traverse deleted directory, clear sub directories and files
    int successClearCount = 0;
    PG_TRY();
    {
        BeginInternalSubTransaction("DeleteExpiredInodeId");
        successClearCount = DeleteFilesByExpiredInodeId();
        FalconXEventBeforeCommit();
        ReleaseCurrentSubTransaction();
        FalconXEventAfterCommit();
    }
    PG_CATCH();
    {
        FlushErrorState();
        RollbackAndReleaseCurrentSubTransaction();
        FalconXEventAfterAbort();
        elog(WARNING, "falcon: delete expired file by inode id failed.");
    }
    PG_END_TRY();

    return successClearCount;
}