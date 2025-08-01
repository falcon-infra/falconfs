/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "metadb/meta_handle.h"

#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#include "dir_path_shmem/dir_path_hash.h"
#include "distributed_backend/remote_comm_falcon.h"
#include "metadb/meta_process_info.h"
#include "metadb/meta_serialize_interface_helper.h"
#include "metadb/shard_table.h"
#include "utils/path_parse.h"
#include "utils/utils_standalone.h"

MemoryManager PgMemoryManager = {.alloc = palloc, .free = pfree, .realloc = repalloc};

#define BATCH_OPERATION_GROUP_SIZE 8

static inline uint16_t HashPartId(const char *fileName);
static inline uint64_t CombineParentIdWithPartId(uint64_t parent_id, uint16_t part_id);

static StringInfo GetInodeShardName(int shardId);
static StringInfo GetInodeIndexShardName(int shardId);
static StringInfo GetXattrShardName(int shardId);
static StringInfo GetXattrIndexShardName(int shardId);

static Oid GetRelationOidByName_FALCON(const char *relationName);
enum ModeCheckType
{
    MODE_CHECK_NONE,
    MODE_CHECK_MUST_BE_FILE,
    MODE_CHECK_MUST_BE_DIRECTORY
};
static bool SearchAndUpdateInodeTableInfo(const char *workerInodeRelationName,
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
                                          int32_t *backupNodeId);
static bool InsertIntoInodeTable(Relation relation,
                                 CatalogIndexState indexState,
                                 uint64_t st_ino,
                                 uint64_t parentid_partid,
                                 const char *name,
                                 uint64_t st_dev,
                                 uint32_t st_mode,
                                 uint64_t st_nlink,
                                 uint32_t st_uid,
                                 uint32_t st_gid,
                                 uint64_t st_rdev,
                                 int64_t st_size,
                                 int64_t st_blksize,
                                 int64_t st_blocks,
                                 TimestampTz st_atim,
                                 TimestampTz st_mtim,
                                 TimestampTz st_ctim,
                                 const char *etag,
                                 uint64_t update_version,
                                 int32_t primaryNodeId,
                                 int32_t backupNodeId);
// mistyped in original video as well
#define CHECK_ERROR_CODE_WITH_CONTINUE(errCode) \
    if ((errCode) != SUCCESS) {                 \
        info->errorCode = (errCode);            \
        continue;                               \
    }
#define CHECK_ERROR_CODE_WITH_RETURN(errCode) \
    if ((errCode) != SUCCESS) {               \
        info->errorCode = (errCode);          \
        return;                               \
    }

void FalconMkdirHandle(MetaProcessInfo *infoArray, int count)
{
    if (GetLocalServerId() != FALCON_CN_SERVER_ID)
        FALCON_ELOG_ERROR(WRONG_WORKER, "mkdir can only be called on CN.");

    // 1.
    for (int i = 0; i < count; ++i) {
        MetaProcessInfo info = infoArray[i];
        info->errorCode = SUCCESS;
        info->errorMsg = NULL;

        int32_t property;
        FalconErrorCode errorCode =
            VerifyPathValidity(info->path, VERIFY_PATH_VALIDITY_REQUIREMENT_MUST_BE_DIRECTORY, &property);
        CHECK_ERROR_CODE_WITH_CONTINUE(errorCode);
    }
    pg_qsort(infoArray, count, sizeof(MetaProcessInfo), pg_qsort_meta_process_info_by_path_cmp);

    RegisterLocalProcessFlag(false);

    int32_t *validInputIndexArray = palloc(sizeof(int32_t) * count);
    int validInputIndexArraySize = 0;
    Relation directoryRel = table_open(DirectoryRelationId(), RowExclusiveLock);
    CatalogIndexState indexState = CatalogOpenIndexes(directoryRel);
    for (int i = 0; i < count; ++i) {
        MetaProcessInfo info = infoArray[i];
        if (info->errorCode != SUCCESS)
            continue;

        FalconErrorCode errorCode =
            PathParseTreeInsert(NULL,
                                directoryRel,
                                info->path,
                                PATH_PARSE_FLAG_TARGET_IS_DIRECTORY | PATH_PARSE_FLAG_TARGET_TO_BE_CREATED |
                                    PATH_PARSE_FLAG_ALLOW_OPERATION_UNDER_CREATED_DIRECTORY,
                                &info->parentId,
                                &info->name,
                                &info->inodeId);
        CHECK_ERROR_CODE_WITH_CONTINUE(errorCode);
        InsertDirectoryByDirectoryHashTable(directoryRel,
                                            indexState,
                                            info->parentId,
                                            info->name,
                                            info->inodeId,
                                            DEFAULT_SUBPART_NUM,
                                            DIR_LOCK_NONE);
        validInputIndexArray[validInputIndexArraySize] = i;
        ++validInputIndexArraySize;
    }
    CatalogCloseIndexes(indexState);
    table_close(directoryRel, RowExclusiveLock);
    if (validInputIndexArraySize == 0)
        return;

    // 2.
    SerializedData subMkdirParam;
    SerializedDataInit(&subMkdirParam, NULL, 0, 0, &PgMemoryManager);
    SerializedDataMetaParamEncodeWithPerProcessFlatBufferBuilder(MKDIR_SUB_MKDIR,
                                                                 infoArray,
                                                                 validInputIndexArray,
                                                                 validInputIndexArraySize,
                                                                 &subMkdirParam);
    List *foreignServerIdList = GetAllForeignServerId(true, false);
    FalconMetaCallOnWorkerList(MKDIR_SUB_MKDIR,
                               validInputIndexArraySize,
                               subMkdirParam,
                               REMOTE_COMMAND_FLAG_WRITE,
                               foreignServerIdList);

    // 3.
    HASHCTL info;
    memset(&info, 0, sizeof(info));
    info.keysize = sizeof(int32_t);
    info.entrysize = sizeof(ServerHashInfo);
    info.hcxt = CurrentMemoryContext;
    int hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    HTAB *subCreateIndexListPerWorker =
        hash_create("Mkdir Sub Create Index List Per Worker Hash Table", FOREIGN_SERVER_NUM_EXPECT, &info, hashFlags);
    ServerHashInfo *entry;
    for (int i = 0; i < validInputIndexArraySize; ++i) {
        int index = validInputIndexArray[i];
        MetaProcessInfo metaProcessInfo = infoArray[index];

        uint16_t partId = HashPartId(metaProcessInfo->name);
        uint64_t parentId_partId = CombineParentIdWithPartId(metaProcessInfo->parentId, partId);
        int shardId, workerId;
        SearchShardInfoByShardValue(parentId_partId, &shardId, &workerId);

        metaProcessInfo->parentId_partId = parentId_partId;
        metaProcessInfo->st_mode = S_IFDIR | 0755;
        metaProcessInfo->st_mtim = GetCurrentTimestamp();
        metaProcessInfo->st_size = 0;

        bool found;
        entry = hash_search(subCreateIndexListPerWorker, &workerId, HASH_ENTER, &found);
        if (!found) {
            entry->serverId = workerId;
            entry->info = NIL;
        }
        entry->info = lappend_int(entry->info, index);
    }

    int32_t *validInputIndexArrayForSubCreate = palloc(sizeof(int32_t) * validInputIndexArraySize);
    int32_t validInputIndexArrayForSubCreateSize = 0;
    HASH_SEQ_STATUS status;
    hash_seq_init(&status, subCreateIndexListPerWorker);
    while ((entry = hash_seq_search(&status)) != 0) {
        validInputIndexArrayForSubCreateSize = list_length(entry->info);
        for (int i = 0; i < validInputIndexArrayForSubCreateSize; ++i)
            validInputIndexArrayForSubCreate[i] = list_nth_int(entry->info, i);

        SerializedData subCreateParam;
        SerializedDataInit(&subCreateParam, NULL, 0, 0, &PgMemoryManager);
        SerializedDataMetaParamEncodeWithPerProcessFlatBufferBuilder(MKDIR_SUB_CREATE,
                                                                     infoArray,
                                                                     validInputIndexArrayForSubCreate,
                                                                     validInputIndexArrayForSubCreateSize,
                                                                     &subCreateParam);
        FalconMetaCallOnWorkerList(MKDIR_SUB_CREATE,
                                   validInputIndexArrayForSubCreateSize,
                                   subCreateParam,
                                   REMOTE_COMMAND_FLAG_WRITE,
                                   list_make1_int(entry->serverId));
    }

    // 4.
    MultipleServerRemoteCommandResult totalRemoteRes = FalconSendCommandAndWaitForResult();

    MetaProcessInfoData *resArray = palloc(sizeof(MetaProcessInfoData) * validInputIndexArraySize);
    for (int i = 0; i < list_length(totalRemoteRes); ++i) {
        RemoteCommandResultPerServerData *remoteRes = list_nth(totalRemoteRes, i);
        int64_t serverId = remoteRes->serverId;

        if (list_length(remoteRes->remoteCommandResult) < 1 || list_length(remoteRes->remoteCommandResult) > 2)
            FALCON_ELOG_ERROR(PROGRAM_ERROR, "unexpected. situation");
        PGresult *res = NULL;

        // 4.1
        res = list_nth(remoteRes->remoteCommandResult, 0);
        if (PQntuples(res) != 1 || PQnfields(res) != 1)
            FALCON_ELOG_ERROR(REMOTE_QUERY_FAILED, "PGresult is corrupt.");
        SerializedData subMkdirResponse;
        SerializedDataInit(&subMkdirResponse,
                           PQgetvalue(res, 0, 0),
                           PQgetlength(res, 0, 0),
                           PQgetlength(res, 0, 0),
                           NULL);
        if (!SerializedDataMetaResponseDecode(MKDIR_SUB_MKDIR, validInputIndexArraySize, &subMkdirResponse, resArray))
            FALCON_ELOG_ERROR(ARGUMENT_ERROR, "serialized response is corrupt.");

        for (int j = 0; j < validInputIndexArraySize; ++j)
            if (resArray[j].errorCode != SUCCESS)
                FALCON_ELOG_ERROR(PROGRAM_ERROR,
                                  "MkdirSubMkdir is supposed to be successful, "
                                  "but it failed.");

        // 4.2
        if (list_length(remoteRes->remoteCommandResult) == 1)
            continue;
        res = list_nth(remoteRes->remoteCommandResult, 1);
        if (PQntuples(res) != 1 || PQnfields(res) != 1)
            FALCON_ELOG_ERROR(REMOTE_QUERY_FAILED, "PGresult is corrupt.");
        SerializedData subCreateResponse;
        SerializedDataInit(&subCreateResponse,
                           PQgetvalue(res, 0, 0),
                           PQgetlength(res, 0, 0),
                           PQgetlength(res, 0, 0),
                           NULL);

        bool found;
        entry = hash_search(subCreateIndexListPerWorker, &serverId, HASH_FIND, &found);
        if (!found)
            FALCON_ELOG_ERROR(PROGRAM_ERROR,
                              "serverId in MultipleServerRemoteCommandResult"
                              "dosen't exist in subCreateIndexListPerWorker. It's impossible.");
        int subCreateCount = list_length(entry->info);

        if (!SerializedDataMetaResponseDecode(MKDIR_SUB_CREATE, subCreateCount, &subCreateResponse, resArray))
            FALCON_ELOG_ERROR(ARGUMENT_ERROR, "serialized response is corrupt.");

        for (int j = 0; j < subCreateCount; ++j)
            if (resArray[j].errorCode != SUCCESS)
                FALCON_ELOG_ERROR(PROGRAM_ERROR,
                                  "MkdirSubCreate is supposed to be successful, "
                                  "but it failed.");
    }
}

void FalconMkdirSubMkdirHandle(MetaProcessInfo *infoArray, int count)
{
    //
    Relation directoryRel = table_open(DirectoryRelationId(), RowExclusiveLock);
    CatalogIndexState indexState = CatalogOpenIndexes(directoryRel);
    for (int i = 0; i < count; ++i) {
        MetaProcessInfo info = infoArray[i];

        //
        if (info->parentId == -1 || info->name == NULL)
            FALCON_ELOG_ERROR(ARGUMENT_ERROR, "FalconMkdirSubMkdir has received invalid input.");
        InsertDirectoryByDirectoryHashTable(directoryRel,
                                            indexState,
                                            info->parentId,
                                            info->name,
                                            info->inodeId,
                                            DEFAULT_SUBPART_NUM,
                                            DIR_LOCK_EXCLUSIVE);

        info->errorCode = SUCCESS;
    }
    CatalogCloseIndexes(indexState);
    table_close(directoryRel, RowExclusiveLock);
}

void FalconMkdirSubCreateHandle(MetaProcessInfo *infoArray, int count)
{
    HASHCTL info;
    memset(&info, 0, sizeof(info));
    info.keysize = sizeof(int32_t);
    info.entrysize = sizeof(ShardHashInfo);
    info.hcxt = CurrentMemoryContext;
    int hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    HTAB *batchMetaProcessInfoListPerShard =
        hash_create("Batch Meta Process Info List Per Shard Hash Table", GetShardTableSize(), &info, hashFlags);
    ShardHashInfo *entry;
    for (int i = 0; i < count; ++i) {
        MetaProcessInfo info = infoArray[i];

        //
        if (info->inodeId == -1 || info->parentId_partId == -1 || info->name == NULL)
            FALCON_ELOG_ERROR(ARGUMENT_ERROR, "FalconMkdirSubCreateHandle has received invalid input.");
        int shardId, workerId;
        SearchShardInfoByShardValue(info->parentId_partId, &shardId, &workerId);
        if (workerId != GetLocalServerId())
            FALCON_ELOG_ERROR(ARGUMENT_ERROR, "FalconMkdirSubCreate has received invalid input.");

        bool found;
        entry = hash_search(batchMetaProcessInfoListPerShard, &shardId, HASH_ENTER, &found);
        if (!found) {
            entry->shardId = shardId;
            entry->info = NIL;
        }

        entry->info = lappend(entry->info, info);
    }

    HASH_SEQ_STATUS status;
    hash_seq_init(&status, batchMetaProcessInfoListPerShard);
    while ((entry = hash_seq_search(&status)) != 0) {
        Relation workerInodeRel =
            table_open(GetRelationOidByName_FALCON(GetInodeShardName(entry->shardId)->data), RowExclusiveLock);
        CatalogIndexState indexState = CatalogOpenIndexes(workerInodeRel);

        for (int i = 0; i < list_length(entry->info); ++i) {
            MetaProcessInfo info = list_nth(entry->info, i);
            info->etag = (char *)"";

            InsertIntoInodeTable(workerInodeRel,
                                 indexState,
                                 info->inodeId,
                                 info->parentId_partId,
                                 info->name,
                                 0,
                                 info->st_mode,
                                 1,
                                 0,
                                 0,
                                 0,
                                 info->st_size,
                                 0,
                                 0,
                                 (TimestampTz)info->st_mtim,
                                 (TimestampTz)info->st_mtim,
                                 (TimestampTz)info->st_mtim,
                                 info->etag,
                                 0,
                                 -1,
                                 -1);

            info->errorCode = SUCCESS;
        }

        CatalogCloseIndexes(indexState);
        table_close(workerInodeRel, RowExclusiveLock);
    }
}

void FalconCreateHandle(MetaProcessInfo *infoArray, int count, bool updateExisted)
{
    for (int i = 0; i < count; ++i) {
        MetaProcessInfo info = infoArray[i];
        //
        info->errorCode = SUCCESS;
        info->errorMsg = NULL;

        int32_t property;
        //
        FalconErrorCode errorCode =
            VerifyPathValidity(info->path, VERIFY_PATH_VALIDITY_REQUIREMENT_MUST_BE_FILE, &property);
        CHECK_ERROR_CODE_WITH_CONTINUE(errorCode);
    }
    pg_qsort(infoArray, count, sizeof(MetaProcessInfo), pg_qsort_meta_process_info_by_path_cmp);

    HASHCTL info;
    memset(&info, 0, sizeof(info));
    info.keysize = sizeof(int32_t);
    info.entrysize = sizeof(ShardHashInfo);
    info.hcxt = CurrentMemoryContext;
    int hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    HTAB *batchMetaProcessInfoListPerShard =
        hash_create("Batch Meta Process Info List Per Shard Hash Table", GetShardTableSize(), &info, hashFlags);
    ShardHashInfo *entry;
    Relation directoryRel = table_open(DirectoryRelationId(), AccessShareLock);
    for (int i = 0; i < count; ++i) {
        MetaProcessInfo info = infoArray[i];
        if (info->errorCode != SUCCESS)
            continue;

        FalconErrorCode errorCode = PathParseTreeInsert(NULL,
                                                        directoryRel,
                                                        info->path,
                                                        PATH_PARSE_FLAG_NOT_ROOT | PATH_PARSE_FLAG_TARGET_TO_BE_CREATED,
                                                        &info->parentId,
                                                        &info->name,
                                                        &info->inodeId);
        CHECK_ERROR_CODE_WITH_CONTINUE(errorCode);

        uint16_t partId = HashPartId(info->name);
        info->parentId_partId = CombineParentIdWithPartId(info->parentId, partId);
        info->st_mode = S_IFREG | 0644;
        info->st_mtim = GetCurrentTimestamp();
        info->st_size = 0;
        info->node_id = -1;
        info->st_nlink = 1;
        info->etag = (char *)"";
        info->st_dev = 0;
        info->st_uid = 0;
        info->st_gid = 0;
        info->st_rdev = 0;
        info->st_blksize = 0;
        info->st_blocks = 0;
        info->st_atim = 0;
        info->st_ctim = 0;

        int shardId, workerId;
        SearchShardInfoByShardValue(info->parentId_partId, &shardId, &workerId);
        if (workerId != GetLocalServerId())
            CHECK_ERROR_CODE_WITH_CONTINUE(WRONG_WORKER);

        bool found;
        entry = hash_search(batchMetaProcessInfoListPerShard, &shardId, HASH_ENTER, &found);
        if (!found) {
            entry->shardId = shardId;
            entry->info = NIL;
        }
        entry->info = lappend(entry->info, info);
    }
    table_close(directoryRel, AccessShareLock);

    HASH_SEQ_STATUS status;
    hash_seq_init(&status, batchMetaProcessInfoListPerShard);
    while ((entry = hash_seq_search(&status)) != 0) {
        StringInfo inodeShardName = GetInodeShardName(entry->shardId);
        StringInfo inodeIndexShardName = GetInodeIndexShardName(entry->shardId);
        Oid inodeIndexOid = GetRelationOidByName_FALCON(inodeIndexShardName->data);

        List *toHandleMetaProcessList = NIL;
        for (int i = list_length(entry->info) - 1; i >= 0; --i) {
            MetaProcessInfo info = list_nth(entry->info, i);

            if (info->errorCode != SUCCESS)
                continue;
            toHandleMetaProcessList = lappend(toHandleMetaProcessList, info);
        }

        MetaProcessInfo info = NULL;
        while (list_length(toHandleMetaProcessList) != 0) {
            BeginInternalSubTransaction(NULL);
            Relation workerInodeRel = table_open(GetRelationOidByName_FALCON(inodeShardName->data), RowExclusiveLock);
            CatalogIndexState indexState = CatalogOpenIndexes(workerInodeRel);
            PG_TRY();
            {
                int currentGroupHandled = BATCH_OPERATION_GROUP_SIZE;
                int toHandleMetaProcessIndex = list_length(toHandleMetaProcessList) - 1;
                while (currentGroupHandled > 0 && toHandleMetaProcessIndex >= 0) {
                    // force the info writen to memory
                    *(volatile MetaProcessInfo *)(&info) = list_nth(toHandleMetaProcessList, toHandleMetaProcessIndex);
                    // info = list_nth(toHandleMetaProcessList, toHandleMetaProcessIndex);
                    --toHandleMetaProcessIndex;
                    if (info->errorCode != SUCCESS) {
                        if (info->errorCode == FILE_EXISTS) {
                            SearchAndUpdateInodeTableInfo(inodeShardName->data,
                                                          NULL,
                                                          inodeIndexShardName->data,
                                                          inodeIndexOid,
                                                          info->parentId_partId,
                                                          info->name,
                                                          false,
                                                          &info->inodeId,
                                                          &info->st_size,
                                                          NULL,
                                                          NULL,
                                                          &info->st_nlink,
                                                          0,
                                                          &info->st_mode,
                                                          NULL,
                                                          MODE_CHECK_NONE,
                                                          NULL,
                                                          NULL,
                                                          NULL,
                                                          NULL,
                                                          NULL,
                                                          &info->node_id,
                                                          NULL,
                                                          NULL);
                        }
                        continue;
                    }

                    InsertIntoInodeTable(workerInodeRel,
                                         indexState,
                                         info->inodeId,
                                         info->parentId_partId,
                                         info->name,
                                         0,
                                         info->st_mode,
                                         1,
                                         0,
                                         0,
                                         0,
                                         info->st_size,
                                         0,
                                         0,
                                         info->st_mtim,
                                         info->st_mtim,
                                         info->st_mtim,
                                         info->etag,
                                         0,
                                         -1,
                                         -1);
                    --currentGroupHandled;
                }
                CatalogCloseIndexes(indexState);
                table_close(workerInodeRel, RowExclusiveLock);
                ReleaseCurrentSubTransaction();

                toHandleMetaProcessList->length = toHandleMetaProcessIndex + 1;
            }
            PG_CATCH();
            {
                FlushErrorState();
                RollbackAndReleaseCurrentSubTransaction();

                //
                if (updateExisted) {
                    SearchAndUpdateInodeTableInfo(inodeShardName->data,
                                                  NULL,
                                                  inodeIndexShardName->data,
                                                  inodeIndexOid,
                                                  info->parentId_partId,
                                                  info->name,
                                                  true,
                                                  NULL,
                                                  NULL,
                                                  &info->st_size,
                                                  NULL,
                                                  NULL,
                                                  0,
                                                  NULL,
                                                  NULL,
                                                  MODE_CHECK_NONE,
                                                  NULL,
                                                  NULL,
                                                  info->etag,
                                                  NULL,
                                                  &info->st_mtim,
                                                  NULL,
                                                  NULL,
                                                  NULL);
                } else {
                    info->errorCode = FILE_EXISTS;
                }
            }
            PG_END_TRY();
        }
    }
}

void FalconStatHandle(MetaProcessInfo *infoArray, int count)
{
    for (int i = 0; i < count; ++i) {
        MetaProcessInfo info = infoArray[i];
        //
        info->errorCode = SUCCESS;
        info->errorMsg = NULL;

        int32_t property;
        //
        FalconErrorCode errorCode = VerifyPathValidity(info->path, 0, &property);
        CHECK_ERROR_CODE_WITH_CONTINUE(errorCode);
    }
    pg_qsort(infoArray, count, sizeof(MetaProcessInfo), pg_qsort_meta_process_info_by_path_cmp);

    SetUpScanCaches();

    HASHCTL info;
    memset(&info, 0, sizeof(info));
    info.keysize = sizeof(int32_t);
    info.entrysize = sizeof(ShardHashInfo);
    info.hcxt = CurrentMemoryContext;
    int hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    HTAB *batchMetaProcessInfoListPerShard =
        hash_create("Batch Meta Process Info List Per Shard Hash Table", GetShardTableSize(), &info, hashFlags);
    ShardHashInfo *entry;
    Relation directoryRel = table_open(DirectoryRelationId(), AccessShareLock);
    for (int i = 0; i < count; ++i) {
        MetaProcessInfo info = infoArray[i];
        if (info->errorCode != SUCCESS)
            continue;

        FalconErrorCode errorCode =
            PathParseTreeInsert(NULL, directoryRel, info->path, 0, &(info->parentId), &(info->name), NULL);
        CHECK_ERROR_CODE_WITH_CONTINUE(errorCode);

        uint16_t partId = HashPartId(info->name);
        info->parentId_partId = CombineParentIdWithPartId(info->parentId, partId);
        int shardId, workerId;
        SearchShardInfoByShardValue(info->parentId_partId, &shardId, &workerId);
        if (workerId != GetLocalServerId())
            CHECK_ERROR_CODE_WITH_CONTINUE(WRONG_WORKER);

        bool found;
        entry = hash_search(batchMetaProcessInfoListPerShard, &shardId, HASH_ENTER, &found);
        if (!found) {
            entry->shardId = shardId;
            entry->info = NULL;
        }
        entry->info = lappend(entry->info, info);
    }
    table_close(directoryRel, AccessShareLock);

    HASH_SEQ_STATUS status;
    hash_seq_init(&status, batchMetaProcessInfoListPerShard);
    while ((entry = hash_seq_search(&status)) != NULL) {
        StringInfo inodeShardName = GetInodeShardName(entry->shardId);
        StringInfo inodeIndexShardName = GetInodeIndexShardName(entry->shardId);
        Relation workerInodeRel = table_open(GetRelationOidByName_FALCON(inodeShardName->data), AccessShareLock);
        Oid workerInodeIndexOid = GetRelationOidByName_FALCON(inodeIndexShardName->data);

        for (int i = 0; i < list_length(entry->info); ++i) {
            MetaProcessInfo info = list_nth(entry->info, i);

            ScanKeyData scanKey[2];
            int scanKeyCount = 2;
            SysScanDesc scanDescriptor = NULL;
            HeapTuple heapTuple;

            scanKey[0] = InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_EQ];
            scanKey[0].sk_argument = UInt64GetDatum(info->parentId_partId);
            scanKey[1] = InodeTableScanKey[INODE_TABLE_NAME_EQ];
            scanKey[1].sk_argument = CStringGetTextDatum(info->name);

            scanDescriptor = systable_beginscan(workerInodeRel,
                                                workerInodeIndexOid,
                                                true,
                                                GetTransactionSnapshot(),
                                                scanKeyCount,
                                                scanKey);
            heapTuple = systable_getnext(scanDescriptor);
            TupleDesc tupleDesc = RelationGetDescr(workerInodeRel);
            if (!HeapTupleIsValid(heapTuple)) {
                info->errorCode = FILE_NOT_EXISTS;
            } else {
                Datum datumArray[Natts_pg_dfs_inode_table];
                bool isNullArray[Natts_pg_dfs_inode_table];
                heap_deform_tuple(heapTuple, tupleDesc, datumArray, isNullArray);
                info->inodeId = DatumGetUInt64(datumArray[Anum_pg_dfs_file_st_ino - 1]);
                info->st_dev = DatumGetUInt64(datumArray[Anum_pg_dfs_file_st_dev - 1]);
                info->st_mode = DatumGetUInt32(datumArray[Anum_pg_dfs_file_st_mode - 1]);
                info->st_nlink = DatumGetUInt64(datumArray[Anum_pg_dfs_file_st_nlink - 1]);
                info->st_uid = DatumGetUInt32(datumArray[Anum_pg_dfs_file_st_uid - 1]);
                info->st_gid = DatumGetUInt32(datumArray[Anum_pg_dfs_file_st_gid - 1]);
                info->st_rdev = DatumGetUInt64(datumArray[Anum_pg_dfs_file_st_rdev - 1]);
                info->st_size = DatumGetInt64(datumArray[Anum_pg_dfs_file_st_size - 1]);
                info->st_blksize = DatumGetInt64(datumArray[Anum_pg_dfs_file_st_blksize - 1]);
                info->st_blocks = DatumGetInt64(datumArray[Anum_pg_dfs_file_st_blocks - 1]);
                info->st_atim = DatumGetInt64(datumArray[Anum_pg_dfs_file_st_atim - 1]);
                info->st_mtim = DatumGetInt64(datumArray[Anum_pg_dfs_file_st_mtim - 1]);
                info->st_ctim = DatumGetInt64(datumArray[Anum_pg_dfs_file_st_ctim - 1]);
                info->etag = TextDatumGetCString(datumArray[Anum_pg_dfs_file_etag - 1]);
            }
            systable_endscan(scanDescriptor);
        }

        table_close(workerInodeRel, AccessShareLock);
    }
}

void FalconOpenHandle(MetaProcessInfo *infoArray, int count)
{
    for (int i = 0; i < count; ++i) {
        MetaProcessInfo info = infoArray[i];
        info->errorCode = SUCCESS;
        info->errorMsg = NULL;

        int32_t property;
        FalconErrorCode errorCode = VerifyPathValidity(info->path, 0, &property);
        CHECK_ERROR_CODE_WITH_CONTINUE(errorCode);
    }
    pg_qsort(infoArray, count, sizeof(MetaProcessInfo), pg_qsort_meta_process_info_by_path_cmp);

    HASHCTL info;
    memset(&info, 0, sizeof(info));
    info.keysize = sizeof(int32_t);
    info.entrysize = sizeof(ShardHashInfo);
    info.hcxt = CurrentMemoryContext;
    int hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    HTAB *batchMetaProcessInfoListPerShard =
        hash_create("Batch Meta Process Info List Per Shard Hash Table", GetShardTableSize(), &info, hashFlags);
    ShardHashInfo *entry;
    Relation directoryRel = table_open(DirectoryRelationId(), AccessShareLock);
    for (int i = 0; i < count; ++i) {
        MetaProcessInfo info = infoArray[i];
        if (info->errorCode != SUCCESS)
            continue;

        FalconErrorCode errorCode = PathParseTreeInsert(NULL,
                                                        directoryRel,
                                                        info->path,
                                                        PATH_PARSE_FLAG_ACQUIRE_SHARED_LOCK_IF_TARGET_IS_DIRECTORY,
                                                        &info->parentId,
                                                        &info->name,
                                                        NULL);
        CHECK_ERROR_CODE_WITH_CONTINUE(errorCode);

        uint16_t partId = HashPartId(info->name);
        info->parentId_partId = CombineParentIdWithPartId(info->parentId, partId);
        int shardId, workerId;
        SearchShardInfoByShardValue(info->parentId_partId, &shardId, &workerId);
        if (workerId != GetLocalServerId())
            CHECK_ERROR_CODE_WITH_CONTINUE(WRONG_WORKER);

        bool found;
        entry = hash_search(batchMetaProcessInfoListPerShard, &shardId, HASH_ENTER, &found);
        if (!found) {
            entry->shardId = shardId;
            entry->info = NULL;
        }
        entry->info = lappend(entry->info, info);
    }
    table_close(directoryRel, AccessShareLock);

    HASH_SEQ_STATUS status;
    hash_seq_init(&status, batchMetaProcessInfoListPerShard);
    while ((entry = hash_seq_search(&status)) != 0) {
        StringInfo inodeShardName = GetInodeShardName(entry->shardId);
        StringInfo inodeIndexShardName = GetInodeIndexShardName(entry->shardId);

        for (int i = 0; i < list_length(entry->info); ++i) {
            MetaProcessInfo info = list_nth(entry->info, i);

            if (info->errorCode != SUCCESS)
                continue;

            bool fileExist = SearchAndUpdateInodeTableInfo(inodeShardName->data,
                                                           NULL,
                                                           inodeIndexShardName->data,
                                                           InvalidOid,
                                                           info->parentId_partId,
                                                           info->name,
                                                           false,
                                                           &info->inodeId,
                                                           &info->st_size,
                                                           NULL,
                                                           NULL,
                                                           &info->st_nlink,
                                                           0,
                                                           &info->st_mode,
                                                           NULL,
                                                           MODE_CHECK_NONE,
                                                           NULL,
                                                           NULL,
                                                           NULL,
                                                           NULL,
                                                           NULL,
                                                           &info->node_id,
                                                           NULL,
                                                           NULL);
            info->st_dev = 0;
            info->st_uid = 0;
            info->st_gid = 0;
            info->st_rdev = 0;
            info->st_blksize = 0;
            info->st_blocks = 0;
            info->st_atim = 0;
            info->st_mtim = 0;
            info->st_ctim = 0;
            info->etag = (char *)"";
            if (!fileExist)
                info->errorCode = FILE_NOT_EXISTS;
            else
                info->errorCode = SUCCESS;
        }
    }
}

void FalconCloseHandle(MetaProcessInfo *infoArray, int count)
{
    for (int i = 0; i < count; ++i) {
        MetaProcessInfo info = infoArray[i];
        info->errorCode = SUCCESS;
        info->errorMsg = NULL;

        int32_t property;
        FalconErrorCode errorCode = VerifyPathValidity(info->path, 0, &property);
        CHECK_ERROR_CODE_WITH_CONTINUE(errorCode);
    }
    pg_qsort(infoArray, count, sizeof(MetaProcessInfo), pg_qsort_meta_process_info_by_path_cmp);

    HASHCTL info;
    memset(&info, 0, sizeof(info));
    info.keysize = sizeof(int32_t);
    info.entrysize = sizeof(ShardHashInfo);
    info.hcxt = CurrentMemoryContext;
    int hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    HTAB *batchMetaProcessInfoListPerShard =
        hash_create("Batch Meta Process Info List Per Shard Hash Table", GetShardTableSize(), &info, hashFlags);
    ShardHashInfo *entry;
    Relation directoryRel = table_open(DirectoryRelationId(), AccessShareLock);
    for (int i = 0; i < count; ++i) {
        MetaProcessInfo info = infoArray[i];
        if (info->errorCode != SUCCESS)
            continue;

        FalconErrorCode errorCode = PathParseTreeInsert(NULL,
                                                        directoryRel,
                                                        info->path,
                                                        PATH_PARSE_FLAG_ACQUIRE_SHARED_LOCK_IF_TARGET_IS_DIRECTORY,
                                                        &info->parentId,
                                                        &info->name,
                                                        NULL);
        CHECK_ERROR_CODE_WITH_CONTINUE(errorCode);

        uint16_t partId = HashPartId(info->name);
        info->parentId_partId = CombineParentIdWithPartId(info->parentId, partId);
        int shardId, workerId;
        SearchShardInfoByShardValue(info->parentId_partId, &shardId, &workerId);
        if (workerId != GetLocalServerId())
            CHECK_ERROR_CODE_WITH_CONTINUE(WRONG_WORKER);

        bool found;
        entry = hash_search(batchMetaProcessInfoListPerShard, &shardId, HASH_ENTER, &found);
        if (!found) {
            entry->shardId = shardId;
            entry->info = NULL;
        }
        entry->info = lappend(entry->info, info);
    }
    table_close(directoryRel, AccessShareLock);

    HASH_SEQ_STATUS status;
    hash_seq_init(&status, batchMetaProcessInfoListPerShard);
    while ((entry = hash_seq_search(&status)) != 0) {
        StringInfo inodeShardName = GetInodeShardName(entry->shardId);
        StringInfo inodeIndexShardName = GetInodeIndexShardName(entry->shardId);

        for (int i = 0; i < list_length(entry->info); ++i) {
            MetaProcessInfo info = list_nth(entry->info, i);

            if (info->errorCode != SUCCESS)
                continue;

            int64_t size = info->st_size;
            int64_t mtime = GetCurrentTimestamp();
            int32_t nodeId = info->node_id;
            bool fileExist = SearchAndUpdateInodeTableInfo(inodeShardName->data,
                                                           NULL,
                                                           inodeIndexShardName->data,
                                                           InvalidOid,
                                                           info->parentId_partId,
                                                           info->name,
                                                           true,
                                                           NULL,
                                                           NULL,
                                                           &size,
                                                           NULL,
                                                           NULL,
                                                           0,
                                                           NULL,
                                                           NULL,
                                                           MODE_CHECK_NONE,
                                                           NULL,
                                                           NULL,
                                                           NULL,
                                                           NULL,
                                                           &mtime,
                                                           NULL,
                                                           &nodeId,
                                                           NULL);
            if (!fileExist)
                info->errorCode = FILE_NOT_EXISTS;
            else
                info->errorCode = SUCCESS;
        }
    }
}

void FalconUnlinkHandle(MetaProcessInfo *infoArray, int count)
{
    for (int i = 0; i < count; ++i) {
        MetaProcessInfo info = infoArray[i];
        info->errorCode = SUCCESS;
        info->errorMsg = NULL;

        int32_t property;
        FalconErrorCode errorCode =
            VerifyPathValidity(info->path, VERIFY_PATH_VALIDITY_REQUIREMENT_MUST_BE_FILE, &property);
        CHECK_ERROR_CODE_WITH_CONTINUE(errorCode);
    }
    pg_qsort(infoArray, count, sizeof(MetaProcessInfo), pg_qsort_meta_process_info_by_path_cmp);

    HASHCTL info;
    memset(&info, 0, sizeof(info));
    info.keysize = sizeof(int32_t);
    info.entrysize = sizeof(ShardHashInfo);
    info.hcxt = CurrentMemoryContext;
    int hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
    HTAB *batchMetaProcessInfoListPerShard =
        hash_create("Batch Meta Process Info List Per Shard Hash Table", GetShardTableSize(), &info, hashFlags);
    ShardHashInfo *entry;
    Relation directoryRel = table_open(DirectoryRelationId(), AccessShareLock);
    for (int i = 0; i < count; ++i) {
        MetaProcessInfo info = infoArray[i];
        if (info->errorCode != SUCCESS)
            continue;

        FalconErrorCode errorCode =
            PathParseTreeInsert(NULL, directoryRel, info->path, 0, &info->parentId, &info->name, NULL);
        CHECK_ERROR_CODE_WITH_CONTINUE(errorCode);

        uint16_t partId = HashPartId(info->name);
        info->parentId_partId = CombineParentIdWithPartId(info->parentId, partId);
        int shardId, workerId;
        SearchShardInfoByShardValue(info->parentId_partId, &shardId, &workerId);
        if (workerId != GetLocalServerId())
            CHECK_ERROR_CODE_WITH_CONTINUE(WRONG_WORKER);

        bool found;
        entry = hash_search(batchMetaProcessInfoListPerShard, &shardId, HASH_ENTER, &found);
        if (!found) {
            entry->shardId = shardId;
            entry->info = NIL;
        }
        entry->info = lappend(entry->info, info);
    }
    table_close(directoryRel, AccessShareLock);

    HASH_SEQ_STATUS status;
    hash_seq_init(&status, batchMetaProcessInfoListPerShard);
    while ((entry = hash_seq_search(&status)) != 0) 
    {
        StringInfo inodeShardName = GetInodeShardName(entry->shardId);
        StringInfo inodeIndexShardName = GetInodeIndexShardName(entry->shardId);

        for (int i = 0; i < list_length(entry->info); ++i) {
            MetaProcessInfo info = list_nth(entry->info, i);

            if (info->errorCode != SUCCESS)
                continue;

            uint64_t nlink;
            mode_t mode;
            bool fileExist = SearchAndUpdateInodeTableInfo(inodeShardName->data,
                                                           NULL,
                                                           inodeIndexShardName->data,
                                                           InvalidOid,
                                                           info->parentId_partId,
                                                           info->name,
                                                           true,
                                                           &info->inodeId,
                                                           &info->st_size,
                                                           NULL,
                                                           NULL,
                                                           &nlink,
                                                           -1,
                                                           &mode,
                                                           NULL,
                                                           MODE_CHECK_MUST_BE_FILE,
                                                           NULL,
                                                           NULL,
                                                           NULL,
                                                           NULL,
                                                           NULL,
                                                           &info->node_id,
                                                           NULL,
                                                           NULL);
            if (!fileExist)
                info->errorCode = FILE_NOT_EXISTS;
            else if (!S_ISREG(mode))
                info->errorCode = PATH_VERIFY_FAILED;
            else if (nlink != 1)
                info->errorCode = PROGRAM_ERROR;
            else
                info->errorCode = SUCCESS;
        }
    }
}

void FalconReadDirHandle(MetaProcessInfo info)
{
    const char *path = info->path;
    int32_t maxReadCount = info->readDirMaxReadCount;
    if (maxReadCount == -1)
        maxReadCount = INT32_MAX;
    int32_t lastShardIndex = info->readDirLastShardIndex;
    const char *lastFileName = info->readDirLastFileName;

    int32_t property;
    VerifyPathValidity(path, VERIFY_PATH_VALIDITY_REQUIREMENT_MUST_BE_DIRECTORY, &property);

    uint64_t directoryId;
    uint64_t parentId;
    Relation directoryRel = table_open(DirectoryRelationId(), AccessShareLock);
    FalconErrorCode errorCode = PathParseTreeInsert(NULL,
                                                    directoryRel,
                                                    path,
                                                    PATH_PARSE_FLAG_ACQUIRE_SHARED_LOCK_IF_TARGET_IS_DIRECTORY |
                                                        PATH_PARSE_FLAG_TARGET_IS_DIRECTORY,
                                                    &parentId,
                                                    NULL,
                                                    &directoryId);
    if (errorCode != SUCCESS)
        FALCON_ELOG_ERROR(errorCode, "path parse error.");
    table_close(directoryRel, AccessShareLock);

    bool firstCall = lastShardIndex == -1;

    SetUpScanCaches();
    List *shardTableData = GetShardTableData();
    int shardTableCount = list_length(shardTableData);

    StreamSearchState state = firstCall ? NEW_SHARD : SAME_ID_GREATER_NAME;
    List *resultList = NIL;
    int32_t readCount = 0;
    int shardIndex = firstCall ? 0 : lastShardIndex;
    while (shardIndex < shardTableCount) {
        int workerId = ((FormData_falcon_shard_table *)list_nth(shardTableData, shardIndex))->server_id;
        int shardId = ((FormData_falcon_shard_table *)list_nth(shardTableData, shardIndex))->range_point;
        if (workerId != GetLocalServerId()) {
            ++shardIndex;
            continue;
        }

        uint64_t lowerId = CombineParentIdWithPartId(directoryId, 0);
        uint64_t upperId = CombineParentIdWithPartId(directoryId, PART_ID_MASK);

        StringInfo inodeShardName = GetInodeShardName(shardId);
        StringInfo inodeIndexShardName = GetInodeIndexShardName(shardId);

        ScanKeyData scanKey[2];
        int scanKeyCount = 2;
        uint16_t partId;
        uint64_t parentId_partId;

        switch (state) {
        case SAME_ID_GREATER_NAME:
            partId = HashPartId(lastFileName);
            parentId_partId = CombineParentIdWithPartId(directoryId, partId);

            scanKey[0] = InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_EQ];
            scanKey[0].sk_argument = UInt64GetDatum(parentId_partId);
            scanKey[1] = InodeTableScanKey[INODE_TABLE_NAME_GT];
            scanKey[1].sk_argument = CStringGetTextDatum(lastFileName);

            state = GREATER_ID;
            break;
        case GREATER_ID:
            partId = HashPartId(lastFileName);
            parentId_partId = CombineParentIdWithPartId(directoryId, partId);

            scanKey[0] = InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_GT];
            scanKey[0].sk_argument = UInt64GetDatum(parentId_partId);
            scanKey[1] = InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_LE];
            scanKey[1].sk_argument = UInt64GetDatum(upperId);

            state = NEW_SHARD;
            break;
        case NEW_SHARD:
            scanKey[0] = InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_GE];
            scanKey[0].sk_argument = UInt64GetDatum(lowerId);
            scanKey[1] = InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_LE];
            scanKey[1].sk_argument = UInt64GetDatum(upperId);
            break;
        default:
            FALCON_ELOG_ERROR(PROGRAM_ERROR, "wrong state in FalconReadDirHandle.");
        }

        Relation workerInodeRel = table_open(GetRelationOidByName_FALCON(inodeShardName->data), AccessShareLock);
        SysScanDesc scanDescriptor = systable_beginscan(workerInodeRel,
                                                        GetRelationOidByName_FALCON(inodeIndexShardName->data),
                                                        true,
                                                        GetTransactionSnapshot(),
                                                        scanKeyCount,
                                                        scanKey);
        TupleDesc tupleDescriptor = RelationGetDescr(workerInodeRel);

        Datum datum;
        bool isNull;
        HeapTuple heapTuple;
        while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor))) {
            OneReadDirResult *result = (OneReadDirResult *)palloc(sizeof(OneReadDirResult));

            datum = heap_getattr(heapTuple, Anum_pg_dfs_file_name, tupleDescriptor, &isNull);
            if (isNull)
                FALCON_ELOG_ERROR(PROGRAM_ERROR, "file name cannot be NULL.");
            result->fileName = TextDatumGetCString(datum);

            datum = heap_getattr(heapTuple, Anum_pg_dfs_file_st_mode, tupleDescriptor, &isNull);
            if (isNull)
                FALCON_ELOG_ERROR(PROGRAM_ERROR, "mode cannot be NULL.");
            result->mode = DatumGetUInt32(datum);

            resultList = lappend(resultList, result);
            readCount++;
            if (readCount >= maxReadCount)
                break;
        }

        systable_endscan(scanDescriptor);
        table_close(workerInodeRel, AccessShareLock);

        if (readCount >= maxReadCount)
            break;

        if (state == NEW_SHARD)
            ++shardIndex;
    }

    bool lastCall = readCount < maxReadCount;
    if (lastCall) {
        info->readDirLastShardIndex = -1;
        info->readDirLastFileName = NULL;
    } else {
        info->readDirLastShardIndex = shardIndex;
        info->readDirLastFileName = ((OneReadDirResult *)lfirst(list_last_cell(resultList)))->fileName;
    }
    if (resultList != NULL)
        info->readDirResultList = (OneReadDirResult **)resultList->elements;
    else
        info->readDirResultList = NULL;
    info->readDirResultCount = list_length(resultList);
    info->errorCode = SUCCESS;
}
void FalconOpenDirHandle(MetaProcessInfo info)
{
    const char *path = info->path;
    int32_t property;
    VerifyPathValidity(path, VERIFY_PATH_VALIDITY_REQUIREMENT_MUST_BE_DIRECTORY, &property);

    Relation directoryRel = table_open(DirectoryRelationId(), AccessShareLock);
    uint64_t directoryId;
    FalconErrorCode errorCode = PathParseTreeInsert(NULL,
                                                    directoryRel,
                                                    path,
                                                    PATH_PARSE_FLAG_ACQUIRE_SHARED_LOCK_IF_TARGET_IS_DIRECTORY |
                                                        PATH_PARSE_FLAG_TARGET_IS_DIRECTORY,
                                                    NULL,
                                                    NULL,
                                                    &directoryId);
    if (errorCode != SUCCESS)
        FALCON_ELOG_ERROR(errorCode, "path parse error.");
    table_close(directoryRel, AccessShareLock);

    info->inodeId = directoryId;
    info->errorCode = SUCCESS;
}

void FalconRmdirHandle(MetaProcessInfo info)
{
    const char *path = info->path;

    if (GetLocalServerId() != FALCON_CN_SERVER_ID)
        FALCON_ELOG_ERROR(WRONG_WORKER, "rmdir can only be called on CN.");

    // 1.
    int32_t property;
    FalconErrorCode errorCode = VerifyPathValidity(path, VERIFY_PATH_VALIDITY_REQUIREMENT_MUST_BE_DIRECTORY, &property);
    if (errorCode != SUCCESS)
        FALCON_ELOG_ERROR(errorCode, "path verify error.");

    RegisterLocalProcessFlag(false);

    Relation directoryRel = table_open(DirectoryRelationId(), RowExclusiveLock);
    errorCode = PathParseTreeInsert(NULL,
                                    directoryRel,
                                    info->path,
                                    PATH_PARSE_FLAG_TARGET_IS_DIRECTORY | PATH_PARSE_FLAG_TARGET_TO_BE_DELETED |
                                        PATH_PARSE_FLAG_NOT_ROOT,
                                    &info->parentId,
                                    &info->name,
                                    &info->inodeId);
    if (errorCode != SUCCESS)
        FALCON_ELOG_ERROR(errorCode, "path parse error.");
    DeleteDirectoryByDirectoryHashTable(directoryRel, info->parentId, info->name, DIR_LOCK_NONE);
    table_close(directoryRel, RowExclusiveLock);

    // 2.
    SerializedData subRmdirParam;
    SerializedDataInit(&subRmdirParam, NULL, 0, 0, &PgMemoryManager);
    SerializedDataMetaParamEncodeWithPerProcessFlatBufferBuilder(RMDIR_SUB_RMDIR, &info, NULL, 1, &subRmdirParam);
    List *foreignServerIdList = GetAllForeignServerId(true, false);
    FalconMetaCallOnWorkerList(RMDIR_SUB_RMDIR, 1, subRmdirParam, REMOTE_COMMAND_FLAG_WRITE, foreignServerIdList);

    // 3.
    uint16_t partId = HashPartId(info->name);
    info->parentId_partId = CombineParentIdWithPartId(info->parentId, partId);
    int shardId, workerId;
    SearchShardInfoByShardValue(info->parentId_partId, &shardId, &workerId);

    SerializedData subUnlinkParam;
    SerializedDataInit(&subUnlinkParam, NULL, 0, 0, &PgMemoryManager);
    SerializedDataMetaParamEncodeWithPerProcessFlatBufferBuilder(RMDIR_SUB_UNLINK, &info, NULL, 1, &subUnlinkParam);
    FalconMetaCallOnWorkerList(RMDIR_SUB_UNLINK,
                               1,
                               subUnlinkParam,
                               REMOTE_COMMAND_FLAG_WRITE,
                               list_make1_int(workerId));

    // 4.
    MultipleServerRemoteCommandResult totalRemoteRes = FalconSendCommandAndWaitForResult();

    MetaProcessInfoData responseInfo;
    for (int i = 0; i < list_length(totalRemoteRes); ++i) {
        RemoteCommandResultPerServerData *remoteRes = list_nth(totalRemoteRes, i);
        int64_t serverId = remoteRes->serverId;

        if (list_length(remoteRes->remoteCommandResult) < 1 || list_length(remoteRes->remoteCommandResult) > 2)
            FALCON_ELOG_ERROR(PROGRAM_ERROR, "unexpected situation.");
        PGresult *res = NULL;

        // 4.1
        res = list_nth(remoteRes->remoteCommandResult, 0);
        if (PQntuples(res) != 1 || PQnfields(res) != 1)
            FALCON_ELOG_ERROR(REMOTE_QUERY_FAILED, "PGresult is corrupt.");
        SerializedData subRmdirResponse;
        SerializedDataInit(&subRmdirResponse,
                           PQgetvalue(res, 0, 0),
                           PQgetlength(res, 0, 0),
                           PQgetlength(res, 0, 0),
                           NULL);
        if (!SerializedDataMetaResponseDecode(RMDIR_SUB_RMDIR, 1, &subRmdirResponse, &responseInfo))
            FALCON_ELOG_ERROR(ARGUMENT_ERROR, "serialized response is corrupt.");

        if (responseInfo.errorCode != SUCCESS)
            FALCON_ELOG_ERROR(PROGRAM_ERROR,
                              "RmdirSubRmdir is supposed to be successful, "
                              "but it failed.");

        // 4.2
        if (list_length(remoteRes->remoteCommandResult) == 1)
            continue;
        res = list_nth(remoteRes->remoteCommandResult, 1);
        if (PQntuples(res) != 1 || PQnfields(res) != 1)
            FALCON_ELOG_ERROR(REMOTE_QUERY_FAILED, "PGresult is corrupt.");
        SerializedData subUnlinkResponse;
        SerializedDataInit(&subUnlinkResponse,
                           PQgetvalue(res, 0, 0),
                           PQgetlength(res, 0, 0),
                           PQgetlength(res, 0, 0),
                           NULL);

        if (serverId != workerId)
            FALCON_ELOG_ERROR(PROGRAM_ERROR, "unexpected situation.");

        if (!SerializedDataMetaResponseDecode(RMDIR_SUB_UNLINK, 1, &subUnlinkResponse, &responseInfo))
            FALCON_ELOG_ERROR(ARGUMENT_ERROR, "serialized response is corrupt.");

        if (responseInfo.errorCode != SUCCESS)
            FALCON_ELOG_ERROR(PROGRAM_ERROR,
                              "RmdirSubUnlink is supposed to be successful, "
                              "but it failed.");
    }
}

void FalconRmdirSubRmdirHandle(MetaProcessInfo info)
{
    uint64_t parentId = info->parentId;
    char *name = info->name;

    // 1.
    Relation rel = table_open(DirectoryRelationId(), RowExclusiveLock);
    uint64_t directoryId = SearchDirectoryByDirectoryHashTable(rel, parentId, name, DIR_LOCK_EXCLUSIVE);
    if (directoryId == DIR_HASH_TABLE_PATH_NOT_EXIST)
        FALCON_ELOG_ERROR(FILE_NOT_EXISTS, "FalconRmdirSubRmdirHandle: unexpected.");
    DeleteDirectoryByDirectoryHashTable(rel, parentId, name, DIR_LOCK_NONE);
    table_close(rel, RowExclusiveLock);

    // 2.
    SetUpScanCaches();
    List *shardTableData = GetShardTableData();
    for (int i = 0; i < list_length(shardTableData); ++i) {
        int32_t workerId = ((FormData_falcon_shard_table *)list_nth(shardTableData, i))->server_id;
        int32_t shardId = ((FormData_falcon_shard_table *)list_nth(shardTableData, i))->range_point;
        if (workerId != GetLocalServerId())
            continue;

        uint64_t lowerId = CombineParentIdWithPartId(directoryId, 0);
        uint64_t upperId = CombineParentIdWithPartId(directoryId, PART_ID_MASK);

        StringInfo inodeShardName = GetInodeShardName(shardId);
        StringInfo inodeIndexShardName = GetInodeIndexShardName(shardId);

        ScanKeyData scanKey[2];
        int scanKeyCount = 2;
        scanKey[0] = InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_GE];
        scanKey[0].sk_argument = UInt64GetDatum(lowerId);
        scanKey[1] = InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_LE];
        scanKey[1].sk_argument = UInt64GetDatum(upperId);
        Relation workerInodeRel = table_open(GetRelationOidByName_FALCON(inodeShardName->data), AccessShareLock);
        SysScanDesc scanDescriptor = systable_beginscan(workerInodeRel,
                                                        GetRelationOidByName_FALCON(inodeIndexShardName->data),
                                                        true,
                                                        GetTransactionSnapshot(),
                                                        scanKeyCount,
                                                        scanKey);
        HeapTuple heapTuple;
        while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
            FALCON_ELOG_ERROR_EXTENDED(ARGUMENT_ERROR,
                                       "target " UINT64_PRINT_SYMBOL ":%s is not empty.",
                                       parentId,
                                       name);

        systable_endscan(scanDescriptor);
        table_close(workerInodeRel, AccessShareLock);
    }

    info->errorCode = SUCCESS;
}

void FalconRmdirSubUnlinkHandle(MetaProcessInfo info)
{
    uint64_t parentId_partId = info->parentId_partId;
    char *name = info->name;

    int shardId, workerId;
    SearchShardInfoByShardValue(parentId_partId, &shardId, &workerId);
    if (workerId != GetLocalServerId())
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "FalconRmdirSubUnlinkHandle has received invalid input.");

    StringInfo inodeShardName = GetInodeShardName(shardId);
    StringInfo inodeIndexShardName = GetInodeIndexShardName(shardId);
    uint64_t nlink;
    bool fileExist = SearchAndUpdateInodeTableInfo(inodeShardName->data,
                                                   NULL,
                                                   inodeIndexShardName->data,
                                                   InvalidOid,
                                                   parentId_partId,
                                                   name,
                                                   true,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   &nlink,
                                                   -1,
                                                   NULL,
                                                   NULL,
                                                   MODE_CHECK_NONE,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL);
    if (!fileExist)
        FALCON_ELOG_ERROR_EXTENDED(FILE_NOT_EXISTS,
                                   UINT64_PRINT_SYMBOL ":%s is not existed in inode_table.",
                                   parentId_partId,
                                   name);
    if (nlink != 1)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "unexpected.");

    info->errorCode = SUCCESS;
}

void FalconRenameHandle(MetaProcessInfo info)
{
    const char *srcPath = info->path;
    const char *dstPath = info->dstPath;

    int32_t srcProperty, dstProperty;
    VerifyPathValidity(srcPath, 0, &srcProperty);
    VerifyPathValidity(dstPath, 0, &dstProperty);

    if (GetLocalServerId() != 0)
        FALCON_ELOG_ERROR(WRONG_WORKER, "rename can only be called on CN.");

    // 1.
    Relation directoryRel = table_open(DirectoryRelationId(), RowExclusiveLock);
    uint64_t srcDirectoryId = CheckWhetherPathExistsInDirectoryTable(directoryRel, srcPath);
    bool renameDirectory = (srcDirectoryId != DIR_HASH_TABLE_PATH_NOT_EXIST);
    if (renameDirectory && !(srcProperty & VERIFY_PATH_VALIDITY_PROPERTY_CAN_BE_DIRECTORY))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "src is expected to be file, but it seems to be directory.");
    if (!renameDirectory && !(srcProperty & VERIFY_PATH_VALIDITY_PROPERTY_CAN_BE_FILE))
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "src is expected to be directory, but it seems to be file.");

    // 2.
    int srcIndex = 0;
    int dstIndex = 1;
    const char *pathArray[2] = {srcPath, dstPath};
    bool pathIsDst[2] = {0, 1};
    uint64_t parentId[2];
    char *name[2];
    uint64_t directoryId[2]; //
    if (pathcmp(srcPath, dstPath) > 0) {
        pathArray[0] = dstPath;
        pathIsDst[0] = 1;
        pathArray[1] = srcPath;
        pathIsDst[1] = 0;
        srcIndex = 1;
        dstIndex = 0;
    }
    for (int i = 0; i < 2; ++i) {
        uint32_t flag = PATH_PARSE_FLAG_NOT_ROOT;
        if (renameDirectory)
            flag |= PATH_PARSE_FLAG_TARGET_IS_DIRECTORY;
        if (pathIsDst[i]) {
            flag |= PATH_PARSE_FLAG_TARGET_TO_BE_CREATED | PATH_PARSE_FLAG_INODE_ID_FOR_INPUT;
            directoryId[i] = srcDirectoryId;
        } else
            flag |= PATH_PARSE_FLAG_TARGET_TO_BE_DELETED;
        FalconErrorCode errorCode =
            PathParseTreeInsert(NULL, directoryRel, pathArray[i], flag, &parentId[i], &name[i], &directoryId[i]);
        if (errorCode != SUCCESS)
            FALCON_ELOG_ERROR(errorCode, "path parse error.");
    }

    // 3.
    if (renameDirectory) {
        DeleteDirectoryByDirectoryHashTable(directoryRel, parentId[srcIndex], name[srcIndex], DIR_LOCK_NONE);
        CommandCounterIncrement();

        CatalogIndexState indexState = CatalogOpenIndexes(directoryRel);
        InsertDirectoryByDirectoryHashTable(directoryRel,
                                            indexState,
                                            parentId[dstIndex],
                                            name[dstIndex],
                                            directoryId[dstIndex],
                                            DEFAULT_SUBPART_NUM,
                                            DIR_LOCK_NONE);
        CommandCounterIncrement();
        CatalogCloseIndexes(indexState);
    }
    table_close(directoryRel, RowExclusiveLock);

    // 4.
    info->targetIsDirectory = renameDirectory;
    info->inodeId = directoryId[dstIndex];
    info->srcLockOrder = srcIndex;

    info->parentId = parentId[srcIndex];
    info->name = name[srcIndex];
    uint16_t srcPartId = HashPartId(name[srcIndex]);
    uint64_t srcParentIdPartId = CombineParentIdWithPartId(parentId[srcIndex], srcPartId);

    info->dstParentId = parentId[dstIndex];
    info->dstName = name[dstIndex];
    uint16_t dstPartId = HashPartId(name[dstIndex]);
    uint64_t dstParentIdPartId = CombineParentIdWithPartId(parentId[dstIndex], dstPartId);

    int srcShardId, srcWorkerId, dstShardId, dstWorkerId;
    SearchShardInfoByShardValue(srcParentIdPartId, &srcShardId, &srcWorkerId);
    SearchShardInfoByShardValue(dstParentIdPartId, &dstShardId, &dstWorkerId);

    SerializedData subRenameLocallyParam;
    // 4.1
    info->parentId_partId = srcParentIdPartId;
    info->dstParentIdPartId = dstWorkerId == srcWorkerId ? dstParentIdPartId : 0;
    SerializedDataInit(&subRenameLocallyParam, NULL, 0, 0, &PgMemoryManager);
    SerializedDataMetaParamEncodeWithPerProcessFlatBufferBuilder(RENAME_SUB_RENAME_LOCALLY,
                                                                 &info,
                                                                 NULL,
                                                                 1,
                                                                 &subRenameLocallyParam);
    FalconMetaCallOnWorkerList(RENAME_SUB_RENAME_LOCALLY,
                               1,
                               subRenameLocallyParam,
                               REMOTE_COMMAND_FLAG_WRITE,
                               list_make1_int(srcWorkerId));

    // 4.2
    info->parentId_partId = 0;
    info->dstParentIdPartId = 0;
    SerializedDataInit(&subRenameLocallyParam, NULL, 0, 0, &PgMemoryManager);
    SerializedDataMetaParamEncodeWithPerProcessFlatBufferBuilder(RENAME_SUB_RENAME_LOCALLY,
                                                                 &info,
                                                                 NULL,
                                                                 1,
                                                                 &subRenameLocallyParam);
    List *foreignServerIdList = GetAllForeignServerId(true, true);
    foreignServerIdList = list_delete_int(foreignServerIdList, srcWorkerId);
    FalconMetaCallOnWorkerList(RENAME_SUB_RENAME_LOCALLY,
                               1,
                               subRenameLocallyParam,
                               REMOTE_COMMAND_FLAG_WRITE,
                               foreignServerIdList);

    MultipleServerRemoteCommandResult totalRemoteRes = FalconSendCommandAndWaitForResult();

    // 5.
    MetaProcessInfoData responseInfo;
    for (int i = 0; i < list_length(totalRemoteRes); ++i) {
        RemoteCommandResultPerServerData *remoteRes = list_nth(totalRemoteRes, i);
        int64_t serverId = remoteRes->serverId;

        if (list_length(remoteRes->remoteCommandResult) != 1)
            FALCON_ELOG_ERROR(PROGRAM_ERROR, "unexpected situation.");
        PGresult *res = NULL;

        // 5.1
        res = list_nth(remoteRes->remoteCommandResult, 0);
        if (PQntuples(res) != 1 || PQnfields(res) != 1)
            FALCON_ELOG_ERROR(REMOTE_QUERY_FAILED, "PGresult is corrupt.");
        SerializedData subRenameLocallyResponse;
        SerializedDataInit(&subRenameLocallyResponse,
                           PQgetvalue(res, 0, 0),
                           PQgetlength(res, 0, 0),
                           PQgetlength(res, 0, 0),
                           NULL);
        if (!SerializedDataMetaResponseDecode(RENAME_SUB_RENAME_LOCALLY, 1, &subRenameLocallyResponse, &responseInfo))
            FALCON_ELOG_ERROR(ARGUMENT_ERROR, "serialized response is corrupt.");

        if (responseInfo.errorCode != SUCCESS)
            FALCON_ELOG_ERROR(PROGRAM_ERROR,
                              "RenameSubRenameLocally is supposed to be successful, "
                              "but it failed.");

        if (srcWorkerId != dstWorkerId && serverId == srcWorkerId) {
            responseInfo.parentId_partId = dstParentIdPartId;
            responseInfo.name = name[dstIndex];
            SerializedData subCreateParam;
            SerializedDataInit(&subCreateParam, NULL, 0, 0, &PgMemoryManager);
            MetaProcessInfo info = &responseInfo;
            SerializedDataMetaParamEncodeWithPerProcessFlatBufferBuilder(RENAME_SUB_CREATE,
                                                                         &info,
                                                                         NULL,
                                                                         1,
                                                                         &subCreateParam);
            FalconMetaCallOnWorkerList(RENAME_SUB_CREATE,
                                       1,
                                       subCreateParam,
                                       REMOTE_COMMAND_FLAG_WRITE,
                                       list_make1_int(dstWorkerId));
        }
    }

    // 6.
    if (srcWorkerId != dstWorkerId) {
        totalRemoteRes = FalconSendCommandAndWaitForResult();
        if (list_length(totalRemoteRes) != 1)
            FALCON_ELOG_ERROR(PROGRAM_ERROR, "unexpected situation 1.");
        RemoteCommandResultPerServerData *remoteRes = list_nth(totalRemoteRes, 0);
        if (remoteRes->serverId != dstWorkerId)
            FALCON_ELOG_ERROR(PROGRAM_ERROR, "unexpected situation 2.");
        if (list_length(remoteRes->remoteCommandResult) != 1)
            FALCON_ELOG_ERROR(PROGRAM_ERROR, "unexpected situation 3.");
        PGresult *res = list_nth(remoteRes->remoteCommandResult, 0);
        if (PQntuples(res) != 1 || PQnfields(res) != 1)
            FALCON_ELOG_ERROR(REMOTE_QUERY_FAILED, "PGresult is corrupt.");
        SerializedData subCreateResponse;
        SerializedDataInit(&subCreateResponse,
                           PQgetvalue(res, 0, 0),
                           PQgetlength(res, 0, 0),
                           PQgetlength(res, 0, 0),
                           NULL);
        if (!SerializedDataMetaResponseDecode(RENAME_SUB_CREATE, 1, &subCreateResponse, &responseInfo))
            FALCON_ELOG_ERROR(ARGUMENT_ERROR, "serialized response is corrupt.");

        if (responseInfo.errorCode != SUCCESS)
            FALCON_ELOG_ERROR(PROGRAM_ERROR,
                              "RenameSubRenameLocally is supposed to be successful, "
                              "but it failed.");
    }

    info->errorCode = SUCCESS;
}

void FalconRenameSubRenameLocallyHandle(MetaProcessInfo info)
{
    // 1.
    if (info->targetIsDirectory) {
        if (info->srcLockOrder < 0 || info->srcLockOrder > 1)
            FALCON_ELOG_ERROR(ARGUMENT_ERROR, "no expected input.");

        Relation directoryRel = table_open(DirectoryRelationId(), RowExclusiveLock);
        CatalogIndexState indexState = CatalogOpenIndexes(directoryRel);
        for (int i = 0; i < 2; ++i) {
            if (i == info->srcLockOrder)
                DeleteDirectoryByDirectoryHashTable(directoryRel, info->parentId, info->name, DIR_LOCK_EXCLUSIVE);
            else
                InsertDirectoryByDirectoryHashTable(directoryRel,
                                                    indexState,
                                                    info->dstParentId,
                                                    info->dstName,
                                                    info->inodeId,
                                                    DEFAULT_SUBPART_NUM,
                                                    DIR_LOCK_EXCLUSIVE);
            CommandCounterIncrement();
        }
        CatalogCloseIndexes(indexState);
        table_close(directoryRel, RowExclusiveLock);
    }

    if (info->parentId_partId == 0) {
        info->errorCode = SUCCESS;
        return;
    }

    // 2.
    Datum fileInfo[Natts_pg_dfs_inode_table];
    bool fileInfoNulls[Natts_pg_dfs_inode_table];

    int srcShardId, srcWorkerId;
    SearchShardInfoByShardValue(info->parentId_partId, &srcShardId, &srcWorkerId);
    if (srcWorkerId != GetLocalServerId())
        FALCON_ELOG_ERROR(WRONG_WORKER, "wrong worker.");

    StringInfo srcInodeShardName = GetInodeShardName(srcShardId);
    StringInfo srcInodeIndexShardName = GetInodeIndexShardName(srcShardId);

    SetUpScanCaches();
    ScanKeyData scanKey[2];
    scanKey[0] = InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_EQ];
    scanKey[0].sk_argument = UInt64GetDatum(info->parentId_partId);
    scanKey[1] = InodeTableScanKey[INODE_TABLE_NAME_EQ];
    scanKey[1].sk_argument = CStringGetTextDatum(info->name);
    Relation srcInodeRel = table_open(GetRelationOidByName_FALCON(srcInodeShardName->data), RowExclusiveLock);
    SysScanDesc scanDescriptor = systable_beginscan(srcInodeRel,
                                                    GetRelationOidByName_FALCON(srcInodeIndexShardName->data),
                                                    true,
                                                    GetTransactionSnapshot(),
                                                    2,
                                                    scanKey);
    HeapTuple heapTuple = systable_getnext(scanDescriptor);
    TupleDesc tupleDesc = RelationGetDescr(srcInodeRel);

    if (!HeapTupleIsValid(heapTuple))
        FALCON_ELOG_ERROR(FILE_NOT_EXISTS, "unexpected.");

    heap_deform_tuple(heapTuple, tupleDesc, fileInfo, fileInfoNulls);
    CatalogTupleDelete(srcInodeRel, &heapTuple->t_self);
    CommandCounterIncrement();

    systable_endscan(scanDescriptor);
    table_close(srcInodeRel, RowExclusiveLock);

    // 3.
    if (info->dstParentIdPartId != 0) {
        int dstShardId, dstWorkerId;
        SearchShardInfoByShardValue(info->dstParentIdPartId, &dstShardId, &dstWorkerId);
        if (dstWorkerId != GetLocalServerId())
            FALCON_ELOG_ERROR(WRONG_WORKER, "wrong worker.");

        StringInfo dstInodeShardName = GetInodeShardName(dstShardId);
        Relation dstInodeRel = table_open(GetRelationOidByName_FALCON(dstInodeShardName->data), RowExclusiveLock);

        fileInfo[Anum_pg_dfs_file_parentid_partid - 1] = UInt64GetDatum(info->dstParentIdPartId);
        fileInfo[Anum_pg_dfs_file_name - 1] = CStringGetTextDatum(info->dstName);

        heapTuple = heap_form_tuple(tupleDesc, fileInfo, fileInfoNulls);
        CatalogTupleInsert(dstInodeRel, heapTuple);
        heap_freetuple(heapTuple);
        CommandCounterIncrement();

        table_close(dstInodeRel, RowExclusiveLock);
    } else {
        info->inodeId = DatumGetUInt64(fileInfo[Anum_pg_dfs_file_st_ino - 1]);
        info->st_dev = DatumGetUInt64(fileInfo[Anum_pg_dfs_file_st_dev - 1]);
        info->st_mode = DatumGetUInt32(fileInfo[Anum_pg_dfs_file_st_mode - 1]);
        info->st_nlink = DatumGetUInt64(fileInfo[Anum_pg_dfs_file_st_nlink - 1]);
        info->st_uid = DatumGetUInt32(fileInfo[Anum_pg_dfs_file_st_uid - 1]);
        info->st_gid = DatumGetUInt32(fileInfo[Anum_pg_dfs_file_st_gid - 1]);
        info->st_rdev = DatumGetUInt64(fileInfo[Anum_pg_dfs_file_st_rdev - 1]);
        info->st_size = DatumGetInt64(fileInfo[Anum_pg_dfs_file_st_size - 1]);
        info->st_blksize = DatumGetInt64(fileInfo[Anum_pg_dfs_file_st_blksize - 1]);
        info->st_blocks = DatumGetInt64(fileInfo[Anum_pg_dfs_file_st_blocks - 1]);
        info->st_atim = DatumGetTimestampTz(fileInfo[Anum_pg_dfs_file_st_atim - 1]);
        info->st_mtim = DatumGetTimestampTz(fileInfo[Anum_pg_dfs_file_st_mtim - 1]);
        info->st_ctim = DatumGetTimestampTz(fileInfo[Anum_pg_dfs_file_st_ctim - 1]);
        info->node_id = DatumGetInt32(fileInfo[Anum_pg_dfs_file_primary_nodeid - 1]);
    }

    info->errorCode = SUCCESS;
}

void FalconRenameSubCreateHandle(MetaProcessInfo info)
{
    int shardId, workerId;
    SearchShardInfoByShardValue(info->parentId_partId, &shardId, &workerId);
    if (workerId != GetLocalServerId())
        FALCON_ELOG_ERROR(WRONG_WORKER, "wrong worker.");

    StringInfo inodeShardName = GetInodeShardName(shardId);

    Relation workerInodeRel = table_open(GetRelationOidByName_FALCON(inodeShardName->data), RowExclusiveLock);
    InsertIntoInodeTable(workerInodeRel,
                         NULL,
                         info->inodeId,
                         info->parentId_partId,
                         info->name,
                         info->st_dev,
                         info->st_mode,
                         info->st_nlink,
                         info->st_uid,
                         info->st_gid,
                         info->st_rdev,
                         info->st_size,
                         info->st_blksize,
                         info->st_blocks,
                         info->st_atim,
                         info->st_mtim,
                         info->st_ctim,
                         "",
                         0,
                         info->node_id,
                         -1);
    table_close(workerInodeRel, RowExclusiveLock);

    info->errorCode = SUCCESS;
}

void FalconUtimeNsHandle(MetaProcessInfo info)
{
    const char *path = info->path;

    TimestampTz accessTime;
    TimestampTz modifyTime;
    if (info->st_atim == -1 || info->st_mtim == -1) {
        accessTime = GetCurrentTimestamp();
        modifyTime = GetCurrentTimestamp();
    } else {
        accessTime = (TimestampTz)info->st_atim;
        modifyTime = (TimestampTz)info->st_mtim;
    }

    int32_t property;
    VerifyPathValidity(path, 0, &property);

    uint64_t parentId = 0;
    char *fileName;
    Relation directoryRel = table_open(DirectoryRelationId(), AccessShareLock);
    FalconErrorCode errorCode = PathParseTreeInsert(NULL,
                                                    directoryRel,
                                                    path,
                                                    PATH_PARSE_FLAG_ACQUIRE_SHARED_LOCK_IF_TARGET_IS_DIRECTORY,
                                                    &parentId,
                                                    &fileName,
                                                    NULL);
    if (errorCode != SUCCESS)
        FALCON_ELOG_ERROR(errorCode, "path parse error.");
    table_close(directoryRel, AccessShareLock);

    uint16_t partId = HashPartId(fileName);
    uint64_t parentId_partId = CombineParentIdWithPartId(parentId, partId);
    int shardId, workerId;
    SearchShardInfoByShardValue(parentId_partId, &shardId, &workerId);
    if (workerId != GetLocalServerId())
        FALCON_ELOG_ERROR(WRONG_WORKER, "wrong worker.");

    StringInfo inodeShardName = GetInodeShardName(shardId);
    StringInfo inodeIndexShardName = GetInodeIndexShardName(shardId);
    bool fileExist = SearchAndUpdateInodeTableInfo(inodeShardName->data,
                                                   NULL,
                                                   inodeIndexShardName->data,
                                                   InvalidOid,
                                                   parentId_partId,
                                                   fileName,
                                                   true,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   0,
                                                   NULL,
                                                   NULL,
                                                   MODE_CHECK_NONE,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   &accessTime,
                                                   &modifyTime,
                                                   NULL,
                                                   NULL,
                                                   NULL);
    if (!fileExist)
        FALCON_ELOG_ERROR(FILE_NOT_EXISTS, "file doesn't exist.");

    info->errorCode = SUCCESS;
}

void FalconChownHandle(MetaProcessInfo info)
{
    const char *path = info->path;

    int32_t property;
    VerifyPathValidity(path, 0, &property);

    uint64_t parentId = 0;
    char *fileName;
    Relation directoryRel = table_open(DirectoryRelationId(), AccessShareLock);
    FalconErrorCode errorCode = PathParseTreeInsert(NULL,
                                                    directoryRel,
                                                    path,
                                                    PATH_PARSE_FLAG_ACQUIRE_SHARED_LOCK_IF_TARGET_IS_DIRECTORY,
                                                    &parentId,
                                                    &fileName,
                                                    NULL);
    if (errorCode != SUCCESS)
        FALCON_ELOG_ERROR(errorCode, "path parse error.");
    table_close(directoryRel, AccessShareLock);

    uint16_t partId = HashPartId(fileName);
    uint64_t parentId_partId = CombineParentIdWithPartId(parentId, partId);
    int shardId, workerId;
    SearchShardInfoByShardValue(parentId_partId, &shardId, &workerId);
    if (workerId != GetLocalServerId())
        FALCON_ELOG_ERROR(WRONG_WORKER, "wrong worker.");

    StringInfo inodeShardName = GetInodeShardName(shardId);
    StringInfo inodeIndexShardName = GetInodeIndexShardName(shardId);
    bool fileExist = SearchAndUpdateInodeTableInfo(inodeShardName->data,
                                                   NULL,
                                                   inodeIndexShardName->data,
                                                   InvalidOid,
                                                   parentId_partId,
                                                   fileName,
                                                   true,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   0,
                                                   NULL,
                                                   NULL,
                                                   MODE_CHECK_NONE,
                                                   &info->st_uid,
                                                   &info->st_gid,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL);
    if (!fileExist)
        FALCON_ELOG_ERROR(FILE_NOT_EXISTS, "file doesn't exist.");

    info->errorCode = SUCCESS;
}

void FalconChmodHandle(MetaProcessInfo info)
{
    const char *path = info->path;
    mode_t newExecMode = info->st_mode;
    newExecMode &= 0x1FF;

    int32_t property;
    VerifyPathValidity(path, 0, &property);

    uint64_t parentId = 0;
    char *fileName;
    Relation directoryRel = table_open(DirectoryRelationId(), AccessShareLock);
    FalconErrorCode errorCode = PathParseTreeInsert(NULL,
                                                    directoryRel,
                                                    path,
                                                    PATH_PARSE_FLAG_ACQUIRE_SHARED_LOCK_IF_TARGET_IS_DIRECTORY,
                                                    &parentId,
                                                    &fileName,
                                                    NULL);
    if (errorCode != SUCCESS)
        FALCON_ELOG_ERROR(errorCode, "path parse error.");
    table_close(directoryRel, AccessShareLock);

    uint16_t partId = HashPartId(fileName);
    uint64_t parentId_partId = CombineParentIdWithPartId(parentId, partId);
    int shardId, workerId;
    SearchShardInfoByShardValue(parentId_partId, &shardId, &workerId);
    if (workerId != GetLocalServerId())
        FALCON_ELOG_ERROR(WRONG_WORKER, "wrong worker.");

    StringInfo inodeShardName = GetInodeShardName(shardId);
    StringInfo inodeIndexShardName = GetInodeIndexShardName(shardId);
    bool fileExist = SearchAndUpdateInodeTableInfo(inodeShardName->data,
                                                   NULL,
                                                   inodeIndexShardName->data,
                                                   InvalidOid,
                                                   parentId_partId,
                                                   fileName,
                                                   true,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   0,
                                                   NULL,
                                                   &newExecMode,
                                                   MODE_CHECK_NONE,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL,
                                                   NULL);
    if (!fileExist)
        FALCON_ELOG_ERROR(FILE_NOT_EXISTS, "file doesn't exist.");

    info->errorCode = SUCCESS;
}

static inline uint16_t HashPartId(const char *fileName)
{
    uint16_t hashValue = 0;
    for (int i = 0; i < strnlen(fileName, FILENAMELENGTH); ++i) {
        hashValue = hashValue * 31 + fileName[i];
    }
    return hashValue & PART_ID_MASK;
}

static inline uint64_t CombineParentIdWithPartId(uint64_t parent_id, uint16_t part_id)
{
    return (parent_id << PART_ID_BIT_COUNT) | part_id;
}

static StringInfo GetInodeShardName(int shardId)
{
    StringInfo inodeShardName = makeStringInfo();
    appendStringInfo(inodeShardName, "%s_%d", InodeTableName, shardId);
    return inodeShardName;
}

static StringInfo GetInodeIndexShardName(int shardId)
{
    StringInfo inodeIndexShardName = makeStringInfo();
    appendStringInfo(inodeIndexShardName, "%s_%d_%s", InodeTableName, shardId, "index");
    return inodeIndexShardName;
}

static StringInfo __attribute__((unused)) GetXattrShardName(int shardId)
{
    StringInfo xattrShardName = makeStringInfo();
    appendStringInfo(xattrShardName, "%s_%d", XattrTableName, shardId);
    return xattrShardName;
}

static StringInfo __attribute__((unused)) GetXattrIndexShardName(int shardId)
{
    StringInfo xattrIndexShardName = makeStringInfo();
    appendStringInfo(xattrIndexShardName, "%s_%d_%s", XattrTableName, shardId, "index");
    return xattrIndexShardName;
}

static Oid GetRelationOidByName_FALCON(const char *relationName)
{
    Oid res = InvalidOid;
    res = get_relname_relid(relationName, PG_CATALOG_NAMESPACE);
    if (res == InvalidOid) {
        FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR, "cannot find relation %s.", relationName);
    }
    return res;
}

static bool SearchAndUpdateInodeTableInfo(const char *workerInodeRelationName,
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
    if (doUpdate)
        memset(doUpdateArray, false, sizeof(doUpdateArray));
    bool isNull;
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

static bool InsertIntoInodeTable(Relation relation,
                                 CatalogIndexState indexState,
                                 uint64_t st_ino,
                                 uint64_t parentid_partid,
                                 const char *name,
                                 uint64_t st_dev,
                                 uint32_t st_mode,
                                 uint64_t st_nlink,
                                 uint32_t st_uid,
                                 uint32_t st_gid,
                                 uint64_t st_rdev,
                                 int64_t st_size,
                                 int64_t st_blksize,
                                 int64_t st_blocks,
                                 TimestampTz st_atim,
                                 TimestampTz st_mtim,
                                 TimestampTz st_ctim,
                                 const char *etag,
                                 uint64_t update_version,
                                 int32_t primaryNodeId,
                                 int32_t backupNodeId)
{
    Datum values[Natts_pg_dfs_inode_table];
    bool isNulls[Natts_pg_dfs_inode_table];
    HeapTuple heapTuple;
    TupleDesc tupleDescriptor = RelationGetDescr(relation);

    /* form new shard tuple */
    memset(values, 0, sizeof(values));
    memset(isNulls, false, sizeof(isNulls));
    values[Anum_pg_dfs_file_name - 1] = CStringGetTextDatum(name);
    values[Anum_pg_dfs_file_st_ino - 1] = UInt64GetDatum(st_ino);
    values[Anum_pg_dfs_file_parentid_partid - 1] = UInt64GetDatum(parentid_partid);
    values[Anum_pg_dfs_file_st_dev - 1] = UInt64GetDatum(st_dev);
    values[Anum_pg_dfs_file_st_mode - 1] = UInt32GetDatum(st_mode);
    values[Anum_pg_dfs_file_st_nlink - 1] = UInt64GetDatum(st_nlink);
    values[Anum_pg_dfs_file_st_uid - 1] = UInt32GetDatum(st_uid);
    values[Anum_pg_dfs_file_st_gid - 1] = UInt32GetDatum(st_gid);
    values[Anum_pg_dfs_file_st_rdev - 1] = UInt64GetDatum(st_rdev);
    values[Anum_pg_dfs_file_st_size - 1] = Int64GetDatum(st_size);
    values[Anum_pg_dfs_file_st_blksize - 1] = Int64GetDatum(st_blksize);
    values[Anum_pg_dfs_file_st_blocks - 1] = Int64GetDatum(st_blocks);
    values[Anum_pg_dfs_file_st_atim - 1] = TimestampTzGetDatum(st_atim);
    values[Anum_pg_dfs_file_st_mtim - 1] = TimestampTzGetDatum(st_mtim);
    values[Anum_pg_dfs_file_st_ctim - 1] = TimestampTzGetDatum(st_ctim);
    values[Anum_pg_dfs_file_etag - 1] = CStringGetTextDatum(etag);
    values[Anum_pg_dfs_file_update_version - 1] = UInt64GetDatum(update_version);
    values[Anum_pg_dfs_file_primary_nodeid - 1] = UInt32GetDatum(primaryNodeId);
    values[Anum_pg_dfs_file_backup_nodeid - 1] = UInt32GetDatum(backupNodeId);

    heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);
    if (indexState == NULL)
        CatalogTupleInsert(relation, heapTuple);
    else
        CatalogTupleInsertWithInfo(relation, heapTuple, indexState);
    heap_freetuple(heapTuple);
    CommandCounterIncrement();
    return true;
}
