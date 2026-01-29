/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "postgres.h"

#include "utils/path_parse.h"

#include "catalog/namespace.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

#include "dir_path_shmem/dir_path_hash.h"
#include "distributed_backend/remote_comm.h"
#include "perf_counter/perf_macros.h"
#include "utils/error_log.h"
#include "utils/utils.h"

static MemoryContext PathParseContext = NULL;
// TransactionLevelPathParseRoot will be reset after each transaction commited
// TBD: maybe we need only reset TransactionLevelPathParseRoot while top transaction is commited or prepared.
//      this can be done by additional check in transaction.c
static PathParseTree TransactionLevelPathParseRoot = NULL;

static int PathParseRBT_cmp(const RBTNode *a, const RBTNode *b, void *arg)
{
    const PathParseRBTreeNode *ea = (const PathParseRBTreeNode *)a;
    const PathParseRBTreeNode *eb = (const PathParseRBTreeNode *)b;

    return strcmp(ea->name, eb->name);
}

static void PathParseRBT_combine(RBTNode *existing, const RBTNode *newdata, void *arg)
{
    const PathParseRBTreeNode *eexist = (const PathParseRBTreeNode *)existing;
    const PathParseRBTreeNode *enew = (const PathParseRBTreeNode *)newdata;

    FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR,
                               "red-black tree combines %s with %s, not expected.",
                               eexist->name,
                               enew->name);
}

static RBTNode *PathParseRBT_alloc(void *arg)
{
    return (RBTNode *)MemoryContextAlloc(PathParseContext, sizeof(PathParseRBTreeNode));
}

static void PathParseRBT_free(RBTNode *node, void *arg) { pfree(node); }

void PathParseTreeInit(PathParseRBTreeNode *root)
{
    root->inodeId = 0;
    root->name = NULL;
    root->children = NULL;
}

uint64_t CheckWhetherPathExistsInDirectoryTable(Relation directoryRel, const char *path)
{
    if (!path || path[0] == '\0')
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "path is null.");
    if (path[0] != '/')
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "path should start with /.");

    uint64_t parentId = 0;
    int currentFileNameStartPos = 0;
    int currentFileNameLength = 1;
    while (path[currentFileNameStartPos] != '\0') {
        char *name = palloc(currentFileNameLength + 1);
        memcpy(name, path + currentFileNameStartPos, currentFileNameLength);
        name[currentFileNameLength] = '\0';
        uint64_t currentDirectoryId = SearchDirectoryByDirectoryHashTable(directoryRel, parentId, name, DIR_LOCK_NONE);
        if (currentDirectoryId == DIR_HASH_TABLE_PATH_NOT_EXIST)
            return DIR_HASH_TABLE_PATH_NOT_EXIST;

        parentId = currentDirectoryId;
        if (currentFileNameStartPos == 0)
            currentFileNameStartPos = 1;
        else if (path[currentFileNameStartPos + currentFileNameLength] == '\0')
            break;
        else
            currentFileNameStartPos += currentFileNameLength + 1;
        currentFileNameLength = 0;
        while (path[currentFileNameStartPos + currentFileNameLength] != '\0' &&
               path[currentFileNameStartPos + currentFileNameLength] != '/')
            currentFileNameLength++;
    }
    return parentId;
}

void PathParseMemoryContextInit()
{
    PathParseContext = AllocSetContextCreate(TopMemoryContext,
                                             "falcon path parse memory context",
                                             ALLOCSET_DEFAULT_MINSIZE,
                                             ALLOCSET_DEFAULT_INITSIZE,
                                             ALLOCSET_DEFAULT_MAXSIZE);
}

void TransactionLevelPathParseReset()
{
    TransactionLevelPathParseRoot = NULL;
    MemoryContextReset(PathParseContext);
}

FalconErrorCode PathParseTreeInsert(PathParseTree root,
                                    Relation directoryRel,
                                    const char *path,
                                    uint16_t flag,
                                    uint64_t *parentId,
                                    char **fileName,
                                    uint64_t *inodeId)
{
    if (path[0] == '/' && path[1] == '\0' && (flag & PATH_PARSE_FLAG_NOT_ROOT)) {
        return PATH_IS_ROOT;
    }

    FalconPerfLatencyShmem *perf = g_FalconPerfLatencyShmem;
    PERF_SCOPED_TIMER(path_parse_timer, perf ? &perf->pathParseLatency : NULL);

    if (root == NULL) {
        if (TransactionLevelPathParseRoot == NULL) {
            TransactionLevelPathParseRoot = MemoryContextAlloc(PathParseContext, sizeof(PathParseRBTreeNode));
            PathParseTreeInit(TransactionLevelPathParseRoot);
        }
        root = TransactionLevelPathParseRoot;
    }

    int currentFileNameStartPos = 0;
    int currentFileNameLength = 1;
    PathParseRBTreeNode *currentNode = root;
    PathParseRBTreeNode *node;
    while (path[currentFileNameStartPos + currentFileNameLength] != '\0') {
        PathParseRBTreeNode target;
        target.name = palloc(currentFileNameLength + 1);
        memcpy(target.name, path + currentFileNameStartPos, currentFileNameLength);
        target.name[currentFileNameLength] = '\0';

        node = NULL;
        if (currentNode->children) {
            node = (PathParseRBTreeNode *)rbt_find(currentNode->children, (RBTNode *)&target);

            if (node && node->lockAcquired == PP_EXCLUSIVE)
                return PATH_LOCK_CONFLICT;
        }
        if (node == NULL) {
            uint64_t currentDirectoryId =
                SearchDirectoryByDirectoryHashTable(directoryRel, currentNode->inodeId, target.name, DIR_LOCK_SHARED);
            if (currentDirectoryId == -1)
                return PATH_IS_INVALID;

            if (!currentNode->children) {
                MemoryContext oldContext = MemoryContextSwitchTo(PathParseContext);
                currentNode->children = rbt_create(sizeof(PathParseRBTreeNode),
                                                   PathParseRBT_cmp,
                                                   PathParseRBT_combine,
                                                   PathParseRBT_alloc,
                                                   PathParseRBT_free,
                                                   NULL);
                MemoryContextSwitchTo(oldContext);
            }
            target.inodeId = currentDirectoryId;
            target.children = NULL;
            target.lockAcquired = PP_SHARED;
            bool isNew;
            node = (PathParseRBTreeNode *)rbt_insert(currentNode->children, (RBTNode *)&target, &isNew);
        } else if (node->lockAcquired == PP_NONE) {
            SearchDirectoryByDirectoryHashTable(directoryRel, node->inodeId, node->name, DIR_LOCK_SHARED);
            node->lockAcquired = PP_SHARED;
        }

        if (currentFileNameStartPos == 0)
            currentFileNameStartPos = 1;
        else
            currentFileNameStartPos += currentFileNameLength + 1;
        currentFileNameLength = 0;
        while (path[currentFileNameStartPos + currentFileNameLength] != '\0' &&
               path[currentFileNameStartPos + currentFileNameLength] != '/')
            ++currentFileNameLength;
        currentNode = node;
    }
    if (parentId != NULL)
        *parentId = currentNode->inodeId;
    if (fileName != NULL) {
        *fileName = palloc(currentFileNameLength + 1);
        memcpy(*fileName, path + currentFileNameStartPos, currentFileNameLength + 1);
    }

    if (flag & PATH_PARSE_FLAG_TARGET_TO_BE_CREATED) {
        if (inodeId == NULL)
            FALCON_ELOG_ERROR(PROGRAM_ERROR,
                              "if target need to be created, the inodeId pointer must not be null since we will return "
                              "generated inodeId by it or fetch inodeId from it.");

        if (flag & PATH_PARSE_FLAG_TARGET_IS_DIRECTORY) {
            PathParseRBTreeNode target;
            target.name = palloc(currentFileNameLength + 1);
            memcpy(target.name, path + currentFileNameStartPos, currentFileNameLength);
            target.name[currentFileNameLength] = '\0';
            if (currentNode->children && rbt_find(currentNode->children, (RBTNode *)&target)) {
                return PATH_EXISTS;
            }

            uint64_t targetInodeId = SearchDirectoryByDirectoryHashTable(directoryRel,
                                                                         currentNode->inodeId,
                                                                         path + currentFileNameStartPos,
                                                                         DIR_LOCK_EXCLUSIVE);

            if (!currentNode->children) {
                MemoryContext oldContext = MemoryContextSwitchTo(PathParseContext);
                currentNode->children = rbt_create(sizeof(PathParseRBTreeNode),
                                                   PathParseRBT_cmp,
                                                   PathParseRBT_combine,
                                                   PathParseRBT_alloc,
                                                   PathParseRBT_free,
                                                   NULL);
                MemoryContextSwitchTo(oldContext);
            }
            if (!(flag & PATH_PARSE_FLAG_INODE_ID_FOR_INPUT)) {
                if (targetInodeId != DIR_HASH_TABLE_PATH_NOT_EXIST)
                    *inodeId = targetInodeId;
                else
                    *inodeId = GenerateInodeIdBySeqAndNodeId(GetNextSequenceNum(), GetLocalServerId());
            }
            target.inodeId = *inodeId;
            target.children = NULL;
            if (flag & PATH_PARSE_FLAG_ALLOW_OPERATION_UNDER_CREATED_DIRECTORY)
                target.lockAcquired = PP_EXCLUSIVE_FOR_CREATE;
            else
                target.lockAcquired = PP_EXCLUSIVE;
            bool isNew;
            rbt_insert(currentNode->children, (RBTNode *)&target, &isNew);

            if (targetInodeId != DIR_HASH_TABLE_PATH_NOT_EXIST)
                return PATH_EXISTS;
        } else {
            if (!(flag & PATH_PARSE_FLAG_INODE_ID_FOR_INPUT)) {
                *inodeId = GenerateInodeIdBySeqAndNodeId(GetNextSequenceNum(), GetLocalServerId());
            }
        }
    } else if ((flag & PATH_PARSE_FLAG_TARGET_TO_BE_DELETED) && (flag & PATH_PARSE_FLAG_TARGET_IS_DIRECTORY)) {
        if (inodeId == NULL)
            FALCON_ELOG_ERROR(PROGRAM_ERROR,
                              "if target need to be removed, the inodeId pointer must not be null since we will return "
                              "inodeId by it.");

        PathParseRBTreeNode target;
        target.name = palloc(currentFileNameLength + 1);
        memcpy(target.name, path + currentFileNameStartPos, currentFileNameLength);
        target.name[currentFileNameLength] = '\0';
        node = NULL;
        if (currentNode->children &&
            (node = (PathParseRBTreeNode *)rbt_find(currentNode->children, (RBTNode *)&target))) {
            if (node->lockAcquired != PP_NONE)
                return PATH_LOCK_CONFLICT;
        }

        *inodeId = SearchDirectoryByDirectoryHashTable(directoryRel,
                                                       currentNode->inodeId,
                                                       path + currentFileNameStartPos,
                                                       DIR_LOCK_EXCLUSIVE);
        if (*inodeId == DIR_HASH_TABLE_PATH_NOT_EXIST) {
            return PATH_NOT_EXISTS;
        }

        if (!currentNode->children) {
            MemoryContext oldContext = MemoryContextSwitchTo(PathParseContext);
            currentNode->children = rbt_create(sizeof(PathParseRBTreeNode),
                                               PathParseRBT_cmp,
                                               PathParseRBT_combine,
                                               PathParseRBT_alloc,
                                               PathParseRBT_free,
                                               NULL);
            MemoryContextSwitchTo(oldContext);
            node = (PathParseRBTreeNode *)rbt_find(currentNode->children, (RBTNode *)&target);
        }
        if (node) {
            if (node->lockAcquired != PP_NONE)
                return PATH_LOCK_CONFLICT;
            node->lockAcquired = PP_EXCLUSIVE;
        } else {
            target.inodeId = *inodeId;
            target.children = NULL;
            target.lockAcquired = PP_EXCLUSIVE;
            bool isNew;
            rbt_insert(currentNode->children, (RBTNode *)&target, &isNew);
        }
    } else if (flag & PATH_PARSE_FLAG_ACQUIRE_SHARED_LOCK_IF_TARGET_IS_DIRECTORY) {
        PathParseRBTreeNode target;
        target.name = palloc(currentFileNameLength + 1);
        memcpy(target.name, path + currentFileNameStartPos, currentFileNameLength);
        target.name[currentFileNameLength] = '\0';

        node = NULL;
        if (currentNode->children) {
            node = (PathParseRBTreeNode *)rbt_find(currentNode->children, (RBTNode *)&target);

            if (node && node->lockAcquired == PP_EXCLUSIVE)
                return PATH_LOCK_CONFLICT;
        }
        if (node == NULL) {
            uint64_t currentDirectoryId =
                SearchDirectoryByDirectoryHashTable(directoryRel, currentNode->inodeId, target.name, DIR_LOCK_SHARED);
            if (currentDirectoryId == DIR_HASH_TABLE_PATH_NOT_EXIST && (flag & PATH_PARSE_FLAG_TARGET_IS_DIRECTORY))
                return PATH_NOT_EXISTS;

            if (!currentNode->children) {
                MemoryContext oldContext = MemoryContextSwitchTo(PathParseContext);
                currentNode->children = rbt_create(sizeof(PathParseRBTreeNode),
                                                   PathParseRBT_cmp,
                                                   PathParseRBT_combine,
                                                   PathParseRBT_alloc,
                                                   PathParseRBT_free,
                                                   NULL);
                MemoryContextSwitchTo(oldContext);
            }

            target.inodeId = currentDirectoryId;
            target.children = NULL;
            if (currentDirectoryId == DIR_HASH_TABLE_PATH_NOT_EXIST) {
                RWLockRelease(DirectoryHashTableLastAcquiredLock);
                target.lockAcquired = PP_NONE;
            } else {
                target.lockAcquired = PP_SHARED;
            }
            bool isNew;
            node = (PathParseRBTreeNode *)rbt_insert(currentNode->children, (RBTNode *)&target, &isNew);
        } else if (node->inodeId != DIR_HASH_TABLE_PATH_NOT_EXIST && node->lockAcquired == PP_NONE) {
            SearchDirectoryByDirectoryHashTable(directoryRel, node->inodeId, node->name, DIR_LOCK_SHARED);
            node->lockAcquired = PP_SHARED;
        }

        if (inodeId)
            *inodeId = node->inodeId;
    }
    return SUCCESS;
}

FalconErrorCode VerifyPathValidity(const char *path, int32_t requirement, int32_t *property)
{
    FalconPerfLatencyShmem *perf = g_FalconPerfLatencyShmem;
    PERF_SCOPED_TIMER(path_verify_timer, perf ? &perf->pathVerifyLatency : NULL);

    *property = 0;

    if (!path || path[0] != '/') // path = null or path doesnt start with '/'
        return PATH_IS_INVALID;

    int pathLen = strlen(path);
    if (path[pathLen - 1] == '/') // path ends with '/'
    {
        *property |= VERIFY_PATH_VALIDITY_PROPERTY_CAN_BE_DIRECTORY;
    } else {
        *property |= VERIFY_PATH_VALIDITY_PROPERTY_CAN_BE_DIRECTORY | VERIFY_PATH_VALIDITY_PROPERTY_CAN_BE_FILE;
    }
    if ((requirement & VERIFY_PATH_VALIDITY_REQUIREMENT_MUST_BE_DIRECTORY) &&
        !(*property & VERIFY_PATH_VALIDITY_PROPERTY_CAN_BE_DIRECTORY))
        return PATH_VERIFY_FAILED;
    if ((requirement & VERIFY_PATH_VALIDITY_REQUIREMENT_MUST_BE_FILE) &&
        !(*property & VERIFY_PATH_VALIDITY_PROPERTY_CAN_BE_FILE))
        return PATH_VERIFY_FAILED;
    return SUCCESS;
}

Oid GetSequenceRelationId()
{
    static Oid sequenceRelationId = InvalidOid;
    if (sequenceRelationId == InvalidOid) {
        text *relationName = cstring_to_text(INODEID_SEQUENCE_NAME);

        List *relationNameList = textToQualifiedNameList(relationName);
        RangeVar *relation = makeRangeVarFromNameList(relationNameList);
        sequenceRelationId = RangeVarGetRelid(relation, NoLock, false);
    }
    return sequenceRelationId;
}

uint64_t GetNextSequenceNum()
{
    static int remainNumberCount = 0;
    static uint64_t currentSequenceNumber = 0;
    if (remainNumberCount == 0) {
        Oid sequenceId = GetSequenceRelationId();
        Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);
        Oid savedUserId = InvalidOid;
        int savedSecurityContext = 0;
        Datum sequenceNumDatum;

        GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
        SetUserIdAndSecContext(FalconExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

        sequenceNumDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

        SetUserIdAndSecContext(savedUserId, savedSecurityContext);

        remainNumberCount = 32; // same with defs in falcon.pg_dfs_inodeid_seq
        currentSequenceNumber = DatumGetUInt64(sequenceNumDatum);
    }
    --remainNumberCount;
    ++currentSequenceNumber;
    return currentSequenceNumber - 1;
}
