/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_DIR_PATH_HASH_H
#define FALCON_DIR_PATH_HASH_H

#include "postgres.h"

#include <stdint.h>

#include "access/heapam.h"
#include "access/htup.h"
#include "catalog/indexing.h"
#include "lib/stringinfo.h"
#include "port/atomics.h"
#include "storage/proclist_types.h"
#include "utils/rwlock.h"

#define MAX_DIRECTORY_PATH_HASH_SIZE 256
#define MAX_DIRECTORY_HASH_TO_COMMIT_ACTION_LENGTH 4096

#define DIR_HASH_TABLE_PATH_NOT_EXIST -1
#define DIR_HASH_TABLE_PATH_UNKNOWN -2

/* A hash key for directory path */
typedef struct
{
    char fileName[MAX_DIRECTORY_PATH_HASH_SIZE];
    uint64_t parentId;
} DirPathHashKey;

/* A hash table entry */
typedef struct
{
    DirPathHashKey key;
    uint64_t inodeId;
    RWLock lock;
    int32_t usageCount;
} DirPathHashItem;

typedef enum { DIR_LOCK_EXCLUSIVE, DIR_LOCK_SHARED, DIR_LOCK_NONE } DirPathLockMode;

extern void AbortForDirPathHash(void);
extern void CommitForDirPathHash(void);
extern void ClearDirPathHash(void);
extern size_t DirPathShmemsize(void);
extern void DirPathShmemInit(void);

// LastAcquiredLock points to the last lock acquired by following functions
extern RWLock *DirectoryHashTableLastAcquiredLock;
extern uint64_t
SearchDirectoryByDirectoryHashTable(Relation relation, uint64_t parentId, const char *name, DirPathLockMode lockMode);
extern void InsertDirectoryByDirectoryHashTable(Relation relation,
                                                CatalogIndexState indexState,
                                                uint64_t parentId,
                                                const char *name,
                                                uint64_t inodeId,
                                                uint32_t numSubparts,
                                                DirPathLockMode lockMode);
extern void
DeleteDirectoryByDirectoryHashTable(Relation relation, uint64_t parentId, const char *name, DirPathLockMode lockMode);

#define DIRECTORY_PATH_HASH_BUCKET_NUM 2048
#define MAX_DIRECTORY_PATH_HASH_CAPACITY 2048
#define MAX_BUCKET_SUM_LRU_CLAER_BEGIN (MAX_DIRECTORY_PATH_HASH_CAPACITY / 4 * 3)

#endif
