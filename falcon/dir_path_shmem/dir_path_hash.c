/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "dir_path_shmem/dir_path_hash.h"

#include "postgres.h"

#include <inttypes.h>
#include <stdio.h>

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/indexing.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_namespace_d.h"
#include "common/hashfn.h"
#include "funcapi.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/dynahash.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/palloc.h"
#include "utils/snapmgr.h"

#include "control/meta_expiration.h"
#include "metadb/directory_path.h"
#include "metadb/directory_table.h"
#include "utils/error_log.h"
#include "utils/rwlock.h"
#include "utils/shmem_control.h"
#include "utils/utils.h"
#include "utils/utils_standalone.h"

#define NUM_FREELISTS 32

typedef HASHELEMENT *HASHBUCKET;

typedef HASHBUCKET *HASHSEGMENT;

typedef struct
{
    slock_t mutex;         /* spinlock for this freelist */
    long nentries;         /* number of entries in associated buckets */
    HASHELEMENT *freeList; /* chain of free elements */
} FreeListData;

struct HASHHDR
{
    FreeListData freeList[NUM_FREELISTS];

    long dsize;        /* directory size */
    long nsegs;        /* number of allocated segments (<= dsize) */
    uint32 max_bucket; /* ID of maximum bucket in use */
    uint32 high_mask;  /* mask to modulo into entire table */
    uint32 low_mask;   /* mask to modulo into lower half of table */

    Size keysize;        /* hash key length in bytes */
    Size entrysize;      /* total user element size in bytes */
    long num_partitions; /* # partitions (must be power of 2), or 0 */
    long max_dsize;      /* 'dsize' limit if directory is fixed size */
    long ssize;          /* segment size --- must be power of 2 */
    int sshift;          /* segment shift = log2(ssize) */
    int nelem_alloc;     /* number of entries to allocate at once */

#ifdef HASH_STATISTICS
    long accesses;
    long collisions;
#endif
};

struct HTAB
{
    HASHHDR *hctl;         /* => shared control information */
    HASHSEGMENT *dir;      /* directory of segment starts */
    HashValueFunc hash;    /* hash function */
    HashCompareFunc match; /* key comparison function */
    HashCopyFunc keycopy;  /* key copying function */
    HashAllocFunc alloc;   /* memory allocator */
    MemoryContext hcxt;    /* memory context if default allocator used */
    char *tabname;         /* table name (for error messages) */
    bool isshared;         /* true if table is in shared memory */
    bool isfixed;          /* if true, don't enlarge */

    bool frozen; /* true = no more inserts allowed */

    Size keysize; /* hash key length in bytes */
    long ssize;   /* segment size --- must be power of 2 */
    int sshift;   /* segment shift = log2(ssize) */
};
// end

#define DIR_PATH_HASH_PARTITION_SIZE 128
#define DIR_PATH_HASH_PARTITION_INDEX(hashcode) ((hashcode) % DIR_PATH_HASH_PARTITION_SIZE)
static int DirPathLWLockTrancheId;
static char *DirPathLWLockTrancheName = "Falcon dir path hash";
static LWLockPadded *DirPathLWLockArray;
#define DIR_PATH_HASH_PARTITION_LOCK(hashcode) (&(DirPathLWLockArray[(hashcode) % DIR_PATH_HASH_PARTITION_SIZE].lock))
static HTAB *PathDirHash[DIR_PATH_HASH_PARTITION_SIZE] = {0};
#define DIR_PATH_HASH_PARTITION(hashcode) (PathDirHash[(hashcode) % DIR_PATH_HASH_PARTITION_SIZE])
static pg_atomic_uint32 *PathDirHashEntryCount = NULL;
#define DIR_PATH_HASH_PARTITION_COUNT(hashcode) (PathDirHashEntryCount + (hashcode) % DIR_PATH_HASH_PARTITION_SIZE)

static char DirPathHashToCommitAction[MAX_DIRECTORY_HASH_TO_COMMIT_ACTION_LENGTH];
static DirPathHashItem DirPathHashToCommitActionInfo[MAX_DIRECTORY_HASH_TO_COMMIT_ACTION_LENGTH];
static int DirPathHashToCommitSize = 0;
void DirPathHashToCommitAddEntry(uint64_t parentId, const char *fileName);
void DirPathHashToCommitUpdateEntry(uint64_t parentId, const char *fileName, uint64_t inodeId, int64_t accessTime);
void DirPathHashToCommitClear(void);

void DirPathHashToCommitAddEntry(uint64_t parentId, const char *fileName)
{
    if (DirPathHashToCommitSize >= MAX_DIRECTORY_HASH_TO_COMMIT_ACTION_LENGTH)
        FALCON_ELOG_ERROR(PROGRAM_ERROR,
                          "concurrency of directory action surpass MAX_DIRECTORY_HASH_TO_COMMIT_ACTION_LENGTH.");
    DirPathHashToCommitAction[DirPathHashToCommitSize] = 'A';
    DirPathHashToCommitActionInfo[DirPathHashToCommitSize].key.parentId = parentId;
    strcpy(DirPathHashToCommitActionInfo[DirPathHashToCommitSize].key.fileName, fileName);
    DirPathHashToCommitSize++;
}
void DirPathHashToCommitUpdateEntry(uint64_t parentId, const char *fileName, uint64_t inodeId, int64_t accessTime)
{
    if (DirPathHashToCommitSize >= MAX_DIRECTORY_HASH_TO_COMMIT_ACTION_LENGTH)
        FALCON_ELOG_ERROR(PROGRAM_ERROR,
                          "concurrency of directory action surpass MAX_DIRECTORY_HASH_TO_COMMIT_ACTION_LENGTH.");
    DirPathHashToCommitAction[DirPathHashToCommitSize] = 'U';
    DirPathHashToCommitActionInfo[DirPathHashToCommitSize].key.parentId = parentId;
    strcpy(DirPathHashToCommitActionInfo[DirPathHashToCommitSize].key.fileName, fileName);
    DirPathHashToCommitActionInfo[DirPathHashToCommitSize].inodeId = inodeId;
    DirPathHashToCommitActionInfo[DirPathHashToCommitSize].accessTime = accessTime;
    DirPathHashToCommitSize++;
}
void DirPathHashToCommitClear() { DirPathHashToCommitSize = 0; }

DirPathHashKey ClockCountList[DIRECTORY_PATH_HASH_BUCKET_NUM];

RWLock *DirectoryHashTableLastAcquiredLock = NULL;

static uint32 dir_path_hash(const void *key, Size keysize);
static int dir_path_compare(const void *key1, const void *key2);
static int dir_path_compare(const void *key1, const void *key2);
static int dir_path_match(const void *key1, const void *key2, Size keysize);
static void *dir_path_keycopy(void *dest, const void *src, Size keysize);
static void ReleaseDirPathHashLock(uint64_t parentId, char *filename);
static DirPathHashItem *FindNextItem(HASH_SEQ_STATUS *status, int32_t *destroyableCnt);
static bool EliminateDirPathHashByLRU(int partitionIndex);

PG_FUNCTION_INFO_V1(falcon_print_dir_path_hash_elem);
PG_FUNCTION_INFO_V1(falcon_acquire_hash_lock);
PG_FUNCTION_INFO_V1(falcon_release_hash_lock);

Datum falcon_print_dir_path_hash_elem(PG_FUNCTION_ARGS)
{
    FuncCallContext *functionContext = NULL;
    TupleDesc tupleDescriptor;
    List *returnInfoList = NIL;
    uint32 d_off;
    DirPathHashItem *entry;
    Datum values[4];
    bool resNulls[4];
    HeapTuple heapTupleRes;

    if (SRF_IS_FIRSTCALL()) {
        functionContext = SRF_FIRSTCALL_INIT();

        MemoryContext oldContext = MemoryContextSwitchTo(functionContext->multi_call_memory_ctx);
        if (get_call_result_type(fcinfo, NULL, &tupleDescriptor) != TYPEFUNC_COMPOSITE) {
            FALCON_ELOG_ERROR(PROGRAM_ERROR, "return type must be a row type.");
        }
        functionContext->tuple_desc = BlessTupleDesc(tupleDescriptor);

        HASH_SEQ_STATUS status;
        for (int i = 0; i < DIR_PATH_HASH_PARTITION_SIZE; ++i) {
            hash_seq_init(&status, PathDirHash[i]);
            while ((entry = hash_seq_search(&status)) != 0) {
                DirPathHashItem *temp_entry = (DirPathHashItem *)palloc(sizeof(DirPathHashItem));
                memcpy(temp_entry, entry, sizeof(DirPathHashItem));
                returnInfoList = lappend(returnInfoList, temp_entry);
            }
        }

        functionContext->user_fctx = returnInfoList;
        functionContext->max_calls = list_length(returnInfoList);
        MemoryContextSwitchTo(oldContext);
    }
    functionContext = SRF_PERCALL_SETUP();
    returnInfoList = functionContext->user_fctx;
    d_off = functionContext->call_cntr;

    if (d_off < functionContext->max_calls) {
        entry = (DirPathHashItem *)list_nth(returnInfoList, d_off);
        memset(resNulls, false, sizeof(resNulls));
        values[0] = CStringGetTextDatum(entry->key.fileName);
        values[1] = Int32GetDatum(entry->key.parentId);
        values[2] = Int32GetDatum(entry->inodeId);
        RWLock *lock = &entry->lock;
        if (pg_atomic_read_u64(&lock->state) == 0) {
            values[3] = CStringGetTextDatum("no lock");
        } else {
            values[3] = CStringGetTextDatum("locked");
        }
        heapTupleRes = heap_form_tuple(functionContext->tuple_desc, values, resNulls);
        SRF_RETURN_NEXT(functionContext, HeapTupleGetDatum(heapTupleRes));
    }
    SRF_RETURN_DONE(functionContext);
}

Datum falcon_acquire_hash_lock(PG_FUNCTION_ARGS)
{
    char *fileName = PG_GETARG_CSTRING(0);
    uint64_t parentId = (uint64_t)PG_GETARG_INT64(1);
    int lockNum = PG_GETARG_INT64(2);
    RWLockMode lockMode;
    switch (lockNum) {
    case 0: {
        lockMode = DIR_LOCK_NONE;
        break;
    }
    case 1: {
        lockMode = DIR_LOCK_SHARED;
        break;
    }
    case 2: {
        lockMode = DIR_LOCK_EXCLUSIVE;
        break;
    }
    default: {
        FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR, "error lockmode num %d.", lockNum);
    }
    }
    uint64_t res = SearchDirectoryByDirectoryHashTable(NULL, parentId, fileName, lockMode);
    if (res == -1) {
        FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR, "can not find elem %s.", fileName);
    }
    PG_RETURN_INT16(SUCCESS);
}

Datum falcon_release_hash_lock(PG_FUNCTION_ARGS)
{
    char *fileName = PG_GETARG_CSTRING(0);
    uint64_t parentId = (uint64_t)PG_GETARG_INT64(1);

    ReleaseDirPathHashLock(parentId, fileName);

    PG_RETURN_INT16(SUCCESS);
}

static uint32 dir_path_hash(const void *key, Size keysize)
{
    const DirPathHashKey *l = (const DirPathHashKey *)key;
    return DatumGetUInt32(hash_any_extended((const unsigned char *)l->fileName, strlen(l->fileName), l->parentId));
}

static int dir_path_compare(const void *key1, const void *key2)
{
    const DirPathHashKey *d1 = (const DirPathHashKey *)key1;
    const DirPathHashKey *d2 = (const DirPathHashKey *)key2;

    /* First, compare by parentId */
    if (d1->parentId > d2->parentId) {
        return 1;
    } else if (d1->parentId < d2->parentId) {
        return -1;
    }
    /* parentId are equal, do a byte-by-byte comparison on fileName */
    return strcmp(d1->fileName, d2->fileName);
}

static int dir_path_match(const void *key1, const void *key2, Size keysize) { return dir_path_compare(key1, key2); }

static void *dir_path_keycopy(void *dest, const void *src, Size keysize)
{
    const DirPathHashKey *srcVal = (const DirPathHashKey *)src;
    DirPathHashKey *destVal = (DirPathHashKey *)dest;
    destVal->parentId = srcVal->parentId;
    strcpy(destVal->fileName, srcVal->fileName);
    return NULL;
}

static void ReleaseDirPathHashLock(uint64_t parentId, char *filename)
{
    DirPathHashKey dirPathHashKey;
    strcpy(dirPathHashKey.fileName, filename);
    dirPathHashKey.parentId = parentId;

    bool isfound = false;
    uint32 hashcode = dir_path_hash(&dirPathHashKey, sizeof(DirPathHashKey));
    LWLock *lock = DIR_PATH_HASH_PARTITION_LOCK(hashcode);
    LWLockAcquire(lock, LW_SHARED);
    DirPathHashItem *item = (DirPathHashItem *)hash_search_with_hash_value(DIR_PATH_HASH_PARTITION(hashcode),
                                                                           (const void *)&dirPathHashKey,
                                                                           hashcode,
                                                                           HASH_FIND,
                                                                           &isfound);
    LWLockRelease(lock);
    if (!isfound) {
        FALCON_ELOG_ERROR_EXTENDED(FILE_NOT_EXISTS, "elem %s does not exist, can not release lock!", filename);
    } else {
        RWLockRelease(&item->lock);
    }
}

void InsertDirectoryByDirectoryHashTable(Relation relation,
                                         CatalogIndexState indexState,
                                         uint64_t parentId,
                                         const char *name,
                                         uint64_t inodeId,
                                         uint32_t numSubparts,
                                         DirPathLockMode lockMode)
{
    DirPathHashKey dirPathHashKey;
    strcpy(dirPathHashKey.fileName, name);
    dirPathHashKey.parentId = parentId;

    int64_t accessTime = 0, updatedAccessTime = 0;
    bool isfound = false;
    uint32 hashcode = dir_path_hash((const void *)&dirPathHashKey, sizeof(DirPathHashKey));
    LWLock *lock = DIR_PATH_HASH_PARTITION_LOCK(hashcode);
    LWLockAcquire(lock, LW_SHARED);
    DirPathHashItem *item = (DirPathHashItem *)hash_search_with_hash_value(DIR_PATH_HASH_PARTITION(hashcode),
                                                                           (const void *)&dirPathHashKey,
                                                                           hashcode,
                                                                           HASH_FIND,
                                                                           &isfound);
    if (!isfound || item->inodeId == DIR_HASH_TABLE_PATH_UNKNOWN) {
        LWLockRelease(lock);
        uint64_t tempId;
        SearchDirectoryTableInfo(relation, parentId, name, &tempId, &accessTime, &updatedAccessTime);
        if (tempId != DIR_HASH_TABLE_PATH_NOT_EXIST)
            FALCON_ELOG_ERROR(ARGUMENT_ERROR, "target exists.");

        for (;;) {
            LWLockAcquire(lock, LW_EXCLUSIVE);
            item = (DirPathHashItem *)hash_search_with_hash_value(DIR_PATH_HASH_PARTITION(hashcode),
                                                                  (const void *)&dirPathHashKey,
                                                                  hashcode,
                                                                  HASH_ENTER_NULL,
                                                                  &isfound);
            if (!isfound && !item) // no space
            {
                if (lockMode != DIR_LOCK_NONE) {
                    LWLockRelease(lock);
                    EliminateDirPathHashByLRU(DIR_PATH_HASH_PARTITION_INDEX(hashcode));
                    continue;
                }
            } else if (!isfound && item) {
                pg_atomic_fetch_add_u32(DIR_PATH_HASH_PARTITION_COUNT(hashcode), 1);

                RWLockInitialize(&item->lock);
                item->usageCount = 0;
                item->inodeId = tempId;
                item->accessTime = accessTime;
                DirPathHashToCommitAddEntry(dirPathHashKey.parentId, dirPathHashKey.fileName);
            } else if (item->inodeId == DIR_HASH_TABLE_PATH_UNKNOWN) {
                item->inodeId = tempId;
                item->accessTime = 0;
            } else if (item->inodeId != tempId)
                FALCON_ELOG_ERROR(PROGRAM_ERROR, "dir path hash table is corrupt.");
            break;
        }
    }
    if (!item) {
        LWLockRelease(lock);
        InsertIntoDirectoryTable(relation, indexState, parentId, name, inodeId, GetCurrentTimeInUs());
        return;
    }
    item->usageCount++;
    if (lockMode != DIR_LOCK_NONE)
        RWLockDeclare(&item->lock);
    LWLockRelease(lock);

    switch (lockMode) {
    case DIR_LOCK_EXCLUSIVE: {
        RWLockAcquire(&item->lock, RW_EXCLUSIVE);
        break;
    }
    case DIR_LOCK_SHARED: {
        RWLockAcquire(&item->lock, RW_SHARED);
        break;
    }
    case DIR_LOCK_NONE:
        break;
    default:
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "wrong lock Mode");
    }
    if (lockMode != DIR_LOCK_NONE) {
        RWLockUndeclare(&item->lock);
        DirectoryHashTableLastAcquiredLock = &item->lock;
    }

    int64_t currentTime = GetCurrentTimeInUs();
    InsertIntoDirectoryTable(relation, indexState, parentId, name, inodeId, currentTime);

    DirPathHashToCommitUpdateEntry(parentId, name, inodeId, currentTime);
}

uint64_t
SearchDirectoryByDirectoryHashTable(Relation relation, uint64_t parentId, const char *name, DirPathLockMode lockMode)
{
    uint64_t inodeId;

    DirPathHashKey dirPathHashKey;
    strcpy(dirPathHashKey.fileName, name);
    dirPathHashKey.parentId = parentId;

    int64_t accessTime = 0, updatedAccessTime = 0;
	bool accessTimeChecked = false;
    bool isfound = false;
    uint32 hashcode = dir_path_hash((const void *)&dirPathHashKey, sizeof(DirPathHashKey));
    LWLock *lock = DIR_PATH_HASH_PARTITION_LOCK(hashcode);
    LWLockAcquire(lock, LW_SHARED);
    DirPathHashItem *item = (DirPathHashItem *)hash_search_with_hash_value(DIR_PATH_HASH_PARTITION(hashcode),
                                                                           (const void *)&dirPathHashKey,
                                                                           hashcode,
                                                                           HASH_FIND,
                                                                           &isfound);
    if (!isfound || item->inodeId == DIR_HASH_TABLE_PATH_UNKNOWN) {
        LWLockRelease(lock);
        SearchDirectoryTableInfo(relation, parentId, name, &inodeId, &accessTime, &updatedAccessTime);
        accessTimeChecked = true;

        for (;;) {
            LWLockAcquire(lock, LW_EXCLUSIVE);
            item = (DirPathHashItem *)hash_search_with_hash_value(DIR_PATH_HASH_PARTITION(hashcode),
                                                                  (const void *)&dirPathHashKey,
                                                                  hashcode,
                                                                  HASH_ENTER_NULL,
                                                                  &isfound);
            if (!isfound && !item) // no space, and must allocate space for rwlock
            {
                if (lockMode != DIR_LOCK_NONE) {
                    LWLockRelease(lock);
                    EliminateDirPathHashByLRU(DIR_PATH_HASH_PARTITION_INDEX(hashcode));
                    continue;
                }
            } else if (!isfound && item) {
                pg_atomic_fetch_add_u32(DIR_PATH_HASH_PARTITION_COUNT(hashcode), 1);

                RWLockInitialize(&item->lock);
                item->usageCount = 0;
                item->inodeId = inodeId;
                item->accessTime = accessTime;
                DirPathHashToCommitAddEntry(dirPathHashKey.parentId, dirPathHashKey.fileName);
            } else if (item->inodeId == DIR_HASH_TABLE_PATH_UNKNOWN) {
                item->inodeId = inodeId;
                item->accessTime = accessTime;
            } else if (item->inodeId != inodeId)
                FALCON_ELOG_ERROR(PROGRAM_ERROR, "dir path hash table is corrupt.");
            break;
        }
    }
    if (!item) {
        LWLockRelease(lock);
        return inodeId;
    }
    item->usageCount++;
    if (lockMode != DIR_LOCK_NONE)
        RWLockDeclare(&item->lock);
    LWLockRelease(lock);

    switch (lockMode) {
    case DIR_LOCK_EXCLUSIVE: {
        RWLockAcquire(&item->lock, RW_EXCLUSIVE);
        break;
    }
    case DIR_LOCK_SHARED: {
        RWLockAcquire(&item->lock, RW_SHARED);
        break;
    }
    case DIR_LOCK_NONE:
        break;
    default:
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "wrong lock Mode");
    }
    if (lockMode != DIR_LOCK_NONE) {
        RWLockUndeclare(&item->lock);
        DirectoryHashTableLastAcquiredLock = &item->lock;
    }

    if (item->inodeId == DIR_HASH_TABLE_PATH_NOT_EXIST)
	{
		return item->inodeId;
	}
    if (!accessTimeChecked)
	{
		int64_t currentTime = GetCurrentTimeInUs();
		accessTime = item->accessTime;
		if (NeedRenewMetaAccessTime(accessTime, currentTime))
		{
			updatedAccessTime = currentTime;
			LockRelation(relation, RowExclusiveLock);
			RenewDirectoryTableAccessTime(relation, parentId, name, updatedAccessTime);
			UnlockRelation(relation, RowExclusiveLock);
		}
	}
    if (updatedAccessTime != 0)
    {
        DirPathHashToCommitUpdateEntry(dirPathHashKey.parentId, dirPathHashKey.fileName, item->inodeId, updatedAccessTime);
    }

    return item->inodeId;
}

void DeleteDirectoryByDirectoryHashTable(Relation relation,
                                         uint64_t parentId,
                                         const char *name,
                                         DirPathLockMode lockMode,
                                         bool forExpired,
                                         uint64_t* inodeId)
{
    if (lockMode == DIR_LOCK_SHARED)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "not supported lockmode while deleting.");
    DirPathHashKey dirPathHashKey;
    strcpy(dirPathHashKey.fileName, name);
    dirPathHashKey.parentId = parentId;

    bool isfound = false;
    uint32 hashcode = dir_path_hash((const void *)&dirPathHashKey, sizeof(DirPathHashKey));
    LWLock *lock = DIR_PATH_HASH_PARTITION_LOCK(hashcode);
    LWLockAcquire(lock, LW_SHARED);
    DirPathHashItem *item = (DirPathHashItem *)hash_search_with_hash_value(DIR_PATH_HASH_PARTITION(hashcode),
                                                                           (const void *)&dirPathHashKey,
                                                                           hashcode,
                                                                           HASH_FIND,
                                                                           &isfound);
    if (!isfound) {
        LWLockRelease(lock);

        for (;;) {
            LWLockAcquire(lock, LW_EXCLUSIVE);
            item = (DirPathHashItem *)hash_search_with_hash_value(DIR_PATH_HASH_PARTITION(hashcode),
                                                                  (const void *)&dirPathHashKey,
                                                                  hashcode,
                                                                  HASH_ENTER_NULL,
                                                                  &isfound);
            if (!isfound && !item) // no space, and must allocate space for rwlock
            {
                if (lockMode != DIR_LOCK_NONE) {
                    LWLockRelease(lock);
                    EliminateDirPathHashByLRU(DIR_PATH_HASH_PARTITION_INDEX(hashcode));
                    continue;
                }
            } else if (!isfound && item) {
                pg_atomic_fetch_add_u32(DIR_PATH_HASH_PARTITION_COUNT(hashcode), 1);

                RWLockInitialize(&item->lock);
                item->usageCount = 0;
                item->inodeId = DIR_HASH_TABLE_PATH_UNKNOWN;
                DirPathHashToCommitAddEntry(dirPathHashKey.parentId, dirPathHashKey.fileName);
            }
            break;
        }
    }
    if (!item) {
        LWLockRelease(lock);
        DeleteFromDirectoryTable(relation, parentId, name, forExpired, inodeId);
        return;
    }
    item->usageCount++;
    if (lockMode == DIR_LOCK_EXCLUSIVE)
        RWLockDeclare(&item->lock);
    LWLockRelease(lock);

    if (lockMode == DIR_LOCK_EXCLUSIVE) {
        RWLockAcquire(&item->lock, RW_EXCLUSIVE);
        RWLockUndeclare(&item->lock);
        DirectoryHashTableLastAcquiredLock = &item->lock;
    }

    DeleteFromDirectoryTable(relation, parentId, name, forExpired, inodeId);

    DirPathHashToCommitUpdateEntry(dirPathHashKey.parentId, dirPathHashKey.fileName, DIR_HASH_TABLE_PATH_NOT_EXIST, 0);
}

static DirPathHashItem *FindNextItem(HASH_SEQ_STATUS *status, int32_t *destroyableCnt)
{
    DirPathHashItem *entry;
    while ((entry = hash_seq_search(status)) != NULL) {
        if (!RWLockCheckDestroyable(&entry->lock)) {
            continue;
        }
        if (destroyableCnt) {
            (*destroyableCnt)++;
        }
        entry->usageCount--;
        if (entry->usageCount <= 0) {
            hash_seq_term(status);
            break;
        }
    }
    return entry;
}

static bool EliminateDirPathHashByLRU(int partitionIndex)
{
    if (pg_atomic_read_u32(&(PathDirHashEntryCount[partitionIndex])) <=
        MAX_BUCKET_SUM_LRU_CLAER_BEGIN / DIR_PATH_HASH_PARTITION_SIZE)
        return false;

    HASH_SEQ_STATUS status;
    int32_t destroyableCnt = 0;
    DirPathHashItem *entry = NULL;
    DirPathHashKey target;

    // eliminate one
    for (;;) {
        LWLockAcquire(&(DirPathLWLockArray[partitionIndex].lock), LW_SHARED);
        hash_seq_init(&status, PathDirHash[partitionIndex]);
        status.curBucket = rand() % PathDirHash[partitionIndex]->hctl->max_bucket;
        entry = FindNextItem(&status, NULL);
        while (entry == NULL) {
            destroyableCnt = 0;
            hash_seq_init(&status, PathDirHash[partitionIndex]);
            entry = FindNextItem(&status, &destroyableCnt);
            if (destroyableCnt == 0) {
                break;
            }
        }
        if (entry != NULL) {
            strcpy(target.fileName, entry->key.fileName);
            target.parentId = entry->key.parentId;
        }
        LWLockRelease(&(DirPathLWLockArray[partitionIndex].lock));

        if (entry == NULL) // no candidate
            return false;

        bool found;
        uint32 hashcode = dir_path_hash((const void *)&target, sizeof(DirPathHashKey));
        LWLockAcquire(&(DirPathLWLockArray[partitionIndex].lock), LW_EXCLUSIVE);
        entry = hash_search_with_hash_value(PathDirHash[partitionIndex],
                                            (const void *)&target,
                                            hashcode,
                                            HASH_FIND,
                                            &found);
        if (!found || !RWLockCheckDestroyable(&entry->lock)) {
            LWLockRelease(&(DirPathLWLockArray[partitionIndex].lock));
            continue;
        }
        hash_search_with_hash_value(PathDirHash[partitionIndex], (const void *)&target, hashcode, HASH_REMOVE, &found);
        LWLockRelease(&(DirPathLWLockArray[partitionIndex].lock));
        if (found) // succeed in eliminating
            break;
    }

    pg_atomic_fetch_sub_u32(&(PathDirHashEntryCount[partitionIndex]), 1);
    return true;
}

void CommitForDirPathHash()
{
    // switch()
    for (int i = 0; i < DirPathHashToCommitSize; ++i) {
        switch (DirPathHashToCommitAction[i]) {
        case 'A': {
            uint32 hashcode =
                dir_path_hash((const void *)&(DirPathHashToCommitActionInfo[i].key), sizeof(DirPathHashKey));
            int partitionIndex = DIR_PATH_HASH_PARTITION_INDEX(hashcode);
            EliminateDirPathHashByLRU(partitionIndex);

            break;
        }
        case 'U': {
            bool found;
            uint32 hashcode =
                dir_path_hash((const void *)&(DirPathHashToCommitActionInfo[i].key), sizeof(DirPathHashKey));
            LWLock *lock = DIR_PATH_HASH_PARTITION_LOCK(hashcode);
            LWLockAcquire(lock, LW_EXCLUSIVE);
            DirPathHashItem *item = hash_search_with_hash_value(DIR_PATH_HASH_PARTITION(hashcode),
                                                                (const void *)&(DirPathHashToCommitActionInfo[i].key),
                                                                hashcode,
                                                                HASH_ENTER_NULL,
                                                                &found);
            if (!found && item) {
                pg_atomic_fetch_add_u32(DIR_PATH_HASH_PARTITION_COUNT(hashcode), 1);

                item->key.parentId = DirPathHashToCommitActionInfo[i].key.parentId;
                strcpy(item->key.fileName, DirPathHashToCommitActionInfo[i].key.fileName);
            }
            if (item)
            {
                item->inodeId = DirPathHashToCommitActionInfo[i].inodeId;
                item->accessTime = DirPathHashToCommitActionInfo[i].accessTime;
            }
            LWLockRelease(lock);
            break;
        }
        default: {
            FALCON_ELOG_ERROR(PROGRAM_ERROR, "wrong action !");
        }
        }
    }

    DirPathHashToCommitClear();
}

void AbortForDirPathHash() { DirPathHashToCommitClear(); }

void ClearDirPathHash()
{
    for (int i = 0; i < DIR_PATH_HASH_PARTITION_SIZE; ++i) {
        LWLockAcquire(&(DirPathLWLockArray[i].lock), LW_EXCLUSIVE);
        hash_clear(PathDirHash[i]);
        LWLockRelease(&(DirPathLWLockArray[i].lock));
    }
}

size_t DirPathShmemsize()
{
    return (sizeof(LWLockPadded) + sizeof(pg_atomic_uint32)) * DIR_PATH_HASH_PARTITION_SIZE +
           (sizeof(DirPathHashItem)) * MAX_DIRECTORY_PATH_HASH_CAPACITY;
}

void DirPathShmemInit()
{
    srand((unsigned)time(NULL) + 0xD5F1);

    bool initialized;
    DirPathLWLockArray =
        ShmemInitStruct("Cucuoo path directory walk path resolution LWLock",
                        (sizeof(LWLockPadded) + sizeof(pg_atomic_uint32)) * DIR_PATH_HASH_PARTITION_SIZE,
                        &initialized);
    PathDirHashEntryCount = (pg_atomic_uint32 *)(DirPathLWLockArray + DIR_PATH_HASH_PARTITION_SIZE);
    if (!initialized) {
        DirPathLWLockTrancheId = LWLockNewTrancheId();
        LWLockRegisterTranche(DirPathLWLockTrancheId, DirPathLWLockTrancheName);
        for (int i = 0; i < DIR_PATH_HASH_PARTITION_SIZE; ++i) {
            LWLockInitialize(&(DirPathLWLockArray[i].lock), DirPathLWLockTrancheId);
            pg_atomic_init_u32(PathDirHashEntryCount + i, 0);
        }
    }
    HASHCTL info;

    info.keysize = sizeof(DirPathHashKey);
    info.entrysize = sizeof(DirPathHashItem);
    info.hash = dir_path_hash;
    info.match = dir_path_match;
    info.keycopy = dir_path_keycopy;
    char buf[64];
    for (int i = 0; i < DIR_PATH_HASH_PARTITION_SIZE; ++i) {
        sprintf(buf, "Falcon path directory hash %d", i);
        PathDirHash[i] = ShmemInitHash(buf,
                                       MAX_DIRECTORY_PATH_HASH_CAPACITY / DIR_PATH_HASH_PARTITION_SIZE,
                                       MAX_DIRECTORY_PATH_HASH_CAPACITY / DIR_PATH_HASH_PARTITION_SIZE,
                                       &info,
                                       HASH_ELEM | HASH_FUNCTION | HASH_KEYCOPY | HASH_COMPARE);
        if (!PathDirHash[i]) {
            elog(FATAL, "invalid shmem status when creating path directory hash ");
        }
    }
}
