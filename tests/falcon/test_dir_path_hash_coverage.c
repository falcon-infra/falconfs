#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "postgres.h"
#include "access/htup.h"
#include "catalog/indexing.h"
#include "funcapi.h"
#include "nodes/pg_list.h"
#include "storage/lwlock.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rwlock.h"

#include "dir_path_shmem/dir_path_hash.h"

#ifdef vsnprintf
#undef vsnprintf
#endif
#ifdef vsprintf
#undef vsprintf
#endif
#ifdef snprintf
#undef snprintf
#endif

#define TEST_DIR_PATH_HASH_PARTITION_SIZE 128

typedef struct FakeHash
{
    HTAB *tab;
    DirPathHashItem entries[64];
    bool used[64];
    int capacity;
} FakeHash;

static char g_shmem[32768];
static bool g_shmem_found;
static FakeHash g_hashes[TEST_DIR_PATH_HASH_PARTITION_SIZE * 2];
static int g_hash_count;
static int g_insert_count;
static int g_delete_count;
static uint64_t g_directory_lookup_result;
MemoryContext CurrentMemoryContext = NULL;

static int ExpectTrue(bool condition, const char *message)
{
    if (!condition) {
        (void)message;
        return 1;
    }
    return 0;
}

static void ResetHarness(void)
{
    memset(g_shmem, 0, sizeof(g_shmem));
    memset(g_hashes, 0, sizeof(g_hashes));
    g_shmem_found = false;
    g_hash_count = 0;
    g_insert_count = 0;
    g_delete_count = 0;
    g_directory_lookup_result = DIR_HASH_TABLE_PATH_NOT_EXIST;
}

static FakeHash *FindFakeHash(HTAB *hashp)
{
    for (int i = 0; i < g_hash_count; ++i) {
        if (g_hashes[i].tab == hashp) {
            return &g_hashes[i];
        }
    }
    return NULL;
}

static int FindEntry(FakeHash *hash, const DirPathHashKey *key)
{
    for (int i = 0; i < hash->capacity; ++i) {
        if (hash->used[i] && hash->entries[i].key.parentId == key->parentId &&
            strcmp(hash->entries[i].key.fileName, key->fileName) == 0) {
            return i;
        }
    }
    return -1;
}

static int FirstFreeEntry(FakeHash *hash)
{
    for (int i = 0; i < hash->capacity; ++i) {
        if (!hash->used[i]) {
            return i;
        }
    }
    return -1;
}

void SearchDirectoryTableInfo(Relation directoryRel, uint64_t parentId, const char *name, uint64_t *inodeId)
{
    (void)directoryRel;
    (void)parentId;
    (void)name;
    *inodeId = g_directory_lookup_result;
}

void InsertIntoDirectoryTable(Relation directoryRel,
                              CatalogIndexState indexState,
                              uint64_t parentId,
                              const char *name,
                              uint64_t inodeId)
{
    (void)directoryRel;
    (void)indexState;
    (void)parentId;
    (void)name;
    (void)inodeId;
    g_insert_count++;
}

void DeleteFromDirectoryTable(Relation directoryRel, uint64_t parentId, const char *name)
{
    (void)directoryRel;
    (void)parentId;
    (void)name;
    g_delete_count++;
}

void *ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
    (void)name;
    if (size > sizeof(g_shmem)) {
        abort();
    }
    *foundPtr = g_shmem_found;
    return g_shmem;
}

int LWLockNewTrancheId(void)
{
    return 41;
}

void LWLockRegisterTranche(int tranche_id, const char *tranche_name)
{
    (void)tranche_id;
    (void)tranche_name;
}

void LWLockInitialize(LWLock *lock, int tranche_id)
{
    (void)lock;
    (void)tranche_id;
}

bool LWLockAcquire(LWLock *lock, LWLockMode mode)
{
    (void)lock;
    (void)mode;
    return true;
}

void LWLockRelease(LWLock *lock)
{
    (void)lock;
}

void RWLockInitialize(RWLock *lock)
{
    pg_atomic_init_u64(&lock->state, 0);
}

void RWLockDeclare(RWLock *lock)
{
    pg_atomic_fetch_add_u64(&lock->state, 1);
}

void RWLockUndeclare(RWLock *lock)
{
    pg_atomic_fetch_sub_u64(&lock->state, 1);
}

bool RWLockCheckDestroyable(RWLock *lock)
{
    return pg_atomic_read_u64(&lock->state) == 0;
}

void RWLockAcquire(RWLock *lock, RWLockMode mode)
{
    (void)mode;
    pg_atomic_fetch_add_u64(&lock->state, 1);
}

void RWLockRelease(RWLock *lock)
{
    pg_atomic_fetch_sub_u64(&lock->state, 1);
}

void ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
    (void)conditionName;
    (void)fileName;
    (void)lineNumber;
    abort();
}

bool errstart(int elevel, const char *domain)
{
    (void)elevel;
    (void)domain;
    return true;
}

bool errstart_cold(int elevel, const char *domain)
{
    return errstart(elevel, domain);
}

int errmsg_internal(const char *fmt, ...)
{
    (void)fmt;
    return 0;
}

int errmsg(const char *fmt, ...)
{
    (void)fmt;
    return 0;
}

void errfinish(const char *filename, int lineno, const char *funcname)
{
    (void)filename;
    (void)lineno;
    (void)funcname;
}

void *palloc(Size size)
{
    return calloc(1, size);
}

void pfree(void *pointer)
{
    free(pointer);
}

List *lappend(List *list, void *datum)
{
    (void)list;
    (void)datum;
    return NIL;
}

TypeFuncClass get_call_result_type(FunctionCallInfo fcinfo, Oid *resultTypeId, TupleDesc *resultTupleDesc)
{
    (void)fcinfo;
    (void)resultTypeId;
    (void)resultTupleDesc;
    return TYPEFUNC_COMPOSITE;
}

TupleDesc BlessTupleDesc(TupleDesc tupdesc)
{
    return tupdesc;
}

HeapTuple heap_form_tuple(TupleDesc tupleDescriptor, const Datum *values, const bool *isnull)
{
    (void)tupleDescriptor;
    (void)values;
    (void)isnull;
    return NULL;
}

Datum HeapTupleHeaderGetDatum(const HeapTupleHeader tuple)
{
    (void)tuple;
    return (Datum)0;
}

uint64 hash_bytes_extended(const unsigned char *k, int keylen, uint64 seed)
{
    uint64 value = seed;
    for (int i = 0; i < keylen; ++i) {
        value = value * 131 + k[i];
    }
    return value;
}

FuncCallContext *init_MultiFuncCall(PG_FUNCTION_ARGS)
{
    (void)fcinfo;
    return calloc(1, sizeof(FuncCallContext));
}

FuncCallContext *per_MultiFuncCall(PG_FUNCTION_ARGS)
{
    (void)fcinfo;
    return calloc(1, sizeof(FuncCallContext));
}

void end_MultiFuncCall(PG_FUNCTION_ARGS, FuncCallContext *funcctx)
{
    (void)fcinfo;
    free(funcctx);
}

text *cstring_to_text(const char *s)
{
    return (text *)s;
}

int pg_sprintf(char *str, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    int ret = vsprintf(str, fmt, args);
    va_end(args);
    return ret;
}

int pg_snprintf(char *str, size_t count, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    int ret = vsnprintf(str, count, fmt, args);
    va_end(args);
    return ret;
}

#include "../../falcon/dir_path_shmem/dir_path_hash.c"

HTAB *ShmemInitHash(const char *name, long init_size, long max_size, HASHCTL *infoP, int hash_flags)
{
    (void)name;
    (void)init_size;
    (void)max_size;
    (void)infoP;
    (void)hash_flags;
    FakeHash *hash = &g_hashes[g_hash_count++];
    hash->capacity = 64;
    hash->tab = calloc(1, sizeof(HTAB));
    hash->tab->hctl = calloc(1, sizeof(HASHHDR));
    hash->tab->hctl->max_bucket = hash->capacity;
    return hash->tab;
}

void *hash_search_with_hash_value(HTAB *hashp, const void *keyPtr, uint32 hashvalue, HASHACTION action, bool *foundPtr)
{
    (void)hashvalue;
    FakeHash *hash = FindFakeHash(hashp);
    const DirPathHashKey *key = (const DirPathHashKey *)keyPtr;
    int index = FindEntry(hash, key);
    if (foundPtr) {
        *foundPtr = index >= 0;
    }

    if (action == HASH_FIND) {
        return index >= 0 ? &hash->entries[index] : NULL;
    }
    if (action == HASH_REMOVE) {
        if (index >= 0) {
            hash->used[index] = false;
            return &hash->entries[index];
        }
        return NULL;
    }
    if (action == HASH_ENTER_NULL) {
        if (index >= 0) {
            return &hash->entries[index];
        }
        index = FirstFreeEntry(hash);
        if (index < 0) {
            return NULL;
        }
        hash->used[index] = true;
        memset(&hash->entries[index], 0, sizeof(hash->entries[index]));
        hash->entries[index].key = *key;
        return &hash->entries[index];
    }
    return NULL;
}

void hash_clear(HTAB *hashp)
{
    FakeHash *hash = FindFakeHash(hashp);
    memset(hash->used, 0, sizeof(hash->used));
}

void hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp)
{
    status->hashp = hashp;
    status->curBucket = 0;
    status->curEntry = NULL;
}

void *hash_seq_search(HASH_SEQ_STATUS *status)
{
    FakeHash *hash = FindFakeHash(status->hashp);
    while (status->curBucket < (uint32)hash->capacity) {
        uint32 index = status->curBucket++;
        if (hash->used[index]) {
            return &hash->entries[index];
        }
    }
    return NULL;
}

void hash_seq_term(HASH_SEQ_STATUS *status)
{
    (void)status;
}

static int TestInitInsertSearchDeleteAndCommitFlow(void)
{
    ResetHarness();
    DirPathShmemInit();
    g_shmem_found = true;
    DirPathShmemInit();

    /*
     * Insert goes through the miss path, initializes a hash entry, records both
     * add/update commit actions, and then the commit path materializes the final
     * inode id in the hash table.
     */
    InsertDirectoryByDirectoryHashTable(NULL, NULL, 10, "alpha", 1001, 0, DIR_LOCK_EXCLUSIVE);
    CommitForDirPathHash();
    if (ExpectTrue(g_insert_count == 1, "insert delegates to directory table"))
        return 1;

    uint64_t inode = SearchDirectoryByDirectoryHashTable(NULL, 10, "alpha", DIR_LOCK_SHARED);
    if (ExpectTrue(inode == 1001 && DirectoryHashTableLastAcquiredLock != NULL, "search returns committed inode"))
        return 1;
    RWLockRelease(DirectoryHashTableLastAcquiredLock);
    DirectoryHashTableLastAcquiredLock = NULL;

    DeleteDirectoryByDirectoryHashTable(NULL, 10, "alpha", DIR_LOCK_EXCLUSIVE);
    CommitForDirPathHash();
    if (ExpectTrue(g_delete_count == 1, "delete delegates to directory table"))
        return 1;
    inode = SearchDirectoryByDirectoryHashTable(NULL, 10, "alpha", DIR_LOCK_NONE);
    if (ExpectTrue(inode == DIR_HASH_TABLE_PATH_NOT_EXIST, "deleted entry is committed as not-exist"))
        return 1;

    DirPathHashToCommitAddEntry(11, "abort_only");
    AbortForDirPathHash();
    CommitForDirPathHash();
    ClearDirPathHash();
    return ExpectTrue(DirPathShmemsize() > 0, "shared-memory size is nonzero");
}

static int TestLRUEliminatesDestroyableEntries(void)
{
    ResetHarness();
    DirPathShmemInit();

    FakeHash *hash = FindFakeHash(PathDirHash[0]);
    for (int i = 0; i < 14; ++i) {
        hash->used[i] = true;
        snprintf(hash->entries[i].key.fileName, sizeof(hash->entries[i].key.fileName), "lru_%d", i);
        hash->entries[i].key.parentId = 99;
        hash->entries[i].inodeId = 2000 + i;
        hash->entries[i].usageCount = 0;
        RWLockInitialize(&hash->entries[i].lock);
    }
    pg_atomic_write_u32(PathDirHashEntryCount, 14);

    /*
     * LRU elimination should scan a partition, choose a destroyable entry,
     * remove it, and decrement that partition's entry count.
     */
    if (ExpectTrue(EliminateDirPathHashByLRU(0), "LRU removes a destroyable entry"))
        return 1;
    if (ExpectTrue(pg_atomic_read_u32(PathDirHashEntryCount) == 13, "LRU decrements partition entry count"))
        return 1;

    pg_atomic_write_u32(PathDirHashEntryCount, 1);
    return ExpectTrue(!EliminateDirPathHashByLRU(0), "LRU skips partitions below threshold");
}

int main(void)
{
    if (TestInitInsertSearchDeleteAndCommitFlow())
        return 1;
    if (TestLRUEliminatesDestroyableEntries())
        return 1;
    return 0;
}
