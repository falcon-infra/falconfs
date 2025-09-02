/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "utils/utils.h"

#include <string.h>

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_extension_d.h"
#include "catalog/pg_type_d.h"
#include "common/hashfn.h"
#include "nodes/parsenodes.h"
#include "storage/lockdefs.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/varlena.h"
#include "commands/extension.h"

#include "dir_path_shmem/dir_path_hash.h"
#include "metadb/directory_table.h"
#include "metadb/inode_table.h"
#include "metadb/shard_table.h"
#include "metadb/xattr_table.h"
#include "utils/error_log.h"

uint64_t GenerateInodeIdBySeqAndNodeId(uint64_t seq, int nodeId) { return (seq << 12) | (nodeId & 0xFFF); }

int32 HashShard(uint64 parentId_partId)
{
    int64 val = (int64)(parentId_partId & PART_ID_MASK);
    uint32 lohalf = (uint32)val;
    uint32 hihalf = (uint32)(val >> 32);

    lohalf ^= (val >= 0) ? hihalf : ~hihalf;

    int32_t res = (int32)hash_bytes_uint32(lohalf);
    res &= ~(1u << 31);
    return res;
}

bool CheckIfRelationExists(const char *relationName, Oid relNamespace)
{
    Oid oid = get_relname_relid(relationName, relNamespace);
    return oid != InvalidOid;
}

static void InitializeInvalidationCallbacks(void);

Oid CachedRelationOid[LAST_CACHED_RELATION_TYPE] = {0};

void GetRelationOid(const char *relationName, Oid *relOid)
{
    InitializeInvalidationCallbacks();
    if (*relOid == InvalidOid) {
        *relOid = get_relname_relid(relationName, PG_CATALOG_NAMESPACE);

        if (*relOid == InvalidOid) {
            FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR, "GetRelationOid failed for %s!", relationName);
        }
    }
}

bool CheckFalconHasBeenLoaded(void)
{
    if (IsBinaryUpgrade) {
        return false;
    }
    Oid falconExtensionOid = get_extension_oid("falcon", true);
    if (falconExtensionOid == InvalidOid || (creating_extension && CurrentExtensionObject == falconExtensionOid)) {
        return false;
    }
    return true;
}

static void InitializeForeignServerTableScanCache(void);
static void InitializeDirectoryTableScanCache(void);
static void InitializeInodeTableScanCache(void);
static void InitializeInodeTableIndexParentIdPartIdNameScanCache(void);
static void InitializeXattrTableScanCache(void);
static void InitializeSliceTableScanCache(void);
static void InitializeKvmetaTableScanCache(void);

static MemoryContext ScanCacheMemoryContext = NULL;

ScanKeyData ForeignServerTableScanKey[LAST_FALCON_FOREIGN_SERVER_TABLE_SCANKEY_TYPE];
static void InitializeForeignServerTableScanCache(void)
{
    memset(ForeignServerTableScanKey, 0, sizeof(ForeignServerTableScanKey));

    fmgr_info_cxt(F_INT4EQ,
                  &ForeignServerTableScanKey[FOREIGN_SERVER_TABLE_SERVER_ID_EQ].sk_func,
                  ScanCacheMemoryContext);
    ForeignServerTableScanKey[FOREIGN_SERVER_TABLE_SERVER_ID_EQ].sk_strategy = BTEqualStrategyNumber;
    ForeignServerTableScanKey[FOREIGN_SERVER_TABLE_SERVER_ID_EQ].sk_subtype = INT4OID;
    ForeignServerTableScanKey[FOREIGN_SERVER_TABLE_SERVER_ID_EQ].sk_collation = DEFAULT_COLLATION_OID;
    ForeignServerTableScanKey[FOREIGN_SERVER_TABLE_SERVER_ID_EQ].sk_attno = Anum_falcon_foreign_server_server_id;
}

ScanKeyData DirectoryTableScanKey[LAST_FALCON_DIRECTORY_TABLE_SCANKEY_TYPE];
static void InitializeDirectoryTableScanCache(void)
{
    memset(DirectoryTableScanKey, 0, sizeof(DirectoryTableScanKey));

    fmgr_info_cxt(F_INT8EQ, &DirectoryTableScanKey[DIRECTORY_TABLE_PARENT_ID_EQ].sk_func, ScanCacheMemoryContext);
    DirectoryTableScanKey[DIRECTORY_TABLE_PARENT_ID_EQ].sk_strategy = BTEqualStrategyNumber;
    DirectoryTableScanKey[DIRECTORY_TABLE_PARENT_ID_EQ].sk_subtype = INT8OID;
    DirectoryTableScanKey[DIRECTORY_TABLE_PARENT_ID_EQ].sk_collation = DEFAULT_COLLATION_OID;
    DirectoryTableScanKey[DIRECTORY_TABLE_PARENT_ID_EQ].sk_attno = Anum_falcon_directory_table_parent_id;

    fmgr_info_cxt(F_TEXTEQ, &DirectoryTableScanKey[DIRECTORY_TABLE_NAME_EQ].sk_func, ScanCacheMemoryContext);
    DirectoryTableScanKey[DIRECTORY_TABLE_NAME_EQ].sk_strategy = BTEqualStrategyNumber;
    DirectoryTableScanKey[DIRECTORY_TABLE_NAME_EQ].sk_subtype = TEXTOID;
    DirectoryTableScanKey[DIRECTORY_TABLE_NAME_EQ].sk_collation = DEFAULT_COLLATION_OID;
    DirectoryTableScanKey[DIRECTORY_TABLE_NAME_EQ].sk_attno = Anum_falcon_directory_table_name;
}

ScanKeyData InodeTableScanKey[LAST_FALCON_INODE_TABLE_SCANKEY_TYPE];
static void InitializeInodeTableScanCache(void)
{
    memset(InodeTableScanKey, 0, sizeof(InodeTableScanKey));

    fmgr_info_cxt(F_INT8GT, &InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_GT].sk_func, ScanCacheMemoryContext);
    InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_GT].sk_strategy = BTGreaterStrategyNumber;
    InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_GT].sk_subtype = INT8OID;
    InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_GT].sk_collation = DEFAULT_COLLATION_OID;
    InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_GT].sk_attno = Anum_pg_dfs_file_parentid_partid;

    fmgr_info_cxt(F_INT8GE, &InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_GE].sk_func, ScanCacheMemoryContext);
    InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_GE].sk_strategy = BTGreaterEqualStrategyNumber;
    InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_GE].sk_subtype = INT8OID;
    InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_GE].sk_collation = DEFAULT_COLLATION_OID;
    InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_GE].sk_attno = Anum_pg_dfs_file_parentid_partid;

    fmgr_info_cxt(F_INT8LE, &InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_LE].sk_func, ScanCacheMemoryContext);
    InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_LE].sk_strategy = BTLessEqualStrategyNumber;
    InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_LE].sk_subtype = INT8OID;
    InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_LE].sk_collation = DEFAULT_COLLATION_OID;
    InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_LE].sk_attno = Anum_pg_dfs_file_parentid_partid;

    fmgr_info_cxt(F_INT8EQ, &InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_EQ].sk_func, ScanCacheMemoryContext);
    InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_EQ].sk_strategy = BTEqualStrategyNumber;
    InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_EQ].sk_subtype = INT8OID;
    InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_EQ].sk_collation = DEFAULT_COLLATION_OID;
    InodeTableScanKey[INODE_TABLE_PARENT_ID_PART_ID_EQ].sk_attno = Anum_pg_dfs_file_parentid_partid;

    fmgr_info_cxt(F_TEXT_GT, &InodeTableScanKey[INODE_TABLE_NAME_GT].sk_func, ScanCacheMemoryContext);
    InodeTableScanKey[INODE_TABLE_NAME_GT].sk_strategy = BTGreaterStrategyNumber;
    InodeTableScanKey[INODE_TABLE_NAME_GT].sk_subtype = TEXTOID;
    InodeTableScanKey[INODE_TABLE_NAME_GT].sk_collation = DEFAULT_COLLATION_OID;
    InodeTableScanKey[INODE_TABLE_NAME_GT].sk_attno = Anum_pg_dfs_file_name;

    fmgr_info_cxt(F_TEXTEQ, &InodeTableScanKey[INODE_TABLE_NAME_EQ].sk_func, ScanCacheMemoryContext);
    InodeTableScanKey[INODE_TABLE_NAME_EQ].sk_strategy = BTEqualStrategyNumber;
    InodeTableScanKey[INODE_TABLE_NAME_EQ].sk_subtype = TEXTOID;
    InodeTableScanKey[INODE_TABLE_NAME_EQ].sk_collation = DEFAULT_COLLATION_OID;
    InodeTableScanKey[INODE_TABLE_NAME_EQ].sk_attno = Anum_pg_dfs_file_name;
}

ScanKeyData InodeTableIndexParentIdPartIdNameScanKey[LAST_FALCON_INODE_TABLE_INDEX_PARENT_ID_PART_ID_NAME_SCANKEY_TYPE];
static void InitializeInodeTableIndexParentIdPartIdNameScanCache(void)
{
    memset(InodeTableIndexParentIdPartIdNameScanKey, 0, sizeof(InodeTableIndexParentIdPartIdNameScanKey));

    fmgr_info_cxt(F_INT8EQ,
                  &InodeTableIndexParentIdPartIdNameScanKey[INODE_TABLE_INDEX_PARENT_ID_PART_ID_EQ].sk_func,
                  ScanCacheMemoryContext);
    InodeTableIndexParentIdPartIdNameScanKey[INODE_TABLE_INDEX_PARENT_ID_PART_ID_EQ].sk_strategy =
        BTEqualStrategyNumber;
    InodeTableIndexParentIdPartIdNameScanKey[INODE_TABLE_INDEX_PARENT_ID_PART_ID_EQ].sk_subtype = INT8OID;
    InodeTableIndexParentIdPartIdNameScanKey[INODE_TABLE_INDEX_PARENT_ID_PART_ID_EQ].sk_collation =
        DEFAULT_COLLATION_OID;
    InodeTableIndexParentIdPartIdNameScanKey[INODE_TABLE_INDEX_PARENT_ID_PART_ID_EQ].sk_attno = 1;

    fmgr_info_cxt(F_TEXTEQ,
                  &InodeTableIndexParentIdPartIdNameScanKey[INODE_TABLE_INDEX_NAME_EQ].sk_func,
                  ScanCacheMemoryContext);
    InodeTableIndexParentIdPartIdNameScanKey[INODE_TABLE_INDEX_NAME_EQ].sk_strategy = BTEqualStrategyNumber;
    InodeTableIndexParentIdPartIdNameScanKey[INODE_TABLE_INDEX_NAME_EQ].sk_subtype = TEXTOID;
    InodeTableIndexParentIdPartIdNameScanKey[INODE_TABLE_INDEX_NAME_EQ].sk_collation = DEFAULT_COLLATION_OID;
    InodeTableIndexParentIdPartIdNameScanKey[INODE_TABLE_INDEX_NAME_EQ].sk_attno = 2;
}

ScanKeyData XattrTableScanKey[LAST_FALCON_XATTR_TABLE_SCANKEY_TYPE];
static void InitializeXattrTableScanCache(void)
{
    memset(XattrTableScanKey, 0, sizeof(XattrTableScanKey));

    fmgr_info_cxt(F_INT8GT, &XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_GT].sk_func, ScanCacheMemoryContext);
    XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_GT].sk_strategy = BTGreaterStrategyNumber;
    XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_GT].sk_subtype = INT8OID;
    XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_GT].sk_collation = DEFAULT_COLLATION_OID;
    XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_GT].sk_attno = Anum_falcon_xattr_table_parentid_partid;

    fmgr_info_cxt(F_INT8GE, &XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_GE].sk_func, ScanCacheMemoryContext);
    XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_GE].sk_strategy = BTGreaterEqualStrategyNumber;
    XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_GE].sk_subtype = INT8OID;
    XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_GE].sk_collation = DEFAULT_COLLATION_OID;
    XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_GE].sk_attno = Anum_falcon_xattr_table_parentid_partid;

    fmgr_info_cxt(F_INT8LE, &XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_LE].sk_func, ScanCacheMemoryContext);
    XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_LE].sk_strategy = BTLessEqualStrategyNumber;
    XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_LE].sk_subtype = INT8OID;
    XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_LE].sk_collation = DEFAULT_COLLATION_OID;
    XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_LE].sk_attno = Anum_falcon_xattr_table_parentid_partid;

    fmgr_info_cxt(F_INT8EQ, &XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_EQ].sk_func, ScanCacheMemoryContext);
    XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_EQ].sk_strategy = BTEqualStrategyNumber;
    XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_EQ].sk_subtype = INT8OID;
    XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_EQ].sk_collation = DEFAULT_COLLATION_OID;
    XattrTableScanKey[XATTR_TABLE_PARENT_ID_PART_ID_EQ].sk_attno = Anum_falcon_xattr_table_parentid_partid;

    fmgr_info_cxt(F_TEXT_GT, &XattrTableScanKey[XATTR_TABLE_NAME_GT].sk_func, ScanCacheMemoryContext);
    XattrTableScanKey[XATTR_TABLE_NAME_GT].sk_strategy = BTGreaterStrategyNumber;
    XattrTableScanKey[XATTR_TABLE_NAME_GT].sk_subtype = TEXTOID;
    XattrTableScanKey[XATTR_TABLE_NAME_GT].sk_collation = DEFAULT_COLLATION_OID;
    XattrTableScanKey[XATTR_TABLE_NAME_GT].sk_attno = Anum_falcon_xattr_table_name;

    fmgr_info_cxt(F_TEXTEQ, &XattrTableScanKey[XATTR_TABLE_NAME_EQ].sk_func, ScanCacheMemoryContext);
    XattrTableScanKey[XATTR_TABLE_NAME_EQ].sk_strategy = BTEqualStrategyNumber;
    XattrTableScanKey[XATTR_TABLE_NAME_EQ].sk_subtype = TEXTOID;
    XattrTableScanKey[XATTR_TABLE_NAME_EQ].sk_collation = DEFAULT_COLLATION_OID;
    XattrTableScanKey[XATTR_TABLE_NAME_EQ].sk_attno = Anum_falcon_xattr_table_name;

    fmgr_info_cxt(F_TEXT_GT, &XattrTableScanKey[XATTR_TABLE_XKEY_GT].sk_func, ScanCacheMemoryContext);
    XattrTableScanKey[XATTR_TABLE_XKEY_GT].sk_strategy = BTGreaterStrategyNumber;
    XattrTableScanKey[XATTR_TABLE_XKEY_GT].sk_subtype = TEXTOID;
    XattrTableScanKey[XATTR_TABLE_XKEY_GT].sk_collation = DEFAULT_COLLATION_OID;
    XattrTableScanKey[XATTR_TABLE_XKEY_GT].sk_attno = Anum_falcon_xattr_table_xkey;

    fmgr_info_cxt(F_TEXTEQ, &XattrTableScanKey[XATTR_TABLE_XKEY_EQ].sk_func, ScanCacheMemoryContext);
    XattrTableScanKey[XATTR_TABLE_XKEY_EQ].sk_strategy = BTEqualStrategyNumber;
    XattrTableScanKey[XATTR_TABLE_XKEY_EQ].sk_subtype = TEXTOID;
    XattrTableScanKey[XATTR_TABLE_XKEY_EQ].sk_collation = DEFAULT_COLLATION_OID;
    XattrTableScanKey[XATTR_TABLE_XKEY_EQ].sk_attno = Anum_falcon_xattr_table_xkey;
}

ScanKeyData SliceTableScanKey[LAST_FALCON_SLICE_TABLE_SCANKEY_TYPE];
static void InitializeSliceTableScanCache(void)
{
    memset(SliceTableScanKey, 0, sizeof(SliceTableScanKey));

    fmgr_info_cxt(F_INT8EQ, &SliceTableScanKey[SLICE_TABLE_INODEID_EQ].sk_func, ScanCacheMemoryContext);
    SliceTableScanKey[SLICE_TABLE_INODEID_EQ].sk_strategy = BTEqualStrategyNumber;
    SliceTableScanKey[SLICE_TABLE_INODEID_EQ].sk_subtype = INT8OID;
    SliceTableScanKey[SLICE_TABLE_INODEID_EQ].sk_collation = DEFAULT_COLLATION_OID;
    SliceTableScanKey[SLICE_TABLE_INODEID_EQ].sk_attno = Anum_falcon_slice_table_inodeid;

    fmgr_info_cxt(F_INT4EQ, &SliceTableScanKey[SLICE_TABLE_CHUNKID_EQ].sk_func, ScanCacheMemoryContext);
    SliceTableScanKey[SLICE_TABLE_CHUNKID_EQ].sk_strategy = BTEqualStrategyNumber;
    SliceTableScanKey[SLICE_TABLE_CHUNKID_EQ].sk_subtype = INT4OID;
    SliceTableScanKey[SLICE_TABLE_CHUNKID_EQ].sk_collation = DEFAULT_COLLATION_OID;
    SliceTableScanKey[SLICE_TABLE_CHUNKID_EQ].sk_attno = Anum_falcon_slice_table_chunkid;
}

ScanKeyData KvmetaTableScanKey[LAST_FALCON_KVMETA_TABLE_SCANKEY_TYPE];
static void InitializeKvmetaTableScanCache(void)
{
    memset(KvmetaTableScanKey, 0, sizeof(KvmetaTableScanKey));

    fmgr_info_cxt(F_TEXTEQ, &KvmetaTableScanKey[KVMETA_TABLE_USERKEY_EQ].sk_func, ScanCacheMemoryContext);
    KvmetaTableScanKey[KVMETA_TABLE_USERKEY_EQ].sk_strategy = BTEqualStrategyNumber;
    KvmetaTableScanKey[KVMETA_TABLE_USERKEY_EQ].sk_subtype = TEXTOID;
    KvmetaTableScanKey[KVMETA_TABLE_USERKEY_EQ].sk_collation = DEFAULT_COLLATION_OID;
    KvmetaTableScanKey[KVMETA_TABLE_USERKEY_EQ].sk_attno = Anum_falcon_kvmeta_table_userkey;
}

/*
 * InitializeInvalidationCallbacks() registers invalidation handlers
 */
static void InitializeInvalidationCallbacks(void)
{
    static bool isInitializeInvalidation = false;
    if (!isInitializeInvalidation) {
        isInitializeInvalidation = true;
        CacheRegisterRelcacheCallback(InvalidateShardTableShmemCacheCallback, (Datum)0);
        CacheRegisterRelcacheCallback(InvalidateForeignServerShmemCacheCallback, (Datum)0);
    }
}

void SetUpScanCaches(void)
{
    static bool isScanCacheInitialization = false;
    if (!isScanCacheInitialization) {
        ScanCacheMemoryContext = NULL;
        PG_TRY();
        {
            isScanCacheInitialization = true;
            if (CacheMemoryContext == NULL) {
                CreateCacheMemoryContext();
            }
            ScanCacheMemoryContext =
                AllocSetContextCreate(CacheMemoryContext, "ScanCacheMemoryContext", ALLOCSET_DEFAULT_SIZES);
            InitializeForeignServerTableScanCache();
            InitializeDirectoryTableScanCache();
            InitializeInodeTableScanCache();
            InitializeInodeTableIndexParentIdPartIdNameScanCache();
            InitializeXattrTableScanCache();
            InitializeSliceTableScanCache();
            InitializeKvmetaTableScanCache();
        }
        PG_CATCH();
        {
            isScanCacheInitialization = false;
            if (ScanCacheMemoryContext != NULL) {
                MemoryContextDelete(ScanCacheMemoryContext);
            }
            ScanCacheMemoryContext = NULL;
            PG_RE_THROW();
        }
        PG_END_TRY();
    }
}

void freeStringInfo(StringInfo s)
{
    pfree(s->data);
    pfree(s);
}

bool ArrayTypeArrayToDatumArrayAndSize(ArrayType *arrayObject, Datum **datumArray, int *datumArrayLength)
{
    bool *datumArrayNulls = NULL;

    bool typeByVal = false;
    char typeAlign = 0;
    int16 typeLength = 0;

    bool arrayHasNull = ARR_HASNULL(arrayObject);
    if (arrayHasNull) {
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "array object cannot contain null values");
    }

    Oid typeId = ARR_ELEMTYPE(arrayObject);
    get_typlenbyvalalign(typeId, &typeLength, &typeByVal, &typeAlign);

    deconstruct_array(arrayObject,
                      typeId,
                      typeLength,
                      typeByVal,
                      typeAlign,
                      datumArray,
                      &datumArrayNulls,
                      datumArrayLength);

    return true;
}

void hash_clear(HTAB *htab)
{
    HASH_SEQ_STATUS status;
    void *entry = NULL;

    hash_seq_init(&status, htab);
    while ((entry = hash_seq_search(&status)) != 0) {
        bool found;
        hash_search(htab, entry, HASH_REMOVE, &found);
    }
}

Oid FalconExtensionOwner(void)
{
    static Oid falconOwner = InvalidOid;

    Relation extRel;
    ScanKeyData key[1];
    SysScanDesc extScan;
    HeapTuple extTup;
    Datum datum;
    bool isnull;
    Oid extOwner = InvalidOid;

    if (falconOwner != InvalidOid) {
        return falconOwner;
    }

    /*
     * Look up the extension --- it must already exist in pg_extension
     */
    extRel = table_open(ExtensionRelationId, AccessShareLock);

    ScanKeyInit(&key[0], Anum_pg_extension_extname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum("falcon"));

    extScan = systable_beginscan(extRel, ExtensionNameIndexId, true, NULL, 1, key);

    extTup = systable_getnext(extScan);

    if (!HeapTupleIsValid(extTup))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("extension falcon does not exist")));

    /*
     * get the falcon extension owner
     */
    datum = heap_getattr(extTup, Anum_pg_extension_extowner, RelationGetDescr(extRel), &isnull);
    if (!isnull)
        extOwner = DatumGetObjectId(datum);

    if (!superuser_arg(extOwner)) {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("falcon extension's owner should be superuser")));
    }

    falconOwner = extOwner;

    systable_endscan(extScan);

    table_close(extRel, AccessShareLock);

    return falconOwner;
}

bool CheckWhetherTargetExistInIndex(Relation heap, Relation index, ScanKeyData *scanKeys)
{
    int indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
    SnapshotData dirtySnapShot;
    InitDirtySnapshot(dirtySnapShot);
    bool targetExist = false;

    IndexScanDesc indexScan = index_beginscan(heap, index, &dirtySnapShot, indnkeyatts, 0);
    index_rescan(indexScan, scanKeys, indnkeyatts, NULL, 0);
    if (index_getnext_tid(indexScan, ForwardScanDirection) != NULL) {
        targetExist = true;
    }
    index_endscan(indexScan);
    return targetExist;
}
