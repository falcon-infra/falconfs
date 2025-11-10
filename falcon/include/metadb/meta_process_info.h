/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_METADB_META_PROCESS_INFO_H
#define FALCON_METADB_META_PROCESS_INFO_H

#include <stdbool.h>
#include <stdint.h>
#include "utils/error_code.h"

typedef struct OneReadDirResult
{
    const char *fileName;
    uint32_t mode;
} OneReadDirResult;
typedef struct MetaProcessInfoData
{
    // input
    const char *path;
    uint64_t parentId;
    const char *plainCommand;
    int32_t readDirMaxReadCount;

    // inode info (input/output)
    uint64_t inodeId;
    uint64_t parentId_partId;
    char *name;
    uint64_t st_dev;
    uint32_t st_mode;
    uint64_t st_nlink;
    uint32_t st_uid;
    uint32_t st_gid;
    uint64_t st_rdev;
    int64_t st_size;
    int64_t st_blksize;
    int64_t st_blocks;
    int64_t st_atim;
    int64_t st_mtim;
    int64_t st_ctim;
    char *etag;
    int32_t node_id;

    // input(or output) for readdir
    int32_t readDirLastShardIndex;
    const char *readDirLastFileName;
    OneReadDirResult **readDirResultList;
    int readDirResultCount;

    // input(or output) for rename
    const char *dstPath;
    uint64_t dstParentId;
    uint64_t dstParentIdPartId;
    char *dstName;
    bool targetIsDirectory;
    int32_t srcLockOrder;

    // output
    FalconErrorCode errorCode;
    char *errorMsg;
} MetaProcessInfoData;
typedef MetaProcessInfoData *MetaProcessInfo;

typedef struct SliceInfo
{
    uint64_t inodeId;
    uint32_t chunkId;
    uint64_t sliceId;
    uint32_t sliceSize;
    uint32_t sliceOffset;
    uint32_t sliceLen;
    uint32_t sliceLoc1;
    uint32_t sliceLoc2;
} SliceInfo;

typedef struct SliceProcessInfoData
{
    uint64_t *inodeIds;
    uint32_t *chunkIds;
    uint64_t *sliceIds;
    uint32_t *sliceSizes;
    uint32_t *sliceOffsets;
    uint32_t *sliceLens;
    uint32_t *sliceLoc1s;
    uint32_t *sliceloc2s;
    const char *name;
    uint64_t inputInodeid;
    uint32_t inputChunkid;
    uint32_t count;
    FalconErrorCode errorCode;
} SliceProcessInfoData;

typedef SliceProcessInfoData *SliceProcessInfo;

typedef struct KvMetaProcessInfoData
{
    const char *userkey;
    uint32_t valuelen;
    uint16_t slicenum;
    uint64_t *valuekey;
    uint64_t *location;
    uint32_t *slicelen;
    FalconErrorCode errorCode;
} KvMetaProcessInfoData;

typedef KvMetaProcessInfoData *KvMetaProcessInfo;

typedef struct SliceIdProcessInfoData
{
    uint32_t count;
    uint64_t start;
    uint64_t end;
    FalconErrorCode errorCode;
} SliceIdProcessInfoData;

typedef SliceIdProcessInfoData *SliceIdProcessInfo;

int pg_qsort_meta_process_info_by_path_cmp(const void *a, const void *b);

#endif
