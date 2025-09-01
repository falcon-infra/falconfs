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

int pg_qsort_meta_process_info_by_path_cmp(const void *a, const void *b);

#endif
