/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_METADB_META_HANDLE_H
#define FALCON_METADB_META_HANDLE_H

#include <stdbool.h>
#include "metadb/meta_process_info.h"
#include "remote_connection_utils/serialized_data.h"

/* Forward declaration for PostgreSQL SPI types */
typedef struct SPITupleTable SPITupleTable;

#define DEFAULT_SUBPART_NUM 100

extern MemoryManager PgMemoryManager;

typedef enum FalconSupportMetaService {
    PLAIN_COMMAND,
    MKDIR,
    MKDIR_SUB_MKDIR,
    MKDIR_SUB_CREATE,
    CREATE,
    STAT,
    OPEN,
    CLOSE,
    UNLINK,
    READDIR,
    OPENDIR,
    RMDIR,
    RMDIR_SUB_RMDIR,
    RMDIR_SUB_UNLINK,
    RENAME,
    RENAME_SUB_RENAME_LOCALLY,
    RENAME_SUB_CREATE,
    UTIMENS,
    CHOWN,
    CHMOD,
    KV_PUT,
    KV_GET,
    KV_DEL,
    SLICE_PUT,
    SLICE_GET,
    SLICE_DEL,
    FETCH_SLICE_ID,
    NOT_SUPPORTED
} FalconSupportMetaService;

// 获取 FalconSupportMetaService 的字符串名称
static inline const char* FalconSupportMetaServiceName(FalconSupportMetaService svc) {
    switch (svc) {
        case PLAIN_COMMAND: return "PLAIN_COMMAND";
        case MKDIR: return "MKDIR";
        case MKDIR_SUB_MKDIR: return "MKDIR_SUB_MKDIR";
        case MKDIR_SUB_CREATE: return "MKDIR_SUB_CREATE";
        case CREATE: return "CREATE";
        case STAT: return "STAT";
        case OPEN: return "OPEN";
        case CLOSE: return "CLOSE";
        case UNLINK: return "UNLINK";
        case READDIR: return "READDIR";
        case OPENDIR: return "OPENDIR";
        case RMDIR: return "RMDIR";
        case RMDIR_SUB_RMDIR: return "RMDIR_SUB_RMDIR";
        case RMDIR_SUB_UNLINK: return "RMDIR_SUB_UNLINK";
        case RENAME: return "RENAME";
        case RENAME_SUB_RENAME_LOCALLY: return "RENAME_SUB_RENAME_LOCALLY";
        case RENAME_SUB_CREATE: return "RENAME_SUB_CREATE";
        case UTIMENS: return "UTIMENS";
        case CHOWN: return "CHOWN";
        case CHMOD: return "CHMOD";
        case KV_PUT: return "KV_PUT";
        case KV_GET: return "KV_GET";
        case KV_DEL: return "KV_DEL";
        case SLICE_PUT: return "SLICE_PUT";
        case SLICE_GET: return "SLICE_GET";
        case SLICE_DEL: return "SLICE_DEL";
        case FETCH_SLICE_ID: return "FETCH_SLICE_ID";
        case NOT_SUPPORTED: return "NOT_SUPPORTED";
        default: return "UNKNOWN";
    }
}

// func whose name ends with internal is not supposed to be called by external user
void FalconMkdirHandle(MetaProcessInfo *infoArray, int count);
void FalconMkdirSubMkdirHandle(MetaProcessInfo *infoArray, int count);
void FalconMkdirSubCreateHandle(MetaProcessInfo *infoArray, int count);
void FalconCreateHandle(MetaProcessInfo *infoArray, int count, bool updateExisted);
void FalconStatHandle(MetaProcessInfo *infoArray, int count);
void FalconOpenHandle(MetaProcessInfo *infoArray, int count);
void FalconCloseHandle(MetaProcessInfo *infoArray, int count);
void FalconUnlinkHandle(MetaProcessInfo *infoArray, int count);
void FalconReadDirHandle(MetaProcessInfo info);
void FalconOpenDirHandle(MetaProcessInfo info);
void FalconRmdirHandle(MetaProcessInfo info);
void FalconRmdirSubRmdirHandle(MetaProcessInfo info);
void FalconRmdirSubUnlinkHandle(MetaProcessInfo info);
void FalconRenameHandle(MetaProcessInfo info);
void FalconRenameSubRenameLocallyHandle(MetaProcessInfo info);
void FalconRenameSubCreateHandle(MetaProcessInfo info);
void FalconUtimeNsHandle(MetaProcessInfo info);
void FalconChownHandle(MetaProcessInfo info);
void FalconChmodHandle(MetaProcessInfo info);

void FalconSlicePutHandle(SliceProcessInfo *infoArray, int count);
void FalconSliceGetHandle(SliceProcessInfo *infoArray, int count);
void FalconSliceDelHandle(SliceProcessInfo *infoArray, int count);

void FalconKvmetaPutHandle(KvMetaProcessInfo info);
void FalconKvmetaGetHandle(KvMetaProcessInfo info);
void FalconKvmetaDelHandle(KvMetaProcessInfo info);

void FalconFetchSliceIdHandle(SliceIdProcessInfo infoData);

// KV 元数据批处理操作
void FalconPutKvMetaHandle(MetaProcessInfo *infoArray, int count, char *paramsData);
SPITupleTable* FalconGetKvMetaHandle(MetaProcessInfo *infoArray, int count);  /* 返回 SPI 结果表 */
void FalconDeleteKvMetaHandle(MetaProcessInfo *infoArray, int count);

#endif
