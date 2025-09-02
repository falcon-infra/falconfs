/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_METADB_META_HANDLE_H
#define FALCON_METADB_META_HANDLE_H

#include <stdbool.h>
#include "metadb/meta_process_info.h"
#include "remote_connection_utils/serialized_data.h"

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
    NOT_SUPPORTED
} FalconSupportMetaService;

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

#endif
