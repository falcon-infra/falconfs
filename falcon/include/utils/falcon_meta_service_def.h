/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_META_SERVICE_DEF_H
#define FALCON_META_SERVICE_DEF_H
typedef enum FalconMetaServiceType {
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
} FalconMetaServiceType;
#endif // FALCON_META_SERVICE_DEF_H
