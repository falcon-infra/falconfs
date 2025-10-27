/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "postgres.h"

#include "fmgr.h"
#include "executor/spi.h"
#include "utils/palloc.h"

#include <unistd.h>
#include <string.h>

#include "connection_pool/connection_pool.h"
#include "metadb/meta_handle.h"
#include "metadb/meta_process_info.h"
#include "utils/error_log.h"

PG_FUNCTION_INFO_V1(falcon_batch_meta_call_by_shmem);

/* 二进制格式头部 */
#define FALCON_BINARY_SIGNATURE 0x46414C43U  /* ASCII "FALC" */

typedef struct BinaryHeader {
    uint32_t signature;       /* 签名: FALCON_BINARY_SIGNATURE */
    uint32_t count;           /* 操作数量 */
    uint32_t operation_type;  /* 操作类型 */
    uint32_t reserved;        /* 保留字段 */
} BinaryHeader;

/* 读取字符串的辅助函数 */
static inline char* ReadString(char** p_ptr)
{
    char* p = *p_ptr;
    uint16_t len = *(uint16_t*)p;
    p += sizeof(uint16_t);

    char* str = palloc(len + 1);
    memcpy(str, p, len);
    str[len] = '\0';
    p += len;

    *p_ptr = p;
    return str;
}

/*
 * falcon_batch_meta_call_by_shmem
 *
 * 新的批处理函数，使用二进制格式（不依赖 FlatBuffers）
 *
 * 参数:
 *   - operation_type: 操作类型
 *   - paramShmemShift: 共享内存偏移量
 *   - signature: 签名
 *
 * 返回: 响应在共享内存中的偏移量
 */
Datum falcon_batch_meta_call_by_shmem(PG_FUNCTION_ARGS)
{
    int32_t operation_type = PG_GETARG_INT32(0);
    uint64_t paramShmemShift = (uint64_t)PG_GETARG_INT64(1);
    int64_t signature = PG_GETARG_INT64(2);

    /* 1. 从共享内存获取数据 */
    FalconShmemAllocator *allocator = &FalconConnectionPoolShmemAllocator;
    if (paramShmemShift > allocator->pageCount * FALCON_SHMEM_ALLOCATOR_PAGE_SIZE)
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "paramShmemShift is invalid.");

    char *paramBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(allocator, paramShmemShift);

    /* 2. 解析头部 */
    BinaryHeader *header = (BinaryHeader *)paramBuffer;
    if (header->signature != FALCON_BINARY_SIGNATURE) {
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "Invalid signature");
    }

    uint32_t count = header->count;
    if (count == 0 || count > 1000) {  /* 安全检查 */
        FALCON_ELOG_ERROR(ARGUMENT_ERROR, "Invalid count");
    }

    char *p = paramBuffer + sizeof(BinaryHeader);

    /* 3. 构造 MetaProcessInfo 数组 */
    void *data = palloc((sizeof(MetaProcessInfoData) + sizeof(MetaProcessInfo)) * count);
    MetaProcessInfoData *infoDataArray = data;
    MetaProcessInfo *infoArray = (MetaProcessInfo *)(infoDataArray + count);

    for (uint32_t i = 0; i < count; i++) {
        infoArray[i] = &infoDataArray[i];
        memset(&infoDataArray[i], 0, sizeof(MetaProcessInfoData));
        infoDataArray[i].errorCode = SUCCESS;

        /* 根据操作类型解析参数 */
        switch (operation_type) {
            case 1:  /* DFC_PUT_KEY_META */
                infoDataArray[i].path = ReadString(&p);  /* 使用 path 字段存储 key */
                infoDataArray[i].st_size = *(uint32_t*)p;  /* valueLen */
                p += 4;
                infoDataArray[i].node_id = *(uint16_t*)p;  /* sliceNum */
                p += 2;
                /* 跳过 slice 数据，PostgreSQL 函数会直接从共享内存读取 */
                for (int j = 0; j < infoDataArray[i].node_id; j++) {
                    p += 8 + 8 + 4;  /* value_key + location + size */
                }
                break;

            case 2:  /* DFC_GET_KV_META */
            case 3:  /* DFC_DELETE_KEY_META */
                infoDataArray[i].path = ReadString(&p);  /* key */
                break;

            case 5:  /* DFC_MKDIR */
            case 6:  /* DFC_CREATE */
            case 7:  /* DFC_STAT */
            case 8:  /* DFC_OPEN */
            case 10: /* DFC_UNLINK */
            case 12: /* DFC_OPENDIR */
            case 13: /* DFC_RMDIR */
                /* 简单路径操作 */
                infoDataArray[i].path = ReadString(&p);
                break;

            case 11: /* DFC_READDIR */
                infoDataArray[i].path = ReadString(&p);
                infoDataArray[i].readDirMaxReadCount = *(int32_t*)p;
                p += 4;
                infoDataArray[i].readDirLastShardIndex = *(int32_t*)p;
                p += 4;
                infoDataArray[i].readDirLastFileName = ReadString(&p);
                break;

            case 15: /* DFC_MKDIR_SUB_MKDIR */
                infoDataArray[i].parentId = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].name = ReadString(&p);  /* name */
                infoDataArray[i].inodeId = *(uint64_t*)p;  /* inode_id */
                p += 8;
                break;

            case 16: /* DFC_MKDIR_SUB_CREATE */
                infoDataArray[i].parentId_partId = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].name = ReadString(&p);  /* name */
                infoDataArray[i].inodeId = *(uint64_t*)p;  /* inode_id */
                p += 8;
                infoDataArray[i].st_mode = *(uint32_t*)p;
                p += 4;
                infoDataArray[i].st_mtim = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].st_size = *(int64_t*)p;
                p += 8;
                break;

            case 17: /* DFC_RMDIR_SUB_RMDIR */
                infoDataArray[i].parentId = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].name = ReadString(&p);  /* name */
                break;

            case 18: /* DFC_RMDIR_SUB_UNLINK */
                infoDataArray[i].parentId_partId = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].name = ReadString(&p);  /* name */
                break;

            case 19: /* DFC_RENAME_SUB_RENAME_LOCALLY */
                infoDataArray[i].parentId = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].parentId_partId = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].name = ReadString(&p);  /* src_name */
                infoDataArray[i].dstParentId = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].dstParentIdPartId = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].dstName = ReadString(&p);  /* dst_name */
                infoDataArray[i].targetIsDirectory = *(uint8_t*)p;
                p += 1;
                infoDataArray[i].inodeId = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].srcLockOrder = *(int32_t*)p;
                p += 4;
                break;

            case 20: /* DFC_RENAME_SUB_CREATE */
                infoDataArray[i].parentId_partId = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].name = ReadString(&p);  /* name */
                infoDataArray[i].inodeId = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].st_dev = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].st_mode = *(uint32_t*)p;
                p += 4;
                infoDataArray[i].st_nlink = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].st_uid = *(uint32_t*)p;
                p += 4;
                infoDataArray[i].st_gid = *(uint32_t*)p;
                p += 4;
                infoDataArray[i].st_rdev = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].st_size = *(int64_t*)p;
                p += 8;
                infoDataArray[i].st_blksize = *(int64_t*)p;
                p += 8;
                infoDataArray[i].st_blocks = *(int64_t*)p;
                p += 8;
                infoDataArray[i].st_atim = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].st_mtim = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].st_ctim = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].node_id = *(int32_t*)p;
                p += 4;
                break;

            case 9:  /* DFC_CLOSE */
                infoDataArray[i].path = ReadString(&p);
                infoDataArray[i].st_size = *(int64_t*)p;
                p += 8;
                infoDataArray[i].st_mtim = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].node_id = *(int32_t*)p;
                p += 4;
                break;

            case 14: /* DFC_RENAME */
                infoDataArray[i].path = ReadString(&p);
                infoDataArray[i].dstPath = ReadString(&p);
                break;

            case 22: /* DFC_CHOWN */
                infoDataArray[i].path = ReadString(&p);
                infoDataArray[i].st_uid = *(uint32_t*)p;
                p += 4;
                infoDataArray[i].st_gid = *(uint32_t*)p;
                p += 4;
                break;

            case 23: /* DFC_CHMOD */
                infoDataArray[i].path = ReadString(&p);
                infoDataArray[i].st_mode = *(uint64_t*)p;
                p += 8;
                break;

            case 21: /* DFC_UTIMENS */
                infoDataArray[i].path = ReadString(&p);
                infoDataArray[i].st_atim = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].st_mtim = *(uint64_t*)p;
                p += 8;
                break;

            default:
                FALCON_ELOG_ERROR(ARGUMENT_ERROR, "Unsupported operation type");
        }
    }

    /* 4. 调用核心批处理函数 */
    SPITupleTable *kvResultTable = NULL;  /* 用于 GET_KV 的 SPI 结果 */

    switch (operation_type) {
        case 1:  /* DFC_PUT_KEY_META */
            FalconPutKvMetaHandle(infoArray, count, paramBuffer + sizeof(BinaryHeader));
            break;
        case 2:  /* DFC_GET_KV_META */
            kvResultTable = FalconGetKvMetaHandle(infoArray, count);
            break;
        case 3:  /* DFC_DELETE_KEY_META */
            FalconDeleteKvMetaHandle(infoArray, count);
            break;
        case 5:  /* DFC_MKDIR */
            FalconMkdirHandle(infoArray, count);
            break;
        case 6:  /* DFC_CREATE */
            FalconCreateHandle(infoArray, count, false);
            break;
        case 7:  /* DFC_STAT */
            FalconStatHandle(infoArray, count);
            break;
        case 8:  /* DFC_OPEN */
            FalconOpenHandle(infoArray, count);
            break;
        case 9:  /* DFC_CLOSE */
            FalconCloseHandle(infoArray, count);
            break;
        case 10: /* DFC_UNLINK */
            FalconUnlinkHandle(infoArray, count);
            break;
        case 11: /* DFC_READDIR */
            FalconReadDirHandle(infoArray[0]);  /* READDIR 不支持批处理 */
            break;
        case 12: /* DFC_OPENDIR */
            FalconOpenDirHandle(infoArray[0]);  /* OPENDIR 不支持批处理 */
            break;
        case 13: /* DFC_RMDIR */
            FalconRmdirHandle(infoArray[0]);  /* RMDIR 不支持批处理 */
            break;
        case 14: /* DFC_RENAME */
            FalconRenameHandle(infoArray[0]);  /* RENAME 不支持批处理 */
            break;
        case 15: /* DFC_MKDIR_SUB_MKDIR */
            FalconMkdirSubMkdirHandle(infoArray, count);
            break;
        case 16: /* DFC_MKDIR_SUB_CREATE */
            FalconMkdirSubCreateHandle(infoArray, count);
            break;
        case 17: /* DFC_RMDIR_SUB_RMDIR */
            FalconRmdirSubRmdirHandle(infoArray[0]);  /* 不支持批处理 */
            break;
        case 18: /* DFC_RMDIR_SUB_UNLINK */
            FalconRmdirSubUnlinkHandle(infoArray[0]);  /* 不支持批处理 */
            break;
        case 19: /* DFC_RENAME_SUB_RENAME_LOCALLY */
            FalconRenameSubRenameLocallyHandle(infoArray[0]);  /* 不支持批处理 */
            break;
        case 20: /* DFC_RENAME_SUB_CREATE */
            FalconRenameSubCreateHandle(infoArray[0]);  /* 不支持批处理 */
            break;
        case 21: /* DFC_UTIMENS */
            FalconUtimeNsHandle(infoArray[0]);  /* UTIMENS 不支持批处理 */
            break;
        case 22: /* DFC_CHOWN */
            FalconChownHandle(infoArray[0]);  /* CHOWN 不支持批处理 */
            break;
        case 23: /* DFC_CHMOD */
            FalconChmodHandle(infoArray[0]);  /* CHMOD 不支持批处理 */
            break;
        default:
            FALCON_ELOG_ERROR(ARGUMENT_ERROR, "Unsupported operation type");
    }

    /* 5. 构造响应并写入共享内存 */
    size_t response_size = 0;

    /* 计算响应大小 */
    switch (operation_type) {
        case 1:  /* DFC_PUT_KEY_META */
        case 3:  /* DFC_DELETE_KEY_META */
            /* 简单操作：每个返回一个 int32 错误码 */
            response_size = count * sizeof(int32_t);
            break;

        case 2:  /* DFC_GET_KV_META */
            /* GET: int32(status) + key_len(2) + key + valueLen(4) + sliceNum(2) + slices */
            for (uint32_t i = 0; i < count; i++) {
                response_size += sizeof(int32_t);
                if (infoDataArray[i].errorCode == SUCCESS) {
                    response_size += sizeof(uint16_t) + strlen(infoDataArray[i].path);
                    response_size += sizeof(uint32_t) + sizeof(uint16_t);
                    response_size += infoDataArray[i].node_id * (8 + 8 + 4);  /* slices: value_key + location + size */
                }
            }
            break;

        case 5:  /* DFC_MKDIR */
        case 6:  /* DFC_CREATE */
        case 9:  /* DFC_CLOSE */
        case 10: /* DFC_UNLINK */
        case 11: /* DFC_READDIR */
        case 12: /* DFC_OPENDIR */
        case 13: /* DFC_RMDIR */
        case 14: /* DFC_RENAME */
        case 15: /* DFC_MKDIR_SUB_MKDIR */
        case 16: /* DFC_MKDIR_SUB_CREATE */
        case 17: /* DFC_RMDIR_SUB_RMDIR */
        case 18: /* DFC_RMDIR_SUB_UNLINK */
        case 19: /* DFC_RENAME_SUB_RENAME_LOCALLY */
        case 20: /* DFC_RENAME_SUB_CREATE */
        case 21: /* DFC_UTIMENS */
        case 22: /* DFC_CHOWN */
        case 23: /* DFC_CHMOD */
            /* 简单操作：每个返回一个 int32 错误码 */
            response_size = count * sizeof(int32_t);
            break;

        case 7:  /* DFC_STAT */
            /* STAT: 每个返回 int32(status) + StatResponse(13个字段) */
            response_size = count * (sizeof(int32_t) + 13 * 8);
            break;

        case 8:  /* DFC_OPEN */
            /* OPEN: 每个返回 int32(status) + OpenResponse(14个字段) */
            /* st_ino(8) + node_id(8) + st_dev(8) + st_mode(4) + st_nlink(8) + st_uid(4) + st_gid(4) + st_rdev(8) + st_size(8) + st_blksize(8) + st_blocks(8) + st_atim(8) + st_mtim(8) + st_ctim(8) = 104 bytes */
            response_size = count * (sizeof(int32_t) + 104);
            break;

        default:
            FALCON_ELOG_ERROR(ARGUMENT_ERROR, "Unsupported operation type");
    }

    /* 分配共享内存 */
    uint64_t responseShmemShift = FalconShmemAllocatorMalloc(allocator, response_size);
    if (responseShmemShift == 0)
        FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR, "FalconShmemAllocMalloc failed. Size: %lu.", response_size);

    char *responseBuffer = FALCON_SHMEM_ALLOCATOR_GET_POINTER(allocator, responseShmemShift);
    FALCON_SHMEM_ALLOCATOR_SET_SIGNATURE(responseBuffer, signature);

    /* 写入响应数据 */
    char *resp_p = responseBuffer;

    for (uint32_t i = 0; i < count; i++) {
        switch (operation_type) {
            case 1:  /* DFC_PUT_KEY_META */
            case 3:  /* DFC_DELETE_KEY_META */
                *(int32_t*)resp_p = infoDataArray[i].errorCode;
                resp_p += sizeof(int32_t);
                break;

            case 2:  /* DFC_GET_KV_META */
                *(int32_t*)resp_p = infoDataArray[i].errorCode;
                resp_p += sizeof(int32_t);

                if (infoDataArray[i].errorCode == SUCCESS) {
                    /* 写入 key */
                    uint16_t key_len = strlen(infoDataArray[i].path);
                    *(uint16_t*)resp_p = key_len;
                    resp_p += 2;
                    memcpy(resp_p, infoDataArray[i].path, key_len);
                    resp_p += key_len;

                    /* 写入 valueLen 和 sliceNum（从 infoDataArray 获取） */
                    *(uint32_t*)resp_p = infoDataArray[i].st_size;
                    resp_p += 4;
                    *(uint16_t*)resp_p = infoDataArray[i].node_id;
                    resp_p += 2;

                    /* 写入 slices 数组（从 kvResultTable 读取） */
                    if (kvResultTable != NULL) {
                        /* 遍历 SPI 结果表，找到当前 key 的所有 slices */
                        for (uint64 row = 0; row < SPI_processed; row++) {
                            char *key = SPI_getvalue(kvResultTable->vals[row], kvResultTable->tupdesc, 1);

                            if (strcmp(key, infoDataArray[i].path) == 0) {
                                /* 读取 slice 数据 */
                                char *value_key_str = SPI_getvalue(kvResultTable->vals[row], kvResultTable->tupdesc, 3);
                                char *location_str = SPI_getvalue(kvResultTable->vals[row], kvResultTable->tupdesc, 4);
                                char *size_str = SPI_getvalue(kvResultTable->vals[row], kvResultTable->tupdesc, 5);

                                uint64_t value_key = (uint64_t)atoll(value_key_str);
                                uint64_t location = (uint64_t)atoll(location_str);
                                uint32_t size = (uint32_t)atoi(size_str);

                                /* 写入二进制格式 */
                                *(uint64_t*)resp_p = value_key;
                                resp_p += 8;
                                *(uint64_t*)resp_p = location;
                                resp_p += 8;
                                *(uint32_t*)resp_p = size;
                                resp_p += 4;
                            }
                        }
                    }
                }
                break;

            case 5:  /* DFC_MKDIR */
            case 6:  /* DFC_CREATE */
            case 9:  /* DFC_CLOSE */
            case 10: /* DFC_UNLINK */
            case 11: /* DFC_READDIR */
            case 12: /* DFC_OPENDIR */
            case 13: /* DFC_RMDIR */
            case 14: /* DFC_RENAME */
            case 15: /* DFC_MKDIR_SUB_MKDIR */
            case 16: /* DFC_MKDIR_SUB_CREATE */
            case 17: /* DFC_RMDIR_SUB_RMDIR */
            case 18: /* DFC_RMDIR_SUB_UNLINK */
            case 19: /* DFC_RENAME_SUB_RENAME_LOCALLY */
            case 20: /* DFC_RENAME_SUB_CREATE */
            case 21: /* DFC_UTIMENS */
            case 22: /* DFC_CHOWN */
            case 23: /* DFC_CHMOD */
                *(int32_t*)resp_p = infoDataArray[i].errorCode;
                resp_p += sizeof(int32_t);
                break;

            case 7:  /* DFC_STAT */
                *(int32_t*)resp_p = infoDataArray[i].errorCode;
                resp_p += sizeof(int32_t);

                if (infoDataArray[i].errorCode == SUCCESS) {
                    *(uint64_t*)resp_p = infoDataArray[i].inodeId;      resp_p += 8;
                    *(uint64_t*)resp_p = infoDataArray[i].st_dev;       resp_p += 8;
                    *(uint32_t*)resp_p = infoDataArray[i].st_mode;      resp_p += 4;
                    *(uint64_t*)resp_p = infoDataArray[i].st_nlink;     resp_p += 8;
                    *(uint32_t*)resp_p = infoDataArray[i].st_uid;       resp_p += 4;
                    *(uint32_t*)resp_p = infoDataArray[i].st_gid;       resp_p += 4;
                    *(uint64_t*)resp_p = infoDataArray[i].st_rdev;      resp_p += 8;
                    *(int64_t*)resp_p  = infoDataArray[i].st_size;      resp_p += 8;
                    *(int64_t*)resp_p  = infoDataArray[i].st_blksize;   resp_p += 8;
                    *(int64_t*)resp_p  = infoDataArray[i].st_blocks;    resp_p += 8;
                    *(uint64_t*)resp_p = infoDataArray[i].st_atim;      resp_p += 8;
                    *(uint64_t*)resp_p = infoDataArray[i].st_mtim;      resp_p += 8;
                    *(uint64_t*)resp_p = infoDataArray[i].st_ctim;      resp_p += 8;
                }
                break;

            case 8:  /* DFC_OPEN */
                *(int32_t*)resp_p = infoDataArray[i].errorCode;
                resp_p += sizeof(int32_t);

                if (infoDataArray[i].errorCode == SUCCESS) {
                    *(uint64_t*)resp_p = infoDataArray[i].inodeId;      resp_p += 8;
                    *(int64_t*)resp_p  = infoDataArray[i].node_id;      resp_p += 8;
                    *(uint64_t*)resp_p = infoDataArray[i].st_dev;       resp_p += 8;
                    *(uint32_t*)resp_p = infoDataArray[i].st_mode;      resp_p += 4;
                    *(uint64_t*)resp_p = infoDataArray[i].st_nlink;     resp_p += 8;
                    *(uint32_t*)resp_p = infoDataArray[i].st_uid;       resp_p += 4;
                    *(uint32_t*)resp_p = infoDataArray[i].st_gid;       resp_p += 4;
                    *(uint64_t*)resp_p = infoDataArray[i].st_rdev;      resp_p += 8;
                    *(int64_t*)resp_p  = infoDataArray[i].st_size;      resp_p += 8;
                    *(int64_t*)resp_p  = infoDataArray[i].st_blksize;   resp_p += 8;
                    *(int64_t*)resp_p  = infoDataArray[i].st_blocks;    resp_p += 8;
                    *(uint64_t*)resp_p = infoDataArray[i].st_atim;      resp_p += 8;
                    *(uint64_t*)resp_p = infoDataArray[i].st_mtim;      resp_p += 8;
                    *(uint64_t*)resp_p = infoDataArray[i].st_ctim;      resp_p += 8;
                }
                break;
        }
    }

    /* 6. 清理 SPI 连接（如果 GET_KV 操作打开了 SPI） */
    if (operation_type == 2 && kvResultTable != NULL) {
        SPI_finish();
    }

    /* 7. 返回响应在共享内存中的偏移量 */
    PG_RETURN_INT64(responseShmemShift);
}
