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

    printf("[debug] falcon_batch_meta_call_by_shmem: ENTRY, operation_type=%d\n", operation_type);
    fflush(stdout);

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
            case 20:  /* KV_PUT (protobuf enum) */
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

            case 21:  /* KV_GET (protobuf enum) */
            case 22:  /* KV_DEL (protobuf enum) */
                infoDataArray[i].path = ReadString(&p);  /* key */
                break;

            case 1:  /* MKDIR (protobuf enum) */
            case 4:  /* CREATE (protobuf enum) */
            case 5:  /* STAT (protobuf enum) */
            case 6:  /* OPEN (protobuf enum) */
            case 8:  /* UNLINK (protobuf enum) */
            case 10: /* OPENDIR (protobuf enum) */
            case 11: /* RMDIR (protobuf enum) */
                /* 简单路径操作 */
                infoDataArray[i].path = ReadString(&p);
                break;

            case 9:  /* READDIR (protobuf enum) */
                infoDataArray[i].path = ReadString(&p);
                infoDataArray[i].readDirMaxReadCount = *(int32_t*)p;
                p += 4;
                infoDataArray[i].readDirLastShardIndex = *(int32_t*)p;
                p += 4;
                infoDataArray[i].readDirLastFileName = ReadString(&p);
                break;

            case 2: /* MKDIR_SUB_MKDIR (protobuf enum) */
                infoDataArray[i].parentId = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].name = ReadString(&p);  /* name */
                infoDataArray[i].inodeId = *(uint64_t*)p;  /* inode_id */
                p += 8;
                break;

            case 3: /* MKDIR_SUB_CREATE (protobuf enum) */
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

            case 12: /* RMDIR_SUB_RMDIR (protobuf enum) */
                infoDataArray[i].parentId = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].name = ReadString(&p);  /* name */
                break;

            case 13: /* RMDIR_SUB_UNLINK (protobuf enum) */
                infoDataArray[i].parentId_partId = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].name = ReadString(&p);  /* name */
                break;

            case 15: /* RENAME_SUB_RENAME_LOCALLY (protobuf enum) */
                infoDataArray[i].parentId = *(uint64_t*)p;  /* src_parent_id */
                p += 8;
                infoDataArray[i].parentId_partId = *(uint64_t*)p;  /* src_parent_id_part_id */
                p += 8;
                infoDataArray[i].name = ReadString(&p);  /* src_name */
                infoDataArray[i].dstParentId = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].dstParentIdPartId = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].dstName = ReadString(&p);  /* dst_name */
                infoDataArray[i].targetIsDirectory = *(uint8_t*)p;
                p += 1;
                infoDataArray[i].inodeId = *(uint64_t*)p;  /* directory_inode_id */
                p += 8;
                infoDataArray[i].srcLockOrder = *(int32_t*)p;
                p += 4;
                break;

            case 16: /* RENAME_SUB_CREATE (protobuf enum) */
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

            case 7:  /* CLOSE (protobuf enum) */
                infoDataArray[i].path = ReadString(&p);
                infoDataArray[i].st_size = *(int64_t*)p;
                p += 8;
                infoDataArray[i].st_mtim = *(uint64_t*)p;
                p += 8;
                infoDataArray[i].node_id = *(int32_t*)p;
                p += 4;
                break;

            case 14: /* RENAME (protobuf enum) */
                infoDataArray[i].path = ReadString(&p);
                infoDataArray[i].dstPath = ReadString(&p);
                break;

            case 18: /* CHOWN (protobuf enum) */
                infoDataArray[i].path = ReadString(&p);
                infoDataArray[i].st_uid = *(uint32_t*)p;
                p += 4;
                infoDataArray[i].st_gid = *(uint32_t*)p;
                p += 4;
                break;

            case 19: /* CHMOD (protobuf enum) */
                infoDataArray[i].path = ReadString(&p);
                infoDataArray[i].st_mode = *(uint64_t*)p;
                p += 8;
                break;

            case 17: /* UTIMENS (protobuf enum) */
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
        case 20:  /* KV_PUT (protobuf enum) */
            FalconPutKvMetaHandle(infoArray, count, paramBuffer + sizeof(BinaryHeader));
            break;
        case 21:  /* KV_GET (protobuf enum) */
            kvResultTable = FalconGetKvMetaHandle(infoArray, count);
            break;
        case 22:  /* KV_DEL (protobuf enum) */
            FalconDeleteKvMetaHandle(infoArray, count);
            break;
        case 1:  /* MKDIR (protobuf enum) */
            FalconMkdirHandle(infoArray, count);
            break;
        case 4:  /* CREATE (protobuf enum) */
            FalconCreateHandle(infoArray, count, false);
            break;
        case 5:  /* STAT (protobuf enum) */
            FalconStatHandle(infoArray, count);
            break;
        case 6:  /* OPEN (protobuf enum) */
            FalconOpenHandle(infoArray, count);
            break;
        case 7:  /* CLOSE (protobuf enum) */
            FalconCloseHandle(infoArray, count);
            break;
        case 8:  /* UNLINK (protobuf enum) */
            FalconUnlinkHandle(infoArray, count);
            break;
        case 9:  /* READDIR (protobuf enum) */
            FalconReadDirHandle(infoArray[0]);  /* READDIR 不支持批处理 */
            break;
        case 10: /* OPENDIR (protobuf enum) */
            FalconOpenDirHandle(infoArray[0]);  /* OPENDIR 不支持批处理 */
            break;
        case 11: /* RMDIR (protobuf enum) */
            FalconRmdirHandle(infoArray[0]);  /* RMDIR 不支持批处理 */
            break;
        case 14: /* RENAME (protobuf enum) */
            FalconRenameHandle(infoArray[0]);  /* RENAME 不支持批处理 */
            break;
        case 2:  /* MKDIR_SUB_MKDIR (protobuf enum) */
            FalconMkdirSubMkdirHandle(infoArray, count);
            break;
        case 3:  /* MKDIR_SUB_CREATE (protobuf enum) */
            FalconMkdirSubCreateHandle(infoArray, count);
            break;
        case 12: /* RMDIR_SUB_RMDIR (protobuf enum) */
            FalconRmdirSubRmdirHandle(infoArray[0]);  /* 不支持批处理 */
            break;
        case 13: /* RMDIR_SUB_UNLINK (protobuf enum) */
            FalconRmdirSubUnlinkHandle(infoArray[0]);  /* 不支持批处理 */
            break;
        case 15: /* RENAME_SUB_RENAME_LOCALLY (protobuf enum) */
            FalconRenameSubRenameLocallyHandle(infoArray[0]);  /* 不支持批处理 */
            break;
        case 16: /* RENAME_SUB_CREATE (protobuf enum) */
            FalconRenameSubCreateHandle(infoArray[0]);  /* 不支持批处理 */
            break;
        case 17: /* UTIMENS (protobuf enum) */
            FalconUtimeNsHandle(infoArray[0]);  /* UTIMENS 不支持批处理 */
            break;
        case 18: /* CHOWN (protobuf enum) */
            FalconChownHandle(infoArray[0]);  /* CHOWN 不支持批处理 */
            break;
        case 19: /* CHMOD (protobuf enum) */
            FalconChmodHandle(infoArray[0]);  /* CHMOD 不支持批处理 */
            break;
        default:
            FALCON_ELOG_ERROR(ARGUMENT_ERROR, "Unsupported operation type");
    }

    /* 5. 构造响应并写入共享内存 */
    size_t response_size = 0;

    /* 计算响应大小 (使用 protobuf MetaServiceType 枚举) */
    switch (operation_type) {
        case 20:  /* KV_PUT */
        case 22:  /* KV_DEL */
            /* 简单操作：每个返回一个 int32 错误码 */
            response_size = count * sizeof(int32_t);
            break;

        case 21:  /* KV_GET */
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

        case 1:  /* MKDIR (protobuf enum) */
        case 7:  /* CLOSE (protobuf enum) */
        case 11: /* RMDIR (protobuf enum) */
        case 14: /* RENAME (protobuf enum) */
        case 2:  /* MKDIR_SUB_MKDIR (protobuf enum) */
        case 3:  /* MKDIR_SUB_CREATE (protobuf enum) */
        case 12: /* RMDIR_SUB_RMDIR (protobuf enum) */
        case 13: /* RMDIR_SUB_UNLINK (protobuf enum) */
        case 15: /* RENAME_SUB_RENAME_LOCALLY (protobuf enum) */
        case 16: /* RENAME_SUB_CREATE (protobuf enum) */
        case 17: /* UTIMENS (protobuf enum) */
        case 18: /* CHOWN (protobuf enum) */
        case 19: /* CHMOD (protobuf enum) */
            /* 简单操作：每个返回一个 int32 错误码 */
            response_size = count * sizeof(int32_t);
            break;

        case 9:  /* READDIR (protobuf enum) */
            response_size = 0;
            for (uint32_t i = 0; i < count; i++) {
                response_size += sizeof(int32_t);
                response_size += 4;
                response_size += 2;
                if (infoDataArray[i].errorCode == SUCCESS && infoDataArray[i].readDirLastFileName != NULL) {
                    response_size += strlen(infoDataArray[i].readDirLastFileName);
                }
                response_size += 4;
                if (infoDataArray[i].errorCode == SUCCESS) {
                    for (int j = 0; j < infoDataArray[i].readDirResultCount; j++) {
                        response_size += 2;
                        response_size += strlen(infoDataArray[i].readDirResultList[j]->fileName);
                        response_size += 4;
                    }
                }
            }
            break;

        case 4:  /* CREATE (protobuf enum) */
            /* CREATE: 返回 int32(status) + CreateResponse(14个字段) */
            response_size = count * (sizeof(int32_t) + 104);
            break;

        case 8:  /* UNLINK (protobuf enum) */
            /* UNLINK: 返回 int32(status) + st_ino(8) + st_size(8) + node_id(8) */
            response_size = count * (sizeof(int32_t) + 24);
            break;

        case 10: /* OPENDIR (protobuf enum) */
            /* OPENDIR: 返回 int32(status) + st_ino(8) */
            response_size = count * (sizeof(int32_t) + 8);
            break;

        case 5:  /* STAT (protobuf enum) */
            /* STAT: 每个返回 int32(status) + StatResponse(13个字段) */
            response_size = count * (sizeof(int32_t) + 13 * 8);
            break;

        case 6:  /* OPEN (protobuf enum) */
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
            case 20:  /* KV_PUT */
            case 22:  /* KV_DEL */
                *(int32_t*)resp_p = infoDataArray[i].errorCode;
                resp_p += sizeof(int32_t);
                break;

            case 21:  /* KV_GET */
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


            case 1:  /* MKDIR (protobuf enum) */
            case 7:  /* CLOSE (protobuf enum) */
            case 11: /* RMDIR (protobuf enum) */
            case 14: /* RENAME (protobuf enum) */
            case 2:  /* MKDIR_SUB_MKDIR (protobuf enum) */
            case 3:  /* MKDIR_SUB_CREATE (protobuf enum) */
            case 12: /* RMDIR_SUB_RMDIR (protobuf enum) */
            case 13: /* RMDIR_SUB_UNLINK (protobuf enum) */
            case 15: /* RENAME_SUB_RENAME_LOCALLY (protobuf enum) */
            case 16: /* RENAME_SUB_CREATE (protobuf enum) */
            case 17: /* UTIMENS (protobuf enum) */
            case 18: /* CHOWN (protobuf enum) */
            case 19: /* CHMOD (protobuf enum) */
                *(int32_t*)resp_p = infoDataArray[i].errorCode;
                resp_p += sizeof(int32_t);
                break;

            case 4:  /* CREATE (protobuf enum) */
                *(int32_t*)resp_p = infoDataArray[i].errorCode;
                resp_p += sizeof(int32_t);

                *(uint64_t*)resp_p = infoDataArray[i].inodeId; resp_p += 8;      /* st_ino */
                *(int64_t*)resp_p = infoDataArray[i].node_id; resp_p += 8;       /* node_id */
                *(uint64_t*)resp_p = infoDataArray[i].st_dev; resp_p += 8;       /* st_dev */
                *(uint32_t*)resp_p = infoDataArray[i].st_mode; resp_p += 4;      /* st_mode */
                *(uint64_t*)resp_p = infoDataArray[i].st_nlink; resp_p += 8;     /* st_nlink */
                *(uint32_t*)resp_p = infoDataArray[i].st_uid; resp_p += 4;       /* st_uid */
                *(uint32_t*)resp_p = infoDataArray[i].st_gid; resp_p += 4;       /* st_gid */
                *(uint64_t*)resp_p = infoDataArray[i].st_rdev; resp_p += 8;      /* st_rdev */
                *(int64_t*)resp_p = infoDataArray[i].st_size; resp_p += 8;       /* st_size */
                *(int64_t*)resp_p = infoDataArray[i].st_blksize; resp_p += 8;    /* st_blksize */
                *(int64_t*)resp_p = infoDataArray[i].st_blocks; resp_p += 8;     /* st_blocks */
                *(uint64_t*)resp_p = infoDataArray[i].st_atim; resp_p += 8;      /* st_atim */
                *(uint64_t*)resp_p = infoDataArray[i].st_mtim; resp_p += 8;      /* st_mtim */
                *(uint64_t*)resp_p = infoDataArray[i].st_ctim; resp_p += 8;      /* st_ctim */
                break;

            case 8:  /* UNLINK (protobuf enum) */
                *(int32_t*)resp_p = infoDataArray[i].errorCode;
                resp_p += sizeof(int32_t);

                *(uint64_t*)resp_p = infoDataArray[i].inodeId; resp_p += 8;      /* st_ino */
                *(int64_t*)resp_p = infoDataArray[i].st_size; resp_p += 8;       /* st_size */
                *(int64_t*)resp_p = infoDataArray[i].node_id; resp_p += 8;       /* node_id */
                break;

            case 10: /* OPENDIR (protobuf enum) */
                *(int32_t*)resp_p = infoDataArray[i].errorCode;
                resp_p += sizeof(int32_t);

                *(uint64_t*)resp_p = infoDataArray[i].inodeId; resp_p += 8;      /* st_ino */
                break;

            case 5:  /* STAT (protobuf enum) */
                *(int32_t*)resp_p = infoDataArray[i].errorCode;
                resp_p += sizeof(int32_t);

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
                break;

            case 6:  /* OPEN (protobuf enum) */
                *(int32_t*)resp_p = infoDataArray[i].errorCode;
                resp_p += sizeof(int32_t);

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
                break;

            case 9:  /* READDIR (protobuf enum) */
                *(int32_t*)resp_p = infoDataArray[i].errorCode;
                resp_p += sizeof(int32_t);

                *(int32_t*)resp_p = infoDataArray[i].readDirLastShardIndex;
                resp_p += 4;

                if (infoDataArray[i].readDirLastFileName == NULL) {
                    *(uint16_t*)resp_p = 0;
                    resp_p += 2;
                } else {
                    uint16_t last_name_len = strlen(infoDataArray[i].readDirLastFileName);
                    *(uint16_t*)resp_p = last_name_len;
                    resp_p += 2;
                    memcpy(resp_p, infoDataArray[i].readDirLastFileName, last_name_len);
                    resp_p += last_name_len;
                }

                *(uint32_t*)resp_p = infoDataArray[i].readDirResultCount;
                resp_p += 4;

                for (int j = 0; j < infoDataArray[i].readDirResultCount; j++) {
                    OneReadDirResult* result = infoDataArray[i].readDirResultList[j];

                    uint16_t file_name_len = strlen(result->fileName);
                    *(uint16_t*)resp_p = file_name_len;
                    resp_p += 2;
                    memcpy(resp_p, result->fileName, file_name_len);
                    resp_p += file_name_len;

                    *(uint32_t*)resp_p = result->mode;
                    resp_p += 4;
                }
                break;
        }
    }

    /* 6. 清理 SPI 连接（如果 GET_KV 操作打开了 SPI） */
    if (operation_type == 21 && kvResultTable != NULL) {  /* KV_GET (protobuf enum) */
        SPI_finish();
    }

    printf("[debug] falcon_batch_meta_call_by_shmem: EXIT, returning responseShmemShift=%lu\n", responseShmemShift);
    fflush(stdout);

    /* 7. 返回响应在共享内存中的偏移量 */
    PG_RETURN_INT64(responseShmemShift);
}
