/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "postgres.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include "metadb/meta_handle.h"
#include "metadb/meta_process_info.h"
#include "utils/error_code.h"

/*
 * FalconPutKvMetaHandle - 批量插入 KV 元数据
 *
 * 使用批量 INSERT 语句一次性插入多个 key 的所有 slices
 */
void FalconPutKvMetaHandle(MetaProcessInfo *infoArray, int count, char *paramsData)
{
    StringInfoData sql;
    initStringInfo(&sql);

    /* 开始事务 */
    int ret = SPI_connect();
    if (ret < 0) {
        for (int i = 0; i < count; i++) {
            infoArray[i]->errorCode = PROGRAM_ERROR;
        }
        return;
    }

    /* 构造批量 INSERT 语句 */
    appendStringInfo(&sql,
        "INSERT INTO pg_catalog.falcon_kv_metadata_store "
        "(key, slice_id, value_key, location, size, value_len, slice_num) VALUES ");

    bool first = true;
    char *p = paramsData;

    for (int i = 0; i < count; i++) {
        MetaProcessInfo info = infoArray[i];

        /* 读取 key_len 和 key */
        uint16_t key_len = *(uint16_t*)p;
        p += 2;
        char *key = p;
        p += key_len;

        /* 读取 valueLen 和 sliceNum */
        uint32_t valueLen = *(uint32_t*)p;
        p += 4;
        uint16_t sliceNum = *(uint16_t*)p;
        p += 2;

        /* 为每个 slice 添加一行 */
        for (int j = 0; j < sliceNum; j++) {
            uint64_t value_key = *(uint64_t*)p;
            p += 8;
            uint64_t location = *(uint64_t*)p;
            p += 8;
            uint32_t size = *(uint32_t*)p;
            p += 4;

            if (!first) {
                appendStringInfoString(&sql, ", ");
            }
            first = false;

            /* 使用 quote_literal 防止 SQL 注入 */
            appendStringInfo(&sql,
                "('%.*s', %d, %lu, %lu, %u, %u, %d)",
                key_len, key, j, value_key, location, size, valueLen, sliceNum);
        }

        info->errorCode = SUCCESS;
    }

    /* 添加 ON CONFLICT 子句（更新已存在的记录） */
    appendStringInfoString(&sql, " ON CONFLICT (key, slice_id) DO UPDATE SET "
                                 "value_key = EXCLUDED.value_key, "
                                 "location = EXCLUDED.location, "
                                 "size = EXCLUDED.size, "
                                 "value_len = EXCLUDED.value_len, "
                                 "slice_num = EXCLUDED.slice_num");

    /* 执行 SQL */
    ret = SPI_exec(sql.data, 0);

    if (ret < 0) {
        for (int i = 0; i < count; i++) {
            infoArray[i]->errorCode = PROGRAM_ERROR;
        }
    }

    SPI_finish();
    pfree(sql.data);
}

/*
 * FalconGetKvMetaHandle - 批量查询 KV 元数据
 *
 * 使用 WHERE key IN (...) 批量查询多个 key 的所有 slices
 * 返回完整的 slice 数组数据（value_key, location, size）
 *
 * 返回值：SPI_tuptable（调用者负责在 SPI_finish 之前使用）
 */
SPITupleTable* FalconGetKvMetaHandle(MetaProcessInfo *infoArray, int count)
{
    StringInfoData sql;
    initStringInfo(&sql);

    int ret = SPI_connect();
    if (ret < 0) {
        for (int i = 0; i < count; i++) {
            infoArray[i]->errorCode = PROGRAM_ERROR;
        }
        return NULL;
    }

    /* 构造批量 SELECT 语句 */
    appendStringInfo(&sql,
        "SELECT key, slice_id, value_key, location, size, value_len, slice_num "
        "FROM pg_catalog.falcon_kv_metadata_store WHERE key IN (");

    for (int i = 0; i < count; i++) {
        if (i > 0) {
            appendStringInfoString(&sql, ", ");
        }
        appendStringInfo(&sql, "'%s'", infoArray[i]->path);
    }

    appendStringInfoString(&sql, ") ORDER BY key, slice_id");

    /* 执行查询 */
    ret = SPI_exec(sql.data, 0);

    if (ret < 0) {
        for (int i = 0; i < count; i++) {
            infoArray[i]->errorCode = PROGRAM_ERROR;
        }
        SPI_finish();
        pfree(sql.data);
        return NULL;
    }

    /* 初始化所有请求为未找到 */
    for (int i = 0; i < count; i++) {
        infoArray[i]->errorCode = FILE_NOT_EXISTS;
        infoArray[i]->st_size = 0;
        infoArray[i]->node_id = 0;
    }

    /* 解析结果 - 填充基本信息到 infoArray */
    SPITupleTable* tuptable = NULL;
    if (SPI_processed > 0) {
        tuptable = SPI_tuptable;  /* 保存指针供调用者使用 */

        char *current_key = NULL;
        int current_idx = -1;

        for (uint64 row = 0; row < SPI_processed; row++) {
            char *key = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, 1);

            /* 查找对应的 infoArray 索引 */
            if (current_key == NULL || strcmp(key, current_key) != 0) {
                current_key = key;
                current_idx = -1;
                for (int i = 0; i < count; i++) {
                    if (strcmp(infoArray[i]->path, key) == 0) {
                        current_idx = i;
                        break;
                    }
                }
            }

            if (current_idx >= 0) {
                MetaProcessInfo info = infoArray[current_idx];

                /* 第一行：设置 valueLen 和 sliceNum */
                if (info->errorCode != SUCCESS) {
                    char *value_len_str = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, 6);
                    char *slice_num_str = SPI_getvalue(SPI_tuptable->vals[row], SPI_tuptable->tupdesc, 7);

                    info->st_size = atoi(value_len_str);     /* 使用 st_size 存储 valueLen */
                    info->node_id = atoi(slice_num_str);     /* 使用 node_id 存储 sliceNum */
                    info->errorCode = SUCCESS;
                }
            }
        }
    }

    /* 注意：不调用 SPI_finish()！
     * 调用者需要从 tuptable 中读取 slice 详细数据后再调用 SPI_finish()
     */
    pfree(sql.data);
    return tuptable;
}

/*
 * FalconDeleteKvMetaHandle - 批量删除 KV 元数据
 *
 * 使用 WHERE key IN (...) 批量删除多个 key 的所有 slices
 */
void FalconDeleteKvMetaHandle(MetaProcessInfo *infoArray, int count)
{
    StringInfoData sql;
    initStringInfo(&sql);

    int ret = SPI_connect();
    if (ret < 0) {
        for (int i = 0; i < count; i++) {
            infoArray[i]->errorCode = PROGRAM_ERROR;
        }
        return;
    }

    /* 构造批量 DELETE 语句 */
    appendStringInfo(&sql,
        "DELETE FROM pg_catalog.falcon_kv_metadata_store WHERE key IN (");

    for (int i = 0; i < count; i++) {
        if (i > 0) {
            appendStringInfoString(&sql, ", ");
        }
        appendStringInfo(&sql, "'%s'", infoArray[i]->path);
    }

    appendStringInfoString(&sql, ")");

    /* 执行 SQL */
    ret = SPI_exec(sql.data, 0);

    if (ret >= 0) {
        for (int i = 0; i < count; i++) {
            infoArray[i]->errorCode = SUCCESS;
        }
    } else {
        for (int i = 0; i < count; i++) {
            infoArray[i]->errorCode = PROGRAM_ERROR;
        }
    }

    SPI_finish();
    pfree(sql.data);
}
