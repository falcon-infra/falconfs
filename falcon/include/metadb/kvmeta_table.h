/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_KVMETA_TABLE_H
#define FALCON_KVMETA_TABLE_H

#include "metadb/metadata.h"
#include "utils/error_code.h"
#include "meta_process_info.h"

#define Natts_falcon_kvmeta_table 6
#define Anum_falcon_kvmeta_table_userkey 1
#define Anum_falcon_kvmeta_table_valuelen 2
#define Anum_falcon_kvmeta_table_slicenum 3
#define Anum_falcon_kvmeta_table_valuekey 4
#define Anum_falcon_kvmeta_table_location 5
#define Anum_falcon_kvmeta_table_slicelen 6

typedef enum FalconKvmetaTableScankeyType {
    KVMETA_TABLE_USERKEY_EQ,
    LAST_FALCON_KVMETA_TABLE_SCANKEY_TYPE
} FalconKvmetaTableScankeyType;

extern const char *KvmetaTableName;

void ConstructCreateKvmetaTableCommand(StringInfo command, const char *name);

void FalconKvmetaPutHandle(KvMetaProcessInfo info);
void FalconKvmetaGetHandle(KvMetaProcessInfo info);
void FalconKvmetaDelHandle(KvMetaProcessInfo info);

#endif