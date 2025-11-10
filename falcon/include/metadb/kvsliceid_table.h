/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_KVSLICE_TABLE_H
#define FALCON_KVSLICE_TABLE_H

#include "metadb/metadata.h"
#include "utils/error_code.h"

#define Natts_falcon_kvsliceid_table 2
#define Anum_falcon_kvsliceid_table_keystr 1
#define Anum_falcon_kvsliceid_table_sliceid 2

typedef enum FalconKvSliceIdTableScankeyType {
    KVSLICEID_TABLE_SLICEID_EQ,
    LAST_FALCON_KVSLICEID_TABLE_SCANKEY_TYPE
} FalconKvSliceIdTableScankeyType;

Oid KvSliceIdRelationId(void);
Oid KvSliceIdRelationIndexId(void);

#endif