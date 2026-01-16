/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_SLICEID_TABLE_H
#define FALCON_SLICEID_TABLE_H

#include "metadb/metadata.h"
#include "utils/error_code.h"

#define Natts_falcon_sliceid_table 2
#define Anum_falcon_sliceid_table_keystr 1
#define Anum_falcon_sliceid_table_sliceid 2

typedef enum FalconSliceIdTableScankeyType {
    SLICEID_TABLE_SLICEID_EQ,
    LAST_FALCON_SLICEID_TABLE_SCANKEY_TYPE
} FalconSliceIdTableScankeyType;

Oid KvSliceIdRelationId(void);
Oid FileSliceIdRelationId(void);

#endif