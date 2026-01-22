/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_SLICE_TABLE_H
#define FALCON_SLICE_TABLE_H

#include "metadb/metadata.h"
#include "utils/error_code.h"

#define Natts_falcon_slice_table 8
#define Anum_falcon_slice_table_inodeid 1
#define Anum_falcon_slice_table_chunkid 2
#define Anum_falcon_slice_table_sliceid 3
#define Anum_falcon_slice_table_slicesize 4
#define Anum_falcon_slice_table_sliceoffset 5
#define Anum_falcon_slice_table_slicelen 6
#define Anum_falcon_slice_table_sliceloc1 7
#define Anum_falcon_slice_table_sliceloc2 8

typedef enum FalconSliceTableScankeyType {
    SLICE_TABLE_INODEID_EQ,
    SLICE_TABLE_CHUNKID_EQ,
    LAST_FALCON_SLICE_TABLE_SCANKEY_TYPE
} FalconSliceTableScankeyType;

extern const char *SliceTableName;

void ConstructCreateSliceTableCommand(StringInfo command, const char *name);

#endif