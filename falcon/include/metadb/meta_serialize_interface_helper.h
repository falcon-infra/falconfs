/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_METADB_META_SERIALIZE_INTERFACE_HELPER_H
#define FALCON_METADB_META_SERIALIZE_INTERFACE_HELPER_H

#include <stdint.h>

#include "metadb/meta_handle.h"
#include "utils/falcon_meta_service_def.h"

#ifdef __cplusplus
extern "C" {
#endif

bool SerializedDataMetaParamDecode(FalconMetaServiceType metaService,
                                   int count,
                                   SerializedData *param,
                                   MetaProcessInfoData *infoArray);

bool SerializedDataMetaParamEncodeWithPerProcessFlatBufferBuilder(FalconMetaServiceType metaService,
                                                                  MetaProcessInfo *infoArray,
                                                                  int32_t *index,
                                                                  int count,
                                                                  SerializedData *param);

bool SerializedDataMetaResponseDecode(FalconMetaServiceType metaService,
                                      int count,
                                      SerializedData *response,
                                      MetaProcessInfoData *infoArray);

bool SerializedDataMetaResponseEncodeWithPerProcessFlatBufferBuilder(FalconMetaServiceType metaService,
                                                                     int count,
                                                                     MetaProcessInfoData *infoArray,
                                                                     SerializedData *response);

bool SerializedKvMetaParamDecode(FalconSupportMetaService metaService,
                                 SerializedData *param,
                                 KvMetaProcessInfo infoData);

bool SerializedKvMetaResponseEncodeWithPerProcessFlatBufferBuilder(FalconSupportMetaService metaService,
                                                                   KvMetaProcessInfo infoData,
                                                                   SerializedData *response);

bool SerializedSliceParamDecode(FalconSupportMetaService metaService,
                                int count,
                                SerializedData *param,
                                SliceProcessInfoData *infoArray);

bool SerializedSliceResponseEncodeWithPerProcessFlatBufferBuilder(FalconSupportMetaService metaService,
                                                                  int count,
                                                                  SliceProcessInfoData *infoArray,
                                                                  SerializedData *response);

bool SerializedSliceIdParamDecode(SerializedData *param, SliceIdProcessInfo infoData);

bool SerializedSliceIdResponseEncodeWithPerProcessFlatBufferBuilder(SliceIdProcessInfo infoData, SerializedData *response);

#ifdef __cplusplus
}
#endif

#endif
