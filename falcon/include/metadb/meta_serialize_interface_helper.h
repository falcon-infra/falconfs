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

#ifdef __cplusplus
}
#endif

#endif
