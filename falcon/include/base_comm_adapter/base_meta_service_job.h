/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef BASE_META_SERVICE_JOB_H
#define BASE_META_SERVICE_JOB_H

#include <functional>
#include "utils/falcon_meta_service_def.h"

class BaseMetaServiceJob {
  public:
    BaseMetaServiceJob() = default;
    virtual ~BaseMetaServiceJob() = default;

    // Call this function after Job finished to send response and release resource
    virtual void Done() = 0;

    // check where batch is allowed by send msg
    virtual bool IsAllowBatchProcess() = 0;

    // check whether request is empty,
    virtual bool IsEmptyRequest() = 0;

    // get Request Meta Service count, GetReqServiceCnt return's 0 while request is empty
    virtual int GetReqServiceCnt() = 0;

    // get Request Data Size;
    virtual size_t GetReqDatasize() = 0;

    // copy request data to dst for job doing
    virtual size_t CopyOutData(void *dst, size_t dstSize) = 0;

    // get falcon support meta service types
    // one Job may contains many Meta Service Request, so get service type by index
    virtual FalconMetaServiceType GetFalconMetaServiceType(int index) = 0;

    // using shared flatBufferBuilder generate error response msg and reply to client
    // BrpcMetaServiceJob need recycle the data bye deleter
    using FalDataDeleter = std::function<void(void *)>;
    virtual void ProcessResponse(void *data, size_t size, FalDataDeleter deleter) = 0;
};

// used for dispatch Falcon meta Job, decouple the dependency connection pool and communication Service
using FalconMetaJobDispatchFun = std::function<void(BaseMetaServiceJob *job)>;

#endif // BASE_META_SERVICE_JOB_H