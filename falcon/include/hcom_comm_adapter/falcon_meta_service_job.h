/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_META_SERVICE_JOB_H
#define FALCON_META_SERVICE_JOB_H

#include <chrono>
#include <vector>

#include "base_comm_adapter/base_meta_service_job.h"
#include "hcom_comm_adapter/falcon_meta_service_interface.h"

namespace falcon {
namespace meta_service {

class FalconMetaServiceJob : public BaseMetaServiceJob {
  public:
    FalconMetaServiceJob(const FalconMetaServiceRequest &request,
                       FalconMetaServiceCallback callback,
                       void *user_context);
    ~FalconMetaServiceJob() override;

    void Done() override;
    bool IsAllowBatchProcess() override;
    bool IsEmptyRequest() override;
    int GetReqServiceCnt() override;
    size_t GetReqDatasize() override;
    size_t CopyOutData(void *dst, size_t dstSize) override;
    FalconMetaServiceType GetFalconMetaServiceType(int index) override;
    void ProcessResponse(void *data, size_t size, FalDataDeleter deleter) override;

    FalconMetaServiceRequest &GetRequest() { return m_request; }
    FalconMetaServiceResponse &GetResponse() { return m_response; }

  private:
    FalconMetaServiceRequest m_request;
    FalconMetaServiceResponse m_response;
    FalconMetaServiceCallback m_callback;
    void *m_user_context;
    std::chrono::steady_clock::time_point m_start_time;
    std::vector<char> m_request_buffer;
};

} // namespace meta_service
} // namespace falcon

#endif // FALCON_META_SERVICE_JOB_H
