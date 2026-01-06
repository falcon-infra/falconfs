/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */
#ifndef BRPC_META_SERVICE_JOB_H
#define BRPC_META_SERVICE_JOB_H

#include <brpc/server.h>
#include "base_comm_adapter/base_meta_service_job.h"
#include "falcon_meta_rpc.pb.h"

using namespace falcon::meta_proto;
class BrpcMetaServiceJob : public BaseMetaServiceJob {
  private:
    brpc::Controller *m_cntl;
    const MetaRequest *m_request;
    Empty *m_response;
    google::protobuf::Closure *m_done;

  private:
    FalconMetaServiceType MetaServiceTypeDecode(falcon::meta_proto::MetaServiceType type);

  public:
    BrpcMetaServiceJob(brpc::Controller *cntl,
                       const MetaRequest *request,
                       Empty *response,
                       google::protobuf::Closure *done)
        : m_cntl(cntl),
          m_request(request),
          m_response(response),
          m_done(done)
    {
    }

    // Call this function after Job is done to send response and release resource
    void Done() override { m_done->Run(); }

    // only while allow_batch_with_others set to true and all operations are same,
    // allows operations processed by batch.
    bool IsAllowBatchProcess() override
    {
        // while no operation type set or allow_batch_with_others set to false, not allow batch process
        bool allowBatchWithOthers = m_request->allow_batch_with_others();
        if (m_request->type_size() == 0 || !allowBatchWithOthers) {
            return false;
        }

        falcon::meta_proto::MetaServiceType type = m_request->type(0);
        // check whether all operations types are same
        for (int i = 1; i < m_request->type_size(); ++i) {
            if (m_request->type(i) != type) {
                return false;
            }
        }
        return true;
    }

    // check whether request is empty
    bool IsEmptyRequest() override { return m_request->type_size() == 0; }

    // get Request Service count
    int GetReqServiceCnt() override { return m_request->type_size(); }

    // get Request Data Size
    size_t GetReqDatasize() override { return m_cntl->request_attachment().size(); }

    // copy data to dst
    size_t CopyOutData(void *dst, size_t dstSize) override { return m_cntl->request_attachment().cutn(dst, dstSize); }

    // get falcon support meta service types
    FalconMetaServiceType GetFalconMetaServiceType(int index) override;

    // using shared flatBufferBuilder generate error response msg and reply to client
    void ProcessResponse(void *data, size_t size, FalDataDeleter deleter) override
    {
        // now data transfer to response, and delete by response.
        m_cntl->response_attachment().append_user_data(data, size, NULL);
    }
};

#endif // BRPC_META_SERVICE_JOB_H
