/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef BRPC_META_SERVICE_IMP_H
#define BRPC_META_SERVICE_IMP_H

#include <brpc/server.h>
#include <butil/iobuf.h>
#include "brpc_comm_adapter/brpc_meta_service_job.h"
#include "base_comm_adapter/comm_server_interface.h"
#include "falcon_meta_rpc.pb.h"

namespace falcon::meta_proto
{

class BrpcMetaServiceImpl : public MetaService {
  private:
    falcon_meta_job_dispatch_func m_jobDispatchFunc;

  public:
    BrpcMetaServiceImpl(falcon_meta_job_dispatch_func jobDispatchFunc)
        : m_jobDispatchFunc(jobDispatchFunc)
    {
    }
    ~BrpcMetaServiceImpl() override {}

    void MetaCall(google::protobuf::RpcController *cntlBase,
                  const MetaRequest *request,
                  Empty *response,
                  google::protobuf::Closure *done) override;
};

} // namespace falcon::meta_proto

#endif // BRPC_META_SERVICE_IMP_H
