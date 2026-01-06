/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "brpc_comm_adapter/brpc_meta_service_imp.h"

#include <brpc/server.h>
#include <butil/iobuf.h>

namespace falcon::meta_proto
{

void BrpcMetaServiceImpl::MetaCall(google::protobuf::RpcController *cntlBase,
                                   const MetaRequest *request,
                                   Empty *response,
                                   google::protobuf::Closure *done)
{
    brpc::ClosureGuard doneGuard(done);
    brpc::Controller *cntl = static_cast<brpc::Controller *>(cntlBase);

    BrpcMetaServiceJob *job = new BrpcMetaServiceJob(cntl, request, response, done);
    m_jobDispatchFunc(static_cast<void *>(job));
    doneGuard.release();
}

} // namespace falcon::meta_proto
