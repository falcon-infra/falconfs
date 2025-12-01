/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_META_SERVICE_H
#define FALCON_META_SERVICE_H

#include <memory>
#include <mutex>
#include <chrono>
#include "connection_pool/falcon_meta_service_interface.h"

class PGConnection;
class PGConnectionPool;

namespace falcon {
namespace meta_service {

/**
 * Falcon 元数据异步任务
 * 封装一个 Falcon 元数据操作请求
 */
class AsyncFalconMetaServiceJob {
private:
    FalconMetaServiceRequest request;
    FalconMetaServiceResponse response;
    FalconMetaServiceCallback callback;
    void* user_context;
    std::chrono::steady_clock::time_point start_time;  // 请求接收时间

    void CleanupResponseData() {
        if (response.data != nullptr) {
            switch (response.opcode) {
                case DFC_GET_KV_META:
                    delete static_cast<KvDataResponse*>(response.data);
                    break;
                case DFC_PLAIN_COMMAND:
                    delete static_cast<PlainCommandResponse*>(response.data);
                    break;
                case DFC_CREATE:
                    delete static_cast<CreateResponse*>(response.data);
                    break;
                case DFC_OPEN:
                    delete static_cast<OpenResponse*>(response.data);
                    break;
                case DFC_STAT:
                    delete static_cast<StatResponse*>(response.data);
                    break;
                case DFC_UNLINK:
                    delete static_cast<UnlinkResponse*>(response.data);
                    break;
                case DFC_READDIR:
                    delete static_cast<ReadDirResponse*>(response.data);
                    break;
                case DFC_OPENDIR:
                    delete static_cast<OpenDirResponse*>(response.data);
                    break;
                case DFC_RENAME_SUB_RENAME_LOCALLY:
                    delete static_cast<RenameSubRenameLocallyResponse*>(response.data);
                    break;
                case DFC_SLICE_GET:
                    delete static_cast<SliceInfoResponse*>(response.data);
                    break;
                case DFC_FETCH_SLICE_ID:
                    delete static_cast<SliceIdResponse*>(response.data);
                    break;
                default:
                    // PUT, DELETE, MKDIR, RMDIR, CLOSE, RENAME, UTIMENS, CHOWN, CHMOD, SLICE_PUT, SLICE_DEL 不返回数据
                    break;
            }
            response.data = nullptr;
        }
    }

public:
    AsyncFalconMetaServiceJob(const FalconMetaServiceRequest& req,
                              FalconMetaServiceCallback cb,
                              void* ctx)
        : request(req), callback(cb), user_context(ctx),
          start_time(std::chrono::steady_clock::now()) {}

    ~AsyncFalconMetaServiceJob() {
        CleanupResponseData();
    }

    FalconMetaServiceRequest& GetRequest() { return request; }
    FalconMetaServiceResponse& GetResponse() { return response; }

    void Done() {
        auto end_time = std::chrono::steady_clock::now();
        auto total_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

        printf("[perf][FalconMetaService] opcode=%d(%s), status=%d, total=%ld us\n",
               response.opcode, FalconMetaOperationTypeName(response.opcode),
               response.status, total_us);
        fflush(stdout);

        if (callback) {
            callback(response, user_context);
            CleanupResponseData();
        }
    }
};

/**
 * Falcon 元数据服务实现类
 * 提供 KV 操作和文件语义操作
 */
class FalconMetaService {
private:
    std::shared_ptr<PGConnectionPool> pgConnectionPool;
    static FalconMetaService* instance;
    static std::mutex instanceMutex;
    bool initialized;

    FalconMetaService();

public:
    static FalconMetaService* Instance();

    bool Init(int port, int pool_size = 10);

    bool IsInitialized() const { return initialized; }

    virtual ~FalconMetaService();

    int DispatchFalconMetaServiceJob(AsyncFalconMetaServiceJob* job);

    /**
     * 提交 Falcon 元数据服务请求（成员方法）
     *
     * @param request: Falcon 元数据服务请求
     * @param callback: 回调函数
     * @param user_context: 用户上下文指针
     * @return: 0 表示成功，非0 表示失败
     */
    int SubmitFalconMetaRequest(const FalconMetaServiceRequest& request,
                                FalconMetaServiceCallback callback,
                                void* user_context = nullptr);
};

} // namespace meta_service
} // namespace falcon

#endif // FALCON_META_SERVICE_H
