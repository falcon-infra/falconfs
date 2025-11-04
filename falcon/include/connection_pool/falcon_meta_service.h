/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_META_SERVICE_H
#define FALCON_META_SERVICE_H

#include <memory>
#include "connection_pool/falcon_meta_service_interface.h"
#include "connection_pool/pg_connection_pool.h"

class PGConnection;

namespace falcon {
namespace meta_service {

// 二进制格式的签名 (0x46414C43 = ASCII "FALC")
#define FALCON_BINARY_SIGNATURE 0x46414C43U

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
                default:
                    // PUT, DELETE, MKDIR, RMDIR, CLOSE, RENAME, UTIMENS, CHOWN, CHMOD 不返回数据
                    break;
            }
            response.data = nullptr;
        }
    }

public:
    AsyncFalconMetaServiceJob(const FalconMetaServiceRequest& req,
                              FalconMetaServiceCallback cb,
                              void* ctx)
        : request(req), callback(cb), user_context(ctx) {}

    ~AsyncFalconMetaServiceJob() {
        CleanupResponseData();
    }

    FalconMetaServiceRequest& GetRequest() { return request; }
    FalconMetaServiceResponse& GetResponse() { return response; }

    void Done() {
        printf("[debug] AsyncFalconMetaServiceJob::Done: ENTRY, job=%p, callback_valid=%d\n",
               this, callback ? 1 : 0);
        fflush(stdout);

        if (callback) {
            printf("[debug] AsyncFalconMetaServiceJob::Done: Calling user callback, opcode=%d, status=%d\n",
                   response.opcode, response.status);
            fflush(stdout);

            callback(response, user_context);

            printf("[debug] AsyncFalconMetaServiceJob::Done: User callback RETURNED\n");
            fflush(stdout);

            CleanupResponseData();
        } else {
            printf("[debug] AsyncFalconMetaServiceJob::Done: WARNING - callback is NULL!\n");
            fflush(stdout);
        }

        printf("[debug] AsyncFalconMetaServiceJob::Done: EXIT\n");
        fflush(stdout);
    }
};

/**
 * Falcon 元数据服务实现类
 * 提供 KV 操作和文件语义操作
 */
class FalconMetaService {
public:
    struct BinaryHeader {
        uint32_t signature;       // 签名: FALCON_BINARY_SIGNATURE
        uint32_t count;           // 请求数量（批处理时 > 1）
        uint32_t operation_type;  // 操作类型 (FalconMetaOperationType)
        uint32_t reserved;        // 保留字段

        BinaryHeader() : signature(FALCON_BINARY_SIGNATURE), count(0), operation_type(0), reserved(0) {}
    };

    static bool SerializeRequestToBinary(
        const FalconMetaServiceRequest& request,
        falcon::meta_proto::MetaRequest* proto_request,
        butil::IOBuf* attachment);

    static bool DeserializeResponseFromBinary(
        const butil::IOBuf& attachment,
        FalconMetaServiceResponse* response,
        FalconMetaOperationType operation);

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
