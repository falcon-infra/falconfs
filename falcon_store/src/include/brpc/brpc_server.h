/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <condition_variable>
#include <memory_resource>
#include <mutex>

#include <brpc/server.h>

#include "brpc_io.pb.h"

namespace falcon::brpc_io
{

class RemoteIOServiceImpl : public RemoteIOService {
  public:
    RemoteIOServiceImpl() = default;
    ~RemoteIOServiceImpl() override = default;

    void OpenFile(google::protobuf::RpcController *cntl_base,
                  const OpenRequest *request,
                  OpenReply *response,
                  google::protobuf::Closure *done) override;

    void CloseFile(google::protobuf::RpcController *cntl_base,
                   const CloseRequest *request,
                   ErrorCodeOnlyReply *response,
                   google::protobuf::Closure *done) override;

    void ReadFile(google::protobuf::RpcController *cntl_base,
                  const ReadRequest *request,
                  ErrorCodeOnlyReply *response,
                  google::protobuf::Closure *done) override;

    void ReadSmallFile(google::protobuf::RpcController *cntl_base,
                       const ReadSmallFileRequest *request,
                       ErrorCodeOnlyReply *response,
                       google::protobuf::Closure *done) override;

    void WriteFile(google::protobuf::RpcController *cntl_base,
                   const WriteRequest *request,
                   WriteReply *response,
                   google::protobuf::Closure *done) override;

    void DeleteFile(google::protobuf::RpcController *cntl_base,
                    const DeleteRequest *request,
                    ErrorCodeOnlyReply *response,
                    google::protobuf::Closure *done) override;

    void StatFS(google::protobuf::RpcController *cntl_base,
                const StatFSRequest *request,
                StatFSReply *response,
                google::protobuf::Closure *done) override;

    void TruncateOpenInstance(google::protobuf::RpcController *cntl_base,
                              const TruncateOpenInstanceRequest *request,
                              ErrorCodeOnlyReply *response,
                              google::protobuf::Closure *done) override;

    void TruncateFile(google::protobuf::RpcController *cntl_base,
                      const TruncateFileRequest *request,
                      ErrorCodeOnlyReply *response,
                      google::protobuf::Closure *done) override;

    void CheckConnection(google::protobuf::RpcController *cntl_base,
                         const CheckConnectionRequest *request,
                         ErrorCodeOnlyReply *response,
                         google::protobuf::Closure *done) override;

    void StatCluster(google::protobuf::RpcController *cntl_base,
                         const StatClusterRequest *request,
                         StatClusterReply *response,
                         google::protobuf::Closure *done) override;
};

class RemoteIOServer {
  public:
    bool isStarted;
    bool isReady;
    std::mutex mutexStart;
    std::condition_variable cvStart;
    std::condition_variable cvReady;

    std::string endPoint;
    brpc::Server server;
    static std::pmr::synchronized_pool_resource &GetMemoryPool()
    {
        // 配置适合小文件的pool选项
        static std::pmr::pool_options opts{
            .max_blocks_per_chunk = 1024,            // 更多小内存块
            .largest_required_pool_block = 64 * 1024 // 最大支持64KB
        };
        static std::pmr::synchronized_pool_resource pool(opts);
        return pool;
    }

    RemoteIOServer()
        : isStarted(false),
          isReady(false)
    {
    }

    static RemoteIOServer &GetInstance()
    {
        static RemoteIOServer instance;
        return instance;
    }

    int Run();
    void Stop();
    void SetReadyFlag();
};

} // namespace falcon::brpc_io
