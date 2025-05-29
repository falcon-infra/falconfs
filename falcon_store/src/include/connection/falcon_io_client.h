/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <securec.h>
#include <memory>
#include <string>

#include <brpc/channel.h>

#include "brpc_io.pb.h"
#include "util/utils.h"
#include "stats/falcon_stats.h"

#define BRPC_RETRY_NUM 3
#define BRPC_RETRY_DELEY 1

class FalconIOClient {
  public:
    FalconIOClient()
    {
        channel = nullptr;
        stub = nullptr;
    }
    FalconIOClient(std::shared_ptr<brpc::Channel> initChannel)
    {
        channel = initChannel;
        stub = std::make_unique<falcon::brpc_io::RemoteIOService_Stub>(initChannel.get());
    }

    int ReadFile(uint64_t inodeId,
                 int oflags,
                 char *readBuffer,
                 uint64_t &physicalFd,
                 int BufferSize,
                 off_t offset,
                 const std::string &path = "");
    int CloseFile(uint64_t physicalFd, bool isFlush, bool isSync, const char *buf, size_t size, off_t offset);
    int OpenFile(uint64_t inodeId,
                 int oflags,
                 uint64_t &physicalFd,
                 uint64_t originalSize,
                 const std::string &path,
                 bool nodeFail);
    int WriteFile(uint64_t physicalFd, const char *writeBuffer, uint64_t size, off_t offset);
    ssize_t
    ReadSmallFile(uint64_t inodeId, ssize_t size, std::string &path, char *readBuffer, int oflags, bool nodeFail);
    int DeleteFile(uint64_t inodeId, int nodeId, std::string &path);
    int StatFS(std::string &path, struct StatFSBuf *fsBuf);
    int TruncateOpenInstance(uint64_t physicalFd, off_t size);
    int TruncateFile(uint64_t physicalFd, off_t size);
    int CheckConnection();
    int StatCluster(int nodeId, std::vector<size_t> &stats, bool scatter);

  private:
    std::shared_ptr<brpc::Channel> channel;
    std::unique_ptr<falcon::brpc_io::RemoteIOService_Stub> stub;
};
