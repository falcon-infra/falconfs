/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <memory>
#include <string>
#include <string_view>

#include <sys/stat.h>

#include <brpc/channel.h>
#include "falcon_kv_meta.h"
#include "falcon_slice_meta.h"
#include "falcon_meta_response_generated.h"
#include "falcon_meta_rpc.pb.h"
#include "remote_connection_utils/error_code_def.h"
#include "remote_connection_utils/serialized_data.h"

struct ServerIdentifier
{
    std::string ip;
    int port;
    int id;
    ServerIdentifier() = default;

    ServerIdentifier(std::string_view ip, int port)
        : ip(ip),
          port(port)
    {
        id = 0;
    }

    ServerIdentifier(std::string_view ip, int port, int id)
        : ip(ip),
          port(port),
          id(id)
    {
    }
    bool operator==(const ServerIdentifier &t) const { return ip == t.ip && port == t.port && id == t.id; }
};

struct ServerIdentifierHash
{
    std::size_t operator()(const ServerIdentifier &t) const
    {
        return std::hash<std::string>()(t.ip) ^ std::hash<int>()(t.port) ^ std::hash<int>()(t.id);
    }
};

class ConnectionCache {
  public:
    flatbuffers::FlatBufferBuilder flatBufferBuilder;
    SerializedData serializedDataBuffer;

    ConnectionCache() { SerializedDataInit(&serializedDataBuffer, nullptr, 0, 0, nullptr); }
    ~ConnectionCache() { SerializedDataDestroy(&serializedDataBuffer); }
};



static thread_local ConnectionCache ThreadLocalConnectionCache;

class Connection {
  private:
    brpc::Channel channel;
    falcon::meta_proto::MetaService_Stub stub;
    template <typename ParamBuilder, typename ResponseHandler, typename ResultType = void>
    FalconErrorCode ProcessRequest(falcon::meta_proto::MetaServiceType type,
                                   const ParamBuilder &paramBuilder,
                                   ResponseHandler responseHandler,
                                   ConnectionCache *cache = nullptr,
                                   ResultType *result = nullptr);

  public:
    ServerIdentifier server;
    Connection(const ServerIdentifier &serverIdentifier)
        : stub(&channel),
          server(serverIdentifier)
    {
        brpc::ChannelOptions options;
        if (channel.Init(serverIdentifier.ip.c_str(), serverIdentifier.port, &options) != 0)
            throw std::runtime_error("Fail to init channel to " + serverIdentifier.ip + ":" +
                                     std::to_string(serverIdentifier.port));
    }
    ~Connection() = default;

    class PlainCommandResult {
        friend Connection;

      protected:
        std::unique_ptr<char[]> responseBuffer;

      public:
        const falcon::meta_fbs::PlainCommandResponse *response;

        PlainCommandResult()
            : responseBuffer(nullptr),
              response(nullptr)
        {
        }
    };
    FalconErrorCode PlainCommand(const char *command, PlainCommandResult &result, ConnectionCache *cache = nullptr);
    FalconErrorCode Mkdir(const char *path, ConnectionCache *cache = nullptr);
    FalconErrorCode
    Create(const char *path, uint64_t &inodeId, int32_t &nodeId, struct stat *stbuf, ConnectionCache *cache = nullptr);
    FalconErrorCode Stat(const char *path, struct stat *stbuf, ConnectionCache *cache = nullptr);
    FalconErrorCode Open(const char *path,
                         uint64_t &inodeId,
                         int64_t &size,
                         int32_t &nodeId,
                         struct stat *stbuf,
                         ConnectionCache *cache = nullptr);
    FalconErrorCode
    Close(const char *path, int64_t size, uint64_t mtime, int32_t nodeId, ConnectionCache *cache = nullptr);
    FalconErrorCode
    Unlink(const char *path, uint64_t &inodeId, int64_t &size, int32_t &nodeId, ConnectionCache *cache = nullptr);

    struct ReadDirResponse
    {
      protected:
        std::unique_ptr<char[]> buffer;

      public:
        const falcon::meta_fbs::ReadDirResponse *response;
        friend class Connection;
    };
    FalconErrorCode ReadDir(const char *path,
                            ReadDirResponse &readDirResponse,
                            int32_t maxReadCount = -1,
                            int32_t lastShardIndex = -1,
                            const char *lastFileName = nullptr,
                            ConnectionCache *cache = nullptr);

    FalconErrorCode OpenDir(const char *path, uint64_t &inodeId, ConnectionCache *cache = nullptr);
    FalconErrorCode Rmdir(const char *path, ConnectionCache *cache = nullptr);
    FalconErrorCode Rename(const char *src, const char *dst, ConnectionCache *cache = nullptr);
    FalconErrorCode UtimeNs(const char *path, int64_t atime = -1, int64_t mtime = -1, ConnectionCache *cache = nullptr);
    FalconErrorCode Chown(const char *path, uint32_t uid, uint32_t gid, ConnectionCache *cache = nullptr);
    FalconErrorCode Chmod(const char *path, uint32_t mode, ConnectionCache *cache = nullptr);
    FalconErrorCode Put(FormData_kv_index &kv_index, ConnectionCache *cache = nullptr);
    FalconErrorCode Get(FormData_kv_index &kv_index, ConnectionCache *cache = nullptr);
    FalconErrorCode Delete(std::string &key, ConnectionCache *cache = nullptr);
    FalconErrorCode SlicePut(const char *filename, std::vector<SliceInfo> &info, ConnectionCache *cache = nullptr);
    FalconErrorCode SliceGet(const char *filename, uint64_t inodeId, uint32_t chunkId, std::vector<SliceInfo> &info, ConnectionCache *cache = nullptr);
    FalconErrorCode SliceDel(const char *filename, uint64_t inodeId, uint32_t chunkId, ConnectionCache *cache = nullptr);
};
