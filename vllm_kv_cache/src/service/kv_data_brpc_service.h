// brpc-compatible adapter for the Store data service. Mirrors the metadata
// adapter pattern: bridges the protobuf-generated `KVDataService` abstract
// base to `KVDataServiceImpl`.
#pragma once

#include <memory>

#include "kv_data_service.pb.h"
#include "vllm_kv_cache/src/store/kv_data_service_impl.h"

namespace falconfs::kv {

class KVDataBrpcServiceAdapter : public KVDataService {
public:
    explicit KVDataBrpcServiceAdapter(std::shared_ptr<KVDataServiceImpl> impl);
    ~KVDataBrpcServiceAdapter() override = default;

    void BatchWriteBlock(::google::protobuf::RpcController* controller,
                         const BatchWriteBlockRequest* request,
                         BatchWriteBlockResponse* response,
                         ::google::protobuf::Closure* done) override;

    void WriteBlock(::google::protobuf::RpcController* controller,
                    const WriteBlockRequest* request,
                    WriteBlockResponse* response,
                    ::google::protobuf::Closure* done) override;

    void ReadBlock(::google::protobuf::RpcController* controller,
                   const ReadBlockRequest* request,
                   ReadBlockResponse* response,
                   ::google::protobuf::Closure* done) override;

    void ReadFromSSD(::google::protobuf::RpcController* controller,
                     const ReadFromSSDRequest* request,
                     ReadFromSSDResponse* response,
                     ::google::protobuf::Closure* done) override;

    void BatchReadBlock(::google::protobuf::RpcController* controller,
                        const BatchReadBlockRequest* request,
                        BatchReadBlockResponse* response,
                        ::google::protobuf::Closure* done) override;

    void BatchReadFromSSD(::google::protobuf::RpcController* controller,
                          const BatchReadFromSSDRequest* request,
                          BatchReadFromSSDResponse* response,
                          ::google::protobuf::Closure* done) override;

private:
    std::shared_ptr<KVDataServiceImpl> impl_;
};

}  // namespace falconfs::kv
