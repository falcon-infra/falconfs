// brpc-compatible adapter for the KV store admin service (v6.5 §5.4).
//
// Hosted on each DN bgworker for {RegisterStoreRegion, Heartbeat} and on
// each falcon_kv_store daemon for {SpillBlockToSSD}. The same generated
// abstract base class is used on both sides; methods that are not hosted
// on a given side respond with INTERNAL_ERROR ("not hosted on this node"),
// keeping the wire surface single-service per design §5.4.
//
// Phase P0 lands ONLY this skeleton header (and the proto). The DN-side
// concrete impl wires up in P1 (idempotent Register/Heartbeat) and the
// Store-side SpillBlockToSSD wires up in P2 (within the falcon_kv_store
// daemon). No `.cpp` is added in P0 to keep behaviour unchanged.
#pragma once

#include <functional>
#include <memory>

#include "kv_store_admin_service.pb.h"

namespace falconfs::kv {

class KVMetadataEngine;        // for region registration on the DN side
class KVStoreEngine;           // for SpillBlockToSSD on the Store side
class StoreRegionRegistry;     // DN-local mirror of registered regions

// Abstract policy for the two host roles (DN bgworker vs. falcon_kv_store
// daemon). The adapter just dispatches into the impl; the impl decides
// what is "hosted here" and what is INTERNAL_ERROR.
class KVStoreAdminBrpcServiceImplBase {
public:
    virtual ~KVStoreAdminBrpcServiceImplBase() = default;

    virtual void RegisterStoreRegion(const RegisterStoreRegionRequest& req,
                                     RegisterStoreRegionResponse* resp) = 0;
    virtual void Heartbeat(const HeartbeatRequest& req,
                           HeartbeatResponse* resp) = 0;
    virtual void SpillBlockToSSD(const SpillBlockToSSDRequest& req,
                                 SpillBlockToSSDResponse* resp) = 0;
    virtual void ValidateEvictedPaths(const ValidateEvictedPathsRequest& req,
                                      ValidateEvictedPathsResponse* resp) = 0;
};

// brpc adapter: translates google::protobuf::Closure-style callbacks into
// the synchronous impl API. Identical pattern to KVDataBrpcServiceAdapter
// (see kv_data_brpc_service.h).
class KVStoreAdminBrpcServiceAdapter : public KVStoreAdminService {
public:
    explicit KVStoreAdminBrpcServiceAdapter(
        std::shared_ptr<KVStoreAdminBrpcServiceImplBase> impl);
    ~KVStoreAdminBrpcServiceAdapter() override = default;

    void RegisterStoreRegion(::google::protobuf::RpcController* controller,
                             const RegisterStoreRegionRequest* request,
                             RegisterStoreRegionResponse* response,
                             ::google::protobuf::Closure* done) override;

    void Heartbeat(::google::protobuf::RpcController* controller,
                   const HeartbeatRequest* request,
                   HeartbeatResponse* response,
                   ::google::protobuf::Closure* done) override;

    void SpillBlockToSSD(::google::protobuf::RpcController* controller,
                         const SpillBlockToSSDRequest* request,
                         SpillBlockToSSDResponse* response,
                         ::google::protobuf::Closure* done) override;

    void ValidateEvictedPaths(::google::protobuf::RpcController* controller,
                              const ValidateEvictedPathsRequest* request,
                              ValidateEvictedPathsResponse* response,
                              ::google::protobuf::Closure* done) override;

private:
    std::shared_ptr<KVStoreAdminBrpcServiceImplBase> impl_;
};

/** DN bgworker implementation: RegisterStoreRegion + Heartbeat; SpillBlockToSSD
 * returns INTERNAL_ERROR ("not hosted on this node"). Optional
 * `after_region_registered` runs after each new successful RegisterStoreRegion
 * (used to replay parked recovery rows once geometry exists).
 * `after_store_epoch_bump` runs after runtime fencing for same-geometry Store
 * restart so the DN can reconcile durable catalog rows. */
std::shared_ptr<KVStoreAdminBrpcServiceImplBase> CreateKVStoreAdminBrpcServiceImplForDn(
    std::shared_ptr<KVMetadataEngine> engine,
    std::function<void()> after_region_registered = nullptr,
    std::function<void(int32_t, const std::string&)> after_store_epoch_bump = nullptr);

/** falcon_kv_store daemon: SpillBlockToSSD only; other RPCs return INTERNAL_ERROR. */
std::shared_ptr<KVStoreAdminBrpcServiceImplBase> CreateKVStoreAdminBrpcServiceImplForStore(
    std::shared_ptr<KVStoreEngine> engine);

}  // namespace falconfs::kv
