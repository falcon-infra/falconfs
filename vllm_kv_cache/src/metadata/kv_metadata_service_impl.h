// Compile-ready metadata service for KV cache v6 contracts.
//
// This class is transport-agnostic and consumes generated protobuf request/
// response messages directly. Mutating operations rely on catalog CAS and
// engine semantics (duplicate allocate → reused slot, version conflicts, etc.)
// rather than a separate response replay cache.
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "kv_metadata_service.pb.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"

namespace falconfs::kv {

class KVMetadataServiceImpl {
public:
    using ClockFn = std::function<int64_t()>;

    KVMetadataServiceImpl();
    explicit KVMetadataServiceImpl(std::shared_ptr<KVMetadataEngine> engine);
    KVMetadataServiceImpl(std::shared_ptr<KVMetadataEngine> engine, ClockFn clock);

    void BatchLookupWithLease(const BatchLookupRequest& request, BatchLookupResponse* response);

    // v6.4 pool-worker path: Pass-1 on in-process DRAM, single catalog sub-batch via
    // `run_catalog_sub_batch` (PostgreSQL backend executes FalconKvblock*), Pass-3
    // merges results and warms DRAM for STORED rows.
    void BatchLookupWithLeaseSplitForPoolWorker(
        const BatchLookupRequest& request,
        BatchLookupResponse* response,
        const std::function<std::string(const std::string& sub_batch_payload)>& run_catalog_sub_batch);
    void BatchAllocateWithLeaseSplitForPoolWorker(
        const BatchAllocateRequest& request,
        BatchAllocateResponse* response,
        const std::function<std::string(const std::string& sub_batch_payload)>& run_catalog_sub_batch);
    void BatchRenewLeaseSplitForPoolWorker(
        const BatchRenewLeaseRequest& request,
        BatchRenewLeaseResponse* response,
        const std::function<std::string(const std::string& sub_batch_payload)>& run_catalog_sub_batch);
    void BatchUpdateBlockStatusSplitForPoolWorker(
        const BatchUpdateStatusRequest& request,
        BatchUpdateStatusResponse* response,
        const std::function<std::string(const std::string& sub_batch_payload)>& run_catalog_sub_batch);
    void BatchFreeAllocatedSplitForPoolWorker(
        const BatchFreeAllocatedRequest& request,
        BatchFreeAllocatedResponse* response,
        const std::function<std::string(const std::string& sub_batch_payload)>& run_catalog_sub_batch);
    void BatchAllocateWithLease(const BatchAllocateRequest& request, BatchAllocateResponse* response);
    void BatchRenewLease(const BatchRenewLeaseRequest& request, BatchRenewLeaseResponse* response);
    void BatchUpdateBlockStatus(const BatchUpdateStatusRequest& request,
                                BatchUpdateStatusResponse* response);
    void BatchFreeAllocated(const BatchFreeAllocatedRequest& request,
                            BatchFreeAllocatedResponse* response);

private:
    int64_t Now() const;

    static void FillResultMeta(const EngineResultMeta& src, ItemResultMeta* dst);
    static void FillLocation(const EngineBlockLocation& src, BlockLocation* dst);
    static void FillLease(const EngineLeaseInfo& src, LeaseInfo* dst);
    static void FillLookupResultFromEngine(const EngineLookupResult& src, LookupResult* dst);
    static void FillAllocateResultFromEngine(const EngineAllocateResult& src, AllocateResult* dst);

    std::shared_ptr<KVMetadataEngine> engine_;
    ClockFn clock_;
};

}  // namespace falconfs::kv
