/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * v6.4 \u00a74.1.1 KV cache job class hierarchy. Mirrors the FalconFS metadata
 * BaseMetaServiceJob / BrpcMetaServiceJob pattern so KV jobs flow through the
 * existing PGConnectionPool: BRPC handler builds a job, dispatches via
 * `FalconDispatchMetaJob2PGConnectionPool`, the pool routes the job onto its
 * KV-specific queue (`kvTaskList`), and a `KVCacheWorkerTask` picks it up on
 * a pool-worker thread that owns its own libpq connection. Multiple workers
 * run concurrently, each with a dedicated PG backend, so catalog round-trips
 * fan out across many backends rather than serializing through one.
 */
#ifndef BASE_KV_CACHE_SERVICE_JOB_H
#define BASE_KV_CACHE_SERVICE_JOB_H

#include <cstring>
#include <string>

#include "base_comm_adapter/base_meta_service_job.h"

/* Method ids that match v6.4 \u00a74.1.1's "FalconKVServiceMethod" mapping table.
 * Stable numeric values are used on the wire (job dispatch + libpq round-trip
 * argument) so the BRPC plugin and the pool-worker side agree without having
 * to share a protobuf descriptor. */
enum class FalconKVServiceMethod : int {
    INVALID                   = 0,
    BATCH_LOOKUP_WITH_LEASE   = 1,
    BATCH_ALLOCATE_WITH_LEASE = 2,
    BATCH_RENEW_LEASE         = 3,
    BATCH_UPDATE_BLOCK_STATUS = 4,
    BATCH_FREE_ALLOCATED      = 5,
};

/* A KV cache job carries one BRPC sub-batch end-to-end: a serialized protobuf
 * request (the full BRPC message), a slot for the serialized response, and a
 * Done() hook that propagates the response back to the BRPC closure.
 *
 * The class extends BaseMetaServiceJob so it shares the existing pool dispatch
 * mechanism, but the meta-service callbacks (CopyOutData / GetReqDatasize /
 * GetReqServiceCnt / GetFalconMetaServiceType / IsAllowBatchProcess /
 * IsEmptyRequest) are stubbed out to no-ops because the KV path goes through
 * `KVCacheWorkerTask`, not `BatchWorkerTask`. */
class BaseKVCacheServiceJob : public BaseMetaServiceJob {
public:
    BaseKVCacheServiceJob() : BaseMetaServiceJob() {}
    ~BaseKVCacheServiceJob() override = default;

    bool IsKVCacheServiceJob() const override { return true; }

    /* KV-specific accessors used by KVCacheWorkerTask. */
    virtual FalconKVServiceMethod GetMethod() const = 0;
    virtual const std::string &GetSerializedRequest() const = 0;
    virtual void SetSerializedResponse(std::string resp) = 0;

    /* BaseMetaServiceJob overrides \u2014 not relevant on the KV path; the worker
     * never goes through BatchWorkerTask so these never get called. */
    bool IsAllowBatchProcess() override { return false; }
    bool IsEmptyRequest() override { return false; }
    int GetReqServiceCnt() override { return 1; }
    size_t GetReqDatasize() override { return GetSerializedRequest().size(); }
    size_t CopyOutData(void *dst, size_t dstSize) override
    {
        const std::string &r = GetSerializedRequest();
        size_t n = (r.size() < dstSize) ? r.size() : dstSize;
        if (n > 0 && dst != nullptr) {
            memcpy(dst, r.data(), n);
        }
        return n;
    }
    FalconMetaServiceType GetFalconMetaServiceType(int /*index*/) override { return NOT_SUPPORTED; }
    void ProcessResponse(void * /*data*/, size_t /*size*/, FalDataDeleter /*deleter*/) override {}
};

#endif  // BASE_KV_CACHE_SERVICE_JOB_H
