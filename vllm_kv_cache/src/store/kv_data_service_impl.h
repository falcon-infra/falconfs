// Compile-ready Store data service for KV cache v6 contracts.
//
// Implements the contract from kv_data_service.proto on top of `KVStoreEngine`
// and an inflight admission counter (v6 §12.5). When the inflight counter
// would exceed `max_inflight_`, all items in the incoming batch are returned
// as `THROTTLED, retryable=true`, matching the v6 §10.4 error model.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "kv_data_service.pb.h"
#include "vllm_kv_cache/src/store/kv_store_engine.h"

namespace falconfs::kv {

class KVDataServiceImpl {
public:
    KVDataServiceImpl();
    explicit KVDataServiceImpl(std::shared_ptr<KVStoreEngine> engine);

    void WriteBlock(const WriteBlockRequest& request, WriteBlockResponse* response);
    void WriteBlockPayload(const WriteBlockRequest& request,
                           const std::string& payload,
                           WriteBlockResponse* response);
    void ReadBlock(const ReadBlockRequest& request, ReadBlockResponse* response);
    void ReadFromSSD(const ReadFromSSDRequest& request, ReadFromSSDResponse* response);

    void BatchWriteBlock(const BatchWriteBlockRequest& request, BatchWriteBlockResponse* response);
    void BatchWriteBlockPayloads(const BatchWriteBlockRequest& request,
                                 const std::vector<std::string>& payloads,
                                 BatchWriteBlockResponse* response);
    void BatchReadBlock(const BatchReadBlockRequest& request, BatchReadBlockResponse* response);
    void BatchReadFromSSD(const BatchReadFromSSDRequest& request, BatchReadFromSSDResponse* response);

    // Admission control knobs. `max_inflight=0` means unlimited (default).
    void SetMaxInflightForTest(int max_inflight);
    int CurrentInflight() const;

    // Test hook to simulate concurrent in-flight requests.
    void OccupyInflightForTest(int n);
    void ReleaseInflightForTest(int n);

    static void FillResultMeta(const StoreResultMeta& src, ItemResultMeta* dst);

private:
    static int64_t NowMs();

    bool TryAdmitCount(int n_items);

    template <typename Request, typename Response>
    bool TryAdmit(const Request& request, Response* response);

    void ReleaseAdmission(int items);

    std::shared_ptr<KVStoreEngine> engine_;
    std::atomic<int> inflight_{0};
    int max_inflight_ = 0;
};

template <typename Request, typename Response>
bool KVDataServiceImpl::TryAdmit(const Request& request, Response* response) {
    const int n_items = request.items_size();
    if (n_items <= 0) {
        return true;
    }
    if (!TryAdmitCount(n_items)) {
        response->clear_results();
        for (const auto& item : request.items()) {
            auto* r = response->add_results();
            r->set_block_hash(item.block_hash());
            auto* m = r->mutable_result();
            m->set_success(false);
            m->set_error_code(ErrorCode::THROTTLED);
            m->set_retryable(true);
            m->set_error_message("store inflight queue full");
        }
        response->set_server_time_ms(NowMs());
        return false;
    }
    return true;
}

}  // namespace falconfs::kv
