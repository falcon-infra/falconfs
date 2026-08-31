#include "vllm_kv_cache/src/store/kv_data_service_impl.h"

#include <chrono>
#include <utility>

namespace falconfs::kv {

KVDataServiceImpl::KVDataServiceImpl()
    : engine_(std::make_shared<KVStoreEngine>()) {}

KVDataServiceImpl::KVDataServiceImpl(std::shared_ptr<KVStoreEngine> engine)
    : engine_(std::move(engine)) {}

int64_t KVDataServiceImpl::NowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

void KVDataServiceImpl::FillResultMeta(const StoreResultMeta& src, ItemResultMeta* dst) {
    dst->set_success(src.success);
    dst->set_error_code(static_cast<ErrorCode>(src.error_code));
    dst->set_retryable(src.retryable);
    dst->set_error_message(src.error_message);
}

void KVDataServiceImpl::SetMaxInflightForTest(int max_inflight) { max_inflight_ = max_inflight; }
int KVDataServiceImpl::CurrentInflight() const { return inflight_.load(); }
void KVDataServiceImpl::OccupyInflightForTest(int n) { inflight_.fetch_add(n); }
void KVDataServiceImpl::ReleaseInflightForTest(int n) { inflight_.fetch_sub(n); }

bool KVDataServiceImpl::TryAdmitCount(int n_items) {
    if (n_items <= 0) {
        return true;
    }
    if (max_inflight_ <= 0) {
        inflight_.fetch_add(n_items);
        return true;
    }
    const int prev = inflight_.fetch_add(n_items);
    if (prev + n_items > max_inflight_) {
        inflight_.fetch_sub(n_items);
        return false;
    }
    return true;
}

void KVDataServiceImpl::ReleaseAdmission(int items) {
    if (items > 0) inflight_.fetch_sub(items);
}

namespace {

void SetThrottledMeta(ItemResultMeta* m) {
    m->set_success(false);
    m->set_error_code(ErrorCode::THROTTLED);
    m->set_retryable(true);
    m->set_error_message("store inflight queue full");
}

void ExecuteOneWritePayload(KVStoreEngine* engine,
                            const WriteItem& item,
                            const std::string& payload,
                            bool verify_checksum,
                            WriteResult* result) {
    result->set_block_hash(item.block_hash());
    StoreWriteResult out = engine->Write(item.block_hash(),
                                         item.pool_offset(),
                                         payload,
                                         static_cast<int32_t>(item.compression()),
                                         item.original_size(),
                                         item.block_size(),
                                         item.expected_version(),
                                         item.expected_store_epoch(),
                                         verify_checksum,
                                         item.crc32());
    KVDataServiceImpl::FillResultMeta(out.result, result->mutable_result());
    result->set_bytes_written(out.bytes_written);
    result->set_crc32(out.crc32);
}

void ExecuteOneWrite(KVStoreEngine* engine,
                     const WriteItem& item,
                     bool verify_checksum,
                     WriteResult* result) {
    ExecuteOneWritePayload(engine, item, item.payload(), verify_checksum, result);
}

void ExecuteOneRead(KVStoreEngine* engine, const ReadItem& item, ReadResult* result) {
    result->set_block_hash(item.block_hash());
    StoreReadResult out = engine->Read(item.block_hash(),
                                       item.pool_offset(),
                                       item.block_size(),
                                       item.expected_version(),
                                       item.expected_store_epoch());
    KVDataServiceImpl::FillResultMeta(out.result, result->mutable_result());
    result->set_payload(out.payload);
    result->set_crc32(out.crc32);
    result->set_compression(static_cast<CompressionType>(out.compression));
    result->set_original_size(out.original_size);
}

void ExecuteOneReadSSD(KVStoreEngine* engine, const SSDReadItem& item, SSDReadResult* result) {
    result->set_block_hash(item.block_hash());
    StoreReadResult out =
        engine->ReadFromSSD(item.block_hash(), item.evicted_path(), item.expected_version());
    KVDataServiceImpl::FillResultMeta(out.result, result->mutable_result());
    result->set_payload(out.payload);
    result->set_crc32(out.crc32);
    result->set_compression(static_cast<CompressionType>(out.compression));
    result->set_original_size(out.original_size);
}

}  // namespace

void KVDataServiceImpl::WriteBlock(const WriteBlockRequest& request, WriteBlockResponse* response) {
    WriteBlockPayload(request, request.item().payload(), response);
}

void KVDataServiceImpl::WriteBlockPayload(const WriteBlockRequest& request,
                                          const std::string& payload,
                                          WriteBlockResponse* response) {
    constexpr int kOne = 1;
    if (!TryAdmitCount(kOne)) {
        auto* r = response->mutable_result();
        r->set_block_hash(request.item().block_hash());
        SetThrottledMeta(r->mutable_result());
        response->set_server_time_ms(NowMs());
        return;
    }
    const int64_t now_ms = NowMs();
    ExecuteOneWritePayload(engine_.get(), request.item(), payload, request.verify_checksum(), response->mutable_result());
    response->set_server_time_ms(now_ms);
    ReleaseAdmission(kOne);
}

void KVDataServiceImpl::ReadBlock(const ReadBlockRequest& request, ReadBlockResponse* response) {
    constexpr int kOne = 1;
    if (!TryAdmitCount(kOne)) {
        auto* r = response->mutable_result();
        r->set_block_hash(request.item().block_hash());
        SetThrottledMeta(r->mutable_result());
        response->set_server_time_ms(NowMs());
        return;
    }
    const int64_t now_ms = NowMs();
    ExecuteOneRead(engine_.get(), request.item(), response->mutable_result());
    (void)request.return_compressed();
    response->set_server_time_ms(now_ms);
    ReleaseAdmission(kOne);
}

void KVDataServiceImpl::ReadFromSSD(const ReadFromSSDRequest& request, ReadFromSSDResponse* response) {
    constexpr int kOne = 1;
    if (!TryAdmitCount(kOne)) {
        auto* r = response->mutable_result();
        r->set_block_hash(request.item().block_hash());
        SetThrottledMeta(r->mutable_result());
        response->set_server_time_ms(NowMs());
        return;
    }
    const int64_t now_ms = NowMs();
    ExecuteOneReadSSD(engine_.get(), request.item(), response->mutable_result());
    (void)request.return_compressed();
    response->set_server_time_ms(now_ms);
    ReleaseAdmission(kOne);
}

void KVDataServiceImpl::BatchWriteBlock(const BatchWriteBlockRequest& request,
                                        BatchWriteBlockResponse* response) {
    if (!TryAdmit(request, response)) return;
    response->clear_results();
    const int64_t now_ms = NowMs();
    for (const auto& item : request.items()) {
        auto* wr = response->add_results();
        ExecuteOneWrite(engine_.get(), item, request.verify_checksum(), wr);
    }
    response->set_server_time_ms(now_ms);
    ReleaseAdmission(request.items_size());
}

void KVDataServiceImpl::BatchWriteBlockPayloads(const BatchWriteBlockRequest& request,
                                                const std::vector<std::string>& payloads,
                                                BatchWriteBlockResponse* response) {
    if (!TryAdmit(request, response)) return;
    response->clear_results();
    const int64_t now_ms = NowMs();
    for (int i = 0; i < request.items_size(); ++i) {
        const auto& item = request.items(i);
        const std::string& payload = (i < static_cast<int>(payloads.size()))
                                         ? payloads[static_cast<size_t>(i)]
                                         : item.payload();
        auto* wr = response->add_results();
        ExecuteOneWritePayload(engine_.get(), item, payload, request.verify_checksum(), wr);
    }
    response->set_server_time_ms(now_ms);
    ReleaseAdmission(request.items_size());
}

void KVDataServiceImpl::BatchReadBlock(const BatchReadBlockRequest& request,
                                       BatchReadBlockResponse* response) {
    if (!TryAdmit(request, response)) return;
    response->clear_results();
    const int64_t now_ms = NowMs();
    for (const auto& item : request.items()) {
        auto* rr = response->add_results();
        ExecuteOneRead(engine_.get(), item, rr);
    }
    (void)request.return_compressed();
    response->set_server_time_ms(now_ms);
    ReleaseAdmission(request.items_size());
}

void KVDataServiceImpl::BatchReadFromSSD(const BatchReadFromSSDRequest& request,
                                         BatchReadFromSSDResponse* response) {
    if (!TryAdmit(request, response)) return;
    response->clear_results();
    const int64_t now_ms = NowMs();
    for (const auto& item : request.items()) {
        auto* sr = response->add_results();
        ExecuteOneReadSSD(engine_.get(), item, sr);
    }
    (void)request.return_compressed();
    response->set_server_time_ms(now_ms);
    ReleaseAdmission(request.items_size());
}

}  // namespace falconfs::kv
