// Verifies that `KVMetadataBrpcServiceAdapter` and `KVDataBrpcServiceAdapter`
// fulfill the protobuf-generated abstract Service contract (which is exactly
// what brpc::Server registers): each RPC method must run the request through
// the underlying impl, populate the response, and invoke the closure.
//
// We do not link brpc here; instead we drive the adapters through the same
// `google::protobuf::Service` interface that brpc would use, with a simple
// flag-setting closure.
#include <gtest/gtest.h>

#include <google/protobuf/service.h>

#include <memory>

#include "kv_data_service.pb.h"
#include "kv_metadata_service.pb.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_service_impl.h"
#include "vllm_kv_cache/src/service/kv_data_brpc_service.h"
#include "vllm_kv_cache/src/service/kv_metadata_brpc_service.h"
#include "vllm_kv_cache/src/store/kv_data_service_impl.h"

namespace falconfs::kv {

namespace {

class FlagClosure : public ::google::protobuf::Closure {
public:
    void Run() override { ran_ = true; }
    bool Ran() const { return ran_; }

private:
    bool ran_ = false;
};

}  // namespace

TEST(KVMetadataBrpcServiceAdapter, AllocateThenLookupSucceedsThroughServiceAbi) {
    auto engine = std::make_shared<KVMetadataEngine>();
    auto impl = std::make_shared<KVMetadataServiceImpl>(engine);
    KVMetadataBrpcServiceAdapter adapter(impl);

    BatchAllocateRequest alloc_req;
    auto* item = alloc_req.add_items();
    item->set_block_hash("brpc-k1");
    item->set_block_size(65536);

    BatchAllocateResponse alloc_rsp;
    FlagClosure alloc_done;
    adapter.BatchAllocateWithLease(/*controller=*/nullptr, &alloc_req, &alloc_rsp, &alloc_done);
    EXPECT_TRUE(alloc_done.Ran());
    ASSERT_EQ(alloc_rsp.results_size(), 1);
    ASSERT_TRUE(alloc_rsp.results(0).result().success());

    BatchUpdateStatusRequest stored_req;
    auto* su = stored_req.add_items();
    su->set_block_hash("brpc-k1");
    su->set_expected_from_status(BlockStatus::BLOCK_STATUS_ALLOCATED);
    su->set_to_status(BlockStatus::BLOCK_STATUS_STORED);
    su->set_expected_version(1);
    BatchUpdateStatusResponse stored_rsp;
    FlagClosure stored_done;
    adapter.BatchUpdateBlockStatus(nullptr, &stored_req, &stored_rsp, &stored_done);
    EXPECT_TRUE(stored_done.Ran());
    ASSERT_TRUE(stored_rsp.results(0).result().success());

    BatchLookupRequest lookup_req;
    auto* li = lookup_req.add_items();
    li->set_block_hash("brpc-k1");
    li->set_renew_lease_on_hit(true);
    BatchLookupResponse lookup_rsp;
    FlagClosure lookup_done;
    adapter.BatchLookupWithLease(nullptr, &lookup_req, &lookup_rsp, &lookup_done);
    EXPECT_TRUE(lookup_done.Ran());
    ASSERT_EQ(lookup_rsp.results_size(), 1);
    EXPECT_TRUE(lookup_rsp.results(0).result().success());
    EXPECT_EQ(lookup_rsp.results(0).status(), BlockStatus::BLOCK_STATUS_STORED);
    EXPECT_GT(lookup_rsp.results(0).lease().lease_token(), 0);
}

TEST(KVMetadataBrpcServiceAdapter, SameRequestIdDoesNotAliasBlocksThroughAdapter) {
    auto engine = std::make_shared<KVMetadataEngine>();
    auto impl = std::make_shared<KVMetadataServiceImpl>(engine);
    KVMetadataBrpcServiceAdapter adapter(impl);

    BatchAllocateRequest req1;
    req1.mutable_meta()->set_request_id("brpc-replay");
    req1.mutable_meta()->set_client_id(7);
    auto* a1 = req1.add_items();
    a1->set_block_hash("brpc-replay-1");
    a1->set_block_size(65536);
    BatchAllocateResponse rsp1;
    FlagClosure done1;
    adapter.BatchAllocateWithLease(nullptr, &req1, &rsp1, &done1);
    EXPECT_TRUE(done1.Ran());
    ASSERT_TRUE(rsp1.results(0).result().success());

    BatchAllocateRequest req2;
    req2.mutable_meta()->set_request_id("brpc-replay");
    req2.mutable_meta()->set_client_id(7);
    auto* a2 = req2.add_items();
    a2->set_block_hash("brpc-replay-2");
    a2->set_block_size(65536);
    BatchAllocateResponse rsp2;
    FlagClosure done2;
    adapter.BatchAllocateWithLease(nullptr, &req2, &rsp2, &done2);
    EXPECT_TRUE(done2.Ran());
    ASSERT_TRUE(rsp2.results(0).result().success());
    EXPECT_EQ(rsp2.results(0).block_hash(), "brpc-replay-2");
}

TEST(KVDataBrpcServiceAdapter, WriteThenReadRoundtripThroughServiceAbi) {
    auto impl = std::make_shared<KVDataServiceImpl>();
    KVDataBrpcServiceAdapter adapter(impl);

    BatchWriteBlockRequest wreq;
    auto* w = wreq.add_items();
    w->set_block_hash("brpc-d1");
    w->set_pool_offset(0);
    w->set_payload("brpc-bytes");
    w->set_block_size(65536);
    w->set_expected_store_epoch(1);
    w->set_expected_version(0);
    BatchWriteBlockResponse wrsp;
    FlagClosure wdone;
    adapter.BatchWriteBlock(nullptr, &wreq, &wrsp, &wdone);
    EXPECT_TRUE(wdone.Ran());
    ASSERT_TRUE(wrsp.results(0).result().success());

    BatchReadBlockRequest rreq;
    auto* r = rreq.add_items();
    r->set_block_hash("brpc-d1");
    r->set_pool_offset(0);
    r->set_block_size(10);
    r->set_expected_version(0);
    r->set_expected_store_epoch(1);
    BatchReadBlockResponse rrsp;
    FlagClosure rdone;
    adapter.BatchReadBlock(nullptr, &rreq, &rrsp, &rdone);
    EXPECT_TRUE(rdone.Ran());
    ASSERT_EQ(rrsp.results_size(), 1);
    EXPECT_TRUE(rrsp.results(0).result().success());
    EXPECT_EQ(rrsp.results(0).payload(), "brpc-bytes");
}

}  // namespace falconfs::kv
