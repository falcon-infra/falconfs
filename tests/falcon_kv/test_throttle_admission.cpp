// Verifies the v6 §12.5 admission control: when the inflight counter would
// exceed `max_inflight`, every item in the new batch is returned with
// `THROTTLED, retryable=true` and the engine is not invoked.
#include <gtest/gtest.h>

#include "kv_data_service.pb.h"
#include "vllm_kv_cache/src/store/kv_data_service_impl.h"

namespace falconfs::kv {

TEST(KVDataServiceImplThrottle, NewBatchExceedingMaxInflightReturnsThrottled) {
    KVDataServiceImpl svc;
    svc.SetMaxInflightForTest(2);
    svc.OccupyInflightForTest(2);  // simulate 2 concurrent in-flight items

    BatchWriteBlockRequest req;
    auto* a = req.add_items();
    a->set_block_hash("a");
    a->set_pool_offset(0);
    a->set_block_size(65536);
    a->set_expected_store_epoch(1);
    auto* b = req.add_items();
    b->set_block_hash("b");
    b->set_pool_offset(0);
    b->set_block_size(65536);
    b->set_expected_store_epoch(1);

    BatchWriteBlockResponse rsp;
    svc.BatchWriteBlock(req, &rsp);
    ASSERT_EQ(rsp.results_size(), 2);
    for (const auto& r : rsp.results()) {
        EXPECT_FALSE(r.result().success());
        EXPECT_EQ(r.result().error_code(), ErrorCode::THROTTLED);
        EXPECT_TRUE(r.result().retryable());
    }
    // Inflight not incremented when admission was rejected.
    EXPECT_EQ(svc.CurrentInflight(), 2);

    svc.ReleaseInflightForTest(2);
}

TEST(KVDataServiceImplThrottle, UnderLimitProceedsAndDecrementsInflight) {
    KVDataServiceImpl svc;
    svc.SetMaxInflightForTest(8);

    BatchWriteBlockRequest req;
    auto* item = req.add_items();
    item->set_block_hash("ok");
    item->set_pool_offset(0);
    item->set_payload("ok-bytes");
    item->set_block_size(65536);
    item->set_expected_store_epoch(1);
    item->set_expected_version(0);

    BatchWriteBlockResponse rsp;
    svc.BatchWriteBlock(req, &rsp);
    ASSERT_EQ(rsp.results_size(), 1);
    EXPECT_TRUE(rsp.results(0).result().success());
    EXPECT_EQ(svc.CurrentInflight(), 0);
}

TEST(KVDataServiceImplThrottle, ReadAndSsdPathsAlsoRespectAdmission) {
    KVDataServiceImpl svc;
    svc.SetMaxInflightForTest(1);
    svc.OccupyInflightForTest(1);

    BatchReadBlockRequest rreq;
    rreq.add_items()->set_block_hash("r");
    BatchReadBlockResponse rrsp;
    svc.BatchReadBlock(rreq, &rrsp);
    ASSERT_EQ(rrsp.results_size(), 1);
    EXPECT_EQ(rrsp.results(0).result().error_code(), ErrorCode::THROTTLED);

    BatchReadFromSSDRequest sreq;
    sreq.add_items()->set_block_hash("s");
    BatchReadFromSSDResponse srsp;
    svc.BatchReadFromSSD(sreq, &srsp);
    ASSERT_EQ(srsp.results_size(), 1);
    EXPECT_EQ(srsp.results(0).result().error_code(), ErrorCode::THROTTLED);

    svc.ReleaseInflightForTest(1);
}

}  // namespace falconfs::kv
