#include <gtest/gtest.h>

#include "vllm_kv_cache/src/store/kv_data_service_impl.h"

namespace falconfs::kv {

TEST(KVDataServiceImpl, WriteStubReturnsPerItemInternalError) {
    KVDataServiceImpl svc;
    BatchWriteBlockRequest req;
    auto* item = req.add_items();
    item->set_block_hash("w1");
    item->set_pool_offset(0);
    item->set_payload("abc");
    item->set_block_size(65536);
    item->set_expected_store_epoch(1);
    item->set_expected_version(0);

    BatchWriteBlockResponse rsp;
    svc.BatchWriteBlock(req, &rsp);

    ASSERT_EQ(rsp.results_size(), 1);
    EXPECT_TRUE(rsp.results(0).result().success());
    EXPECT_EQ(rsp.results(0).bytes_written(), 3);
    EXPECT_GT(rsp.server_time_ms(), 0);
}

TEST(KVDataServiceImpl, ReadStubReturnsPerItemInternalError) {
    KVDataServiceImpl svc;
    BatchWriteBlockRequest wreq;
    auto* wi = wreq.add_items();
    wi->set_block_hash("r1");
    wi->set_pool_offset(0);
    wi->set_payload("hello");
    wi->set_block_size(65536);
    wi->set_expected_store_epoch(1);
    wi->set_expected_version(0);
    BatchWriteBlockResponse wrsp;
    svc.BatchWriteBlock(wreq, &wrsp);
    ASSERT_TRUE(wrsp.results(0).result().success());

    BatchReadBlockRequest req;
    auto* ri = req.add_items();
    ri->set_block_hash("r1");
    ri->set_pool_offset(0);
    ri->set_block_size(5);
    ri->set_expected_version(0);
    ri->set_expected_store_epoch(1);

    BatchReadBlockResponse rsp;
    svc.BatchReadBlock(req, &rsp);

    ASSERT_EQ(rsp.results_size(), 1);
    EXPECT_TRUE(rsp.results(0).result().success());
    EXPECT_EQ(rsp.results(0).payload(), "hello");
}

TEST(KVDataServiceImpl, SSDReadStubReturnsPerItemInternalError) {
    auto engine = std::make_shared<KVStoreEngine>();
    engine->PutSSDForTest("s1",
                          "/tmp/falconfs/evicted/s1.bin",
                          "ssd-payload",
                          static_cast<int32_t>(CompressionType::COMPRESSION_NONE),
                          11,
                          /*version=*/7);
    KVDataServiceImpl svc(engine);

    BatchReadFromSSDRequest req;
    auto* s = req.add_items();
    s->set_block_hash("s1");
    s->set_evicted_path("/tmp/falconfs/evicted/s1.bin");
    s->set_expected_version(7);

    BatchReadFromSSDResponse rsp;
    svc.BatchReadFromSSD(req, &rsp);

    ASSERT_EQ(rsp.results_size(), 1);
    const auto& item = rsp.results(0);
    EXPECT_TRUE(item.result().success());
    EXPECT_EQ(item.payload(), "ssd-payload");
}

TEST(KVDataServiceImpl, WriteRejectsStaleEpoch) {
    KVDataServiceImpl svc;
    BatchWriteBlockRequest req;
    auto* item = req.add_items();
    item->set_block_hash("w2");
    item->set_pool_offset(0);
    item->set_payload("abc");
    item->set_block_size(65536);
    item->set_expected_store_epoch(2);
    item->set_expected_version(0);

    BatchWriteBlockResponse rsp;
    svc.BatchWriteBlock(req, &rsp);
    ASSERT_EQ(rsp.results_size(), 1);
    EXPECT_FALSE(rsp.results(0).result().success());
    EXPECT_EQ(rsp.results(0).result().error_code(), ErrorCode::STALE_EPOCH);
}

}  // namespace falconfs::kv
