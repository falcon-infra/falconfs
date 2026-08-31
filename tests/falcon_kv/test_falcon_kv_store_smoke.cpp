#include <gtest/gtest.h>

#include <memory>
#include <cstdlib>
#include <fstream>

#include "vllm_kv_cache/src/service/kv_store_admin_brpc_service.h"
#include "vllm_kv_cache/src/store/kv_store_engine.h"
#include "vllm_kv_cache/src/store/ssd_spill_manager.h"

namespace falconfs::kv {

TEST(FalconKvStoreSmoke, StoreAdminSpillFailsWithoutSpillManager) {
    auto engine = std::make_shared<KVStoreEngine>(/*store_node_id=*/1,
                                                   /*base_offset=*/0,
                                                   /*region_bytes=*/64LL * 65536LL,
                                                   /*block_size=*/65536,
                                                   /*store_epoch=*/1);
    auto admin = CreateKVStoreAdminBrpcServiceImplForStore(engine);

    SpillBlockToSSDRequest req;
    SpillBlockToSSDResponse resp;
    req.mutable_meta()->set_request_id("ut_spill");
    req.set_store_node_id(1);
    req.set_pool_offset(0);
    req.set_block_hash("nope");
    req.set_expected_version(1);
    req.set_expected_store_epoch(1);
    admin->SpillBlockToSSD(req, &resp);
    EXPECT_FALSE(resp.result().success());
}


TEST(FalconKvStoreSmoke, ValidateEvictedPathsChecksStoreLocalSSD) {
    const std::string root = "/tmp/falcon_kv_validate_evicted_ut";
    const std::string good = root + "/1/aa/good.kv";
    ASSERT_EQ(::system(("mkdir -p " + root + "/1/aa").c_str()), 0);
    { std::ofstream f(good, std::ios::binary); f << "payload"; }

    auto engine = std::make_shared<KVStoreEngine>(/*store_node_id=*/1,
                                                   /*base_offset=*/0,
                                                   /*region_bytes=*/64LL * 65536LL,
                                                   /*block_size=*/65536,
                                                   /*store_epoch=*/1);
    engine->SetSSDSpillManager(std::make_shared<SSDSpillManager>(root));
    auto admin = CreateKVStoreAdminBrpcServiceImplForStore(engine);

    ValidateEvictedPathsRequest req;
    ValidateEvictedPathsResponse resp;
    req.mutable_meta()->set_request_id("ut_validate");
    req.set_store_node_id(1);
    auto* ok = req.add_items();
    ok->set_block_hash("good");
    ok->set_evicted_path(good);
    auto* bad = req.add_items();
    bad->set_block_hash("bad");
    bad->set_evicted_path(root + "/1/aa/missing.kv");

    admin->ValidateEvictedPaths(req, &resp);
    ASSERT_TRUE(resp.result().success());
    ASSERT_EQ(resp.results_size(), 2);
    EXPECT_TRUE(resp.results(0).valid());
    EXPECT_FALSE(resp.results(1).valid());
    EXPECT_EQ(resp.valid_count(), 1);
    EXPECT_EQ(resp.invalid_count(), 1);
}

}  // namespace falconfs::kv
