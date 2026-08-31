// v6.5 P3: DN eviction spill path calls KVStoreAdminService::SpillBlockToSSD on
// falcon_kv.store_spill_endpoint. This test exercises the store-side BRPC
// surface the worker hits (same proto/stub path as production).
#include <gtest/gtest.h>

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/endpoint.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>

#include "kv_store_admin_service.pb.h"
#include "vllm_kv_cache/src/service/kv_store_admin_brpc_service.h"
#include "vllm_kv_cache/src/store/kv_store_engine.h"
#include "vllm_kv_cache/src/store/ssd_spill_manager.h"

namespace falconfs::kv {

namespace fs = std::filesystem;

TEST(KvEvictionWorkerBrpc, SpillBlockToSSDOverBrpcSucceedsAgainstStore) {
    const auto root =
        (fs::temp_directory_path() / ("kv_ev_brpc_ok_" + std::to_string(::getpid()))).string();
    fs::create_directories(root);
    auto spill = std::make_shared<SSDSpillManager>(root);

    auto engine = std::make_shared<KVStoreEngine>(/*store_node_id=*/1,
                                                  /*base_offset=*/0,
                                                  /*region_bytes=*/64LL * 65536LL,
                                                  /*block_size=*/65536,
                                                  /*store_epoch=*/1);
    engine->SetSSDSpillManager(spill);
    ASSERT_TRUE(engine
                    ->Write("spill-brpc-ok",
                            /*pool_offset=*/0,
                            "payload-brpc",
                            /*compression=*/0,
                            /*original_size=*/12,
                            /*block_size=*/65536,
                            /*expected_version=*/0,
                            /*expected_store_epoch=*/1,
                            /*verify_checksum=*/false,
                            /*checksum_hint=*/0)
                    .result.success);

    auto impl    = CreateKVStoreAdminBrpcServiceImplForStore(engine);
    auto adapter = std::make_shared<KVStoreAdminBrpcServiceAdapter>(impl);

    brpc::Server server;
    ASSERT_EQ(0, server.AddService(adapter.get(), brpc::SERVER_DOESNT_OWN_SERVICE));

    butil::EndPoint point;
    ASSERT_EQ(0, butil::str2endpoint("127.0.0.1", 0, &point));
    brpc::ServerOptions options;
    ASSERT_EQ(0, server.Start(point, &options));

    const std::string ep =
        std::string("127.0.0.1:") + std::to_string(server.listen_address().port);

    brpc::Channel ch;
    brpc::ChannelOptions ch_opt;
    ch_opt.timeout_ms         = 10000;
    ch_opt.connect_timeout_ms = 2000;
    ch_opt.max_retry          = 0;
    ASSERT_EQ(0, ch.Init(ep.c_str(), &ch_opt));

    KVStoreAdminService_Stub stub(&ch);
    SpillBlockToSSDRequest req;
    SpillBlockToSSDResponse resp;
    req.mutable_meta()->set_request_id("ut_evict_spill");
    req.set_store_node_id(1);
    req.set_pool_offset(0);
    req.set_block_hash("spill-brpc-ok");
    req.set_expected_version(1);
    req.set_expected_store_epoch(1);
    req.set_dram_read_size(12);
    brpc::Controller cntl;
    stub.SpillBlockToSSD(&cntl, &req, &resp, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    ASSERT_TRUE(resp.result().success()) << resp.result().error_message();

    server.Stop(0);
    server.Join();
}

TEST(KvEvictionWorkerBrpc, SpillBlockToSSDOverBrpcFailsOnStaleStoreEpoch) {
    const auto root =
        (fs::temp_directory_path() / ("kv_ev_brpc_bad_" + std::to_string(::getpid()))).string();
    fs::create_directories(root);
    auto spill = std::make_shared<SSDSpillManager>(root);

    auto engine = std::make_shared<KVStoreEngine>(/*store_node_id=*/1,
                                                  /*base_offset=*/0,
                                                  /*region_bytes=*/64LL * 65536LL,
                                                  /*block_size=*/65536,
                                                  /*store_epoch=*/1);
    engine->SetSSDSpillManager(spill);
    ASSERT_TRUE(engine
                    ->Write("spill-brpc-bad",
                            /*pool_offset=*/0,
                            "x",
                            /*compression=*/0,
                            /*original_size=*/1,
                            /*block_size=*/65536,
                            /*expected_version=*/0,
                            /*expected_store_epoch=*/1,
                            /*verify_checksum=*/false,
                            /*checksum_hint=*/0)
                    .result.success);

    auto impl    = CreateKVStoreAdminBrpcServiceImplForStore(engine);
    auto adapter = std::make_shared<KVStoreAdminBrpcServiceAdapter>(impl);

    brpc::Server server;
    ASSERT_EQ(0, server.AddService(adapter.get(), brpc::SERVER_DOESNT_OWN_SERVICE));
    butil::EndPoint point;
    ASSERT_EQ(0, butil::str2endpoint("127.0.0.1", 0, &point));
    brpc::ServerOptions options;
    ASSERT_EQ(0, server.Start(point, &options));
    const std::string ep =
        std::string("127.0.0.1:") + std::to_string(server.listen_address().port);

    brpc::Channel ch;
    brpc::ChannelOptions ch_opt;
    ch_opt.timeout_ms         = 10000;
    ch_opt.connect_timeout_ms = 2000;
    ch_opt.max_retry          = 0;
    ASSERT_EQ(0, ch.Init(ep.c_str(), &ch_opt));

    KVStoreAdminService_Stub stub(&ch);
    SpillBlockToSSDRequest req;
    SpillBlockToSSDResponse resp;
    req.mutable_meta()->set_request_id("ut_evict_spill_bad");
    req.set_store_node_id(1);
    req.set_pool_offset(0);
    req.set_block_hash("spill-brpc-bad");
    req.set_expected_version(1);
    req.set_expected_store_epoch(99);
    req.set_dram_read_size(1);
    brpc::Controller cntl;
    stub.SpillBlockToSSD(&cntl, &req, &resp, nullptr);
    ASSERT_FALSE(cntl.Failed());
    EXPECT_FALSE(resp.result().success());

    server.Stop(0);
    server.Join();
}

}  // namespace falconfs::kv
