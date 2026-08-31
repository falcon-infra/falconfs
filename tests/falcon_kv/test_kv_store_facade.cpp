#include <gtest/gtest.h>

#include <brpc/channel.h>
#include <brpc/server.h>
#include <butil/endpoint.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>

#include "kv_data_service.pb.h"
#include "vllm_kv_cache/src/service/kv_data_brpc_service.h"
#include "vllm_kv_cache/src/store/kv_data_service_impl.h"
#include "vllm_kv_cache/src/store/kv_store_facade.h"

namespace falconfs::kv {

namespace fs = std::filesystem;

TEST(KvStoreFacade, RemoteResolveRoundtripsWriteThroughRegistry) {
    auto engine = std::make_shared<KVStoreEngine>();
    auto impl    = std::make_shared<KVDataServiceImpl>(engine);
    auto adapter = std::make_shared<KVDataBrpcServiceAdapter>(impl);

    brpc::Server server;
    ASSERT_EQ(0, server.AddService(adapter.get(), brpc::SERVER_DOESNT_OWN_SERVICE));
    butil::EndPoint point;
    ASSERT_EQ(0, butil::str2endpoint("127.0.0.1", 0, &point));
    brpc::ServerOptions options;
    ASSERT_EQ(0, server.Start(point, &options));
    const std::string ep =
        std::string("127.0.0.1:") + std::to_string(server.listen_address().port);

    KVStoreFacadeRegistry reg;
    reg.Start(/*local_host=*/"other-node", /*cn_conninfo=*/"");
    reg.RegisterRemote(1, ep);
    (void)reg.RefreshNow(3000);

    auto f = reg.Resolve(1);
    ASSERT_NE(f, nullptr);
    EXPECT_FALSE(f->IsLocal());
    EXPECT_TRUE(f->IsHealthy());

    BatchWriteBlockRequest wreq;
    wreq.mutable_meta()->set_request_id("facade_ut");
    auto* wi = wreq.add_items();
    wi->set_block_hash("facade-h1");
    wi->set_pool_offset(0);
    wi->set_payload("abc");
    wi->set_compression(CompressionType::COMPRESSION_NONE);
    wi->set_original_size(3);
    wi->set_block_size(65536);
    wi->set_expected_version(0);
    wi->set_expected_store_epoch(1);
    BatchWriteBlockResponse wrsp;
    f->BatchWriteBlock(wreq, &wrsp);
    ASSERT_EQ(wrsp.results_size(), 1);
    EXPECT_TRUE(wrsp.results(0).result().success());

    reg.Stop();
    server.Stop(0);
    server.Join();
}

}  // namespace falconfs::kv
