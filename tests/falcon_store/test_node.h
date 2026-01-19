#pragma once

#include <sstream>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "brpc/brpc_server.h"
#include "conf/falcon_property_key.h"
#include "init/falcon_init.h"
#include "log/logging.h"

class NodeUT : public testing::Test {
  public:
    static void SetUpTestSuite()
    {
        FALCON_LOG(LOG_INFO) << "Calling SetUpTestSuite!";
        int ret = GetInit().Init();
        if (ret != 0) {
            exit(1);
        }
        config = GetInit().GetFalconConfig();

        try {
            falcon::brpc_io::RemoteIOServer &server = falcon::brpc_io::RemoteIOServer::GetInstance();
            std::string clusterView = config->GetArray(FalconPropertyKey::FALCON_CLUSTER_VIEW);
            std::stringstream ss(clusterView);
            while (ss.good()) {
                std::string substr;
                getline(ss, substr, ',');
                views.push_back(substr);
            }
            int nodeId = config->GetUint32(FalconPropertyKey::FALCON_NODE_ID);
            localEndpoint = views[nodeId];
            server.endPoint = localEndpoint;
            FALCON_LOG(LOG_INFO) << "brpc endpoint = " << server.endPoint;
            std::thread brpcServerThread(&falcon::brpc_io::RemoteIOServer::Run, &server);
            {
                std::unique_lock<std::mutex> lk(server.mutexStart);
                server.cvStart.wait(lk, [&server]() { return server.isStarted; });
            }
            brpcServerThread.detach();
            server.SetReadyFlag();
        } catch (const std::exception &e) {
            FALCON_LOG(LOG_ERROR) << "发生错误: " << e.what();
            exit(1);
        }
    }
    static void TearDownTestSuite()
    {
        falcon::brpc_io::RemoteIOServer &server = falcon::brpc_io::RemoteIOServer::GetInstance();
        server.Stop();
    }
    void SetUp() override {}
    void TearDown() override {}
    static std::shared_ptr<FalconConfig> config;
    static std::string localEndpoint;
    static std::vector<std::string> views;
};
