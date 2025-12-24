#pragma once

#include <filesystem>
#include <iostream>
#include <fstream>
#include <memory>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "zookeeper/zookeeper.h"

#include "cm/falcon_cm.h"

class FalconCMIT : public testing::Test {
protected:
    static void SetUpTestSuite() {
        // zookeeper container should be running and already initialized
        const char *zkEndPoint = std::getenv("zk_endpoint");
        if (zkEndPoint == nullptr) {
            std::cout << "env zk_endpoint not set!" << std::endl;
            exit(1);
        }
        int ret = FalconCM::GetInstance(zkEndPoint, 10000, "/falcon")->GetInitStatus();
        if (ret != 0) {
            std::cout << "FalconCM init connection failed! : " << ret << std::endl;
            exit(1);
        }

        zhandle = zookeeper_init(zkEndPoint, nullptr, 10000, nullptr, nullptr, 0);
        if (zhandle == nullptr) {
            std::cout << "zookeeper connect failed" << std::endl;
            exit(1);
        }

        falconCM = FalconCM::GetInstance();
    }

    static void TearDownTestSuite() {
        FalconCM::DeleteInstance();
    }

    static FalconCM* falconCM;
    static zhandle_t *zhandle;
};
