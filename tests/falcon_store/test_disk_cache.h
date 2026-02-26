#pragma once

#include <filesystem>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "log/logging.h"

class DiskCacheUT : public testing::Test {
  public:
    static void SetUpTestSuite()
    {
        try {
            auto cachePath = rootPath;
            if (std::filesystem::exists(cachePath)) {
                std::filesystem::remove_all(cachePath);
                FALCON_LOG(LOG_INFO) << "已删除目录及其内容: " << cachePath;
            }
            std::filesystem::create_directory(cachePath);
            FALCON_LOG(LOG_INFO) << "已重新创建目录: " << cachePath;

            // 在目录下创建 100 个子目录
            for (int i = 0; i <= 100; ++i) {
                std::string subdir_name = cachePath + "/" + std::to_string(i);
                std::filesystem::create_directory(subdir_name);
            }
            FALCON_LOG(LOG_INFO) << "已创建子目录: 0 ~ 100 ";
        } catch (const std::exception &e) {
            FALCON_LOG(LOG_ERROR) << "发生错误: " << e.what();
            exit(1);
        }
    }
    static void TearDownTestSuite() {}
    void SetUp() override {}
    void TearDown() override {}

    static std::string rootPath;
};
