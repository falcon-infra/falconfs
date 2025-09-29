
/*
1. 单例
2. put/get 小数
3. put/get 大数
4. 覆盖 put
5. delete
6. get 不存在的key
7. 异常输入
8. 多线程调用
9. 稳定性
*/

#include "falcon_kv_server.h"

#include <iostream>
#include <cstring>
#include <vector>
#include <random>

#include <gtest/gtest.h>

bool memory_equal(char* a, char* b, size_t len) {
    return std::memcmp(a, b, len) == 0;
}

std::string random_string(size_t length) {
    const std::string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> local_dis(0, charset.size()-1);
    std::string result;
    for ( int i=0; i<length; ++i) {
        result += charset[local_dis(gen)];
    }
    return result;
}


class KVServerTest : public :: testing::Test {
    protected:
        void SetUp() override {
            std::cout << "\n---- Start a new test ----\n";
            // 初始化资源
        }
        void TearDown() override {
            std::cout << "---- Test finished ----\n";
            // 清理资源
        }
};

TEST_F(KVServerTest, SingletonInstanceIsUnique) {
    KVServer& instance1 = KVServer::getInstance();
    KVServer& instance2 = KVServer::getInstance();
    EXPECT_EQ(&instance1, &instance2);
}

TEST_F(KVServerTest, PutGetMultiBlock) {
    KVServer& kv = KVServer::getInstance();

    const size_t valueSize = 6 * 1024 * 1024 + 250;
    const uint32_t blockSize = 3 * 1024 * 1024;
    const size_t numBlocks = (valueSize + blockSize - 1) / blockSize;

    // 1. 生成随机大值
    std::string value = random_string(valueSize);
    std::string key = random_string(2);

    // 2. 分配多个 block（模拟共享内存块）
    std::vector<std::unique_ptr<char[]>> blockBuffers;
    std::vector<void*> blockAddrs;

    blockBuffers.reserve(numBlocks);
    blockAddrs.reserve(numBlocks);

    const char* valueData = reinterpret_cast<const char*>(value.data());

    for (size_t i = 0; i < numBlocks; ++i) {
        size_t offset = i * blockSize;
        size_t copySize = std::min(static_cast<size_t>(blockSize), valueSize - offset);

        auto buffer = std::make_unique<char[]>(blockSize);
        std::memcpy(buffer.get(), valueData + offset, copySize); // 填充数据

        blockBuffers.push_back(std::move(buffer));
        blockAddrs.push_back(blockBuffers.back().get());
    }

    // 3. 调用 Put 接口
    int32_t putRet = kv.Put(key, static_cast<uint32_t>(valueSize),
                            blockAddrs, blockSize);
    EXPECT_EQ(putRet, 0) << "Put failed with return code: " << putRet;

    // 4. 准备 Get 缓冲区
    std::vector<std::unique_ptr<char[]>> writeBlocks;
    std::vector<void*> writeAddrs;
    writeBlocks.reserve(numBlocks);
    writeAddrs.reserve(numBlocks);
    for (size_t i = 0; i < numBlocks; ++i){
        auto buffer = std::make_unique<char[]>(blockSize);
        std::fill(buffer.get(), buffer.get()+blockSize, 0); // 先填充0
        
        writeAddrs.push_back(buffer.get());
        writeBlocks.push_back(std::move(buffer));
    }

    int32_t getRet = kv.Get(key, writeAddrs, blockSize);
    EXPECT_EQ(getRet, 0) << "Get failed with return code: " << getRet;


    for (size_t i = 0; i < numBlocks; ++i){
        size_t offset = i * blockSize;
        size_t cmpSize = std::min(static_cast<size_t>(blockSize), valueSize - offset);
        EXPECT_TRUE(memory_equal(writeBlocks[i].get(), blockBuffers[i].get(), cmpSize))
        << "Retrieved data does not match original value in block " << i;
    }
}