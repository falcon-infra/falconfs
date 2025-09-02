
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

TEST_F(KVServerTest, PutAndGetSmallValue) {
    KVServer& kv = KVServer::getInstance();

    std::string key = random_string(10);
    std::string value = "hello, kvserver";
    int32_t putRet = kv.Put(key, value.data(), static_cast<uint32_t>(value.size()));
    EXPECT_EQ(putRet, 0) << "Put failed with return code: "<<putRet;

    std::vector<char> buffer(value.size() + 10);
    uint32_t actualSize = 0;
    int32_t getRet = kv.Get(key, buffer.data(), actualSize);
    EXPECT_EQ(getRet, 0) << "Get failed with return code: " << getRet;
    EXPECT_EQ(actualSize, value.size());
    EXPECT_TRUE(memory_equal(buffer.data(), value.data(), value.size()));
}

TEST_F(KVServerTest, PutAndGetLargeValue) {
    KVServer& kv = KVServer::getInstance();

    std::string key = random_string(10);
    std::string value = random_string(15*1024*1024); // 15MB
    int32_t putRet = kv.Put(key, value.data(), static_cast<uint32_t>(value.size()));
    EXPECT_EQ(putRet, 0) << "Put failed with return code: "<<putRet;

    std::vector<char> buffer(value.size() + 10);
    uint32_t actualSize = 0;
    int32_t getRet = kv.Get(key, buffer.data(), actualSize);
    EXPECT_EQ(getRet, 0) << "Get failed with return code: " << getRet;
    EXPECT_EQ(actualSize, value.size());
    EXPECT_TRUE(memory_equal(buffer.data(), value.data(), value.size()));
}

TEST_F(KVServerTest, PutOverwritesExistingKey) {
    KVServer& kv = KVServer::getInstance();

    std::string key = random_string(10);
    std::string value1 = random_string(15*1024*1024); // 15MB
    std::string value2 = random_string(1*1024*1024); // 1MB
    int32_t putRet = kv.Put(key, value1.data(), static_cast<uint32_t>(value1.size()));
    EXPECT_EQ(putRet, 0) << "Put value1 failed with return code: "<<putRet;
    putRet = kv.Put(key, value2.data(), static_cast<uint32_t>(value2.size()));
    EXPECT_EQ(putRet, 0) << "Put value2 failed with return code: "<<putRet;

    std::vector<char> buffer(value2.size() + 10);
    uint32_t actualSize = 0;
    int32_t getRet = kv.Get(key, buffer.data(), actualSize);
    EXPECT_EQ(getRet, 0) << "Get value2 failed with return code: " << getRet;
    EXPECT_EQ(actualSize, value2.size());
    EXPECT_TRUE(memory_equal(buffer.data(), value2.data(), value2.size()));
}

TEST_F(KVServerTest, DeleteKey) {
    auto& kv = KVServer::getInstance();
    std::string key = "delete_test";
    std::string value = "to be deleted";

    EXPECT_EQ(kv.Put(key, value.data(), value.size()), 0);
    EXPECT_EQ(kv.Delete(key), 0);

    std::vector<char> buffer(100);
    uint32_t actualSize = 0;
    int32_t ret = kv.Get(key, buffer.data(), actualSize);

    EXPECT_NE(ret, 0) << "Get after Delete should fail";
}

TEST_F(KVServerTest, InvalidInputs) {
    auto& kv = KVServer::getInstance();
    std::string key = "invalid_input_test";

    // valPtr 为 nullptr，size > 0
    EXPECT_NE(kv.Put(key, nullptr, 10), 0);

    // valSize 为 0
    EXPECT_NE(kv.Put(key, "valid", 0), 0) << "Put with size 0 should failed";

    // Get 时 valPtr 为 nullptr
    uint32_t size = 100;
    EXPECT_NE(kv.Get(key, nullptr, size), 0);
}

TEST_F(KVServerTest, ConcurrentOperations) {
    auto& kv = KVServer::getInstance();
    const int numThreads = 10;
    const int opsPerThread = 100;
    std::vector<std::thread> threads;

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < opsPerThread; ++i) {
                std::string key = "thread_" + std::to_string(t) + "_op_" + std::to_string(i);
                std::string value = random_string(1024); // 1KB

                // Put
                EXPECT_EQ(kv.Put(key, value.data(), value.size()), 0);

                // Get
                std::vector<char> buffer(value.size() + 10);
                uint32_t actualSize = 0;
                EXPECT_EQ(kv.Get(key, buffer.data(), actualSize), 0);
                EXPECT_EQ(actualSize, value.size());
                EXPECT_TRUE(memory_equal(buffer.data(), value.data(), value.size()));

                // Delete
                EXPECT_EQ(kv.Delete(key), 0);
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }
}
