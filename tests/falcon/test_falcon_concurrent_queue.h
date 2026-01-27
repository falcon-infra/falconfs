#pragma once

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include <functional>
#include <chrono>
#include <future>
#include <iostream>
#include "connection_pool/falcon_concurrent_queue.h"

using namespace pg_connection_pool;

class ConcurrentQueueTest : public ::testing::Test {
protected:
    void SetUp() override {
        queue.SetConsumer(std::this_thread::get_id());
    }
    
    void TearDown() override {
        queue.clear();
    }
    
    ConcurrentQueue<int> queue;
};
