#include "test_falcon_concurrent_queue.h"

TEST_F(ConcurrentQueueTest, ConstructionAndSetup) {
    ConcurrentQueue<int> defaultQueue;
    EXPECT_TRUE(defaultQueue.empty());
    EXPECT_EQ(defaultQueue.size_approx(), 0);

    ConcurrentQueue<int> sizedQueue(512);
    EXPECT_TRUE(sizedQueue.empty());

    std::thread::id testId = std::this_thread::get_id();
    queue.SetConsumer(testId);
    EXPECT_EQ(queue.active_producer_count(), 0);
}

TEST_F(ConcurrentQueueTest, SingleElementEnqueueDequeue) {
    EXPECT_TRUE(queue.enqueue(1));
    EXPECT_TRUE(queue.enqueue(2));
    EXPECT_TRUE(queue.enqueue(3));

    EXPECT_EQ(queue.size_approx(), 3);
    EXPECT_FALSE(queue.empty());

    int value;
    EXPECT_TRUE(queue.dequeue(value));
    EXPECT_EQ(value, 1);

    EXPECT_TRUE(queue.dequeue(value));
    EXPECT_EQ(value, 2);

    EXPECT_TRUE(queue.dequeue(value));
    EXPECT_EQ(value, 3);

    EXPECT_FALSE(queue.dequeue(value));
    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(queue.size_approx(), 0);
}

TEST_F(ConcurrentQueueTest, MoveSemantics) {
    ConcurrentQueue<int *> ptrQueue;
    ptrQueue.SetConsumer(std::this_thread::get_id());

    auto *ptr = new int(42);
    EXPECT_TRUE(ptrQueue.enqueue(std::move(ptr)));

    int *out;
    EXPECT_TRUE(ptrQueue.dequeue(out));
    EXPECT_NE(out, nullptr);
    EXPECT_EQ(*out, 42);
}

TEST_F(ConcurrentQueueTest, BulkOperations) {
    std::vector<int> input = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    EXPECT_TRUE(queue.enqueue_bulk(input.begin(), 10));

    EXPECT_EQ(queue.size_approx(), 10);

    std::vector<int> output;
    size_t dequeued = queue.dequeue_bulk([&output](int val) {
        output.push_back(val);
    }, 5);

    EXPECT_EQ(dequeued, 5);
    EXPECT_EQ(output.size(), 5);
    EXPECT_EQ(queue.size_approx(), 5);

    output.clear();
    dequeued = queue.dequeue_bulk([&output](int val) {
        output.push_back(val);
    }, 10);

    EXPECT_EQ(dequeued, 5);
    EXPECT_EQ(queue.size_approx(), 0);
    EXPECT_TRUE(queue.empty());
}

TEST_F(ConcurrentQueueTest, EdgeCases) {
    int value;
    EXPECT_FALSE(queue.dequeue(value));

    std::vector<int> emptyVec;
    EXPECT_TRUE(queue.enqueue_bulk(emptyVec.begin(), 0));

    size_t dequeued = queue.dequeue_bulk([](int){}, 0);
    EXPECT_EQ(dequeued, 0);

    dequeued = queue.dequeue_bulk([](int){}, 10);
    EXPECT_EQ(dequeued, 0);
}

TEST_F(ConcurrentQueueTest, ProducerManagement) {
    EXPECT_EQ(queue.active_producer_count(), 0);

    bool stop = false;
    std::thread producer1([&]() {
        queue.enqueue(100);
        while (!stop) {
            usleep(10);
        }
    });

    usleep(100);
    EXPECT_EQ(queue.active_producer_count(), 1);

    std::thread producer2([&]() {
        queue.enqueue(200);
        queue.enqueue(300);
        while (!stop) {
            usleep(10);
        }
    });

    usleep(100);
    EXPECT_EQ(queue.active_producer_count(), 2);

    stop = true;
    producer1.join();
    producer2.join();

    queue.clear();
    EXPECT_EQ(queue.active_producer_count(), 0);
    EXPECT_TRUE(queue.empty());
}

TEST_F(ConcurrentQueueTest, MultipleProducers) {
    constexpr int NUM_THREADS = 5;
    constexpr int ITEMS_PER_THREAD = 100;

    std::vector<std::thread> producers;
    std::atomic<int> total_enqueued{0};

    for (int i = 0; i < NUM_THREADS; ++i) {
        producers.emplace_back([&, i]() {
            for (int j = 0; j < ITEMS_PER_THREAD; ++j) {
                if (queue.enqueue(i * 1000 + j)) {
                    total_enqueued++;
                }
            }
        });
    }

    for (auto& t : producers) {
        t.join();
    }

    EXPECT_EQ(queue.size_approx(), NUM_THREADS * ITEMS_PER_THREAD);
    EXPECT_EQ(total_enqueued.load(), NUM_THREADS * ITEMS_PER_THREAD);

    std::vector<int> received;
    size_t total_dequeued = 0;

    while (total_dequeued < NUM_THREADS * ITEMS_PER_THREAD) {
        int value;
        if (queue.dequeue(value)) {
            received.push_back(value);
            total_dequeued++;
        }
    }

    EXPECT_EQ(received.size(), NUM_THREADS * ITEMS_PER_THREAD);
    EXPECT_TRUE(queue.empty());
}

struct Traits : public QueueTraits<int> {
    static constexpr bool SINGLE_CONSUMER = true;
};
TEST_F(ConcurrentQueueTest, ConsumerThreadRestriction) {
    ConcurrentQueue<int, Traits> singleConsumerQueue;
    singleConsumerQueue.SetConsumer(std::this_thread::get_id());

    singleConsumerQueue.enqueue(1);
    int value;
    EXPECT_TRUE(singleConsumerQueue.dequeue(value));

    std::thread non_consumer([&]() {
        int val;
        EXPECT_FALSE(singleConsumerQueue.dequeue(val));
    });
    non_consumer.join();
}

struct StatsTraits : public QueueTraits<int> {
    static constexpr bool ENABLE_STATS = true;
};
TEST(ConcurrentQueueStatsTest, StatisticsEnabled) {

    ConcurrentQueue<int, StatsTraits> statsQueue;
    statsQueue.SetConsumer(std::this_thread::get_id());

    for (int i = 0; i < 10; ++i) {
        statsQueue.enqueue(i);
    }

    int value;
    for (int i = 0; i < 5; ++i) {
        statsQueue.dequeue(value);
    }

    auto* stats = statsQueue.get_stats();
    
    EXPECT_EQ(stats->total_enqueues.load(), 10);
    EXPECT_EQ(stats->total_dequeues.load(), 5);
}

TEST_F(ConcurrentQueueTest, PerformanceTest) {
    constexpr int ITERATIONS = 10000;

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<int> data(ITERATIONS);

    EXPECT_TRUE(queue.enqueue_bulk(data.begin(), ITERATIONS));

    auto mid = std::chrono::high_resolution_clock::now();

    std::atomic<int> count{0};
    size_t dequeued = queue.dequeue_bulk([&count](int) {
        count++;
    }, ITERATIONS);

    auto end = std::chrono::high_resolution_clock::now();

    EXPECT_EQ(dequeued, ITERATIONS);
    EXPECT_EQ(count.load(), ITERATIONS);

    auto enqueue_time = std::chrono::duration_cast<std::chrono::microseconds>(mid - start);
    auto dequeue_time = std::chrono::duration_cast<std::chrono::microseconds>(end - mid);

    std::cout << "Bulk enqueue " << ITERATIONS << " elements cost: " 
              << enqueue_time.count() << " us" << std::endl;
    std::cout << "Buld dequeue " << ITERATIONS << " elements cost: " 
              << dequeue_time.count() << " us" << std::endl;
}

TEST_F(ConcurrentQueueTest, ConcurrentProducersConsumer) {
    constexpr int PRODUCER_COUNT = 3;
    constexpr int ITEMS_PER_PRODUCER = 1000;
    constexpr int TOTAL_ITEMS = PRODUCER_COUNT * ITEMS_PER_PRODUCER;
    
    std::atomic<int> produced{0};
    std::atomic<int> consumed{0};
    std::atomic<bool> stop_producing{false};

    std::vector<std::thread> producers;
    for (int i = 0; i < PRODUCER_COUNT; ++i) {
        producers.emplace_back([&, i]() {
            for (int j = 0; j < ITEMS_PER_PRODUCER; ++j) {
                if (queue.enqueue(i * ITEMS_PER_PRODUCER + j)) {
                    produced++;
                }
                std::this_thread::sleep_for(std::chrono::microseconds(1));
            }
        });
    }

    std::thread consumer([&]() {
        queue.SetConsumer(std::this_thread::get_id());
        while (consumed < TOTAL_ITEMS) {
            int value;
            if (queue.dequeue(value)) {
                consumed++;
            } else {
                std::this_thread::yield();
            }
        }
    });

    for (auto& t : producers) {
        t.join();
    }
    
    consumer.join();

    EXPECT_EQ(produced.load(), TOTAL_ITEMS);
    EXPECT_EQ(consumed.load(), TOTAL_ITEMS);
    EXPECT_TRUE(queue.empty());
}

TEST_F(ConcurrentQueueTest, ThreadExitCleanup) {
    size_t initial_count = queue.active_producer_count();

    std::thread temp_thread([&]() {
        for (int i = 0; i < 10; ++i) {
            queue.enqueue(i);
        }
    });

    temp_thread.join();

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    EXPECT_EQ(queue.active_producer_count(), initial_count);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
