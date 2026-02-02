#pragma once

#include <boost/lockfree/queue.hpp>
#include <boost/lockfree/policies.hpp>
#include <atomic>
#include <thread>
#include <vector>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <optional>
#include <functional>
#include <algorithm>
#include <iostream>
#include <condition_variable>

namespace pg_connection_pool {

template<typename T>
struct QueueTraits {
    using value_type = T;

    static constexpr size_t BATCH_THRESHOLD = 32;

    static constexpr size_t INITIAL_LOCAL_QUEUE_SIZE = 256;

    static constexpr bool ENABLE_STATS = false;

    static constexpr bool SINGLE_CONSUMER = false;

    using allocator = std::allocator<T>;
};

template<typename T, typename Traits = QueueTraits<T>>
class ConcurrentQueue {
protected:
    struct ThreadExitHelper {
        std::atomic<bool> queueDestroyed = false;
        std::function<void()> exitCallback;
        ThreadExitHelper(const std::function<void()> &cb) : exitCallback(cb) {}
        ~ThreadExitHelper() {
            if (!queueDestroyed && exitCallback) {
                exitCallback();
            }
        }
    };

    using LocalQueue = boost::lockfree::queue<
        T, 
        boost::lockfree::allocator<typename Traits::allocator>
    >;

    struct ProducerInfo {
        LocalQueue queue;
        std::atomic<size_t> approx_size{0};
        std::atomic<bool> active{true};

        ProducerInfo(size_t size) : queue(size) {}

        bool is_empty() const {
            return queue.empty();
        }
    };

private:
    mutable std::shared_mutex producers_mutex_;
    // producers access to find itself
    std::unordered_map<std::thread::id, std::shared_ptr<ProducerInfo>> producer_map_;
    // consumer accesses to iterate all producers
    std::vector<std::shared_ptr<ProducerInfo>> active_producers_;
    // notify all producer threads that queue is destructed
    std::vector<std::weak_ptr<ThreadExitHelper>> thread_helpers_;
    
    std::atomic<bool> stop = false;
    std::atomic<bool> needGC = false;
    std::condition_variable_any gcCv_;
    std::thread gcWorkerThread_;

    size_t initial_queue_size_ = Traits::INITIAL_LOCAL_QUEUE_SIZE;

    std::thread::id consumer_thread_id_;
    mutable std::atomic<bool> has_consumer_{false};

public:
    struct Stats {
        std::atomic<size_t> total_enqueues{0};
        std::atomic<size_t> total_dequeues{0};
        std::atomic<size_t> queue_empty_count{0};
    };
    Stats stats_;

    ConcurrentQueue() {
        gcWorkerThread_ = std::thread([this]() {
            this->GarbageCollectWorker();
        });
    }

    ConcurrentQueue(size_t queueSize) : initial_queue_size_(queueSize) {
        gcWorkerThread_ = std::thread([this]() {
            this->GarbageCollectWorker();
        });
    }

    ~ConcurrentQueue() {
        MarkAllHelperDestroyed();
        clear_all_producers();
        stop = true;
        gcCv_.notify_all();
        gcWorkerThread_.join();
    }

    ConcurrentQueue(const ConcurrentQueue&) = delete;
    ConcurrentQueue& operator=(const ConcurrentQueue&) = delete;

    void MarkAllHelperDestroyed() {
        std::unique_lock lock(producers_mutex_);

        for (auto& weak_helper : thread_helpers_) {
            if (auto helper = weak_helper.lock()) {
                helper->queueDestroyed = true;
            }
        }
    }

    void GarbageCollectWorker() {
        while (!stop) {
            std::unique_lock lock(producers_mutex_);
            for (auto it = active_producers_.begin(); it != active_producers_.end(); it++) {
                if ((*it)->approx_size.load() == 0 && !((*it)->active)) {
                    it = active_producers_.erase(it);
                }
            }
            needGC = false;
            gcCv_.wait(lock, [this]() {
                return needGC || stop;
            });
        }
    }

    void SetConsumer(std::thread::id threadId) {
        consumer_thread_id_ = threadId;
        has_consumer_.store(true, std::memory_order_release);
    }

    bool enqueue(const T& value) {
        return enqueue_to_producer(value);
    }

    bool enqueue(T&& value) {
        return enqueue_to_producer(std::move(value));
    }

    template<typename InputIterator>
    bool enqueue_bulk(InputIterator first, size_t count) {
        if (count == 0) return true;

        if (count <= Traits::BATCH_THRESHOLD) {
            bool all_success = true;
            for (size_t i = 0; i < count; ++i) {
                if (!enqueue(*first++)) {
                    all_success = false;
                }
            }
            return all_success;
        }

        return enqueue_bulk_to_producer(first, count);
    }

    bool dequeue(T& value) {
        if constexpr (Traits::SINGLE_CONSUMER) {
            if (!is_consumer_thread()) {
                return false;
            }            
        }

        if (try_dequeue_from_producers(value)) {
            if constexpr (Traits::ENABLE_STATS) {
                stats_.total_dequeues.fetch_add(1, std::memory_order_relaxed);
            }
            return true;
        }

        return false;
    }

    size_t dequeue_bulk(std::function<void(T)> &&func, size_t max_count) {
        if constexpr (Traits::SINGLE_CONSUMER) {
            if (!is_consumer_thread()) {
                return 0;
            }            
        }
        if (max_count == 0) {
            return 0;
        }

        size_t total_dequeued = dequeue_bulk_from_producers(std::forward<std::function<void(T)>>(func), max_count);

        return total_dequeued;
    }

    size_t size_approx() const {
        size_t total_size = 0;
        {
            std::shared_lock lock(producers_mutex_);
            for (auto producer : active_producers_) {
                if (producer) {
                    total_size += producer->approx_size.load(std::memory_order_relaxed);
                }
            }
        }

        return total_size;
    }

    bool empty() const {
        std::shared_lock lock(producers_mutex_);
        for (auto producer : active_producers_) {
            if (producer && !producer->is_empty()) {
                return false;
            }
        }

        return true;
    }

    void clear() {
        clear_all_producers();
    }

    template<bool Enabled = Traits::ENABLE_STATS>
    std::enable_if_t<Enabled, const Stats*> get_stats() const {
        return &stats_;
    }

    size_t active_producer_count() const {
        std::shared_lock lock(producers_mutex_);
        size_t size = 0;
        for (auto &producer : active_producers_) {
            if (producer->active) {
                ++size;
            }
        }
        return size;
    }

private:
    bool is_consumer_thread() const {
        return has_consumer_.load(std::memory_order_acquire) && 
               std::this_thread::get_id() == consumer_thread_id_;
    }

    bool enqueue_to_producer(const T& value) {
        auto producer_info = get_or_create_producer_info();
        if (!producer_info) return false;

        if (producer_info->queue.push(value)) {
            producer_info->approx_size.fetch_add(1, std::memory_order_relaxed);

            if constexpr (Traits::ENABLE_STATS) {
                stats_.total_enqueues.fetch_add(1, std::memory_order_relaxed);
            }
            return true;
        }

        return false;
    }

    bool enqueue_to_producer(T&& value) {
        auto producer_info = get_or_create_producer_info();
        if (!producer_info) return false;

        if (producer_info->queue.push(std::move(value))) {
            producer_info->approx_size.fetch_add(1, std::memory_order_relaxed);

            if constexpr (Traits::ENABLE_STATS) {
                stats_.total_enqueues.fetch_add(1, std::memory_order_relaxed);
            }
            return true;
        }

        return false;
    }

    template<typename InputIterator>
    bool enqueue_bulk_to_producer(InputIterator first, size_t count) {
        auto producer_info = get_or_create_producer_info();
        if (!producer_info) return false;

        size_t success_count = 0;
        for (size_t i = 0; i < count; ++i) {
            if (producer_info->queue.push(*first++)) {
                success_count++;
            } else {
                break;
            }
        }

        producer_info->approx_size.fetch_add(success_count, std::memory_order_relaxed);

        if constexpr (Traits::ENABLE_STATS) {
            stats_.total_enqueues.fetch_add(success_count, std::memory_order_relaxed);
        }

        return success_count == count;
    }

    bool try_dequeue_from_producers(T& value) {
        std::shared_lock lock(producers_mutex_);

        if (active_producers_.empty()) {
            return false;
        }

        static thread_local size_t dequeue_index = 0;
        size_t start_index = dequeue_index++ % active_producers_.size();

        for (size_t i = 0; i < active_producers_.size(); ++i) {
            size_t idx = (start_index + i) % active_producers_.size();
            auto producer = active_producers_[idx];

            if (producer) {
                if (producer->queue.pop(value)) {
                    producer->approx_size.fetch_sub(1, std::memory_order_relaxed);
                    if (producer->approx_size == 0) {
                        if (!producer->active) {
                            needGC = true;
                            gcCv_.notify_one();
                        } else if constexpr (Traits::ENABLE_STATS) {
                            stats_.queue_empty_count.fetch_add(1, std::memory_order_relaxed);
                        }
                    }
                    return true;
                }
            }
        }

        return false;
    }

    size_t dequeue_bulk_from_producers(std::function<void(T)> &&func, size_t max_count) {
        if (max_count == 0) return 0;

        std::shared_lock lock(producers_mutex_);
        if (active_producers_.size() == 0) return 0;

        size_t total_stolen = 0;
        static thread_local size_t dequeue_index = 0;
        size_t start_index = dequeue_index++ % active_producers_.size();

        for (size_t i = 0; i < active_producers_.size(); ++i) {
            if (total_stolen >= max_count) break;

            size_t idx = (start_index + i) % active_producers_.size();
            auto producer = active_producers_[idx];

            if (producer) {
                size_t stolen = 0;
                // dequeue each producer queue
                for (size_t j = 0; j < max_count - total_stolen; ++j) {
                    if (producer->queue.consume_one(func)) {
                        stolen++;
                    } else {
                        break;
                    }
                }
                if (stolen > 0) {
                    producer->approx_size.fetch_sub(stolen, std::memory_order_relaxed);
                    total_stolen += stolen;
                }
                if (producer->approx_size == 0) {
                    if (!producer->active) {
                        needGC = true;
                        gcCv_.notify_one();
                    } else if constexpr (Traits::ENABLE_STATS) {
                        stats_.queue_empty_count.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                
            }
        }

        return total_stolen;
    }

    std::shared_ptr<ProducerInfo> get_or_create_producer_info() {
        std::thread::id tid = std::this_thread::get_id();

        {
            std::shared_lock lock(producers_mutex_);
            auto it = producer_map_.find(tid);
            if (it != producer_map_.end()) {
                return it->second;
            }
        }

        std::unique_lock lock(producers_mutex_);
        auto it = producer_map_.find(tid);
        if (it != producer_map_.end()) {
            return it->second;
        }

        auto producer_info = std::make_shared<ProducerInfo>(initial_queue_size_);

        producer_map_[tid] = producer_info;

        active_producers_.push_back(producer_info);

        static thread_local std::shared_ptr<ThreadExitHelper> exit_helper = std::make_shared<ThreadExitHelper>([this, tid]() {
            this->on_thread_exit(tid);
        });
        thread_helpers_.push_back(std::weak_ptr<ThreadExitHelper>(exit_helper));

        return producer_info;
    }

    void on_thread_exit(std::thread::id tid) {
        std::unique_lock lock(producers_mutex_);
        auto it = producer_map_.find(tid);
        if (it != producer_map_.end()) {
            if (it->second->approx_size == 0) {
                active_producers_.erase(
                    std::remove(active_producers_.begin(), active_producers_.end(), it->second),
                    active_producers_.end());
            } else {
                // set inactive, to be erased on dequeue to empty
                it->second->active.store(false);
            }
            producer_map_.erase(it);
        }
    }

    void clear_all_producers() {
        std::unique_lock hash_lock(producers_mutex_);

        for (auto& pair : producer_map_) {
            T value;
            while (pair.second->queue.pop(value)) {}
            pair.second->approx_size.store(0, std::memory_order_relaxed);
        }

        producer_map_.clear();
        active_producers_.clear();
    }
};

template<typename Queue, typename InputIterator>
bool enqueue_bulk(Queue& queue, InputIterator first, size_t count) {
    return queue.enqueue_bulk(first, count);
}

template<typename Queue, typename T>
size_t dequeue_bulk(Queue& queue, std::function<void(T)> &&func, size_t max_count) {
    return queue.dequeue_bulk(func, max_count);
}

} // namespace pg_connection_pool