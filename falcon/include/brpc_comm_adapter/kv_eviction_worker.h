/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * Background eviction worker that lives inside `libbrpcplugin.so`.
 *
 * v6 §14 design: a dedicated thread spawned by the BRPC server scans the
 * `KVMetadataEngine`'s DRAM CLOCK candidates on a periodic timer and drives
 * each cold candidate through the two-phase eviction state machine
 * (`STORED -> EVICTING -> EVICTED`, with rollback to `STORED` on spill
 * failure). The thread:
 *
 *   - owns its own libpq connection so it can issue catalog CAS round-trips
 *     against `pg_catalog.falcon_kv_metadata_catalog_call` independently of
 *     the connection-pool worker threads (those are reserved for client BRPC
 *     traffic);
 *   - performs the cache + catalog CAS through
 *     `KVMetadataServiceImpl::BatchUpdateBlockStatusSplitForPoolWorker`
 *     so the DRAM Pass-1 + catalog Pass-2 contract matches the BRPC handler
 *     path exactly (no duplicate engine logic);
 *   - calls `KVStoreEngine::SpillBlockToSSD` for the actual byte spill;
 *   - cooperatively shuts down via an internal flag + condition variable.
 *
 * Trigger policy (§14.1):
 *   - every `falcon_kv.eviction_period_ms`, OR
 *   - when any region's free ratio drops below
 *     `falcon_kv.eviction_low_watermark_pct` (eager mode skips the period
 *     wait for the next cycle).
 *
 * Each cycle handles up to `falcon_kv.eviction_chunk` candidates.
 */
#ifndef KV_EVICTION_WORKER_H
#define KV_EVICTION_WORKER_H

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace falconfs::kv {
class KVMetadataEngine;
class KVMetadataServiceImpl;
class KVStoreEngine;
}  // namespace falconfs::kv

namespace falcon::kv_proto {

class KVEvictionWorker {
public:
    KVEvictionWorker(std::shared_ptr<::falconfs::kv::KVMetadataEngine> engine,
                     std::shared_ptr<::falconfs::kv::KVMetadataServiceImpl> impl,
                     std::shared_ptr<::falconfs::kv::KVStoreEngine> store_engine,
                     int pg_port);
    ~KVEvictionWorker();

    KVEvictionWorker(const KVEvictionWorker&) = delete;
    KVEvictionWorker& operator=(const KVEvictionWorker&) = delete;

    // Spawns the worker thread. Safe to call once.
    void Start();
    // Cooperatively stops the worker thread, joins, and closes the libpq
    // connection. Idempotent.
    void Stop();

private:
    void Loop();

    std::shared_ptr<::falconfs::kv::KVMetadataEngine> engine_;
    std::shared_ptr<::falconfs::kv::KVMetadataServiceImpl> impl_;
    std::shared_ptr<::falconfs::kv::KVStoreEngine> store_engine_;
    int pg_port_;

    std::atomic<bool> stop_{false};
    std::mutex mu_;
    std::condition_variable cv_;
    std::thread thread_;
    void* pg_conn_ = nullptr;  // PGconn*; void* to keep the header libpq-free
};

}  // namespace falcon::kv_proto

#endif  // KV_EVICTION_WORKER_H
