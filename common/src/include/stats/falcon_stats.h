/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <securec.h>
#include <atomic>
#include <chrono>
#include <cmath>
#include <stop_token>
#include <numeric>
#include <iostream>

enum {
    FUSE_OPS = 0,
    FUSE_READ_OPS,
    FUSE_WRITE_OPS,
    META_OPS,
    META_OPEN,
    META_OPEN_ATOMIC,
    META_RELEASE,
    META_STAT,
    META_LOOKUP,
    META_CREATE,
    META_UNLINK,
    META_MKDIR,
    META_RMDIR,
    META_OPENDIR,
    META_READDIR,
    META_RENAME,
    META_ACCESS,
    META_RELEASEDIR,
    META_TRUNCATE,
    META_FLUSH,
    META_FSYNC,
    OPS_END,
    FUSE_LAT,
    FUSE_LAT_MAX,
    FUSE_READ_LAT,
    FUSE_READ_LAT_MAX,
    FUSE_WRITE_LAT,
    FUSE_WRITE_LAT_MAX,
    META_LAT,
    META_LAT_MAX,
    META_OPEN_LAT,
    META_OPEN_LAT_MAX,
    META_RELEASE_LAT,
    META_RELEASE_LAT_MAX,
    META_STAT_LAT,
    META_STAT_LAT_MAX,
    META_CREATE_LAT,
    META_CREATE_LAT_MAX,
    LAT_END,
    FUSE_READ,
    FUSE_WRITE,
    BLOCKCACHE_READ,
    BLOCKCACHE_WRITE,
    OBJ_GET,
    OBJ_PUT,
    STATS_END
};

/* if stat max latency */
inline bool g_statMax = false;
inline void setStatMax(bool set)
{
    g_statMax = set;
}
inline bool getStatMax()
{
    return g_statMax;
}

class FalconStats {
  public:
    static FalconStats &GetInstance()
    {
        static FalconStats instance;
        return instance;
    }
    FalconStats()
    {
        for (auto &s : stats) {
            s.store(0);
        }
    }
    void storeStatforGet(std::stop_token stoken);
    std::atomic<size_t> stats[STATS_END];
    std::atomic<size_t> storedStats[STATS_END];
};

class StatFuseTimer {
  public:
    StatFuseTimer(int item1_ = FUSE_LAT, int item2_ = -1) : item1(item1_), item2(item2_)
    {
        start_time = std::chrono::steady_clock::now();
    }
    ~StatFuseTimer()
    {
        auto end_time = std::chrono::steady_clock::now();
        size_t elaspedTime = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        FalconStats::GetInstance().stats[item1] += elaspedTime;
        if (item2 != -1) {
            FalconStats::GetInstance().stats[item2] += elaspedTime;
        }
        // update max latency. cas
        if (!getStatMax()) return;
        auto itemtoUpdate = item2 == -1 ? item1 : item2;
        auto oldMax = FalconStats::GetInstance().stats[itemtoUpdate + 1].load(std::memory_order_acquire);
        while (oldMax < elaspedTime) {
            if (FalconStats::GetInstance().stats[itemtoUpdate + 1].compare_exchange_weak(oldMax, elaspedTime,
                std::memory_order_release, std::memory_order_acquire)) {
                break;
            }
        }
    }
    std::chrono::steady_clock::time_point start_time;
    int item1;
    int item2;
};

std::string formatU64(size_t size);
std::string formatOp(size_t size);
std::string formatTime(size_t mus, size_t ops);
double formatTimeDouble(size_t mus, size_t ops);

std::vector<std::string> convertStatstoString(const std::vector<size_t> &stats);
void PrintStats(std::string_view mountPath, std::stop_token stoken);
void printStatsVector(const std::vector<std::string> &stats);
inline void printStatsHeader()
{
    std::cout << "-------------------------------------fuse------------------------------------ --------------------------------------------------meta-------------------------------------------------- -blockcache ---object--\n";
    std::cout << " ops     lat:avg/max | ops    lat:avg/max  read  | ops    lat:avg/max  write | ops    lat:avg/max | open   lat:avg/max | close  lat:avg/max | stat   lat:avg/max | create lat:avg/max | read write| get   put \n";
    std::cout << "—————————————————————————————————————————————————————————————————————————————|————————————————————————————————————————————————————————————————————————————————————————————————————————|———————————|———————————\n";
}
