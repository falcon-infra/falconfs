/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "stats/falcon_stats.h"

#include <condition_variable>
#include <csignal>
#include <fstream>

#include "log/logging.h"

void FalconStats::storeStatforGet(std::stop_token stoken)
{
    while(!stoken.stop_requested()) {
        std::fill(storedStats, storedStats + STATS_END, 0);
        for (int i = 0; i < STATS_END; i++) {
            storedStats[i] = stats[i].exchange(storedStats[i]);
        }
        storedStats[META_OPS] = std::accumulate(storedStats + META_OPS, storedStats + OPS_END, 0);
        storedStats[FUSE_OPS] = std::accumulate(storedStats + FUSE_OPS, storedStats + META_OPS + 1, 0);
        storedStats[FUSE_LAT] = storedStats[FUSE_LAT] + storedStats[META_LAT];
        for (int i = META_LAT_MAX + 2; i < LAT_END; i += 2) {
            storedStats[META_LAT_MAX] = std::max({storedStats[META_LAT_MAX].load(), storedStats[i].load()});
        }
        for (int i = FUSE_LAT_MAX + 2; i <= META_LAT_MAX; i += 2) {
            storedStats[FUSE_LAT_MAX] = std::max({storedStats[FUSE_LAT_MAX].load(), storedStats[i].load()});
        }
        sleep(1);
    }
}

std::string formatU64(size_t size)
{
    if (size == 0)
        return "0";

    constexpr std::array units = {"B", "K", "M", "G", "T", "P"};
    int unitIdx = 0;

    while (size >= 10000 && unitIdx < std::ssize(units) - 1) {
        size >>= 10;
        unitIdx++;
    }

    return std::format("{}{}", size, units[unitIdx]);
}

std::string formatOp(size_t size)
{
    if (size == 0)
        return "0";

    constexpr std::array units = {"", "K", "M", "G"};
    int unitIdx = 0;

    while (size >= 10000 && unitIdx < std::ssize(units) - 1) {
        size >>= 10;
        unitIdx++;
    }

    return std::format("{}{}", size, units[unitIdx]);
}

double formatTimeDouble(size_t mus, size_t ops)
{
    if (ops == 0) {
        return double(mus);
    }

    double time = static_cast<double>(mus) / (ops * 1000);
    const auto precision = time < 10 ? 3 : time < 100 ? 2 : time < 1000 ? 1 : 0;
    time = std::round(time * std::pow(10, precision)) / std::pow(10, precision);
    return time < 0.001 ? 0 : time;
}

std::string formatTime(size_t mus, size_t ops)
{
    std::ostringstream oss;
    oss << formatTimeDouble(mus, ops);
    return oss.str();
}

void PrintStats(std::string_view mountPath, std::stop_token stoken)
{
    std::mutex mtx;
    std::condition_variable cv;
    auto nextWake = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    // Process mount path
    auto lastNonSlash = mountPath.find_last_not_of('/');
    auto lastSlash = mountPath.substr(0, lastNonSlash).find_last_of('/');
    std::string statPath = std::format("{}/stats.out", mountPath.substr(0, lastSlash + 1));

    std::remove(statPath.c_str());
    std::ofstream outFile;
    size_t currentStats[STATS_END];
    errno_t err = memset_s(currentStats, sizeof(size_t) * STATS_END, 0, sizeof(size_t) * STATS_END);
    if (err != 0) {
        FALCON_LOG(LOG_ERROR) << "Secure func failed: " << err;
        return;
    }

    while (!stoken.stop_requested()) {
        for (int i = 0; i < STATS_END; i++) {
            currentStats[i] = FalconStats::GetInstance().stats[i].exchange(currentStats[i]);
        }
        outFile.open(statPath, std::ios::out);
        if (!outFile.is_open()) {
            FALCON_LOG(LOG_ERROR) << "Open stats file " << statPath << " failed: " << strerror(errno);
            std::unique_lock lock(mtx);
            cv.wait_for(lock, std::chrono::seconds(5), [&] { return stoken.stop_requested(); });
            continue;
        }

        // Using std::println for all outputs
        std::println(outFile, "Falcon File System Statistics");
        std::println(outFile, "============================");
        std::println(outFile);

        // FUSE Operations
        std::println(outFile, "FUSE Operations:");
        std::println(outFile, "  Total Operations: {}", currentStats[FUSE_OPS]);
        std::println(outFile, "  Average Latency: {} μs", formatTime(currentStats[FUSE_LAT], currentStats[FUSE_OPS]));
        std::println(outFile, "  Read: {}", formatU64(currentStats[FUSE_READ]));
        std::println(outFile, "  Read Operations: {}", currentStats[FUSE_READ_OPS]);
        std::println(outFile,
                     "  Read Average Latency: {} μs",
                     formatTime(currentStats[FUSE_READ_LAT], currentStats[FUSE_READ_OPS]));
        std::println(outFile, "  Write: {}", formatU64(currentStats[FUSE_WRITE]));
        std::println(outFile, "  Write Operations: {}", currentStats[FUSE_WRITE_OPS]);
        std::println(outFile,
                     "  Write Average Latency: {} μs\n",
                     formatTime(currentStats[FUSE_WRITE_LAT], currentStats[FUSE_WRITE_OPS]));

        // Metadata Operations
        std::println(outFile, "Metadata Operations:");
        // std::println(outFile, "  Total Operations: {}", formatOp(currentStats[META_OPS]));
        // std::println(outFile, "  Average Latency: {} μs", formatTime(currentStats[META_LAT],

        std::println(outFile, "  Open: {}", currentStats[META_OPEN]);
        std::println(outFile, "  Open Atomic: {}", currentStats[META_OPEN_ATOMIC]);
        std::println(outFile, "  Release: {}", currentStats[META_RELEASE]);
        std::println(outFile, "  Stat: {}", currentStats[META_STAT]);
        std::println(outFile,
                     "  Stat Latency: {} μs",
                     formatTime(currentStats[META_STAT_LAT], currentStats[META_STAT]));
        std::println(outFile, "  Lookup: {}", currentStats[META_LOOKUP]);
        std::println(outFile, "  Create: {}", currentStats[META_CREATE]);
        std::println(outFile, "  Unlink: {}", currentStats[META_UNLINK]);
        std::println(outFile, "  Mkdir: {}", currentStats[META_MKDIR]);
        std::println(outFile, "  Rmdir: {}", currentStats[META_RMDIR]);
        std::println(outFile, "  Opendir: {}", currentStats[META_OPENDIR]);
        std::println(outFile, "  Readdir: {}", currentStats[META_READDIR]);
        std::println(outFile, "  Rename: {}", currentStats[META_RENAME]);
        std::println(outFile, "  Access: {}", currentStats[META_ACCESS]);
        std::println(outFile, "  Releasedir: {}", currentStats[META_RELEASEDIR]);
        std::println(outFile, "  Truncate: {}", currentStats[META_TRUNCATE]);
        std::println(outFile, "  Flush: {}", currentStats[META_FLUSH]);
        std::println(outFile, "  Fsync: {}", currentStats[META_FSYNC]);

        // Block Cache and Object Operations
        std::println(outFile, "\nBlock Cache Operations:");
        std::println(outFile, "  Reads: {}", formatU64(currentStats[BLOCKCACHE_READ]));
        std::println(outFile, "  Writes: {}", formatU64(currentStats[BLOCKCACHE_WRITE]));

        std::println(outFile, "\nObject Operations:");
        std::println(outFile, "  Gets: {}", currentStats[OBJ_GET]);
        std::println(outFile, "  Puts: {}", currentStats[OBJ_PUT]);

        outFile.close();
        {
            std::unique_lock lock(mtx);
            cv.wait_until(lock, nextWake, [&] { return stoken.stop_requested(); });
        }
        nextWake += std::chrono::seconds(1);
    }
    FALCON_LOG(LOG_INFO) << "Stats collection terminated gracefully";
}

std::vector<std::string> convertStatstoString(const std::vector<size_t> &stats)
{
    std::vector<std::string> stringStats(STATS_END);
    stringStats[FUSE_OPS] = formatOp(stats[FUSE_OPS]);
    stringStats[FUSE_LAT] = formatTime(stats[FUSE_LAT], stats[FUSE_OPS]);
    stringStats[FUSE_LAT_MAX] = formatTime(stats[FUSE_LAT_MAX], 1);
    stringStats[FUSE_READ] = formatU64(stats[FUSE_READ]);
    stringStats[FUSE_READ_OPS] = formatOp(stats[FUSE_READ_OPS]);
    stringStats[FUSE_READ_LAT] = formatTime(stats[FUSE_READ_LAT], stats[FUSE_READ_OPS]);
    stringStats[FUSE_READ_LAT_MAX] = formatTime(stats[FUSE_READ_LAT_MAX], 1);
    stringStats[FUSE_WRITE] = formatU64(stats[FUSE_WRITE]);
    stringStats[FUSE_WRITE_OPS] = formatOp(stats[FUSE_WRITE_OPS]);
    stringStats[FUSE_WRITE_LAT] = formatTime(stats[FUSE_WRITE_LAT], stats[FUSE_WRITE_OPS]);
    stringStats[FUSE_WRITE_LAT_MAX] = formatTime(stats[FUSE_WRITE_LAT_MAX], 1);
    stringStats[META_OPS] = formatOp(stats[META_OPS]);
    stringStats[META_LAT] = formatTime(stats[META_LAT], stats[META_OPS]);
    stringStats[META_LAT_MAX] = formatTime(stats[META_LAT_MAX], 1);
    stringStats[META_OPEN] = formatOp(stats[META_OPEN]);
    stringStats[META_OPEN_LAT] = formatTime(stats[META_OPEN_LAT], stats[META_OPEN]);
    stringStats[META_OPEN_LAT_MAX] = formatTime(stats[META_OPEN_LAT_MAX], 1);
    stringStats[META_STAT] = formatOp(stats[META_STAT] + stats[META_LOOKUP]);
    stringStats[META_STAT_LAT] = formatTime(stats[META_STAT_LAT], stats[META_STAT] + stats[META_LOOKUP]);
    stringStats[META_STAT_LAT_MAX] = formatTime(stats[META_STAT_LAT_MAX], 1);
    stringStats[META_RELEASE] = formatOp(stats[META_RELEASE]);
    stringStats[META_RELEASE_LAT] = formatTime(stats[META_RELEASE_LAT], stats[META_RELEASE]);
    stringStats[META_RELEASE_LAT_MAX] = formatTime(stats[META_RELEASE_LAT_MAX], 1);
    stringStats[META_CREATE] = formatOp(stats[META_CREATE]);
    stringStats[META_CREATE_LAT] = formatTime(stats[META_CREATE_LAT], stats[META_CREATE]);
    stringStats[META_CREATE_LAT_MAX] = formatTime(stats[META_CREATE_LAT_MAX], 1);
    stringStats[BLOCKCACHE_READ] = formatU64(stats[BLOCKCACHE_READ]);
    stringStats[BLOCKCACHE_WRITE] = formatU64(stats[BLOCKCACHE_WRITE]);
    stringStats[OBJ_GET] = formatU64(stats[OBJ_GET]);
    stringStats[OBJ_PUT] = formatU64(stats[OBJ_PUT]);

    return stringStats;
}

void printStatsVector(const std::vector<std::string> &stats)
{
    const std::string &fuseOps = stats[FUSE_OPS];
    std::cout << fuseOps;
    std::cout << std::setw(14 - fuseOps.size()) << stats[FUSE_LAT] << "/" << std::setw(6) << stats[FUSE_LAT_MAX] << "|";
    const std::string &fuseReadOps = stats[FUSE_READ_OPS];
    std::cout << fuseReadOps;
    std::cout << std::setw(13 - fuseReadOps.size()) << stats[FUSE_READ_LAT] << "/" << std::setw(6) << stats[FUSE_READ_LAT_MAX] << " ";
    std::cout << std::setw(6) << stats[FUSE_READ] << "|";
    const std::string &fuseWriteOps = stats[FUSE_WRITE_OPS];
    std::cout << fuseWriteOps;
    std::cout << std::setw(13 - fuseWriteOps.size()) << stats[FUSE_WRITE_LAT] << "/" << std::setw(6) << stats[FUSE_WRITE_LAT_MAX] << " ";
    std::cout << std::setw(6) << stats[FUSE_WRITE] << "|";

    const std::string &metaOps = stats[META_OPS];
    std::cout << metaOps;
    std::cout << std::setw(13 - metaOps.size()) << stats[META_LAT] << "/" << std::setw(6) << stats[META_LAT_MAX] << "|";
    const std::string &metaOpenOps = stats[META_OPEN];
    std::cout << metaOpenOps;
    std::cout << std::setw(13 - metaOpenOps.size()) << stats[META_OPEN_LAT] << "/" << std::setw(6) << stats[META_OPEN_LAT_MAX] << "|";
    const std::string &metaCloseOps = stats[META_RELEASE];
    std::cout << metaCloseOps;
    std::cout << std::setw(13 - metaCloseOps.size()) << stats[META_RELEASE_LAT] << "/" << std::setw(6) << stats[META_RELEASE_LAT_MAX] << "|";
    const std::string &metaStatOps = stats[META_STAT];
    std::cout << metaStatOps;
    std::cout << std::setw(13 - metaStatOps.size()) << stats[META_STAT_LAT] << "/" << std::setw(6) << stats[META_STAT_LAT_MAX] << "|";
    const std::string &metaCreateOps = stats[META_CREATE];
    std::cout << metaCreateOps;
    std::cout << std::setw(13 - metaCreateOps.size()) << stats[META_CREATE_LAT] << "/" << std::setw(6) << stats[META_CREATE_LAT_MAX] << "|";

    std::cout << std::setw(5) << stats[BLOCKCACHE_READ] << " " << std::setw(5) << stats[BLOCKCACHE_WRITE] << "|";
    std::cout << std::setw(5) << stats[OBJ_GET] << " " << std::setw(5) << stats[OBJ_PUT] << "\n";
}