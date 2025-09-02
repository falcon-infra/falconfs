#include <print>
#include <thread>
#include <vector>
#include <iostream>
#include <sys/time.h>
#include <stdlib.h>
#include "kv_utils.h"
#include "log/logging.h"

static constexpr uint64_t DEFAULT_PAGE_SIZE = 4096UL;

// TODO 后续适配Falcon Log
void Print(int level, const char *msg)
{
    const char *levelStr[] = { "DEBUG", "INFO", "WARN", "ERROR" };

    struct timeval tv {};
    char strTime[24];
    time_t timeStamp = tv.tv_sec;
    struct tm localTime {};
    struct tm *resultTime = localtime_r(&timeStamp, &localTime);
    if ((resultTime != nullptr) && (strftime(strTime, sizeof strTime, "%Y-%m-%d %H:%M:%S.", resultTime) != 0)) {
        FALCON_LOG(LOG_INFO) << strTime << tv.tv_usec << " " << levelStr[level] << " " << msg << '\n';
    } else {
        FALCON_LOG(LOG_INFO) << "Invalid time trace " << tv.tv_usec << " " << levelStr[level] << " " << msg << '\n';
    }
}

void MultiReservePhysicalPage(uint8_t *mappedAddress, uint64_t size)
{
    // auto cpuCores = std::thread::hardware_concurrency();
    // auto taskThreads = cpuCores / SPLIT_OF_CPU_CORES_FLAG;
    // TODO 暂时设置16线程 踩物理页
    uint8_t taskThreads = 16;
    /* reserver physical page task */
    auto reserveTask = [](uint64_t reserveSize, uint8_t *startPos) {
        uint64_t setLength = 0UL;
        while (setLength < reserveSize) {
            *startPos = 0;
            setLength += DEFAULT_PAGE_SIZE;
            startPos += DEFAULT_PAGE_SIZE;
        }
    };

    if (size < DEFAULT_PAGE_SIZE * taskThreads || taskThreads == 0) {
        reserveTask(size, mappedAddress);
        return;
    }

    auto completePageCount = size / DEFAULT_PAGE_SIZE;
    auto everyReservePages = completePageCount / taskThreads;
    auto everyReserveSize = everyReservePages * DEFAULT_PAGE_SIZE;

    std::vector<std::thread> threadPool;
    for (uint8_t i = 0; i < taskThreads; ++i) {
        auto startPos = mappedAddress + i * everyReserveSize;
        if (i == taskThreads - 1) {
            everyReserveSize = size - i * everyReserveSize;
        }
        threadPool.emplace_back(reserveTask, everyReserveSize, startPos);
    }

    /* wait all reserve */
    for (auto &item : threadPool) {
        item.join();
    }
}