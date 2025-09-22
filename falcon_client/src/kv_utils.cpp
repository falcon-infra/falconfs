#include <print>
#include <thread>
#include <vector>
#include <iostream>
#include <sys/time.h>
#include <stdlib.h>
#include "kv_utils.h"
#include "log/logging.h"

static constexpr uint64_t DEFAULT_PAGE_SIZE = 4096UL;

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