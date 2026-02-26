/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <iostream>
#include <ostream>
#include <cstdarg>
#include <cstdio>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <variant>

#include "glog/logging.h"

#include "falcon_definition.h"

enum Logger { STD_LOGGER = 1, GLOGGER = 2, EXTERNAL_LOGGER = 3 };

#define FILENAME_ (strrchr(__FILE__, '/') ? (strrchr(__FILE__, '/') + 1) : __FILE__)
#define FALCON_LOG_INTERNAL(level) FalconLog(FILENAME_, __LINE__, level)
#define FALCON_LOG_CM(file, line, level) FalconLog(file, line, level)
#define FALCON_LOG(level) FALCON_LOG_INTERNAL(FalconLogLevel::level)

#define RETURN_ON_ERROR(expr)                                         \
    do {                                                              \
        auto exprStatus = (expr);                                     \
        if (exprStatus != FALCON_SUCCESS) {                           \
            FALCON_LOG(LOG_ERROR) << #expr " failed, " << exprStatus; \
            return exprStatus;                                        \
        }                                                             \
    } while (0)

class StdLog {
  public:
    ~StdLog()
    {
        if (has_logged_) {
            std::cout << std::endl;
        }
    }

    std::ostream &Stream()
    {
        has_logged_ = true;
        return std::cout;
    }

    template <typename T>
    StdLog &operator<<(const T &t)
    {
        Stream() << t;
        return *this;
    }

  private:
    bool has_logged_{false};
};

class ExternalLog {
  public:
    ExternalLog(const char *fileName, int32_t lineNumber, FalconLogLevel severity)
        : file_name_(fileName),
          line_number_(lineNumber),
          severity_(severity)
    {
    }

    ~ExternalLog()
    {
        if (has_logged_) {
            externalLogger_(severity_, file_name_, line_number_, ("[FALCON] " + os_.str()).c_str());
        }
    }

    std::ostream &Stream()
    {
        has_logged_ = true;
        return os_;
    }

    template <typename T>
    ExternalLog &operator<<(const T &t)
    {
        Stream() << t;
        return *this;
    }

    inline static FalconLogHandler externalLogger_;

  private:
    bool has_logged_{false};
    std::ostringstream os_;
    const char *file_name_;
    int32_t line_number_;
    FalconLogLevel severity_;
};

using LogProvider = std::variant<StdLog, google::LogMessage, ExternalLog>;
class FalconLog {
  public:
    FalconLog() = default;
    FalconLog(const char *file_name, int32_t line_number, FalconLogLevel severity);

    bool IsEnabled() const;

    template <typename T>
    FalconLog &operator<<(const T &t)
    {
        if (IsEnabled()) {
            Stream() << t;
        }
        return *this;
    }

    int32_t InitLog(FalconLogLevel initSeverityThreshold,
                    Logger logger,
                    const std::string &initLogDir = "",
                    const std::string &name = "",
                    uint32_t logMaxSize = 0,
                    uint32_t initReservedNum = 4,
                    uint32_t initReservedTime = 24);

    static FalconLogLevel GetFalconLogLevel();

    static void SetFalconLogLevel(FalconLogLevel level);

    static std::string GetLogPrefix(const char *fileName, int lineNumber, FalconLogLevel severity);

    static void SetExternalLogger(const FalconLogHandler &logger);

    static FalconLog *GetInstance();

  protected:
    std::ostream &Stream();

  private:
    void Cleaner(std::stop_token stoken);
    void Clean();
    bool HasPrefix(const std::string &str, const std::string &prefix);
    void DeleteLogFiles(const std::unordered_set<std::string> &excludeFiles);
    struct FileInfo
    {
        std::string filePath;
        time_t mtime;

        FileInfo(const std::string &path, time_t time)
            : filePath(path),
              mtime(time)
        {
        }
    };
    inline static FalconLogLevel severityThreshold{LOG_INFO};
    inline static Logger defaultLogger{STD_LOGGER};
    inline static std::unordered_map<int, int> glogSeverityMap;
    inline static std::unordered_map<int, std::string> severityPrefixMap{{LOG_TRACE, "[TRACE]"},
                                                                         {LOG_DEBUG, "[DEBUG]"},
                                                                         {LOG_INFO, "[INFO]"},
                                                                         {LOG_WARNING, "[WARNING]"},
                                                                         {LOG_ERROR, "[ERROR]"},
                                                                         {LOG_FATAL, "[FATAL]"}};
    LogProvider logProvider;
    bool isEnabled;
    std::string logDir;
    uint32_t reservedNum;
    uint32_t reservedTime; // unit : h
    std::jthread cleanLogfileThread;
};

inline void FalconLogPrintfInternal(const char *file, int line, FalconLogLevel level, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    va_list args_copy;
    va_copy(args_copy, args);
    int len = std::vsnprintf(nullptr, 0, fmt, args_copy);
    va_end(args_copy);
    if (len < 0) {
        va_end(args);
        FALCON_LOG_CM(file, line, level) << "printf format error: " << fmt;
        return;
    }
    std::string buf;
    buf.resize(static_cast<size_t>(len) + 1);
    std::vsnprintf(buf.data(), buf.size(), fmt, args);
    va_end(args);
    if (!buf.empty() && buf.back() == '\0') {
        buf.pop_back();
    }
    while (!buf.empty() && (buf.back() == '\n' || buf.back() == '\r')) {
        buf.pop_back();
    }
    FALCON_LOG_CM(file, line, level) << buf;
}

#define FALCON_LOG_PRINTF(level, fmt, ...) \
    FalconLogPrintfInternal(FILENAME_, __LINE__, FalconLogLevel::level, (fmt), ##__VA_ARGS__)
