/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "log/logging.h"

#include <dirent.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <climits>
#include <cstring>
#include <filesystem>
#include <string>
#include <ranges>

#include <sys/stat.h>
#include <sys/types.h>

#include "falcon_code.h"

template <typename... Func>
struct overload : Func...
{
    using Func::operator()...;
};
template <typename... Func>
overload(Func...) -> overload<Func...>;

std::ostream &FalconLog::Stream()
{
    overload logStream{[](StdLog &logger) -> std::ostream & { return logger.Stream(); },
                       [](ExternalLog &logger) -> std::ostream & { return logger.Stream(); },
                       [](google::LogMessage &logger) -> std::ostream & { return logger.stream(); }};

    return std::visit(logStream, logProvider);
}

FalconLog::FalconLog(const char *file_name, int32_t line_number, FalconLogLevel severity)
    : isEnabled(severity >= severityThreshold)
{
    if (isEnabled) {
        switch (defaultLogger) {
        case STD_LOGGER:
            logProvider.emplace<StdLog>() << GetLogPrefix(file_name, line_number, severity);
            break;
        case EXTERNAL_LOGGER:
            logProvider.emplace<ExternalLog>(file_name, line_number, severity);
            break;
        case GLOGGER:
            logProvider.emplace<google::LogMessage>(file_name, line_number, glogSeverityMap[severity]).stream()
                << "[FALCON]" << severityPrefixMap[severity] << " ";
            break;
        }
    }
}

FalconLog *FalconLog::GetInstance()
{
    static FalconLog instance;
    return &instance;
}

bool FalconLog::IsEnabled() const { return isEnabled; }

int32_t FalconLog::InitLog(FalconLogLevel initSeverityThreshold = LOG_INFO,
                           Logger logger = STD_LOGGER,
                           const std::string &initLogDir,
                           const std::string &name,
                           uint32_t logMaxSize,
                           uint32_t initReservedNum,
                           uint32_t initReservedTime)
{
    if (!std::filesystem::exists(initLogDir)) {
        FALCON_LOG(LOG_ERROR) << "InitLog(): initLogDir " << initLogDir << " does not exist!";
        return FALCON_IEC_INIT_LOG_FAILED;
    }
    logDir = initLogDir;
    reservedNum = initReservedNum;
    reservedTime = initReservedTime;
    glogSeverityMap = {{LOG_TRACE, google::INFO},
                       {LOG_DEBUG, google::INFO},
                       {LOG_INFO, google::INFO},
                       {LOG_WARNING, google::WARNING},
                       {LOG_ERROR, google::ERROR},
                       {LOG_FATAL, google::FATAL}};

    if (logger == GLOGGER) {
        if (initLogDir.empty() || name.empty() || logMaxSize == 0) {
            FALCON_LOG(LOG_ERROR) << "Init log failed, param error, initLogDir: " << initLogDir << " name: " << name
                                  << " logMaxSize: " << logMaxSize;
            return FALCON_IEC_INIT_LOG_FAILED;
        }

        FLAGS_minloglevel = glogSeverityMap[initSeverityThreshold];
        FLAGS_stderrthreshold = google::FATAL;
        FLAGS_log_dir = initLogDir;
        FLAGS_max_log_size = logMaxSize;
        FLAGS_logbufsecs = 0;

        google::InitGoogleLogging(name.c_str());

        auto prefix = initLogDir + "/" + name;
        google::SetLogDestination(google::INFO, (prefix + ".INFO.").c_str());
        google::SetLogDestination(google::WARNING, (prefix + ".WARNING.").c_str());
        google::SetLogDestination(google::ERROR, (prefix + ".ERROR.").c_str());
        google::SetLogDestination(google::FATAL, (prefix + ".FATAL.").c_str());
    }

    severityThreshold = initSeverityThreshold;
    defaultLogger = logger;

    cleanLogfileThread = std::jthread([this](std::stop_token stoken) { this->Cleaner(stoken); });
    return OK;
}

std::string FalconLog::GetLogPrefix(const char *fileName, int lineNumber, FalconLogLevel severity)
{
    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()) % 1000000;

    std::time_t tt = std::chrono::system_clock::to_time_t(now);

    std::tm time_tm;
    localtime_r(&tt, &time_tm);

    std::ostringstream oss;
    oss << std::setfill('0') 
        << std::setw(4) << (time_tm.tm_year + 1900) << "-"
        << std::setw(2) << (time_tm.tm_mon + 1) << "-"
        << std::setw(2) << time_tm.tm_mday << " "
        << std::setw(2) << time_tm.tm_hour << ":"
        << std::setw(2) << time_tm.tm_min << ":"
        << std::setw(2) << time_tm.tm_sec << " "
        << std::setw(6) << micros.count();
    std::string logTime = oss.str();

    std::string_view baseFileName = std::strrchr(fileName, '/') ? std::strrchr(fileName, '/') + 1 : fileName;
    return "[" + logTime + "] [FALCON] " + std::string(severityPrefixMap[severity]) + " [" + 
           std::string(baseFileName) + ":" + std::to_string(lineNumber) + "] ";
}

FalconLogLevel FalconLog::GetFalconLogLevel() { return severityThreshold; }

void FalconLog::SetFalconLogLevel(FalconLogLevel level) { severityThreshold = level; }

void FalconLog::SetExternalLogger(const FalconLogHandler &logger)
{
    defaultLogger = EXTERNAL_LOGGER;
    ExternalLog::externalLogger_ = logger;
}

void FalconLog::Cleaner(std::stop_token stoken)
{
    while (!stoken.stop_requested()) {
        Clean();
        // 每 1 秒检查一次停止信号，累计等待 10 分钟
        for (int i = 0; i < 600 && !stoken.stop_requested(); ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}

void FalconLog::Clean()
{
    // 定义日志级别名称集合
    constexpr std::array severityLogNames{"falcon.INFO", "falcon.WARNING", "falcon.ERROR", "falcon.FATAL"};

    // 使用函数式风格处理符号链接
    auto excludeFiles = severityLogNames |
                        std::views::transform([this](const auto &name) { return std::string(logDir) + "/" + std::string(name); }) |
                        std::views::filter([](const auto &path) { return access(path.c_str(), F_OK) == 0; }) |
                        std::views::transform([](const auto &path) {
                            char buf[PATH_MAX];
                            ssize_t len = readlink(path.c_str(), buf, sizeof(buf) - 1);
                            if (len < 0) {
                                FALCON_LOG(LOG_ERROR) << path << ": " << std::strerror(errno);
                                return std::optional<std::string>{};
                            }
                            buf[len] = '\0';
                            return std::optional<std::string>{buf};
                        }) |
                        std::views::filter([](const auto &opt) { return opt.has_value(); }) |
                        std::views::transform([](const auto &opt) { return opt.value(); });

    // 转换为unordered_set并删除日志文件
    DeleteLogFiles({excludeFiles.begin(), excludeFiles.end()});
}

bool FalconLog::HasPrefix(const std::string &str, const std::string &prefix)
{
    if (str.size() < prefix.size()) {
        return false;
    }

    return std::equal(prefix.begin(), prefix.end(), str.begin());
}

void FalconLog::DeleteLogFiles(const std::unordered_set<std::string> &excludeFiles)
{
    // Open directory and check for errors
    auto dirCloser = [](DIR *dir) {
        if (dir)
            closedir(dir);
    };
    std::unique_ptr<DIR, decltype(dirCloser)> dirPtr(opendir(logDir.c_str()), dirCloser);
    if (!dirPtr) {
        FALCON_LOG(LOG_ERROR) << "opendir failed for " << logDir;
        return;
    }

    const time_t now = time(nullptr);
    const auto reserved_seconds = reservedTime * 3600; // Convert hours to seconds

    auto processEntry = [&](const dirent *entity) -> std::optional<FileInfo> {
        // 跳过特殊目录项和符号链接
        if (strcmp(entity->d_name, ".") == 0 || strcmp(entity->d_name, "..") == 0 || entity->d_type == DT_LNK) {
            return std::nullopt;
        }

        // 只处理普通文件且以"falcon"开头且不在排除列表中的文件
        if (entity->d_type != DT_REG || !HasPrefix(entity->d_name, "falcon") || excludeFiles.contains(entity->d_name)) {
            return std::nullopt;
        }

        const std::string filePath = std::string(logDir) + "/" + entity->d_name;

        // 对于可能是符号链接的情况，再次检查文件类型
        struct stat st;
        if (lstat(filePath.c_str(), &st) != 0) {
            FALCON_LOG(LOG_ERROR) << filePath << " lstat failed: " << std::strerror(errno);
            return std::nullopt;
        }

        // 确保不是符号链接（即使d_type可能不准确）
        if (S_ISLNK(st.st_mode)) {
            return std::nullopt;
        }

        // 检查是否是普通文件
        if (!S_ISREG(st.st_mode)) {
            return std::nullopt;
        }

        if (now - st.st_mtime <= reserved_seconds) {
            return FileInfo{filePath, st.st_mtime};
        }

        if (unlink(filePath.c_str()) == 0) {
            FALCON_LOG(LOG_INFO) << "Deleted (older than reserved_time_): " << filePath;
        } else {
            FALCON_LOG(LOG_ERROR) << "Failed to delete " << filePath << ": " << std::strerror(errno);
        }
        return std::nullopt;
    };

    // Collect remaining files
    std::vector<FileInfo> remainFiles;
    for (dirent *entity; (entity = readdir(dirPtr.get()));) {
        if (auto fileInfo = processEntry(entity)) {
            remainFiles.push_back(*fileInfo);
        }
    }

    // Handle remaining files exceeding count limit
    if (remainFiles.size() > reservedNum) {
        std::ranges::sort(remainFiles, {}, &FileInfo::mtime);

        auto filesToDelete = remainFiles | std::views::take(remainFiles.size() - reservedNum);

        for (const auto &file : filesToDelete) {
            if (unlink(file.filePath.c_str()) == 0) {
                FALCON_LOG(LOG_INFO) << "Deleted (exceeds max count): " << file.filePath;
            } else {
                FALCON_LOG(LOG_ERROR) << "Failed to delete " << file.filePath << ": " << std::strerror(errno);
            }
        }
    }
}
