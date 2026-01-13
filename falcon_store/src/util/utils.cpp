/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "util/utils.h"

#include <string>
#include <random>

std::string rootPath;
int totalDirectory;
uint32_t FALCON_BLOCK_SIZE;
uint32_t READ_BIGFILE_SIZE;

void SetRootPath(std::string str) { rootPath = str; }

void SetTotalDirectory(int num) { totalDirectory = num; }

std::string GetFilePath(uint64_t inodeId)
{
    int directoryId = inodeId % totalDirectory;
    return std::string(rootPath) + "/" + std::to_string(directoryId) + "/" + std::to_string(inodeId) + "-large";
}

int GenerateRandom(int minValue, int maxValue)
{
    static std::random_device seed;
    static std::ranlux48 engine(seed());
    static std::uniform_int_distribution<> distrib(minValue, maxValue);
    int randomValue = distrib(engine);
    return randomValue;
}

std::optional<std::string> GetUserName()
{
    if (const char *envUserName = std::getenv("USER")) {
        return envUserName;
    }
    return std::nullopt;
}

std::optional<std::string_view> SplitIp(std::string_view ipPort) { return ipPort.substr(0, ipPort.find(':')); }

std::expected<std::string, std::string> GetPodIPPort()
{
    if (const char *podIP = std::getenv("POD_IP")) {
        const char *brpcPort = std::getenv("BRPC_PORT");
        return std::string(podIP) + ":" + (brpcPort ? brpcPort : "56039");
    }
    return std::unexpected("POD_IP environment variable not set");
}

float GetStorageThreshold(bool persistToStorage)
{
    const char *storageThreshold = std::getenv("STORAGE_THRESHOLD");
    if (storageThreshold == nullptr) {
        if (persistToStorage) {
            return 0.8;
        } else {
            return 1;
        }
    }
    return atof(storageThreshold);
}

int GetParentPathLevel()
{
    const char *parentPathLevel = std::getenv("PARENT_PATH_LEVEL");
    if (parentPathLevel == nullptr) {
        return -1;
    }
    return atof(parentPathLevel);
}
