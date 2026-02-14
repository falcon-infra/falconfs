/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#ifdef WITH_OBS_STORAGE

#include <atomic>
#include <cstdlib>

#include "eSDKOBS.h"

#include "storage.h"

#define REQUEST_MAX_COUNT (1024)
#define LIST_OBJECT_MAX_COUNT (1000)
#define DOWNLOAD_LIMIT (819200)
#define OBS_COMMON_LEN (256)
#define SECTOR_SIZE (512)
#define DEFAULT_OBS_BLK_SIZE (4096)
#define RETRY_NUM (10)
#define TIME_BASE (2)
#define TIME_INTERVAL (50)
#define TIME_UNIT (1000)

constexpr uint64_t UPLOAD_SLICE_SIZE = 512L * 1024 * 1024;

class OBSStorage : public Storage {
  private:
    std::atomic<bool> isInit{false};
    OBSStorage() = default;

  public:
    ~OBSStorage() noexcept override = default;

  private:
    void InitObsOptions(obs_options &option);
    bool IsNeedRetry(obs_status status);
    void DoRetry(obs_status status, int &retry);
    obs_status ObsUploadFile(const std::string &objectKey, const std::string &filePath, uint64_t contentLen);
    obs_status ObsPutObject(const std::string &objectKey, const std::string &filePath, uint64_t contentLen);
    int HeadBucket();
    int GetStorageInfo(size_t &objNum, size_t &cap);
    int GetQuota(uint64_t &quota);
    std::string hostName;
    std::string bucketName;
    std::string accessKey;
    std::string secretAccessKey;
    bool isHttps{true};

  public:
    static OBSStorage *GetInstance();
    void DeleteInstance() override;
    int Init() override;

    ssize_t ReadObject(const std::string &objectKey, uint64_t offset, uint64_t size, int fd, char *destBuffer) override;
    int PutFile(const std::string &objectKey, const std::string &filePath) override;
    ssize_t
    PutBuffer(const std::string &objectKey, const char *buf, const uint64_t size, const uint64_t offset) override;
    int DeleteObject(const std::string &objectKey) override;
    int CopyObject(const std::string &fromPath, const std::string &toPath) override;
    int StatFs(struct statvfs *vfsbuf) override;
};

#endif
