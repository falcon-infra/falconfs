/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifdef WITH_OBS_STORAGE

#include "storage/obs_storage.h"

#include <fcntl.h>
#include <math.h>
#include <cstring>
#include <time.h>
#include <unistd.h>

#include "log/logging.h"
#include "stats/falcon_stats.h"

struct NormalBackType
{
    const obs_error_details *error = nullptr;
    obs_status retStatus = OBS_STATUS_OK;
};

struct PutFileCallbackType
{
    FILE *infile = nullptr;
    uint64_t contentLength = 0;
    obs_status retStatus = OBS_STATUS_OK;
    const obs_error_details *error = nullptr;
};

struct PutBufCallbackType
{
    const char *putBuffer = nullptr;
    uint64_t bufferSize = 0;
    uint64_t curOffset = 0;
    obs_status retStatus = OBS_STATUS_OK;
    const obs_error_details *error = nullptr;
};

struct GetObjectCallbackType
{
    int fd = 0;
    char *destBuffer = nullptr;
    size_t destBuffSize = 0;
    int realSize = 0;
    off_t offset = 0;
    obs_status retStatus = OBS_STATUS_OK;
    const obs_error_details *error = nullptr;
};

OBSStorage *OBSStorage::GetInstance()
{
    static OBSStorage m_singleton;
    return &m_singleton;
}

int OBSStorage::Init()
{
    if (!isInit.load()) {
        int ret = 0;
        ret = obs_initialize(OBS_INIT_ALL);
        if (ret != 0) {
            FALCON_LOG(LOG_ERROR) << "obs initialize fail, ret is " << ret;
            return ret;
        }
        ret = set_online_request_max_count(REQUEST_MAX_COUNT);
        if (ret != 0) {
            FALCON_LOG(LOG_ERROR) << "obs set online request max count fail, ret is " << ret;
            return ret;
        }
        hostName = std::getenv("OBS_HOST");

        auto iPos = hostName.find("://");
        if (iPos != std::string::npos) {
            isHttps = hostName.find("https") != std::string::npos;
            hostName = hostName.substr(iPos + 3, hostName.length() - iPos - 3);
        }

        bucketName = std::getenv("OBS_BUCKET");
        accessKey = std::getenv("OBS_AK");
        secretAccessKey = std::getenv("OBS_SK");
        if (hostName.empty() || bucketName.empty() || accessKey.empty() || secretAccessKey.empty()) {
            FALCON_LOG(LOG_ERROR) << "Not enough info given to access obs";
            return -1;
        }
        if (HeadBucket() != 0) {
            FALCON_LOG(LOG_ERROR) << "Access obs bucket failed";
            return -1;
        }
        isInit.store(true);
        FALCON_LOG(LOG_INFO) << "successfully init obs, host name is " << hostName;
    }
    return 0;
}

void OBSStorage::DeleteInstance()
{
    if (isInit.load()) {
        obs_deinitialize();
        isInit.store(false);
    }
}

bool OBSStorage::IsNeedRetry(obs_status status)
{
    switch (status) {
    case OBS_STATUS_NameLookupError:
    case OBS_STATUS_FailedToConnect:
    case OBS_STATUS_ConnectionFailed:
    case OBS_STATUS_InternalError:
    case OBS_STATUS_AbortedByCallback:
    case OBS_STATUS_RequestTimeout:
    case OBS_STATUS_PartialFile:
    case OBS_STATUS_NoToken:
    case OBS_STATUS_HttpErrorForbidden:
        return true;
    default:
        return false;
    }
}

void OBSStorage::DoRetry(obs_status status, int &retry)
{
    if (IsNeedRetry(status)) {
        if (retry > 0) {
            uint64_t wait_time = (uint64_t)pow(TIME_BASE, RETRY_NUM - retry + 1) * TIME_INTERVAL;
            usleep(wait_time * TIME_UNIT);
        }
        --retry;
    } else {
        retry = 0;
    }
}

void common_error_handle(const obs_error_details *error)
{
    if (error && error->message) {
        FALCON_LOG_PRINTF(LOG_ERROR, "Error Message: \n  %s\n", error->message);
    }
    if (error && error->resource) {
        FALCON_LOG_PRINTF(LOG_ERROR, "Error resource: \n  %s\n", error->resource);
    }
    if (error && error->further_details) {
        FALCON_LOG_PRINTF(LOG_ERROR, "Error further_details: \n  %s\n", error->further_details);
    }
    if (error && error->extra_details_count) {
        int i;
        for (i = 0; i < error->extra_details_count; i++) {
            FALCON_LOG_PRINTF(LOG_ERROR, "Error Extra Detail(%d):\n  %s:%s\n",
                              i,
                              error->extra_details[i].name,
                              error->extra_details[i].value);
        }
    }
}

void response_complete_callback(obs_status status, const obs_error_details *error, void *callbackData)
{
    if (callbackData) {
        auto *ret_status = (obs_status *)callbackData;
        *ret_status = status;
    }
    common_error_handle(error);
}

void NormalCompleteCallback(obs_status status, const obs_error_details *error, void *callbackData)
{
    if (callbackData) {
        auto *cbdata = (NormalBackType *)callbackData;
        cbdata->retStatus = status;
        if (error && status != OBS_STATUS_OK) {
            cbdata->error = error;
        }
    }
}

obs_status NormalPropertiesCallback(const obs_response_properties *properties, void * /*callbackData*/)
{
    if (properties == nullptr) {
        return OBS_STATUS_ErrorUnknown;
    }
    return OBS_STATUS_OK;
}

void PutFileCompleteCallback(obs_status status, const obs_error_details *error, void *callbackData)
{
    if (callbackData) {
        auto *data = static_cast<PutFileCallbackType *>(callbackData);
        data->retStatus = status;
        if (error && status != OBS_STATUS_OK) {
            data->error = error;
        }
    }
}

void PutBufCompleteCallback(obs_status status, const obs_error_details *error, void *callbackData)
{
    if (callbackData) {
        auto *data = static_cast<PutBufCallbackType *>(callbackData);
        data->retStatus = status;
        if (error && status != OBS_STATUS_OK) {
            data->error = error;
        }
    }
}

int PutFileCallback(int bufferSize, char *buffer, void *callbackData)
{
    int ret = 0;
    if ((callbackData == nullptr) || (buffer == nullptr)) {
        return OBS_STATUS_ErrorUnknown;
    }
    auto *data = static_cast<PutFileCallbackType *>(callbackData);
    if (data->infile == nullptr) {
        return OBS_STATUS_ErrorUnknown;
    }
    if (data->contentLength) {
        int toRead = ((data->contentLength > (unsigned)bufferSize) ? (unsigned)bufferSize : data->contentLength);
        ret = fread(buffer, 1, toRead, data->infile);
        FalconStats::GetInstance().stats[OBJ_PUT] += toRead;
    }
    data->contentLength -= ret;
    return ret;
}

int PutBufCallBack(int bufferSize, char *buffer, void *callbackData)
{
    int toRead = 0;
    if ((callbackData == nullptr) || (buffer == nullptr)) {
        return OBS_STATUS_ErrorUnknown;
    }
    auto *data = static_cast<PutBufCallbackType *>(callbackData);
    if (data->putBuffer == nullptr) {
        return OBS_STATUS_ErrorUnknown;
    }
    if (data->bufferSize) {
        toRead = ((data->bufferSize > (unsigned)bufferSize) ? (unsigned)bufferSize : data->bufferSize);

        (void)memcpy(buffer, data->putBuffer + data->curOffset, toRead);
        FalconStats::GetInstance().stats[OBJ_PUT] += toRead;
    }
    data->bufferSize -= toRead;
    data->curOffset += toRead;
    return toRead;
}

void GetObjectCompleteCallback(obs_status status, const obs_error_details *error, void *callbackData)
{
    if (callbackData) {
        auto *data = static_cast<GetObjectCallbackType *>(callbackData);
        data->retStatus = status;
        if (error && status != OBS_STATUS_OK) {
            data->error = error;
        }
    }
}

obs_status GetObjectDataCallback(int bufferSize, const char *buffer, void *callbackData)
{
    if ((callbackData == nullptr) || (buffer == nullptr)) {
        return OBS_STATUS_ErrorUnknown;
    }
    auto *data = static_cast<GetObjectCallbackType *>(callbackData);
    data->realSize += bufferSize;

    if (data->destBuffer != nullptr) {
        FalconStats::GetInstance().stats[OBJ_GET] += bufferSize;
        (void)memcpy(data->destBuffer + data->offset, buffer, bufferSize);
    }
    if (data->fd != -1) {
        FalconStats::GetInstance().stats[BLOCKCACHE_WRITE] += bufferSize;
        if (pwrite(data->fd, buffer, bufferSize, data->offset) == -1) {
            return OBS_STATUS_AbortedByCallback;
        }
    }
    data->offset += bufferSize;
    return OBS_STATUS_OK;
}

ssize_t OBSStorage::ReadObject(const std::string &objectKey, uint64_t offset, uint64_t size, int fd, char *destBuffer)
{
    obs_options option;
    InitObsOptions(option);

    obs_object_info objectInfo;
    (void)memset(&objectInfo, 0, sizeof(objectInfo));

    objectInfo.key = const_cast<char *>(objectKey.c_str());
    objectInfo.version_id = nullptr;
    GetObjectCallbackType data;
    data.retStatus = OBS_STATUS_BUTT;
    data.fd = fd;
    data.offset = 0;
    data.destBuffer = destBuffer;
    data.realSize = 0;
    data.destBuffSize = size;

    obs_get_conditions getcondition;
    (void)memset(&getcondition, 0, sizeof(getcondition));

    init_get_properties(&getcondition);
    // The starting position of the reading
    getcondition.start_byte = offset;
    // Read length, default is 0: read to the end of the object
    getcondition.byte_count = size;
    obs_get_object_handler getObjectHandler = {{&NormalPropertiesCallback, &GetObjectCompleteCallback},
                                               &GetObjectDataCallback};
    ssize_t ret = 0;
    int retryCount = RETRY_NUM;
    while (retryCount > 0) {
        get_object(&option, &objectInfo, &getcondition, nullptr, &getObjectHandler, &data);
        if (OBS_STATUS_OK == data.retStatus) {
            ret = data.realSize;
        } else {
            FALCON_LOG(LOG_ERROR) << "ReadObject() " << objectKey << " failed: " << data.retStatus;
            FALCON_LOG(LOG_ERROR) << obs_get_status_name(data.retStatus);
            ret = -1;
        }
        DoRetry(data.retStatus, retryCount);
    }
    return ret;
}

uint64_t OpenFileGetLength(const std::string &localFile)
{
    struct stat statbuf;
    if (stat(localFile.c_str(), &statbuf) == -1) {
        return 0;
    }
    return statbuf.st_size;
}

int OBSStorage::PutFile(const std::string &objectKey, const std::string &filePath)
{
    uint64_t contentLen = OpenFileGetLength(filePath);
    obs_status retStatus = OBS_STATUS_BUTT;
    if (contentLen < UPLOAD_SLICE_SIZE) {
        retStatus = ObsPutObject(objectKey, filePath, contentLen);
    } else {
        retStatus = ObsUploadFile(objectKey, filePath, contentLen);
    }
    return static_cast<int>(retStatus);
}

obs_status OBSStorage::ObsPutObject(const std::string &objectKey, const std::string &filePath, uint64_t contentLen)
{
    // Initialize option
    obs_options option;
    InitObsOptions(option);

    // Initialize upload object properties
    obs_put_properties putProperties;
    init_put_properties(&putProperties);

    // Initialize the structure that stores the uploaded data
    PutFileCallbackType data;
    if (!(data.infile = fopen(filePath.c_str(), "rb"))) {
        perror(nullptr);
    }
    data.contentLength = contentLen;

    // Set callback function
    obs_put_object_handler putObjectHandler = {{&NormalPropertiesCallback, &PutFileCompleteCallback},
                                               &PutFileCallback,
                                               nullptr};

    int retryCount = RETRY_NUM;
    while (retryCount > 0) {
        put_object(&option,
                   const_cast<char *>(objectKey.c_str()),
                   contentLen,
                   &putProperties,
                   nullptr,
                   &putObjectHandler,
                   &data);
        DoRetry(data.retStatus, retryCount);
    }
    if (data.infile != nullptr) {
        fclose(data.infile);
        data.infile = nullptr;
    }
    if (OBS_STATUS_OK != data.retStatus) {
        FALCON_LOG(LOG_ERROR) << "ObsPutObject " << objectKey << " error: " << data.retStatus;
        FALCON_LOG(LOG_ERROR) << obs_get_status_name(data.retStatus);
    }
    return data.retStatus;
}

ssize_t OBSStorage::PutBuffer(const std::string &objectKey, const char *buf, const uint64_t size, const uint64_t offset)
{
    // Initialize option
    obs_options option;
    InitObsOptions(option);

    // Initialize upload object properties
    obs_put_properties putProperties;
    init_put_properties(&putProperties);

    // Initialize the structure that stores the uploaded data
    PutBufCallbackType data;
    // Open the file and get the file length
    uint64_t contentLen = 0;
    if (buf) {
        contentLen = size;
        data.putBuffer = buf;
        data.bufferSize = contentLen;
        data.curOffset = offset;
    }

    // Set callback function
    obs_put_object_handler putObjectHandler = {{&NormalPropertiesCallback, &PutBufCompleteCallback},
                                               &PutBufCallBack,
                                               nullptr};
    put_object(&option,
               const_cast<char *>(objectKey.c_str()),
               contentLen,
               &putProperties,
               nullptr,
               &putObjectHandler,
               &data);
    if (OBS_STATUS_OK == data.retStatus) {
        return static_cast<int>(contentLen);
    } else {
        FALCON_LOG(LOG_ERROR) << "Obs PutBuffer " << objectKey << " error: " << data.retStatus;
        FALCON_LOG(LOG_ERROR) << obs_get_status_name(data.retStatus);
        return -1;
    }
}

void ResponseCompleteCallbackForMultiTask(obs_status status, const obs_error_details * /*error*/, void *callbackData)
{
    if (callbackData) {
        auto *retStatus = static_cast<obs_status *>(callbackData);
        // assume single threaded here
        // if multi-threaded later, consider race condition here
        if (*retStatus == OBS_STATUS_OK || *retStatus == OBS_STATUS_BUTT) {
            *retStatus = status;
        }
    }
}

void UploadFileResultCallback(obs_status status,
                              char * /*resultMsg*/,
                              int /*partCountReturn*/,
                              obs_upload_file_part_info * /*uploadInfoList*/,
                              void *callbackData)
{
    if (callbackData) {
        auto *retStatus = (obs_status *)callbackData;
        *retStatus = status;
    }
}

// 断点续传上传
obs_status OBSStorage::ObsUploadFile(const std::string &objectKey, const std::string &filePath, uint64_t contentLen)
{
    // Initialize option
    obs_options option;
    InitObsOptions(option);

    obs_upload_file_configuration uploadFileInfo;
    (void)memset(&uploadFileInfo, 0, sizeof(obs_upload_file_configuration));
    uploadFileInfo.check_point_file = nullptr;
    uploadFileInfo.enable_check_point = 1;
    uploadFileInfo.part_size = UPLOAD_SLICE_SIZE;
    uploadFileInfo.task_num = 4;
    uploadFileInfo.upload_file = const_cast<char *>(filePath.c_str());
    FalconStats::GetInstance().stats[OBJ_PUT] += contentLen;

    // callback
    obs_upload_file_response_handler handler = {{&NormalPropertiesCallback, &ResponseCompleteCallbackForMultiTask},
                                                &UploadFileResultCallback,
                                                nullptr};

    obs_upload_file_server_callback server_callback;
    init_server_callback(&server_callback);

    obs_status retStatus = OBS_STATUS_BUTT;
    int retryCount = RETRY_NUM;
    while (retryCount > 0) {
        initialize_break_point_lock();
        upload_file(&option,
                    const_cast<char *>(objectKey.c_str()),
                    nullptr,
                    &uploadFileInfo,
                    server_callback,
                    &handler,
                    &retStatus);
        deinitialize_break_point_lock();
        DoRetry(retStatus, retryCount);
    }
    if (OBS_STATUS_OK != retStatus) {
        FALCON_LOG(LOG_ERROR) << "ObsUploadFile " << objectKey << " error: " << retStatus;
        FALCON_LOG(LOG_ERROR) << obs_get_status_name(retStatus);
    }
    return retStatus;
}

int OBSStorage::DeleteObject(const std::string &objectKey)
{
    // Initialize option
    obs_options option;
    InitObsOptions(option);

    obs_object_info objectInfo;
    (void)memset(&objectInfo, 0, sizeof(obs_object_info));
    objectInfo.key = const_cast<char *>(objectKey.c_str());
    obs_response_handler responseHandler = {&NormalPropertiesCallback, &NormalCompleteCallback};
    NormalBackType attrBackData;
    delete_object(&option, &objectInfo, &responseHandler, &attrBackData);
    int obsRet = 0;
    if (OBS_STATUS_OK == attrBackData.retStatus) {
        FALCON_LOG(LOG_INFO) << "delete object " << objectKey << " successfully";
        obsRet = OBS_STATUS_OK;
    } else {
        FALCON_LOG(LOG_ERROR) << "delete object " << objectKey << " failed: " << attrBackData.error->message;
        obsRet = -1;
    }
    return obsRet;
}

std::string findBucket(std::string &fullName)
{
    int pos = fullName.find('/');
    if (pos != (int)fullName.npos) {
        return fullName.substr(0, pos);
    }
    return fullName;
}

int OBSStorage::GetStorageInfo(size_t &objNum, size_t &cap)
{
    obs_options option;
    InitObsOptions(option);
    std::string buck = findBucket(bucketName);
    option.bucket_options.bucket_name = buck.data();

    char capacity[OBS_COMMON_LEN_256] = {0};
    char obj_num[OBS_COMMON_LEN_256] = {0};

    obs_response_handler response_handler = {nullptr, &response_complete_callback};

    obs_status ret_status = OBS_STATUS_BUTT;
    int retryCount = RETRY_NUM;
    while (retryCount > 0) {
        get_bucket_storage_info(&option,
                                OBS_COMMON_LEN_256,
                                capacity,
                                OBS_COMMON_LEN_256,
                                obj_num,
                                &response_handler,
                                &ret_status);
        DoRetry(ret_status, retryCount);
    }

    if (ret_status == OBS_STATUS_OK) {
        objNum = strtol(obj_num, nullptr, 10);
        cap = strtol(capacity, nullptr, 10);
        return 0;
    } else {
        FALCON_LOG(LOG_ERROR) << "GetStorageInfo() failed (" << obs_get_status_name(ret_status) << ").";
        return -1;
    }
}

int OBSStorage::GetQuota(uint64_t &quota)
{
    obs_options option;
    InitObsOptions(option);
    std::string buck = findBucket(bucketName);
    option.bucket_options.bucket_name = buck.data();

    obs_response_handler response_handler = {nullptr, &response_complete_callback};

    obs_status ret_status = OBS_STATUS_BUTT;
    int retryCount = RETRY_NUM;
    while (retryCount > 0) {
        get_bucket_quota(&option, &quota, &response_handler, &ret_status);
        DoRetry(ret_status, retryCount);
    }

    if (ret_status == OBS_STATUS_OK) {
        return 0;
    } else {
        FALCON_LOG(LOG_ERROR) << "GetQuota() failed (" << obs_get_status_name(ret_status) << ").";
        return -1;
    }
}

int OBSStorage::StatFs(struct statvfs *vfsbuf)
{
    uint64_t objNum = 0;
    uint64_t cap = 0;
    uint64_t quota = 0;

    int ret = GetStorageInfo(objNum, cap);
    if (ret != 0) {
        return -EIO;
    }
    ret = GetQuota(quota);
    if (ret != 0) {
        return -EIO;
    }
    quota = (quota == 0) ? UINT64_MAX : quota;
    vfsbuf->f_bsize = 4096;
    vfsbuf->f_frsize = 4096;
    auto usedBlocks = (cap + vfsbuf->f_frsize - 1) / vfsbuf->f_frsize;
    vfsbuf->f_blocks = quota / vfsbuf->f_frsize;
    vfsbuf->f_bfree = vfsbuf->f_blocks - usedBlocks;
    vfsbuf->f_bavail = vfsbuf->f_bfree;
    vfsbuf->f_files = objNum;
    vfsbuf->f_ffree = UINT32_MAX;
    vfsbuf->f_favail = UINT32_MAX;
    vfsbuf->f_fsid = 3645364; // be unique
    vfsbuf->f_flag = 4096;    // ST_RELATIME
    vfsbuf->f_namemax = 255;
    return 0;
}

int OBSStorage::CopyObject(const std::string &fromPath, const std::string &toPath)
{
    NormalBackType data;
    obs_options option;
    InitObsOptions(option);

    obs_response_handler responseHandler = {&NormalPropertiesCallback, &NormalCompleteCallback};

    char eTag[OBS_COMMON_LEN] = {0};
    obs_copy_destination_object_info dstObjectinfo = {};
    int64_t lastModified;
    dstObjectinfo.destination_bucket = option.bucket_options.bucket_name;
    dstObjectinfo.destination_key = const_cast<char *>(toPath.c_str());
    dstObjectinfo.etag_return = eTag;
    dstObjectinfo.etag_return_size = sizeof(eTag);
    dstObjectinfo.last_modified_return = &lastModified;

    copy_object(&option,
                const_cast<char *>(fromPath.c_str()),
                nullptr,
                &dstObjectinfo,
                1,
                nullptr,
                nullptr,
                &responseHandler,
                &data);

    if (OBS_STATUS_OK != data.retStatus) {
        FALCON_LOG(LOG_ERROR) << "CopyObject " << fromPath << " to " << toPath
                              << " failed: " << obs_get_status_name(data.retStatus);
        return -1;
    }
    return 0;
}

int OBSStorage::HeadBucket()
{
    obs_options option;
    InitObsOptions(option);

    obs_response_handler responseHandler = {nullptr, &response_complete_callback};

    obs_status ret_status = OBS_STATUS_BUTT;
    obs_head_bucket(&option, &responseHandler, &ret_status);

    if (ret_status != OBS_STATUS_OK) {
        FALCON_LOG(LOG_ERROR) << "HeadBucket " << bucketName << " failed: " << obs_get_status_name(ret_status);
        return -1;
    }
    return 0;
}

void OBSStorage::InitObsOptions(obs_options &option)
{
    init_obs_options(&option);
    option.bucket_options.host_name = hostName.data();
    option.bucket_options.bucket_name = bucketName.data();
    option.bucket_options.access_key = accessKey.data();
    option.bucket_options.secret_access_key = secretAccessKey.data();
    option.bucket_options.token = nullptr;
    option.bucket_options.uri_style = OBS_URI_STYLE_PATH;
    if (isHttps) {
        option.bucket_options.protocol = OBS_PROTOCOL_HTTPS;
        FALCON_LOG(LOG_DEBUG) << "in https";
    } else {
        option.bucket_options.protocol = OBS_PROTOCOL_HTTP;
    }
    FALCON_LOG(LOG_DEBUG) << "InitObsOptions done";
    FALCON_LOG(LOG_DEBUG) << "host is " << hostName;
}

#endif
