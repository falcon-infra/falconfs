/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "falcon_router.h"
#include "falcon_slice_meta.h"
#include "log/logging.h"
#include "cm/falcon_cm.h"


int FalconSlicePut(const std::string &path, std::vector<SliceInfo> &info)
{
    std::shared_ptr<Connection> conn = router->GetWorkerConnByPath(path);
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }

    auto filename = router->GetFilenameByPath(path);
    int errorCode = conn->SlicePut(filename.data(), info);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateCNConn(conn);
        errorCode = conn->SlicePut(filename.data(), info);
    }
#endif

    if (errorCode != SUCCESS) {
        FALCON_LOG(LOG_ERROR) << "FalconSlicePut failed, DN: " << conn->server.id << ", ip: " << conn->server.ip << ", error code: " << errorCode;
    }
    return errorCode;
}

int FalconSliceGet(const std::string &path, uint64_t inodeId, uint32_t chunkId, std::vector<SliceInfo> &info)
{
    std::shared_ptr<Connection> conn = router->GetWorkerConnByPath(path);
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }

    auto filename = router->GetFilenameByPath(path);
    int errorCode = conn->SliceGet(filename.data(), inodeId, chunkId, info);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateCNConn(conn);
        errorCode = conn->SliceGet(filename.data(), inodeId, chunkId, info);
    }
#endif

    if (errorCode != SUCCESS) {
        FALCON_LOG(LOG_ERROR) << "FalconSliceGet failed, inodeId: " << inodeId << ", chunkId: " << chunkId
                              << ", DN: " << conn->server.id << ", ip: " << conn->server.ip << ", error code: " << errorCode;
    }
    return errorCode;
}

int FalconSliceDel(const std::string &path, uint64_t inodeId, uint32_t chunkId)
{
    std::shared_ptr<Connection> conn = router->GetWorkerConnByPath(path);
    if (!conn) {
        FALCON_LOG(LOG_ERROR) << "route error";
        return PROGRAM_ERROR;
    }

    auto filename = router->GetFilenameByPath(path);
    int errorCode = conn->SliceDel(filename.data(), inodeId, chunkId);
#ifdef ZK_INIT
    int cnt = 0;
    while (cnt < RETRY_CNT && errorCode == SERVER_FAULT) {
        ++cnt;
        sleep(SLEEPTIME);
        conn = router->TryToUpdateCNConn(conn);
        errorCode = conn->SliceDel(filename.data(), inodeId, chunkId);
    }
#endif

    if (errorCode != SUCCESS) {
        FALCON_LOG(LOG_ERROR) << "FalconSliceDel failed, inodeId: " << inodeId << ", chunkId: " << chunkId
                              << ", DN: " << conn->server.id << ", ip: " << conn->server.ip << ", error code: " << errorCode;
    }
    return errorCode;
}