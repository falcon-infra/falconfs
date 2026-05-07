/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <fcntl.h>
#include <unistd.h>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <functional>
#include <cassert>

#include <sys/stat.h>

#include "zookeeper/zookeeper.h"

#ifndef RETURN_OK
#define RETURN_OK 0
#endif

#ifndef RETURN_ERROR
#define RETURN_ERROR (-1)
#endif

typedef enum {
    UNINITIALIZED = 0,
    EXPIRED,
    VALID
} StoreNodeStatus;

typedef enum {
    ZOO_NOTCONNECTED = 0,
    ZOO_CONNECTED,
    ZOO_CONNECTING,
    ZOO_EXPIRED_SESSION,
    ZOO_CONNECTION_FAILED
} ZKConnectionStatus;

class FalconCM {
  public:
    static FalconCM *GetInstance(const std::string &zkEndPoint, int zkTimeout, const std::string &clusterName);
    static FalconCM *GetInstance();
    static void DeleteInstance();
    int GetInitStatus();
    int Connect();
    int FetchLeaders(std::vector<std::string> &leaders);
    int FetchCNLeader(std::string &cnleader);
    int FetchCoordinatorInfo(std::string &coordinatorIp, int &coordinatorPort);
    int FetchStoreNodes(std::unordered_map<int, std::string> &storeNodes);
    int Upload(const std::string &path, std::string &nodeInfoParam, int &nodeIdParam, std::string &rootPath);
    int ReUpload();
    bool GetNodeStatus();
    bool GetMetaDataStatus();
    void UpdateNodeStatus();
    int unsetNodeStatus();
    void UpdateMetaDataStatus();
    void CheckMetaDataStatus();
    std::condition_variable &GetStoreNodeCompleteCv();
    std::condition_variable &GetMetaDataReadyCv();
    bool RetryWithNumAndInterval(std::function<int()> func, int retryCnt, int sleepTimeS);
    static std::string GetExitControlFilePath() { return exitControlFilePath; };
    static void ExitByControlFile(int err);
    ZKConnectionStatus GetConnState() { return connectionStatus.load(); };
    StoreNodeStatus GetNodeState() { return localNodeStatus.load(); };

    // ut function
    #ifdef UNIT_TEST
    void TestTriggerWatcher(int type, int state) {
        InitWatcher(nullptr, type, state, nullptr, this);
    }
    void ResetState() { connectionStatus = ZOO_NOTCONNECTED; }
    #endif

  private:
    static FalconCM singleton;
    static std::atomic<bool> init;
    static int initStatus;
    std::string zkEndPoint;
    int zkTimeout{10000};
    std::string clusterName{"/falcon"};
    std::atomic<StoreNodeStatus> localNodeStatus = UNINITIALIZED;
    int nodeId{-1};
    std::string nodeInfo;
    int isReconnection{0};
    std::atomic<bool> ready;
    std::atomic<bool> metaDataReady;
    zhandle_t *zhandle = nullptr;
    std::mutex zkMutex;
    std::condition_variable zkCV;
    std::atomic<ZKConnectionStatus> connectionStatus = ZOO_NOTCONNECTED;
    bool connectionFailed = false;
    static void InitWatcher(zhandle_t *zh, int type, int state, const char *path, void *ctx);
    void HandleConnected();
    void HandleNotConnected();
    void HandleConnecting();
    void HandleExpired();
    int WaitForConnect();
    int CheckStatusAndReconnect();
    int ReConnect();
    void DestroyCM();
    std::condition_variable storeNodeCompleteCv;
    std::condition_variable metaDataReadyCv;
    static std::string exitControlFilePath;
};
