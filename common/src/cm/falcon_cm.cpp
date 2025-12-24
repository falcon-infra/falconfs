/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "cm/falcon_cm.h"

#include <fstream>

#include "log/logging.h"

constexpr int BUFF_SIZE = 512;

FalconCM FalconCM::singleton;
std::atomic<bool> FalconCM::init = false;
int FalconCM::initStatus = 0;

std::string FalconCM::exitControlFilePath = "/opt/falcon/exit";

FalconCM *FalconCM::GetInstance(const std::string &zkEndPoint, int zkTimeout, const std::string &clusterName)
{
    if (init.exchange(true)) {
        // 如果已经初始化，不允许重复调用带参版本
        throw std::runtime_error("FalconCM already initialized. Use GetInstance() without parameters.");
    }
    singleton.zkEndPoint = zkEndPoint;
    singleton.zkTimeout = zkTimeout;
    singleton.clusterName = clusterName;
    initStatus = singleton.Connect();
    return &singleton;
}

FalconCM *FalconCM::GetInstance()
{
    if (!init.load()) {
        throw std::runtime_error(
            "FalconCM must be initialized first with GetInstance(zkEndPoint, zkTimeout, clusterName)");
    }
    return &singleton;
}

int FalconCM::GetInitStatus() { return initStatus; }

void FalconCM::DeleteInstance() { singleton.DestroyCM(); }

void FalconCM::DestroyCM()
{
    if (zhandle) {
        int ret = zookeeper_close(zhandle);
        if (ret != ZOK) {
            FALCON_LOG(LOG_ERROR) << "zookeeper close failed : " << ret;
        }
        zhandle = nullptr;
    }
    init.store(false);
    connectionStatus = ZOO_NOTCONNECTED;
}

int FalconCM::WaitForConnect()
{
    std::unique_lock<std::mutex> checkLock(zkMutex);
    while (!zkCV.wait_for(checkLock, std::chrono::seconds(3), [this] { return connectionStatus != ZOO_NOTCONNECTED; })) {
        FALCON_LOG(LOG_WARNING) << "WaitForConnect: Waiting 3s for zookeeper connection";
    }

    if (connectionStatus == ZOO_CONNECTED) {
        return RETURN_OK;
    }
    // only OK for rapid connection
    return RETURN_ERROR;
}

void FalconCM::HandleConnected()
{
    std::unique_lock<std::mutex> checkLock(zkMutex);
    if (isReconnection == 1) {
        std::function<int()> reUploadFunc = [this]() {
            return ReUpload();
        };
        if (!RetryWithNumAndInterval(reUploadFunc, 3, 3)) {
            FALCON_LOG(LOG_ERROR) << "ReUpload failed after 3 times! exit process";
            ExitByControlFile(1);
        }
    }
    isReconnection = 0;
    zkCV.notify_one();
}

void FalconCM::HandleNotConnected()
{
    std::unique_lock<std::mutex> checkLock(zkMutex);
    zkCV.notify_one();
}

void FalconCM::HandleConnecting()
{
    std::unique_lock<std::mutex> checkLock(zkMutex);
    zkCV.notify_one();
}

void FalconCM::HandleExpired()
{
    std::unique_lock<std::mutex> checkLock(zkMutex);
    localNodeStatus = localNodeStatus == UNINITIALIZED ? UNINITIALIZED : EXPIRED;
    ReConnect();
}

void FalconCM::InitWatcher(zhandle_t * /*zh*/, int type, int state, const char * /*path*/, void *ctx)
{
    auto *falconCM = static_cast<FalconCM *>(ctx);
    if (type != ZOO_SESSION_EVENT) {
        FALCON_LOG(LOG_ERROR) << "Wrong zoo type!";
        return;
    }
    if (state == ZOO_CONNECTED_STATE) {
        FALCON_LOG(LOG_INFO) << "connection success";
        falconCM->connectionStatus = ZOO_CONNECTED;
        falconCM->HandleConnected();
    } else if (state == ZOO_CONNECTING_STATE) {
        FALCON_LOG(LOG_INFO) << "zk connecting";
        falconCM->connectionStatus = ZOO_CONNECTING;
        falconCM->HandleConnecting();
    } else if (state == ZOO_EXPIRED_SESSION_STATE) {
        FALCON_LOG(LOG_ERROR) << "zk expired";
        falconCM->connectionStatus = ZOO_EXPIRED_SESSION;
        falconCM->HandleExpired();
    } else {
        FALCON_LOG(LOG_ERROR) << "Connection failed : " << state;
        falconCM->connectionStatus = ZOO_CONNECTION_FAILED;
        falconCM->HandleNotConnected();
        ExitByControlFile(1);
    }
}

int FalconCM::Connect()
{
    if (zhandle != nullptr) {
        FALCON_LOG(LOG_ERROR) << "Connect event repeat init";
        return RETURN_ERROR;
    }
    zhandle = zookeeper_init(zkEndPoint.c_str(), FalconCM::InitWatcher, zkTimeout, nullptr, this, 0);
    if (zhandle == nullptr) {
        FALCON_LOG(LOG_ERROR) << "zookeeper connect failed";
        return RETURN_ERROR;
    }
    return WaitForConnect();
}

int FalconCM::CheckStatusAndReconnect()
{
    int connectedStat = zoo_state(zhandle);
    int ret = ZOK;
    if (connectedStat == ZOO_CONNECTED_STATE) {
        return ret;
    }
    if (zhandle) {
        ret = zookeeper_close(zhandle);
        zhandle = nullptr;
        // exit(1);
    }
    ret = Connect();
    return ret;
}

int FalconCM::ReConnect()
{
    FALCON_LOG(LOG_INFO) << "zk connect session expired, reconnect it";
    int ret = ZOK;
    if (zhandle) {
        ret = zookeeper_close(zhandle);
        zhandle = nullptr;
    }
    zhandle = zookeeper_init(zkEndPoint.c_str(), FalconCM::InitWatcher, zkTimeout, nullptr, this, 0);
    if (zhandle == nullptr) {
        FALCON_LOG(LOG_ERROR) << "reconnection zk failed";
    }
    isReconnection = 1;
    return ret;
}

int FalconCM::FetchLeaders(std::vector<std::string> &leaders)
{
    int ret = 0;
    std::string leaderPath = clusterName + "/leaders";
    String_vector leaderNames;
    ret = zoo_get_children(zhandle, leaderPath.c_str(), 0, &leaderNames);
    if (ret != ZOK) {
        FALCON_LOG(LOG_ERROR) << "zoo_get_children failed : " << ret;
        return RETURN_ERROR;
    }
    int n = leaderNames.count;
    for (int i = 0; i < n; i++) {
        std::string childName = leaderNames.data[i];
        std::cout << childName << std::endl;
        std::string nodeLeaderPath = leaderPath + "/" + childName;
        char buf[BUFF_SIZE] = {0};
        int len = BUFF_SIZE;
        int getRet = zoo_get(zhandle, nodeLeaderPath.c_str(), 0, buf, &len, nullptr);
        if (getRet != ZOK) {
            deallocate_String_vector(&leaderNames);
            FALCON_LOG(LOG_ERROR) << "zookeeper get node info failed :" << getRet;
            return RETURN_ERROR;
        }
        std::string nodeInfo = buf;
        leaders.push_back(nodeInfo);
    }
    deallocate_String_vector(&leaderNames);
    return RETURN_OK;
}

int FalconCM::FetchCNLeader(std::string &cnLeader)
{
    int ret = 0;
    std::string cnLeaderPath = clusterName + "/leaders/cn";
    char buf[BUFF_SIZE] = {0};
    int len = BUFF_SIZE;
    ret = zoo_get(zhandle, cnLeaderPath.c_str(), 0, buf, &len, nullptr);
    if (ret != ZOK) {
        FALCON_LOG(LOG_ERROR) << "zookeeper get node info failed :" << ret;
        return RETURN_ERROR;
    }
    cnLeader = buf;
    return RETURN_OK;
}

int FalconCM::FetchCoordinatorInfo(std::string &coordinatorIp, int &coordinatorPort)
{
    std::string cnLeaderInfo;
    int ret = FetchCNLeader(cnLeaderInfo);
    if (ret != 0 || cnLeaderInfo.empty()) {
        return RETURN_ERROR;
    }
    std::string::size_type iPos = cnLeaderInfo.find(':');
    if (iPos == std::string::npos) {
        return RETURN_ERROR;
    }
    coordinatorIp = cnLeaderInfo.substr(0, iPos);
    coordinatorPort = std::atoi(cnLeaderInfo.substr(iPos + 1, cnLeaderInfo.length() - iPos - 1).c_str()) + 10;
    return RETURN_OK;
}

void clusterStatusCallback(zhandle_t * /*zh*/, int type, int /*state*/, const char * /*path*/, void *watcherCtx)
{
    auto *falconCM = static_cast<FalconCM *>(watcherCtx);
    if (type == ZOO_CHANGED_EVENT) {
        falconCM->UpdateNodeStatus();
    }
}

void MetaDataStatusCallback(zhandle_t * /*zh*/, int type, int /*state*/, const char * /*path*/, void *watcherCtx)
{
    auto *falconCM = static_cast<FalconCM *>(watcherCtx);
    if (type == ZOO_CREATED_EVENT) {
        falconCM->UpdateMetaDataStatus();
    }
}

int FalconCM::Upload(const std::string & /*path*/, std::string &nodeInfoParam, int &nodeIdParam, std::string &rootPath)
{
    int ret = 0;
    exitControlFilePath = rootPath + "/exit";
    std::string myidPath = rootPath + "/myid";
    std::string nodePath = clusterName + "/StoreNode/Nodes/Node";
    int tmpNodeId = -1;
    if (access(exitControlFilePath.c_str(), F_OK) != 0) {
        FALCON_LOG(LOG_WARNING) << "write 1 to local exit control file: " << exitControlFilePath;
        std::ofstream fout;
        fout.open(exitControlFilePath.c_str(), std::ios::out);
        if (!fout.is_open()) {
            FALCON_LOG(LOG_ERROR) << "create exitControlFilePath file failed " << strerror(errno);
            return RETURN_ERROR;
        }
        fout << 1;
        fout.close();
    }
    if (access(myidPath.c_str(), F_OK) == 0) {
        std::ifstream fin;
        fin.open(myidPath.c_str(), std::ios::in);
        if (!fin.is_open()) {
            FALCON_LOG(LOG_ERROR) << "open existed myid file failed " << strerror(errno);
            return RETURN_ERROR;
        }
        fin >> tmpNodeId;
        fin.close();
    }
    char nodeBuffer[BUFF_SIZE];
    if (tmpNodeId == -1) {
        ret = zoo_create(zhandle,
                         nodePath.c_str(),
                         nodeInfoParam.c_str(),
                         nodeInfoParam.length(),
                         &ZOO_OPEN_ACL_UNSAFE,
                         ZOO_SEQUENCE | ZOO_EPHEMERAL,
                         nodeBuffer,
                         BUFF_SIZE);
        if (ret != ZOK) {
            FALCON_LOG(LOG_ERROR) << "create node failed in zk " << ret;
            return RETURN_ERROR;
        }
    } else {
        nodePath = nodePath + "000" + std::to_string(tmpNodeId);
        ret = zoo_create(zhandle,
                         nodePath.c_str(),
                         nodeInfoParam.c_str(),
                         nodeInfoParam.length(),
                         &ZOO_OPEN_ACL_UNSAFE,
                         ZOO_EPHEMERAL,
                         nodeBuffer,
                         BUFF_SIZE);
        if (ret != ZOK) {
            FALCON_LOG(LOG_ERROR) << "create node failed in zk " << ret;
            return RETURN_ERROR;
        }
    }
    std::string nodeName = std::string(nodeBuffer);
    size_t iPos = nodeName.find_last_of('/');
    std::string nodeIdStr(nodeName.substr(iPos + 5, nodeName.length() - iPos - 5));
    nodeIdParam = atoi(nodeIdStr.c_str());
    if (tmpNodeId == -1) {
        FALCON_LOG(LOG_INFO) << "write to local myid: " << myidPath;
        std::ofstream fout;
        fout.open(myidPath.c_str(), std::ios::out);
        if (!fout.is_open()) {
            FALCON_LOG(LOG_ERROR) << "create myid faile failed " << strerror(errno);
            return RETURN_ERROR;
        }

        fout << nodeIdParam;
        fout.close();
    }

    nodeId = nodeIdParam;
    nodeInfo = nodeInfoParam;

    std::string statusPath = clusterName + "/StoreNode/storeNodeStatus";
    char buf[BUFF_SIZE] = {0};
    int length = BUFF_SIZE;
    ret = zoo_wget(zhandle, statusPath.c_str(), clusterStatusCallback, this, buf, &length, nullptr);
    if (ret != ZOK) {
        FALCON_LOG(LOG_ERROR) << "wget " << statusPath << " failed in zk " << ret;
        return RETURN_ERROR;
    }
    if (strcmp(buf, "1") == 0) {
        UpdateNodeStatus();
    }
    localNodeStatus = VALID;
    return RETURN_OK;
}

int FalconCM::FetchStoreNodes(std::unordered_map<int, std::string> &storeNodes)
{
    int ret = 0;
    std::string storeClusterViewPath = clusterName + "/StoreNode/Nodes";
    String_vector clusterViews;
    ret = zoo_get_children(zhandle, storeClusterViewPath.c_str(), 0, &clusterViews);
    if (ret != ZOK) {
        FALCON_LOG(LOG_ERROR) << "zookeeper get children failed :" << ret;
        return ret;
    }
    int n = clusterViews.count;
    storeNodes.clear();
    for (int i = 0; i < n; ++i) {
        std::string childName = clusterViews.data[i];
        std::string nodePath = storeClusterViewPath + "/" + childName;
        char buf[BUFF_SIZE] = {0};
        int len = BUFF_SIZE;
        int getRet = zoo_get(zhandle, nodePath.c_str(), 0, buf, &len, nullptr);
        if (getRet != ZOK) {
            deallocate_String_vector(&clusterViews);
            FALCON_LOG(LOG_ERROR) << "zookeeper get node info failed : " << getRet;
            return ret;
        }
        std::string nodeIdStr(childName.substr(5, childName.length() - 5));
        int nodeId = atoi(nodeIdStr.c_str());
        storeNodes.emplace(nodeId, std::string(buf));
    }
    deallocate_String_vector(&clusterViews);
    return RETURN_OK;
}

/*
 * Should be called single threaded by zk watcher
*/
int FalconCM::ReUpload()
{
    // Reupload after previous uploaded zk nodeId expired due to connection breakdown
    if (localNodeStatus.load() == UNINITIALIZED) {
        // reconnect before store node initialized, failed node initialization should terminate the process
        return RETURN_OK;
    }
    assert(localNodeStatus.load() == EXPIRED);
    int ret = 0;
    std::string nodePath = clusterName + "/StoreNode/Nodes/Node00" + std::to_string(nodeId);
    std::cout << nodePath << std::endl;
    ret = zoo_create(zhandle,
                     nodePath.c_str(),
                     nodeInfo.c_str(),
                     nodeInfo.length(),
                     &ZOO_OPEN_ACL_UNSAFE,
                     ZOO_EPHEMERAL,
                     nullptr,
                     0);
    if (ret != ZOK) {
        FALCON_LOG(LOG_ERROR) << "Create node " << nodeId << " = " << nodeInfo << " failed in zk: " << ret;
        return RETURN_ERROR;
    }
    localNodeStatus = VALID;
    return RETURN_OK;
}

bool FalconCM::GetNodeStatus() { return ready.load(); }

void FalconCM::CheckMetaDataStatus()
{
    std::string metaDataStatusPath = clusterName + "/ready";
    int ret = zoo_wexists(zhandle, metaDataStatusPath.c_str(), MetaDataStatusCallback, this, nullptr);
    if (ret == ZOK) {
        /*
                the ready node exists
        */
        UpdateMetaDataStatus();
    }
}

bool FalconCM::GetMetaDataStatus() { return metaDataReady.load(); }

int FalconCM::unsetNodeStatus()
{
    ready.store(false);
    return 0;
}

void FalconCM::UpdateNodeStatus()
{
    if (!ready.load()) {
        ready.store(true);
        storeNodeCompleteCv.notify_all();
    }
}

void FalconCM::UpdateMetaDataStatus()
{
    if (!metaDataReady.load()) {
        metaDataReady.store(true);
        metaDataReadyCv.notify_all();
    }
}

std::condition_variable &FalconCM::GetStoreNodeCompleteCv() { return storeNodeCompleteCv; }

std::condition_variable &FalconCM::GetMetaDataReadyCv() { return metaDataReadyCv; }

bool FalconCM::RetryWithNumAndInterval(std::function<int()> func, int retryCnt, int sleepTimeS)
{
    while (retryCnt--) {
        if (func() == RETURN_OK) {
            return true;
        }
        sleep(sleepTimeS);
    }
    return false;
}

void FalconCM::ExitByControlFile(int err) {
    FALCON_LOG(LOG_INFO) << "exitControlFilePath = " << exitControlFilePath;
    if (access(exitControlFilePath.c_str(), F_OK) == 0) {
        uint32_t doExit = 0;
        std::ifstream fin;
        fin.open(exitControlFilePath.c_str(), std::ios::in);
        if (!fin.is_open()) {
            FALCON_LOG(LOG_ERROR) << "open existed " << exitControlFilePath << " file failed, do not exit: " << strerror(errno);
            return;
        }
        fin >> doExit;
        fin.close();
        if (doExit > 0) {
            FALCON_LOG(LOG_ERROR) << "exitControlFilePath contains x > 0, exit";
            exit(err);
        }
        FALCON_LOG(LOG_WARNING) << "exitControlFilePath contains 0, do not exit";
    } else {
        FALCON_LOG(LOG_WARNING) << "exitControlFile " << exitControlFilePath << " does not exist, do not exit";
    }
}
