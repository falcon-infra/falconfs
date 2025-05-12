/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "connection/node.h"

#include <chrono>
#include <print>
#include <ranges>
#include <thread>

#include "cm/falcon_cm.h"
#include "log/logging.h"

#define CONNECTION_RETRY_NUM 300

using namespace std::chrono_literals;

StoreNode::~StoreNode() {
    std::unique_lock<std::shared_mutex> lock(nodeMutex);
    nodeMap.clear();
    FALCON_LOG(LOG_INFO) << "StoreNode destroyed";
}

int StoreNode::SetNodeConfig(int initNodeId, std::string &clusterView)
{
    std::unique_lock<std::shared_mutex> nodeLock(nodeMutex);
    nodeId = initNodeId;
    std::println("falcon_store nodeId = {}", nodeId);
    initStatus = 0;
    std::ranges::for_each(clusterView | std::views::split(',') | std::views::transform([](auto &&rng) {
                              return std::string(&*rng.begin(), std::ranges::distance(rng));
                          }) | std::views::enumerate,
                          [this](auto &&enumerated) {
                              auto &&[i, rpcEndPoint] = enumerated;
                              FALCON_LOG(LOG_INFO) << "node " << i << " = " << rpcEndPoint;
                              bool connected = false;
                              int retry_num = 0;
#ifdef USE_RDMA
                              std::shared_ptr<FalconIOClient> connection;
                              do {
                                  connection = std::shared_ptr<FalconIOClient>(CreateIOConnection(rpcEndPoint));
                                  connected = connection->CheckConnection() == 0;
                                  if (!connected) {
                                      retry_num++;
                                      std::this_thread::sleep_for(1s);
                                      FALCON_LOG(LOG_WARNING) << "Connect failed, retry";
                                  }
                              } while (!connected && retry_num < CONNECTION_RETRY_NUM);
#else
                              auto connection = std::shared_ptr<FalconIOClient>(CreateIOConnection(rpcEndPoint));
                              do {
                                  connected = connection->CheckConnection() == 0;
                                  if (!connected) {
                                      retry_num++;
                                      std::this_thread::sleep_for(1s);
                                      FALCON_LOG(LOG_WARNING) << "Connect failed, retry";
                                  }
                              } while (!connected && retry_num < CONNECTION_RETRY_NUM);
#endif
                              if (connected) {
                                  nodeMap.emplace(i, std::make_pair(rpcEndPoint, connection));
                              } else {
                                  initStatus = 1;
                                  return;
                              }
                          });
    return initStatus;
}

int StoreNode::UpdateNodeConfig()
{
    int ret = 0;
#ifdef ZK_INIT
    std::unordered_map<int, std::string> storeNodes;
    ret = FalconCM::GetInstance()->FetchStoreNodes(storeNodes);
    if (ret != 0) {
        return ret;
    }
    std::unique_lock<std::shared_mutex> nodeLock(nodeMutex);

    std::vector<int> toDel;
    for (auto &kv : nodeMap) {
        if (storeNodes.count(kv.first) == 0) {
            toDel.emplace_back(kv.first);
        } else {
            storeNodes.erase(kv.first);
        }
    }
    for (auto &delNode : toDel) {
        nodeMap.erase(delNode);
    }
    for (auto &newNodeKv : storeNodes) {
        std::shared_ptr<FalconIOClient> connection(CreateIOConnection(newNodeKv.second));
        nodeMap.emplace(newNodeKv.first, std::make_pair(newNodeKv.second, connection));
    }
#endif
    return ret;
}

int StoreNode::SetNodeConfig(std::string &rootPath)
{
    auto ipPort = GetPodIPPort();
    if (!ipPort) {
        FALCON_LOG(LOG_ERROR) << "GetPodIPPort failed: " << ipPort.error();
        return -1; // 或定义明确的错误码
    }
    std::string podIP = ipPort.value_or("127.0.0.1:56039");
    int ret = FalconCM::GetInstance()->Upload("", podIP, nodeId, rootPath);
    if (ret != 0) {
        return ret;
    }
    FALCON_LOG(LOG_INFO) << "In SetNodeConfig(): local nodeId = " << nodeId;
    std::mutex mu;
    std::unique_lock<std::mutex> lock(mu);
    FalconCM::GetInstance()->GetStoreNodeCompleteCv().wait(lock,
                                                           []() { return FalconCM::GetInstance()->GetNodeStatus(); });

    ret = UpdateNodeConfig();
    std::thread FetchView([this]() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(3));
            int ret = UpdateNodeConfig();
            if (ret != 0) {
                FALCON_LOG(LOG_ERROR) << "UpdateAndFetch failed, wait for next term";
            }
        }
    });
    FetchView.detach();
    return ret;
}

StoreNode *StoreNode::GetInstance()
{
    static std::mutex instanceMutex;
    static StoreNode* instance = nullptr;

    std::lock_guard<std::mutex> lock(instanceMutex);
    if (!instance) {
        instance = new StoreNode();
    }
    return instance;
}

int StoreNode::GetInitStatus() { return initStatus; }

void StoreNode::DeleteInstance() {
    static std::mutex delMutex;
    static StoreNode* instance = GetInstance();

    std::lock_guard<std::mutex> lock(delMutex);
    if (instance) {
        delete instance;
        instance = nullptr;
        FALCON_LOG(LOG_INFO) << "StoreNode instance deleted";
    }
}

void StoreNode::Delete()
{
    std::unique_lock<std::shared_mutex> lock(nodeMutex);
    nodeMap.clear();
}

FalconIOClient *StoreNode::CreateIOConnection(const std::string &rpcEndPoint)
{
    std::shared_ptr<brpc::Channel> channel = std::make_shared<brpc::Channel>();
    brpc::ChannelOptions options;
    options.connection_type = "pooled";
    options.connect_timeout_ms = 5000;
    options.timeout_ms = 10000;
#ifdef USE_RDMA
    options.use_rdma = true;
#endif
    if (channel->Init(rpcEndPoint.c_str(), &options) != 0) {
        FALCON_LOG(LOG_ERROR) << "Fail to initialize channel for " << rpcEndPoint;
        return nullptr;
    }

    auto *falconIOClient = new (std::nothrow) FalconIOClient(channel);
    return falconIOClient;
}

std::shared_ptr<FalconIOClient> StoreNode::GetRpcConnection(int nodeId)
{
    std::shared_lock<std::shared_mutex> slock(nodeMutex);
    if (nodeMap.find(nodeId) == nodeMap.end()) {
        FALCON_LOG(LOG_ERROR) << "No rpc connection at node " << nodeId;
        return nullptr;
    }
    return nodeMap[nodeId].second;
}

int StoreNode::GetNodeId() { return nodeId; }

int StoreNode::GetNodeId(std::string_view ipPort)
{
    std::shared_lock<std::shared_mutex> slock(nodeMutex);
    return SplitIp(ipPort)
        .and_then([this](const auto &metaIp) {
            auto it = std::find_if(nodeMap.begin(), nodeMap.end(), [&metaIp](const auto &entry) {
                return SplitIp(entry.second.first) == metaIp;
            });
            return it != nodeMap.end() ? std::optional(it->first) : std::nullopt;
        })
        .value_or(-1);
}

bool StoreNode::IsLocal(int otherNodeId)
{
    std::shared_lock<std::shared_mutex> lock(nodeMutex);
    return nodeId == otherNodeId;
}

bool StoreNode::IsLocal(std::string_view ipPort)
{
    std::shared_lock lock(nodeMutex);
    return SplitIp(ipPort) == SplitIp(nodeMap[nodeId].first);
}

std::string StoreNode::GetRpcEndPoint(int nodeId)
{
    std::shared_lock<std::shared_mutex> lock(nodeMutex);
    if (nodeMap.find(nodeId) == nodeMap.end()) {
        FALCON_LOG(LOG_ERROR) << "nodeId " << nodeId << " is not in nodeMap";
        return {};
    }
    return nodeMap[nodeId].first;
}

int StoreNode::GetBackupNodeId()
{
    std::shared_lock<std::shared_mutex> lock(nodeMutex);
    int nodeNum = nodeMap.size();
    int backupNodeIdx = GenerateRandom(0, nodeNum - 1);
    int backupNodeId = -1;
    auto it = nodeMap.begin();
    std::advance(it, backupNodeIdx);
    if (nodeId == it->first) {
        std::advance(it, 1);
        if (it == nodeMap.end()) {
            it = nodeMap.begin();
        }
    }
    backupNodeId = it->first;
    return backupNodeId;
}

int StoreNode::GetNumberofAllNodes()
{
    std::shared_lock<std::shared_mutex> lock(nodeMutex);
    return nodeMap.size();
}

uint64_t hash64(uint64_t x)
{
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    x = x ^ (x >> 31);
    return x;
}

int StoreNode::AllocNode(uint64_t inodeId)
{
    std::shared_lock<std::shared_mutex> lock(nodeMutex);
    if (!nodeMap.empty()) {
        int index = (hash64(inodeId)) % nodeMap.size();
        auto it = nodeMap.begin();
        std::advance(it, index);
        return it->first;
    }
    return nodeId;
}

int StoreNode::GetNextNode(int nodeId, uint64_t inodeId)
{
    std::shared_lock<std::shared_mutex> lock(nodeMutex);
    if (!nodeMap.empty()) {
        auto it = nodeMap.find(nodeId);
        if (it == nodeMap.end()) {
            FALCON_LOG(LOG_WARNING) << "nodeId is not in nodeMap, rehash";
            lock.unlock();
            return AllocNode(inodeId);
        }
        it = std::next(it);
        if (it == nodeMap.end()) {
            it = nodeMap.begin();
        }
        return it->first;
    }
    return nodeId;
}

void StoreNode::DeleteNode(int nodeId)
{
    std::unique_lock<std::shared_mutex> lock(nodeMutex);
    nodeMap.erase(nodeId);
}

std::vector<int> StoreNode::GetAllNodeId()
{
    std::shared_lock<std::shared_mutex> nodeLock(nodeMutex);
    std::vector<int> nodeVector;
    nodeVector.reserve(nodeMap.size());
    for (const auto &it : nodeMap) {
        nodeVector.emplace_back(it.first);
    }
    return nodeVector;
}
