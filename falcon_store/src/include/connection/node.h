/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "falcon_io_client.h"

class StoreNode {
  private:
    std::shared_mutex nodeMutex;
    int initStatus = 0;
    int nodeId;
    std::unordered_map<int, std::pair<std::string, std::shared_ptr<FalconIOClient>>> nodeMap;

  public:
    int SetNodeConfig(int initNodeId, std::string &clusterView);
    int SetNodeConfig(std::string &rootPath);
    static StoreNode *GetInstance();
    static void DeleteInstance();
    FalconIOClient *CreateIOConnection(const std::string &rpcEndPoint);
    void Delete();
    int GetNodeId();
    int GetNodeId(std::string_view ipPort);
    bool IsLocal(int otherNodeId);
    bool IsLocal(std::string_view ip);
    std::string GetRpcEndPoint(int nodeId);
    int GetBackupNodeId();
    std::shared_ptr<FalconIOClient> GetRpcConnection(int nodeId);
    int GetInitStatus();
    int GetNumberofAllNodes();
    int AllocNode(uint64_t inodeId);
    int GetNextNode(int nodeId, uint64_t inodeId);
    void DeleteNode(int nodeId);
    std::vector<int> GetAllNodeId();
    int UpdateNodeConfig();
};
