/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <map>
#include <string>
#include <string_view>
#include <unordered_map>
#include <shared_mutex>

#include "connection.h"

#define SLEEPTIME 3
#define RETRY_CNT 3

struct InodeIdentifier
{
    uint64_t parentId;
    std::string name;
    InodeIdentifier(uint64_t parentId, std::string_view name)
        : parentId(parentId),
          name(name)
    {
    }
    bool operator==(const InodeIdentifier &t) const { return parentId == t.parentId && name == t.name; }
};

struct InodeIdentifierHash
{
    std::size_t operator()(const InodeIdentifier &t) const
    {
        return std::hash<uint64_t>()(t.parentId) ^ std::hash<std::string>()(t.name);
    }
};

class Router {
  private:
    std::shared_ptr<Connection> coordinatorConn;
    std::map<int, ServerIdentifier> shardTable;
    std::unordered_map<ServerIdentifier, std::shared_ptr<Connection>, ServerIdentifierHash> routeMap;
    std::shared_mutex coordinatorMtx;
    std::shared_mutex mapMtx;

  public:
    Router(const ServerIdentifier &coordinator);

    int FetchShardTable(std::shared_ptr<Connection> conn);

    std::shared_ptr<Connection> GetCoordinatorConn();

    std::shared_ptr<Connection> GetWorkerConnByPath(std::string_view path);

    int GetAllWorkerConnection(std::unordered_map<std::string, std::shared_ptr<Connection>> &workerInfo);

    std::shared_ptr<Connection> TryToUpdateCNConn(std::shared_ptr<Connection> conn);

    std::shared_ptr<Connection> TryToUpdateWorkerConn(std::shared_ptr<Connection> conn);

    std::shared_ptr<Connection> GetWorkerConnBySvrId(int id);

    ~Router() = default;
};
