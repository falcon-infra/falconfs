/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "router.h"

#include <format>
#include <ranges>

#include "cm/falcon_cm.h"
#include "log/logging.h"
#include "utils.h"

Router::Router(const ServerIdentifier &coordinator)
    : coordinatorConn(std::make_shared<Connection>(coordinator))
{
    FetchShardTable(coordinatorConn);
}

int Router::FetchShardTable(std::shared_ptr<Connection> conn)
{
    // Init shard table
    Connection::PlainCommandResult res;
    if (conn->PlainCommand("select * from falcon_renew_shard_table();", res) == SERVER_FAULT) {
        return SERVER_FAULT;
    }

    const auto response = res.response;
    const int shardCount = response->row();
    const int col = response->col();
    int lastShardMaxValue = INT32_MIN;

    std::unique_lock<std::shared_mutex> lock(mapMtx);
    auto tmpRouteMap = routeMap;
    shardTable.clear();
    routeMap.clear();
    for (const auto i : std::views::iota(0, shardCount)) {
        const int shardMinValue = StringToInt32(response->data()->Get(i * col + 0)->c_str());
        const int shardMaxValue = StringToInt32(response->data()->Get(i * col + 1)->c_str());

        ServerIdentifier server(response->data()->Get(i * col + 2)->c_str(),
                                StringToInt32(response->data()->Get(i * col + 3)->c_str()) + 10,
                                StringToInt32(response->data()->Get(i * col + 4)->c_str()));

        // Validate shard ranges
        if (lastShardMaxValue != INT32_MIN && lastShardMaxValue + 1 != shardMinValue) {
            throw std::runtime_error("shard table is corrupt");
        }

        if (lastShardMaxValue == INT32_MIN && shardMinValue != INT32_MIN) {
            shardTable.emplace(shardMinValue - 1, ServerIdentifier("", 0, -1));
        }

        shardTable.emplace(shardMaxValue, server);
        if (!tmpRouteMap.empty() && tmpRouteMap.contains(server)) {
            routeMap.try_emplace(server, tmpRouteMap[server]);
        } else {
            routeMap.try_emplace(server, std::make_shared<Connection>(server));
        }
        lastShardMaxValue = shardMaxValue;
    }

    if (lastShardMaxValue != INT32_MAX) {
        throw std::runtime_error("shard table is corrupt");
    }
    return 0;
}

std::shared_ptr<Connection> Router::GetCoordinatorConn()
{
    std::shared_lock<std::shared_mutex> lock(coordinatorMtx);
    return coordinatorConn;
}

std::shared_ptr<Connection> Router::GetWorkerConnByPath(std::string_view path)
{
    if (path.empty() || path[0] != '/') {
        return nullptr;
    }

    // Normalize path
    if (path.size() > 1 && path.back() == '/') {
        path.remove_suffix(1);
    }

    // Extract filename
    auto last_slash = path.find_last_of('/');
    auto [dir, filename] = std::pair(path.substr(0, last_slash ? last_slash - 1 : 0), path.substr(last_slash + 1));

    if (filename.empty() && dir.empty()) {
        filename = "/";
    }

    // Find shard
    std::shared_lock<std::shared_mutex> lock(mapMtx);
    uint16_t partId = HashPartId(filename.data());
    auto shardIt = shardTable.lower_bound(HashInt8(partId));
    if (shardIt == shardTable.end()) {
        throw std::runtime_error("shard table is corrupt.");
    }

    // Return connection
    if (auto connIt = routeMap.find(shardIt->second); connIt != routeMap.end()) {
        return connIt->second;
    }

    throw std::runtime_error("no such server.");
}

std::shared_ptr<Connection> Router::GetWorkerConnByKey(std::string_view key)
{
    if (key.empty()) {
        return nullptr;
    }

    // Find shard
    std::shared_lock<std::shared_mutex> lock(mapMtx);
    uint16_t partId = HashPartId(key.data());
    auto shardIt = shardTable.lower_bound(HashInt8(partId));
    if (shardIt == shardTable.end()) {
        throw std::runtime_error("shard table is corrupt.");
    }

    // Return connection
    if (auto connIt = routeMap.find(shardIt->second); connIt != routeMap.end()) {
        return connIt->second;
    }

    throw std::runtime_error("no such server.");
}

int Router::GetAllWorkerConnection(std::unordered_map<std::string, std::shared_ptr<Connection>> &workerInfo)
{
    std::shared_lock<std::shared_mutex> lock(mapMtx);
    for (const auto &[server, conn] : routeMap) {
        workerInfo.emplace(std::format("{}:{}", server.ip, server.port), conn);
    }
    return 0;
}

std::shared_ptr<Connection> Router::TryToUpdateCNConn(std::shared_ptr<Connection> conn)
{
    std::unique_lock<std::shared_mutex> lock(coordinatorMtx);
    if (!(conn->server == coordinatorConn->server)) {
        FALCON_LOG(LOG_WARNING) << "CN info has already updated, ip: " << coordinatorConn->server.ip
                                << ", port: " << coordinatorConn->server.port;
        return coordinatorConn;
    }
    std::string coordinatorIp;
    int coordinatorPort = 0;
    ServerIdentifier newCoordinatorServer;
    int ret = RETURN_OK;
    int cnt = 0;

    do {
        ret = FalconCM::GetInstance()->FetchCoordinatorInfo(coordinatorIp, coordinatorPort);
        newCoordinatorServer = ServerIdentifier(coordinatorIp, coordinatorPort, conn->server.id);
        sleep(SLEEPTIME);
        ++cnt;
    } while ((cnt <= RETRY_CNT) && (ret != RETURN_OK || newCoordinatorServer == conn->server));

    if (ret != RETURN_OK || newCoordinatorServer == conn->server) {
        FALCON_LOG(LOG_WARNING) << "CN info has not changed, ip: " << coordinatorConn->server.ip
                                << ", port: " << coordinatorConn->server.port;
        return coordinatorConn;
    }
    coordinatorConn = std::make_shared<Connection>(newCoordinatorServer);
    FALCON_LOG(LOG_WARNING) << "Update CN info, ip: " << coordinatorConn->server.ip
                            << ", port: " << coordinatorConn->server.port;
    return coordinatorConn;
}

std::shared_ptr<Connection> Router::GetWorkerConnBySvrId(int id)
{
    std::shared_lock<std::shared_mutex> lock(mapMtx);
    for (auto [server, connection] : routeMap) {
        if (id == connection->server.id) {
            return connection;
        }
    }
    return nullptr;
}

std::shared_ptr<Connection> Router::TryToUpdateWorkerConn(std::shared_ptr<Connection> conn)
{
    int cnt = 0;
    std::shared_ptr<Connection> newConn;
    do {
        newConn = GetWorkerConnBySvrId(conn->server.id);
        if (newConn == nullptr) {
            throw std::runtime_error("not found server by id.");
        }
        if (!(newConn->server == conn->server)) {
            FALCON_LOG(LOG_WARNING) << "DN" << newConn->server.id << " info has updated, ip: " << newConn->server.ip
                                    << ", port: " << newConn->server.port;
            return newConn;
        }
        sleep(SLEEPTIME);
        std::shared_ptr<Connection> coordinatorConn = GetCoordinatorConn();
        int ret = FetchShardTable(coordinatorConn);
        if (ret == SERVER_FAULT) {
            coordinatorConn = TryToUpdateCNConn(coordinatorConn);
            ret = FetchShardTable(coordinatorConn);
        }
        ++cnt;
    } while (cnt <= RETRY_CNT && newConn->server == conn->server);

    FALCON_LOG(LOG_WARNING) << "DN" << conn->server.id << " info has not changed, ip: " << conn->server.ip
                            << ", port: " << conn->server.port;

    return conn;
}
