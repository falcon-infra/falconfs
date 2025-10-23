/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "connection_pool/brpc_server.h"

#include <memory>

#include <brpc/server.h>

#include "connection_pool/connection_pool_config.h"
#include "connection_pool/falcon_meta_rpc.h"
#include "connection_pool/falcon_meta_service.h"
#include "connection_pool/pg_connection_pool.h"

namespace falcon {
namespace meta_service {
    FalconMetaService* g_falcon_meta_service = nullptr;
}
}

class ConnectionPoolBrpcServer {
  private:
    int port;
    int poolSize;

    brpc::Server server;
    falcon::meta_service::FalconMetaService* falconMetaService;

  public:
    ConnectionPoolBrpcServer(int port, int poolSize)
        : port(port),
          poolSize(poolSize),
          falconMetaService(nullptr)
    {
    }

    ~ConnectionPoolBrpcServer()
    {
        if (falconMetaService != nullptr) {
            delete falconMetaService;
            falconMetaService = nullptr;
        }
    }
    void Run()
    {
        char *userName = getenv("USER");
        std::shared_ptr<PGConnectionPool> pgConnectionPool =
            std::make_shared<PGConnectionPool>(FalconPGPort, userName, poolSize, 20, 400);

        falcon::meta_proto::MetaServiceImpl metaServiceImpl(pgConnectionPool);
        if (server.AddService(&metaServiceImpl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
            throw std::runtime_error("ConnectionPoolBrpcServer: brpc server AddService failed");

        // 初始化 Falcon 元数据服务
        falconMetaService = new falcon::meta_service::FalconMetaService(pgConnectionPool);
        falcon::meta_service::g_falcon_meta_service = falconMetaService;

        butil::EndPoint point;
        point = butil::EndPoint(butil::IP_ANY, port);
        brpc::ServerOptions options;
        if (server.Start(point, &options) != 0)
            throw std::runtime_error("ConnectionPoolBrpcServer: fail to start server.");

        server.RunUntilAskedToQuit();
    }
    void Shutdown()
    {
        falcon::meta_service::g_falcon_meta_service = nullptr;
        if (falconMetaService != nullptr) {
            delete falconMetaService;
            falconMetaService = nullptr;
        }

        server.Stop(0);
        server.Join();
    }
};

static std::unique_ptr<ConnectionPoolBrpcServer> ConnectionPoolBrpcServerInstance = NULL;

bool PG_RunConnectionPoolBrpcServer()
{
    try {
        if (ConnectionPoolBrpcServerInstance == NULL) {
            ConnectionPoolBrpcServerInstance =
                std::make_unique<ConnectionPoolBrpcServer>(FalconConnectionPoolPort, FalconConnectionPoolSize);
            ConnectionPoolBrpcServerInstance->Run();
            return true;
        }
    } catch (const std::runtime_error &e) {
        printf("%s", e.what());
        fflush(stdout);
        return false;
    }
    return false;
}

bool PG_ShutdownConnectionPoolBrpcServer()
{
    try {
        if (ConnectionPoolBrpcServerInstance != NULL) {
            ConnectionPoolBrpcServerInstance->Shutdown();
            ConnectionPoolBrpcServerInstance = NULL;
            return true;
        }
    } catch (const std::exception &e) {
        printf("%s", e.what());
        fflush(stdout);
        return false;
    }
    return false;
}
