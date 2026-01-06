/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "brpc_comm_adapter/falcon_brpc_server.h"

#include <brpc/server.h>
#include <functional>
#include <memory>
#include <sstream>
#include "base_comm_adapter/base_meta_service_job.h"
#include "brpc_comm_adapter/brpc_meta_service_imp.h"
class FalconBrpcServer {
  public:
    FalconBrpcServer(falcon_meta_job_dispatch_func dispatchFun, const char *serverIp, int port)
        : m_jobDispatchFunc(dispatchFun),
          m_serverIp(serverIp),
          m_port(port)
    {
    }
    void Run()
    {
        falcon::meta_proto::BrpcMetaServiceImpl BrpcMetaServiceImpl(m_jobDispatchFunc);
        if (m_server.AddService(&BrpcMetaServiceImpl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            throw std::runtime_error("FalconBrpcServer: brpc server AddService failed");
        }

        butil::ip_t brpcServerIp;
        int ret = butil::str2ip(m_serverIp.c_str(), &brpcServerIp);
        if (ret != 0) {
            printf("FalconBrpcServer: failed to convert %s to brpc ip_t type, using 127.0.0.1 as server ip.",
                   m_serverIp.c_str());
            brpcServerIp = butil::IP_ANY;
        } else {
            printf("FalconBrpcServer: convert %s to brpc ip_t type success, using it as server ip.",
                   m_serverIp.c_str());
        }

        butil::EndPoint point;
        point = butil::EndPoint(brpcServerIp, m_port);
        brpc::ServerOptions options;
        if (m_server.Start(point, &options) != 0)
            throw std::runtime_error("FalconBrpcServer: failed to start server.");

        m_server.RunUntilAskedToQuit();
    }
    void Shutdown()
    {
        m_server.Stop(0);
        m_server.Join();
    }

  private:
    // the function for dispatch metaServerJob
    falcon_meta_job_dispatch_func m_jobDispatchFunc;
    // the ip for brpc server
    std::string m_serverIp;
    // the listen port for brpc server
    int m_port;
    brpc::Server m_server;
};

static std::unique_ptr<FalconBrpcServer> g_falconBrpcServerInstance = NULL;
int StartFalconCommunicationServer(falcon_meta_job_dispatch_func dispatchFunc, const char *serverIp, int serverListenPort)
{
    try {
        if (g_falconBrpcServerInstance == NULL) {
            g_falconBrpcServerInstance = std::make_unique<FalconBrpcServer>(dispatchFunc, serverIp, serverListenPort);
            g_falconBrpcServerInstance->Run();
            return true;
        }
    } catch (const std::runtime_error &e) {
        printf("%s", e.what());
        fflush(stdout);
        return 1;
    }
    return 0;
}

int StopFalconCommunicationServer()
{
    try {
        if (g_falconBrpcServerInstance != NULL) {
            g_falconBrpcServerInstance->Shutdown();
            g_falconBrpcServerInstance = NULL;
            return 0;
        }
    } catch (const std::exception &e) {
        printf("%s", e.what());
        fflush(stdout);
        return 1;
    }
    return 1;
}
