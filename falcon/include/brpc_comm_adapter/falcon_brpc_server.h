/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_BRPC_SERVER_H
#define FALCON_BRPC_SERVER_H
#include "base_comm_adapter/comm_server_interface.h"

#ifdef __cplusplus
extern "C" {
#endif
// define start interface of falcon brpc Server
int StartFalconCommunicationServer(falcon_meta_job_dispatch_func dispatchFunc,
                                   const char *serverIp,
                                   int serverListenPort);
// define shut down interface of falcon brpc Server
int StopFalconCommunicationServer(void);
#ifdef __cplusplus
}
#endif

#endif // FALCON_BRPC_SERVER_H
