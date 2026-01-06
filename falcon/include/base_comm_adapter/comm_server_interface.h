/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef COMM_SERVER_INTERFACE_H
#define COMM_SERVER_INTERFACE_H

#include <stdbool.h>
#include <stddef.h>
#include <unistd.h>

#define FALCON_PLUGIN_START_COMM_FUNC_NAME "StartFalconCommunicationServer"
#define FALCON_PLUGIN_STOP_COMM_FUNC_NAME "StopFalconCommunicationServer"

// the meta job dispatch function prototype for communication plugin to use.
// implement by falcon and transfer to communication plugin by start function.
typedef void (*falcon_meta_job_dispatch_func)(void *job);

// the function prototype of communication server start, need implemented in the plugin.
typedef int (*falcon_plugin_start_comm_func_t)(falcon_meta_job_dispatch_func dispatchFunc,
                                               const char *serverIp,
                                               int serverListenPort);

// the function prototype of communication server stop, need implemented in the plugin.
typedef void (*falcon_plugin_stop_comm_func_t)();

#endif // COMM_SERVER_INTERFACE_H