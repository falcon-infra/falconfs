/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_PLUGIN_FRAMEWORK_H
#define FALCON_PLUGIN_FRAMEWORK_H

#include <stdbool.h>
#include <stddef.h>
#include <unistd.h>

#ifdef __cplusplus
extern "C" {
#endif

#define FALCON_PLUGIN_INIT_FUNC_NAME          "plugin_init"
#define FALCON_PLUGIN_WORK_FUNC_NAME          "plugin_work"
#define FALCON_PLUGIN_GET_TYPE_FUNC_NAME      "plugin_get_type"
#define FALCON_PLUGIN_CLEANUP_FUNC_NAME       "plugin_cleanup"

#define FALCON_PLUGIN_MAX_CONFIG_SIZE     4096
#define FALCON_PLUGIN_MAX_NAME_SIZE       256
#define FALCON_PLUGIN_MAX_PATH_SIZE       1024
#define FALCON_PLUGIN_BUFFER_SIZE         1024
#define FALCON_PLUGIN_MAX_PLUGINS         100

typedef enum {
    FALCON_PLUGIN_TYPE_INLINE = 0,
    FALCON_PLUGIN_TYPE_BACKGROUND = 1
} FalconPluginWorkType;

typedef struct FalconPluginData {
    char plugin_name[FALCON_PLUGIN_MAX_NAME_SIZE];
    char plugin_path[FALCON_PLUGIN_MAX_PATH_SIZE];
    pid_t main_pid;
    bool in_use;

    union {
        char raw_buffer[FALCON_PLUGIN_BUFFER_SIZE];
    } plugin_data;
} FalconPluginData;

typedef struct FalconPluginSharedMemory {
    int num_slots;
    FalconPluginData plugins[FALCON_PLUGIN_MAX_PLUGINS];
} FalconPluginSharedMemory;

typedef int (*falcon_plugin_init_func_t)(FalconPluginData *data);
typedef int (*falcon_plugin_work_func_t)(FalconPluginData *data);
typedef FalconPluginWorkType (*falcon_plugin_get_type_func_t)(void);
typedef void (*falcon_plugin_cleanup_func_t)(FalconPluginData *data);

#ifdef __cplusplus
}
#endif

#endif /* FALCON_PLUGIN_FRAMEWORK_H */