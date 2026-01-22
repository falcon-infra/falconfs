/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_PLUGIN_LOADER_H
#define FALCON_PLUGIN_LOADER_H

#include "plugin/falcon_plugin_framework.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Initialize plugin system and register background plugins
 * Must be called during _PG_init() phase
 *
 * @param plugin_dir: Plugin directory path
 * @return: 0 on success, negative on error
 *
 * This function will:
 * 1. Register BACKGROUND plugins immediately (during _PG_init)
 * 2. Setup a hook to automatically execute INLINE plugins after shmem init
 */
int FalconPluginSystemInit(const char *plugin_dir);

/*
 * Background worker main function
 * Called by PostgreSQL background worker system
 * @param main_arg: PostgreSQL Datum argument (only defined in PostgreSQL context)
 */
#ifdef POSTGRES_H
void FalconPluginBackgroundWorkerMain(Datum main_arg);

/*
 * Shared memory size calculation
 * Returns the size needed for plugin shared memory
 */
Size FalconPluginShmemSize(void);

/*
 * Initialize plugin shared memory
 * Called during PostgreSQL shared memory initialization
 */
void FalconPluginShmemInit(void);

void FalconPluginInitBackgroundPlugins(void);
#endif

#ifdef __cplusplus
}
#endif

#endif /* FALCON_PLUGIN_LOADER_H */
