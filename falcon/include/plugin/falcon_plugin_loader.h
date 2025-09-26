/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_PLUGIN_LOADER_H
#define FALCON_PLUGIN_LOADER_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Load and execute all .so plugins from the specified directory
 *
 * @param plugin_dir: Plugin directory path
 * @return: 0 on success, negative on error
 *
 * Functionality:
 * - INLINE type plugins: Execute directly in main process then cleanup
 * - BACKGROUND type plugins: Create PostgreSQL background worker process
 */
int FalconLoadPluginsFromDirectory(const char *plugin_dir);

/*
 * Background worker main function
 * Called by PostgreSQL background worker system
 * @param main_arg: PostgreSQL Datum argument (only defined in PostgreSQL context)
 */
#ifdef POSTGRES_H
void FalconPluginBackgroundWorkerMain(Datum main_arg);
#endif

#ifdef __cplusplus
}
#endif

#endif /* FALCON_PLUGIN_LOADER_H */