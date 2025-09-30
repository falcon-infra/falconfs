/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "postgres.h"
#include "fmgr.h"
#include "utils/elog.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "miscadmin.h"
#include "tcop/tcopprot.h"

#include "plugin/falcon_plugin_framework.h"
#include "utils/falcon_plugin_guc.h"
#include <dlfcn.h>
#include <dirent.h>
#include <string.h>
#include <stdlib.h>

static FalconPluginBackgroundData *g_plugin_data = NULL;

/*
 * Load and execute all .so plugins from the specified directory
 * This is a simplified implementation for basic loading and calling
 */
int FalconLoadPluginsFromDirectory(const char *plugin_dir)
{
    DIR *dir;
    struct dirent *entry;
    char plugin_path[512];
    void *dl_handle;
    falcon_plugin_init_func_t init_func;
    falcon_plugin_get_type_func_t get_type_func;
    falcon_plugin_work_func_t work_func;
    falcon_plugin_cleanup_func_t cleanup_func;
    int ret;
    FalconPluginWorkType work_type;

    if (!plugin_dir) {
        ereport(LOG, (errmsg("Plugin directory not specified")));
        return -1;
    }

    dir = opendir(plugin_dir);
    if (!dir) {
        ereport(LOG, (errmsg("Cannot open plugin directory: %s", plugin_dir)));
        return -1;
    }

    ereport(LOG, (errmsg("Loading plugins from directory: %s, Main PID: %d, Memory addr: %p",
                        plugin_dir, getpid(), (void*)&dl_handle)));

    while ((entry = readdir(dir)) != NULL) {
        if (strstr(entry->d_name, ".so") == NULL) {
            continue;
        }

        snprintf(plugin_path, sizeof(plugin_path), "%s/%s", plugin_dir, entry->d_name);

        dl_handle = dlopen(plugin_path, RTLD_LAZY);
        if (!dl_handle) {
            ereport(WARNING, (errmsg("Failed to load plugin %s: %s", plugin_path, dlerror())));
            continue;
        }

        init_func = (falcon_plugin_init_func_t)dlsym(dl_handle, FALCON_PLUGIN_INIT_FUNC_NAME);
        get_type_func = (falcon_plugin_get_type_func_t)dlsym(dl_handle, FALCON_PLUGIN_GET_TYPE_FUNC_NAME);
        work_func = (falcon_plugin_work_func_t)dlsym(dl_handle, FALCON_PLUGIN_WORK_FUNC_NAME);
        cleanup_func = (falcon_plugin_cleanup_func_t)dlsym(dl_handle, FALCON_PLUGIN_CLEANUP_FUNC_NAME);

        if (!init_func || !get_type_func || !work_func || !cleanup_func) {
            ereport(WARNING, (errmsg("Plugin %s missing required functions", entry->d_name)));
            dlclose(dl_handle);
            continue;
        }

        ret = init_func();
        if (ret != 0) {
            ereport(WARNING, (errmsg("Plugin %s initialization failed with code %d", entry->d_name, ret)));
            dlclose(dl_handle);
            continue;
        }

        work_type = get_type_func();
        ereport(LOG, (errmsg("Plugin %s type: %s", entry->d_name,
                            (work_type == FALCON_PLUGIN_TYPE_INLINE) ? "INLINE" : "BACKGROUND")));

        if (work_type == FALCON_PLUGIN_TYPE_INLINE) {
            /* For INLINE plugins, create temporary data and call work_func directly */
            FalconPluginBackgroundData inline_data;
            memset(&inline_data, 0, sizeof(FalconPluginBackgroundData));
            strncpy(inline_data.plugin_name, entry->d_name, FALCON_PLUGIN_MAX_NAME_SIZE - 1);
            strncpy(inline_data.plugin_path, plugin_path, FALCON_PLUGIN_MAX_NAME_SIZE - 1);
            if (falcon_plugin_custom_config) {
                strncpy(inline_data.custom_config, falcon_plugin_custom_config, FALCON_PLUGIN_MAX_CONFIG_SIZE - 1);
            }
            inline_data.main_pid = getpid();
            
            ret = work_func(&inline_data);
            ereport(LOG, (errmsg("Plugin %s INLINE work result: %d", entry->d_name, ret)));
            cleanup_func();
            dlclose(dl_handle);
        } else {
            /* For BACKGROUND plugins, prepare data for background worker */
            BackgroundWorker worker;
            
            /* Allocate data structure for this plugin */
            if (!g_plugin_data) {
                g_plugin_data = (FalconPluginBackgroundData*)malloc(sizeof(FalconPluginBackgroundData));
            }
            memset(g_plugin_data, 0, sizeof(FalconPluginBackgroundData));
            
            /* Fill information to pass to background worker */
            strncpy(g_plugin_data->plugin_name, entry->d_name, FALCON_PLUGIN_MAX_NAME_SIZE - 1);
            strncpy(g_plugin_data->plugin_path, plugin_path, FALCON_PLUGIN_MAX_NAME_SIZE - 1);
            if (falcon_plugin_custom_config) {
                strncpy(g_plugin_data->custom_config, falcon_plugin_custom_config, FALCON_PLUGIN_MAX_CONFIG_SIZE - 1);
            }
            g_plugin_data->main_pid = getpid();

            memset(&worker, 0, sizeof(BackgroundWorker));
            snprintf(worker.bgw_name, BGW_MAXLEN, "falcon_plugin_%s", entry->d_name);
            strcpy(worker.bgw_type, "falcon_plugin_worker");
            worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
            worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
            worker.bgw_restart_time = BGW_NEVER_RESTART;
            strcpy(worker.bgw_library_name, "falcon");
            strcpy(worker.bgw_function_name, "FalconPluginBackgroundWorkerMain");
            worker.bgw_main_arg = (Datum) 0;

            if (process_shared_preload_libraries_in_progress) {
                RegisterBackgroundWorker(&worker);
                ereport(LOG, (errmsg("Registered background worker for plugin: %s", entry->d_name)));
            } else {
                BackgroundWorkerHandle *bg_handle;
                if (RegisterDynamicBackgroundWorker(&worker, &bg_handle)) {
                    ereport(LOG, (errmsg("Registered dynamic background worker for plugin: %s", entry->d_name)));
                } else {
                    ereport(WARNING, (errmsg("Failed to register background worker for plugin: %s", entry->d_name)));
                }
            }

            dlclose(dl_handle);
        }

        ereport(LOG, (errmsg("Plugin %s loaded and executed", entry->d_name)));
    }

    closedir(dir);
    return 0;
}

void FalconPluginBackgroundWorkerMain(Datum main_arg)
{
    void *dl_handle = NULL;
    falcon_plugin_work_func_t work_func;
    int ret;
    FalconPluginBackgroundData *plugin_data = NULL;

    BackgroundWorkerUnblockSignals();

    /* Get the plugin data passed from parent process */
    /* Note: This data was copied during fork(), so it's safe to access */
    plugin_data = g_plugin_data;
    
    if (!plugin_data || strlen(plugin_data->plugin_path) == 0) {
        ereport(ERROR, (errmsg("Plugin data not found in background worker")));
        proc_exit(1);
    }

    ereport(LOG, (errmsg("Falcon plugin background worker started for: %s, PID: %d",
                        plugin_data->plugin_name, getpid())));

    dl_handle = dlopen(plugin_data->plugin_path, RTLD_LAZY);
    if (!dl_handle) {
        ereport(ERROR, (errmsg("Failed to reload plugin in background worker: %s, error: %s",
                              plugin_data->plugin_path, dlerror())));
        proc_exit(1);
    }

    work_func = (falcon_plugin_work_func_t)dlsym(dl_handle, FALCON_PLUGIN_WORK_FUNC_NAME);

    if (!work_func) {
        ereport(ERROR, (errmsg("Plugin %s missing work function in background worker", plugin_data->plugin_name)));
        dlclose(dl_handle);
        proc_exit(1);
    }

    ereport(LOG, (errmsg("Plugin %s background worker ready", plugin_data->plugin_name)));

    ret = work_func(plugin_data);
    ereport(LOG, (errmsg("Plugin work function returned %d for background worker: %s",
                        ret, plugin_data->plugin_name)));

    ereport(LOG, (errmsg("Plugin background worker stopping: %s", plugin_data->plugin_name)));
    dlclose(dl_handle);

    proc_exit(0);
}