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
#include <dlfcn.h>
#include <dirent.h>
#include <string.h>
#include <stdlib.h>

/* 插件加载实现 - 支持真正的background worker */

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

        if (!init_func || !get_type_func || !work_func) {
            ereport(WARNING, (errmsg("Plugin %s missing required functions", entry->d_name)));
            dlclose(dl_handle);
            continue;
        }

        ret = init_func();
        if (ret != 0) {
            ereport(WARNING, (errmsg("Plugin %s initialization failed", entry->d_name)));
            if (cleanup_func) cleanup_func();
            dlclose(dl_handle);
            continue;
        }

        work_type = get_type_func();
        ereport(LOG, (errmsg("Plugin %s type: %s", entry->d_name,
                            (work_type == FALCON_PLUGIN_TYPE_INLINE) ? "INLINE" : "BACKGROUND")));

        if (work_type == FALCON_PLUGIN_TYPE_INLINE) {
            ret = work_func();
            ereport(LOG, (errmsg("Plugin %s INLINE work result: %d", entry->d_name, ret)));
            if (cleanup_func) {
                cleanup_func();
            }
            dlclose(dl_handle);
        } else {
            BackgroundWorker worker;
            char *plugin_path_copy = strdup(plugin_path);

            memset(&worker, 0, sizeof(BackgroundWorker));
            snprintf(worker.bgw_name, BGW_MAXLEN, "falcon_plugin_%s", entry->d_name);
            strcpy(worker.bgw_type, "falcon_plugin_worker");
            worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
            worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
            worker.bgw_restart_time = BGW_NEVER_RESTART;
            strcpy(worker.bgw_library_name, "falcon");
            strcpy(worker.bgw_function_name, "FalconPluginBackgroundWorkerMain");
            worker.bgw_main_arg = PointerGetDatum(plugin_path_copy);

            if (process_shared_preload_libraries_in_progress) {
                RegisterBackgroundWorker(&worker);
                ereport(LOG, (errmsg("Registered background worker for plugin: %s", entry->d_name)));
            } else {
                BackgroundWorkerHandle *bg_handle;
                if (RegisterDynamicBackgroundWorker(&worker, &bg_handle)) {
                    ereport(LOG, (errmsg("Registered dynamic background worker for plugin: %s", entry->d_name)));
                } else {
                    ereport(WARNING, (errmsg("Failed to register background worker for plugin: %s", entry->d_name)));
                    free(plugin_path_copy);
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
    char *plugin_path = DatumGetPointer(main_arg);
    void *dl_handle = NULL;
    falcon_plugin_init_func_t init_func;
    falcon_plugin_work_func_t work_func;
    falcon_plugin_cleanup_func_t cleanup_func;
    int ret;

    BackgroundWorkerUnblockSignals();

    ereport(LOG, (errmsg("Falcon plugin background worker started for: %s, PID: %d, Memory addr: %p",
                        plugin_path, getpid(), (void*)&dl_handle)));

    dl_handle = dlopen(plugin_path, RTLD_LAZY);
    if (!dl_handle) {
        ereport(ERROR, (errmsg("Failed to reload plugin in background worker: %s, error: %s",
                              plugin_path, dlerror())));
        proc_exit(1);
    }

    init_func = (falcon_plugin_init_func_t)dlsym(dl_handle, FALCON_PLUGIN_INIT_FUNC_NAME);
    work_func = (falcon_plugin_work_func_t)dlsym(dl_handle, FALCON_PLUGIN_WORK_FUNC_NAME);
    cleanup_func = (falcon_plugin_cleanup_func_t)dlsym(dl_handle, FALCON_PLUGIN_CLEANUP_FUNC_NAME);

    if (!init_func || !work_func) {
        ereport(ERROR, (errmsg("Plugin %s missing required functions in background worker", plugin_path)));
        dlclose(dl_handle);
        proc_exit(1);
    }

    ret = init_func();
    if (ret != 0) {
        ereport(ERROR, (errmsg("Plugin initialization failed in background worker: %s", plugin_path)));
        if (cleanup_func) cleanup_func();
        dlclose(dl_handle);
        proc_exit(1);
    }

    ereport(LOG, (errmsg("Plugin %s initialized in background worker", plugin_path)));

    for (;;) {
        CHECK_FOR_INTERRUPTS();

        ret = work_func();
        if (ret != 0) {
            ereport(LOG, (errmsg("Plugin work function returned %d, stopping background worker: %s",
                                ret, plugin_path)));
            break;
        }
    }

    ereport(LOG, (errmsg("Plugin background worker stopping: %s", plugin_path)));
    if (cleanup_func) {
        cleanup_func();
    }
    dlclose(dl_handle);

    free(plugin_path);

    proc_exit(0);
}