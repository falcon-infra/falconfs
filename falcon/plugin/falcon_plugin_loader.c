/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "postgres.h"
#include "fmgr.h"
#include "utils/elog.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/lwlock.h"
#include "miscadmin.h"
#include "tcop/tcopprot.h"

#include "plugin/falcon_plugin_framework.h"
#include "plugin/falcon_plugin_loader.h"
#include "utils/falcon_plugin_guc.h"
#include <dlfcn.h>
#include <dirent.h>
#include <string.h>
#include <stdlib.h>

static FalconPluginSharedMemory *falcon_plugin_shmem = NULL;

static bool falcon_plugin_shmem_initialized = false;

Size FalconPluginShmemSize(void)
{
    return sizeof(FalconPluginSharedMemory);
}

void FalconPluginShmemInit(void)
{
    bool found;
    Size size = sizeof(FalconPluginSharedMemory);

    falcon_plugin_shmem = (FalconPluginSharedMemory *)
        ShmemInitStruct("FalconPluginSharedMemory", size, &found);
    
    if (!found) {
        memset(falcon_plugin_shmem, 0, size);
        falcon_plugin_shmem->num_slots = FALCON_PLUGIN_MAX_PLUGINS;

        for (int i = 0; i < FALCON_PLUGIN_MAX_PLUGINS; i++) {
            falcon_plugin_shmem->plugins[i].in_use = false;
        }
        
        ereport(LOG, (errmsg("Initialized plugin shared memory with %d slots", FALCON_PLUGIN_MAX_PLUGINS)));
    }
    
    falcon_plugin_shmem_initialized = true;
}

static int FalconPluginGetFreeSlot(void)
{
    if (!falcon_plugin_shmem_initialized) {
        FalconPluginShmemInit();
    }

    for (int i = 0; i < falcon_plugin_shmem->num_slots; i++) {
        if (!falcon_plugin_shmem->plugins[i].in_use) {
            falcon_plugin_shmem->plugins[i].in_use = true;
            return i;
        }
    }

    return -1;  /* No free slots available */
}

static void FalconPluginReleaseSlot(int slot_index)
{
    if (slot_index < 0 || slot_index >= falcon_plugin_shmem->num_slots) {
        return;
    }

    FalconPluginData *plugin_data = &falcon_plugin_shmem->plugins[slot_index];

    memset(plugin_data, 0, sizeof(FalconPluginData));
    plugin_data->in_use = false;
}

/*
 * Load plugin shared library and validate required functions
 * Returns dl_handle on success, NULL on failure
 */
static void *FalconPluginLoadAndValidate(const char *plugin_path, const char *plugin_name,
                                         falcon_plugin_init_func_t *init_func,
                                         falcon_plugin_get_type_func_t *get_type_func,
                                         falcon_plugin_work_func_t *work_func,
                                         falcon_plugin_cleanup_func_t *cleanup_func)
{
    void *dl_handle = dlopen(plugin_path, RTLD_LAZY);
    if (!dl_handle) {
        ereport(WARNING, (errmsg("Failed to load plugin %s: %s", plugin_path, dlerror())));
        return NULL;
    }

    *init_func = (falcon_plugin_init_func_t)dlsym(dl_handle, FALCON_PLUGIN_INIT_FUNC_NAME);
    *get_type_func = (falcon_plugin_get_type_func_t)dlsym(dl_handle, FALCON_PLUGIN_GET_TYPE_FUNC_NAME);
    *work_func = (falcon_plugin_work_func_t)dlsym(dl_handle, FALCON_PLUGIN_WORK_FUNC_NAME);
    *cleanup_func = (falcon_plugin_cleanup_func_t)dlsym(dl_handle, FALCON_PLUGIN_CLEANUP_FUNC_NAME);

    if (!*init_func || !*get_type_func || !*work_func || !*cleanup_func) {
        ereport(WARNING, (errmsg("Plugin %s missing required functions", plugin_name)));
        dlclose(dl_handle);
        return NULL;
    }

    return dl_handle;
}

static void FalconPluginInitializeSlot(int slot_index, const char *plugin_name, const char *plugin_path)
{
    FalconPluginData *plugin_data = &falcon_plugin_shmem->plugins[slot_index];

    memset(plugin_data, 0, sizeof(FalconPluginData));
    plugin_data->in_use = true;
    strncpy(plugin_data->plugin_name, plugin_name, FALCON_PLUGIN_MAX_NAME_SIZE - 1);
    strncpy(plugin_data->plugin_path, plugin_path, FALCON_PLUGIN_MAX_PATH_SIZE - 1);
    plugin_data->main_pid = getpid();
}

static int FalconPluginExecuteInline(int slot_index, const char *plugin_name,
                                     falcon_plugin_init_func_t init_func,
                                     falcon_plugin_work_func_t work_func,
                                     falcon_plugin_cleanup_func_t cleanup_func)
{
    FalconPluginData *plugin_data = &falcon_plugin_shmem->plugins[slot_index];
    int ret;

    ret = init_func(plugin_data);
    if (ret != 0) {
        ereport(WARNING, (errmsg("Plugin %s initialization failed with code %d", plugin_name, ret)));
        return -1;
    }

    ret = work_func(plugin_data);
    ereport(LOG, (errmsg("Plugin %s INLINE execution result: %d", plugin_name, ret)));

    cleanup_func(plugin_data);

    return 0;
}

static int FalconPluginRegisterBackgroundWorker(int slot_index, const char *plugin_name,
                                                falcon_plugin_init_func_t init_func)
{
    BackgroundWorker worker;
    FalconPluginData *plugin_data = &falcon_plugin_shmem->plugins[slot_index];
    int ret;

    ret = init_func(plugin_data);
    if (ret != 0) {
        ereport(WARNING, (errmsg("Plugin %s initialization failed with code %d", plugin_name, ret)));
        return -1;
    }

    memset(&worker, 0, sizeof(BackgroundWorker));
    snprintf(worker.bgw_name, BGW_MAXLEN, "falcon_plugin_%s", plugin_name);
    strcpy(worker.bgw_type, "falcon_plugin_worker");
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    strcpy(worker.bgw_library_name, "falcon");
    strcpy(worker.bgw_function_name, "FalconPluginBackgroundWorkerMain");
    worker.bgw_main_arg = Int32GetDatum(slot_index);

    if (process_shared_preload_libraries_in_progress) {
        RegisterBackgroundWorker(&worker);
        ereport(LOG, (errmsg("Registered background worker for plugin: %s", plugin_name)));
    } else {
        BackgroundWorkerHandle *bg_handle;
        if (RegisterDynamicBackgroundWorker(&worker, &bg_handle)) {
            ereport(LOG, (errmsg("Registered dynamic background worker for plugin: %s", plugin_name)));
        } else {
            ereport(WARNING, (errmsg("Failed to register background worker for plugin: %s", plugin_name)));
            return -1;
        }
    }

    return 0;
}

/*
 * Load and execute all .so plugins from the specified directory
 */
int FalconLoadPluginsFromDirectory(const char *plugin_dir)
{
    DIR *dir;
    struct dirent *entry;
    char plugin_path[512];

    if (!plugin_dir) {
        ereport(LOG, (errmsg("Plugin directory not specified")));
        return -1;
    }

    dir = opendir(plugin_dir);
    if (!dir) {
        ereport(LOG, (errmsg("Cannot open plugin directory: %s", plugin_dir)));
        return -1;
    }

    if (!falcon_plugin_shmem_initialized) {
        FalconPluginShmemInit();
    }

    ereport(LOG, (errmsg("Loading plugins from directory: %s, Main PID: %d",
                        plugin_dir, getpid())));

    while ((entry = readdir(dir)) != NULL) {
        void *dl_handle;
        falcon_plugin_init_func_t init_func;
        falcon_plugin_get_type_func_t get_type_func;
        falcon_plugin_work_func_t work_func;
        falcon_plugin_cleanup_func_t cleanup_func;
        FalconPluginWorkType work_type;
        int slot_index;
        int ret;

        /* Skip non-.so files */
        if (strstr(entry->d_name, ".so") == NULL) {
            continue;
        }

        snprintf(plugin_path, sizeof(plugin_path), "%s/%s", plugin_dir, entry->d_name);

        dl_handle = FalconPluginLoadAndValidate(plugin_path, entry->d_name,
                                                &init_func, &get_type_func,
                                                &work_func, &cleanup_func);
        if (!dl_handle) {
            continue;
        }

        work_type = get_type_func();
        ereport(LOG, (errmsg("Plugin %s type: %s", entry->d_name,
                            (work_type == FALCON_PLUGIN_TYPE_INLINE) ? "INLINE" : "BACKGROUND")));

        slot_index = FalconPluginGetFreeSlot();
        if (slot_index < 0) {
            ereport(WARNING, (errmsg("Cannot allocate shared memory slot for plugin %s", entry->d_name)));
            dlclose(dl_handle);
            continue;
        }

        FalconPluginInitializeSlot(slot_index, entry->d_name, plugin_path);
        ereport(LOG, (errmsg("Allocated shared memory slot %d for plugin %s", slot_index, entry->d_name)));

        if (work_type == FALCON_PLUGIN_TYPE_INLINE) {
            ret = FalconPluginExecuteInline(slot_index, entry->d_name, init_func, work_func, cleanup_func);
            if (ret == 0) {
                ereport(LOG, (errmsg("Plugin %s INLINE execution completed", entry->d_name)));
            }
            FalconPluginReleaseSlot(slot_index);
            dlclose(dl_handle);
        } else {
            ret = FalconPluginRegisterBackgroundWorker(slot_index, entry->d_name, init_func);
            if (ret < 0) {
                FalconPluginReleaseSlot(slot_index);
            } else {
                ereport(LOG, (errmsg("Plugin %s BACKGROUND worker registered", entry->d_name)));
            }
            dlclose(dl_handle);
        }
    }

    closedir(dir);
    return 0;
}

void FalconPluginBackgroundWorkerMain(Datum main_arg)
{
    void *dl_handle = NULL;
    falcon_plugin_work_func_t work_func;
    falcon_plugin_cleanup_func_t cleanup_func;
    FalconPluginData *plugin_data = NULL;
    int slot_index;
    int ret;

    BackgroundWorkerUnblockSignals();

    if (!falcon_plugin_shmem_initialized) {
        FalconPluginShmemInit();
    }

    slot_index = DatumGetInt32(main_arg);

    if (slot_index < 0 || slot_index >= falcon_plugin_shmem->num_slots) {
        ereport(ERROR, (errmsg("Invalid slot_index: %d", slot_index)));
        proc_exit(1);
    }

    plugin_data = &falcon_plugin_shmem->plugins[slot_index];

    if (!plugin_data->in_use) {
        ereport(ERROR, (errmsg("Plugin slot %d is not in use", slot_index)));
        proc_exit(1);
    }

    ereport(LOG, (errmsg("Background worker started: plugin=%s", plugin_data->plugin_name)));

    dl_handle = dlopen(plugin_data->plugin_path, RTLD_LAZY);
    if (!dl_handle) {
        ereport(ERROR, (errmsg("Failed to load plugin in background worker: %s, error: %s",
                              plugin_data->plugin_path, dlerror())));
        proc_exit(1);
    }

    work_func = (falcon_plugin_work_func_t)dlsym(dl_handle, FALCON_PLUGIN_WORK_FUNC_NAME);
    cleanup_func = (falcon_plugin_cleanup_func_t)dlsym(dl_handle, FALCON_PLUGIN_CLEANUP_FUNC_NAME);

    if (!work_func || !cleanup_func) {
        ereport(ERROR, (errmsg("Plugin %s missing required functions", plugin_data->plugin_name)));
        dlclose(dl_handle);
        proc_exit(1);
    }

    ret = work_func(plugin_data);
    ereport(LOG, (errmsg("Plugin work function returned %d: %s",
                        ret, plugin_data->plugin_name)));

    ereport(LOG, (errmsg("Background worker stopping: %s", plugin_data->plugin_name)));

    cleanup_func(plugin_data);

    FalconPluginReleaseSlot(slot_index);
    dlclose(dl_handle);

    proc_exit(0);
}