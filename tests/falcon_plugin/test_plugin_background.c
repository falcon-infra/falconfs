// Test plugin - BACKGROUND type

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include "plugin/falcon_plugin_framework.h"

/* Plugin private data structure */
typedef struct {
    int counter;
    pid_t worker_pid;
} BackgroundPluginPrivateData;

int plugin_init(FalconPluginData *data)
{
    printf("[TEST PLUGIN BACKGROUND] plugin_init() called\n");
    if (data) {
        BackgroundPluginPrivateData *private_data = (BackgroundPluginPrivateData *)data->plugin_data.raw_buffer;
        private_data->counter = 0;
        private_data->worker_pid = getpid();
        printf("[TEST PLUGIN BACKGROUND] Init - Worker PID: %d\n", private_data->worker_pid);
    }
    return 0;
}

FalconPluginWorkType plugin_get_type(void)
{
    printf("[TEST PLUGIN BACKGROUND] plugin_get_type() called, returning BACKGROUND\n");
    return FALCON_PLUGIN_TYPE_BACKGROUND;
}

int plugin_work(FalconPluginData *data)
{
    printf("[TEST PLUGIN BACKGROUND] plugin_work() called\n");
    if (data) {
        printf("[TEST PLUGIN BACKGROUND] Plugin name: %s\n", data->plugin_name);
        printf("[TEST PLUGIN BACKGROUND] Shared memory buffer address: %p\n", (void*)data);

        BackgroundPluginPrivateData *private_data = (BackgroundPluginPrivateData *)data->plugin_data.raw_buffer;
        private_data->counter++;
        printf("[TEST PLUGIN BACKGROUND] Work - Counter: %d, Worker PID: %d\n",
               private_data->counter, private_data->worker_pid);

        /* Simulate some background work */
        usleep(10000);
    }
    printf("[TEST PLUGIN BACKGROUND] Background work completed\n");
    return 0;
}

void plugin_cleanup(FalconPluginData *data)
{
    printf("[TEST PLUGIN BACKGROUND] plugin_cleanup() called\n");
    if (data) {
        BackgroundPluginPrivateData *private_data = (BackgroundPluginPrivateData *)data->plugin_data.raw_buffer;
        printf("[TEST PLUGIN BACKGROUND] Cleanup - Final counter: %d\n", private_data->counter);
    }
}
