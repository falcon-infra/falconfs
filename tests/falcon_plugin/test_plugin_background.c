#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "plugin/falcon_plugin_framework.h"

static int g_work_called = 0;
static int g_work_limit = 3;
static int g_initialized = 0;

int plugin_init(void)
{
    printf("[TEST PLUGIN BACKGROUND] plugin_init() called\n");
    return 0;
}

FalconPluginWorkType plugin_get_type(void)
{
    printf("[TEST PLUGIN BACKGROUND] plugin_get_type() called, returning BACKGROUND\n");
    return FALCON_PLUGIN_TYPE_BACKGROUND;
}

int plugin_work(const FalconPluginBackgroundData *data)
{
    printf("[TEST PLUGIN BACKGROUND] plugin_work() called for single execution\n");
    
    if (data) {
        printf("[TEST PLUGIN BACKGROUND] Plugin name: %s\n", data->plugin_name);
        printf("[TEST PLUGIN BACKGROUND] Plugin path: %s\n", data->plugin_path);
        printf("[TEST PLUGIN BACKGROUND] Main PID: %d\n", data->main_pid);
        printf("[TEST PLUGIN BACKGROUND] Current PID: %d\n", getpid());
        
        /* Parse and use custom_config (JSON string from GUC) */
        if (data->custom_config && strlen(data->custom_config) > 0) {
            printf("[TEST PLUGIN BACKGROUND] Custom config: %s\n", data->custom_config);
            /* parse this JSON to get configuration */
            /* Example: parse_json_config(data->custom_config); */
        }
    }
    
    g_work_called++;
    
    /* Simulate some background work */
    usleep(10000);
    
    printf("[TEST PLUGIN BACKGROUND] Background work completed\n");
    
    /* Return 0 for success (single execution completed) */
    return 0;
}

void plugin_cleanup(void)
{
    printf("[TEST PLUGIN BACKGROUND] plugin_cleanup() called\n");
}

int test_get_work_count(void) { return g_work_called; }

void test_set_work_limit(int limit)
{
    g_work_limit = limit;
}

void test_reset_counters(void)
{
    g_work_called = 0;
    g_initialized = 0;
}