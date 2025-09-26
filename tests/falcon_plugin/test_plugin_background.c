// Test plugin - BACKGROUND type

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "plugin/falcon_plugin_framework.h"

static int g_init_called = 0;
static int g_work_called = 0;
static int g_cleanup_called = 0;
static int g_work_limit = 3;

int plugin_init(void)
{
    printf("[TEST PLUGIN BACKGROUND] plugin_init() called\n");
    g_init_called++;
    return 0;
}

FalconPluginWorkType plugin_get_type(void)
{
    printf("[TEST PLUGIN BACKGROUND] plugin_get_type() called, returning BACKGROUND\n");
    return FALCON_PLUGIN_TYPE_BACKGROUND;
}

int plugin_work(void)
{
    printf("[TEST PLUGIN BACKGROUND] plugin_work() called (count: %d)\n", g_work_called + 1);
    g_work_called++;

    usleep(10000); // simulate work

    if (g_work_called >= g_work_limit) {
        printf("[TEST PLUGIN BACKGROUND] Work limit reached, stopping\n");
        return 1; // stop work
    }

    return 0; // continue working
}

void plugin_cleanup(void)
{
    printf("[TEST PLUGIN BACKGROUND] plugin_cleanup() called\n");
    g_cleanup_called++;
}

// Test helper functions
int test_get_init_count(void) { return g_init_called; }
int test_get_work_count(void) { return g_work_called; }
int test_get_cleanup_count(void) { return g_cleanup_called; }

void test_set_work_limit(int limit)
{
    g_work_limit = limit;
}

void test_reset_counters(void)
{
    g_init_called = 0;
    g_work_called = 0;
    g_cleanup_called = 0;
}