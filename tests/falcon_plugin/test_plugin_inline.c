// Test plugin - INLINE type

#include <stdio.h>
#include <stdlib.h>
#include "plugin/falcon_plugin_framework.h"

static int g_init_called = 0;
static int g_work_called = 0;
static int g_cleanup_called = 0;

int plugin_init(void)
{
    printf("[TEST PLUGIN INLINE] plugin_init() called\n");
    g_init_called++;
    return 0;
}

FalconPluginWorkType plugin_get_type(void)
{
    printf("[TEST PLUGIN INLINE] plugin_get_type() called, returning INLINE\n");
    return FALCON_PLUGIN_TYPE_INLINE;
}

int plugin_work(void)
{
    printf("[TEST PLUGIN INLINE] plugin_work() called\n");
    g_work_called++;
    return 1; // stop work for INLINE type
}

void plugin_cleanup(void)
{
    printf("[TEST PLUGIN INLINE] plugin_cleanup() called\n");
    g_cleanup_called++;
}

// Test helper functions
int test_get_init_count(void) { return g_init_called; }
int test_get_work_count(void) { return g_work_called; }
int test_get_cleanup_count(void) { return g_cleanup_called; }

void test_reset_counters(void)
{
    g_init_called = 0;
    g_work_called = 0;
    g_cleanup_called = 0;
}