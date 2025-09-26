// Invalid test plugin - missing required functions

#include <stdio.h>
#include <stdlib.h>

int plugin_init(void)
{
    printf("[TEST PLUGIN INVALID] plugin_init() called\n");
    return 0;
}

// Missing plugin_get_type and plugin_work intentionally

void plugin_cleanup(void)
{
    printf("[TEST PLUGIN INVALID] plugin_cleanup() called\n");
}