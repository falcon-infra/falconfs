// Invalid test plugin - missing required functions

#include <stdio.h>
#include <stdlib.h>

// Only has some functions, missing plugin_get_type and plugin_work intentionally

void plugin_dummy_function(void)
{
    printf("[TEST PLUGIN INVALID] plugin_dummy_function() called\n");
}