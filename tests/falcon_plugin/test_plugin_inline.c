// Test plugin - INLINE type

#include <stdio.h>
#include <string.h>
#include "plugin/falcon_plugin_framework.h"

/* Plugin private data structure */
typedef struct {
    int counter;
} InlinePluginPrivateData;

int plugin_init(FalconPluginData *data)
{
    printf("[TEST PLUGIN INLINE] plugin_init() called\n");
    if (data) {
        InlinePluginPrivateData *private_data = (InlinePluginPrivateData *)data->plugin_data.raw_buffer;
        private_data->counter = 0;
        printf("[TEST PLUGIN INLINE] Init - Private data initialized\n");
    }
    return 0;
}

FalconPluginWorkType plugin_get_type(void)
{
    printf("[TEST PLUGIN INLINE] plugin_get_type() called, returning INLINE\n");
    return FALCON_PLUGIN_TYPE_INLINE;
}

int plugin_work(FalconPluginData *data)
{
    printf("[TEST PLUGIN INLINE] plugin_work() called\n");
    if (data) {
        printf("[TEST PLUGIN INLINE] Plugin name: %s\n", data->plugin_name);
        printf("[TEST PLUGIN INLINE] Shared memory buffer address: %p\n", (void*)data);

        InlinePluginPrivateData *private_data = (InlinePluginPrivateData *)data->plugin_data.raw_buffer;
        private_data->counter++;
        printf("[TEST PLUGIN INLINE] Work - Counter: %d\n", private_data->counter);
    }
    return 0;
}

void plugin_cleanup(FalconPluginData *data)
{
    printf("[TEST PLUGIN INLINE] plugin_cleanup() called\n");
    if (data) {
        InlinePluginPrivateData *private_data = (InlinePluginPrivateData *)data->plugin_data.raw_buffer;
        printf("[TEST PLUGIN INLINE] Cleanup - Final counter: %d\n", private_data->counter);
    }
}
