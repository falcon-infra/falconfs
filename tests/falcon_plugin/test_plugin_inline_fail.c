#include "plugin/falcon_plugin_framework.h"

int plugin_init(FalconPluginData *data)
{
    (void)data;
    /* Test sentinel: any non-zero init result must make the loader skip inline work. */
    return -1;
}

FalconPluginWorkType plugin_get_type(void)
{
    return FALCON_PLUGIN_TYPE_INLINE;
}

int plugin_work(FalconPluginData *data)
{
    (void)data;
    return 0;
}

void plugin_cleanup(FalconPluginData *data)
{
    (void)data;
}
