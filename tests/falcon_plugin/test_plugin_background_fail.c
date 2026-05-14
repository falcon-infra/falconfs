#include "plugin/falcon_plugin_framework.h"

int plugin_init(FalconPluginData *data)
{
    (void)data;
    /* Test sentinel: any non-zero init result must release the background slot. */
    return -1;
}

FalconPluginWorkType plugin_get_type(void)
{
    return FALCON_PLUGIN_TYPE_BACKGROUND;
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
