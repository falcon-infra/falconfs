#include "base_comm_adapter/comm_server_interface.h"

static int g_started;
static int g_stopped;
static int g_flushed;

int StartFalconCommunicationServer(falcon_meta_job_dispatch_func dispatchFunc, const char *serverIp, int serverListenPort)
{
    (void)serverIp;
    (void)serverListenPort;
    g_started++;
    if (dispatchFunc != 0) {
        dispatchFunc(0);
    }
    return 0;
}

void StopFalconCommunicationServer(void)
{
    g_stopped++;
}

void FlushFalconCommunicationCoverageData(void)
{
    g_flushed++;
}

int FalconCommCoverageStarted(void)
{
    return g_started;
}

int FalconCommCoverageStopped(void)
{
    return g_stopped;
}

int FalconCommCoverageFlushed(void)
{
    return g_flushed;
}
