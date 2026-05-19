#include <gtest/gtest.h>

#include <cstdarg>
#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

extern "C" {
#include "postgres.h"
#include "control/control_flag.h"
#include "connection_pool/falcon_connection_pool.h"
#include "fmgr.h"
#include "port/atomics.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "storage/lwlock.h"
#include "utils/shmem_control.h"
#include "utils/resowner.h"
#include "utils/palloc.h"

Datum falcon_start_background_service(PG_FUNCTION_ARGS);
}

#ifdef vsnprintf
#undef vsnprintf
#endif
#ifdef snprintf
#undef snprintf
#endif

namespace {

std::vector<unsigned char> g_poolShmem;
bool g_poolShmemFound = false;
int g_startPoolResult = 1;
int g_startPoolCount = 0;
int g_destroyPoolCount = 0;
int g_dispatchCount = 0;
int g_transactionCount = 0;
pqsigfunc g_lastSignalHandler = nullptr;
std::vector<std::string> g_poolLogs;

void ResetPoolHarness()
{
    g_poolShmem.clear();
    g_poolShmemFound = false;
    g_startPoolResult = 1;
    g_startPoolCount = 0;
    g_destroyPoolCount = 0;
    g_dispatchCount = 0;
    g_transactionCount = 0;
    g_lastSignalHandler = nullptr;
    g_poolLogs.clear();
    FalconConnectionPoolShmemSize = 2 * 1024 * 1024;
    FalconCommunicationPluginPath = nullptr;
    FalconCommunicationServerIp = const_cast<char *>("127.0.0.1");
    FalconConnectionPoolPort = 45678;
    FalconPGPort = 0;
    PostPortNumber = 15432;
}

std::string CommPluginPath()
{
    return "build/tests/falcon/test_comm_plugins/libtest_comm_plugin_coverage.so";
}

} // namespace

extern "C" {

int PostPortNumber = 15432;
ResourceOwner CurrentResourceOwner = nullptr;
MemoryContext CurrentMemoryContext = nullptr;
MemoryContext TopMemoryContext = nullptr;

void *ShmemInitStruct(const char *, Size size, bool *found)
{
    *found = g_poolShmemFound;
    if (g_poolShmem.empty()) {
        g_poolShmem.resize(size);
        std::memset(g_poolShmem.data(), 0, size);
    }
    return g_poolShmem.data();
}

bool errstart(int, const char *)
{
    return true;
}

bool errstart_cold(int elevel, const char *domain)
{
    return errstart(elevel, domain);
}

void errfinish(const char *, int, const char *)
{
}

int errmsg(const char *fmt, ...)
{
    char buffer[512];
    va_list args;
    va_start(args, fmt);
    std::vsnprintf(buffer, sizeof(buffer), fmt, args);
    va_end(args);
    g_poolLogs.emplace_back(buffer);
    return 0;
}

int errmsg_internal(const char *fmt, ...)
{
    char buffer[512];
    va_list args;
    va_start(args, fmt);
    std::vsnprintf(buffer, sizeof(buffer), fmt, args);
    va_end(args);
    g_poolLogs.emplace_back(buffer);
    return 0;
}

int errcode(int)
{
    return 0;
}

int pg_vsnprintf(char *str, size_t count, const char *fmt, va_list args)
{
    return std::vsnprintf(str, count, fmt, args);
}

int pg_snprintf(char *str, size_t count, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    int ret = std::vsnprintf(str, count, fmt, args);
    va_end(args);
    return ret;
}

void elog_start(const char *, int, const char *) {}
void elog_finish(int, const char *fmt, ...)
{
    char buffer[512];
    va_list args;
    va_start(args, fmt);
    std::vsnprintf(buffer, sizeof(buffer), fmt, args);
    va_end(args);
    g_poolLogs.emplace_back(buffer);
}

pqsigfunc pqsignal(int, pqsigfunc handler)
{
    g_lastSignalHandler = handler;
    return handler;
}

void BackgroundWorkerUnblockSignals(void) {}
void BackgroundWorkerInitializeConnection(const char *, const char *, uint32) {}
void StartTransactionCommand(void)
{
    g_transactionCount++;
}
void CommitTransactionCommand(void) {}
bool CheckFalconHasBeenLoaded(void)
{
    return true;
}
bool RecoveryInProgress(void)
{
    return false;
}

ResourceOwner ResourceOwnerCreate(ResourceOwner, const char *)
{
    return reinterpret_cast<ResourceOwner>(0x1);
}

MemoryContext AllocSetContextCreateInternal(MemoryContext, const char *, Size, Size, Size)
{
    return reinterpret_cast<MemoryContext>(0x2);
}

int LWLockNewTrancheId(void)
{
    return 77;
}

void LWLockRegisterTranche(int, const char *) {}

void LWLockInitialize(LWLock *, int) {}

bool StartPGConnectionPool(void)
{
    g_startPoolCount++;
    return g_startPoolResult != 0;
}

void DestroyPGConnectionPool(void)
{
    g_destroyPoolCount++;
}

void FalconDispatchMetaJob2PGConnectionPool(void *job)
{
    EXPECT_EQ(job, nullptr);
    g_dispatchCount++;
}

} // extern "C"

TEST(FalconConnectionPoolCCoverageUT, InitializesSharedMemoryAllocator)
{
    /* Initialize fresh and existing shared-memory blocks for the pool allocator. */
    ResetPoolHarness();
    EXPECT_EQ(FalconConnectionPoolShmemsize(), FalconConnectionPoolShmemSize);

    FalconConnectionPoolShmemInit();
    EXPECT_FALSE(g_poolShmem.empty());

    g_poolShmemFound = true;
    FalconConnectionPoolShmemInit();
}

TEST(FalconConnectionPoolCCoverageUT, ControlFlagsTrackBackgroundServiceAndAbortState)
{
    /* Initialize control shared memory and toggle process-wide service/abort flags. */
    ResetPoolHarness();
    EXPECT_EQ(FalconControlShmemsize(), sizeof(ShmemControlData) + sizeof(pg_atomic_uint32));

    FalconControlShmemInit();
    EXPECT_FALSE(CheckFalconBackgroundServiceStarted());
    EXPECT_FALSE(FalconIsInAbortProgress());

    falcon_start_background_service(nullptr);
    EXPECT_TRUE(CheckFalconBackgroundServiceStarted());

    FalconEnterAbortProgress();
    EXPECT_TRUE(FalconIsInAbortProgress());
    FalconQuitAbortProgress();
    EXPECT_FALSE(FalconIsInAbortProgress());

    /* Reinitializing an existing block must preserve the already-started service flag. */
    g_poolShmemFound = true;
    FalconControlShmemInit();
    EXPECT_TRUE(CheckFalconBackgroundServiceStarted());
}

TEST(FalconConnectionPoolCCoverageUT, RunsCommunicationPluginAndDaemonMain)
{
    /* Load the test communication plugin and run both direct and background daemon entry paths. */
    ResetPoolHarness();
    std::string pluginPath = CommPluginPath();
    FalconCommunicationPluginPath = pluginPath.data();

    RunConnectionPoolServer();
    EXPECT_EQ(g_startPoolCount, 1);
    EXPECT_EQ(g_dispatchCount, 1);

    FalconDaemonConnectionPoolProcessMain(0);
    EXPECT_EQ(g_transactionCount, 1);
    EXPECT_EQ(FalconPGPort, PostPortNumber);
    EXPECT_EQ(g_startPoolCount, 2);
    EXPECT_EQ(g_dispatchCount, 2);
    ASSERT_NE(g_lastSignalHandler, nullptr);
    g_lastSignalHandler(SIGTERM);
    EXPECT_EQ(g_destroyPoolCount, 1);
}
