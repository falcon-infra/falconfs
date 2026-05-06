#include <gtest/gtest.h>

#include <cstdarg>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

extern "C" {
#include "postgres.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "storage/ipc.h"
#include "utils/resowner.h"
#include "utils/palloc.h"
#include "plugin/falcon_plugin_loader.h"
#include "connection_pool/connection_pool_config.h"
}

#ifdef vsnprintf
#undef vsnprintf
#endif
#ifdef snprintf
#undef snprintf
#endif

namespace {

std::vector<unsigned char> g_pluginShmem;
bool g_pluginShmemFound = false;
std::vector<std::string> g_pluginLogs;
std::vector<BackgroundWorker> g_registeredWorkers;

void ResetPluginHarness()
{
    g_pluginShmem.clear();
    g_pluginShmemFound = false;
    g_pluginLogs.clear();
    g_registeredWorkers.clear();
    shmem_startup_hook = nullptr;
    FalconNodeLocalIp = nullptr;
    PostPortNumber = 5432;
    FalconConnectionPoolPort = 15432;
}

std::filesystem::path FindBuiltPluginDir()
{
    std::vector<std::filesystem::path> candidates = {
        "build/tests/falcon_plugin/test_plugins",
        "tests/falcon_plugin/test_plugins",
        "./test_plugins",
    };
    for (const auto &candidate : candidates) {
        if (std::filesystem::exists(candidate / "libtest_plugin_inline.so")) {
            return candidate;
        }
    }
    return {};
}

std::filesystem::path PrepareRuntimePluginDir()
{
    auto builtDir = FindBuiltPluginDir();
    EXPECT_FALSE(builtDir.empty());

    auto runtimeDir = std::filesystem::current_path() / "plugin_loader_coverage_dir";
    std::filesystem::remove_all(runtimeDir);
    std::filesystem::create_directories(runtimeDir);

    std::filesystem::copy_file(builtDir / "libtest_plugin_inline.so",
                               runtimeDir / "libtest_inline.so",
                               std::filesystem::copy_options::overwrite_existing);
    std::filesystem::copy_file(builtDir / "libtest_plugin_background.so",
                               runtimeDir / "libtest_background.so",
                               std::filesystem::copy_options::overwrite_existing);
    std::filesystem::copy_file(builtDir / "libtest_plugin_invalid.so",
                               runtimeDir / "libtest_invalid.so",
                               std::filesystem::copy_options::overwrite_existing);
    std::ofstream(runtimeDir / "readme.txt") << "not a plugin\n";
    return runtimeDir;
}

} // namespace

extern "C" {

char *FalconNodeLocalIp = nullptr;
int FalconConnectionPoolPort = 15432;
int PostPortNumber = 5432;
shmem_startup_hook_type shmem_startup_hook = nullptr;
BackgroundWorker *MyBgworkerEntry = nullptr;
ResourceOwner CurrentResourceOwner = nullptr;
MemoryContext CurrentMemoryContext = nullptr;
MemoryContext TopMemoryContext = nullptr;

void *ShmemInitStruct(const char *, Size size, bool *found)
{
    *found = g_pluginShmemFound;
    if (g_pluginShmem.empty()) {
        g_pluginShmem.resize(size);
        std::memset(g_pluginShmem.data(), 0, size);
    }
    return g_pluginShmem.data();
}

char *pstrdup(const char *in)
{
    return in == nullptr ? nullptr : strdup(in);
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
    g_pluginLogs.emplace_back(buffer);
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

void RegisterBackgroundWorker(BackgroundWorker *worker)
{
    g_registeredWorkers.push_back(*worker);
}

bool CheckFalconHasBeenLoaded(void)
{
    return true;
}

bool RecoveryInProgress(void)
{
    return false;
}

void BackgroundWorkerUnblockSignals(void) {}
void BackgroundWorkerInitializeConnection(const char *, const char *, uint32) {}
void StartTransactionCommand(void) {}
void CommitTransactionCommand(void) {}
void proc_exit(int)
{
    std::abort();
}

ResourceOwner ResourceOwnerCreate(ResourceOwner, const char *)
{
    return reinterpret_cast<ResourceOwner>(0x1);
}

MemoryContext AllocSetContextCreateInternal(MemoryContext, const char *, Size, Size, Size)
{
    return reinterpret_cast<MemoryContext>(0x2);
}

} // extern "C"

TEST(PluginLoaderCoverageUT, HandlesNullAndMissingPluginDirectories)
{
    ResetPluginHarness();

    EXPECT_EQ(FalconPluginSystemInit(nullptr), -1);
    EXPECT_EQ(FalconPluginSystemInit("./missing_plugin_loader_coverage_dir"), -1);
    EXPECT_NE(shmem_startup_hook, nullptr);
}

TEST(PluginLoaderCoverageUT, InitializesShmemNodeInfoAndRunsPluginPhases)
{
    ResetPluginHarness();
    auto pluginDir = PrepareRuntimePluginDir();
    ASSERT_FALSE(pluginDir.empty());

    EXPECT_EQ(FalconPluginShmemSize(), sizeof(FalconPluginSharedMemory));
    EXPECT_EQ(FalconPluginSystemInit(pluginDir.c_str()), 0);
    EXPECT_EQ(g_registeredWorkers.size(), 1);
    ASSERT_NE(shmem_startup_hook, nullptr);

    FalconPluginShmemInit();
    shmem_startup_hook();
    FalconPluginInitBackgroundPlugins();

    FalconNodeInfo info;
    std::memset(&info, 0, sizeof(info));
    FalconNodeLocalIp = const_cast<char *>("10.2.3.4");
    FalconPluginGetNodeInfo(&info);
    EXPECT_STREQ(info.node_ip, "10.2.3.4");
    EXPECT_EQ(info.node_port, 5432);
    EXPECT_EQ(info.pooler_port, 15432);
    FalconPluginGetNodeInfo(nullptr);

    bool sawInline = false;
    bool sawBackground = false;
    bool sawInvalid = false;
    for (const auto &message : g_pluginLogs) {
        sawInline = sawInline || message.find("INLINE execution completed") != std::string::npos;
        sawBackground = sawBackground || message.find("Saved background plugin") != std::string::npos;
        sawInvalid = sawInvalid || message.find("missing required functions") != std::string::npos;
    }
    EXPECT_TRUE(sawInline);
    EXPECT_TRUE(sawBackground);
    EXPECT_TRUE(sawInvalid);

    std::filesystem::remove_all(pluginDir);
}
