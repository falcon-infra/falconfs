#include <gtest/gtest.h>

#include <cstdarg>
#include <csetjmp>
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
int g_prevHookCount = 0;
jmp_buf g_procExitJump;
bool g_procExitJumpActive = false;
int g_procExitCode = -1;
int g_lastElevel = 0;
int g_falconLoadChecksBeforeReady = 0;
int g_recoveryChecksBeforeReady = 0;

void PreviousPluginShmemHook()
{
    g_prevHookCount++;
}

void ResetPluginHarness()
{
    g_pluginShmem.clear();
    g_pluginShmemFound = false;
    g_pluginLogs.clear();
    g_registeredWorkers.clear();
    g_prevHookCount = 0;
    g_falconLoadChecksBeforeReady = 0;
    g_recoveryChecksBeforeReady = 0;
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

    /* Build a mixed plugin directory so one loader pass covers success and failure paths. */
    std::filesystem::copy_file(builtDir / "libtest_plugin_inline.so",
                               runtimeDir / "libtest_inline.so",
                               std::filesystem::copy_options::overwrite_existing);
    std::filesystem::copy_file(builtDir / "libtest_plugin_background.so",
                               runtimeDir / "libtest_background.so",
                               std::filesystem::copy_options::overwrite_existing);
    /* Missing required plugin symbols. */
    std::filesystem::copy_file(builtDir / "libtest_plugin_invalid.so",
                               runtimeDir / "libtest_invalid.so",
                               std::filesystem::copy_options::overwrite_existing);
    /* Valid plugin ABI, but plugin_init returns non-zero. */
    std::filesystem::copy_file(builtDir / "libtest_plugin_inline_fail.so",
                               runtimeDir / "libtest_inline_fail.so",
                               std::filesystem::copy_options::overwrite_existing);
    std::filesystem::copy_file(builtDir / "libtest_plugin_background_fail.so",
                               runtimeDir / "libtest_background_fail.so",
                               std::filesystem::copy_options::overwrite_existing);
    /* Non-plugin and broken shared object entries verify directory filtering and dlopen failure. */
    std::ofstream(runtimeDir / "readme.txt") << "not a plugin\n";
    std::ofstream(runtimeDir / "broken.so") << "not a shared object\n";
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

bool errstart(int elevel, const char *)
{
    g_lastElevel = elevel;
    return true;
}

bool errstart_cold(int elevel, const char *domain)
{
    return errstart(elevel, domain);
}

void errfinish(const char *, int, const char *)
{
    if (g_procExitJumpActive && g_lastElevel >= ERROR) {
        g_procExitCode = 1;
        longjmp(g_procExitJump, 1);
    }
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
    if (g_falconLoadChecksBeforeReady > 0) {
        g_falconLoadChecksBeforeReady--;
        return false;
    }
    return true;
}

bool RecoveryInProgress(void)
{
    if (g_recoveryChecksBeforeReady > 0) {
        g_recoveryChecksBeforeReady--;
        return true;
    }
    return false;
}

void BackgroundWorkerUnblockSignals(void) {}
void BackgroundWorkerInitializeConnection(const char *, const char *, uint32) {}
void StartTransactionCommand(void) {}
void CommitTransactionCommand(void) {}
void proc_exit(int code)
{
    if (g_procExitJumpActive) {
        g_procExitCode = code;
        longjmp(g_procExitJump, 1);
    }
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
    /* Initialize the loader without a usable plugin directory and verify the failure path still installs the hook. */
    ResetPluginHarness();

    /* Cover plugin loader initialization failures before any plugin file is scanned. */
    /* Invalid plugin directories fail but still install the startup hook used by plugin loading. */
    EXPECT_EQ(FalconPluginSystemInit(nullptr), -1);
    EXPECT_EQ(FalconPluginSystemInit("./missing_plugin_loader_coverage_dir"), -1);
    EXPECT_NE(shmem_startup_hook, nullptr);

    FalconPluginShmemInit();
    shmem_startup_hook();
}

TEST(PluginLoaderCoverageUT, InitializesShmemNodeInfoAndRunsPluginPhases)
{
    /* Drive shared-memory setup, inline work, background registration, and init-failure handling in one plugin directory. */
    ResetPluginHarness();
    auto pluginDir = PrepareRuntimePluginDir();
    ASSERT_FALSE(pluginDir.empty());

    /* Cover the normal plugin lifecycle plus invalid, broken, and init-failing plugin entries. */
    EXPECT_EQ(FalconPluginShmemSize(), sizeof(FalconPluginSharedMemory));
    /* Preserve and call any previously registered shared-memory startup hook. */
    shmem_startup_hook = PreviousPluginShmemHook;
    EXPECT_EQ(FalconPluginSystemInit(pluginDir.c_str()), 0);
    /* Both the normal and init-failing background plugins are registered before init runs. */
    EXPECT_EQ(g_registeredWorkers.size(), 2);
    ASSERT_NE(shmem_startup_hook, nullptr);

    /* First hook run happens before explicit shmem init and exercises lazy initialization. */
    shmem_startup_hook();
    EXPECT_EQ(g_prevHookCount, 1);

    FalconPluginShmemInit();
    /* Second hook run executes inline plugins against initialized shared memory. */
    shmem_startup_hook();
    EXPECT_EQ(g_prevHookCount, 2);
    /* Background plugin init is delayed until shared memory is ready. */
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
    bool sawBroken = false;
    bool sawInlineInitFailure = false;
    bool sawBackgroundInitFailure = false;
    for (const auto &message : g_pluginLogs) {
        /* Match log text rather than exact return codes; loader behavior only depends on non-zero. */
        sawInline = sawInline || message.find("INLINE execution completed") != std::string::npos;
        sawBackground = sawBackground || message.find("Saved background plugin") != std::string::npos;
        sawInvalid = sawInvalid || message.find("missing required functions") != std::string::npos;
        sawBroken = sawBroken || message.find("Failed to load plugin") != std::string::npos;
        sawInlineInitFailure = sawInlineInitFailure || message.find("initialization failed") != std::string::npos;
        sawBackgroundInitFailure = sawBackgroundInitFailure || message.find("init_func failed") != std::string::npos;
    }
    EXPECT_TRUE(sawInline);
    EXPECT_TRUE(sawBackground);
    EXPECT_TRUE(sawInvalid);
    EXPECT_TRUE(sawBroken);
    EXPECT_TRUE(sawInlineInitFailure);
    EXPECT_TRUE(sawBackgroundInitFailure);

    /* Exhaust slots to cover the inline path that cannot allocate plugin shared memory. */
    auto *shared = reinterpret_cast<FalconPluginSharedMemory *>(g_pluginShmem.data());
    for (int i = 0; i < shared->num_slots; ++i) {
        shared->plugins[i].in_use = true;
    }
    shmem_startup_hook();

    std::filesystem::remove_all(pluginDir);
}

TEST(PluginLoaderCoverageUT, BackgroundWorkerExitsWhenSlotIsMissing)
{
    /* Start a mocked background worker without a saved slot and verify it exits at slot lookup. */
    ResetPluginHarness();

    /* Cover the worker exit path when the plugin was never saved into shared memory. */
    /* Simulate PostgreSQL passing a plugin path through BackgroundWorker.bgw_extra. */
    BackgroundWorker worker;
    std::memset(&worker, 0, sizeof(worker));
    std::strncpy(worker.bgw_extra, "missing_background_plugin.so", BGW_EXTRALEN - 1);
    MyBgworkerEntry = &worker;
    FalconPluginShmemInit();

    /* Convert the expected proc_exit(1) into a test assertion instead of aborting. */
    g_procExitCode = -1;
    g_procExitJumpActive = true;
    if (setjmp(g_procExitJump) == 0) {
        FalconPluginBackgroundWorkerMain(static_cast<Datum>(0));
        ADD_FAILURE() << "FalconPluginBackgroundWorkerMain should call proc_exit";
    } else {
        EXPECT_EQ(g_procExitCode, 1);
    }
    g_procExitJumpActive = false;
    MyBgworkerEntry = nullptr;

    bool sawWorkerStart = false;
    bool sawMissingSlot = false;
    for (const auto &message : g_pluginLogs) {
        /* The worker should get past database readiness and fail at shared-memory slot lookup. */
        sawWorkerStart = sawWorkerStart || message.find("Background worker started") != std::string::npos;
        sawMissingSlot = sawMissingSlot || message.find("Cannot find shared memory slot") != std::string::npos;
    }
    EXPECT_TRUE(sawWorkerStart);
    EXPECT_TRUE(sawMissingSlot);
}

TEST(PluginLoaderCoverageUT, BackgroundWorkerExitsWhenPluginCannotLoad)
{
    /* Seed a slot for a missing shared object so the worker reaches the dlopen failure path. */
    ResetPluginHarness();

    /* Cover the worker exit path after slot lookup succeeds but dlopen fails. */
    const char *pluginPath = "/tmp/missing_background_plugin_loader_coverage.so";
    BackgroundWorker worker;
    std::memset(&worker, 0, sizeof(worker));
    std::strncpy(worker.bgw_extra, pluginPath, BGW_EXTRALEN - 1);
    MyBgworkerEntry = &worker;
    FalconPluginShmemInit();

    /* Pre-create the matching shared-memory slot so the worker reaches dlopen failure handling. */
    auto *shared = reinterpret_cast<FalconPluginSharedMemory *>(g_pluginShmem.data());
    ASSERT_GT(shared->num_slots, 0);
    shared->plugins[0].in_use = true;
    std::strncpy(shared->plugins[0].plugin_path, pluginPath, FALCON_PLUGIN_MAX_PATH_SIZE - 1);
    std::strncpy(shared->plugins[0].plugin_name, "missing_background_plugin_loader_coverage.so",
                 FALCON_PLUGIN_MAX_NAME_SIZE - 1);

    g_procExitCode = -1;
    g_procExitJumpActive = true;
    if (setjmp(g_procExitJump) == 0) {
        FalconPluginBackgroundWorkerMain(static_cast<Datum>(0));
        ADD_FAILURE() << "FalconPluginBackgroundWorkerMain should call proc_exit";
    } else {
        EXPECT_EQ(g_procExitCode, 1);
    }
    g_procExitJumpActive = false;
    MyBgworkerEntry = nullptr;

    bool sawSlot = false;
    bool sawLoadFailure = false;
    for (const auto &message : g_pluginLogs) {
        /* The missing file should fail only after the slot has been found. */
        sawSlot = sawSlot || message.find("Found slot") != std::string::npos;
        sawLoadFailure = sawLoadFailure || message.find("Failed to load plugin in background worker") != std::string::npos;
    }
    EXPECT_TRUE(sawSlot);
    EXPECT_TRUE(sawLoadFailure);
}

TEST(PluginLoaderCoverageUT, BackgroundWorkerExitsWhenPluginMissingWorkFunctions)
{
    /* Load an invalid plugin object and verify missing work/cleanup symbols terminate the worker. */
    ResetPluginHarness();
    auto builtDir = FindBuiltPluginDir();
    ASSERT_FALSE(builtDir.empty());

    /* Cover the worker exit path for a loadable plugin that fails required symbol lookup. */
    auto pluginPath = std::filesystem::current_path() / "libtest_invalid_background_worker.so";
    /* Reuse the invalid plugin to exercise the background worker's dlsym validation path. */
    std::filesystem::copy_file(builtDir / "libtest_plugin_invalid.so",
                               pluginPath,
                               std::filesystem::copy_options::overwrite_existing);

    BackgroundWorker worker;
    std::memset(&worker, 0, sizeof(worker));
    std::strncpy(worker.bgw_extra, pluginPath.c_str(), BGW_EXTRALEN - 1);
    MyBgworkerEntry = &worker;
    FalconPluginShmemInit();

    auto *shared = reinterpret_cast<FalconPluginSharedMemory *>(g_pluginShmem.data());
    ASSERT_GT(shared->num_slots, 0);
    shared->plugins[0].in_use = true;
    std::strncpy(shared->plugins[0].plugin_path, pluginPath.c_str(), FALCON_PLUGIN_MAX_PATH_SIZE - 1);
    std::strncpy(shared->plugins[0].plugin_name, "libtest_invalid_background_worker.so",
                 FALCON_PLUGIN_MAX_NAME_SIZE - 1);

    /* Force one wait loop iteration for both Falcon load and recovery readiness. */
    g_falconLoadChecksBeforeReady = 1;
    g_recoveryChecksBeforeReady = 1;
    g_procExitCode = -1;
    g_procExitJumpActive = true;
    if (setjmp(g_procExitJump) == 0) {
        FalconPluginBackgroundWorkerMain(static_cast<Datum>(0));
        ADD_FAILURE() << "FalconPluginBackgroundWorkerMain should call proc_exit";
    } else {
        EXPECT_EQ(g_procExitCode, 1);
    }
    g_procExitJumpActive = false;
    MyBgworkerEntry = nullptr;

    bool sawSlot = false;
    bool sawMissingFunctions = false;
    for (const auto &message : g_pluginLogs) {
        /* The invalid shared object loads, but lacks the work/cleanup entry points. */
        sawSlot = sawSlot || message.find("Found slot") != std::string::npos;
        sawMissingFunctions = sawMissingFunctions ||
            message.find("missing required functions (work/cleanup)") != std::string::npos;
    }
    EXPECT_TRUE(sawSlot);
    EXPECT_TRUE(sawMissingFunctions);

    std::filesystem::remove(pluginPath);
}
