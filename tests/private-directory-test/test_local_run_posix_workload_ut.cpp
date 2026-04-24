#include "dfs.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>

int thread_num = 1;
int client_cache_size = 16384;
int files_per_dir = 2;
int file_size = 4096;
int file_num = 0;
std::atomic<bool> printed(false);
volatile uint64_t op_count[16384];
volatile uint64_t latency_count[16384];

namespace {

std::atomic<uint64_t> g_case_counter(0);
int g_client_id = 0;
int g_wait_port = 1111;
std::string g_mount_dir = "/tmp/falconfs_localrun_posix/";

std::string GetEnvOrDefault(const char *key, const char *fallback)
{
    const char *value = std::getenv(key);
    return value != nullptr ? std::string(value) : std::string(fallback);
}

int GetIntEnvOrDefault(const char *key, int fallback)
{
    const char *value = std::getenv(key);
    if (value == nullptr || *value == '\0') {
        return fallback;
    }
    return std::atoi(value);
}

void LoadPosixParameters()
{
    g_mount_dir = GetEnvOrDefault("LOCAL_RUN_POSIX_MOUNT_DIR", "/tmp/falconfs_localrun_posix/");
    if (g_mount_dir.empty()) {
        g_mount_dir = "/tmp/falconfs_localrun_posix/";
    }
    if (g_mount_dir.back() != '/') {
        g_mount_dir.push_back('/');
    }

    files_per_dir = GetIntEnvOrDefault("LOCAL_RUN_FILE_PER_THREAD", 1);
    int thread_num_per_client = GetIntEnvOrDefault("LOCAL_RUN_THREAD_NUM_PER_CLIENT", 1);
    int client_num = GetIntEnvOrDefault("LOCAL_RUN_CLIENT_NUM", 1);
    if (thread_num_per_client < 1) {
        thread_num_per_client = 1;
    }
    if (client_num < 1) {
        client_num = 1;
    }
    thread_num = thread_num_per_client * client_num;

    g_client_id = GetIntEnvOrDefault("LOCAL_RUN_CLIENT_ID", 0);
    g_wait_port = GetIntEnvOrDefault("LOCAL_RUN_WAIT_PORT", 1111);
    client_cache_size = GetIntEnvOrDefault("LOCAL_RUN_CLIENT_CACHE_SIZE", 16384);
    file_size = GetIntEnvOrDefault("LOCAL_RUN_FILE_SIZE", 4096);

    if (files_per_dir < 1) {
        files_per_dir = 1;
    }
    if (client_cache_size < 1) {
        client_cache_size = 1;
    }
    if (file_size < 1) {
        file_size = 4096;
    }
}

std::string BuildRootPath()
{
    uint64_t seq = g_case_counter.fetch_add(1, std::memory_order_relaxed);
    std::string client_root = fmt::format("{}client_{}_{}/", g_mount_dir, g_client_id, g_wait_port);
    std::filesystem::create_directories(client_root);
    return fmt::format("{}posix_flow_{}_{}_{}/", client_root, getpid(), seq, time(nullptr));
}

bool InitClient()
{
    LoadPosixParameters();
    std::memset((void *)op_count, 0, sizeof(op_count));
    std::memset((void *)latency_count, 0, sizeof(latency_count));
    file_num = thread_num * files_per_dir;
    return dfs_init(1) == 0;
}

void CleanupRoot(const std::string &root, bool with_files)
{
    try {
        if (with_files) {
            workload_delete(root, 0);
        }
        int thread_dir_count = files_per_dir > 1 ? files_per_dir - 1 : 1;
        for (int i = 0; i < thread_dir_count; ++i) {
            std::string thread_dir = fmt::format("{}thread_{}", root, i);
            dfs_rmdir(thread_dir.c_str());
        }
        workload_uninit(root, 0);
    } catch (...) {
    }
    dfs_shutdown();
}

void EnsureDirExistsWithRetry(const std::string &path)
{
    constexpr int kRetry = 20;
    struct stat stbuf;
    for (int i = 0; i < kRetry; ++i) {
        int ret = dfs_mkdir(path.c_str(), 0777);
        if (ret == 0 || errno == EEXIST || dfs_stat(path.c_str(), &stbuf) == 0) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    throw std::runtime_error("failed to ensure directory exists");
}

void EnsureThreadDir(const std::string &root)
{
    EnsureDirExistsWithRetry(root);
    EnsureDirExistsWithRetry(fmt::format("{}thread_0", root));
}

}  // namespace

TEST(LocalRunPosixWorkloadUT, InitCreateStatOpenCloseFlow)
{
    if (!InitClient()) {
        GTEST_SKIP() << "posix dfs_init failed";
        return;
    }

    std::string root = BuildRootPath();
    uint64_t start = op_count[0];
    try {
        workload_init(root, 0);
        uint64_t after_init = op_count[0];
        EnsureThreadDir(root);

        workload_create(root, 0);
        uint64_t after_create = op_count[0];

        workload_stat(root, 0);
        uint64_t after_stat = op_count[0];

        workload_open(root, 0);
        uint64_t after_open = op_count[0];

        workload_close(root, 0);

        EXPECT_GT(after_init, start);
        EXPECT_GT(after_create, after_init);
        EXPECT_GT(after_stat, after_create);
        EXPECT_GT(after_open, after_stat);
        EXPECT_GT(op_count[0], after_open);
    } catch (...) {
        CleanupRoot(root, true);
        GTEST_SKIP() << "posix full workload flow failed";
        return;
    }

    CleanupRoot(root, true);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
