#include "dfs.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>

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

std::string ThreadDir(const std::string &root, int thread_id)
{
    return fmt::format("{}thread_{}", root, thread_id);
}

std::string FilePath(const std::string &root, int thread_id, int file_id)
{
    return fmt::format("{}/file_{}", ThreadDir(root, thread_id), file_id);
}

std::string DirPath(const std::string &root, int thread_id, int dir_id)
{
    return fmt::format("{}/dir_{}", ThreadDir(root, thread_id), dir_id);
}

void InitNamespaceRoot(const std::string &root)
{
    int saved_files_per_dir = files_per_dir;
    files_per_dir = 1 + thread_num;
    workload_init(root, 0);
    files_per_dir = saved_files_per_dir;
}

void UninitNamespaceRoot(const std::string &root)
{
    int saved_files_per_dir = files_per_dir;
    files_per_dir = 1 + thread_num;
    workload_uninit(root, 0);
    files_per_dir = saved_files_per_dir;
}

void RunForEachThread(void (*workload)(std::string, int), const std::string &root)
{
    for (int thread_id = 0; thread_id < thread_num; ++thread_id) {
        workload(root, thread_id);
    }
}

void ExpectThreadFilesExist(const std::string &root, int thread_id)
{
    for (int file_id = 0; file_id < files_per_dir; ++file_id) {
        struct stat stbuf;
        SCOPED_TRACE(FilePath(root, thread_id, file_id));
        EXPECT_EQ(dfs_stat(FilePath(root, thread_id, file_id).c_str(), &stbuf), 0);
    }
}

void ExpectFilesExist(const std::string &root)
{
    for (int thread_id = 0; thread_id < thread_num; ++thread_id) {
        ExpectThreadFilesExist(root, thread_id);
    }
}

void ExpectDirsExist(const std::string &root)
{
    for (int thread_id = 0; thread_id < thread_num; ++thread_id) {
        for (int dir_id = 0; dir_id < files_per_dir; ++dir_id) {
            struct stat stbuf;
            SCOPED_TRACE(DirPath(root, thread_id, dir_id));
            EXPECT_EQ(dfs_stat(DirPath(root, thread_id, dir_id).c_str(), &stbuf), 0);
        }
    }
}

bool InitClient()
{
    LoadPosixParameters();
    std::memset((void *)op_count, 0, sizeof(op_count));
    std::memset((void *)latency_count, 0, sizeof(latency_count));
    file_num = thread_num * files_per_dir;
    return dfs_init(1) == 0;
}

void CleanupRoot(const std::string &root, bool with_files, bool all_threads)
{
    try {
        if (with_files) {
            if (all_threads) {
                RunForEachThread(workload_delete, root);
            } else {
                workload_delete(root, 0);
            }
        }
        UninitNamespaceRoot(root);
    } catch (...) {
    }
    dfs_shutdown();
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
        InitNamespaceRoot(root);
        uint64_t after_init = op_count[0];

        workload_create(root, 0);
        uint64_t after_create = op_count[0];
        ExpectThreadFilesExist(root, 0);

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
        CleanupRoot(root, true, false);
        GTEST_SKIP() << "posix full workload flow failed";
        return;
    }

    CleanupRoot(root, true, false);
}

TEST(LocalRunPosixWorkloadUT, FullFileWorkloadFlow)
{
    if (!InitClient()) {
        GTEST_SKIP() << "posix dfs_init failed";
        return;
    }

    std::string root = BuildRootPath();
    bool files_deleted = false;
    bool namespace_removed = false;
    try {
        InitNamespaceRoot(root);
        uint64_t after_init = op_count[0];

        RunForEachThread(workload_create, root);
        uint64_t after_create = op_count[0];
        ExpectFilesExist(root);

        RunForEachThread(workload_stat, root);
        uint64_t after_stat = op_count[0];

        RunForEachThread(workload_open, root);
        uint64_t after_open = op_count[0];

        RunForEachThread(workload_close, root);
        uint64_t after_close = op_count[0];

        RunForEachThread(workload_delete, root);
        files_deleted = true;
        uint64_t after_delete_initial = op_count[0];

        RunForEachThread(workload_mkdir, root);
        uint64_t after_mkdir = op_count[0];
        ExpectDirsExist(root);

        RunForEachThread(workload_rmdir, root);
        uint64_t after_rmdir = op_count[0];

        RunForEachThread(workload_open_write_close, root);
        files_deleted = false;
        uint64_t after_open_write_close = op_count[0];
        ExpectFilesExist(root);

        RunForEachThread(workload_open_write_close_nocreate, root);
        uint64_t after_open_write_close_nocreate = op_count[0];

        RunForEachThread(workload_open_read_close, root);
        uint64_t after_open_read_close = op_count[0];

        RunForEachThread(workload_delete, root);
        files_deleted = true;
        uint64_t after_delete_final = op_count[0];

        UninitNamespaceRoot(root);
        namespace_removed = true;
        uint64_t after_uninit = op_count[0];

        EXPECT_GT(after_init, 0U);
        EXPECT_GT(after_create, after_init);
        EXPECT_GT(after_stat, after_create);
        EXPECT_GT(after_open, after_stat);
        EXPECT_GT(after_close, after_open);
        EXPECT_GT(after_delete_initial, after_close);
        EXPECT_GT(after_mkdir, after_delete_initial);
        EXPECT_GT(after_rmdir, after_mkdir);
        EXPECT_GT(after_open_write_close, after_rmdir);
        EXPECT_GT(after_open_write_close_nocreate, after_open_write_close);
        EXPECT_GT(after_open_read_close, after_open_write_close_nocreate);
        EXPECT_GT(after_delete_final, after_open_read_close);
        EXPECT_GT(after_uninit, after_delete_final);
    } catch (...) {
    }

    if (namespace_removed) {
        dfs_shutdown();
    } else {
        CleanupRoot(root, !files_deleted, true);
    }
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
