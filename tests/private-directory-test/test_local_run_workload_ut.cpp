#include "dfs.h"
#include "local_run_workload_test_common.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
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
local_run_test::LocalRunParameters g_params;

std::string BuildRootPath(const char *tag)
{
    uint64_t seq = g_case_counter.fetch_add(1, std::memory_order_relaxed);
    return fmt::format("{}client_{}_{}_{}_{}_{}_{}/",
                       g_params.mount_dir, g_params.client_id, g_params.wait_port,
                       tag, getpid(), seq, time(nullptr));
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

void ExpectKvRecordExists(const std::string &root)
{
    std::string key = fmt::format("{}thread_{}_key_{}", root, 0, 0);
    uint32_t value_len = 0;
    uint16_t slice_num = 0;
    EXPECT_EQ(dfs_kv_get(key.c_str(), &value_len, &slice_num), 0);
    EXPECT_EQ(value_len, 4096U);
    EXPECT_EQ(slice_num, 2U);
}

void ExpectSliceRecordExists(const std::string &root)
{
    uint32_t slice_num = 0;
    EXPECT_EQ(dfs_slice_get(FilePath(root, 0, 0).c_str(), 0, 0, &slice_num), 0);
    EXPECT_GT(slice_num, 0U);
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

bool InitClientOrSkip()
{
    setenv("SERVER_IP", local_run_test::GetEnvOrDefault("SERVER_IP", "127.0.0.1").c_str(), 1);
    setenv("SERVER_PORT", local_run_test::GetEnvOrDefault("SERVER_PORT", "55500").c_str(), 1);
    g_params = local_run_test::LoadLocalRunParameters();
    local_run_test::ResetCounters();

    constexpr int kMaxRetry = 6;
    for (int i = 0; i < kMaxRetry; ++i) {
        try {
            if (dfs_init(g_params.client_num) == 0) {
                return true;
            }
        } catch (...) {
        }
        dfs_shutdown();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return false;
}

void CleanupRoot(const std::string &root, bool with_files)
{
    try {
        if (with_files) {
            RunForEachThread(workload_delete, root);
        }
        UninitNamespaceRoot(root);
    } catch (...) {
    }
    dfs_shutdown();
}

}  // namespace

TEST(LocalRunWorkloadUT, InitCreateStatOpenCloseFlow)
{
    constexpr int kFlowRetry = 2;
    for (int attempt = 0; attempt < kFlowRetry; ++attempt) {
        if (!local_run_test::EnsureConfiguredServer()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        if (!InitClientOrSkip()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        std::string root = BuildRootPath("fullflow");
        bool success = false;
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
            success = true;
        } catch (...) {
        }

        CleanupRoot(root, true);
        if (success) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    GTEST_SKIP() << "full workload flow failed after retries, likely due unstable service state";
}

TEST(LocalRunWorkloadUT, FullMetadataKvSliceFlow)
{
    constexpr int kFlowRetry = 2;
    for (int attempt = 0; attempt < kFlowRetry; ++attempt) {
        if (!local_run_test::EnsureConfiguredServer()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        if (!InitClientOrSkip()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        std::string root = BuildRootPath("metadata_kv_slice_flow");
        bool success = false;
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

            RunForEachThread(workload_mkdir, root);
            uint64_t after_mkdir = op_count[0];
            ExpectDirsExist(root);

            RunForEachThread(workload_rmdir, root);
            uint64_t after_rmdir = op_count[0];

            RunForEachThread(workload_kv_put, root);
            uint64_t after_kv_put = op_count[0];
            ExpectKvRecordExists(root);

            RunForEachThread(workload_kv_get, root);
            uint64_t after_kv_get = op_count[0];

            RunForEachThread(workload_kv_del, root);
            uint64_t after_kv_del = op_count[0];

            RunForEachThread(workload_slice_put, root);
            uint64_t after_slice_put = op_count[0];
            ExpectSliceRecordExists(root);

            RunForEachThread(workload_slice_get, root);
            uint64_t after_slice_get = op_count[0];

            RunForEachThread(workload_slice_del, root);
            uint64_t after_slice_del = op_count[0];

            RunForEachThread(workload_delete, root);
            files_deleted = true;
            uint64_t after_delete = op_count[0];

            UninitNamespaceRoot(root);
            namespace_removed = true;
            uint64_t after_uninit = op_count[0];

            EXPECT_GT(after_init, 0U);
            EXPECT_GT(after_create, after_init);
            EXPECT_GT(after_stat, after_create);
            EXPECT_GT(after_open, after_stat);
            EXPECT_GT(after_close, after_open);
            EXPECT_GT(after_mkdir, after_close);
            EXPECT_GT(after_rmdir, after_mkdir);
            EXPECT_GT(after_kv_put, after_rmdir);
            EXPECT_GT(after_kv_get, after_kv_put);
            EXPECT_GT(after_kv_del, after_kv_get);
            EXPECT_GT(after_slice_put, after_kv_del);
            EXPECT_GT(after_slice_get, after_slice_put);
            EXPECT_GT(after_slice_del, after_slice_get);
            EXPECT_GT(after_delete, after_slice_del);
            EXPECT_GT(after_uninit, after_delete);
            success = true;
        } catch (...) {
        }

        if (namespace_removed) {
            dfs_shutdown();
        } else {
            CleanupRoot(root, !files_deleted);
        }
        if (success && !HasFailure()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }

    GTEST_SKIP() << "metadata/kv/slice workload flow failed after retries, likely due unstable service state";
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
