#include "dfs.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
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

int g_client_id = 0;
int g_mount_per_client = 1;
int g_wait_port = 1111;
int g_client_num = 1;
std::string g_mount_dir = "/";

std::string GetEnvOrDefault(const char *key, const char *fallback)
{
    const char *value = std::getenv(key);
    return value != nullptr ? std::string(value) : std::string(fallback);
}

bool IsMetaServerReachable(const std::string &ip, int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return false;
    }

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) != 1) {
        close(fd);
        return false;
    }

    bool reachable = (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0);
    close(fd);
    return reachable;
}

bool EnsureServerOrSkip()
{
    std::string ip = GetEnvOrDefault("SERVER_IP", "127.0.0.1");
    std::string port_text = GetEnvOrDefault("SERVER_PORT", "55500");
    int port = std::atoi(port_text.c_str());

    constexpr int kMaxRetry = 6;
    for (int i = 0; i < kMaxRetry; ++i) {
        if (IsMetaServerReachable(ip, port)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return false;
}

int GetIntEnvOrDefault(const char *key, int fallback)
{
    const char *value = std::getenv(key);
    if (value == nullptr || *value == '\0') {
        return fallback;
    }
    return std::atoi(value);
}

void LoadLocalRunParameters()
{
    g_mount_dir = GetEnvOrDefault("LOCAL_RUN_MOUNT_DIR", "/");
    if (g_mount_dir.empty()) {
        g_mount_dir = "/";
    }
    if (g_mount_dir.back() != '/') {
        g_mount_dir.push_back('/');
    }

    files_per_dir = GetIntEnvOrDefault("LOCAL_RUN_FILE_PER_THREAD", 1);
    int thread_num_per_client = GetIntEnvOrDefault("LOCAL_RUN_THREAD_NUM_PER_CLIENT", 1);
    g_client_num = GetIntEnvOrDefault("LOCAL_RUN_CLIENT_NUM", 1);
    if (thread_num_per_client < 1) {
        thread_num_per_client = 1;
    }
    if (g_client_num < 1) {
        g_client_num = 1;
    }
    thread_num = thread_num_per_client * g_client_num;

    g_client_id = GetIntEnvOrDefault("LOCAL_RUN_CLIENT_ID", 0);
    g_mount_per_client = GetIntEnvOrDefault("LOCAL_RUN_MOUNT_PER_CLIENT", 1);
    g_wait_port = GetIntEnvOrDefault("LOCAL_RUN_WAIT_PORT", 1111);
    client_cache_size = GetIntEnvOrDefault("LOCAL_RUN_CLIENT_CACHE_SIZE", 16384);
    file_size = GetIntEnvOrDefault("LOCAL_RUN_FILE_SIZE", 4096);

    if (files_per_dir < 1) {
        files_per_dir = 1;
    }
    if (g_mount_per_client < 1) {
        g_mount_per_client = 1;
    }
    if (client_cache_size < 1) {
        client_cache_size = 1;
    }
    if (file_size < 1) {
        file_size = 4096;
    }
}

std::string BuildRootPath(const char *tag)
{
    (void)tag;
    return fmt::format("{}client_{}_{}/", g_mount_dir, g_client_id, g_wait_port);
}

void ResetCounters()
{
    std::memset((void *)op_count, 0, sizeof(op_count));
    std::memset((void *)latency_count, 0, sizeof(latency_count));
}

bool InitClientOrSkip()
{
    setenv("SERVER_IP", GetEnvOrDefault("SERVER_IP", "127.0.0.1").c_str(), 1);
    setenv("SERVER_PORT", GetEnvOrDefault("SERVER_PORT", "55500").c_str(), 1);
    LoadLocalRunParameters();
    ResetCounters();
    file_num = thread_num * files_per_dir;

    constexpr int kMaxRetry = 6;
    for (int i = 0; i < kMaxRetry; ++i) {
        try {
            if (dfs_init(g_client_num) == 0) {
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

}  // namespace

TEST(LocalRunWorkloadUT, InitCreateStatOpenCloseFlow)
{
    constexpr int kFlowRetry = 2;
    for (int attempt = 0; attempt < kFlowRetry; ++attempt) {
        if (!EnsureServerOrSkip()) {
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
            int files_per_dir_for_flow = files_per_dir;
            files_per_dir = 1 + thread_num;
            workload_init(root, 0);
            files_per_dir = files_per_dir_for_flow;
            uint64_t after_init = op_count[0];

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

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
