#pragma once

#include "dfs.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

namespace local_run_test {

struct LocalRunParameters {
    std::string mount_dir = "/";
    int client_id = 0;
    int mount_per_client = 1;
    int wait_port = 1111;
    int client_num = 1;
};

inline std::string GetEnvOrDefault(const char *key, const char *fallback)
{
    const char *value = std::getenv(key);
    return value != nullptr ? std::string(value) : std::string(fallback);
}

inline int GetIntEnvOrDefault(const char *key, int fallback)
{
    const char *value = std::getenv(key);
    if (value == nullptr || *value == '\0') {
        return fallback;
    }
    return std::atoi(value);
}

inline bool IsEndpointReachable(const std::string &ip, int port)
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

inline bool WaitForEndpoint(const std::string &ip, int port, int retry_count = 6)
{
    for (int i = 0; i < retry_count; ++i) {
        if (IsEndpointReachable(ip, port)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return false;
}

inline bool EnsureConfiguredServer()
{
    std::string ip = GetEnvOrDefault("SERVER_IP", "127.0.0.1");
    int port = GetIntEnvOrDefault("SERVER_PORT", 55500);
    return WaitForEndpoint(ip, port);
}

inline void ResetCounters()
{
    std::memset((void *)op_count, 0, sizeof(op_count));
    std::memset((void *)latency_count, 0, sizeof(latency_count));
}

inline LocalRunParameters LoadLocalRunParameters()
{
    LocalRunParameters params;
    params.mount_dir = GetEnvOrDefault("LOCAL_RUN_MOUNT_DIR", "/");
    if (params.mount_dir.empty()) {
        params.mount_dir = "/";
    }
    if (params.mount_dir.back() != '/') {
        params.mount_dir.push_back('/');
    }

    files_per_dir = GetIntEnvOrDefault("LOCAL_RUN_FILE_PER_THREAD", 1);
    int thread_num_per_client = GetIntEnvOrDefault("LOCAL_RUN_THREAD_NUM_PER_CLIENT", 1);
    params.client_num = GetIntEnvOrDefault("LOCAL_RUN_CLIENT_NUM", 1);
    if (thread_num_per_client < 1) {
        thread_num_per_client = 1;
    }
    if (params.client_num < 1) {
        params.client_num = 1;
    }
    thread_num = thread_num_per_client * params.client_num;

    params.client_id = GetIntEnvOrDefault("LOCAL_RUN_CLIENT_ID", 0);
    params.mount_per_client = GetIntEnvOrDefault("LOCAL_RUN_MOUNT_PER_CLIENT", 1);
    params.wait_port = GetIntEnvOrDefault("LOCAL_RUN_WAIT_PORT", 1111);
    client_cache_size = GetIntEnvOrDefault("LOCAL_RUN_CLIENT_CACHE_SIZE", 16384);
    file_size = GetIntEnvOrDefault("LOCAL_RUN_FILE_SIZE", 4096);

    if (files_per_dir < 1) {
        files_per_dir = 1;
    }
    if (params.mount_per_client < 1) {
        params.mount_per_client = 1;
    }
    if (client_cache_size < 1) {
        client_cache_size = 1;
    }
    if (file_size < 1) {
        file_size = 4096;
    }

    file_num = thread_num * files_per_dir;
    return params;
}

}  // namespace local_run_test
