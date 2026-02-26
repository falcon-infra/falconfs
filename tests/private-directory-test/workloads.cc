#include "dfs.h"

#include "log/logging.h"

using namespace std;

void workload_init(string root_dir, int thread_id)
{
    // cout << "workload_init";
    int ret = dfs_mkdir(root_dir.c_str(), 0777);
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Failed to mkdir: " << root_dir << ", ret = " << ret << ", errno = " << errno;
        // assert (0);
    }
    op_count[thread_id]++;
    for (int i = 0; i < files_per_dir - 1; i++) {
        string new_path = fmt::format("{}thread_{}", root_dir, i);
        int ret = dfs_mkdir(new_path.c_str(), 0777);
        if (ret != 0) {
            FALCON_LOG(LOG_ERROR) << "Failed to mkdir: " << new_path << ", ret = " << ret << ", errno = " << errno;
            // assert (0);
        }
        op_count[thread_id]++;
    }
}

void workload_create(string root_dir, int thread_id)
{
    struct timespec start_time, end_time;
    string thread_dir = fmt::format("{}thread_{}/", root_dir, thread_id);
    for (int i = 0; i < files_per_dir; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        string new_path = fmt::format("{}file_{}", thread_dir, i);
        int ret = dfs_create(new_path.c_str(), S_IFREG | 0777);
        if (ret != 0) {
            FALCON_LOG(LOG_ERROR) << "Failed to create: " << new_path << ", ret = " << ret << ", errno = " << errno;
            // assert (0);
        }
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        op_count[thread_id]++;
        uint64_t elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
        latency_count[thread_id] += elapsed_time;
    }
}

void workload_stat(string root_dir, int thread_id)
{
    struct timespec start_time, end_time;
    struct stat stbuf;
    string thread_dir = fmt::format("{}thread_{}/", root_dir, thread_id);
    for (int i = 0; i < files_per_dir; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        string new_path = fmt::format("{}file_{}", thread_dir, i);
        int ret = dfs_stat(new_path.c_str(), &stbuf);
        if (ret != 0) {
            FALCON_LOG(LOG_ERROR) << "Failed to stat: " << new_path << ", ret = " << ret << ", errno = " << errno;
            // assert (0);
        }
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        op_count[thread_id]++;
        uint64_t elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
        latency_count[thread_id] += elapsed_time;
    }
}

void workload_open(string root_dir, int thread_id)
{
    struct timespec start_time, end_time;
    string thread_dir = fmt::format("{}thread_{}/", root_dir, thread_id);
    for (int i = 0; i < files_per_dir; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        string new_path = fmt::format("{}file_{}", thread_dir, i);
        int fd = dfs_open(new_path.c_str(), O_RDONLY, 0666);
        if (fd < 0) {
            FALCON_LOG(LOG_ERROR) << "Failed to open file: " << new_path << ", fd = " << fd << ", errno = " << errno;
            assert (0);
        }
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        op_count[thread_id]++;
        uint64_t elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
        latency_count[thread_id] += elapsed_time;
    }
}

void workload_close(string root_dir, int thread_id)
{
    struct timespec start_time, end_time;
    string thread_dir = fmt::format("{}thread_{}/", root_dir, thread_id);
    for (int i = 0; i < files_per_dir; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        string new_path = fmt::format("{}file_{}", thread_dir, i);
        dfs_close(-1, new_path.c_str());
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        op_count[thread_id]++;
        uint64_t elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
        latency_count[thread_id] += elapsed_time;
    }
}

void workload_delete(string root_dir, int thread_id)
{
    struct timespec start_time, end_time;
    string thread_dir = fmt::format("{}thread_{}/", root_dir, thread_id);
    for (int i = 0; i < files_per_dir; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        string new_path = fmt::format("{}file_{}", thread_dir, i);
        int ret = dfs_unlink(new_path.c_str());
        if (ret != 0) {
            FALCON_LOG(LOG_ERROR) << "Failed to delete: " << new_path << ", ret = " << ret << ", errno = " << errno;
            // assert (0);
        }
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        op_count[thread_id]++;
        uint64_t elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
        latency_count[thread_id] += elapsed_time;
    }
}


void workload_mkdir(string root_dir, int thread_id)
{
    struct timespec start_time, end_time;
    string thread_dir = fmt::format("{}thread_{}/", root_dir, thread_id);
    for (int i = 0; i < files_per_dir; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        string new_path = fmt::format("{}dir_{}", thread_dir, i);
        int ret = dfs_mkdir(new_path.c_str(), 0777);
        if (ret != 0) {
            FALCON_LOG(LOG_ERROR) << "Failed to mkdir: " << new_path << ", ret = " << ret << ", errno = " << errno;
            // assert (0);
        }
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        op_count[thread_id]++;
        uint64_t elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
        latency_count[thread_id] += elapsed_time;
    }
}

void workload_rmdir(string root_dir, int thread_id)
{
    struct timespec start_time, end_time;
    string thread_dir = fmt::format("{}thread_{}/", root_dir, thread_id);
    for (int i = 0; i < files_per_dir; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        string new_path = fmt::format("{}dir_{}", thread_dir, i);
        int ret = dfs_rmdir(new_path.c_str());
        if (ret != 0) {
            FALCON_LOG(LOG_ERROR) << "Failed to rmdir: " << new_path << ", ret = " << ret << ", errno = " << errno;
            // assert (0);
        }
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        op_count[thread_id]++;
        uint64_t elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
        latency_count[thread_id] += elapsed_time;
    }
}

void workload_uninit(string root_dir, int thread_id)
{
    for (int i = 0; i < files_per_dir - 1; i++) {
        string new_path = fmt::format("{}thread_{}", root_dir, i);
        int ret = dfs_rmdir(new_path.c_str());
        if (ret != 0) {
            FALCON_LOG(LOG_ERROR) << "Failed to rmdir: " << new_path << ", ret = " << ret << ", errno = " << errno;
            // assert (0);
        }
        op_count[thread_id]++;
    }
    int ret = dfs_rmdir(root_dir.c_str());
    if (ret != 0) {
        FALCON_LOG(LOG_ERROR) << "Failed to rmdir: " << root_dir << ", ret = " << ret << ", errno = " << errno;
        // assert (0);
    }
    op_count[thread_id]++;
}

void workload_open_write_close(string root_dir, int thread_id)
{
    struct timespec start_time, end_time;
    char *buf = (char *)malloc(FS_BLOCKSIZE + file_size);
    buf = (char *)(((uintptr_t)buf + FS_BLOCKSIZE_ALIGN) &~ ((uintptr_t)FS_BLOCKSIZE_ALIGN));
    string thread_dir = fmt::format("{}thread_{}/", root_dir, thread_id);
    for (int i = 0; i < files_per_dir; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        string new_path = fmt::format("{}file_{}", thread_dir, i);
        // int fd = dfs_open(new_path.c_str(), O_CREAT | O_DIRECT | O_WRONLY | O_TRUNC, 0666);
        int fd = dfs_open(new_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0666);
        if (fd < 0) {
            FALCON_LOG(LOG_ERROR) << "Failed to open file: " << new_path << ", fd = " << fd << ", errno = " << errno;
            assert (0);
        }
        int ret = dfs_write(fd, buf, file_size, 0);
        if (ret < file_size) {
            FALCON_LOG(LOG_ERROR) << "Failed to write file: " << new_path << ", fd = " << fd << ", write " << ret << " bytes" << ", errno = " << strerror(errno);
            assert (0);
        }
        dfs_close(fd, new_path.c_str());
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        op_count[thread_id]++;
        uint64_t elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
        latency_count[thread_id] += elapsed_time;
    }
}

void workload_open_read_close(string root_dir, int thread_id)
{
    struct timespec start_time, end_time;
    char *buf = (char *)malloc(FS_BLOCKSIZE + file_size);
    buf = (char *)(((uintptr_t)buf + FS_BLOCKSIZE_ALIGN) &~ ((uintptr_t)FS_BLOCKSIZE_ALIGN));
    string thread_dir = fmt::format("{}thread_{}/", root_dir, thread_id);
    for (int i = 0; i < files_per_dir; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        string new_path = fmt::format("{}file_{}", thread_dir, i);
        // int fd = dfs_open(new_path.c_str(), O_DIRECT | O_RDONLY, 0666);
        int fd = dfs_open(new_path.c_str(), O_RDONLY, 0666);
        if (fd < 0) {
            FALCON_LOG(LOG_ERROR) << "Failed to open file: " << new_path << ", fd = " << fd << ", errno = " << errno;
            assert (0);
        }
        int ret = dfs_read(fd, buf, file_size, 0);
        if (ret < file_size) {
            FALCON_LOG(LOG_ERROR) << "Failed to read file: " << new_path << ", fd = " << fd << ", write " << ret << " bytes" << ", errno = " << strerror(errno);
            assert (0);
        }
        dfs_close(fd, new_path.c_str());
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        op_count[thread_id]++;
        uint64_t elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
        latency_count[thread_id] += elapsed_time;
    }
}

void workload_open_write_close_nocreate(string root_dir, int thread_id)
{
    struct timespec start_time, end_time;
    char *buf = (char *)malloc(FS_BLOCKSIZE + file_size);
    buf = (char *)(((uintptr_t)buf + FS_BLOCKSIZE_ALIGN) &~ ((uintptr_t)FS_BLOCKSIZE_ALIGN));
    string thread_dir = fmt::format("{}thread_{}/", root_dir, thread_id);
    for (int i = 0; i < files_per_dir; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        string new_path = fmt::format("{}file_{}", thread_dir, i);
        // int fd = dfs_open(new_path.c_str(), O_DIRECT | O_WRONLY, 0666);
        int fd = dfs_open(new_path.c_str(), O_WRONLY, 0666);
        if (fd < 0) {
            FALCON_LOG(LOG_ERROR) << "Failed to open file: " << new_path << ", fd = " << fd << ", errno = " << errno;
            assert (0);
        }
        int ret = dfs_write(fd, buf, file_size, 0);
        if (ret < file_size) {
            FALCON_LOG(LOG_ERROR) << "Failed to write file: " << new_path << ", fd = " << fd << ", write " << ret << " bytes" << ", errno = " << strerror(errno);
            assert (0);
        }
        dfs_close(fd, new_path.c_str());
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        op_count[thread_id]++;
        uint64_t elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
        latency_count[thread_id] += elapsed_time;
    }
}
