#include "dfs.h"

using namespace std;

void workload_init(string root_dir, int thread_id)
{
    // cout << "workload_init" << std::endl;
    int ret = dfs_mkdir(root_dir.c_str(), 0777);
    if (ret != 0) {
        cerr << "Failed to mkdir: " << root_dir << ", ret = " << ret << ", errno = " << errno << std::endl;
        // assert (0);
    }
    op_count[thread_id]++;
    for (int i = 0; i < files_per_dir - 1; i++) {
        string new_path = fmt::format("{}thread_{}", root_dir, i);
        int ret = dfs_mkdir(new_path.c_str(), 0777);
        if (ret != 0) {
            cerr << "Failed to mkdir: " << new_path << ", ret = " << ret << ", errno = " << errno << std::endl;
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
            cerr << "Failed to create: " << new_path << ", ret = " << ret << ", errno = " << errno << std::endl;
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
            cerr << "Failed to stat: " << new_path << ", ret = " << ret << ", errno = " << errno << std::endl;
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
            cerr << "Failed to open file: " << new_path << ", fd = " << fd << ", errno = " << errno << std::endl;
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
            cerr << "Failed to delete: " << new_path << ", ret = " << ret << ", errno = " << errno << std::endl;
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
            cerr << "Failed to mkdir: " << new_path << ", ret = " << ret << ", errno = " << errno << std::endl;
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
            cerr << "Failed to rmdir: " << new_path << ", ret = " << ret << ", errno = " << errno << std::endl;
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
            cerr << "Failed to rmdir: " << new_path << ", ret = " << ret << ", errno = " << errno << std::endl;
            // assert (0);
        }
        op_count[thread_id]++;
    }
    int ret = dfs_rmdir(root_dir.c_str());
    if (ret != 0) {
        cerr << "Failed to rmdir: " << root_dir << ", ret = " << ret << ", errno = " << errno << std::endl;
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
            cerr << "Failed to open file: " << new_path << ", fd = " << fd << ", errno = " << errno << std::endl;
            assert (0);
        }
        int ret = dfs_write(fd, buf, file_size, 0);
        if (ret < file_size) {
            cerr << "Failed to write file: " << new_path << ", fd = " << fd << ", write " << ret << " bytes" << ", errno = " << strerror(errno) << std::endl;
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
            cerr << "Failed to open file: " << new_path << ", fd = " << fd << ", errno = " << errno << std::endl;
            assert (0);
        }
        int ret = dfs_read(fd, buf, file_size, 0);
        if (ret < file_size) {
            cerr << "Failed to read file: " << new_path << ", fd = " << fd << ", write " << ret << " bytes" << ", errno = " << strerror(errno) << std::endl;
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
            cerr << "Failed to open file: " << new_path << ", fd = " << fd << ", errno = " << errno << std::endl;
            assert (0);
        }
        int ret = dfs_write(fd, buf, file_size, 0);
        if (ret < file_size) {
            cerr << "Failed to write file: " << new_path << ", fd = " << fd << ", write " << ret << " bytes" << ", errno = " << strerror(errno) << std::endl;
            assert (0);
        }
        dfs_close(fd, new_path.c_str());
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        op_count[thread_id]++;
        uint64_t elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
        latency_count[thread_id] += elapsed_time;
    }
}

void workload_kv_put(string root_dir, int thread_id)
{
    struct timespec start_time, end_time;
    for (int i = 0; i < files_per_dir; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        string key = fmt::format("{}thread_{}_key_{}", root_dir, thread_id, i);

        uint64_t value_key[2] = {(uint64_t)(thread_id * 10000 + i), (uint64_t)(thread_id * 10000 + i + 1)};
        uint64_t location[2] = {0, 2048};
        uint32_t size[2] = {2048, 2048};
        uint32_t value_len = 4096;
        uint16_t slice_num = 2;

        int ret = dfs_kv_put(key.c_str(), value_len, slice_num, value_key, location, size);
        if (ret != 0) {
            cerr << "Failed to kv_put: " << key << ", ret = " << ret << std::endl;
        }
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        op_count[thread_id]++;
        uint64_t elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
        latency_count[thread_id] += elapsed_time;
    }
}

void workload_kv_get(string root_dir, int thread_id)
{
    struct timespec start_time, end_time;
    for (int i = 0; i < files_per_dir; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        string key = fmt::format("{}thread_{}_key_{}", root_dir, thread_id, i);

        uint32_t value_len = 0;
        uint16_t slice_num = 0;
        int ret = dfs_kv_get(key.c_str(), &value_len, &slice_num);
        if (ret != 0) {
            cerr << "Failed to kv_get: " << key << ", ret = " << ret << std::endl;
        }
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        op_count[thread_id]++;
        uint64_t elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
        latency_count[thread_id] += elapsed_time;
    }
}

void workload_kv_del(string root_dir, int thread_id)
{
    struct timespec start_time, end_time;
    for (int i = 0; i < files_per_dir; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        string key = fmt::format("{}thread_{}_key_{}", root_dir, thread_id, i);

        int ret = dfs_kv_del(key.c_str());
        if (ret != 0) {
            cerr << "Failed to kv_del: " << key << ", ret = " << ret << std::endl;
        }
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        op_count[thread_id]++;
        uint64_t elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
        latency_count[thread_id] += elapsed_time;
    }
}

// Slice workloads
void workload_slice_put(string root_dir, int thread_id)
{
    struct timespec start_time, end_time;
    string thread_dir = fmt::format("{}thread_{}/", root_dir, thread_id);

    uint64_t start_slice_id = 0, end_slice_id = 0;
    int fetch_ret = dfs_fetch_slice_id(files_per_dir, &start_slice_id, &end_slice_id);
    if (fetch_ret != 0) {
        cerr << "Failed to fetch slice id, ret = " << fetch_ret << std::endl;
        return;
    }

    for (int i = 0; i < files_per_dir; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        string filename = fmt::format("{}file_{}", thread_dir, i);

        uint64_t inode_id = thread_id * 100000 + i;
        uint32_t chunk_id = 0;
        uint64_t slice_id = start_slice_id + i;
        uint32_t slice_size = 4096;
        uint32_t slice_offset = 0;
        uint32_t slice_len = 4096;

        int ret = dfs_slice_put(filename.c_str(), inode_id, chunk_id, slice_id, slice_size, slice_offset, slice_len);
        if (ret != 0) {
            cerr << "Failed to slice_put: " << filename << ", ret = " << ret << std::endl;
        }
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        op_count[thread_id]++;
        uint64_t elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
        latency_count[thread_id] += elapsed_time;
    }
}

void workload_slice_get(string root_dir, int thread_id)
{
    struct timespec start_time, end_time;
    string thread_dir = fmt::format("{}thread_{}/", root_dir, thread_id);

    for (int i = 0; i < files_per_dir; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        string filename = fmt::format("{}file_{}", thread_dir, i);

        uint64_t inode_id = thread_id * 100000 + i;
        uint32_t chunk_id = 0;
        uint32_t slice_num = 0;

        int ret = dfs_slice_get(filename.c_str(), inode_id, chunk_id, &slice_num);
        if (ret != 0) {
            cerr << "Failed to slice_get: " << filename << ", ret = " << ret << std::endl;
        }
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        op_count[thread_id]++;
        uint64_t elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
        latency_count[thread_id] += elapsed_time;
    }
}

void workload_slice_del(string root_dir, int thread_id)
{
    struct timespec start_time, end_time;
    string thread_dir = fmt::format("{}thread_{}/", root_dir, thread_id);

    for (int i = 0; i < files_per_dir; i++) {
        clock_gettime(CLOCK_MONOTONIC, &start_time);
        string filename = fmt::format("{}file_{}", thread_dir, i);

        uint64_t inode_id = thread_id * 100000 + i;
        uint32_t chunk_id = 0;

        int ret = dfs_slice_del(filename.c_str(), inode_id, chunk_id);
        if (ret != 0) {
            cerr << "Failed to slice_del: " << filename << ", ret = " << ret << std::endl;
        }
        clock_gettime(CLOCK_MONOTONIC, &end_time);
        op_count[thread_id]++;
        uint64_t elapsed_time = (end_time.tv_sec - start_time.tv_sec) * 1000000000 + (end_time.tv_nsec - start_time.tv_nsec);
        latency_count[thread_id] += elapsed_time;
    }
}