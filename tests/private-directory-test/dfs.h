#include <cstdint>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/uio.h>
#include <sys/time.h>

#include <limits.h>
#include <sys/xattr.h>

#include <fmt/format.h>
#include <map>
#include <vector>
#include <thread>
#include <regex>
#include <iostream>
#include <random>
#include <algorithm>
#include <atomic>
#include <time.h>
#include <cassert>
#include <pthread.h>
#include <fstream>

#define SLEEP_TIME 4
#define UDP_PORT 1111
#define FS_BLOCKSIZE 4096
#define FS_BLOCKSIZE_ALIGN 4095

extern int thread_num;
extern int client_cache_size;
extern int files_per_dir;
extern int file_size;

extern int file_num;
extern std::atomic<bool> printed;
extern volatile uint64_t op_count[16384];
extern volatile uint64_t latency_count[16384];

int dfs_init(int client_number = 1);
int dfs_open(const char *path, int flags, mode_t mode);
int dfs_read(int fd, void *buf, size_t count, off_t offset);
int dfs_write(int fd, const void *buf, size_t count, off_t offset);
int dfs_close(int fd, const char* path = nullptr);
int dfs_mkdir(const char *path, mode_t mode);
int dfs_rmdir(const char *path);
int dfs_create(const char *path, mode_t mode);
int dfs_unlink(const char *path);
int dfs_stat(const char *path, struct stat *stbuf);
void dfs_shutdown();

int dfs_kv_put(const char *key, uint32_t value_len, uint16_t slice_num,
               const uint64_t *value_key, const uint64_t *location, const uint32_t *size);
int dfs_kv_get(const char *key, uint32_t *value_len, uint16_t *slice_num);
int dfs_kv_del(const char *key);

int dfs_slice_put(const char *filename, uint64_t inode_id, uint32_t chunk_id,
                  uint64_t slice_id, uint32_t slice_size, uint32_t slice_offset, uint32_t slice_len);
int dfs_slice_get(const char *filename, uint64_t inode_id, uint32_t chunk_id, uint32_t *slice_num);
int dfs_slice_del(const char *filename, uint64_t inode_id, uint32_t chunk_id);
int dfs_fetch_slice_id(uint32_t count, uint64_t *start_id, uint64_t *end_id);

void workload_init(std::string root_dir, int thread_id);
void workload_create(std::string root_dir, int thread_id);
void workload_stat(std::string root_dir, int thread_id);
void workload_delete(std::string root_dir, int thread_id);
void workload_mkdir(std::string root_dir, int thread_id);
void workload_rmdir(std::string root_dir, int thread_id);
void workload_uninit(std::string root_dir, int thread_id);
void workload_open_read_close(std::string root_dir, int thread_id);
void workload_open_write_close(std::string root_dir, int thread_id);
void workload_open_write_close_nocreate(std::string root_dir, int thread_id);
void workload_open(std::string root_dir, int thread_id);
void workload_close(std::string root_dir, int thread_id);

void workload_kv_put(std::string root_dir, int thread_id);
void workload_kv_get(std::string root_dir, int thread_id);
void workload_kv_del(std::string root_dir, int thread_id);
void workload_slice_put(std::string root_dir, int thread_id);
void workload_slice_get(std::string root_dir, int thread_id);
void workload_slice_del(std::string root_dir, int thread_id);
