#include "dfs.h"

using namespace std;


int dfs_init(int client_number)
{
    return 0;
}

int dfs_open(const char *path, int flags, mode_t mode)
{
    return open(path, flags, mode);
}

int dfs_read(int fd, void *buf, size_t count, off_t offset)
{
    return pread(fd, buf, count, offset);
}
int dfs_write(int fd, const void *buf, size_t count, off_t offset)
{
    return pwrite(fd, buf, count, offset);
}
int dfs_close(int fd, const char* path)
{
    return close(fd);
}
int dfs_mkdir(const char *path, mode_t mode)
{
    return mkdir(path, mode);
}
int dfs_rmdir(const char *path)
{
    return rmdir(path);
}
int dfs_create(const char *path, mode_t mode)
{
    return mknod(path, mode, 0);
}
int dfs_unlink(const char *path)
{
    return unlink(path);
}
int dfs_stat(const char *path, struct stat *stbuf)
{
    return stat(path, stbuf);
}

void dfs_shutdown()
{
    return;
}

int dfs_kv_put(const char *key, uint32_t value_len, uint16_t slice_num,
               const uint64_t *value_key, const uint64_t *location, const uint32_t *size)
{
    cerr << "KV operations not supported in POSIX mode" << endl;
    return -1;
}

int dfs_kv_get(const char *key, uint32_t *value_len, uint16_t *slice_num)
{
    cerr << "KV operations not supported in POSIX mode" << endl;
    return -1;
}

int dfs_kv_del(const char *key)
{
    cerr << "KV operations not supported in POSIX mode" << endl;
    return -1;
}

// Slice operations - not supported in POSIX mode
int dfs_slice_put(const char *filename, uint64_t inode_id, uint32_t chunk_id,
                  uint64_t slice_id, uint32_t slice_size, uint32_t slice_offset, uint32_t slice_len)
{
    cerr << "Slice operations not supported in POSIX mode" << endl;
    return -1;
}

int dfs_slice_get(const char *filename, uint64_t inode_id, uint32_t chunk_id, uint32_t *slice_num)
{
    cerr << "Slice operations not supported in POSIX mode" << endl;
    return -1;
}

int dfs_slice_del(const char *filename, uint64_t inode_id, uint32_t chunk_id)
{
    cerr << "Slice operations not supported in POSIX mode" << endl;
    return -1;
}

int dfs_fetch_slice_id(uint32_t count, uint64_t *start_id, uint64_t *end_id)
{
    cerr << "Slice operations not supported in POSIX mode" << endl;
    return -1;
}
