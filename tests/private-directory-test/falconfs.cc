#include "dfs.h"
#include "falcon_meta.h"

#include <cstdlib>
#include <iostream>
#include <vector>
#include <atomic>

using namespace std;

#define SERVER_IP "10.0.3.1"
#define SERVER_PORT "56910"

#define PROGRAM_ERROR 17

static int s_clientNumber = 0;
static std::vector<std::shared_ptr<Router>> routers;
static std::atomic<uint64_t> routerIndex = 0;


int dfs_init(int client_number)
{
    if (getenv("SERVER_IP") == nullptr || getenv("SERVER_PORT") == nullptr) {
        cout << "env SERVER_IP or SERVER_PORT is empty" << endl;
        return -1;
    }
    std::string serverIp = getenv("SERVER_IP");
    std::string serverPort = getenv("SERVER_PORT");
    ServerIdentifier coordinator(serverIp, std::stoi(serverPort));
    s_clientNumber = client_number;
    while (client_number-- > 0) {
        routers.emplace_back(std::make_shared<Router>(coordinator));
    }
    return 0;
}

int dfs_open(const char *path, int flags, mode_t mode)
{
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetWorkerConnByPath(std::string(path));
    if (!conn)
    {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }
    uint64_t inodeId;
    int64_t size = 0;
    int32_t nodeId;
    struct stat st;
    // memset(&st, 0, sizeof(st));
    int errorCode = conn->Open(path, inodeId, size, nodeId, &st);
    return errorCode;
}

int dfs_read(int fd, void *buf, size_t count, off_t offset)
{
    assert(0);
    return pread(fd, buf, count, offset);
}
int dfs_write(int fd, const void *buf, size_t count, off_t offset)
{
    assert(0);
    return pwrite(fd, buf, count, offset);
}
int dfs_close(int fd, const char* path)
{
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetWorkerConnByPath(std::string(path));
    if (!conn)
    {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }
    int64_t size = 0;
    int32_t nodeId = 0;
    int errorCode = conn->Close(path, size, 0, nodeId);
    return errorCode;
}
int dfs_mkdir(const char *path, mode_t mode)
{
    std::string dirPath(path);
    if (dirPath.length() > 1 && dirPath.back() == '/') {
        dirPath.pop_back();
    }

    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetCoordinatorConn();
    if (!conn) {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }
    int errorCode = conn->Mkdir(dirPath.c_str());
    return errorCode;
}
int dfs_rmdir(const char *path)
{
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetCoordinatorConn();
    if (!conn) {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }
    int errorCode = conn->Rmdir(path);

    return errorCode;
}
int dfs_create(const char *path, mode_t mode)
{
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetWorkerConnByPath(std::string(path));
    if (!conn)
    {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }
    uint64_t inodeId;
    int32_t nodeId;
    struct stat st;
    memset(&st, 0, sizeof(st));
    int errorCode = conn->Create(path, inodeId, nodeId, &st);
    return errorCode;
}
int dfs_unlink(const char *path)
{   
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetWorkerConnByPath(std::string(path));
    if (!conn) {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }

    uint64_t inodeId = 0;
    int64_t size = 0;
    int32_t nodeId = 0;
    int errorCode = conn->Unlink(path, inodeId, size, nodeId);
    return errorCode;
}
int dfs_stat(const char *path, struct stat *stbuf)
{
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetWorkerConnByPath(std::string(path));
    if (!conn) {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }
    int errorCode = conn->Stat(path, stbuf);
    return errorCode;
}

void dfs_shutdown()
{
    return;
}

int dfs_kv_put(const char *key, uint32_t value_len, uint16_t slice_num,
               const uint64_t *value_key, const uint64_t *location, const uint32_t *size)
{
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetWorkerConnByPath(std::string(key));
    if (!conn) {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }

    std::vector<uint64_t> valueKeyVec(value_key, value_key + slice_num);
    std::vector<uint64_t> locationVec(location, location + slice_num);
    std::vector<uint32_t> sizeVec(size, size + slice_num);

    int errorCode = conn->KvPut(key, value_len, slice_num, valueKeyVec, locationVec, sizeVec);
    return errorCode;
}

int dfs_kv_get(const char *key, uint32_t *value_len, uint16_t *slice_num)
{
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetWorkerConnByPath(std::string(key));
    if (!conn) {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }

    Connection::KvGetResult result;
    int errorCode = conn->KvGet(key, result);
    if (errorCode == 0) {
        if (value_len) *value_len = result.valueLen;
        if (slice_num) *slice_num = result.sliceNum;
    }
    return errorCode;
}

int dfs_kv_del(const char *key)
{
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetWorkerConnByPath(std::string(key));
    if (!conn) {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }

    int errorCode = conn->KvDel(key);
    return errorCode;
}

// Slice operations
int dfs_slice_put(const char *filename, uint64_t inode_id, uint32_t chunk_id,
                  uint64_t slice_id, uint32_t slice_size, uint32_t slice_offset, uint32_t slice_len)
{
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetWorkerConnByPath(std::string(filename));
    if (!conn) {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }

    std::vector<uint64_t> inodeIdVec = {inode_id};
    std::vector<uint32_t> chunkIdVec = {chunk_id};
    std::vector<uint64_t> sliceIdVec = {slice_id};
    std::vector<uint32_t> sliceSizeVec = {slice_size};
    std::vector<uint32_t> sliceOffsetVec = {slice_offset};
    std::vector<uint32_t> sliceLenVec = {slice_len};
    std::vector<uint32_t> sliceLoc1Vec = {1};
    std::vector<uint32_t> sliceLoc2Vec = {2};

    int errorCode = conn->SlicePut(filename, 1, inodeIdVec, chunkIdVec, sliceIdVec,
                                   sliceSizeVec, sliceOffsetVec, sliceLenVec, sliceLoc1Vec, sliceLoc2Vec);
    return errorCode;
}

int dfs_slice_get(const char *filename, uint64_t inode_id, uint32_t chunk_id, uint32_t *slice_num)
{
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetWorkerConnByPath(std::string(filename));
    if (!conn) {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }

    Connection::SliceGetResult result;
    int errorCode = conn->SliceGet(filename, inode_id, chunk_id, result);
    if (errorCode == 0 && slice_num) {
        *slice_num = result.sliceNum;
    }
    return errorCode;
}

int dfs_slice_del(const char *filename, uint64_t inode_id, uint32_t chunk_id)
{
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetWorkerConnByPath(std::string(filename));
    if (!conn) {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }

    int errorCode = conn->SliceDel(filename, inode_id, chunk_id);
    return errorCode;
}

int dfs_fetch_slice_id(uint32_t count, uint64_t *start_id, uint64_t *end_id)
{
    uint64_t index = routerIndex.fetch_add(1, std::memory_order_relaxed) % s_clientNumber;
    std::shared_ptr<Connection> conn = routers[index]->GetCoordinatorConn();
    if (!conn) {
        std::cout << "route error.\n";
        return PROGRAM_ERROR;
    }

    uint64_t startId = 0, endId = 0;
    int errorCode = conn->FetchSliceId(count, 0, startId, endId);
    if (errorCode == 0) {
        if (start_id) *start_id = startId;
        if (end_id) *end_id = endId;
    }
    return errorCode;
}
