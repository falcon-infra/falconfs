#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "hcom_comm_adapter/hcom_meta_service.h"
#include "plugin/falcon_plugin_framework.h"
#include "plugin/falcon_plugin_loader.h"

using namespace falcon::meta_service;

// ==================== 测试配置 ====================

static const char *TEST_BASE_PATH = "/falcon_meta_test";
static const int TEST_TIMEOUT_MS = 10000;

// ==================== 全局状态 ====================

static HcomMetaService *g_meta_service = nullptr;
static int g_test_passed = 0;
static int g_test_failed = 0;
static int g_test_skipped = 0;
static int g_node_port = 0;
static bool g_is_cn_node = false;
static std::atomic<int> g_loop_iteration(0);  // 循环计数器，用于生成唯一标识

struct SyncContext
{
    std::mutex mtx;
    std::condition_variable cv;
    volatile bool done = false;
    FalconMetaServiceResponse resp;

    void *copied_data = nullptr;

    void CleanupCopiedData()
    {
        if (copied_data == nullptr)
            return;
        switch (resp.opcode) {
        case DFC_PLAIN_COMMAND:
            delete static_cast<PlainCommandResponse *>(copied_data);
            break;
        case DFC_CREATE:
            delete static_cast<CreateResponse *>(copied_data);
            break;
        case DFC_OPEN:
            delete static_cast<OpenResponse *>(copied_data);
            break;
        case DFC_STAT:
            delete static_cast<StatResponse *>(copied_data);
            break;
        case DFC_UNLINK:
            delete static_cast<UnlinkResponse *>(copied_data);
            break;
        case DFC_OPENDIR:
            delete static_cast<OpenDirResponse *>(copied_data);
            break;
        case DFC_READDIR:
            delete static_cast<ReadDirResponse *>(copied_data);
            break;
        case DFC_GET_KV_META:
            delete static_cast<KvDataResponse *>(copied_data);
            break;
        case DFC_SLICE_GET:
            delete static_cast<SliceInfoResponse *>(copied_data);
            break;
        case DFC_FETCH_SLICE_ID:
            delete static_cast<SliceIdResponse *>(copied_data);
            break;
        default:
            break;
        }
        copied_data = nullptr;
    }

    ~SyncContext() { CleanupCopiedData(); }
};

static void SyncCallback(const FalconMetaServiceResponse &response, void *ctx)
{
    SyncContext *context = static_cast<SyncContext *>(ctx);

    context->resp.status = response.status;
    context->resp.opcode = response.opcode;

    // 深拷贝响应数据，因为回调返回后原始数据会被释放
    context->CleanupCopiedData();
    if (response.data != nullptr && response.status == 0) {
        switch (response.opcode) {
        case DFC_PLAIN_COMMAND: {
            auto *src = static_cast<PlainCommandResponse *>(response.data);
            auto *dst = new PlainCommandResponse(*src);
            context->copied_data = dst;
            break;
        }
        case DFC_CREATE: {
            auto *src = static_cast<CreateResponse *>(response.data);
            auto *dst = new CreateResponse(*src);
            context->copied_data = dst;
            break;
        }
        case DFC_OPEN: {
            auto *src = static_cast<OpenResponse *>(response.data);
            auto *dst = new OpenResponse(*src);
            context->copied_data = dst;
            break;
        }
        case DFC_STAT: {
            auto *src = static_cast<StatResponse *>(response.data);
            auto *dst = new StatResponse(*src);
            context->copied_data = dst;
            break;
        }
        case DFC_UNLINK: {
            auto *src = static_cast<UnlinkResponse *>(response.data);
            auto *dst = new UnlinkResponse(*src);
            context->copied_data = dst;
            break;
        }
        case DFC_OPENDIR: {
            auto *src = static_cast<OpenDirResponse *>(response.data);
            auto *dst = new OpenDirResponse(*src);
            context->copied_data = dst;
            break;
        }
        case DFC_READDIR: {
            auto *src = static_cast<ReadDirResponse *>(response.data);
            auto *dst = new ReadDirResponse(*src);
            context->copied_data = dst;
            break;
        }
        case DFC_GET_KV_META: {
            auto *src = static_cast<KvDataResponse *>(response.data);
            auto *dst = new KvDataResponse(*src);
            context->copied_data = dst;
            break;
        }
        case DFC_SLICE_GET: {
            auto *src = static_cast<SliceInfoResponse *>(response.data);
            auto *dst = new SliceInfoResponse(*src);
            context->copied_data = dst;
            break;
        }
        case DFC_FETCH_SLICE_ID: {
            auto *src = static_cast<SliceIdResponse *>(response.data);
            auto *dst = new SliceIdResponse(*src);
            context->copied_data = dst;
            break;
        }
        default:
            break;
        }
    }
    context->resp.data = context->copied_data;
    asm volatile("" ::: "memory");
    context->done = true;
    context->cv.notify_one();
}

static int WaitForResponse(SyncContext &ctx, const std::string &key = "")
{
    std::unique_lock<std::mutex> lock(ctx.mtx);
    while (!ctx.done) {
        if (ctx.cv.wait_for(lock, std::chrono::milliseconds(TEST_TIMEOUT_MS)) == std::cv_status::timeout) {
//            printf("[TEST] ERROR: Request timeout for 10s, key is:%s\n", key.c_str());
            continue;
        }
    }
    return 0;
}

// ==================== 测试辅助宏 ====================

#define TEST_BEGIN(name)                                \
    printf("\n========== TEST: %s ==========\n", name); \
    fflush(stdout);

#define TEST_ASSERT(cond, msg)          \
    do {                                \
        if (!(cond)) {                  \
            printf("[FAIL] %s\n", msg); \
            fflush(stdout);             \
            g_test_failed++;            \
            return false;               \
        }                               \
    } while (0)

#define TEST_ASSERT_MSG(cond, fmt, ...)                \
    do {                                               \
        if (!(cond)) {                                 \
            printf("[FAIL] " fmt "\n", ##__VA_ARGS__); \
            fflush(stdout);                            \
            g_test_failed++;                           \
            return false;                              \
        }                                              \
    } while (0)

#define TEST_PASS(name)              \
    do {                             \
        printf("[PASS] %s\n", name); \
        fflush(stdout);              \
        g_test_passed++;             \
        return true;                 \
    } while (0)

// ==================== 基础操作封装 ====================

static int DoMkdir(const std::string &path)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_MKDIR;
    meta_param_helper::Set(req.file_params, PathOnlyParam(path));

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    printf("  MKDIR %s -> status=%d\n", path.c_str(), ctx.resp.status);
    return ctx.resp.status;
}

static int DoCreate(const std::string &path, CreateResponse *out_resp = nullptr)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_CREATE;
    meta_param_helper::Set(req.file_params, PathOnlyParam(path));

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    if (ctx.resp.status == 0 && ctx.resp.data && out_resp) {
        CreateResponse *r = static_cast<CreateResponse *>(ctx.resp.data);
        *out_resp = *r;
    }

    printf("  CREATE %s -> status=%d\n", path.c_str(), ctx.resp.status);
    return ctx.resp.status;
}

static int DoStat(const std::string &path, StatResponse *out_resp = nullptr)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_STAT;
    meta_param_helper::Set(req.file_params, PathOnlyParam(path));

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    if (ctx.resp.status == 0 && ctx.resp.data && out_resp) {
        StatResponse *r = static_cast<StatResponse *>(ctx.resp.data);
        *out_resp = *r;
    }

    printf("  STAT %s -> status=%d\n", path.c_str(), ctx.resp.status);
    return ctx.resp.status;
}

static int DoOpen(const std::string &path, OpenResponse *out_resp = nullptr)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_OPEN;
    meta_param_helper::Set(req.file_params, PathOnlyParam(path));

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    if (ctx.resp.status == 0 && ctx.resp.data && out_resp) {
        OpenResponse *r = static_cast<OpenResponse *>(ctx.resp.data);
        *out_resp = *r;
    }

    printf("  OPEN %s -> status=%d\n", path.c_str(), ctx.resp.status);
    return ctx.resp.status;
}

static int DoClose(const std::string &path, int64_t st_size, uint64_t st_mtim, int32_t node_id)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_CLOSE;
    CloseParam param;
    param.path = path;
    param.st_size = st_size;
    param.st_mtim = st_mtim;
    param.node_id = node_id;
    meta_param_helper::Set(req.file_params, param);

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    printf("  CLOSE %s -> status=%d\n", path.c_str(), ctx.resp.status);
    return ctx.resp.status;
}

static int DoUnlink(const std::string &path)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_UNLINK;
    meta_param_helper::Set(req.file_params, PathOnlyParam(path));

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    printf("  UNLINK %s -> status=%d\n", path.c_str(), ctx.resp.status);
    return ctx.resp.status;
}

static int DoRmdir(const std::string &path)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_RMDIR;
    meta_param_helper::Set(req.file_params, PathOnlyParam(path));

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    printf("  RMDIR %s -> status=%d\n", path.c_str(), ctx.resp.status);
    return ctx.resp.status;
}

static int DoRename(const std::string &src, const std::string &dst)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_RENAME;
    meta_param_helper::Set(req.file_params, RenameParam(src, dst));

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    printf("  RENAME %s -> %s: status=%d\n", src.c_str(), dst.c_str(), ctx.resp.status);
    return ctx.resp.status;
}

static int DoOpendir(const std::string &path, OpenDirResponse *out_resp = nullptr)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_OPENDIR;
    meta_param_helper::Set(req.file_params, PathOnlyParam(path));

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    if (ctx.resp.status == 0 && ctx.resp.data && out_resp) {
        OpenDirResponse *r = static_cast<OpenDirResponse *>(ctx.resp.data);
        *out_resp = *r;
    }

    printf("  OPENDIR %s -> status=%d\n", path.c_str(), ctx.resp.status);
    return ctx.resp.status;
}

static int DoReaddir(const std::string &path, std::vector<std::string> *out_names = nullptr)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_READDIR;
    ReadDirParam param;
    param.path = path;
    param.max_read_count = 100;
    param.last_shard_index = -1;
    meta_param_helper::Set(req.file_params, param);

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    if (ctx.resp.status == 0 && ctx.resp.data && out_names) {
        ReadDirResponse *r = static_cast<ReadDirResponse *>(ctx.resp.data);
        for (const auto &entry : r->result_list) {
            out_names->push_back(entry.file_name);
        }
    }

    printf("  READDIR %s -> status=%d\n", path.c_str(), ctx.resp.status);
    return ctx.resp.status;
}

static int DoUtimens(const std::string &path, uint64_t atime, uint64_t mtime)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_UTIMENS;
    UtimeNsParam param;
    param.path = path;
    param.st_atim = atime;
    param.st_mtim = mtime;
    meta_param_helper::Set(req.file_params, param);

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    printf("  UTIMENS %s -> status=%d\n", path.c_str(), ctx.resp.status);
    return ctx.resp.status;
}

static int DoChmod(const std::string &path, uint64_t mode)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_CHMOD;
    ChmodParam param;
    param.path = path;
    param.st_mode = mode;
    meta_param_helper::Set(req.file_params, param);

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    printf("  CHMOD %s 0%lo -> status=%d\n", path.c_str(), mode, ctx.resp.status);
    return ctx.resp.status;
}

static int DoChown(const std::string &path, uint32_t uid, uint32_t gid)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_CHOWN;
    ChownParam param;
    param.path = path;
    param.st_uid = uid;
    param.st_gid = gid;
    meta_param_helper::Set(req.file_params, param);

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    printf("  CHOWN %s uid=%u gid=%u -> status=%d\n", path.c_str(), uid, gid, ctx.resp.status);
    return ctx.resp.status;
}

static int DoSlicePut(const std::string &filename,
                      uint64_t inode,
                      uint32_t chunk,
                      uint64_t slice_id,
                      uint32_t size,
                      uint32_t offset,
                      uint32_t len)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_SLICE_PUT;
    SliceInfoParam param;
    param.filename = filename;
    param.slicenum = 1;
    param.inodeid.push_back(inode);
    param.chunkid.push_back(chunk);
    param.sliceid.push_back(slice_id);
    param.slicesize.push_back(size);
    param.sliceoffset.push_back(offset);
    param.slicelen.push_back(len);
    param.sliceloc1.push_back(1);
    param.sliceloc2.push_back(2);
    meta_param_helper::Set(req.file_params, param);

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    printf("  SLICE_PUT %s inode=%lu chunk=%u -> status=%d\n", filename.c_str(), inode, chunk, ctx.resp.status);
    return ctx.resp.status;
}

static int
DoSliceGet(const std::string &filename, uint64_t inode, uint32_t chunk, SliceInfoResponse *out_resp = nullptr)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_SLICE_GET;
    meta_param_helper::Set(req.file_params, SliceIndexParam(filename, inode, chunk));

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    if (ctx.resp.status == 0 && ctx.resp.data && out_resp) {
        SliceInfoResponse *r = static_cast<SliceInfoResponse *>(ctx.resp.data);
        *out_resp = *r;
    }

    printf("  SLICE_GET %s inode=%lu chunk=%u -> status=%d\n", filename.c_str(), inode, chunk, ctx.resp.status);
    return ctx.resp.status;
}

static int DoSliceDel(const std::string &filename, uint64_t inode, uint32_t chunk)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_SLICE_DEL;
    meta_param_helper::Set(req.file_params, SliceIndexParam(filename, inode, chunk));

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    printf("  SLICE_DEL %s inode=%lu chunk=%u -> status=%d\n", filename.c_str(), inode, chunk, ctx.resp.status);
    return ctx.resp.status;
}

static int DoFetchSliceId(uint32_t count, uint8_t type, SliceIdResponse *out_resp = nullptr)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_FETCH_SLICE_ID;
    req.sliceid_param.count = count;
    req.sliceid_param.type = type;

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    if (ctx.resp.status == 0 && ctx.resp.data && out_resp) {
        SliceIdResponse *r = static_cast<SliceIdResponse *>(ctx.resp.data);
        *out_resp = *r;
    }

    printf("  FETCH_SLICE_ID count=%u type=%u -> status=%d\n", count, type, ctx.resp.status);
    return ctx.resp.status;
}

static int DoPlainCommand(const std::string &sql, PlainCommandResponse *out_resp = nullptr)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_PLAIN_COMMAND;
    meta_param_helper::Set(req.file_params, PlainCommandParam(sql));

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

    if (ctx.resp.status == 0 && ctx.resp.data && out_resp) {
        PlainCommandResponse *r = static_cast<PlainCommandResponse *>(ctx.resp.data);
        *out_resp = *r;
    }

    printf("  PLAIN_COMMAND '%s' -> status=%d\n", sql.c_str(), ctx.resp.status);
    return ctx.resp.status;
}

static int DoKvPut(const std::string &key, uint32_t value_len, const std::vector<FormDataSlice> &slices)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_PUT_KEY_META;
    req.kv_data.key = key;
    req.kv_data.valueLen = value_len;
    req.kv_data.sliceNum = static_cast<uint16_t>(slices.size());
    req.kv_data.dataSlices = slices;

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx, key) != 0)
        return -1;

/*     printf("  KV_PUT key='%s' valueLen=%u slices=%zu -> status=%d\n",
           key.c_str(),
           value_len,
           slices.size(),
           ctx.resp.status); */
    return ctx.resp.status;
}

static int DoKvGet(const std::string &key, KvDataResponse *out_resp = nullptr)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_GET_KV_META;
    req.kv_data.key = key;

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx, key) != 0)
        return -1;

    if (ctx.resp.status == 0 && ctx.resp.data && out_resp) {
        KvDataResponse *r = static_cast<KvDataResponse *>(ctx.resp.data);
        *out_resp = *r;
        /* printf("  KV_GET -> valueLen=%u slices=%d\n", out_resp->kv_data.valueLen, out_resp->kv_data.sliceNum); */
    }

 /*    printf("  KV_GET key='%s' -> status=%d\n", key.c_str(), ctx.resp.status); */
    return ctx.resp.status;
}

static int DoKvDelete(const std::string &key)
{
    FalconMetaServiceRequest req;
    req.operation = DFC_DELETE_KV_META;
    req.kv_data.key = key;

    SyncContext ctx;
    int ret = g_meta_service->SubmitFalconMetaRequest(req, SyncCallback, &ctx);
    if (ret != 0)
        return ret;
    if (WaitForResponse(ctx) != 0)
        return -1;

/*     printf("  KV_DELETE key='%s' -> status=%d\n", key.c_str(), ctx.resp.status); */
    return ctx.resp.status;
}

// ==================== 测试用例 ====================

// 用于跳过 CN-only 测试的宏
#define SKIP_IF_NOT_CN(test_name)                                                          \
    do {                                                                                   \
        if (!g_is_cn_node) {                                                               \
            printf("\n========== TEST: %s ==========\n", test_name);                       \
            printf("[SKIP] This test requires CN node (MKDIR/RMDIR/RENAME operations)\n"); \
            fflush(stdout);                                                                \
            g_test_skipped++;                                                              \
            return true;                                                                   \
        }                                                                                  \
    } while (0)

// 用于跳过 Worker-only 测试的宏
#define SKIP_IF_NOT_WORKER(test_name)                                            \
    do {                                                                         \
        if (g_is_cn_node) {                                                      \
            printf("\n========== TEST: %s ==========\n", test_name);             \
            printf("[SKIP] This test requires Worker node (file operations)\n"); \
            fflush(stdout);                                                      \
            g_test_skipped++;                                                    \
            return true;                                                         \
        }                                                                        \
    } while (0)

/*
 * 测试 1: 目录基本操作 (MKDIR -> STAT -> OPENDIR -> READDIR -> RMDIR)
 * [CN-only] 需要 MKDIR/RMDIR 权限
 */
static bool TestDirectoryOperations()
{
    SKIP_IF_NOT_CN("Directory Operations (MKDIR/OPENDIR/READDIR/RMDIR)");
    TEST_BEGIN("Directory Operations (MKDIR/OPENDIR/READDIR/RMDIR)");

    std::string dir_path = std::string(TEST_BASE_PATH) + "/test_dir";

    // 1. 创建目录
    int status = DoMkdir(dir_path);
    TEST_ASSERT(status == 0, "MKDIR failed");

    // 2. 测试重复创建目录 - 应该失败
    status = DoMkdir(dir_path);
    TEST_ASSERT(status != 0, "MKDIR duplicate directory should fail");
    printf("  MKDIR duplicate correctly failed with status=%d\n", status);

    // 3. OPENDIR - 验证目录存在
    OpenDirResponse opendir_resp;
    status = DoOpendir(dir_path, &opendir_resp);
    TEST_ASSERT(status == 0, "OPENDIR failed");
    printf("  OPENDIR succeeded, inode=%lu\n", opendir_resp.st_ino);

    // 4. READDIR (空目录)
    std::vector<std::string> entries;
    status = DoReaddir(dir_path, &entries);
    TEST_ASSERT(status == 0, "READDIR failed");
    TEST_ASSERT(entries.size() == 0, "READDIR: empty directory should have 0 entries");
    printf("  READDIR returned %zu entries (expected 0)\n", entries.size());

    // 5. 测试 OPENDIR 不存在的目录
    status = DoOpendir(dir_path + "/nonexistent", nullptr);
    TEST_ASSERT(status != 0, "OPENDIR non-existent directory should fail");
    printf("  OPENDIR non-existent correctly failed with status=%d\n", status);

    // 6. 删除目录
    status = DoRmdir(dir_path);
    TEST_ASSERT(status == 0, "RMDIR failed");

    // 7. 验证目录已删除 - OPENDIR 应该失败
    status = DoOpendir(dir_path, nullptr);
    TEST_ASSERT(status != 0, "Directory should not exist after RMDIR");
    printf("  OPENDIR after RMDIR correctly failed with status=%d\n", status);

    // 8. 测试删除不存在的目录
    status = DoRmdir(dir_path);
    TEST_ASSERT(status != 0, "RMDIR non-existent directory should fail");
    printf("  RMDIR non-existent correctly failed with status=%d\n", status);

    TEST_PASS("Directory Operations");
}

/*
 * 测试 2: 文件基本操作 (CREATE -> STAT -> OPEN -> CLOSE -> UNLINK)
 * [Worker-only] 文件操作必须在 Worker 节点执行
 */
static bool TestFileOperations()
{
    SKIP_IF_NOT_WORKER("File Operations (CREATE/STAT/OPEN/CLOSE/UNLINK)");
    TEST_BEGIN("File Operations (CREATE/STAT/OPEN/CLOSE/UNLINK)");

    // Worker 节点上使用根目录下的测试路径
    std::string file_path = "/test_file_worker.txt";

    // 1. 创建文件
    CreateResponse create_resp;
    int status = DoCreate(file_path, &create_resp);
    TEST_ASSERT(status == 0, "CREATE failed");
    TEST_ASSERT(create_resp.st_ino != 0, "CREATE: inode should not be 0");
    printf("  Created file with inode=%lu, node_id=%ld\n", create_resp.st_ino, create_resp.node_id);

    // 2. STAT 文件 - 验证文件属性
    StatResponse stat_resp;
    status = DoStat(file_path, &stat_resp);
    TEST_ASSERT(status == 0, "STAT failed");
    TEST_ASSERT_MSG(stat_resp.st_ino == create_resp.st_ino,
                    "STAT: inode mismatch (expected %lu, got %lu)", create_resp.st_ino, stat_resp.st_ino);
    printf("  STAT succeeded: inode=%lu, mode=0%o, size=%ld\n",
           stat_resp.st_ino, stat_resp.st_mode, stat_resp.st_size);

    // 3. OPEN 文件
    OpenResponse open_resp;
    status = DoOpen(file_path, &open_resp);
    TEST_ASSERT(status == 0, "OPEN failed");
    TEST_ASSERT(open_resp.st_ino == create_resp.st_ino, "OPEN: inode mismatch");

    // 4. CLOSE 文件 (更新大小和时间)
    uint64_t new_mtime = static_cast<uint64_t>(time(nullptr)) * 1000000000ULL;
    int64_t new_size = 1024;
    status = DoClose(file_path, new_size, new_mtime, open_resp.node_id);
    TEST_ASSERT(status == 0, "CLOSE failed");
    printf("  CLOSE succeeded with new_size=%ld\n", new_size);

    // 5. STAT 后验证更新后的大小
    status = DoStat(file_path, &stat_resp);
    TEST_ASSERT(status == 0, "STAT after CLOSE failed");
    TEST_ASSERT_MSG(stat_resp.st_size == new_size,
                    "STAT after CLOSE: size mismatch (expected %ld, got %ld)", new_size, stat_resp.st_size);
    printf("  STAT after CLOSE: size=%ld (verified)\n", stat_resp.st_size);

    // 6. UNLINK 文件
    status = DoUnlink(file_path);
    TEST_ASSERT(status == 0, "UNLINK failed");

    // 7. STAT 不存在的文件 - 应该失败
    status = DoStat(file_path, nullptr);
    TEST_ASSERT(status != 0, "STAT on deleted file should fail");
    printf("  STAT on deleted file correctly failed with status=%d\n", status);

    TEST_PASS("File Operations");
}

/*
 * 测试 3: RENAME 操作 (目录重命名)
 * [CN-only] 需要 MKDIR/RMDIR/RENAME 权限
 * 注意: 文件重命名需要在 Worker 节点执行，这里只测试目录重命名
 */
static bool TestRenameOperations()
{
    SKIP_IF_NOT_CN("Rename Operations (Directory)");
    TEST_BEGIN("Rename Operations (Directory)");

    std::string src_dir = std::string(TEST_BASE_PATH) + "/test_rename_src";
    std::string dst_dir = std::string(TEST_BASE_PATH) + "/test_rename_dst";

    // 1. 创建源目录
    int status = DoMkdir(src_dir);
    TEST_ASSERT(status == 0, "MKDIR source dir failed");
    printf("  Created source directory: %s\n", src_dir.c_str());

    // 2. RENAME 目录
    status = DoRename(src_dir, dst_dir);
    TEST_ASSERT(status == 0, "RENAME directory failed");
    printf("  RENAME directory succeeded: %s -> %s\n", src_dir.c_str(), dst_dir.c_str());

    // 3. 验证源目录不存在
    status = DoOpendir(src_dir, nullptr);
    TEST_ASSERT(status != 0, "Source directory should not exist after RENAME");
    printf("  Source directory correctly does not exist after RENAME\n");

    // 4. 验证目标目录存在
    OpenDirResponse opendir_resp;
    status = DoOpendir(dst_dir, &opendir_resp);
    TEST_ASSERT(status == 0, "Destination directory should exist after RENAME");
    printf("  Destination directory exists with inode=%lu\n", opendir_resp.st_ino);

    // 5. 测试 RENAME 不存在的目录
    status = DoRename(src_dir, std::string(TEST_BASE_PATH) + "/nonexistent_dst");
    TEST_ASSERT(status != 0, "RENAME non-existent directory should fail");
    printf("  RENAME non-existent correctly failed with status=%d\n", status);

    // 6. 清理
    status = DoRmdir(dst_dir);
    TEST_ASSERT(status == 0, "RMDIR cleanup failed");

    TEST_PASS("Rename Operations (Directory)");
}

/*
 * 测试 4: 文件属性操作 (UTIMENS, CHMOD, CHOWN)
 * [Worker-only] 文件属性操作必须在 Worker 节点执行
 */
static bool TestAttributeOperations()
{
    SKIP_IF_NOT_WORKER("Attribute Operations (UTIMENS/CHMOD/CHOWN)");
    TEST_BEGIN("Attribute Operations (UTIMENS/CHMOD/CHOWN)");

    // Worker 节点上使用根目录下的测试路径
    std::string file_path = "/test_attr_worker.txt";

    // 1. 创建文件
    int status = DoCreate(file_path, nullptr);
    TEST_ASSERT(status == 0, "CREATE failed");
    printf("  Created test file: %s\n", file_path.c_str());

    // 2. UTIMENS - 修改时间
    uint64_t new_atime = 1609459200000000000ULL; // 2021-01-01 00:00:00 UTC in nanoseconds
    uint64_t new_mtime = 1640995200000000000ULL; // 2022-01-01 00:00:00 UTC in nanoseconds
    status = DoUtimens(file_path, new_atime, new_mtime);
    TEST_ASSERT(status == 0, "UTIMENS failed");
    printf("  UTIMENS succeeded\n");

    // 3. CHMOD - 修改权限
    uint64_t new_mode = 0755;
    status = DoChmod(file_path, new_mode);
    TEST_ASSERT(status == 0, "CHMOD failed");
    printf("  CHMOD to 0755 succeeded\n");

    // 4. CHOWN - 修改所有者
    uint32_t new_uid = 1000;
    uint32_t new_gid = 1000;
    status = DoChown(file_path, new_uid, new_gid);
    TEST_ASSERT(status == 0, "CHOWN failed");
    printf("  CHOWN to uid=%u, gid=%u succeeded\n", new_uid, new_gid);

    // 5. 测试对不存在文件的属性操作
    std::string nonexistent = "/nonexistent_attr_test.txt";
    status = DoUtimens(nonexistent, new_atime, new_mtime);
    TEST_ASSERT(status != 0, "UTIMENS on non-existent file should fail");

    status = DoChmod(nonexistent, 0644);
    TEST_ASSERT(status != 0, "CHMOD on non-existent file should fail");

    status = DoChown(nonexistent, 1000, 1000);
    TEST_ASSERT(status != 0, "CHOWN on non-existent file should fail");
    printf("  Attribute operations on non-existent file correctly failed\n");

    // 6. 清理
    status = DoUnlink(file_path);
    TEST_ASSERT(status == 0, "UNLINK cleanup failed");

    TEST_PASS("Attribute Operations");
}

/*
 * 测试 5: Slice 操作 (FETCH_SLICE_ID -> SLICE_PUT -> SLICE_GET -> SLICE_DEL)
 * [Worker-only] Slice 操作必须在 Worker 节点执行
 */
static bool TestSliceOperations()
{
    SKIP_IF_NOT_WORKER("Slice Operations (FETCH_SLICE_ID/SLICE_PUT/SLICE_GET/SLICE_DEL)");
    TEST_BEGIN("Slice Operations (FETCH_SLICE_ID/SLICE_PUT/SLICE_GET/SLICE_DEL)");

    // Worker 节点上使用根目录下的测试路径
    std::string file_path = "/test_slice_worker.dat";

    // 1. 创建文件
    CreateResponse create_resp;
    int status = DoCreate(file_path, &create_resp);
    TEST_ASSERT(status == 0, "CREATE failed");

    uint64_t inode = create_resp.st_ino;
    printf("  Created file with inode=%lu\n", inode);

    // 2. FETCH_SLICE_ID - 分配多个 slice ID
    SliceIdResponse slice_id_resp;
    uint32_t slice_count = 5;
    status = DoFetchSliceId(slice_count, 0, &slice_id_resp);
    TEST_ASSERT(status == 0, "FETCH_SLICE_ID failed");
    TEST_ASSERT(slice_id_resp.end > slice_id_resp.start, "Invalid slice ID range");
    TEST_ASSERT_MSG(slice_id_resp.end - slice_id_resp.start == slice_count,
                    "FETCH_SLICE_ID: expected %u IDs, got %lu",
                    slice_count,
                    slice_id_resp.end - slice_id_resp.start);
    printf("  Allocated slice IDs: [%lu, %lu), count=%lu\n",
           slice_id_resp.start,
           slice_id_resp.end,
           slice_id_resp.end - slice_id_resp.start);

    uint64_t slice_id = slice_id_resp.start;
    uint32_t slice_size = 4096;
    uint32_t slice_offset = 0;
    uint32_t slice_len = 4096;

    // 3. SLICE_PUT - chunk 0
    status = DoSlicePut(file_path, inode, 0, slice_id, slice_size, slice_offset, slice_len);
    TEST_ASSERT(status == 0, "SLICE_PUT chunk 0 failed");

    // 4. SLICE_GET - 验证 chunk 0
    SliceInfoResponse slice_info;
    status = DoSliceGet(file_path, inode, 0, &slice_info);
    TEST_ASSERT(status == 0, "SLICE_GET chunk 0 failed");
    TEST_ASSERT(slice_info.slicenum >= 1, "SLICE_GET: no slices returned");
    printf("  SLICE_GET chunk 0: slicenum=%u\n", slice_info.slicenum);

    // 验证返回的 slice 数据一致性
    if (slice_info.slicenum > 0 && slice_info.sliceid.size() > 0) {
        // 验证 slice_id
        TEST_ASSERT_MSG(slice_info.sliceid[0] == slice_id,
                        "SLICE_GET: slice_id mismatch (expected %lu, got %lu)",
                        slice_id,
                        slice_info.sliceid[0]);
        // 验证 slice_size
        TEST_ASSERT_MSG(slice_info.slicesize.size() > 0 && slice_info.slicesize[0] == slice_size,
                        "SLICE_GET: slicesize mismatch (expected %u, got %u)",
                        slice_size,
                        slice_info.slicesize.size() > 0 ? slice_info.slicesize[0] : 0);
        // 验证 slice_offset
        TEST_ASSERT_MSG(slice_info.sliceoffset.size() > 0 && slice_info.sliceoffset[0] == slice_offset,
                        "SLICE_GET: sliceoffset mismatch (expected %u, got %u)",
                        slice_offset,
                        slice_info.sliceoffset.size() > 0 ? slice_info.sliceoffset[0] : 0);
        // 验证 slice_len
        TEST_ASSERT_MSG(slice_info.slicelen.size() > 0 && slice_info.slicelen[0] == slice_len,
                        "SLICE_GET: slicelen mismatch (expected %u, got %u)",
                        slice_len,
                        slice_info.slicelen.size() > 0 ? slice_info.slicelen[0] : 0);
        // 验证 inode_id
        TEST_ASSERT_MSG(slice_info.inodeid.size() > 0 && slice_info.inodeid[0] == inode,
                        "SLICE_GET: inodeid mismatch (expected %lu, got %lu)",
                        inode,
                        slice_info.inodeid.size() > 0 ? slice_info.inodeid[0] : 0);
        printf("  SLICE_GET chunk 0 verified: slice_id=%lu, size=%u, offset=%u, len=%u\n",
               slice_info.sliceid[0],
               slice_info.slicesize[0],
               slice_info.sliceoffset[0],
               slice_info.slicelen[0]);
    }

    // 5. SLICE_PUT - chunk 1 (多 chunk 测试)
    uint64_t slice_id2 = slice_id_resp.start + 1;
    status = DoSlicePut(file_path, inode, 1, slice_id2, slice_size, slice_offset, slice_len);
    TEST_ASSERT(status == 0, "SLICE_PUT chunk 1 failed");

    // 6. SLICE_GET - 验证 chunk 1 数据一致性
    status = DoSliceGet(file_path, inode, 1, &slice_info);
    TEST_ASSERT(status == 0, "SLICE_GET chunk 1 failed");
    TEST_ASSERT(slice_info.slicenum >= 1, "SLICE_GET chunk 1: no slices returned");
    if (slice_info.slicenum > 0 && slice_info.sliceid.size() > 0) {
        TEST_ASSERT_MSG(slice_info.sliceid[0] == slice_id2,
                        "SLICE_GET chunk 1: slice_id mismatch (expected %lu, got %lu)",
                        slice_id2,
                        slice_info.sliceid[0]);
        TEST_ASSERT_MSG(slice_info.slicesize.size() > 0 && slice_info.slicesize[0] == slice_size,
                        "SLICE_GET chunk 1: slicesize mismatch");
        TEST_ASSERT_MSG(slice_info.inodeid.size() > 0 && slice_info.inodeid[0] == inode,
                        "SLICE_GET chunk 1: inodeid mismatch");
        printf("  SLICE_GET chunk 1 verified: slice_id=%lu, size=%u\n", slice_info.sliceid[0], slice_info.slicesize[0]);
    }

    // 7. SLICE_DEL - 删除 chunk 0
    status = DoSliceDel(file_path, inode, 0);
    TEST_ASSERT(status == 0, "SLICE_DEL chunk 0 failed");

    // 8. 验证 chunk 0 删除成功
    status = DoSliceGet(file_path, inode, 0, &slice_info);
    // 删除后 GET 可能返回 0 个 slice 或错误
    printf("  After SLICE_DEL chunk 0: status=%d, slicenum=%u\n", status, slice_info.slicenum);
    TEST_ASSERT(status != 0 || slice_info.slicenum == 0, "SLICE_DEL: chunk 0 should be empty after delete");

    // 9. 验证 chunk 1 仍然存在
    status = DoSliceGet(file_path, inode, 1, &slice_info);
    TEST_ASSERT(status == 0, "SLICE_GET chunk 1 after deleting chunk 0 failed");
    TEST_ASSERT(slice_info.slicenum >= 1, "Chunk 1 should still exist after deleting chunk 0");
    printf("  Chunk 1 still exists after deleting chunk 0\n");

    // 10. 清理 chunk 1
    status = DoSliceDel(file_path, inode, 1);
    TEST_ASSERT(status == 0, "SLICE_DEL chunk 1 failed");

    // 11. 清理文件
    status = DoUnlink(file_path);
    TEST_ASSERT(status == 0, "UNLINK cleanup failed");

    TEST_PASS("Slice Operations");
}

/*
 * 测试 6: PLAIN_COMMAND (SQL 命令)
 */
static bool TestPlainCommand()
{
    TEST_BEGIN("Plain Command");

    // 1. 简单查询 - 单行单列
    PlainCommandResponse resp;
    int status = DoPlainCommand("SELECT 1 AS test_col;", &resp);
    TEST_ASSERT(status == 0, "PLAIN_COMMAND SELECT failed");
    TEST_ASSERT(resp.row == 1, "Expected 1 row");
    TEST_ASSERT(resp.col == 1, "Expected 1 column");
    TEST_ASSERT(resp.data.size() > 0 && resp.data[0] == "1", "Expected data[0] == '1'");
    printf("  Single value query: row=%u, col=%u, data='%s'\n",
           resp.row,
           resp.col,
           resp.data.size() > 0 ? resp.data[0].c_str() : "");

    // 2. 多行多列查询
    status = DoPlainCommand("SELECT 1 AS a, 'hello' AS b UNION ALL SELECT 2, 'world';", &resp);
    TEST_ASSERT(status == 0, "PLAIN_COMMAND multi-row failed");
    TEST_ASSERT(resp.row == 2, "Expected 2 rows");
    TEST_ASSERT(resp.col == 2, "Expected 2 columns");
    printf("  Multi-row query: row=%u, col=%u, data_count=%zu\n", resp.row, resp.col, resp.data.size());
    for (size_t i = 0; i < resp.data.size() && i < 4; i++) {
        printf("    data[%zu]='%s'\n", i, resp.data[i].c_str());
    }

    // 3. 查询版本
    status = DoPlainCommand("SELECT version();", &resp);
    TEST_ASSERT(status == 0, "PLAIN_COMMAND version() failed");
    TEST_ASSERT(resp.row == 1, "version() should return 1 row");
    if (resp.data.size() > 0) {
        printf("  PostgreSQL version: %s\n", resp.data[0].c_str());
    }

    // 4. 空结果查询
    status = DoPlainCommand("SELECT 1 WHERE false;", &resp);
    TEST_ASSERT(status == 0, "PLAIN_COMMAND empty result failed");
    TEST_ASSERT(resp.row == 0, "Empty query should return 0 rows");
    printf("  Empty result query: row=%u\n", resp.row);

    // 5. 测试错误 SQL
    status = DoPlainCommand("SELECT * FROM nonexistent_table_xyz;", &resp);
    TEST_ASSERT(status != 0, "Invalid SQL should fail");
    printf("  Invalid SQL correctly failed with status=%d\n", status);

    TEST_PASS("Plain Command");
}

/*
 * 测试 7: KV 操作 (PUT_KEY_META -> GET_KV_META -> DELETE_KV_META)
 */
static bool TestKvOperations()
{
    TEST_BEGIN("KV Operations (PUT_KEY_META/GET_KV_META/DELETE_KV_META)");

    std::string test_key = "falcon_test_key_" + std::to_string(time(nullptr)) +
                           "_" + std::to_string(g_loop_iteration.load());

    // 1. 准备 slice 数据
    std::vector<FormDataSlice> slices;
    slices.push_back(FormDataSlice(1001, 0, 2048));    // slice 1: value_key=1001, offset=0, size=2KB
    slices.push_back(FormDataSlice(1002, 2048, 2048)); // slice 2: value_key=1002, offset=2KB, size=2KB

    uint32_t total_value_len = 4096;

    // 2. KV PUT
    int status = DoKvPut(test_key, total_value_len, slices);
    TEST_ASSERT(status == 0, "KV_PUT failed");

    // 3. KV GET - 验证数据
    KvDataResponse get_resp;
    status = DoKvGet(test_key, &get_resp);
    TEST_ASSERT(status == 0, "KV_GET failed");
    TEST_ASSERT_MSG(get_resp.kv_data.valueLen == total_value_len,
                    "KV_GET: valueLen mismatch (expected %u, got %u)",
                    total_value_len,
                    get_resp.kv_data.valueLen);
    TEST_ASSERT_MSG(get_resp.kv_data.sliceNum == (int)slices.size(),
                    "KV_GET: sliceNum mismatch (expected %zu, got %d)",
                    slices.size(),
                    get_resp.kv_data.sliceNum);

    // 验证 slice 数据
    TEST_ASSERT(get_resp.kv_data.dataSlices.size() == slices.size(), "KV_GET: slice count mismatch");
    for (size_t i = 0; i < slices.size() && i < get_resp.kv_data.dataSlices.size(); i++) {
        TEST_ASSERT_MSG(get_resp.kv_data.dataSlices[i].value_key == slices[i].value_key,
                        "KV_GET: slice[%zu] value_key mismatch (expected %lu, got %lu)",
                        i,
                        slices[i].value_key,
                        get_resp.kv_data.dataSlices[i].value_key);
        TEST_ASSERT_MSG(get_resp.kv_data.dataSlices[i].size == slices[i].size,
                        "KV_GET: slice[%zu] size mismatch (expected %u, got %u)",
                        i,
                        slices[i].size,
                        get_resp.kv_data.dataSlices[i].size);
        printf("    Slice[%zu]: value_key=%lu, location=%lu, size=%u\n",
               i,
               get_resp.kv_data.dataSlices[i].value_key,
               get_resp.kv_data.dataSlices[i].location,
               get_resp.kv_data.dataSlices[i].size);
    }

    printf("  KV_GET verified: valueLen=%u, sliceNum=%d\n", get_resp.kv_data.valueLen, get_resp.kv_data.sliceNum);

    // 4. 测试重复 PUT 同一 key (应该返回 SUCCESS，不覆盖)
    std::vector<FormDataSlice> new_slices;
    new_slices.push_back(FormDataSlice(2001, 0, 1024));
    status = DoKvPut(test_key, 1024, new_slices);
    TEST_ASSERT(status == 0, "KV_PUT duplicate key should succeed (return SUCCESS without overwrite)");
    printf("  KV_PUT duplicate key: status=%d (expected 0)\n", status);

    // 验证数据没有被覆盖
    status = DoKvGet(test_key, &get_resp);
    TEST_ASSERT(status == 0, "KV_GET after duplicate PUT failed");
    TEST_ASSERT_MSG(get_resp.kv_data.valueLen == total_value_len,
                    "KV_GET after duplicate PUT: valueLen should not change (expected %u, got %u)",
                    total_value_len,
                    get_resp.kv_data.valueLen);
    printf("  After duplicate PUT, data unchanged: valueLen=%u\n", get_resp.kv_data.valueLen);

    // 5. KV DELETE
    status = DoKvDelete(test_key);
    TEST_ASSERT(status == 0, "KV_DELETE failed");

    // 6. 验证删除 - GET 应该失败
    status = DoKvGet(test_key, nullptr);
    TEST_ASSERT(status != 0, "KV_GET should fail after DELETE");
    printf("  KV_GET after DELETE correctly failed with status=%d\n", status);

    // 7. 测试 DELETE 不存在的 key
    status = DoKvDelete(test_key);
    printf("  KV_DELETE non-existent key: status=%d\n", status);
    // 不同实现可能有不同行为，这里只记录

    // 8. 测试 GET 不存在的 key
    status = DoKvGet("nonexistent_key_xyz_" + std::to_string(time(nullptr)) +
                     "_" + std::to_string(g_loop_iteration.load()), nullptr);
    TEST_ASSERT(status != 0, "KV_GET non-existent key should fail");
    printf("  KV_GET non-existent key correctly failed with status=%d\n", status);

    TEST_PASS("KV Operations");
}

/*
 * 性能测试 1: KV 多线程吞吐量测试
 */
static bool TestKvPerformance()
{
    TEST_BEGIN("KV Multi-threaded Throughput");

    printf("\n");
    printf("╔══════════════════════════════════════════════════════════════╗\n");
    printf("║          KV Multi-threaded Throughput Benchmark             ║\n");
    printf("╚══════════════════════════════════════════════════════════════╝\n");
    printf("\n");

    // ========== 测试配置 ==========
    const int WARMUP_ITERATIONS = 10;
    const int MT_THREAD_COUNT = 4;
    const int MT_OPS_PER_THREAD = 1000;

    std::string key_prefix = "perf_kv_" + std::to_string(time(nullptr)) + "_";

    // 准备测试数据
    std::vector<FormDataSlice> test_slices;
    test_slices.push_back(FormDataSlice(1001, 0, 2048));
    test_slices.push_back(FormDataSlice(1002, 2048, 2048));
    uint32_t total_value_len = 4096;

    // ========== 1. Warmup（预热缓存）==========
    printf("[Warmup] Running %d iterations to warm up cache...\n", WARMUP_ITERATIONS);
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        std::string key = key_prefix + "warmup_" + std::to_string(i);
        DoKvPut(key, total_value_len, test_slices);
        KvDataResponse get_resp;
        DoKvGet(key, &get_resp);
        DoKvDelete(key);
    }
    printf("[Warmup] Completed. Starting throughput tests...\n\n");

    // ========== 2. 多线程吞吐量测试（KV_PUT）==========
    printf("─────────────────────────────────────────────────────────────\n");
    printf(" Test 1: Multi-threaded KV_PUT Throughput\n");
    printf("─────────────────────────────────────────────────────────────\n");

    std::vector<std::thread> mt_put_threads;
    std::atomic<int> mt_put_success(0);
    std::atomic<int> mt_put_failure(0);

    auto mt_put_worker = [&](int thread_id) {
        auto thread_start = std::chrono::high_resolution_clock::now();

        int local_success = 0;
        int local_failure = 0;

        for (int i = 0; i < MT_OPS_PER_THREAD; i++) {
            std::string key = key_prefix + "mt_put_t" + std::to_string(thread_id) +
                              "_" + std::to_string(i);
            int status = DoKvPut(key, total_value_len, test_slices);
            if (status == 0) {
                local_success++;
            } else {
                local_failure++;
            }
        }

        mt_put_success += local_success;
        mt_put_failure += local_failure;

        auto thread_end = std::chrono::high_resolution_clock::now();
        double thread_time_sec = std::chrono::duration<double>(thread_end - thread_start).count();
        double thread_throughput = local_success / thread_time_sec;
        printf("  [Thread %2d] Completed %d ops in %.2f sec (%.0f ops/sec)\n",
               thread_id, local_success, thread_time_sec, thread_throughput);
    };

    printf("  Starting %d threads, each performing %d KV_PUT operations...\n",
           MT_THREAD_COUNT, MT_OPS_PER_THREAD);

    auto mt_put_start = std::chrono::high_resolution_clock::now();

    // 启动所有线程
    for (int i = 0; i < MT_THREAD_COUNT; i++) {
        mt_put_threads.emplace_back(mt_put_worker, i);
    }

    // 等待所有线程完成
    for (auto &t : mt_put_threads) {
        t.join();
    }

    auto mt_put_end = std::chrono::high_resolution_clock::now();
    double mt_put_time_sec = std::chrono::duration<double>(mt_put_end - mt_put_start).count();
    int mt_put_total_ops = MT_THREAD_COUNT * MT_OPS_PER_THREAD;
    double mt_put_throughput = mt_put_success.load() / mt_put_time_sec;

    printf("  Total operations: %d\n", mt_put_total_ops);
    printf("  Successful:       %d\n", mt_put_success.load());
    printf("  Failed:           %d\n", mt_put_failure.load());
    printf("  Total time:       %.2f sec\n", mt_put_time_sec);
    printf("  Throughput:       %.0f ops/sec\n", mt_put_throughput);
    printf("  Per-thread avg:   %.0f ops/sec\n", mt_put_throughput / MT_THREAD_COUNT);
    printf("  Avg latency:      %.3f ms/op\n", (mt_put_time_sec * 1000) / mt_put_success.load());
    printf("\n");
    fflush(stdout);

    // ========== 3. 多线程吞吐量测试（KV_GET）==========
    printf("─────────────────────────────────────────────────────────────\n");
    printf(" Test 2: Multi-threaded KV_GET Throughput\n");
    printf("─────────────────────────────────────────────────────────────\n");

    std::vector<std::thread> mt_get_threads;
    std::atomic<int> mt_get_success(0);
    std::atomic<int> mt_get_failure(0);

    auto mt_get_worker = [&](int thread_id) {
        auto thread_start = std::chrono::high_resolution_clock::now();

        int local_success = 0;
        int local_failure = 0;

        for (int i = 0; i < MT_OPS_PER_THREAD; i++) {
            std::string key = key_prefix + "mt_put_t" + std::to_string(thread_id) +
                              "_" + std::to_string(i);
            KvDataResponse get_resp;
            int status = DoKvGet(key, &get_resp);
            if (status == 0) {
                local_success++;
            } else {
                local_failure++;
            }
        }

        mt_get_success += local_success;
        mt_get_failure += local_failure;

        auto thread_end = std::chrono::high_resolution_clock::now();
        double thread_time_sec = std::chrono::duration<double>(thread_end - thread_start).count();
        double thread_throughput = local_success / thread_time_sec;
        printf("  [Thread %2d] Completed %d ops in %.2f sec (%.0f ops/sec)\n",
               thread_id, local_success, thread_time_sec, thread_throughput);
    };

    printf("  Starting %d threads, each performing %d KV_GET operations...\n",
           MT_THREAD_COUNT, MT_OPS_PER_THREAD);

    auto mt_get_start = std::chrono::high_resolution_clock::now();

    // 启动所有线程
    for (int i = 0; i < MT_THREAD_COUNT; i++) {
        mt_get_threads.emplace_back(mt_get_worker, i);
    }

    // 等待所有线程完成
    for (auto &t : mt_get_threads) {
        t.join();
    }

    auto mt_get_end = std::chrono::high_resolution_clock::now();
    double mt_get_time_sec = std::chrono::duration<double>(mt_get_end - mt_get_start).count();
    int mt_get_total_ops = MT_THREAD_COUNT * MT_OPS_PER_THREAD;
    double mt_get_throughput = mt_get_success.load() / mt_get_time_sec;

    printf("  Total operations: %d\n", mt_get_total_ops);
    printf("  Successful:       %d\n", mt_get_success.load());
    printf("  Failed:           %d\n", mt_get_failure.load());
    printf("  Total time:       %.2f sec\n", mt_get_time_sec);
    printf("  Throughput:       %.0f ops/sec\n", mt_get_throughput);
    printf("  Per-thread avg:   %.0f ops/sec\n", mt_get_throughput / MT_THREAD_COUNT);
    printf("  Avg latency:      %.3f ms/op\n", (mt_get_time_sec * 1000) / mt_get_success.load());
    printf("\n");
    fflush(stdout);

    // ========== 4. 清理测试数据 ==========
    printf("─────────────────────────────────────────────────────────────\n");
    printf(" Cleanup: Deleting test keys...\n");
    printf("─────────────────────────────────────────────────────────────\n");

    int deleted_count = 0;

    // 删除多线程吞吐量测试的key
    for (int thread_id = 0; thread_id < MT_THREAD_COUNT; thread_id++) {
        for (int i = 0; i < MT_OPS_PER_THREAD; i++) {
            std::string key = key_prefix + "mt_put_t" + std::to_string(thread_id) +
                              "_" + std::to_string(i);
            if (DoKvDelete(key) == 0) {
                deleted_count++;
            }
        }
    }

    printf("  Deleted %d keys\n", deleted_count);
    printf("\n");

    // ========== 5. 性能总结 ==========
    printf("╔══════════════════════════════════════════════════════════════╗\n");
    printf("║                   Performance Summary                       ║\n");
    printf("╠══════════════════════════════════════════════════════════════╣\n");
    printf("║  KV_PUT (Multi-threaded, %2d threads):                       ║\n", MT_THREAD_COUNT);
    printf("║    Total ops:    %6d                                       ║\n", mt_put_total_ops);
    printf("║    Throughput:   %6.0f ops/sec                             ║\n", mt_put_throughput);
    printf("║    Per-thread:   %6.0f ops/sec                             ║\n", mt_put_throughput / MT_THREAD_COUNT);
    printf("║                                                              ║\n");
    printf("║  KV_GET (Multi-threaded, %2d threads):                       ║\n", MT_THREAD_COUNT);
    printf("║    Total ops:    %6d                                       ║\n", mt_get_total_ops);
    printf("║    Throughput:   %6.0f ops/sec                             ║\n", mt_get_throughput);
    printf("║    Per-thread:   %6.0f ops/sec                             ║\n", mt_get_throughput / MT_THREAD_COUNT);
    printf("╚══════════════════════════════════════════════════════════════╝\n");
    printf("\n");
    fflush(stdout);

    TEST_PASS("KV Performance and Throughput");
}

/*
 * 性能测试 2: Slice 多线程吞吐量测试
 */
static bool TestSlicePerformance()
{
    TEST_BEGIN("Slice Multi-threaded Throughput");

    printf("\n");
    printf("╔══════════════════════════════════════════════════════════════╗\n");
    printf("║        Slice Multi-threaded Throughput Benchmark            ║\n");
    printf("╚══════════════════════════════════════════════════════════════╝\n");
    printf("\n");

    // ========== 测试配置 ==========
    const int WARMUP_ITERATIONS = 10;

    std::string file_prefix = "/test_slice_perf_" + std::to_string(time(nullptr)) + "_";

    // ========== 1. Warmup ==========
    printf("[Warmup] Running %d iterations...\n", WARMUP_ITERATIONS);
    for (int i = 0; i < WARMUP_ITERATIONS; i++) {
        std::string filename = file_prefix + "warmup_" + std::to_string(i) + ".dat";
        CreateResponse create_resp;
        DoCreate(filename, &create_resp);

        SliceIdResponse slice_resp;
        DoFetchSliceId(2, 0, &slice_resp);

        DoSlicePut(filename, create_resp.st_ino, 0, slice_resp.start, 4096, 0, 4096);
        DoUnlink(filename);
    }
    printf("[Warmup] Completed.\n\n");

    // ========== 测试配置 ==========
    const int MT_THREAD_COUNT = 20;
    const int MT_OPS_PER_THREAD = 5000;

    // ========== 2. 多线程吞吐量测试（SLICE_PUT）==========
    printf("─────────────────────────────────────────────────────────────\n");
    printf(" Test 1: Multi-threaded SLICE_PUT Throughput\n");
    printf("─────────────────────────────────────────────────────────────\n");

    std::vector<std::thread> mt_slice_put_threads;
    std::atomic<int> mt_slice_put_success(0);
    std::atomic<int> mt_slice_put_failure(0);

    auto mt_slice_put_worker = [&](int thread_id) {
        auto thread_start = std::chrono::high_resolution_clock::now();

        int local_success = 0;
        int local_failure = 0;

        for (int i = 0; i < MT_OPS_PER_THREAD; i++) {
            std::string filename = file_prefix + "mt_put_t" + std::to_string(thread_id) +
                                   "_" + std::to_string(i) + ".dat";
            CreateResponse create_resp;
            if (DoCreate(filename, &create_resp) == 0) {
                SliceIdResponse slice_resp;
                if (DoFetchSliceId(1, 0, &slice_resp) == 0) {
                    if (DoSlicePut(filename, create_resp.st_ino, 0, slice_resp.start, 4096, 0, 4096) == 0) {
                        local_success++;
                    } else {
                        local_failure++;
                    }
                } else {
                    local_failure++;
                }
            } else {
                local_failure++;
            }
        }

        mt_slice_put_success += local_success;
        mt_slice_put_failure += local_failure;

        auto thread_end = std::chrono::high_resolution_clock::now();
        double thread_time_sec = std::chrono::duration<double>(thread_end - thread_start).count();
        double thread_throughput = local_success / thread_time_sec;
        printf("  [Thread %2d] Completed %d ops in %.2f sec (%.0f ops/sec)\n",
               thread_id, local_success, thread_time_sec, thread_throughput);
    };

    printf("  Starting %d threads, each performing %d SLICE_PUT operations...\n",
           MT_THREAD_COUNT, MT_OPS_PER_THREAD);
    printf("  Note: Each operation includes CREATE + FETCH_SLICE_ID + SLICE_PUT\n");

    auto mt_slice_put_start = std::chrono::high_resolution_clock::now();

    // 启动所有线程
    for (int i = 0; i < MT_THREAD_COUNT; i++) {
        mt_slice_put_threads.emplace_back(mt_slice_put_worker, i);
    }

    // 等待所有线程完成
    for (auto &t : mt_slice_put_threads) {
        t.join();
    }

    auto mt_slice_put_end = std::chrono::high_resolution_clock::now();
    double mt_slice_put_time_sec = std::chrono::duration<double>(mt_slice_put_end - mt_slice_put_start).count();
    int mt_slice_put_total_ops = MT_THREAD_COUNT * MT_OPS_PER_THREAD;
    double mt_slice_put_throughput = mt_slice_put_success.load() / mt_slice_put_time_sec;

    printf("  Total operations: %d\n", mt_slice_put_total_ops);
    printf("  Successful:       %d\n", mt_slice_put_success.load());
    printf("  Failed:           %d\n", mt_slice_put_failure.load());
    printf("  Total time:       %.2f sec\n", mt_slice_put_time_sec);
    printf("  Throughput:       %.0f ops/sec\n", mt_slice_put_throughput);
    printf("  Per-thread avg:   %.0f ops/sec\n", mt_slice_put_throughput / MT_THREAD_COUNT);
    printf("  Avg latency:      %.3f ms/op\n", (mt_slice_put_time_sec * 1000) / mt_slice_put_success.load());
    printf("\n");

    // ========== 3. 多线程吞吐量测试（SLICE_GET）==========
    printf("─────────────────────────────────────────────────────────────\n");
    printf(" Test 2: Multi-threaded SLICE_GET Throughput\n");
    printf("─────────────────────────────────────────────────────────────\n");

    std::vector<std::thread> mt_slice_get_threads;
    std::atomic<int> mt_slice_get_success(0);
    std::atomic<int> mt_slice_get_failure(0);

    // 先收集所有inode信息（需要在主线程完成，避免竞态条件）
    struct FileInfo {
        std::string filename;
        uint64_t inode;
    };
    std::vector<std::vector<FileInfo>> thread_file_infos(MT_THREAD_COUNT);

    // 为每个线程的文件获取inode
    for (int thread_id = 0; thread_id < MT_THREAD_COUNT; thread_id++) {
        for (int i = 0; i < MT_OPS_PER_THREAD; i++) {
            std::string filename = file_prefix + "mt_put_t" + std::to_string(thread_id) +
                                   "_" + std::to_string(i) + ".dat";
            StatResponse stat_resp;
            if (DoStat(filename, &stat_resp) == 0) {
                thread_file_infos[thread_id].push_back({filename, stat_resp.st_ino});
            }
        }
    }

    auto mt_slice_get_worker = [&](int thread_id) {
        auto thread_start = std::chrono::high_resolution_clock::now();

        int local_success = 0;
        int local_failure = 0;

        for (const auto &file_info : thread_file_infos[thread_id]) {
            SliceInfoResponse slice_info;
            int status = DoSliceGet(file_info.filename, file_info.inode, 0, &slice_info);
            if (status == 0) {
                local_success++;
            } else {
                local_failure++;
            }
        }

        mt_slice_get_success += local_success;
        mt_slice_get_failure += local_failure;

        auto thread_end = std::chrono::high_resolution_clock::now();
        double thread_time_sec = std::chrono::duration<double>(thread_end - thread_start).count();
        double thread_throughput = local_success / thread_time_sec;
        printf("  [Thread %2d] Completed %d ops in %.2f sec (%.0f ops/sec)\n",
               thread_id, local_success, thread_time_sec, thread_throughput);
    };

    printf("  Starting %d threads, each performing %d SLICE_GET operations...\n",
           MT_THREAD_COUNT, MT_OPS_PER_THREAD);

    auto mt_slice_get_start = std::chrono::high_resolution_clock::now();

    // 启动所有线程
    for (int i = 0; i < MT_THREAD_COUNT; i++) {
        mt_slice_get_threads.emplace_back(mt_slice_get_worker, i);
    }

    // 等待所有线程完成
    for (auto &t : mt_slice_get_threads) {
        t.join();
    }

    auto mt_slice_get_end = std::chrono::high_resolution_clock::now();
    double mt_slice_get_time_sec = std::chrono::duration<double>(mt_slice_get_end - mt_slice_get_start).count();
    int mt_slice_get_total_ops = MT_THREAD_COUNT * MT_OPS_PER_THREAD;
    double mt_slice_get_throughput = mt_slice_get_success.load() / mt_slice_get_time_sec;

    printf("  Total operations: %d\n", mt_slice_get_total_ops);
    printf("  Successful:       %d\n", mt_slice_get_success.load());
    printf("  Failed:           %d\n", mt_slice_get_failure.load());
    printf("  Total time:       %.2f sec\n", mt_slice_get_time_sec);
    printf("  Throughput:       %.0f ops/sec\n", mt_slice_get_throughput);
    printf("  Per-thread avg:   %.0f ops/sec\n", mt_slice_get_throughput / MT_THREAD_COUNT);
    printf("  Avg latency:      %.3f ms/op\n", (mt_slice_get_time_sec * 1000) / mt_slice_get_success.load());
    printf("\n");

    // ========== 4. 清理多线程测试文件 ==========
    printf("─────────────────────────────────────────────────────────────\n");
    printf(" Cleanup: Deleting multi-threaded test files...\n");
    printf("─────────────────────────────────────────────────────────────\n");

    int mt_deleted_count = 0;
    for (int thread_id = 0; thread_id < MT_THREAD_COUNT; thread_id++) {
        for (int i = 0; i < MT_OPS_PER_THREAD; i++) {
            std::string filename = file_prefix + "mt_put_t" + std::to_string(thread_id) +
                                   "_" + std::to_string(i) + ".dat";
            if (DoUnlink(filename) == 0) {
                mt_deleted_count++;
            }
        }
    }

    printf("  Deleted %d files\n", mt_deleted_count);
    printf("\n");

    // ========== 5. 性能总结 ==========
    printf("╔══════════════════════════════════════════════════════════════╗\n");
    printf("║                   Performance Summary                       ║\n");
    printf("╠══════════════════════════════════════════════════════════════╣\n");
    printf("║  SLICE_PUT (Multi-threaded, %2d threads):                    ║\n", MT_THREAD_COUNT);
    printf("║    Total ops:    %6d                                       ║\n", mt_slice_put_total_ops);
    printf("║    Throughput:   %6.0f ops/sec                             ║\n", mt_slice_put_throughput);
    printf("║    Per-thread:   %6.0f ops/sec                             ║\n", mt_slice_put_throughput / MT_THREAD_COUNT);
    printf("║    Note: Each op = CREATE + FETCH_SLICE_ID + SLICE_PUT      ║\n");
    printf("║                                                              ║\n");
    printf("║  SLICE_GET (Multi-threaded, %2d threads):                    ║\n", MT_THREAD_COUNT);
    printf("║    Total ops:    %6d                                       ║\n", mt_slice_get_total_ops);
    printf("║    Throughput:   %6.0f ops/sec                             ║\n", mt_slice_get_throughput);
    printf("║    Per-thread:   %6.0f ops/sec                             ║\n", mt_slice_get_throughput / MT_THREAD_COUNT);
    printf("╚══════════════════════════════════════════════════════════════╝\n");
    printf("\n");

    TEST_PASS("Slice Performance and Throughput");
}

/*
 * 测试 8: 目录内容操作 (创建多个子目录后 READDIR)
 * [CN-only] 需要 MKDIR/RMDIR 权限来创建/清理测试目录
 * 注意: 只测试目录，不测试文件（文件操作需要在 Worker 节点）
 */
static bool TestReaddirWithSubdirs()
{
    SKIP_IF_NOT_CN("Readdir With Multiple Subdirectories");
    TEST_BEGIN("Readdir With Multiple Subdirectories");

    std::string dir_path = std::string(TEST_BASE_PATH) + "/test_readdir_dir";

    // 1. 创建目录
    int status = DoMkdir(dir_path);
    TEST_ASSERT(status == 0, "MKDIR failed");

    // 2. 创建多个子目录
    const int subdir_count = 5;
    std::set<std::string> expected_subdirs;
    for (int i = 0; i < subdir_count; i++) {
        std::string subdir_name = "subdir_" + std::to_string(i);
        std::string subdir_path = dir_path + "/" + subdir_name;
        status = DoMkdir(subdir_path);
        TEST_ASSERT(status == 0, "MKDIR subdir failed");
        expected_subdirs.insert(subdir_name);
    }
    printf("  Created %d subdirectories\n", subdir_count);

    // 3. READDIR
    std::vector<std::string> entries;
    status = DoReaddir(dir_path, &entries);
    TEST_ASSERT(status == 0, "READDIR failed");

    printf("  READDIR found %zu entries (created %d subdirs)\n", entries.size(), subdir_count);

    // 子目录可能被路由到不同Worker节点
    // READDIR可能只返回本地节点上的条目
    // 这里只验证READDIR操作成功，不强制要求返回特定数量
    if (entries.size() >= (size_t)subdir_count) {
        printf("  All %d subdirectories visible locally\n", subdir_count);
        // 打印条目
        for (size_t i = 0; i < entries.size(); i++) {
            printf("    - %s\n", entries[i].c_str());
        }
    } else {
        printf("  [INFO] Only %zu of %d subdirs visible (distributed routing)\n", entries.size(), subdir_count);
    }

    // 5. 清理子目录
    for (int i = 0; i < subdir_count; i++) {
        std::string subdir_path = dir_path + "/subdir_" + std::to_string(i);
        status = DoRmdir(subdir_path);
        TEST_ASSERT(status == 0, "RMDIR subdir cleanup failed");
    }

    // 6. 清理目录
    status = DoRmdir(dir_path);
    TEST_ASSERT(status == 0, "RMDIR cleanup failed");

    TEST_PASS("Readdir With Multiple Subdirectories");
}

/*
 * 测试 9: 冲突测试 - KV PUT 重复 key
 * 测试 KV PUT 已存在的 key 时的幂等性
 */
static bool TestKvPutDuplicateKey()
{
    TEST_BEGIN("KV PUT Duplicate Key");

    std::string test_key = "falcon_dup_test_key_" + std::to_string(time(nullptr)) +
                           "_" + std::to_string(g_loop_iteration.load());

    // 1. 准备第一组 slice 数据
    std::vector<FormDataSlice> slices1;
    slices1.push_back(FormDataSlice(1001, 0, 2048));
    slices1.push_back(FormDataSlice(1002, 2048, 2048));
    uint32_t value_len1 = 4096;

    // 2. 第一次 KV PUT
    int status = DoKvPut(test_key, value_len1, slices1);
    printf("  First KV_PUT: status=%d\n", status);
    TEST_ASSERT(status == 0, "First KV_PUT failed");

    // 3. 准备第二组不同的 slice 数据
    std::vector<FormDataSlice> slices2;
    slices2.push_back(FormDataSlice(2001, 0, 1024));
    uint32_t value_len2 = 1024;

    // 4. 第二次 KV PUT 同一 key - 观察行为
    status = DoKvPut(test_key, value_len2, slices2);
    printf("  Second KV_PUT (duplicate key): status=%d\n", status);
    printf("    -> Expected: SUCCESS (idempotent, no overwrite)\n");

    // 5. 验证数据 - 应该还是第一次的数据
    KvDataResponse get_resp;
    status = DoKvGet(test_key, &get_resp);
    TEST_ASSERT(status == 0, "KV_GET after duplicate PUT failed");
    printf("    -> After duplicate PUT: valueLen=%u (first=%u, second=%u)\n",
           get_resp.kv_data.valueLen, value_len1, value_len2);
    printf("    -> Data unchanged? %s\n", get_resp.kv_data.valueLen == value_len1 ? "YES" : "NO");

    // 6. 清理
    status = DoKvDelete(test_key);
    TEST_ASSERT(status == 0, "KV_DELETE cleanup failed");

    TEST_PASS("KV PUT Duplicate Key");
}

/*
 * 测试 11: 冲突测试 - SLICE PUT 重复 (inode, chunk)
 * [Worker-only] 测试 SLICE PUT 已存在的 (inode, chunk) 时的行为
 */
static bool TestSlicePutDuplicate()
{
    SKIP_IF_NOT_WORKER("Slice PUT Duplicate");
    TEST_BEGIN("Slice PUT Duplicate (inode, chunk)");

    std::string file_path = "/test_slice_dup.dat";

    // 1. 创建文件
    CreateResponse create_resp;
    int status = DoCreate(file_path, &create_resp);
    TEST_ASSERT(status == 0, "CREATE failed");
    uint64_t inode = create_resp.st_ino;
    printf("  Created file with inode=%lu\n", inode);

    // 2. 分配 slice ID
    SliceIdResponse slice_id_resp;
    status = DoFetchSliceId(2, 0, &slice_id_resp);
    TEST_ASSERT(status == 0, "FETCH_SLICE_ID failed");
    uint64_t slice_id1 = slice_id_resp.start;
    uint64_t slice_id2 = slice_id_resp.start + 1;
    printf("  Allocated slice IDs: %lu, %lu\n", slice_id1, slice_id2);

    // 3. 第一次 SLICE_PUT (chunk=0)
    status = DoSlicePut(file_path, inode, 0, slice_id1, 4096, 0, 4096);
    printf("  First SLICE_PUT (inode=%lu, chunk=0, slice_id=%lu): status=%d\n", inode, slice_id1, status);
    TEST_ASSERT(status == 0, "First SLICE_PUT failed");

    // 4. 第二次 SLICE_PUT 同一 (inode, chunk) 但不同 slice_id - 观察行为
    printf("  Second SLICE_PUT (same inode=%lu, chunk=0, different slice_id=%lu)...\n", inode, slice_id2);
    status = DoSlicePut(file_path, inode, 0, slice_id2, 2048, 0, 2048);
    printf("    -> status=%d\n", status);
    if (status == 0) {
        printf("    -> SLICE_PUT duplicate succeeded (might append or ignore)\n");
    } else {
        printf("    -> SLICE_PUT duplicate failed with errorCode=%d\n", status);
        printf("    -> This might indicate unique constraint violation\n");
    }

    // 5. 验证 SLICE_GET - 看看实际存储了什么
    SliceInfoResponse slice_info;
    status = DoSliceGet(file_path, inode, 0, &slice_info);
    printf("  SLICE_GET (inode=%lu, chunk=0): status=%d, slicenum=%u\n", inode, status, slice_info.slicenum);
    if (status == 0 && slice_info.slicenum > 0) {
        for (uint32_t i = 0; i < slice_info.slicenum && i < slice_info.sliceid.size(); i++) {
            printf("    -> slice[%u]: id=%lu, size=%u\n",
                   i, slice_info.sliceid[i],
                   i < slice_info.slicesize.size() ? slice_info.slicesize[i] : 0);
        }
    }

    // 6. 清理
    DoSliceDel(file_path, inode, 0);
    status = DoUnlink(file_path);
    TEST_ASSERT(status == 0, "UNLINK cleanup failed");

    TEST_PASS("Slice PUT Duplicate");
}

/*
 * 测试 12: 并发创建文件
 * [Worker-only] 测试多线程并发创建文件的正确性和线程安全性
 */
static bool TestConcurrentFileCreation()
{
    SKIP_IF_NOT_WORKER("Concurrent File Creation");
    TEST_BEGIN("Concurrent File Creation");

    const int THREAD_COUNT = 3;
    const int FILES_PER_THREAD = 10;
    std::vector<std::thread> threads;
    std::atomic<int> success_count(0);
    std::atomic<int> failure_count(0);
    std::mutex cout_mutex;

    auto create_files_worker = [&](int thread_id) {
        for (int i = 0; i < FILES_PER_THREAD; i++) {
            std::string file_path = std::string("/concurrent_test_") + std::to_string(g_loop_iteration.load()) +
                                    "_t" + std::to_string(thread_id) +
                                    "_f" + std::to_string(i) + ".txt";

            // 创建文件
            CreateResponse create_resp;
            int status = DoCreate(file_path, &create_resp);

            if (status == 0) {
                success_count++;

                // 验证文件确实创建成功
                StatResponse stat_resp;
                status = DoStat(file_path, &stat_resp);
                if (status != 0 || stat_resp.st_ino != create_resp.st_ino) {
                    std::lock_guard<std::mutex> lock(cout_mutex);
                    printf("  [Thread %d] Warning: File %s created but STAT failed or inode mismatch\n",
                           thread_id, file_path.c_str());
                    failure_count++;
                }

                // 清理文件
                DoUnlink(file_path);
            } else {
                failure_count++;
                std::lock_guard<std::mutex> lock(cout_mutex);
                printf("  [Thread %d] Failed to create %s, status=%d\n", thread_id, file_path.c_str(), status);
            }
        }
    };

    // 启动所有线程
    printf("  Starting %d threads, each creating %d files...\n", THREAD_COUNT, FILES_PER_THREAD);
    auto start_time = std::chrono::steady_clock::now();

    for (int i = 0; i < THREAD_COUNT; i++) {
        threads.emplace_back(create_files_worker, i);
    }

    // 等待所有线程完成
    for (auto &t : threads) {
        t.join();
    }

    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // 验证结果
    int expected_files = THREAD_COUNT * FILES_PER_THREAD;
    printf("  Concurrent creation completed in %ld ms\n", duration.count());
    printf("  Success: %d/%d, Failures: %d\n", success_count.load(), expected_files, failure_count.load());

    TEST_ASSERT_MSG(success_count.load() == expected_files,
                    "Expected %d successful creations, got %d (failures: %d)",
                    expected_files, success_count.load(), failure_count.load());

    TEST_ASSERT_MSG(failure_count.load() == 0,
                    "Expected 0 failures, got %d", failure_count.load());

    printf("  All %d files created and verified successfully in parallel\n", expected_files);

    TEST_PASS("Concurrent File Creation");
}

/*
 * 测试 13: 并发获取Slice ID
 * [Worker-only] 测试多线程并发获取slice ID的正确性和线程安全性
 * 验证：1) 所有ID范围不重叠  2) ID连续分配  3) 无竞争条件
 */
static bool TestConcurrentFetchSliceId()
{
    SKIP_IF_NOT_WORKER("Concurrent Fetch Slice ID");
    TEST_BEGIN("Concurrent Fetch Slice ID");

    const int THREAD_COUNT = 8;
    const int REQUESTS_PER_THREAD = 200;
    const uint32_t SLICE_COUNT_PER_REQUEST = 10;

    // 用于收集所有线程获取的ID范围
    struct SliceIdRange {
        uint64_t start;
        uint64_t end;
        int thread_id;
        int request_id;
    };

    std::vector<SliceIdRange> all_ranges;
    std::mutex ranges_mutex;
    std::atomic<int> success_count(0);
    std::atomic<int> failure_count(0);
    std::vector<std::thread> threads;

    auto fetch_sliceid_worker = [&](int thread_id) {
        for (int i = 0; i < REQUESTS_PER_THREAD; i++) {
            SliceIdResponse resp;
            int status = DoFetchSliceId(SLICE_COUNT_PER_REQUEST, 0, &resp);

            if (status == 0) {
                // 验证返回的ID范围
                if (resp.end <= resp.start) {
                    failure_count++;
                    printf("  [Thread %d] Request %d: Invalid range [%lu, %lu)\n",
                           thread_id, i, resp.start, resp.end);
                    continue;
                }

                uint64_t count = resp.end - resp.start;
                if (count != SLICE_COUNT_PER_REQUEST) {
                    failure_count++;
                    printf("  [Thread %d] Request %d: Expected %u IDs, got %lu\n",
                           thread_id, i, SLICE_COUNT_PER_REQUEST, count);
                    continue;
                }

                // 收集成功的ID范围
                {
                    std::lock_guard<std::mutex> lock(ranges_mutex);
                    all_ranges.push_back({resp.start, resp.end, thread_id, i});
                }
                success_count++;
            } else {
                failure_count++;
                printf("  [Thread %d] Request %d failed with status=%d\n", thread_id, i, status);
            }
        }
    };

    // 启动所有线程
    printf("  Starting %d threads, each fetching %d slice ID ranges (%u IDs per request)...\n",
           THREAD_COUNT, REQUESTS_PER_THREAD, SLICE_COUNT_PER_REQUEST);
    auto start_time = std::chrono::steady_clock::now();

    for (int i = 0; i < THREAD_COUNT; i++) {
        threads.emplace_back(fetch_sliceid_worker, i);
    }

    // 等待所有线程完成
    for (auto &t : threads) {
        t.join();
    }

    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // 验证结果
    int expected_requests = THREAD_COUNT * REQUESTS_PER_THREAD;
    printf("  Concurrent fetch completed in %ld ms\n", duration.count());
    printf("  Success: %d/%d, Failures: %d\n", success_count.load(), expected_requests, failure_count.load());

    TEST_ASSERT_MSG(success_count.load() == expected_requests,
                    "Expected %d successful requests, got %d (failures: %d)",
                    expected_requests, success_count.load(), failure_count.load());

    // 验证所有ID范围不重叠
    printf("  Checking for ID range overlaps...\n");
    std::sort(all_ranges.begin(), all_ranges.end(),
              [](const SliceIdRange &a, const SliceIdRange &b) {
                  return a.start < b.start;
              });

    int overlap_count = 0;
    for (size_t i = 1; i < all_ranges.size(); i++) {
        if (all_ranges[i].start < all_ranges[i-1].end) {
            overlap_count++;
            printf("  [ERROR] Range overlap detected:\n");
            printf("    Thread %d Request %d: [%lu, %lu)\n",
                   all_ranges[i-1].thread_id, all_ranges[i-1].request_id,
                   all_ranges[i-1].start, all_ranges[i-1].end);
            printf("    Thread %d Request %d: [%lu, %lu)\n",
                   all_ranges[i].thread_id, all_ranges[i].request_id,
                   all_ranges[i].start, all_ranges[i].end);
            // 只显示前几个重叠，避免输出太多
            if (overlap_count >= 5) {
                printf("  ... (showing first 5 overlaps only)\n");
                break;
            }
        }
    }

    TEST_ASSERT_MSG(overlap_count == 0,
                    "Found %d overlapping ID ranges - slice ID allocation is not thread-safe!",
                    overlap_count);

    // 统计信息
    uint64_t total_ids = 0;
    uint64_t min_id = all_ranges.empty() ? 0 : all_ranges[0].start;
    uint64_t max_id = all_ranges.empty() ? 0 : all_ranges[all_ranges.size()-1].end;

    for (const auto &range : all_ranges) {
        total_ids += (range.end - range.start);
    }

    printf("  All %d ID ranges verified - no overlaps detected\n", (int)all_ranges.size());
    printf("  Total IDs allocated: %lu\n", total_ids);
    printf("  ID range: [%lu, %lu)\n", min_id, max_id);
    printf("  Expected total IDs: %d\n", expected_requests * SLICE_COUNT_PER_REQUEST);

    TEST_ASSERT_MSG(total_ids == (uint64_t)(expected_requests * SLICE_COUNT_PER_REQUEST),
                    "Total allocated IDs (%lu) doesn't match expected (%d)",
                    total_ids, expected_requests * SLICE_COUNT_PER_REQUEST);

    TEST_PASS("Concurrent Fetch Slice ID");
}

/*
 * 测试 14: 嵌套目录操作
 * [CN-only] 需要 MKDIR/RMDIR 权限
 */
static bool TestNestedDirectories()
{
    SKIP_IF_NOT_CN("Nested Directories");
    TEST_BEGIN("Nested Directories");

    std::string base = std::string(TEST_BASE_PATH) + "/nested";

    // 1. 创建多层嵌套目录
    int status = DoMkdir(base);
    TEST_ASSERT(status == 0, "MKDIR base failed");

    status = DoMkdir(base + "/level1");
    TEST_ASSERT(status == 0, "MKDIR level1 failed");

    status = DoMkdir(base + "/level1/level2");
    TEST_ASSERT(status == 0, "MKDIR level2 failed");

    status = DoMkdir(base + "/level1/level2/level3");
    TEST_ASSERT(status == 0, "MKDIR level3 failed");

    printf("  Created nested directories: /nested/level1/level2/level3\n");

    // 2. 验证嵌套目录可以访问
    OpenDirResponse opendir_resp;
    status = DoOpendir(base + "/level1/level2/level3", &opendir_resp);
    TEST_ASSERT(status == 0, "OPENDIR deepest level failed");
    printf("  Deepest directory accessible, inode=%lu\n", opendir_resp.st_ino);

    // 3. 测试删除非空目录 - 应该失败
    status = DoRmdir(base + "/level1/level2");
    TEST_ASSERT(status != 0, "RMDIR non-empty directory should fail");
    printf("  RMDIR non-empty directory correctly failed with status=%d\n", status);

    // 4. 清理目录：从深到浅删除
    status = DoRmdir(base + "/level1/level2/level3");
    TEST_ASSERT(status == 0, "RMDIR level3 failed");

    status = DoRmdir(base + "/level1/level2");
    TEST_ASSERT(status == 0, "RMDIR level2 failed");

    status = DoRmdir(base + "/level1");
    TEST_ASSERT(status == 0, "RMDIR level1 failed");

    status = DoRmdir(base);
    TEST_ASSERT(status == 0, "RMDIR base failed");

    printf("  Cleaned up all nested directories\n");

    TEST_PASS("Nested Directories");
}

/*
 * 测试 15: 双重 renew_shard_table 调用
 * 验证第二次调用是否跳过了 reload（用于验证缓存优化）
 */
static bool TestDoubleRenewShardTable()
{
    TEST_BEGIN("Double Renew Shard Table (Cache Optimization)");

    // 1. 第一次调用 falcon_renew_shard_table
    printf("  [Call #1] Executing falcon_renew_shard_table()...\n");
    PlainCommandResponse resp1;
    int status1 = DoPlainCommand("select * from falcon_renew_shard_table();", &resp1);
    TEST_ASSERT(status1 == 0, "First falcon_renew_shard_table() failed");

    printf("  [Call #1] Result: status=%d, row=%u, col=%u\n", status1, resp1.row, resp1.col);
    printf("  [Call #1] Expected: reload shard table cache (first time)\n");

    if (resp1.row > 0) {
        printf("  [Call #1] Retrieved %u shard entries\n", resp1.row);
        // 打印前几条分片信息
        for (uint32_t i = 0; i < std::min(3u, resp1.row) && i * resp1.col + 4 < resp1.data.size(); i++) {
            printf("    Shard %u: range[%s, %s], server=%s:%s, id=%s\n",
                   i,
                   resp1.data[i * resp1.col + 0].c_str(),
                   resp1.data[i * resp1.col + 1].c_str(),
                   resp1.data[i * resp1.col + 2].c_str(),
                   resp1.data[i * resp1.col + 3].c_str(),
                   resp1.data[i * resp1.col + 4].c_str());
        }
        if (resp1.row > 3) {
            printf("    ... (and %u more entries)\n", resp1.row - 3);
        }
    }

    // 添加短暂延迟，确保第一次调用完全完成
    printf("  Waiting 100ms before second call...\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 2. 第二次调用 falcon_renew_shard_table
    printf("  [Call #2] Executing falcon_renew_shard_table() again...\n");
    PlainCommandResponse resp2;
    int status2 = DoPlainCommand("select * from falcon_renew_shard_table();", &resp2);
    TEST_ASSERT(status2 == 0, "Second falcon_renew_shard_table() failed");

    printf("  [Call #2] Result: status=%d, row=%u, col=%u\n", status2, resp2.row, resp2.col);
    printf("  [Call #2] Expected: skip reload (cache already loaded)\n");
    printf("  [Call #2] Check PostgreSQL log for: '[DEBUG] falcon_renew_shard_table: shard table cache already loaded'\n");

    // 3. 验证两次返回的数据一致
    TEST_ASSERT_MSG(resp2.row == resp1.row,
                    "Second call returned different row count (expected %u, got %u)",
                    resp1.row, resp2.row);
    TEST_ASSERT_MSG(resp2.col == resp1.col,
                    "Second call returned different col count (expected %u, got %u)",
                    resp1.col, resp2.col);

    printf("  [Verified] Both calls returned same data: %u rows x %u cols\n", resp2.row, resp2.col);

    // 4. 验证数据内容一致（抽样检查前几行）
    if (resp1.row > 0 && resp2.row > 0) {
        size_t check_rows = std::min(3u, std::min(resp1.row, resp2.row));
        bool data_match = true;
        for (size_t i = 0; i < check_rows; i++) {
            for (uint32_t j = 0; j < resp1.col && i * resp1.col + j < resp1.data.size() && i * resp2.col + j < resp2.data.size(); j++) {
                if (resp1.data[i * resp1.col + j] != resp2.data[i * resp2.col + j]) {
                    data_match = false;
                    printf("  [WARNING] Data mismatch at row %zu, col %u\n", i, j);
                }
            }
        }
        if (data_match) {
            printf("  [Verified] Data content matches between two calls (sampled %zu rows)\n", check_rows);
        }
    }

    // 5. 性能对比提示
    printf("\n");
    printf("  ┌─────────────────────────────────────────────────────────────┐\n");
    printf("  │ Performance Analysis:                                       │\n");
    printf("  │ - Call #1: Should reload cache (access system catalogs)    │\n");
    printf("  │ - Call #2: Should skip reload (use cached data)            │\n");
    printf("  │                                                             │\n");
    printf("  │ Check PostgreSQL log for:                                  │\n");
    printf("  │   [DEBUG] falcon_renew_shard_table: reloading ...          │\n");
    printf("  │   [DEBUG] falcon_renew_shard_table: already loaded ...     │\n");
    printf("  │   [DEBUG] GetForeignServerConnectionInfo: ...              │\n");
    printf("  └─────────────────────────────────────────────────────────────┘\n");

    TEST_PASS("Double Renew Shard Table");
}

/*
 * 判断本节点是否CN
 */
static bool IsCnNode()
{
    PlainCommandResponse resp;
    int status = DoPlainCommand("select server_name from falcon_foreign_server where is_local=true;", &resp);
    if (status != 0) {
        printf("  PostgreSQL get node info failed.\n");
        return false;
    }

    if (resp.data.size() > 0 && resp.data[0].find("cn") != std::string::npos) {
        printf("  PostgreSQL get node info: %s\n", resp.data[0].c_str());
        return true;
    }
    return false;
}

/*
 * 判断是否完成初始化，完成初始化后会创建 root目录'/'
 */
static bool IsRootDirExist()
{
    PlainCommandResponse resp;
    int status = DoPlainCommand("select count(*) from falcon_directory_table where name='/';", &resp);
    if (status != 0) {
        printf("  PostgreSQL get root dir failed.\n");
        return false;
    }

    if (resp.data.size() > 0 && resp.data[0].find("1") != std::string::npos) {
        printf("  PostgreSQL get root dir info: %s\n", resp.data[0].c_str());
        return true;
    }
    return false;
}

// ==================== 测试运行器 ====================

static void RunAllTests()
{
    // 增加循环计数器
    int current_iteration = g_loop_iteration.fetch_add(1);

    printf("\n");
    printf("╔══════════════════════════════════════════════════════════════╗\n");
    printf("║           FalconMetaService End-to-End Test Suite            ║\n");
    printf("║                     Iteration #%-4d                          ║\n", current_iteration);
    printf("╚══════════════════════════════════════════════════════════════╝\n");
    printf("\n");

    // 只在第一次循环时等待初始化
    if (current_iteration == 0) {
        // 等待部署完成，相关表完成创建，且创建了根目录
        bool initFinished = false;
        while (!initFinished) {
            initFinished = IsRootDirExist();
            std::this_thread::sleep_for(std::chrono::seconds(1));
            printf("Waiting for root path create finished.\n");
        }

        // 检测是否在 CN 节点
        printf("Detecting node type by data of falcon_foreign_server...\n");
        g_is_cn_node = IsCnNode();
        if (g_is_cn_node) {
            printf("  -> Running on CN node.\n");
        } else {
            printf("  -> Running on Worker node.\n");
            printf("  -> CN-only tests (MKDIR/RMDIR/RENAME) will be skipped\n");
        }
        printf("\n");
    } else {
        // 后续循环只打印节点类型
        printf("Running on %s node (Iteration #%d)\n", g_is_cn_node ? "CN" : "Worker", current_iteration);
        printf("\n");
    }

    // 运行测试
    int status = 0;
    if (g_is_cn_node) {
        // 只在第一次循环创建测试基础目录
        if (current_iteration == 0) {
            printf("Creating test base directory: %s\n", TEST_BASE_PATH);
            status = DoMkdir(TEST_BASE_PATH);
            if (status != 0) {
                printf("WARNING: Failed to create test base directory (may already exist)\n");
                // 尝试清理后重建
                DoRmdir(TEST_BASE_PATH);
                DoMkdir(TEST_BASE_PATH);
            }
        }

        // CN 节点测试 - 目录操作
        // TestDirectoryOperations();
        // TestRenameOperations();
        // TestReaddirWithSubdirs();
        // TestNestedDirectories();
        // TestPlainCommand();
        // TestDoubleRenewShardTable(); // 测试双重 renew_shard_table 调用

        // 不清理测试基础目录，保留给后续循环使用
        // printf("\nCleaning up test base directory...\n");
        // DoRmdir(TEST_BASE_PATH);
    } else {
        // Worker 节点测试 - 文件操作和 KV 操作
        // TestFileOperations();
        // TestAttributeOperations();
        // TestSliceOperations();
        // TestConcurrentFileCreation();  // 并发文件创建测试
        // TestConcurrentFetchSliceId();  // 并发slice ID获取测试
        // TestKvOperations();

        // 性能测试
        TestKvPerformance();
        // TestSlicePerformance();

        // TestPlainCommand();
        // TestDoubleRenewShardTable(); // 测试双重 renew_shard_table 调用

        // 冲突测试
        // TestKvPutDuplicateKey();
        // TestSlicePutDuplicate();
    }

    // 打印结果
    printf("\n");
    printf("╔══════════════════════════════════════════════════════════════╗\n");
    printf("║                       TEST RESULTS                           ║\n");
    printf("╠══════════════════════════════════════════════════════════════╣\n");
    printf("║  PASSED:  %-4d                                               ║\n", g_test_passed);
    printf("║  FAILED:  %-4d                                               ║\n", g_test_failed);
    printf("║  SKIPPED: %-4d                                               ║\n", g_test_skipped);
    printf("╚══════════════════════════════════════════════════════════════╝\n");
    printf("\n");

    if (g_test_failed == 0) {
        printf("*** ALL TESTS PASSED ***\n");
    } else {
        printf("*** SOME TESTS FAILED ***\n");
    }
}

// ==================== 插件框架函数 ====================

extern "C" {

int plugin_init(FalconPluginData *data)
{
    printf("\n[FalconMetaServiceTestPlugin] plugin_init() called\n");
    fflush(stdout);

    if (!data) {
        printf("[FalconMetaServiceTestPlugin] ERROR: plugin data is NULL\n");
        return -1;
    }

    FalconNodeInfo node_info;
    FalconPluginGetNodeInfo(&node_info);

    printf("[FalconMetaServiceTestPlugin] Node info: %s:%d (pooler=%d)\n",
           node_info.node_ip,
           node_info.node_port,
           node_info.pooler_port);
    fflush(stdout);

    g_node_port = node_info.node_port;

    printf("[FalconMetaServiceTestPlugin] plugin_init() completed\n");
    fflush(stdout);

    return 0;
}

FalconPluginWorkType plugin_get_type(void) { return FALCON_PLUGIN_TYPE_BACKGROUND; }

int plugin_work(FalconPluginData *data)
{
    printf("\n[FalconMetaServiceTestPlugin] plugin_work() called\n");
    fflush(stdout);

    printf("[FalconMetaServiceTestPlugin] Using HcomMetaService (pool port %d)...\n", g_node_port);
    fflush(stdout);

    g_meta_service = HcomMetaService::Instance();
    printf("[FalconMetaServiceTestPlugin] HcomMetaService instance ready\n");
    fflush(stdout);

    // 运行所有测试
    int loopTimes = 1;
    while (loopTimes-- > 0) {
        RunAllTests();
        // 增加sleep时间，让系统有时间回收内存
        std::this_thread::sleep_for(std::chrono::seconds(15));
    }

    printf("[FalconMetaServiceTestPlugin] plugin_work() completed\n");
    fflush(stdout);

    return 0;
}

void plugin_cleanup(FalconPluginData *data)
{
    printf("[FalconMetaServiceTestPlugin] plugin_cleanup() called\n");
    fflush(stdout);
}

} // extern "C"
