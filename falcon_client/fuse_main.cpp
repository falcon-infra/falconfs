/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#define FUSE_USE_VERSION 26

#include <execinfo.h>
#include <securec.h>
#include <string.h>
#include <unistd.h>
#include <atomic>
#include <csignal>
#include <print>
#include <thread>

#include <fuse/fuse.h>
#include <gflags/gflags.h>

#include "brpc/brpc_server.h"
#include "conf/falcon_property_key.h"
#include "error_code.h"
#include "falcon_code.h"
#include "falcon_meta.h"
#include "init/falcon_init.h"
#include "stats/falcon_stats.h"

static bool g_persist = false;

int DoGetAttr(const char *path, struct stat *stbuf)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }

    const char *lastSlash = strrchr(path, '/');
    if (lastSlash != nullptr && *(lastSlash + 1) == 1) {
        char middle_component_flag = *(lastSlash + 2);

        FalconStats::GetInstance().stats[META_LOOKUP].fetch_add(1);
        errno_t err = memmove_s((char *)(lastSlash + 1),
                                strlen(lastSlash + 1) + 1,
                                (char *)(lastSlash + 3),
                                strlen(lastSlash + 3) + 1);
        if (err != 0) {
            return -err;
        }
        if (middle_component_flag == '1') {
            stbuf->st_mode = 040777;
            return 0;
        }
    } else {
        FalconStats::GetInstance().stats[META_STAT].fetch_add(1);
    }

    StatFuseTimer t;
    int ret = FalconGetStat(path, stbuf);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoMkDir(const char *path, mode_t /*mode*/)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[META_MKDIR].fetch_add(1);
    StatFuseTimer t;
    int ret = FalconMkdir(path);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoOpen(const char *path, struct fuse_file_info *fi)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[META_OPEN].fetch_add(1);
    StatFuseTimer t;
    int oflags = fi->flags;
    uint64_t fd = -1;
    struct stat st;
    errno_t err = memset_s(&st, sizeof(st), 0, sizeof(st));
    if (err != 0) {
        return -err;
    }
    int ret = FalconOpen(path, oflags, fd, &st);
    fi->fh = fd;
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoOpenAtomic(const char *path, struct stat *stbuf, mode_t /*mode*/, struct fuse_file_info *fi)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }

    FalconStats::GetInstance().stats[META_OPEN_ATOMIC].fetch_add(1);
    uint64_t fd = -1;
    int oflags = fi->flags;
    int ret = 0;
    if (oflags & O_CREAT) {
        ret = FalconCreate(path, fd, oflags, stbuf);
        if (ret == FILE_EXISTS && !(oflags & O_EXCL)) {
            ret = SUCCESS;
        }
    } else {
        ret = FalconOpen(path, oflags, fd, stbuf);
    }

    fi->fh = fd;
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoOpenDir(const char *path, struct fuse_file_info *fi)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[META_OPENDIR].fetch_add(1);
    StatFuseTimer t;
    auto *ti = (struct FalconFuseInfo *)fi;
    int ret = FalconOpenDir(path, ti);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoReadDir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
    if (path == nullptr || buf == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[META_READDIR].fetch_add(1);
    StatFuseTimer t;
    auto *ti = (struct FalconFuseInfo *)fi;
    int ret = 0;

    ret = FalconReadDir(path, buf, (FalconFuseFiller)filler, offset, ti);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoCreate(const char *path, mode_t /*mode*/, struct fuse_file_info *fi)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[META_CREATE].fetch_add(1);
    StatFuseTimer t;
    uint64_t fd = 0;
    int oflags = fi->flags;
    struct stat st;
    errno_t err = memset_s(&st, sizeof(st), 0, sizeof(st));
    if (err != 0) {
        return -err;
    }
    int ret = FalconCreate(path, fd, oflags, &st);
    fi->fh = fd;
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoAccess(const char *path, int /*mask*/)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[META_ACCESS].fetch_add(1);
    StatFuseTimer t;
    return 0;
}

int DoRelease(const char *path, struct fuse_file_info *fi)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[META_RELEASE].fetch_add(1);
    StatFuseTimer t;
    uint64_t fd = fi->fh;
    int ret = FalconClose(path, fd);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoReleaseDir(const char *path, struct fuse_file_info *fi)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[META_RELEASEDIR].fetch_add(1);
    StatFuseTimer t;
    uint64_t fd = fi->fh;
    int ret = FalconCloseDir(fd);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoUnlink(const char *path)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[META_UNLINK].fetch_add(1);
    StatFuseTimer t;
    int ret;
    ret = FalconUnlink(path);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoRmDir(const char *path)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[META_RMDIR].fetch_add(1);
    StatFuseTimer t;
    int ret;
    ret = FalconRmDir(path);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

void DoDestroy(void * /*userdata*/)
{
    StatFuseTimer t;
    FalconDestroy();
}

int DoWrite(const char *path, const char *buffer, size_t size, off_t offset, struct fuse_file_info *fi)
{
    if (path == nullptr || buffer == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[FUSE_WRITE_OPS].fetch_add(1);
    StatFuseTimer t(FUSE_WRITE_LAT);
    uint ret;
    int64_t fd = fi->fh;
    ret = FalconWrite(fd, path, buffer, size, offset);
    if (ret != 0) {
        return ret;
    }
    FalconStats::GetInstance().stats[FUSE_WRITE] += size;
    return size;
}

int DoRead(const char *path, char *buffer, size_t size, off_t offset, struct fuse_file_info *fi)
{
    if (path == nullptr || buffer == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[FUSE_READ_OPS].fetch_add(1);
    StatFuseTimer t(FUSE_READ_LAT);
    uint64_t fd = fi->fh;
    int retSize = FalconRead(path, fd, buffer, size, offset);
    FalconStats::GetInstance().stats[FUSE_READ] += retSize >= 0 ? retSize : 0;
    return retSize;
}

int DoSetXAttr(const char *path, const char * /*key*/, const char *value, size_t /*size*/, int /*offset*/)
{
    if (path == nullptr || value == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    StatFuseTimer t;
    return 0;
}

int DoTruncate(const char *path, off_t size)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[META_TRUNCATE].fetch_add(1);
    StatFuseTimer t;
    int ret = FalconTruncate(std::string(path), size);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoFtruncate(const char *path, off_t size, struct fuse_file_info * /*fi*/)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[META_TRUNCATE].fetch_add(1);
    StatFuseTimer t;
    int ret = FalconTruncate(std::string(path), size);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoFlush(const char *path, struct fuse_file_info *fi)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[META_FLUSH].fetch_add(1);
    StatFuseTimer t;
    int64_t fd = fi->fh;
    int ret = FalconClose(path, fd, true);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoRename(const char *srcPath, const char *dstPath)
{
    if (srcPath == nullptr || dstPath == nullptr || strlen(srcPath) == 0 || strlen(dstPath) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[META_RENAME].fetch_add(1);
    StatFuseTimer t;
    int ret = 0;
    if (g_persist) {
        ret = FalconRenamePersist(srcPath, dstPath);
    } else {
        ret = FalconRename(srcPath, dstPath);
    }
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoFsync(const char *path, int datasync, struct fuse_file_info *fi)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    FalconStats::GetInstance().stats[META_FSYNC].fetch_add(1);
    StatFuseTimer t;
    uint64_t fd = fi->fh;
    int ret = FalconFsync(path, fd, datasync);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoStatfs(const char *path, struct statvfs *vfsBuf)
{
    if (path == nullptr || vfsBuf == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    StatFuseTimer t;
    int ret = FalconStatFS(vfsBuf);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoUtimens(const char *path, const struct timespec tv[2])
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    int ret = 0;
    if (!tv || tv[0].tv_nsec == UTIME_NOW) {
        ret = FalconUtimens(path);
    } else {
        ret = FalconUtimens(path, tv[0].tv_sec, tv[1].tv_sec);
    }
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoChmod(const char *path, mode_t mode)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    int ret = FalconChmod(path, mode);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

int DoChown(const char *path, uid_t uid, gid_t gid)
{
    if (path == nullptr || strlen(path) == 0) {
        return -EINVAL;
    }
    int ret = FalconChown(path, uid, gid);
    return ret > 0 ? -ErrorCodeToErrno(ret) : ret;
}

static struct fuse_operations falconOperations = {
    .getattr = DoGetAttr,
    .readlink = nullptr,
    .getdir = nullptr,
    .mknod = nullptr,
    .mkdir = DoMkDir,
    .unlink = DoUnlink,
    .rmdir = DoRmDir,
    .symlink = nullptr,
    .rename = DoRename,
    .link = nullptr,
    .chmod = DoChmod,
    .chown = DoChown,
    .truncate = DoTruncate,
    .utime = nullptr,
    .open = DoOpen,
    .read = DoRead,
    .write = DoWrite,
    .statfs = DoStatfs,
    .flush = DoFlush,
    .release = DoRelease,
    .fsync = DoFsync,
    .setxattr = DoSetXAttr,
    .getxattr = nullptr,
    .listxattr = nullptr,
    .removexattr = nullptr,
    .opendir = DoOpenDir,
    .readdir = DoReadDir,
    .releasedir = DoReleaseDir,

    .fsyncdir = nullptr,
    .init = nullptr,
    .destroy = DoDestroy,
    .access = DoAccess,
    .create = DoCreate,
    .ftruncate = DoFtruncate,
    .fgetattr = nullptr,
    .lock = nullptr,
    .utimens = DoUtimens,
    .bmap = nullptr,
    .flag_nullpath_ok = 1,
    .flag_nopath = 0,
    .flag_utime_omit_ok = 1,
    .flag_reserved = 0,
    .ioctl = nullptr,
    .poll = nullptr,
    .write_buf = nullptr,
    .read_buf = nullptr,
    .flock = nullptr,
    .fallocate = nullptr,
#ifdef WITH_FUSE_OPT
#if WITH_FUSE_OPT
    .open_atomic = DoOpenAtomic,
#endif
#endif
};

DEFINE_string(rpc_endpoint, "0.0.0.0:56039", "endpoint of rpc server");
DEFINE_string(f, "", "fuse ops, unneeded");
DEFINE_string(o, "", "fuse ops, unneeded");
DEFINE_string(d, "", "fuse ops, unneeded");
DEFINE_string(brpc, "", "optional ops, unneeded");

int main(int argc, char *argv[])
{
    int fuseArgc = argc;
    for (int i = 0; i < argc; ++i) {
        if (strcmp(argv[i], "-brpc") == 0) {
            fuseArgc = i;
            break;
        }
    }
    std::vector<std::unique_ptr<char[]>> fuseArgvStorage(fuseArgc);
    std::vector<char *> fuseArgv(fuseArgc);

    for (int i = 0; i < fuseArgc; i++) {
        fuseArgvStorage[i] = std::make_unique<char[]>(strlen(argv[i]) + 1);
        strcpy(fuseArgvStorage[i].get(), argv[i]);
        fuseArgv[i] = fuseArgvStorage[i].get();
    }

    struct fuse_args args = FUSE_ARGS_INIT(fuseArgc, fuseArgv.data());

    gflags::ParseCommandLineFlags(&argc, &argv, false);

    falcon::brpc_io::RemoteIOServer &server = falcon::brpc_io::RemoteIOServer::GetInstance();
    server.endPoint = FLAGS_rpc_endpoint;
    std::println("brpc endpoint = {}", server.endPoint);
    std::thread brpcServerThread(&falcon::brpc_io::RemoteIOServer::Run, &server);
    {
        std::unique_lock<std::mutex> lk(server.mutexStart);
        server.cvStart.wait(lk, [&server]() { return server.isStarted; });
    }

    int ret;
    ret = GetInit().Init();
    if (ret != FALCON_SUCCESS) {
        std::println(stderr, "Falcon init failed");
    }
    auto &config = GetInit().GetFalconConfig();
    std::string serverIp = config->GetString(FalconPropertyKey::FALCON_SERVER_IP);
    std::string serverPort = config->GetString(FalconPropertyKey::FALCON_SERVER_PORT);
    g_persist = config->GetBool(FalconPropertyKey::FALCON_PERSIST);
#ifdef ZK_INIT
    std::println("Initialize with ZK");
    const char *zkEndPoint = std::getenv("zk_endpoint");
    if (zkEndPoint == nullptr) {
        std::println(stderr, "Fetch zk endpoint failed!");
        return -1;
    }
    ret = FalconInitWithZK(zkEndPoint);
#else
    ret = FalconInit(serverIp, std::stoi(serverPort));
#endif
    if (ret != FALCON_SUCCESS) {
        server.Stop();
        if (brpcServerThread.joinable()) {
            brpcServerThread.join();
        }
        std::println(stderr, "Falcon cluster init failed");
        return ret;
    }
    server.SetReadyFlag();
    std::println("{}", ret);
    ret = fuse_main(args.argc, args.argv, &falconOperations, nullptr);
    fuse_opt_free_args(&args);
    return ret;
}
