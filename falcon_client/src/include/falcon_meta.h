/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <stdint.h>
#include "falcon_router.h"


struct FalconFuseInfo
{
    int flags;

    unsigned long fhOld;

    int writePage;

    unsigned int directIO : 1;

    unsigned int keepCache : 1;

    unsigned int flush : 1;

    unsigned int nonSeekAble : 1;

    unsigned int flockRelease : 1;

    unsigned int padding : 27;

    uint64_t fh;

    uint64_t lockOwner;
};

using FalconFuseFiller = int (*)(void *, const char *, const struct stat *, off_t);

int FalconMkdir(const std::string &path);

int FalconCreate(const std::string &path, uint64_t &fd, int oflags, struct stat *stbuf);

int FalconOpen(const std::string &path, int oflags, uint64_t &fd, struct stat *stbuf);

int FalconUnlink(const std::string &path);

int FalconOpenDir(const std::string &path, struct FalconFuseInfo *fi);

int FalconReadDir(const std::string &path, void *buf, FalconFuseFiller filler, off_t offset, struct FalconFuseInfo *fi);

int FalconClose(const std::string &path, uint64_t fd, bool isFlush = false, int datasync = -1);

int FalconGetStat(const std::string &path, struct stat *stbuf);

int FalconCloseDir(uint64_t fd);

int FalconInitWithZK(std::string zkEndPoint, const std::string &zkPath = "/falcon");

int FalconInit(std::string &coordinatorIp, int coordinatorPort);

int FalconDestroy();

int FalconRmDir(const std::string &path);

int FalconWrite(uint64_t fd, const std::string &path, const char *buffer, size_t size, off_t offset);

int FalconRead(const std::string &path, uint64_t fd, char *buffer, size_t size, off_t offset);

int FalconRename(const std::string &srcName, const std::string &dstName);

int FalconFsync(const std::string &path, uint64_t fd, int datasync);

int FalconStatFS(struct statvfs *vfsbuf);

int FalconDeleteCache(const std::string &path);

int FalconUtimens(const std::string &path, int64_t accessTime = -1, int64_t modifyTime = -1);

int FalconChown(const std::string &path, uid_t uid, gid_t gid);

int FalconChmod(const std::string &path, mode_t mode);

int FalconTruncate(const std::string &path, off_t size);

int FalconRenamePersist(const std::string &srcName, const std::string &dstName);
