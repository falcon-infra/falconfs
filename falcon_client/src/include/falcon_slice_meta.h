/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#pragma once

#include <cstdint>
#include <string>
#include <vector>

struct SliceInfo
{
    uint64_t inodeId;
    uint32_t chunkId;
    uint64_t sliceId;
    uint32_t sliceSize;
    uint32_t sliceOffset;
    uint32_t sliceLen;
    uint32_t sliceLoc1;
    uint32_t sliceLoc2;
};

int FalconSlicePut(const std::string &path, std::vector<SliceInfo> &info);

int FalconSliceGet(const std::string &path, uint64_t inodeId, uint32_t chunkId, std::vector<SliceInfo> &info);

int FalconSliceDel(const std::string &path, uint64_t inodeId, uint32_t chunkId);