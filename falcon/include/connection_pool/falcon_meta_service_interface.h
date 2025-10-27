/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_META_SERVICE_INTERFACE_H
#define FALCON_META_SERVICE_INTERFACE_H

#include <string>
#include <vector>
#include <variant>
#include <cstdint>
#include <functional>

namespace falcon {
namespace meta_service {

/**
 * 数据切片结构（元数据索引）
 */
struct FormDataSlice {
    uint64_t value_key;      // 全局 key 生成器（唯一数据块 ID）
    uint64_t location;       // 数据存储位置（偏移或地址）
    uint32_t size;           // 切片大小（字节）

    FormDataSlice() : value_key(0), location(0), size(0) {}
    FormDataSlice(uint64_t vk, uint64_t loc, uint32_t sz)
        : value_key(vk), location(loc), size(sz) {}
};

/**
 * KV 索引数据结构
 * 表示一个完整的 KV 对的元数据索引，value 被切成多个 slice（每个 2MB）
 */
struct FormDataKvIndex {
    std::string key;                        // 用户的 key
    uint32_t valueLen;                      // 完整 value 的总长度
    uint16_t sliceNum;                      // 切片数量（2MB 拆分）
    std::vector<FormDataSlice> dataSlices;  // 所有切片的元数据

    FormDataKvIndex() : valueLen(0), sliceNum(0) {}
};

/**
 * Falcon 元数据操作类型
 * 对应 FlatBuffers 中的操作类型
 */
enum FalconMetaOperationType {
    // KV 语义操作
    DFC_PUT_KEY_META = 1,              // 插入或更新 KV 元数据
    DFC_GET_KV_META = 2,               // 查询 KV 元数据
    DFC_DELETE_KV_META = 3,            // 删除 KV 元数据
    DFC_PLAIN_COMMAND = 4,             // 查询分片表信息（falcon_renew_shard_table）

    // 文件语义操作 - 基础操作
    DFC_MKDIR = 5,                     // 创建目录
    DFC_CREATE = 6,                    // 创建文件
    DFC_STAT = 7,                      // 查询文件/目录状态
    DFC_OPEN = 8,                      // 打开文件
    DFC_CLOSE = 9,                     // 关闭文件
    DFC_UNLINK = 10,                   // 删除文件
    DFC_READDIR = 11,                  // 读取目录内容
    DFC_OPENDIR = 12,                  // 打开目录
    DFC_RMDIR = 13,                    // 删除目录
    DFC_RENAME = 14,                   // 重命名文件/目录

    // 文件语义操作 - 子操作
    DFC_MKDIR_SUB_MKDIR = 15,          // MKDIR 子操作：创建子目录
    DFC_MKDIR_SUB_CREATE = 16,         // MKDIR 子操作：创建元数据
    DFC_RMDIR_SUB_RMDIR = 17,          // RMDIR 子操作：删除子目录
    DFC_RMDIR_SUB_UNLINK = 18,         // RMDIR 子操作：删除文件
    DFC_RENAME_SUB_RENAME_LOCALLY = 19,// RENAME 子操作：本地重命名
    DFC_RENAME_SUB_CREATE = 20,        // RENAME 子操作：创建新条目

    // 文件属性操作
    DFC_UTIMENS = 21,                  // 修改文件时间戳
    DFC_CHOWN = 22,                    // 修改文件所有者
    DFC_CHMOD = 23                     // 修改文件权限
};

/**
 * Shard Table 信息结构（用于 DFC_PLAIN_COMMAND）
 */
struct ShardTableInfo {
    int range_min;     // 哈希范围最小值
    int range_max;     // 哈希范围最大值
    std::string host;  // 服务器 IP
    int port;          // 服务器端口
    int server_id;     // 服务器 ID

    ShardTableInfo() : range_min(0), range_max(0), port(0), server_id(0) {}
};

/**
 * 响应数据结构 - KV GET 操作
 */
struct KvDataResponse {
    FormDataKvIndex kv_data;               // KV 元数据

    KvDataResponse() {}
};

/**
 * 响应数据结构 - 通用命令查询（PlainCommand）
 */
struct PlainCommandResponse {
    uint32_t row;                                   // 行数（PQntuples）
    uint32_t col;                                   // 列数（PQnfields）
    std::vector<std::string> data;                  // 原始数据（一维数组，按行优先顺序：data[row * col + col_idx]）

    PlainCommandResponse() : row(0), col(0) {}
};

/**
 * 通用命令参数（DFC_PLAIN_COMMAND）
 */
struct PlainCommandParam {
    std::string command;

    PlainCommandParam() {}
    PlainCommandParam(const std::string& cmd) : command(cmd) {}
};

/**
 * 仅路径参数（DFC_MKDIR, DFC_CREATE, DFC_STAT, DFC_OPEN, DFC_UNLINK, DFC_OPENDIR, DFC_RMDIR）
 */
struct PathOnlyParam {
    std::string path;

    PathOnlyParam() {}
    PathOnlyParam(const std::string& p) : path(p) {}
};

/**
 * CLOSE 操作参数
 */
struct CloseParam {
    std::string path;
    int64_t st_size;
    uint64_t st_mtim;
    int32_t node_id;

    CloseParam() : st_size(0), st_mtim(0), node_id(0) {}
};

/**
 * READDIR 操作参数
 */
struct ReadDirParam {
    std::string path;
    int32_t max_read_count;
    int32_t last_shard_index;
    std::string last_file_name;

    ReadDirParam() : max_read_count(-1), last_shard_index(-1) {}
};

/**
 * MKDIR 子操作 - 创建子目录参数
 */
struct MkdirSubMkdirParam {
    uint64_t parent_id;
    std::string name;
    uint64_t inode_id;

    MkdirSubMkdirParam() : parent_id(0), inode_id(0) {}
};

/**
 * MKDIR 子操作 - 创建元数据参数
 */
struct MkdirSubCreateParam {
    uint64_t parent_id_part_id;
    std::string name;
    uint64_t inode_id;
    uint32_t st_mode;
    uint64_t st_mtim;
    int64_t st_size;

    MkdirSubCreateParam() : parent_id_part_id(0), inode_id(0), st_mode(0), st_mtim(0), st_size(0) {}
};

/**
 * RMDIR 子操作 - 删除子目录参数
 */
struct RmdirSubRmdirParam {
    uint64_t parent_id;
    std::string name;

    RmdirSubRmdirParam() : parent_id(0) {}
};

/**
 * RMDIR 子操作 - 删除文件参数
 */
struct RmdirSubUnlinkParam {
    uint64_t parent_id_part_id;
    std::string name;

    RmdirSubUnlinkParam() : parent_id_part_id(0) {}
};

/**
 * RENAME 操作参数
 */
struct RenameParam {
    std::string src;
    std::string dst;

    RenameParam() {}
    RenameParam(const std::string& s, const std::string& d) : src(s), dst(d) {}
};

/**
 * RENAME 子操作 - 本地重命名参数
 */
struct RenameSubRenameLocallyParam {
    uint64_t src_parent_id;
    uint64_t src_parent_id_part_id;
    std::string src_name;
    uint64_t dst_parent_id;
    uint64_t dst_parent_id_part_id;
    std::string dst_name;
    bool target_is_directory;
    uint64_t directory_inode_id;
    int32_t src_lock_order;

    RenameSubRenameLocallyParam()
        : src_parent_id(0), src_parent_id_part_id(0),
          dst_parent_id(0), dst_parent_id_part_id(0),
          target_is_directory(false), directory_inode_id(0), src_lock_order(0) {}
};

/**
 * RENAME 子操作 - 创建新条目参数
 */
struct RenameSubCreateParam {
    uint64_t parentid_partid;
    std::string name;
    uint64_t st_ino;
    uint64_t st_dev;
    uint32_t st_mode;
    uint64_t st_nlink;
    uint32_t st_uid;
    uint32_t st_gid;
    uint64_t st_rdev;
    int64_t st_size;
    int64_t st_blksize;
    int64_t st_blocks;
    uint64_t st_atim;
    uint64_t st_mtim;
    uint64_t st_ctim;
    int32_t node_id;

    RenameSubCreateParam()
        : parentid_partid(0), st_ino(0), st_dev(0), st_mode(0), st_nlink(0),
          st_uid(0), st_gid(0), st_rdev(0), st_size(0), st_blksize(0),
          st_blocks(0), st_atim(0), st_mtim(0), st_ctim(0), node_id(0) {}
};

/**
 * UTIMENS 操作参数
 */
struct UtimeNsParam {
    std::string path;
    uint64_t st_atim;
    uint64_t st_mtim;

    UtimeNsParam() : st_atim(0), st_mtim(0) {}
};

/**
 * CHOWN 操作参数
 */
struct ChownParam {
    std::string path;
    uint32_t st_uid;
    uint32_t st_gid;

    ChownParam() : st_uid(0), st_gid(0) {}
};

/**
 * CHMOD 操作参数
 */
struct ChmodParam {
    std::string path;
    uint64_t st_mode;

    ChmodParam() : st_mode(0) {}
};

struct EmptyParam {};

using AnyMetaParam = std::variant<
    EmptyParam,
    PlainCommandParam,
    PathOnlyParam,
    CloseParam,
    ReadDirParam,
    MkdirSubMkdirParam,
    MkdirSubCreateParam,
    RmdirSubRmdirParam,
    RmdirSubUnlinkParam,
    RenameParam,
    RenameSubRenameLocallyParam,
    RenameSubCreateParam,
    UtimeNsParam,
    ChownParam,
    ChmodParam
>;

namespace meta_param_helper {

template<typename T>
inline T* Get(AnyMetaParam& param) {
    if (std::holds_alternative<T>(param)) {
        return &std::get<T>(param);
    }
    return nullptr;
}

template<typename T>
inline const T* Get(const AnyMetaParam& param) {
    if (std::holds_alternative<T>(param)) {
        return &std::get<T>(param);
    }
    return nullptr;
}

template<typename T>
inline void Set(AnyMetaParam& param, const T& value) {
    param = value;
}

template<typename T>
inline void Set(AnyMetaParam& param, T&& value) {
    param = std::move(value);
}

} // namespace meta_param_helper

/**
 * 响应数据结构 - 文件创建（CREATE）
 */
struct CreateResponse {
    uint64_t st_ino;
    int64_t node_id;
    uint64_t st_dev;
    uint32_t st_mode;
    uint64_t st_nlink;
    uint32_t st_uid;
    uint32_t st_gid;
    uint64_t st_rdev;
    int64_t st_size;
    int64_t st_blksize;
    int64_t st_blocks;
    uint64_t st_atim;
    uint64_t st_mtim;
    uint64_t st_ctim;

    CreateResponse()
        : st_ino(0), node_id(0), st_dev(0), st_mode(0), st_nlink(0),
          st_uid(0), st_gid(0), st_rdev(0), st_size(0), st_blksize(0),
          st_blocks(0), st_atim(0), st_mtim(0), st_ctim(0) {}
};

/**
 * 响应数据结构 - 文件打开（OPEN）
 */
struct OpenResponse {
    uint64_t st_ino;
    int64_t node_id;
    uint64_t st_dev;
    uint32_t st_mode;
    uint64_t st_nlink;
    uint32_t st_uid;
    uint32_t st_gid;
    uint64_t st_rdev;
    int64_t st_size;
    int64_t st_blksize;
    int64_t st_blocks;
    uint64_t st_atim;
    uint64_t st_mtim;
    uint64_t st_ctim;

    OpenResponse()
        : st_ino(0), node_id(0), st_dev(0), st_mode(0), st_nlink(0),
          st_uid(0), st_gid(0), st_rdev(0), st_size(0), st_blksize(0),
          st_blocks(0), st_atim(0), st_mtim(0), st_ctim(0) {}
};

/**
 * 响应数据结构 - 文件状态（STAT）
 */
struct StatResponse {
    uint64_t st_ino;
    uint64_t st_dev;
    uint32_t st_mode;
    uint64_t st_nlink;
    uint32_t st_uid;
    uint32_t st_gid;
    uint64_t st_rdev;
    int64_t st_size;
    int64_t st_blksize;
    int64_t st_blocks;
    uint64_t st_atim;
    uint64_t st_mtim;
    uint64_t st_ctim;

    StatResponse()
        : st_ino(0), st_dev(0), st_mode(0), st_nlink(0),
          st_uid(0), st_gid(0), st_rdev(0), st_size(0), st_blksize(0),
          st_blocks(0), st_atim(0), st_mtim(0), st_ctim(0) {}
};

/**
 * 响应数据结构 - 文件删除（UNLINK）
 */
struct UnlinkResponse {
    uint64_t st_ino;
    int64_t st_size;
    int64_t node_id;

    UnlinkResponse() : st_ino(0), st_size(0), node_id(0) {}
};

/**
 * 单个目录项
 */
struct OneReadDirResponse {
    std::string file_name;
    uint32_t st_mode;

    OneReadDirResponse() : st_mode(0) {}
};

/**
 * 响应数据结构 - 目录读取（READDIR）
 */
struct ReadDirResponse {
    int32_t last_shard_index;
    std::string last_file_name;
    std::vector<OneReadDirResponse> result_list;

    ReadDirResponse() : last_shard_index(0) {}
};

/**
 * 响应数据结构 - 打开目录（OPENDIR）
 */
struct OpenDirResponse {
    uint64_t st_ino;

    OpenDirResponse() : st_ino(0) {}
};

/**
 * 响应数据结构 - 重命名本地操作
 */
struct RenameSubRenameLocallyResponse {
    uint64_t st_ino;
    uint64_t st_dev;
    uint32_t st_mode;
    uint64_t st_nlink;
    uint32_t st_uid;
    uint32_t st_gid;
    uint64_t st_rdev;
    int64_t st_size;
    int64_t st_blksize;
    int64_t st_blocks;
    uint64_t st_atim;
    uint64_t st_mtim;
    uint64_t st_ctim;
    int32_t node_id;

    RenameSubRenameLocallyResponse()
        : st_ino(0), st_dev(0), st_mode(0), st_nlink(0),
          st_uid(0), st_gid(0), st_rdev(0), st_size(0), st_blksize(0),
          st_blocks(0), st_atim(0), st_mtim(0), st_ctim(0), node_id(0) {}
};

/**
 * Falcon 元数据服务响应结构
 */
struct FalconMetaServiceResponse {
    int status;                       // 0: 成功, 非0: 错误码
    FalconMetaOperationType opcode;   // 操作码
    void* data;                       // 返回数据指针（不同操作码对应不同数据结构）

    FalconMetaServiceResponse() : status(0), opcode(DFC_PUT_KEY_META), data(nullptr) {}

    ~FalconMetaServiceResponse() {}
};

/**
 * 回调函数类型
 * 用于异步操作完成后的通知
 *
 * @param response: 操作结果
 * @param user_context: 用户传入的上下文指针
 */
using FalconMetaServiceCallback = std::function<void(const FalconMetaServiceResponse&, void*)>;

/**
 * Falcon 元数据服务请求结构
 */
struct FalconMetaServiceRequest {
    FalconMetaOperationType operation;
    // KV 操作数据（用于 DFC_PUT_KEY_META, DFC_GET_KV_META, DFC_DELETE_KV_META）
    FormDataKvIndex kv_data;
    // 文件操作路径（用于简单文件操作）
    std::string path;
    AnyMetaParam file_params;
    bool allow_batch_with_others_;

    FalconMetaServiceRequest()
        : operation(DFC_PUT_KEY_META),
          allow_batch_with_others_(true) {}

    bool allow_batch_with_others() const { return allow_batch_with_others_; }
    void set_allow_batch_with_others(bool value) { allow_batch_with_others_ = value; }
};

} // namespace meta_service
} // namespace falcon

#endif // FALCON_META_SERVICE_INTERFACE_H
