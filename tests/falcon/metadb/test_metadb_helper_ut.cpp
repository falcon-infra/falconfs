#include "test_metadb_coverage_common.h"

#include <algorithm>

using namespace metadb_test;

namespace {

MetaProcessInfoData BuildSampleMetaInfo(char *name, char *dst_name)
{
    MetaProcessInfoData info{};
    info.path = "/roundtrip";
    info.parentId = 11;
    info.parentId_partId = 22;
    info.name = name;
    info.inodeId = 33;
    info.st_dev = 44;
    info.st_mode = 0644;
    info.st_nlink = 1;
    info.st_uid = 1000;
    info.st_gid = 1000;
    info.st_rdev = 55;
    info.st_size = 4096;
    info.st_blksize = 4096;
    info.st_blocks = 8;
    info.st_atim = 101;
    info.st_mtim = 102;
    info.st_ctim = 103;
    info.node_id = 7;
    info.dstParentId = 66;
    info.dstParentIdPartId = 0;
    info.dstName = dst_name;
    info.targetIsDirectory = false;
    info.srcLockOrder = 1;
    info.errorCode = SUCCESS;
    return info;
}

}  // 匿名命名空间

TEST(MetadbCoverageUT, SerializedMetaSubParamRoundTrip)
{
    /*
     * DT 对应关系:
     * - TC-DIR-001 创建一级目录成功、TC-DIR-006 删除空目录成功;
     * - TC-FILE-001 CREATE 成功并可见、TC-FILE-006 UNLINK 删除后不可见;
     * - TC-REN-001 同目录文件重命名成功。
     *
     * 该用例只覆盖 serialized 子操作参数的编码/解码，不依赖 local-run 服务。
     */
    char name[] = "child";
    char dst_name[] = "dst";
    MetaProcessInfoData info = BuildSampleMetaInfo(name, dst_name);
    MetaProcessInfo info_ptrs[] = {&info};
    std::vector<FalconMetaServiceType> sub_param_types = {
        MKDIR_SUB_MKDIR,
        MKDIR_SUB_CREATE,
        RMDIR_SUB_RMDIR,
        RMDIR_SUB_UNLINK,
        RENAME_SUB_RENAME_LOCALLY,
        RENAME_SUB_CREATE,
    };
    for (auto type : sub_param_types) {
        SerializedDataGuard param;
        MetaProcessInfoData decoded{};
        // TC-DIR/TC-FILE/TC-REN: 子操作参数编码后应能被同类型解码。
        ASSERT_TRUE(SerializedDataMetaParamEncodeWithPerProcessFlatBufferBuilder(type, info_ptrs, nullptr, 1, param.get()));
        ASSERT_TRUE(SerializedDataMetaParamDecode(type, 1, param.get(), &decoded));
    }
}

TEST(MetadbCoverageUT, SerializedMetaResponseEncodeFlow)
{
    /*
     * DT 对应关系:
     * - TC-DIR、TC-FILE、TC-REN、TC-ATTR 相关用例的 serialized 响应编码支撑覆盖。
     *
     * 该用例只验证常用元数据操作响应可以被编码。
     */
    char name[] = "child";
    char dst_name[] = "dst";
    MetaProcessInfoData info = BuildSampleMetaInfo(name, dst_name);

    OneReadDirResult child{.fileName = "child", .mode = 0644};
    OneReadDirResult *children[] = {&child};
    info.readDirLastShardIndex = 2;
    info.readDirLastFileName = "last";
    info.readDirResultList = children;
    info.readDirResultCount = 1;

    std::vector<FalconMetaServiceType> response_types = {
        MKDIR,
        CREATE,
        STAT,
        OPEN,
        CLOSE,
        UNLINK,
        READDIR,
        OPENDIR,
        RMDIR,
        RENAME,
        RENAME_SUB_RENAME_LOCALLY,
        UTIMENS,
        CHOWN,
        CHMOD,
    };
    for (auto type : response_types) {
        SerializedDataGuard response;
        // TC-DIR/TC-FILE/TC-REN/TC-ATTR: 常用操作响应编码应成功。
        ASSERT_TRUE(SerializedDataMetaResponseEncodeWithPerProcessFlatBufferBuilder(type, 1, &info, response.get()));
    }
}

TEST(MetadbCoverageUT, SerializedMetaSubResponseRoundTripAndErrorFlow)
{
    /*
     * DT 对应关系:
     * - TC-DIR-001、TC-DIR-006、TC-FILE-001、TC-FILE-006、TC-REN-001;
     * - TC-REN-003 / TC-ATTR-004 的失败错误码解码。
     *
     * 该用例只覆盖子操作响应成功解码和错误码解码。
     */
    char name[] = "child";
    char dst_name[] = "dst";
    MetaProcessInfoData info = BuildSampleMetaInfo(name, dst_name);

    std::vector<FalconMetaServiceType> response_decode_types = {
        MKDIR_SUB_MKDIR,
        MKDIR_SUB_CREATE,
        RMDIR_SUB_RMDIR,
        RMDIR_SUB_UNLINK,
        RENAME_SUB_RENAME_LOCALLY,
        RENAME_SUB_CREATE,
    };
    for (auto type : response_decode_types) {
        SerializedDataGuard response;
        MetaProcessInfoData decoded{};
        // TC-DIR/TC-FILE/TC-REN: 子操作响应编码后应能解码出 SUCCESS。
        ASSERT_TRUE(SerializedDataMetaResponseEncodeWithPerProcessFlatBufferBuilder(type, 1, &info, response.get()));
        ASSERT_TRUE(SerializedDataMetaResponseDecode(type, 1, response.get(), &decoded));
        EXPECT_EQ(decoded.errorCode, SUCCESS);
    }

    info.errorCode = FILE_NOT_EXISTS;
    // TC-REN-003 / TC-ATTR-004: 覆盖元数据响应中的失败错误码解码。
    SerializedDataGuard error_response;
    MetaProcessInfoData decoded_error{};
    ASSERT_TRUE(SerializedDataMetaResponseEncodeWithPerProcessFlatBufferBuilder(MKDIR_SUB_MKDIR, 1, &info, error_response.get()));
    ASSERT_TRUE(SerializedDataMetaResponseDecode(MKDIR_SUB_MKDIR, 1, error_response.get(), &decoded_error));
    EXPECT_EQ(decoded_error.errorCode, FILE_NOT_EXISTS);
}

TEST(MetadbCoverageUT, SerializedKvResponseEncodeFlow)
{
    /*
     * DT 对应关系:
     * - TC-KV-001 KV_PUT 新 key 成功;
     * - TC-KV-002 KV_GET 命中返回一致;
     * - TC-KV-003 KV_DEL 删除后不可读。
     *
     * 该用例只覆盖 KV serialized 响应编码。
     */
    uint64_t value_keys[] = {101, 102};
    uint64_t locations[] = {201, 202};
    uint32_t slice_lens[] = {301, 302};
    KvMetaProcessInfoData kv_info{};
    kv_info.errorCode = SUCCESS;
    kv_info.valuelen = 603;
    kv_info.slicenum = 2;
    kv_info.valuekey = value_keys;
    kv_info.location = locations;
    kv_info.slicelen = slice_lens;
    for (auto type : {KV_PUT, KV_GET, KV_DEL}) {
        SerializedDataGuard response;
        // TC-KV-001/002/003: KV put/get/del 响应编码应成功。
        ASSERT_TRUE(SerializedKvMetaResponseEncodeWithPerProcessFlatBufferBuilder(type, 1, &kv_info, response.get()));
    }
}

TEST(MetadbCoverageUT, SerializedSliceResponseEncodeFlow)
{
    /*
     * DT 对应关系:
     * - TC-SLICE-004 SLICE_PUT 后 GET 一致;
     * - TC-SLICE-005 SLICE_DEL 删除后 GET 失败。
     *
     * 该用例只覆盖 slice put/get/delete 的 serialized 响应编码。
     */
    uint64_t inode_ids[] = {301, 302};
    uint32_t chunk_ids[] = {1, 2};
    uint64_t slice_ids[] = {401, 402};
    uint32_t slice_sizes[] = {4096, 8192};
    uint32_t slice_offsets[] = {0, 4096};
    uint32_t slice_lengths[] = {4096, 4096};
    uint32_t slice_loc1s[] = {7, 8};
    uint32_t slice_loc2s[] = {9, 10};
    SliceProcessInfoData slice_info{};
    slice_info.errorCode = SUCCESS;
    slice_info.count = 2;
    slice_info.inodeIds = inode_ids;
    slice_info.chunkIds = chunk_ids;
    slice_info.sliceIds = slice_ids;
    slice_info.sliceSizes = slice_sizes;
    slice_info.sliceOffsets = slice_offsets;
    slice_info.sliceLens = slice_lengths;
    slice_info.sliceLoc1s = slice_loc1s;
    slice_info.sliceloc2s = slice_loc2s;
    for (auto type : {SLICE_PUT, SLICE_GET, SLICE_DEL}) {
        SerializedDataGuard response;
        // TC-SLICE-004/005: slice put/get/del 响应编码应成功。
        ASSERT_TRUE(SerializedSliceResponseEncodeWithPerProcessFlatBufferBuilder(type, 1, &slice_info, response.get()));
    }
}

TEST(MetadbCoverageUT, SerializedSliceIdResponseEncodeFlow)
{
    /*
     * DT 对应关系:
     * - TC-SLICE-001 单线程 FETCH_SLICE_ID。
     *
     * 该用例只覆盖 slice-id 分配响应编码。
     */
    SliceIdProcessInfoData slice_id_info{};
    slice_id_info.errorCode = SUCCESS;
    slice_id_info.start = 500;
    slice_id_info.end = 502;
    SerializedDataGuard slice_id_response;
    // TC-SLICE-001: slice-id 响应编码应成功。
    ASSERT_TRUE(SerializedSliceIdResponseEncodeWithPerProcessFlatBufferBuilder(&slice_id_info, slice_id_response.get()));
}

TEST(MetadbCoverageUT, MetaProcessInfoPathComparatorFlow)
{
    /*
     * DT 支撑覆盖:
     * - 支撑 TC-REN-001 同目录文件重命名成功 和 TC-REN-002 跨目录重命名成功。
     * - 覆盖 metadb 在 rename 等多对象元数据操作前，对每个路径的 process info
     *   排序时使用的路径比较辅助函数。
     *
     * 这是一个不依赖服务的单元测试:
     * 1. 构造父路径、子路径和兄弟路径的 MetaProcessInfo 记录;
     * 2. 分别按两个方向直接调用 pg_qsort_meta_process_info_by_path_cmp;
     * 3. 对 MetaProcessInfo 指针数组排序，并验证父路径排在子路径前面。
     */
    MetaProcessInfoData parent{};
    MetaProcessInfoData child{};
    MetaProcessInfoData sibling{};
    parent.path = "/metadb/path";
    child.path = "/metadb/path/child";
    sibling.path = "/metadb/sibling";

    MetaProcessInfo parent_ptr = &parent;
    MetaProcessInfo child_ptr = &child;
    MetaProcessInfo sibling_ptr = &sibling;
    // TC-REN-001 / TC-REN-002: rename 前多路径加锁排序需要父路径排在子路径前。
    EXPECT_LT(pg_qsort_meta_process_info_by_path_cmp(&parent_ptr, &child_ptr), 0);
    EXPECT_GT(pg_qsort_meta_process_info_by_path_cmp(&child_ptr, &parent_ptr), 0);

    std::vector<MetaProcessInfo> infos = {sibling_ptr, child_ptr, parent_ptr};
    // TC-REN-001 / TC-REN-002: 验证排序结果满足父子路径顺序约束。
    std::sort(infos.begin(), infos.end(), [](const MetaProcessInfo &lhs, const MetaProcessInfo &rhs) {
        return pg_qsort_meta_process_info_by_path_cmp(&lhs, &rhs) < 0;
    });
    ASSERT_EQ(infos.size(), 3U);
    EXPECT_STREQ(infos[0]->path, "/metadb/path");
    EXPECT_STREQ(infos[1]->path, "/metadb/path/child");
    EXPECT_STREQ(infos[2]->path, "/metadb/sibling");
}
