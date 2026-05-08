#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "hcom_comm_adapter/falcon_meta_service_interface.h"

namespace {

using namespace falcon::meta_service;

TEST(HcomMetaServiceInterfaceCoverageUT, OperationTypeNamesCoverAllBranches)
{
    const std::vector<std::pair<FalconMetaOperationType, const char *>> names = {
        {DFC_PUT_KEY_META, "DFC_PUT_KEY_META"},
        {DFC_GET_KV_META, "DFC_GET_KV_META"},
        {DFC_DELETE_KV_META, "DFC_DELETE_KV_META"},
        {DFC_PLAIN_COMMAND, "DFC_PLAIN_COMMAND"},
        {DFC_MKDIR, "DFC_MKDIR"},
        {DFC_CREATE, "DFC_CREATE"},
        {DFC_STAT, "DFC_STAT"},
        {DFC_OPEN, "DFC_OPEN"},
        {DFC_CLOSE, "DFC_CLOSE"},
        {DFC_UNLINK, "DFC_UNLINK"},
        {DFC_READDIR, "DFC_READDIR"},
        {DFC_OPENDIR, "DFC_OPENDIR"},
        {DFC_RMDIR, "DFC_RMDIR"},
        {DFC_RENAME, "DFC_RENAME"},
        {DFC_MKDIR_SUB_MKDIR, "DFC_MKDIR_SUB_MKDIR"},
        {DFC_MKDIR_SUB_CREATE, "DFC_MKDIR_SUB_CREATE"},
        {DFC_RMDIR_SUB_RMDIR, "DFC_RMDIR_SUB_RMDIR"},
        {DFC_RMDIR_SUB_UNLINK, "DFC_RMDIR_SUB_UNLINK"},
        {DFC_RENAME_SUB_RENAME_LOCALLY, "DFC_RENAME_SUB_RENAME_LOCALLY"},
        {DFC_RENAME_SUB_CREATE, "DFC_RENAME_SUB_CREATE"},
        {DFC_UTIMENS, "DFC_UTIMENS"},
        {DFC_CHOWN, "DFC_CHOWN"},
        {DFC_CHMOD, "DFC_CHMOD"},
        {DFC_SLICE_PUT, "DFC_SLICE_PUT"},
        {DFC_SLICE_GET, "DFC_SLICE_GET"},
        {DFC_SLICE_DEL, "DFC_SLICE_DEL"},
        {DFC_FETCH_SLICE_ID, "DFC_FETCH_SLICE_ID"},
        {NOT_SUPPORTED, "NOT_SUPPORTED"},
    };

    for (const auto &[op, name] : names) {
        EXPECT_STREQ(FalconMetaOperationTypeName(op), name);
    }
    EXPECT_STREQ(FalconMetaOperationTypeName(static_cast<FalconMetaOperationType>(999)), "UNKNOWN");
}

TEST(HcomMetaServiceInterfaceCoverageUT, ParamsAndResponsesInitializeFields)
{
    FormDataSlice slice(1, 2, 3);
    EXPECT_EQ(slice.value_key, 1U);
    EXPECT_EQ(slice.location, 2U);
    EXPECT_EQ(slice.size, 3U);

    FormDataKvIndex kvIndex;
    ShardTableInfo shard;
    EXPECT_EQ(kvIndex.valueLen, 0U);
    EXPECT_EQ(kvIndex.sliceNum, 0U);
    EXPECT_EQ(shard.port, 0);
    EXPECT_EQ(shard.server_id, 0);

    EXPECT_EQ(PlainCommandParam("select 1").command, "select 1");
    EXPECT_EQ(PathOnlyParam("/tmp/a").path, "/tmp/a");
    EXPECT_EQ(CloseParam().node_id, 0);
    EXPECT_EQ(ReadDirParam().max_read_count, -1);
    EXPECT_EQ(MkdirSubMkdirParam().parent_id, 0U);
    EXPECT_EQ(MkdirSubCreateParam().st_size, 0);
    EXPECT_EQ(RmdirSubRmdirParam().parent_id, 0U);
    EXPECT_EQ(RmdirSubUnlinkParam().parent_id_part_id, 0U);
    EXPECT_EQ(RenameParam("old", "new").dst, "new");
    EXPECT_FALSE(RenameSubRenameLocallyParam().target_is_directory);
    EXPECT_EQ(RenameSubCreateParam().node_id, 0);
    EXPECT_EQ(UtimeNsParam().st_mtim, 0U);
    EXPECT_EQ(ChownParam().st_gid, 0U);
    EXPECT_EQ(ChmodParam().st_mode, 0U);

    SliceIndexParam sliceIndex("file", 11, 12);
    EXPECT_EQ(sliceIndex.filename, "file");
    EXPECT_EQ(sliceIndex.inodeid, 11U);
    EXPECT_EQ(sliceIndex.chunkid, 12U);

    std::vector<uint64_t> u64s = {1, 2};
    std::vector<uint32_t> u32s = {3, 4};
    SliceInfoParam sliceInfo("file", 2, u64s, u32s, u64s, u32s, u32s, u32s, u32s, u32s);
    EXPECT_EQ(sliceInfo.slicenum, 2U);
    EXPECT_EQ(sliceInfo.inodeid, u64s);
    EXPECT_EQ(SliceIdParam(5, 6).type, 6U);

    EXPECT_EQ(CreateResponse().st_ino, 0U);
    EXPECT_EQ(OpenResponse().node_id, 0);
    EXPECT_EQ(StatResponse().st_size, 0);
    EXPECT_EQ(UnlinkResponse().node_id, 0);
    EXPECT_EQ(OneReadDirResponse().st_mode, 0U);
    EXPECT_EQ(ReadDirResponse().last_shard_index, 0);
    EXPECT_EQ(OpenDirResponse().st_ino, 0U);
    EXPECT_EQ(RenameSubRenameLocallyResponse().node_id, 0);

    SliceInfoResponse sliceResponse(2, u64s, u32s, u64s, u32s, u32s, u32s, u32s, u32s);
    EXPECT_EQ(sliceResponse.slicenum, 2U);
    EXPECT_EQ(sliceResponse.inodeid, u64s);
    EXPECT_EQ(SliceIdResponse(100, 200).end, 200U);

    FalconMetaServiceResponse response;
    EXPECT_EQ(response.status, 0);
    EXPECT_EQ(response.opcode, DFC_PUT_KEY_META);
    EXPECT_EQ(response.data, nullptr);

    FalconMetaServiceRequest request;
    EXPECT_EQ(request.operation, DFC_PUT_KEY_META);
    EXPECT_EQ(request.kv_data.valueLen, 0U);
}

TEST(HcomMetaServiceInterfaceCoverageUT, VariantHelpersCoverGetAndSet)
{
    AnyMetaParam param = EmptyParam{};
    EXPECT_EQ(meta_param_helper::Get<PathOnlyParam>(param), nullptr);

    meta_param_helper::Set(param, PathOnlyParam("/meta"));
    ASSERT_NE(meta_param_helper::Get<PathOnlyParam>(param), nullptr);
    EXPECT_EQ(meta_param_helper::Get<PathOnlyParam>(param)->path, "/meta");

    const AnyMetaParam constParam = param;
    ASSERT_NE(meta_param_helper::Get<PathOnlyParam>(constParam), nullptr);
    EXPECT_EQ(meta_param_helper::Get<PathOnlyParam>(constParam)->path, "/meta");

    meta_param_helper::Set(param, PlainCommandParam("cmd"));
    ASSERT_NE(meta_param_helper::Get<PlainCommandParam>(param), nullptr);
    EXPECT_EQ(meta_param_helper::Get<PlainCommandParam>(param)->command, "cmd");
}

} // namespace
