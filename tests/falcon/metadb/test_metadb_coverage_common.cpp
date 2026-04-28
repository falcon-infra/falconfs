#include "test_metadb_coverage_common.h"

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <thread>

int thread_num = 1;
int client_cache_size = 16384;
int files_per_dir = 2;
int file_size = 4096;
int file_num = 0;
std::atomic<bool> printed(false);
volatile uint64_t op_count[16384];
volatile uint64_t latency_count[16384];

namespace metadb_test {

namespace {
std::atomic<uint64_t> g_case_counter(0);
local_run_test::LocalRunParameters g_params;
}  // 匿名命名空间

std::string BuildRootPath(const char *tag)
{
    uint64_t seq = g_case_counter.fetch_add(1, std::memory_order_relaxed);
    return fmt::format("{}metadb_{}_{}_{}_{}_{}_{}/",
                       g_params.mount_dir, g_params.client_id, g_params.wait_port,
                       tag, getpid(), seq, time(nullptr));
}

std::string ThreadDir(const std::string &root, int thread_id)
{
    return fmt::format("{}thread_{}", root, thread_id);
}

std::string FilePath(const std::string &root, int thread_id, int file_id)
{
    return fmt::format("{}/file_{}", ThreadDir(root, thread_id), file_id);
}

std::string DirPath(const std::string &root, int thread_id, int dir_id)
{
    return fmt::format("{}/dir_{}", ThreadDir(root, thread_id), dir_id);
}

SerializedDataGuard::SerializedDataGuard() { EXPECT_TRUE(SerializedDataInit(&data_, nullptr, 0, 0, nullptr)); }
SerializedDataGuard::~SerializedDataGuard() { SerializedDataDestroy(&data_); }
SerializedData *SerializedDataGuard::get() { return &data_; }

void InitNamespaceRoot(const std::string &root)
{
    int saved_files_per_dir = files_per_dir;
    files_per_dir = 1 + thread_num;
    workload_init(root, 0);
    files_per_dir = saved_files_per_dir;
}

void UninitNamespaceRoot(const std::string &root)
{
    int saved_files_per_dir = files_per_dir;
    files_per_dir = 1 + thread_num;
    workload_uninit(root, 0);
    files_per_dir = saved_files_per_dir;
}

bool InitClientOrSkip()
{
    setenv("SERVER_IP", local_run_test::GetEnvOrDefault("SERVER_IP", "127.0.0.1").c_str(), 1);
    setenv("SERVER_PORT", local_run_test::GetEnvOrDefault("SERVER_PORT", "55500").c_str(), 1);
    g_params = local_run_test::LoadLocalRunParameters();
    local_run_test::ResetCounters();

    constexpr int kMaxRetry = 6;
    for (int i = 0; i < kMaxRetry; ++i) {
        try {
            if (dfs_init(g_params.client_num) == 0) {
                return true;
            }
        } catch (...) {
        }
        dfs_shutdown();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return false;
}

void CleanupRoot(const std::string &root, const std::string &file_path, const std::string &dir_path)
{
    try {
        dfs_unlink(file_path.c_str());
        dfs_rmdir(dir_path.c_str());
        UninitNamespaceRoot(root);
    } catch (...) {
    }
    dfs_shutdown();
}

std::string SqlQuote(const std::string &value)
{
    std::string quoted = "'";
    for (char ch : value) {
        if (ch == '\'') {
            quoted += "''";
        } else {
            quoted += ch;
        }
    }
    quoted += "'";
    return quoted;
}

std::string HexEncode(const std::vector<uint8_t> &bytes)
{
    std::ostringstream out;
    out << std::hex << std::setfill('0');
    for (uint8_t byte : bytes) {
        out << std::setw(2) << static_cast<int>(byte);
    }
    return out.str();
}

std::vector<uint8_t> WrapSerializedItem(const flatbuffers::FlatBufferBuilder &builder)
{
    uint32_t payload_size = static_cast<uint32_t>(builder.GetSize());
    uint32_t aligned_size = (payload_size + 3U) & ~3U;
    std::vector<uint8_t> bytes(sizeof(uint32_t) + aligned_size, 0);
    bytes[0] = static_cast<uint8_t>(aligned_size & 0xFFU);
    bytes[1] = static_cast<uint8_t>((aligned_size >> 8U) & 0xFFU);
    bytes[2] = static_cast<uint8_t>((aligned_size >> 16U) & 0xFFU);
    bytes[3] = static_cast<uint8_t>((aligned_size >> 24U) & 0xFFU);
    std::copy(builder.GetBufferPointer(), builder.GetBufferPointer() + payload_size, bytes.begin() + sizeof(uint32_t));
    return bytes;
}

std::vector<uint8_t> BuildPathOnlyParam(const std::string &path)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreatePathOnlyParamDirect(builder, path.c_str());
    auto meta = falcon::meta_fbs::CreateMetaParam(builder, falcon::meta_fbs::AnyMetaParam_PathOnlyParam, param.Union());
    builder.Finish(meta);
    return WrapSerializedItem(builder);
}

std::vector<uint8_t> BuildCloseParam(const std::string &path, int64_t size, uint64_t mtime, int32_t node_id)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreateCloseParamDirect(builder, path.c_str(), size, mtime, node_id);
    auto meta = falcon::meta_fbs::CreateMetaParam(builder, falcon::meta_fbs::AnyMetaParam_CloseParam, param.Union());
    builder.Finish(meta);
    return WrapSerializedItem(builder);
}

std::vector<uint8_t> BuildReadDirParam(const std::string &path, int32_t max_read_count,
                                       int32_t last_shard_index, const std::string &last_file_name)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreateReadDirParamDirect(builder, path.c_str(), max_read_count,
                                                            last_shard_index, last_file_name.c_str());
    auto meta = falcon::meta_fbs::CreateMetaParam(builder, falcon::meta_fbs::AnyMetaParam_ReadDirParam, param.Union());
    builder.Finish(meta);
    return WrapSerializedItem(builder);
}

std::vector<uint8_t> BuildRenameParam(const std::string &src, const std::string &dst)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreateRenameParamDirect(builder, src.c_str(), dst.c_str());
    auto meta = falcon::meta_fbs::CreateMetaParam(builder, falcon::meta_fbs::AnyMetaParam_RenameParam, param.Union());
    builder.Finish(meta);
    return WrapSerializedItem(builder);
}

std::vector<uint8_t> BuildUtimeNsParam(const std::string &path, uint64_t atime, uint64_t mtime)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreateUtimeNsParam(builder, builder.CreateString(path), atime, mtime);
    auto meta = falcon::meta_fbs::CreateMetaParam(builder, falcon::meta_fbs::AnyMetaParam_UtimeNsParam, param.Union());
    builder.Finish(meta);
    return WrapSerializedItem(builder);
}

std::vector<uint8_t> BuildChownParam(const std::string &path, uint32_t uid, uint32_t gid)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreateChownParamDirect(builder, path.c_str(), uid, gid);
    auto meta = falcon::meta_fbs::CreateMetaParam(builder, falcon::meta_fbs::AnyMetaParam_ChownParam, param.Union());
    builder.Finish(meta);
    return WrapSerializedItem(builder);
}

std::vector<uint8_t> BuildChmodParam(const std::string &path, uint64_t mode)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreateChmodParam(builder, builder.CreateString(path), mode);
    auto meta = falcon::meta_fbs::CreateMetaParam(builder, falcon::meta_fbs::AnyMetaParam_ChmodParam, param.Union());
    builder.Finish(meta);
    return WrapSerializedItem(builder);
}

std::vector<uint8_t> BuildKvParam(const std::string &key, uint32_t value_len,
                                  const std::vector<uint64_t> &value_key,
                                  const std::vector<uint64_t> &location,
                                  const std::vector<uint32_t> &size)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreateKVParamDirect(builder, key.c_str(), value_len,
                                                       static_cast<uint16_t>(value_key.size()), &value_key,
                                                       &location, &size);
    auto meta = falcon::meta_fbs::CreateMetaParam(builder, falcon::meta_fbs::AnyMetaParam_KVParam, param.Union());
    builder.Finish(meta);
    return WrapSerializedItem(builder);
}

std::vector<uint8_t> BuildKeyOnlyParam(const std::string &key)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreateKeyOnlyParamDirect(builder, key.c_str());
    auto meta = falcon::meta_fbs::CreateMetaParam(builder, falcon::meta_fbs::AnyMetaParam_KeyOnlyParam, param.Union());
    builder.Finish(meta);
    return WrapSerializedItem(builder);
}

std::vector<uint8_t> BuildSliceInfoParam(const std::string &name, const std::vector<uint64_t> &inode_id,
                                         const std::vector<uint32_t> &chunk_id,
                                         const std::vector<uint64_t> &slice_id,
                                         const std::vector<uint32_t> &slice_size,
                                         const std::vector<uint32_t> &slice_offset,
                                         const std::vector<uint32_t> &slice_len,
                                         const std::vector<uint32_t> &slice_loc1,
                                         const std::vector<uint32_t> &slice_loc2)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreateSliceInfoParamDirect(builder, name.c_str(),
                                                              static_cast<uint32_t>(inode_id.size()), &inode_id,
                                                              &chunk_id, &slice_id, &slice_size, &slice_offset,
                                                              &slice_len, &slice_loc1, &slice_loc2);
    auto meta =
        falcon::meta_fbs::CreateMetaParam(builder, falcon::meta_fbs::AnyMetaParam_SliceInfoParam, param.Union());
    builder.Finish(meta);
    return WrapSerializedItem(builder);
}

std::vector<uint8_t> BuildSliceIndexParam(const std::string &name, uint64_t inode_id, uint32_t chunk_id)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreateSliceIndexParamDirect(builder, name.c_str(), inode_id, chunk_id);
    auto meta =
        falcon::meta_fbs::CreateMetaParam(builder, falcon::meta_fbs::AnyMetaParam_SliceIndexParam, param.Union());
    builder.Finish(meta);
    return WrapSerializedItem(builder);
}

std::vector<uint8_t> BuildSliceIdParam(uint32_t count, uint8_t type)
{
    flatbuffers::FlatBufferBuilder builder;
    auto param = falcon::meta_fbs::CreateSliceIdParam(builder, count, type);
    auto meta = falcon::meta_fbs::CreateMetaParam(builder, falcon::meta_fbs::AnyMetaParam_SliceIdParam, param.Union());
    builder.Finish(meta);
    return WrapSerializedItem(builder);
}

PgConnection::PgConnection(const std::string &host, int port)
{
    std::ostringstream conninfo;
    conninfo << "host=" << host << " port=" << port << " dbname=postgres connect_timeout=3";
    conn_.reset(PQconnectdb(conninfo.str().c_str()));
}

bool PgConnection::IsOpen() const
{
    return conn_ != nullptr && PQstatus(conn_.get()) == CONNECTION_OK;
}

std::string PgConnection::ErrorMessage() const
{
    return conn_ ? PQerrorMessage(conn_.get()) : "connection is null";
}

bool PgConnection::ExecOk(const std::string &sql)
{
    std::unique_ptr<PGresult, decltype(&PQclear)> result(PQexec(conn_.get(), sql.c_str()), PQclear);
    if (!result) {
        return false;
    }
    ExecStatusType status = PQresultStatus(result.get());
    return status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK;
}

bool PgConnection::ScalarText(const std::string &sql, std::string *value)
{
    std::unique_ptr<PGresult, decltype(&PQclear)> result(PQexec(conn_.get(), sql.c_str()), PQclear);
    if (!result || PQresultStatus(result.get()) != PGRES_TUPLES_OK || PQntuples(result.get()) != 1) {
        return false;
    }
    if (value) {
        *value = PQgetvalue(result.get(), 0, 0);
    }
    return true;
}

bool PgConnection::ScalarInt(const std::string &sql, int *value)
{
    std::string text;
    if (!ScalarText(sql, &text)) {
        return false;
    }
    if (value) {
        *value = std::atoi(text.c_str());
    }
    return true;
}

bool PgConnection::SerializedCall(FalconMetaServiceType type, const std::vector<uint8_t> &param, int *response_size)
{
    return ScalarInt("SELECT octet_length(falcon_meta_call_by_serialized_data(" +
                         std::to_string(static_cast<int>(type)) + ", 1, decode('" + HexEncode(param) + "', 'hex')))",
                     response_size);
}

bool ConnectPlainSql(int pg_port, PgConnection *&connection_holder, std::unique_ptr<PgConnection> &owner)
{
    std::string ip = local_run_test::GetEnvOrDefault("SERVER_IP", "127.0.0.1");
    if (!local_run_test::WaitForEndpoint(ip, pg_port, 6)) {
        return false;
    }

    owner = std::make_unique<PgConnection>(ip, pg_port);
    if (!owner->IsOpen()) {
        return false;
    }
    connection_holder = owner.get();
    return true;
}

}  // 命名空间 metadb_test
