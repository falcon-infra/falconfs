#include "error_code.h"
#include "connection.h"
#include "router.h"
#include "falcon_meta.h"
#include "utils.h"
#include "brpc_comm_adapter/falcon_brpc_server.h"
#include "brpc_comm_adapter/brpc_meta_service_job.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <ctime>
#include <cstdlib>
#include <fcntl.h>
#include <cstring>
#include <memory>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <sys/statvfs.h>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <arpa/inet.h>
#include <unistd.h>
#include <vector>

#include <brpc/server.h>

extern "C" {
#include "remote_connection_utils/error_code_def.h"
}

#define FUSE_USE_VERSION 26
#include <fuse/fuse.h>

extern "C" int FalconFuseMainNoop(int, char **, const struct fuse_operations *, void *);
extern "C" void FalconFuseOptFreeArgsNoop(struct fuse_args *);

#define main FalconFuseMainCoverageEntry
#undef fuse_main
#define fuse_main FalconFuseMainNoop
#define fuse_opt_free_args FalconFuseOptFreeArgsNoop
#include "../../falcon_client/fuse_main.cpp"
#undef fuse_opt_free_args
#undef fuse_main
#undef main

extern "C" int FalconFuseMainNoop(int, char **, const struct fuse_operations *, void *)
{
    return 0;
}

extern "C" void FalconFuseOptFreeArgsNoop(struct fuse_args *) {}

TEST(FalconClientErrorCodeUT, MapsKnownFalconErrorsToErrno)
{
    /* Exercise maps Known Falcon Errors To Errno and assert the relevant success or failure branch. */
    EXPECT_EQ(ErrorCodeToErrno(SUCCESS), 0);
    EXPECT_EQ(ErrorCodeToErrno(PATH_IS_INVALID), ENOENT);
    EXPECT_EQ(ErrorCodeToErrno(WRONG_WORKER), ESRCH);
    EXPECT_EQ(ErrorCodeToErrno(OBSOLETE_SHARD), ENXIO);
    EXPECT_EQ(ErrorCodeToErrno(REMOTE_QUERY_FAILED), EREMOTEIO);
    EXPECT_EQ(ErrorCodeToErrno(ARGUMENT_ERROR), EINVAL);
    EXPECT_EQ(ErrorCodeToErrno(INODE_ROW_TYPE_ERROR), EILSEQ);
    EXPECT_EQ(ErrorCodeToErrno(FILE_EXISTS), EEXIST);
    EXPECT_EQ(ErrorCodeToErrno(FILE_NOT_EXISTS), ENOENT);
    EXPECT_EQ(ErrorCodeToErrno(OUT_OF_MEMORY), ENOMEM);
    EXPECT_EQ(ErrorCodeToErrno(STREAM_ERROR), EIO);
    EXPECT_EQ(ErrorCodeToErrno(PROGRAM_ERROR), EFAULT);
    EXPECT_EQ(ErrorCodeToErrno(SMART_ROUTE_ERROR), EHOSTUNREACH);
    EXPECT_EQ(ErrorCodeToErrno(SERVER_FAULT), ECONNRESET);
    EXPECT_EQ(ErrorCodeToErrno(UNDEFINED), EAGAIN);
    EXPECT_EQ(ErrorCodeToErrno(MKDIR_DUPLICATE), MKDIR_DUPLICATE);
    EXPECT_EQ(ErrorCodeToErrno(XKEY_EXISTS), EEXIST);
    EXPECT_EQ(ErrorCodeToErrno(XKEY_NOT_EXISTS), ENOENT);
    EXPECT_EQ(ErrorCodeToErrno(ZK_NOT_CONNECT), ENOTCONN);
    EXPECT_EQ(ErrorCodeToErrno(ZK_FETCH_RESULT_FAILED), EIO);
    EXPECT_EQ(ErrorCodeToErrno(TRANSACTION_FAULT), EIO);
    EXPECT_EQ(ErrorCodeToErrno(POOLED_FAULT), EAGAIN);
    EXPECT_EQ(ErrorCodeToErrno(NOT_FOUND_FD), EBADF);
    EXPECT_EQ(ErrorCodeToErrno(GET_ALL_WORKER_CONN_FAILED), EHOSTUNREACH);
    EXPECT_EQ(ErrorCodeToErrno(IO_ERROR), EIO);
    EXPECT_EQ(ErrorCodeToErrno(PATH_NOT_EXISTS), ENOENT);
    EXPECT_EQ(ErrorCodeToErrno(PATH_VERIFY_FAILED), EINVAL);
    EXPECT_EQ(ErrorCodeToErrno(999999), EIO);
}

TEST(FalconClientUtilsUT, ConvertsAndHashesBoundaryInputs)
{
    /* Exercise converts And Hashes Boundary Inputs and assert the relevant success or failure branch. */
    EXPECT_EQ(StringToInt64("-9223372036854775807"), -9223372036854775807LL);
    EXPECT_EQ(StringToUint64("1844674407370955161"), 1844674407370955161ULL);
    EXPECT_EQ(StringToInt32("-12345"), -12345);
    EXPECT_EQ(StringToUint32("429496729"), 429496729U);
    EXPECT_TRUE(StringToBool("true"));
    EXPECT_TRUE(StringToBool("True"));
    EXPECT_FALSE(StringToBool("false"));

    EXPECT_EQ(StrnLen("abc", 16), 3);
    EXPECT_EQ(StrnLen("abcdef", 3), 3);
    EXPECT_EQ(HashPartId(""), 0U);
    EXPECT_LT(HashPartId("falcon-client-utils-coverage"), 8192U);
    EXPECT_EQ(HashInt8(0), HashInt8(0));
    EXPECT_NE(HashInt8(123456789), HashInt8(-123456789));
}

TEST(BrpcMetaServiceJobCoverageUT, MapsTypesAndRejectsInvalidRequests)
{
    /* Exercise maps Types And rejects invalid Requests and assert the relevant success or failure branch. */
    brpc::Controller controller;
    controller.request_attachment().append("payload", 7);
    falcon::meta_proto::Empty response;
    falcon::meta_proto::MetaRequest request;
    request.set_allow_batch_with_others(true);

    const std::vector<std::pair<falcon::meta_proto::MetaServiceType, FalconMetaServiceType>> cases = {
        {falcon::meta_proto::PLAIN_COMMAND, FalconMetaServiceType::PLAIN_COMMAND},
        {falcon::meta_proto::MKDIR, FalconMetaServiceType::MKDIR},
        {falcon::meta_proto::MKDIR_SUB_MKDIR, FalconMetaServiceType::MKDIR_SUB_MKDIR},
        {falcon::meta_proto::MKDIR_SUB_CREATE, FalconMetaServiceType::MKDIR_SUB_CREATE},
        {falcon::meta_proto::CREATE, FalconMetaServiceType::CREATE},
        {falcon::meta_proto::STAT, FalconMetaServiceType::STAT},
        {falcon::meta_proto::OPEN, FalconMetaServiceType::OPEN},
        {falcon::meta_proto::CLOSE, FalconMetaServiceType::CLOSE},
        {falcon::meta_proto::UNLINK, FalconMetaServiceType::UNLINK},
        {falcon::meta_proto::READDIR, FalconMetaServiceType::READDIR},
        {falcon::meta_proto::OPENDIR, FalconMetaServiceType::OPENDIR},
        {falcon::meta_proto::RMDIR, FalconMetaServiceType::RMDIR},
        {falcon::meta_proto::RMDIR_SUB_RMDIR, FalconMetaServiceType::RMDIR_SUB_RMDIR},
        {falcon::meta_proto::RMDIR_SUB_UNLINK, FalconMetaServiceType::RMDIR_SUB_UNLINK},
        {falcon::meta_proto::RENAME, FalconMetaServiceType::RENAME},
        {falcon::meta_proto::RENAME_SUB_RENAME_LOCALLY, FalconMetaServiceType::RENAME_SUB_RENAME_LOCALLY},
        {falcon::meta_proto::RENAME_SUB_CREATE, FalconMetaServiceType::RENAME_SUB_CREATE},
        {falcon::meta_proto::UTIMENS, FalconMetaServiceType::UTIMENS},
        {falcon::meta_proto::CHOWN, FalconMetaServiceType::CHOWN},
        {falcon::meta_proto::CHMOD, FalconMetaServiceType::CHMOD},
        {falcon::meta_proto::KV_PUT, FalconMetaServiceType::KV_PUT},
        {falcon::meta_proto::KV_GET, FalconMetaServiceType::KV_GET},
        {falcon::meta_proto::KV_DEL, FalconMetaServiceType::KV_DEL},
        {falcon::meta_proto::SLICE_PUT, FalconMetaServiceType::SLICE_PUT},
        {falcon::meta_proto::SLICE_GET, FalconMetaServiceType::SLICE_GET},
        {falcon::meta_proto::SLICE_DEL, FalconMetaServiceType::SLICE_DEL},
        {falcon::meta_proto::FETCH_SLICE_ID, FalconMetaServiceType::FETCH_SLICE_ID},
    };
    for (const auto &[protoType, falconType] : cases) {
        request.add_type(protoType);
        request.add_type(protoType);
        BrpcMetaServiceJob job(&controller, &request, &response, nullptr);
        EXPECT_FALSE(job.IsEmptyRequest());
        EXPECT_TRUE(job.IsAllowBatchProcess());
        EXPECT_EQ(job.GetReqServiceCnt(), 2);
        EXPECT_EQ(job.GetReqDatasize(), 7U);
        char out[8] = {};
        EXPECT_EQ(job.CopyOutData(out, sizeof(out)), 7U);
        EXPECT_STREQ(out, "payload");
        EXPECT_EQ(job.GetFalconMetaServiceType(0), falconType);
        request.clear_type();
        controller.request_attachment().append("payload", 7);
    }

    BrpcMetaServiceJob emptyJob(&controller, &request, &response, nullptr);
    EXPECT_TRUE(emptyJob.IsEmptyRequest());
    EXPECT_FALSE(emptyJob.IsAllowBatchProcess());
    EXPECT_THROW(emptyJob.GetFalconMetaServiceType(0), std::runtime_error);

    request.add_type(falcon::meta_proto::MKDIR);
    request.add_type(falcon::meta_proto::CREATE);
    BrpcMetaServiceJob mixedJob(&controller, &request, &response, nullptr);
    EXPECT_FALSE(mixedJob.IsAllowBatchProcess());
    EXPECT_THROW(mixedJob.GetFalconMetaServiceType(2), std::runtime_error);

    request.clear_type();
    request.add_type(static_cast<falcon::meta_proto::MetaServiceType>(999));
    BrpcMetaServiceJob invalidJob(&controller, &request, &response, nullptr);
    EXPECT_THROW(invalidJob.GetFalconMetaServiceType(0), std::runtime_error);
}

namespace {

bool IsRemoteFailure(FalconErrorCode code)
{
    return code == REMOTE_QUERY_FAILED || code == SERVER_FAULT;
}

void NoopMetaJobDispatch(void *) {}

int ReserveAnyAddressPort()
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    int reuse = 1;
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = 0;
    if (bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    if (listen(fd, 1) != 0) {
        close(fd);
        return -1;
    }
    socklen_t len = sizeof(addr);
    if (getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

void AppendMetaResponse(brpc::Controller *cntl,
                        flatbuffers::FlatBufferBuilder &builder,
                        flatbuffers::Offset<falcon::meta_fbs::MetaResponse> response)
{
    builder.Finish(response);

    SerializedData data;
    ASSERT_TRUE(SerializedDataInit(&data, nullptr, 0, 0, nullptr));
    char *segment = SerializedDataApplyForSegment(&data, builder.GetSize());
    ASSERT_NE(segment, nullptr);
    std::memcpy(segment, builder.GetBufferPointer(), builder.GetSize());
    cntl->response_attachment().append(data.buffer, data.size);
    SerializedDataDestroy(&data);
}

flatbuffers::Offset<falcon::meta_fbs::MetaResponse>
BuildSuccessMetaResponse(flatbuffers::FlatBufferBuilder &builder, falcon::meta_proto::MetaServiceType type, int serverPort)
{
    using namespace falcon::meta_fbs;
    constexpr uint64_t kPgTime = 1000000;
    constexpr uint32_t kMode = S_IFREG | 0644;

    switch (type) {
    case falcon::meta_proto::PLAIN_COMMAND: {
        std::vector<flatbuffers::Offset<flatbuffers::String>> data = {
            builder.CreateString(std::to_string(INT32_MIN)),
            builder.CreateString(std::to_string(INT32_MAX)),
            builder.CreateString("127.0.0.1"),
            builder.CreateString(std::to_string(serverPort - 10)),
            builder.CreateString("8"),
        };
        auto payload = CreatePlainCommandResponseDirect(builder, 1, 5, &data);
        return CreateMetaResponse(builder, SUCCESS, AnyMetaResponse_PlainCommandResponse, payload.Union());
    }
    case falcon::meta_proto::CREATE: {
        auto payload = CreateCreateResponse(builder, 101, 2, 3, kMode, 1, 4, 5, 6, 128, 4096, 1, kPgTime, kPgTime, kPgTime);
        return CreateMetaResponse(builder, SUCCESS, AnyMetaResponse_CreateResponse, payload.Union());
    }
    case falcon::meta_proto::STAT: {
        auto payload = CreateStatResponse(builder, 102, 3, kMode, 1, 4, 5, 6, 256, 4096, 1, kPgTime, kPgTime, kPgTime);
        return CreateMetaResponse(builder, SUCCESS, AnyMetaResponse_StatResponse, payload.Union());
    }
    case falcon::meta_proto::OPEN: {
        auto payload = CreateOpenResponse(builder, 103, 2, 3, kMode, 1, 4, 5, 6, 512, 4096, 1, kPgTime, kPgTime, kPgTime);
        return CreateMetaResponse(builder, SUCCESS, AnyMetaResponse_OpenResponse, payload.Union());
    }
    case falcon::meta_proto::UNLINK: {
        auto payload = CreateUnlinkResponse(builder, 104, 64, 2);
        return CreateMetaResponse(builder, SUCCESS, AnyMetaResponse_UnlinkResponse, payload.Union());
    }
    case falcon::meta_proto::READDIR: {
        std::vector<flatbuffers::Offset<OneReadDirResponse>> entries = {
            CreateOneReadDirResponseDirect(builder, "entry", kMode),
        };
        auto payload = CreateReadDirResponseDirect(builder, 0, "entry", &entries);
        return CreateMetaResponse(builder, SUCCESS, AnyMetaResponse_ReadDirResponse, payload.Union());
    }
    case falcon::meta_proto::OPENDIR: {
        auto payload = CreateOpenDirResponse(builder, 105);
        return CreateMetaResponse(builder, SUCCESS, AnyMetaResponse_OpenDirResponse, payload.Union());
    }
    case falcon::meta_proto::KV_GET: {
        std::vector<uint64_t> keys = {11, 12};
        std::vector<uint64_t> locs = {21, 22};
        std::vector<uint32_t> sizes = {31, 32};
        auto payload = CreateGetKVMetaResponseDirect(builder, 4096, 2, &keys, &locs, &sizes);
        return CreateMetaResponse(builder, SUCCESS, AnyMetaResponse_GetKVMetaResponse, payload.Union());
    }
    case falcon::meta_proto::SLICE_GET: {
        std::vector<uint64_t> inodeIds = {101, 102};
        std::vector<uint32_t> chunkIds = {1, 2};
        std::vector<uint64_t> sliceIds = {201, 202};
        std::vector<uint32_t> values = {7, 8};
        auto payload = CreateSliceInfoResponseDirect(
            builder, 2, &inodeIds, &chunkIds, &sliceIds, &values, &values, &values, &values, &values);
        return CreateMetaResponse(builder, SUCCESS, AnyMetaResponse_SliceInfoResponse, payload.Union());
    }
    case falcon::meta_proto::FETCH_SLICE_ID: {
        auto payload = CreateSliceIdResponse(builder, 1000, 1003);
        return CreateMetaResponse(builder, SUCCESS, AnyMetaResponse_SliceIdResponse, payload.Union());
    }
    default:
        return CreateMetaResponse(builder, SUCCESS);
    }
}

flatbuffers::Offset<falcon::meta_fbs::MetaResponse>
BuildShardTableMetaResponse(flatbuffers::FlatBufferBuilder &builder,
                            const std::vector<std::tuple<int32_t, int32_t, int>> &rows,
                            int serverPort)
{
    using namespace falcon::meta_fbs;
    std::vector<flatbuffers::Offset<flatbuffers::String>> data;
    for (const auto &[minValue, maxValue, serverId] : rows) {
        data.push_back(builder.CreateString(std::to_string(minValue)));
        data.push_back(builder.CreateString(std::to_string(maxValue)));
        data.push_back(builder.CreateString("127.0.0.1"));
        data.push_back(builder.CreateString(std::to_string(serverPort - 10 + serverId)));
        data.push_back(builder.CreateString(std::to_string(serverId)));
    }
    auto payload = CreatePlainCommandResponseDirect(builder, rows.size(), 5, &data);
    return CreateMetaResponse(builder, SUCCESS, AnyMetaResponse_PlainCommandResponse, payload.Union());
}

flatbuffers::Offset<falcon::meta_fbs::MetaResponse>
BuildErrorMetaResponse(flatbuffers::FlatBufferBuilder &builder,
                       falcon::meta_proto::MetaServiceType type,
                       FalconErrorCode errorCode,
                       int serverPort)
{
    if (type == falcon::meta_proto::PLAIN_COMMAND) {
        return BuildSuccessMetaResponse(builder, type, serverPort);
    }
    if (type == falcon::meta_proto::CREATE && errorCode == FILE_EXISTS) {
        auto payload = falcon::meta_fbs::CreateCreateResponse(
            builder, 201, 2, S_IFREG | 0644, 1, getuid(), getgid(), 0, 0, 4096, 1, 1000000, 1000000, 1000000);
        return falcon::meta_fbs::CreateMetaResponse(
            builder, FILE_EXISTS, falcon::meta_fbs::AnyMetaResponse_CreateResponse, payload.Union());
    }
    return falcon::meta_fbs::CreateMetaResponse(builder, errorCode);
}

class MockMetaService : public falcon::meta_proto::MetaService {
  public:
    explicit MockMetaService(int serverPort)
        : serverPort(serverPort)
    {
    }

    void MetaCall(google::protobuf::RpcController *cntlBase,
                  const falcon::meta_proto::MetaRequest *request,
                  falcon::meta_proto::Empty *,
                  google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        auto *cntl = static_cast<brpc::Controller *>(cntlBase);
        flatbuffers::FlatBufferBuilder builder;
        auto type = request->type_size() > 0 ? request->type(0) : falcon::meta_proto::PLAIN_COMMAND;
        AppendMetaResponse(cntl, builder, BuildSuccessMetaResponse(builder, type, serverPort));
    }

  private:
    int serverPort;
};

class ShardTableMetaService : public falcon::meta_proto::MetaService {
  public:
    ShardTableMetaService(int serverPort, std::vector<std::tuple<int32_t, int32_t, int>> rows)
        : serverPort(serverPort), rows(std::move(rows))
    {
    }

    void MetaCall(google::protobuf::RpcController *cntlBase,
                  const falcon::meta_proto::MetaRequest *request,
                  falcon::meta_proto::Empty *,
                  google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        auto *cntl = static_cast<brpc::Controller *>(cntlBase);
        flatbuffers::FlatBufferBuilder builder;
        if (request->type_size() > 0 && request->type(0) == falcon::meta_proto::PLAIN_COMMAND) {
            AppendMetaResponse(cntl, builder, BuildShardTableMetaResponse(builder, rows, serverPort));
        } else {
            AppendMetaResponse(cntl, builder, BuildSuccessMetaResponse(builder, request->type(0), serverPort));
        }
    }

  private:
    int serverPort;
    std::vector<std::tuple<int32_t, int32_t, int>> rows;
};

class FaultShardTableMetaService : public falcon::meta_proto::MetaService {
  public:
    explicit FaultShardTableMetaService(int serverPort)
        : serverPort(serverPort)
    {
    }

    void MetaCall(google::protobuf::RpcController *cntlBase,
                  const falcon::meta_proto::MetaRequest *request,
                  falcon::meta_proto::Empty *,
                  google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        auto *cntl = static_cast<brpc::Controller *>(cntlBase);
        flatbuffers::FlatBufferBuilder builder;
        auto type = request->type_size() > 0 ? request->type(0) : falcon::meta_proto::PLAIN_COMMAND;
        if (type == falcon::meta_proto::PLAIN_COMMAND) {
            AppendMetaResponse(cntl, builder, falcon::meta_fbs::CreateMetaResponse(builder, SERVER_FAULT));
            return;
        }
        AppendMetaResponse(cntl, builder, BuildSuccessMetaResponse(builder, type, serverPort));
    }

  private:
    int serverPort;
};

class ErrorMetaService : public falcon::meta_proto::MetaService {
  public:
    explicit ErrorMetaService(int serverPort)
        : serverPort(serverPort)
    {
    }

    void SetError(falcon::meta_proto::MetaServiceType type, FalconErrorCode errorCode)
    {
        errors[type] = errorCode;
    }

    void MetaCall(google::protobuf::RpcController *cntlBase,
                  const falcon::meta_proto::MetaRequest *request,
                  falcon::meta_proto::Empty *,
                  google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        auto *cntl = static_cast<brpc::Controller *>(cntlBase);
        flatbuffers::FlatBufferBuilder builder;
        auto type = request->type_size() > 0 ? request->type(0) : falcon::meta_proto::PLAIN_COMMAND;
        auto it = errors.find(type);
        if (it == errors.end()) {
            AppendMetaResponse(cntl, builder, BuildSuccessMetaResponse(builder, type, serverPort));
        } else {
            AppendMetaResponse(cntl, builder, BuildErrorMetaResponse(builder, type, it->second, serverPort));
        }
    }

  private:
    int serverPort;
    std::unordered_map<falcon::meta_proto::MetaServiceType, FalconErrorCode> errors;
};

class DirectoryStatMetaService : public falcon::meta_proto::MetaService {
  public:
    explicit DirectoryStatMetaService(int serverPort)
        : serverPort(serverPort)
    {
    }

    void MetaCall(google::protobuf::RpcController *cntlBase,
                  const falcon::meta_proto::MetaRequest *request,
                  falcon::meta_proto::Empty *,
                  google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard doneGuard(done);
        auto *cntl = static_cast<brpc::Controller *>(cntlBase);
        flatbuffers::FlatBufferBuilder builder;
        auto type = request->type_size() > 0 ? request->type(0) : falcon::meta_proto::PLAIN_COMMAND;
        if (type == falcon::meta_proto::STAT) {
            constexpr uint64_t kPgTime = 1000000;
            auto payload = falcon::meta_fbs::CreateStatResponse(
                builder, 301, 1, S_IFDIR | 0755, 1, getuid(), getgid(), 0, 0, 4096, 1, kPgTime, kPgTime, kPgTime);
            AppendMetaResponse(
                cntl,
                builder,
                falcon::meta_fbs::CreateMetaResponse(
                    builder, SUCCESS, falcon::meta_fbs::AnyMetaResponse_StatResponse, payload.Union()));
            return;
        }
        AppendMetaResponse(cntl, builder, BuildSuccessMetaResponse(builder, type, serverPort));
    }

  private:
    int serverPort;
};

int GetUnusedLoopbackPort()
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }
    socklen_t len = sizeof(addr);
    if (getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len) != 0) {
        close(fd);
        return -1;
    }
    int port = ntohs(addr.sin_port);
    close(fd);
    return port;
}

int CollectReaddirFiller(void *buf, const char *name, const struct stat *st, off_t off)
{
    auto *entries = static_cast<std::vector<std::string> *>(buf);
    entries->push_back(name);
    EXPECT_GE(off, 0);
    if (st != nullptr) {
        EXPECT_NE(st->st_mode, 0U);
    }
    return 0;
}

int StopAfterFirstReaddirFiller(void *buf, const char *name, const struct stat *, off_t)
{
    auto *entries = static_cast<std::vector<std::string> *>(buf);
    entries->push_back(name);
    return entries->size() >= 1 ? 1 : 0;
}

int StopAfterFirstRealEntryFiller(void *buf, const char *name, const struct stat *st, off_t)
{
    if (st == nullptr) {
        return 0;
    }
    auto *entries = static_cast<std::vector<std::string> *>(buf);
    entries->push_back(name);
    return 1;
}

}  // namespace

TEST(FalconBrpcServerCoverageUT, RejectsOccupiedPortAndMissingStop)
{
    /* Exercise rejects Occupied Port And missing Stop and assert the relevant success or failure branch. */
    int reservedFd = ReserveAnyAddressPort();
    ASSERT_GE(reservedFd, 0);
    sockaddr_in addr {};
    socklen_t len = sizeof(addr);
    ASSERT_EQ(getsockname(reservedFd, reinterpret_cast<sockaddr *>(&addr), &len), 0);
    int reservedPort = ntohs(addr.sin_port);
    ASSERT_GT(reservedPort, 0);

    EXPECT_EQ(StartFalconCommunicationServer(NoopMetaJobDispatch, "not-an-ip-address", reservedPort), 1);
    close(reservedFd);
    EXPECT_EQ(StopFalconCommunicationServer(), 0);
    EXPECT_EQ(StopFalconCommunicationServer(), 1);
}

TEST(FalconClientConnectionUT, UnreachableEndpointCoversRequestBuilders)
{
    /* Exercise Unreachable Endpoint covers Request Builders and assert the relevant success or failure branch. */
    ServerIdentifier defaultServer;
    (void)defaultServer;
    EXPECT_THROW(Connection(ServerIdentifier("bad endpoint", 1, 0)), std::runtime_error);

    Connection conn(ServerIdentifier("127.0.0.1", 1, 7));
    ConnectionCache cache;
    const char *path = "/connection_coverage";
    const char *renamed = "/connection_coverage_renamed";
    struct stat stbuf {};
    uint64_t inodeId = 0;
    int64_t size = 0;
    int32_t nodeId = 0;

    Connection::PlainCommandResult plainResult;
    EXPECT_TRUE(IsRemoteFailure(conn.PlainCommand("select 1", plainResult, &cache)));
    EXPECT_TRUE(IsRemoteFailure(conn.Mkdir(path, &cache)));
    EXPECT_TRUE(IsRemoteFailure(conn.Create(path, inodeId, nodeId, &stbuf, &cache)));
    EXPECT_TRUE(IsRemoteFailure(conn.Stat(path, &stbuf, &cache)));
    EXPECT_TRUE(IsRemoteFailure(conn.Open(path, inodeId, size, nodeId, &stbuf, &cache)));
    EXPECT_TRUE(IsRemoteFailure(conn.Close(path, 8, 123456, nodeId, &cache)));
    EXPECT_TRUE(IsRemoteFailure(conn.Unlink(path, inodeId, size, nodeId, &cache)));

    Connection::ReadDirResponse readDirResponse;
    EXPECT_TRUE(IsRemoteFailure(conn.ReadDir(path, readDirResponse, 2, 1, "last", &cache)));
    EXPECT_TRUE(IsRemoteFailure(conn.OpenDir(path, inodeId, &cache)));
    EXPECT_TRUE(IsRemoteFailure(conn.Rmdir(path, &cache)));
    EXPECT_TRUE(IsRemoteFailure(conn.Rename(path, renamed, &cache)));
    EXPECT_TRUE(IsRemoteFailure(conn.UtimeNs(path, 11, 22, &cache)));
    EXPECT_TRUE(IsRemoteFailure(conn.Chown(path, 1000, 1000, &cache)));
    EXPECT_TRUE(IsRemoteFailure(conn.Chmod(path, 0644, &cache)));

    std::vector<uint64_t> valueKey = {1, 2};
    std::vector<uint64_t> location = {3, 4};
    std::vector<uint32_t> kvSize = {5, 6};
    EXPECT_TRUE(IsRemoteFailure(conn.KvPut("key", 16, 2, valueKey, location, kvSize, &cache)));
    Connection::KvGetResult kvResult;
    EXPECT_TRUE(IsRemoteFailure(conn.KvGet("key", kvResult, &cache)));
    EXPECT_TRUE(IsRemoteFailure(conn.KvDel("key", &cache)));

    std::vector<uint64_t> inodeIds = {10, 11};
    std::vector<uint32_t> chunkIds = {1, 2};
    std::vector<uint64_t> sliceIds = {20, 21};
    std::vector<uint32_t> sliceSizes = {4096, 8192};
    std::vector<uint32_t> sliceOffsets = {0, 4096};
    std::vector<uint32_t> sliceLens = {128, 256};
    std::vector<uint32_t> sliceLoc1 = {30, 31};
    std::vector<uint32_t> sliceLoc2 = {40, 41};
    EXPECT_TRUE(IsRemoteFailure(conn.SlicePut(path,
                                              2,
                                              inodeIds,
                                              chunkIds,
                                              sliceIds,
                                              sliceSizes,
                                              sliceOffsets,
                                              sliceLens,
                                              sliceLoc1,
                                              sliceLoc2,
                                              &cache)));
    Connection::SliceGetResult sliceResult;
    EXPECT_TRUE(IsRemoteFailure(conn.SliceGet(path, 10, 1, sliceResult, &cache)));
    EXPECT_TRUE(IsRemoteFailure(conn.SliceDel(path, 10, 1, &cache)));
    uint64_t startId = 0;
    uint64_t endId = 0;
    EXPECT_TRUE(IsRemoteFailure(conn.FetchSliceId(4, 1, startId, endId, &cache)));
}

TEST(FalconClientConnectionUT, MockServerCoversSuccessfulResponseHandlers)
{
    /* Exercise Mock Server covers Successful Response Handlers and assert the relevant success or failure branch. */
    int port = GetUnusedLoopbackPort();
    ASSERT_GT(port, 0);

    MockMetaService service(port);
    brpc::Server server;
    ASSERT_EQ(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server.Start(port, nullptr), 0);

    Connection conn(ServerIdentifier("127.0.0.1", port, 8));
    ConnectionCache cache;
    const char *path = "/connection_mock";
    struct stat stbuf {};
    uint64_t inodeId = 0;
    int64_t size = 0;
    int32_t nodeId = 0;

    Connection::PlainCommandResult plainResult;
    EXPECT_EQ(conn.PlainCommand("select 1", plainResult, &cache), SUCCESS);
    ASSERT_NE(plainResult.response, nullptr);
    EXPECT_EQ(plainResult.response->row(), 1U);
    EXPECT_EQ(conn.Mkdir(path, &cache), SUCCESS);
    EXPECT_EQ(conn.Create(path, inodeId, nodeId, &stbuf, &cache), SUCCESS);
    EXPECT_EQ(inodeId, 101U);
    EXPECT_EQ(nodeId, 2);
    EXPECT_EQ(conn.Stat(path, &stbuf, &cache), SUCCESS);
    EXPECT_EQ(stbuf.st_ino, 102U);
    EXPECT_EQ(conn.Open(path, inodeId, size, nodeId, &stbuf, &cache), SUCCESS);
    EXPECT_EQ(inodeId, 103U);
    EXPECT_EQ(size, 512);
    EXPECT_EQ(conn.Close(path, size, 123456, nodeId, &cache), SUCCESS);
    EXPECT_EQ(conn.Unlink(path, inodeId, size, nodeId, &cache), SUCCESS);
    EXPECT_EQ(inodeId, 104U);

    Connection::ReadDirResponse readDirResponse;
    EXPECT_EQ(conn.ReadDir(path, readDirResponse, 2, 1, "last", &cache), SUCCESS);
    ASSERT_NE(readDirResponse.response, nullptr);
    ASSERT_NE(readDirResponse.response->result_list(), nullptr);
    EXPECT_EQ(readDirResponse.response->result_list()->size(), 1U);
    EXPECT_EQ(conn.OpenDir(path, inodeId, &cache), SUCCESS);
    EXPECT_EQ(inodeId, 105U);
    EXPECT_EQ(conn.Rmdir(path, &cache), SUCCESS);
    EXPECT_EQ(conn.Rename(path, "/connection_mock_renamed", &cache), SUCCESS);
    EXPECT_EQ(conn.UtimeNs(path, 11, 22, &cache), SUCCESS);
    EXPECT_EQ(conn.Chown(path, 1000, 1000, &cache), SUCCESS);
    EXPECT_EQ(conn.Chmod(path, 0644, &cache), SUCCESS);

    std::vector<uint64_t> valueKey = {1, 2};
    std::vector<uint64_t> location = {3, 4};
    std::vector<uint32_t> kvSize = {5, 6};
    EXPECT_EQ(conn.KvPut("key", 16, 2, valueKey, location, kvSize, &cache), SUCCESS);
    Connection::KvGetResult kvResult;
    EXPECT_EQ(conn.KvGet("key", kvResult, &cache), SUCCESS);
    EXPECT_EQ(kvResult.sliceNum, 2U);
    EXPECT_EQ(kvResult.slices.size(), 2U);
    EXPECT_EQ(conn.KvDel("key", &cache), SUCCESS);

    std::vector<uint64_t> inodeIds = {10, 11};
    std::vector<uint32_t> chunkIds = {1, 2};
    std::vector<uint64_t> sliceIds = {20, 21};
    std::vector<uint32_t> sliceValues = {30, 31};
    EXPECT_EQ(conn.SlicePut(path,
                            2,
                            inodeIds,
                            chunkIds,
                            sliceIds,
                            sliceValues,
                            sliceValues,
                            sliceValues,
                            sliceValues,
                            sliceValues,
                            &cache),
              SUCCESS);
    Connection::SliceGetResult sliceResult;
    EXPECT_EQ(conn.SliceGet(path, 10, 1, sliceResult, &cache), SUCCESS);
    EXPECT_EQ(sliceResult.sliceNum, 2U);
    EXPECT_EQ(sliceResult.sliceId.size(), 2U);
    EXPECT_EQ(conn.SliceDel(path, 10, 1, &cache), SUCCESS);
    uint64_t startId = 0;
    uint64_t endId = 0;
    EXPECT_EQ(conn.FetchSliceId(4, 1, startId, endId, &cache), SUCCESS);
    EXPECT_EQ(startId, 1000U);
    EXPECT_EQ(endId, 1003U);

    server.Stop(0);
    server.Join();
}

TEST(FalconClientMetaUT, MockRouterCoversMetadataSuccessFlow)
{
    /* Exercise Mock Router covers Metadata Success flow and assert the relevant success or failure branch. */
    int port = GetUnusedLoopbackPort();
    ASSERT_GT(port, 10);

    MockMetaService service(port);
    brpc::Server server;
    ASSERT_EQ(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server.Start(port, nullptr), 0);

    router = std::make_shared<Router>(ServerIdentifier("127.0.0.1", port));
    ASSERT_NE(router, nullptr);
    EXPECT_NE(router->GetCoordinatorConn(), nullptr);
    EXPECT_EQ(router->GetWorkerConnByPath("relative"), nullptr);
    EXPECT_EQ(router->GetWorkerConnByPath(""), nullptr);
    EXPECT_NE(router->GetWorkerConnByPath("/mock_file"), nullptr);
    EXPECT_NE(router->GetWorkerConnByPath("/mock_dir/"), nullptr);
    std::unordered_map<std::string, std::shared_ptr<Connection>> workers;
    EXPECT_EQ(router->GetAllWorkerConnection(workers), SUCCESS);
    ASSERT_EQ(workers.size(), 1U);
    EXPECT_NE(router->GetWorkerConnBySvrId(8), nullptr);
    EXPECT_THROW(router->TryToUpdateWorkerConn(std::make_shared<Connection>(ServerIdentifier("127.0.0.1", port, 99))),
                 std::runtime_error);
    EXPECT_EQ(router->FetchShardTable(router->GetCoordinatorConn()), SUCCESS);

    EXPECT_EQ(FalconMkdir("/mock_dir"), SUCCESS);

    struct stat stbuf {};
    uint64_t invalidFd = 0;
    EXPECT_EQ(FalconCreate("relative", invalidFd, O_RDWR | O_CREAT, &stbuf), PROGRAM_ERROR);
    EXPECT_EQ(FalconGetStat("relative", &stbuf), PROGRAM_ERROR);
    EXPECT_EQ(FalconOpen("relative", O_RDONLY, invalidFd, &stbuf), PROGRAM_ERROR);
    EXPECT_EQ(FalconUnlink("relative"), PROGRAM_ERROR);
    EXPECT_EQ(FalconUtimens("relative", 11, 22), PROGRAM_ERROR);
    EXPECT_EQ(FalconChown("relative", getuid(), getgid()), PROGRAM_ERROR);
    EXPECT_EQ(FalconChmod("relative", 0644), PROGRAM_ERROR);

    EXPECT_EQ(FalconGetStat("/mock_file", &stbuf), SUCCESS);
    EXPECT_EQ(stbuf.st_ino, 102U);

    uint64_t fd = 0;
    EXPECT_EQ(FalconCreate("/mock_file", fd, O_RDWR | O_CREAT, &stbuf), SUCCESS);
    ASSERT_NE(fd, 0U);
    EXPECT_EQ(FalconClose("/mock_file", fd), SUCCESS);

    fd = 0;
    EXPECT_EQ(FalconOpen("/mock_file", O_RDWR, fd, &stbuf), SUCCESS);
    ASSERT_NE(fd, 0U);
    EXPECT_EQ(FalconFsync("/mock_file", fd, 0), SUCCESS);
    EXPECT_EQ(FalconClose("/mock_file", fd), SUCCESS);

    FalconFuseInfo fi {};
    EXPECT_EQ(FalconOpenDir("/mock_dir", &fi), SUCCESS);
    std::vector<std::string> entries;
    EXPECT_EQ(FalconReadDir("/mock_dir", &entries, CollectReaddirFiller, 0, &fi), SUCCESS);
    EXPECT_NE(std::find(entries.begin(), entries.end(), "entry"), entries.end());
    EXPECT_EQ(FalconCloseDir(fi.fh), SUCCESS);

    EXPECT_EQ(FalconOpenDir("/mock_dir", &fi), SUCCESS);
    std::vector<std::string> stoppedEntries;
    EXPECT_EQ(FalconReadDir("/mock_dir", &stoppedEntries, StopAfterFirstRealEntryFiller, 0, &fi), SUCCESS);
    EXPECT_EQ(stoppedEntries.size(), 1U);
    EXPECT_EQ(FalconCloseDir(fi.fh), SUCCESS);

    EXPECT_EQ(FalconRename("/mock_file", "/mock_file_renamed"), SUCCESS);
    EXPECT_EQ(FalconUtimens("/mock_file_renamed", 11, 22), SUCCESS);
    EXPECT_EQ(FalconChown("/mock_file_renamed", getuid(), getgid()), SUCCESS);
    EXPECT_EQ(FalconChmod("/mock_file_renamed", 0644), SUCCESS);
    EXPECT_EQ(FalconRmDir("/mock_dir"), SUCCESS);

    router.reset();
    server.Stop(0);
    server.Join();
}

TEST(FalconClientMetaUT, LocalFdCloseBranchesWithoutService)
{
    /* Exercise local Fd Close branches Without Service and assert the relevant success or failure branch. */
    uint64_t readFailFd =
        FalconFd::GetInstance()->AttachFd(93001, O_RDONLY, nullptr, 8, "/fd/read-failed", 0);
    ASSERT_NE(readFailFd, UINT64_MAX);
    auto readFailInstance = FalconFd::GetInstance()->GetOpenInstanceByFd(readFailFd);
    ASSERT_NE(readFailInstance, nullptr);
    readFailInstance->readFail = true;
    EXPECT_EQ(FalconClose(readFailInstance->path, readFailFd), -EIO);

    uint64_t datasyncFd =
        FalconFd::GetInstance()->AttachFd(93002, O_RDONLY, nullptr, 4, "/fd/datasync", 0);
    ASSERT_NE(datasyncFd, UINT64_MAX);
    auto datasyncInstance = FalconFd::GetInstance()->GetOpenInstanceByFd(datasyncFd);
    ASSERT_NE(datasyncInstance, nullptr);
    datasyncInstance->currentSize = 9;
    EXPECT_EQ(FalconFsync(datasyncInstance->path, datasyncFd, 1), SUCCESS);
    EXPECT_EQ(FalconFd::GetInstance()->DeleteOpenInstance(datasyncFd), 0);
}

TEST(FalconClientMetaUT, CreateReturnsEmfileWhenFdTableIsFull)
{
    /* Exercise Create Returns Emfile When Fd Table Is Full and assert the relevant success or failure branch. */
    int port = GetUnusedLoopbackPort();
    ASSERT_GT(port, 10);

    MockMetaService service(port);
    brpc::Server server;
    ASSERT_EQ(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server.Start(port, nullptr), 0);

    router = std::make_shared<Router>(ServerIdentifier("127.0.0.1", port));
    ASSERT_NE(router, nullptr);

    SetMaxOpenInstanceNum(1);
    uint64_t reservedFd =
        FalconFd::GetInstance()->AttachFd(93003, O_RDONLY, nullptr, 0, "/fd/reserved-emfile", 0);
    ASSERT_NE(reservedFd, UINT64_MAX);
    SetMaxOpenInstanceNum(FalconFd::GetInstance()->GetCurrentOpenInstanceCount());
    uint64_t fd = 0;
    struct stat stbuf {};
    EXPECT_EQ(FalconCreate("/emfile_file", fd, O_CREAT, &stbuf), -EMFILE);
    EXPECT_EQ(FalconFd::GetInstance()->DeleteOpenInstance(reservedFd), 0);
    SetMaxOpenInstanceNum(40000);

    router.reset();
    server.Stop(0);
    server.Join();
}

TEST(FalconClientMetaUT, RenamePersistRejectsDirectoriesBeforeCopy)
{
    /* Exercise Rename Persist rejects Directories Before Copy and assert the relevant success or failure branch. */
    int port = GetUnusedLoopbackPort();
    ASSERT_GT(port, 10);

    DirectoryStatMetaService service(port);
    brpc::Server server;
    ASSERT_EQ(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server.Start(port, nullptr), 0);

    router = std::make_shared<Router>(ServerIdentifier("127.0.0.1", port));
    ASSERT_NE(router, nullptr);
    EXPECT_EQ(FalconRenamePersist("/mock_dir", "/mock_dir_renamed"), -EOPNOTSUPP);
    bool oldPersist = g_persist;
    g_persist = true;
    EXPECT_EQ(DoRename("/mock_dir", "/mock_dir_renamed"), -EOPNOTSUPP);
    g_persist = oldPersist;

    router.reset();
    server.Stop(0);
    server.Join();
}

TEST(FalconClientMetaUT, ReaddirCachedEntriesWithoutService)
{
    /* Exercise Readdir Cached Entries Without Service and assert the relevant success or failure branch. */
    uint64_t fd = FalconFd::GetInstance()->AttachDirFd(93004);
    ASSERT_NE(fd, UINT64_MAX);
    DirOpenInstance *dirOpenInstance = FalconFd::GetInstance()->GetDirOpenInstanceByFd(fd);
    ASSERT_NE(dirOpenInstance, nullptr);
    dirOpenInstance->partialEntryVec = {"cached-a", "cached-b"};
    dirOpenInstance->fileModes = {S_IFREG | 0644, S_IFDIR | 0755};

    FalconFuseInfo fi {};
    fi.fh = fd;
    std::vector<std::string> stoppedEntries;
    EXPECT_EQ(FalconReadDir("/cached_dir", &stoppedEntries, StopAfterFirstRealEntryFiller, 1, &fi), SUCCESS);
    ASSERT_EQ(stoppedEntries.size(), 1U);
    EXPECT_EQ(stoppedEntries[0], "cached-a");
    EXPECT_EQ(dirOpenInstance->offset, 0U);

    std::vector<std::string> entries;
    EXPECT_EQ(FalconReadDir("/cached_dir", &entries, CollectReaddirFiller, 2, &fi), SUCCESS);
    EXPECT_EQ(entries, (std::vector<std::string>{"cached-a", "cached-b"}));
    EXPECT_EQ(dirOpenInstance->offset, dirOpenInstance->partialEntryVec.size());
    EXPECT_EQ(FalconFd::GetInstance()->DeleteDirOpenInstance(fd), 0);
}

TEST(FalconClientMetaUT, RouterRejectsCorruptShardTables)
{
    /* Exercise Router rejects Corrupt Shard Tables and assert the relevant success or failure branch. */
    const std::vector<std::vector<std::tuple<int32_t, int32_t, int>>> corruptTables = {
        {{INT32_MIN, 10, 1}, {12, INT32_MAX, 2}},
        {{INT32_MIN, 10, 1}},
    };

    for (const auto &rows : corruptTables) {
        int port = GetUnusedLoopbackPort();
        ASSERT_GT(port, 10);

        ShardTableMetaService service(port, rows);
        brpc::Server server;
        ASSERT_EQ(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
        ASSERT_EQ(server.Start(port, nullptr), 0);
        EXPECT_THROW(Router(ServerIdentifier("127.0.0.1", port)), std::runtime_error);
        server.Stop(0);
        server.Join();
    }
}

TEST(FalconClientMetaUT, RouterUpdateAndFailureBranches)
{
    /* Exercise Router Update And Failure branches and assert the relevant success or failure branch. */
    int port = GetUnusedLoopbackPort();
    ASSERT_GT(port, 10);

    FaultShardTableMetaService faultService(port);
    brpc::Server faultServer;
    ASSERT_EQ(faultServer.AddService(&faultService, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(faultServer.Start(port, nullptr), 0);
    EXPECT_THROW(Router(ServerIdentifier("127.0.0.1", port)), std::runtime_error);
    faultServer.Stop(0);
    faultServer.Join();

    port = GetUnusedLoopbackPort();
    ASSERT_GT(port, 10);
    ShardTableMetaService service(port, {{INT32_MIN, INT32_MAX, 8}});
    brpc::Server server;
    ASSERT_EQ(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server.Start(port, nullptr), 0);

    router = std::make_shared<Router>(ServerIdentifier("127.0.0.1", port));
    ASSERT_NE(router, nullptr);
    auto currentCn = router->GetCoordinatorConn();
    auto staleCn = std::make_shared<Connection>(ServerIdentifier("127.0.0.1", port + 1, currentCn->server.id));
    EXPECT_EQ(router->TryToUpdateCNConn(staleCn), currentCn);

    auto currentWorker = router->GetWorkerConnBySvrId(8);
    ASSERT_NE(currentWorker, nullptr);
    auto staleWorker = std::make_shared<Connection>(ServerIdentifier("127.0.0.1", port + 123, 8));
    EXPECT_EQ(router->TryToUpdateWorkerConn(staleWorker), currentWorker);
    EXPECT_EQ(router->TryToUpdateWorkerConn(currentWorker), currentWorker);

    router.reset();
    server.Stop(0);
    server.Join();
}

TEST(FalconClientMetaUT, MetadataFunctionsReturnServerErrors)
{
    /* Exercise Metadata Functions Return Server Errors and assert the relevant success or failure branch. */
    int port = GetUnusedLoopbackPort();
    ASSERT_GT(port, 10);

    ErrorMetaService service(port);
    service.SetError(falcon::meta_proto::MKDIR, PROGRAM_ERROR);
    service.SetError(falcon::meta_proto::CREATE, FILE_EXISTS);
    service.SetError(falcon::meta_proto::STAT, FILE_NOT_EXISTS);
    service.SetError(falcon::meta_proto::OPEN, FILE_NOT_EXISTS);
    service.SetError(falcon::meta_proto::CLOSE, PROGRAM_ERROR);
    service.SetError(falcon::meta_proto::UNLINK, FILE_NOT_EXISTS);
    service.SetError(falcon::meta_proto::OPENDIR, FILE_NOT_EXISTS);
    service.SetError(falcon::meta_proto::RMDIR, FILE_NOT_EXISTS);
    service.SetError(falcon::meta_proto::RENAME, FILE_NOT_EXISTS);
    service.SetError(falcon::meta_proto::UTIMENS, FILE_NOT_EXISTS);
    service.SetError(falcon::meta_proto::CHOWN, FILE_NOT_EXISTS);
    service.SetError(falcon::meta_proto::CHMOD, FILE_NOT_EXISTS);

    brpc::Server server;
    ASSERT_EQ(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server.Start(port, nullptr), 0);

    router = std::make_shared<Router>(ServerIdentifier("127.0.0.1", port));
    ASSERT_NE(router, nullptr);

    struct stat stbuf {};
    EXPECT_EQ(FalconMkdir("/error_dir"), PROGRAM_ERROR);
    EXPECT_EQ(FalconGetStat("/error_file", &stbuf), FILE_NOT_EXISTS);
    service.SetError(falcon::meta_proto::STAT, PROGRAM_ERROR);
    EXPECT_EQ(FalconGetStat("/error_file", &stbuf), PROGRAM_ERROR);
    service.SetError(falcon::meta_proto::STAT, FILE_NOT_EXISTS);

    uint64_t fd = 0;
    EXPECT_EQ(FalconCreate("/error_file", fd, O_CREAT, &stbuf), SUCCESS);
    if (fd != 0 && fd != UINT64_MAX) {
        auto openInstance = FalconFd::GetInstance()->GetOpenInstanceByFd(fd);
        ASSERT_NE(openInstance, nullptr);
        openInstance->currentSize = openInstance->originalSize + 1;
        EXPECT_EQ(FalconClose("/error_file", fd), PROGRAM_ERROR);
        FalconFd::GetInstance()->DeleteOpenInstance(fd);
    }
    fd = 0;
    EXPECT_EQ(FalconOpen("/error_file", O_RDONLY, fd, &stbuf), FILE_NOT_EXISTS);

    FalconFuseInfo fi {};
    EXPECT_EQ(FalconOpenDir("/error_dir", &fi), FILE_NOT_EXISTS);
    EXPECT_EQ(FalconUnlink("/error_file"), FILE_NOT_EXISTS);
    EXPECT_EQ(FalconRmDir("/error_dir"), FILE_NOT_EXISTS);
    EXPECT_EQ(FalconRename("/error_file", "/error_file_renamed"), FILE_NOT_EXISTS);
    EXPECT_EQ(FalconUtimens("/error_file", 11, 22), FILE_NOT_EXISTS);
    EXPECT_EQ(FalconChown("/error_file", getuid(), getgid()), FILE_NOT_EXISTS);
    EXPECT_EQ(FalconChmod("/error_file", 0644), FILE_NOT_EXISTS);

    router.reset();
    server.Stop(0);
    server.Join();
}

TEST(FalconClientMetaUT, ReaddirReturnsServerError)
{
    /* Exercise Readdir Returns Server Error and assert the relevant success or failure branch. */
    int port = GetUnusedLoopbackPort();
    ASSERT_GT(port, 10);

    ErrorMetaService service(port);
    service.SetError(falcon::meta_proto::READDIR, PROGRAM_ERROR);

    brpc::Server server;
    ASSERT_EQ(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server.Start(port, nullptr), 0);

    router = std::make_shared<Router>(ServerIdentifier("127.0.0.1", port));
    ASSERT_NE(router, nullptr);

    FalconFuseInfo fi {};
    EXPECT_EQ(FalconOpenDir("/error_dir", &fi), SUCCESS);
    std::vector<std::string> entries;
    EXPECT_EQ(FalconReadDir("/error_dir", &entries, CollectReaddirFiller, 0, &fi), PROGRAM_ERROR);
    EXPECT_EQ(FalconCloseDir(fi.fh), SUCCESS);

    router.reset();
    server.Stop(0);
    server.Join();
}

TEST(FalconClientMetaUT, InvalidFdOperationsReturnErrorsWithoutService)
{
    /* Exercise invalid Fd Operations Return Errors Without Service and assert the relevant success or failure branch. */
    const uint64_t missing_fd = UINT64_MAX - 7;
    char read_buffer[8] = {};
    const char write_buffer[] = "falcon";

    EXPECT_EQ(FalconWrite(missing_fd, "/missing", write_buffer, sizeof(write_buffer), 0), -EBADF);
    EXPECT_EQ(FalconRead("/missing", missing_fd, read_buffer, sizeof(read_buffer), 0), -EBADF);
    EXPECT_EQ(FalconClose("/missing", missing_fd, false, -1), NOT_FOUND_FD);
    EXPECT_EQ(FalconClose("/missing", missing_fd, true, -1), NOT_FOUND_FD);
    EXPECT_EQ(FalconFsync("/missing", missing_fd, 0), NOT_FOUND_FD);
    EXPECT_EQ(FalconFsync("/missing", missing_fd, 1), NOT_FOUND_FD);
    EXPECT_EQ(FalconCloseDir(missing_fd), NOT_FOUND_FD);
}

TEST(FalconClientMetaUT, FalconFdLifecycleBranchesWithoutService)
{
    /* Exercise Falcon Fd Lifecycle branches Without Service and assert the relevant success or failure branch. */
    auto *fdTable = FalconFd::GetInstance();
    EXPECT_EQ(fdTable->DeleteOpenInstance(UINT64_MAX), 0);

    auto instance = std::make_shared<OpenInstance>();
    instance->inodeId = 91001;
    uint64_t attached = fdTable->AttachFd("/fd_lifecycle", instance);
    ASSERT_NE(attached, UINT64_MAX);
    EXPECT_EQ(fdTable->GetOpenInstanceByFd(attached), instance);
    EXPECT_FALSE(fdTable->GetInodetoOpenInstanceSet(instance->inodeId).empty());
    EXPECT_EQ(fdTable->DeleteOpenInstance(attached, false), 0);
    EXPECT_TRUE(fdTable->GetInodetoOpenInstanceSet(instance->inodeId).empty());

    auto duplicate = std::make_shared<OpenInstance>();
    fdTable->AddOpenInstance(424242, duplicate);
    fdTable->AddOpenInstance(424242, std::make_shared<OpenInstance>());
    EXPECT_EQ(fdTable->DeleteOpenInstance(424242, false), 0);

    uint64_t dirFd = fdTable->AttachDirFd(1);
    ASSERT_NE(dirFd, UINT64_MAX);
    ASSERT_NE(fdTable->GetDirOpenInstanceByFd(dirFd), nullptr);
    auto *duplicateDir = new DirOpenInstance(dirFd);
    EXPECT_EQ(fdTable->AddDirOpenInstance(dirFd, duplicateDir), -EBADF);
    delete duplicateDir;
    EXPECT_EQ(fdTable->DeleteDirOpenInstance(dirFd), 0);

    SetMaxOpenInstanceNum(1);
    auto limited = fdTable->WaitGetNewOpenInstance();
    ASSERT_NE(limited, nullptr);
    EXPECT_EQ(fdTable->GetCurrentOpenInstanceCount(), 1U);
    EXPECT_EQ(fdTable->WaitGetNewOpenInstance(), nullptr);
    fdTable->ReleaseOpenInstance();
    SetMaxOpenInstanceNum(40000);
}

TEST(FalconClientFuseWrapperUT, InvalidArgumentsReturnEinval)
{
    /* Exercise invalid Arguments Return Einval and assert the relevant success or failure branch. */
    struct fuse_file_info fi {};
    struct stat stbuf {};
    struct statvfs vfsbuf {};
    char buffer[8] = {};

    EXPECT_EQ(DoGetAttr(nullptr, &stbuf), -EINVAL);
    EXPECT_EQ(DoGetAttr("", &stbuf), -EINVAL);
    EXPECT_EQ(DoMkDir(nullptr, 0755), -EINVAL);
    EXPECT_EQ(DoOpen(nullptr, &fi), -EINVAL);
    EXPECT_EQ(DoOpenAtomic(nullptr, &stbuf, 0644, &fi), -EINVAL);
    EXPECT_EQ(DoOpenDir(nullptr, &fi), -EINVAL);
    EXPECT_EQ(DoReadDir(nullptr, buffer, nullptr, 0, &fi), -EINVAL);
    EXPECT_EQ(DoCreate(nullptr, 0644, &fi), -EINVAL);
    EXPECT_EQ(DoRelease(nullptr, &fi), -EINVAL);
    EXPECT_EQ(DoReleaseDir(nullptr, &fi), -EINVAL);
    EXPECT_EQ(DoUnlink(nullptr), -EINVAL);
    EXPECT_EQ(DoRmDir(nullptr), -EINVAL);
    EXPECT_EQ(DoWrite(nullptr, buffer, sizeof(buffer), 0, &fi), -EINVAL);
    EXPECT_EQ(DoWrite("/path", nullptr, sizeof(buffer), 0, &fi), -EINVAL);
    EXPECT_EQ(DoRead(nullptr, buffer, sizeof(buffer), 0, &fi), -EINVAL);
    EXPECT_EQ(DoSetXAttr(nullptr, "key", "value", 5, 0), -EINVAL);
    EXPECT_EQ(DoSetXAttr("/path", "key", nullptr, 5, 0), -EINVAL);
    EXPECT_EQ(DoTruncate(nullptr, 0), -EINVAL);
    EXPECT_EQ(DoFtruncate(nullptr, 0, &fi), -EINVAL);
    EXPECT_EQ(DoFlush(nullptr, &fi), -EINVAL);
    EXPECT_EQ(DoRename(nullptr, "/dst"), -EINVAL);
    EXPECT_EQ(DoRename("/src", nullptr), -EINVAL);
    EXPECT_EQ(DoFsync(nullptr, 0, &fi), -EINVAL);
    EXPECT_EQ(DoStatfs(nullptr, &vfsbuf), -EINVAL);
    EXPECT_EQ(DoStatfs("/path", nullptr), -EINVAL);
    EXPECT_EQ(DoUtimens(nullptr, nullptr), -EINVAL);
    EXPECT_EQ(DoChmod(nullptr, 0644), -EINVAL);
    EXPECT_EQ(DoChown(nullptr, getuid(), getgid()), -EINVAL);
    EXPECT_EQ(DoAccess(nullptr, 0), -EINVAL);
    EXPECT_EQ(DoAccess("", 0), -EINVAL);
}

TEST(FalconClientFuseWrapperUT, LocalBranchesWithoutService)
{
    /* Exercise local branches Without Service and assert the relevant success or failure branch. */
    struct stat stbuf {};
    struct fuse_file_info fi {};
    char syntheticPath[] = "/\0011middle";
    char buffer[8] = {};

    EXPECT_EQ(DoAccess("/any", 0), 0);
    EXPECT_EQ(DoSetXAttr("/any", "key", "value", 5, 0), 0);
    EXPECT_EQ(DoGetAttr(syntheticPath, &stbuf), 0);
    EXPECT_TRUE(S_ISDIR(stbuf.st_mode));
    EXPECT_EQ(DoWrite("/missing", buffer, sizeof(buffer), 0, &fi), -EBADF);
    EXPECT_EQ(DoRead("/missing", buffer, sizeof(buffer), 0, &fi), -EBADF);
}

TEST(FalconClientFuseWrapperUT, MainStatsModeRejectsUnknownCommand)
{
    /* Exercise Main Stats Mode rejects Unknown Command and assert the relevant success or failure branch. */
    char arg0[] = "falcon_client";
    char arg1[] = "stats-invalid";
    char *argv[] = {arg0, arg1};
    EXPECT_EQ(FalconFuseMainCoverageEntry(2, argv), 1);

    char statsArg[] = "stats";
    char endpointArg[] = "--rpc_endpoint=bad endpoint";
    char *statsArgv[] = {arg0, statsArg, endpointArg};
    EXPECT_EQ(FalconFuseMainCoverageEntry(3, statsArgv), 1);

    char statsAllArg[] = "stats-all";
    char brpcArg[] = "-brpc";
    char *statsAllArgv[] = {arg0, statsAllArg, brpcArg, endpointArg};
    EXPECT_EQ(FalconFuseMainCoverageEntry(4, statsAllArgv), 1);
}

namespace {

bool ServiceCoverageEnabled()
{
    const char *enabled = std::getenv("FALCON_CLIENT_SERVICE_COVERAGE");
    return enabled != nullptr && std::string(enabled) == "1";
}

std::string GetEnvOrDefault(const char *key, const char *fallback)
{
    const char *value = std::getenv(key);
    return value != nullptr && *value != '\0' ? std::string(value) : std::string(fallback);
}

int GetIntEnvOrDefault(const char *key, int fallback)
{
    const char *value = std::getenv(key);
    if (value == nullptr || *value == '\0') {
        return fallback;
    }
    return std::atoi(value);
}

bool ServiceEndpointReady(const std::string &serverIp, int serverPort)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return false;
    }

    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(serverPort));
    if (inet_pton(AF_INET, serverIp.c_str(), &addr.sin_addr) != 1) {
        close(fd);
        return false;
    }

    int ret = connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr));
    close(fd);
    return ret == 0;
}

bool InitFalconClientOrSkip()
{
    if (!ServiceCoverageEnabled()) {
        return false;
    }

    std::string serverIp = GetEnvOrDefault("SERVER_IP", "127.0.0.1");
    int serverPort = GetIntEnvOrDefault("SERVER_PORT", 55510);
    if (!ServiceEndpointReady(serverIp, serverPort)) {
        return false;
    }

    constexpr int kMaxRetry = 6;
    for (int i = 0; i < kMaxRetry; ++i) {
        int ret = PROGRAM_ERROR;
        try {
            ret = FalconInit(serverIp, serverPort);
        } catch (const std::exception &) {
            ret = PROGRAM_ERROR;
        }
        if (ret == SUCCESS) {
            return true;
        }
        FalconDestroy();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    return false;
}

std::string BuildUniquePath(const char *suffix)
{
    return "/falcon_client_coverage_" + std::to_string(getpid()) + "_" +
           std::to_string(time(nullptr)) + "_" + suffix;
}

int ReaddirFiller(void *buf, const char *name, const struct stat *st, off_t off)
{
    auto *entries = static_cast<std::vector<std::string> *>(buf);
    entries->push_back(name);
    EXPECT_GE(off, 0);
    if (st != nullptr) {
        EXPECT_NE(st->st_mode, 0U);
    }
    return 0;
}

}  // namespace

TEST(FalconClientFuseWrapperServiceUT, MetadataAndIoWrappers)
{
    /* Exercise Metadata And IO Wrappers and assert the relevant success or failure branch. */
    if (!InitFalconClientOrSkip()) {
        GTEST_SKIP() << "Falcon client service coverage is disabled or service is unavailable";
    }

    std::string dir = BuildUniquePath("fuse_dir");
    std::string file = dir + "/file";
    std::string renamed = dir + "/renamed";

    ASSERT_EQ(DoMkDir(dir.c_str(), 0755), 0);

    struct fuse_file_info fi {};
    fi.flags = O_RDWR | O_CREAT;
    ASSERT_EQ(DoCreate(file.c_str(), 0644, &fi), 0);
    ASSERT_NE(fi.fh, 0U);

    const char payload[] = "fuse-wrapper-coverage";
    EXPECT_EQ(DoWrite(file.c_str(), payload, sizeof(payload), 0, &fi), static_cast<int>(sizeof(payload)));
    EXPECT_EQ(DoFsync(file.c_str(), 0, &fi), 0);
    EXPECT_EQ(DoFlush(file.c_str(), &fi), 0);
    EXPECT_EQ(DoRelease(file.c_str(), &fi), 0);

    struct stat stbuf {};
    EXPECT_EQ(DoGetAttr(file.c_str(), &stbuf), 0);
    EXPECT_TRUE(S_ISREG(stbuf.st_mode));
    std::string syntheticLookup = dir + "/\0010file";
    std::vector<char> syntheticPath(syntheticLookup.begin(), syntheticLookup.end());
    syntheticPath.push_back('\0');
    memset(&stbuf, 0, sizeof(stbuf));
    EXPECT_EQ(DoGetAttr(syntheticPath.data(), &stbuf), 0);
    EXPECT_TRUE(S_ISREG(stbuf.st_mode));

    struct fuse_file_info atomicFi {};
    atomicFi.flags = O_CREAT;
    EXPECT_EQ(DoOpenAtomic(file.c_str(), &stbuf, 0644, &atomicFi), 0);
    if (atomicFi.fh != 0 && atomicFi.fh != UINT64_MAX) {
        EXPECT_EQ(DoRelease(file.c_str(), &atomicFi), 0);
    }
    struct fuse_file_info exclusiveFi {};
    exclusiveFi.flags = O_CREAT | O_EXCL;
    EXPECT_EQ(DoOpenAtomic(file.c_str(), &stbuf, 0644, &exclusiveFi), -EEXIST);

    struct fuse_file_info readFi {};
    readFi.flags = O_RDONLY;
    EXPECT_EQ(DoOpen(file.c_str(), &readFi), 0);
    char readBuffer[sizeof(payload)] = {};
    EXPECT_EQ(DoRead(file.c_str(), readBuffer, sizeof(readBuffer), 0, &readFi), static_cast<int>(sizeof(readBuffer)));
    EXPECT_EQ(std::memcmp(readBuffer, payload, sizeof(payload)), 0);
    EXPECT_EQ(DoRelease(file.c_str(), &readFi), 0);

    EXPECT_EQ(DoTruncate(file.c_str(), 4), 0);
    struct fuse_file_info truncFi {};
    truncFi.flags = O_RDWR;
    EXPECT_EQ(DoOpen(file.c_str(), &truncFi), 0);
    EXPECT_EQ(DoFtruncate(file.c_str(), 2, &truncFi), 0);
    EXPECT_EQ(DoRelease(file.c_str(), &truncFi), 0);

    EXPECT_EQ(DoUtimens(file.c_str(), nullptr), 0);
    const struct timespec explicitTimes[2] = {{.tv_sec = 1, .tv_nsec = 0}, {.tv_sec = 2, .tv_nsec = 0}};
    EXPECT_EQ(DoUtimens(file.c_str(), explicitTimes), 0);
    EXPECT_EQ(DoChmod(file.c_str(), 0644), 0);
    EXPECT_EQ(DoChown(file.c_str(), getuid(), getgid()), 0);
    EXPECT_EQ(DoRename(file.c_str(), renamed.c_str()), 0);

    FalconFuseInfo dirFi {};
    EXPECT_EQ(DoOpenDir(dir.c_str(), reinterpret_cast<struct fuse_file_info *>(&dirFi)), 0);
    std::vector<std::string> entries;
    EXPECT_EQ(DoReadDir(dir.c_str(), &entries, ReaddirFiller, 0, reinterpret_cast<struct fuse_file_info *>(&dirFi)), 0);
    std::vector<std::string> stoppedEntries;
    EXPECT_EQ(DoReadDir(dir.c_str(),
                        &stoppedEntries,
                        StopAfterFirstReaddirFiller,
                        1,
                        reinterpret_cast<struct fuse_file_info *>(&dirFi)),
              0);
    EXPECT_EQ(DoReleaseDir(dir.c_str(), reinterpret_cast<struct fuse_file_info *>(&dirFi)), 0);

    struct statvfs vfsbuf {};
    EXPECT_EQ(DoStatfs("/", &vfsbuf), 0);

    EXPECT_EQ(DoUnlink(renamed.c_str()), 0);
    EXPECT_EQ(DoRmDir(dir.c_str()), 0);
    DoDestroy(nullptr);
}

TEST(FalconClientMetaServiceUT, CreateReadWriteRenameAndCleanup)
{
    /* Exercise Create Read Write Rename And Cleanup and assert the relevant success or failure branch. */
    if (!InitFalconClientOrSkip()) {
        GTEST_SKIP() << "Falcon client service coverage is disabled or service is unavailable";
    }

    std::string dir = BuildUniquePath("dir");
    std::string file = dir + "/file";
    std::string renamed = dir + "/renamed";

    EXPECT_EQ(FalconMkdir(dir), SUCCESS);
    EXPECT_NE(FalconMkdir(dir), SUCCESS);

    struct stat stbuf;
    std::memset(&stbuf, 0, sizeof(stbuf));
    uint64_t fd = 0;
    EXPECT_EQ(FalconCreate(file, fd, O_RDWR | O_CREAT, &stbuf), SUCCESS);
    ASSERT_NE(fd, 0U);

    const char payload[] = "falcon-client-service-coverage";
    EXPECT_EQ(FalconWrite(fd, file, payload, sizeof(payload), 0), 0);
    EXPECT_EQ(FalconFsync(file, fd, 0), SUCCESS);
    EXPECT_EQ(FalconClose(file, fd), SUCCESS);

    std::memset(&stbuf, 0, sizeof(stbuf));
    EXPECT_EQ(FalconGetStat(file, &stbuf), SUCCESS);
    EXPECT_TRUE(S_ISREG(stbuf.st_mode));
    EXPECT_GE(stbuf.st_size, static_cast<off_t>(sizeof(payload)));

    fd = 0;
    std::memset(&stbuf, 0, sizeof(stbuf));
    EXPECT_EQ(FalconOpen(file, O_RDONLY, fd, &stbuf), SUCCESS);
    ASSERT_NE(fd, 0U);

    char readBuffer[sizeof(payload)] = {};
    EXPECT_EQ(FalconRead(file, fd, readBuffer, sizeof(readBuffer), 0), static_cast<int>(sizeof(readBuffer)));
    EXPECT_EQ(std::memcmp(readBuffer, payload, sizeof(payload)), 0);
    EXPECT_EQ(FalconClose(file, fd), SUCCESS);

    fd = 0;
    std::memset(&stbuf, 0, sizeof(stbuf));
    EXPECT_EQ(FalconOpen(file, O_RDONLY | __O_DIRECT, fd, &stbuf), SUCCESS);
    ASSERT_NE(fd, 0U);
    EXPECT_EQ(FalconClose(file, fd), SUCCESS);

    EXPECT_EQ(FalconRename(file, renamed), SUCCESS);
    EXPECT_EQ(FalconGetStat(file, &stbuf), FILE_NOT_EXISTS);
    EXPECT_EQ(FalconGetStat(renamed, &stbuf), SUCCESS);

    EXPECT_EQ(FalconTruncate(renamed, 4), SUCCESS);
    EXPECT_EQ(FalconGetStat(renamed, &stbuf), SUCCESS);
    EXPECT_GE(stbuf.st_size, 4);

    struct statvfs vfsbuf;
    std::memset(&vfsbuf, 0, sizeof(vfsbuf));
    EXPECT_EQ(FalconStatFS(&vfsbuf), SUCCESS);

    EXPECT_EQ(FalconUnlink(renamed), SUCCESS);
    EXPECT_EQ(FalconRmDir(dir), SUCCESS);
    FalconDestroy();
}

TEST(FalconClientMetaServiceUT, DirectoryAndAttributeOperations)
{
    /* Exercise Directory And Attribute Operations and assert the relevant success or failure branch. */
    if (!InitFalconClientOrSkip()) {
        GTEST_SKIP() << "Falcon client service coverage is disabled or service is unavailable";
    }

    std::string dir = BuildUniquePath("listdir");
    std::string file = dir + "/entry";

    EXPECT_EQ(FalconMkdir(dir), SUCCESS);

    struct stat stbuf;
    std::memset(&stbuf, 0, sizeof(stbuf));
    uint64_t fd = 0;
    EXPECT_EQ(FalconCreate(file, fd, O_RDWR | O_CREAT, &stbuf), SUCCESS);
    EXPECT_EQ(FalconClose(file, fd), SUCCESS);

    FalconFuseInfo fi {};
    EXPECT_EQ(FalconOpenDir(dir, &fi), SUCCESS);
    std::vector<std::string> entries;
    EXPECT_EQ(FalconReadDir(dir, &entries, ReaddirFiller, 0, &fi), SUCCESS);
    EXPECT_NE(std::find(entries.begin(), entries.end(), "."), entries.end());
    EXPECT_NE(std::find(entries.begin(), entries.end(), ".."), entries.end());
    EXPECT_EQ(FalconCloseDir(fi.fh), SUCCESS);

    EXPECT_EQ(FalconUtimens(file, 1, 2), SUCCESS);
    EXPECT_EQ(FalconChmod(file, 0644), SUCCESS);
    EXPECT_EQ(FalconChown(file, getuid(), getgid()), SUCCESS);

    EXPECT_EQ(FalconUnlink(file), SUCCESS);
    EXPECT_EQ(FalconRmDir(dir), SUCCESS);
    FalconDestroy();
}

TEST(FalconClientMetaServiceUT, RouterAndMissingPathBranches)
{
    /* Exercise Router And missing Path branches and assert the relevant success or failure branch. */
    if (!InitFalconClientOrSkip()) {
        GTEST_SKIP() << "Falcon client service coverage is disabled or service is unavailable";
    }

    ASSERT_NE(router, nullptr);
    EXPECT_NE(router->GetCoordinatorConn(), nullptr);
    EXPECT_EQ(router->GetWorkerConnByPath("relative/path"), nullptr);
    EXPECT_EQ(router->GetWorkerConnByPath(""), nullptr);
    EXPECT_NE(router->GetWorkerConnByPath("/"), nullptr);
    EXPECT_NE(router->GetWorkerConnByPath("/trailing/"), nullptr);

    std::unordered_map<std::string, std::shared_ptr<Connection>> workers;
    EXPECT_EQ(router->GetAllWorkerConnection(workers), SUCCESS);
    ASSERT_FALSE(workers.empty());
    int existingWorkerId = workers.begin()->second->server.id;
    EXPECT_NE(router->GetWorkerConnBySvrId(existingWorkerId), nullptr);
    EXPECT_EQ(router->GetWorkerConnBySvrId(999999), nullptr);

    std::string missing = BuildUniquePath("missing");
    struct stat stbuf;
    std::memset(&stbuf, 0, sizeof(stbuf));
    uint64_t fd = 0;
    FalconFuseInfo fi {};

    EXPECT_EQ(FalconGetStat("relative/path", &stbuf), PROGRAM_ERROR);
    EXPECT_EQ(FalconOpen("relative/path", O_RDONLY, fd, &stbuf), PROGRAM_ERROR);
    EXPECT_EQ(FalconUnlink("relative/path"), PROGRAM_ERROR);
    EXPECT_NE(FalconOpenDir("relative/path", &fi), SUCCESS);
    EXPECT_NE(FalconFsync("relative/path", UINT64_MAX - 9, 0), SUCCESS);
    EXPECT_EQ(FalconUtimens("relative/path", 1, 2), PROGRAM_ERROR);
    EXPECT_EQ(FalconChmod("relative/path", 0644), PROGRAM_ERROR);
    EXPECT_EQ(FalconChown("relative/path", getuid(), getgid()), PROGRAM_ERROR);

    EXPECT_NE(FalconOpen(missing, O_RDONLY, fd, &stbuf), SUCCESS);
    EXPECT_NE(FalconGetStat(missing, &stbuf), SUCCESS);
    EXPECT_NE(FalconUnlink(missing), SUCCESS);
    EXPECT_NE(FalconRmDir(missing), SUCCESS);
    EXPECT_NE(FalconRename(missing, missing + "_dst"), SUCCESS);
    EXPECT_NE(FalconOpenDir(missing, &fi), SUCCESS);
    EXPECT_NE(FalconUtimens(missing, 1, 2), SUCCESS);
    EXPECT_NE(FalconChmod(missing, 0644), SUCCESS);
    EXPECT_NE(FalconChown(missing, getuid(), getgid()), SUCCESS);
    EXPECT_NE(FalconTruncate(missing, 16), SUCCESS);

    FalconFuseInfo missingDir {};
    EXPECT_LT(DoOpen(missing.c_str(), reinterpret_cast<struct fuse_file_info *>(&missingDir)), 0);
    EXPECT_LT(DoOpenAtomic(missing.c_str(), &stbuf, 0644, reinterpret_cast<struct fuse_file_info *>(&missingDir)), 0);
    EXPECT_LT(DoCreate((missing + "/child").c_str(), 0644, reinterpret_cast<struct fuse_file_info *>(&missingDir)), 0);
    EXPECT_LT(DoRmDir(missing.c_str()), 0);
    EXPECT_LT(DoUnlink(missing.c_str()), 0);
    EXPECT_LT(DoRename(missing.c_str(), (missing + "_dst").c_str()), 0);
    EXPECT_LT(DoTruncate(missing.c_str(), 16), 0);
    EXPECT_LT(DoUtimens(missing.c_str(), nullptr), 0);
    EXPECT_LT(DoChmod(missing.c_str(), 0644), 0);
    EXPECT_LT(DoChown(missing.c_str(), getuid(), getgid()), 0);

    FalconDestroy();
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
