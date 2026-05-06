#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <condition_variable>
#include <deque>
#include <functional>
#include <iostream>
#include <memory>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "base_comm_adapter/base_meta_service_job.h"
#include "connection_pool/falcon_worker_task.h"
#include "falcon_meta_param_generated.h"
#include "perf_counter/falcon_per_request_stat.h"
#include "remote_connection_utils/error_code_def.h"

extern "C" {
int FalconPGPort = 0;
int FalconConnectionPoolPort = 56999;
int FalconConnectionPoolSize = 32;
int FalconConnectionPoolBatchSize = 4;
int FalconConnectionPoolWaitAdjust = 1;
int FalconConnectionPoolWaitMin = 1;
int FalconConnectionPoolWaitMax = 512;
uint64_t FalconConnectionPoolShmemSize = 256 * 1024 * 1024;
char *FalconNodeLocalIp = nullptr;
char *FalconCommunicationServerIp = nullptr;
char *FalconCommunicationPluginPath = nullptr;

int32_t PerRequestStatAllocIndex(void)
{
    return -1;
}

void PerRequestStatComplete(int32_t, int32_t) {}

FalconErrorCode FalconErrorMsgAnalyse(const char *, const char **errorMsg)
{
    if (errorMsg != nullptr) {
        *errorMsg = "";
    }
    return PROGRAM_ERROR;
}
}

FalconPerRequestStatShmem *g_FalconPerRequestStatShmem = nullptr;

namespace {
struct FakePgResult {
    ExecStatusType status{PGRES_TUPLES_OK};
    int rows{0};
    int cols{0};
    std::vector<std::string> values;
    std::string error;
};

std::deque<FakePgResult *> g_pgResults;
bool g_querySent = false;
int g_sendQueryReturn = 1;
std::string g_pgError;

void ResetFakePg()
{
    for (auto *result : g_pgResults) {
        delete result;
    }
    g_pgResults.clear();
    g_querySent = false;
    g_sendQueryReturn = 1;
    g_pgError.clear();
}

void QueueFakePgResult(FakePgResult *result)
{
    g_pgResults.push_back(result);
}

FakePgResult *AsFakeResult(PGresult *result)
{
    return reinterpret_cast<FakePgResult *>(result);
}
}

extern "C" {
PGresult *PQgetResult(PGconn *)
{
    if (!g_querySent || g_pgResults.empty()) {
        g_querySent = false;
        return nullptr;
    }
    FakePgResult *result = g_pgResults.front();
    g_pgResults.pop_front();
    return reinterpret_cast<PGresult *>(result);
}

void PQclear(PGresult *result)
{
    delete AsFakeResult(result);
}

int PQsendQuery(PGconn *, const char *)
{
    g_querySent = true;
    return g_sendQueryReturn;
}

char *PQerrorMessage(const PGconn *)
{
    return g_pgError.empty() ? const_cast<char *>("fake pg error") : g_pgError.data();
}

ExecStatusType PQresultStatus(const PGresult *result)
{
    return reinterpret_cast<const FakePgResult *>(result)->status;
}

char *PQresultErrorMessage(const PGresult *result)
{
    const auto *fakeResult = reinterpret_cast<const FakePgResult *>(result);
    return const_cast<char *>(fakeResult->error.c_str());
}

int PQntuples(const PGresult *result)
{
    return reinterpret_cast<const FakePgResult *>(result)->rows;
}

int PQnfields(const PGresult *result)
{
    return reinterpret_cast<const FakePgResult *>(result)->cols;
}

char *PQgetvalue(const PGresult *result, int row, int col)
{
    const auto *fakeResult = reinterpret_cast<const FakePgResult *>(result);
    size_t index = static_cast<size_t>(row * fakeResult->cols + col);
    return const_cast<char *>(fakeResult->values[index].c_str());
}
}

#ifndef FALCON_POOLER_PG_CONNECTION_H
#define FALCON_POOLER_PG_CONNECTION_H
class PGConnection {
  public:
    using FinishFunc = std::function<void(PGConnection *)>;

    PGConnection(FinishFunc finishFunc, const char *, const int, const char *)
        : finishFunc_(std::move(finishFunc))
    {
        constructed++;
    }

    ~PGConnection()
    {
        destructed++;
    }

    void Exec(std::shared_ptr<BaseWorkerTask> taskToExec)
    {
        execCount++;
        lastTask = std::move(taskToExec);
        if (finishFunc_) {
            finishFunc_(this);
        }
    }

    void Stop()
    {
        stopCount++;
    }

    static void Reset()
    {
        constructed = 0;
        destructed = 0;
        execCount = 0;
        stopCount = 0;
        lastTask.reset();
    }

    static int constructed;
    static int destructed;
    static int execCount;
    static int stopCount;
    static std::shared_ptr<BaseWorkerTask> lastTask;

  private:
    FinishFunc finishFunc_;
};

int PGConnection::constructed = 0;
int PGConnection::destructed = 0;
int PGConnection::execCount = 0;
int PGConnection::stopCount = 0;
std::shared_ptr<BaseWorkerTask> PGConnection::lastTask;
#endif

#define private public
#include "../../falcon/connection_pool/pg_connection_pool.cpp"
#undef private
#include "../../falcon/connection_pool/falcon_worker_task.cpp"

static std::vector<char> MakeSerializedPlainCommand(const char *command)
{
    flatbuffers::FlatBufferBuilder builder;
    auto plainCommand = falcon::meta_fbs::CreatePlainCommandParamDirect(builder, command);
    auto metaParam = falcon::meta_fbs::CreateMetaParam(
        builder,
        falcon::meta_fbs::AnyMetaParam::AnyMetaParam_PlainCommandParam,
        plainCommand.Union());
    builder.Finish(metaParam);

    SerializedData data;
    SerializedDataInit(&data, nullptr, 0, 0, nullptr);
    char *segment = SerializedDataApplyForSegment(&data, builder.GetSize());
    std::memcpy(segment, builder.GetBufferPointer(), builder.GetSize());
    std::vector<char> result(data.buffer, data.buffer + data.size);
    SerializedDataDestroy(&data);
    return result;
}

static std::vector<char> MakeSerializedBytes(const char *payload, size_t size)
{
    SerializedData data;
    SerializedDataInit(&data, nullptr, 0, 0, nullptr);
    char *segment = SerializedDataApplyForSegment(&data, size);
    if (size != 0) {
        std::memcpy(segment, payload, size);
    }
    std::vector<char> result(data.buffer, data.buffer + data.size);
    SerializedDataDestroy(&data);
    return result;
}

class TestAllocator {
  public:
    explicit TestAllocator(size_t size) : shmem_(size)
    {
        EXPECT_EQ(FalconShmemAllocatorInit(&allocator_, shmem_.data(), shmem_.size()), 0);
    }

    FalconShmemAllocator *get()
    {
        return &allocator_;
    }

  private:
    std::vector<char> shmem_;
    FalconShmemAllocator allocator_{};
};

class FakeMetaServiceJob : public BaseMetaServiceJob {
  public:
    explicit FakeMetaServiceJob(FalconMetaServiceType serviceType,
                                bool allowBatch = true,
                                bool empty = false,
                                int serviceCount = 1,
                                std::vector<char> requestData = MakeSerializedBytes("", 0))
        : serviceType_(serviceType),
          allowBatch_(allowBatch),
          empty_(empty),
          serviceCount_(serviceCount),
          requestData_(std::move(requestData))
    {
    }

    ~FakeMetaServiceJob() override = default;

    void Done() override
    {
        done = true;
        doneCount++;
    }

    void MarkFailed() override
    {
        failed = true;
        failedCount++;
    }

    bool IsAllowBatchProcess() override
    {
        return allowBatch_;
    }

    bool IsEmptyRequest() override
    {
        return empty_;
    }

    int GetReqServiceCnt() override
    {
        return serviceCount_;
    }

    size_t GetReqDatasize() override
    {
        return requestData_.size();
    }

    size_t CopyOutData(void *dst, size_t dstSize) override
    {
        size_t toCopy = std::min(dstSize, requestData_.size());
        if (toCopy != 0) {
            std::memcpy(dst, requestData_.data(), toCopy);
        }
        return toCopy;
    }

    FalconMetaServiceType GetFalconMetaServiceType(int) override
    {
        return serviceType_;
    }

    void ProcessResponse(void *, size_t, FalDataDeleter) override
    {
        responseProcessed = true;
        responseCount++;
    }

    bool done{false};
    bool failed{false};
    bool responseProcessed{false};
    inline static int doneCount{0};
    inline static int failedCount{0};
    inline static int responseCount{0};

    static void ResetCounters()
    {
        doneCount = 0;
        failedCount = 0;
        responseCount = 0;
    }

  private:
    FalconMetaServiceType serviceType_;
    bool allowBatch_;
    bool empty_;
    int serviceCount_;
    std::vector<char> requestData_;
};

TEST(ConnectionPoolCoverageUT, ResolvesBatchSupportedAndUnsupportedServiceTypes)
{
    FakeMetaServiceJob mkdirJob(FalconMetaServiceType::MKDIR, true);
    int serviceType = -1;
    EXPECT_EQ(FalconConnectionPoolTestResolveBatchType(&mkdirJob, &serviceType),
              static_cast<int>(FalconBatchServiceType::MKDIR));
    EXPECT_EQ(serviceType, static_cast<int>(FalconMetaServiceType::MKDIR));

    FakeMetaServiceJob plainJob(FalconMetaServiceType::PLAIN_COMMAND, true);
    EXPECT_EQ(FalconConnectionPoolTestResolveBatchType(&plainJob, &serviceType),
              static_cast<int>(FalconBatchServiceType::NOT_SUPPORT));
    EXPECT_EQ(serviceType, static_cast<int>(FalconMetaServiceType::PLAIN_COMMAND));

    FakeMetaServiceJob nonBatchCreateJob(FalconMetaServiceType::CREATE, false);
    EXPECT_EQ(FalconConnectionPoolTestResolveBatchType(&nonBatchCreateJob, nullptr),
              static_cast<int>(FalconBatchServiceType::NOT_SUPPORT));
}

TEST(ConnectionPoolCoverageUT, AdjustWaitTimeHonorsConfigAndBounds)
{
    FalconConnectionPoolWaitAdjust = 0;
    EXPECT_EQ(FalconConnectionPoolTestAdjustWaitTime(64, 0), 64);

    FalconConnectionPoolWaitAdjust = 1;
    FalconConnectionPoolBatchSize = 4;
    FalconConnectionPoolWaitMin = 2;
    FalconConnectionPoolWaitMax = 100;

    EXPECT_EQ(FalconConnectionPoolTestAdjustWaitTime(64, 8), 100);
    EXPECT_EQ(FalconConnectionPoolTestAdjustWaitTime(64, 9), 32);
    EXPECT_EQ(FalconConnectionPoolTestAdjustWaitTime(3, 9), 2);
}

TEST(ConnectionPoolCoverageUT, WorkerTaskDestructorsFailAndCompleteOwnedJobs)
{
    FakeMetaServiceJob::ResetCounters();
    auto *singleJob = new FakeMetaServiceJob(FalconMetaServiceType::CREATE);
    {
        SingleWorkerTask task(nullptr, singleJob);
        EXPECT_FALSE(singleJob->done);
        EXPECT_FALSE(singleJob->failed);
    }
    EXPECT_EQ(FakeMetaServiceJob::doneCount, 1);
    EXPECT_EQ(FakeMetaServiceJob::failedCount, 1);

    auto *batchJobA = new FakeMetaServiceJob(FalconMetaServiceType::STAT);
    auto *batchJobB = new FakeMetaServiceJob(FalconMetaServiceType::STAT);
    {
        std::vector<BaseMetaServiceJob *> jobs{batchJobA, batchJobB};
        BatchWorkerTask task(nullptr, jobs);
        EXPECT_FALSE(batchJobA->done);
        EXPECT_FALSE(batchJobA->failed);
        EXPECT_FALSE(batchJobB->done);
        EXPECT_FALSE(batchJobB->failed);
    }
    EXPECT_EQ(FakeMetaServiceJob::doneCount, 3);
    EXPECT_EQ(FakeMetaServiceJob::failedCount, 3);
}

TEST(ConnectionPoolCoverageUT, StubbedPoolInitCreatesAndStopsConnections)
{
    PGConnection::Reset();
    PGConnectionPool &pool = PGConnectionPool::GetInstance();

    ASSERT_TRUE(pool.Init(55510, "tester", 2, 20, 40));
    EXPECT_EQ(PGConnection::constructed, 2);

    pool.Destroy();
    EXPECT_EQ(PGConnection::stopCount, 2);
    EXPECT_EQ(PGConnection::destructed, 2);
}

TEST(ConnectionPoolCoverageUT, DispatchAndDequeueExecuteBatchAndSingleJobs)
{
    PGConnection::Reset();
    FakeMetaServiceJob::ResetCounters();
    PGConnectionPool &pool = PGConnectionPool::GetInstance();

    auto drainQueue = [](auto &queue) {
        std::vector<BaseMetaServiceJob *> drained;
        auto collect = [&drained](BaseMetaServiceJob *job) {
            drained.emplace_back(job);
        };
        while (queue.dequeue_bulk(collect, 16) > 0) {
        }
        for (auto *job : drained) {
            delete job;
        }
    };
    for (int i = 0; i <= static_cast<int>(FalconBatchServiceType::NOT_SUPPORT); ++i) {
        drainQueue(pool.supportBatchTaskList[i].jobList);
    }
    {
        std::unique_lock<std::mutex> lock(pool.connPoolMutex);
        while (!pool.connPool.empty()) {
            pool.connPool.pop();
        }
    }

    FakeMetaServiceJob emptyJob(FalconMetaServiceType::MKDIR, true, true);
    pool.DispatchMetaServiceJob(&emptyJob);
    EXPECT_EQ(pool.supportBatchTaskList[static_cast<int>(FalconBatchServiceType::MKDIR)].jobList.size_approx(), 0U);

    auto *batchJobA = new FakeMetaServiceJob(FalconMetaServiceType::MKDIR, true);
    auto *batchJobB = new FakeMetaServiceJob(FalconMetaServiceType::MKDIR, true);
    pool.DispatchMetaServiceJob(batchJobA);
    pool.DispatchMetaServiceJob(batchJobB);
    EXPECT_GE(batchJobA->statArrayIndex, -1);

    PGConnection batchConn(nullptr, "127.0.0.1", 55510, "tester");
    {
        std::unique_lock<std::mutex> lock(pool.connPoolMutex);
        pool.connPool.push(&batchConn);
    }
    pool.cvPoolNotEmpty.notify_one();
    EXPECT_EQ(pool.BatchDequeueExec(2, static_cast<int>(FalconBatchServiceType::MKDIR)), 2);
    EXPECT_EQ(PGConnection::execCount, 1);
    PGConnection::lastTask.reset();
    EXPECT_EQ(FakeMetaServiceJob::doneCount, 2);

    auto *singleJob = new FakeMetaServiceJob(FalconMetaServiceType::PLAIN_COMMAND, false);
    pool.DispatchMetaServiceJob(singleJob);
    PGConnection singleConn(nullptr, "127.0.0.1", 55510, "tester");
    {
        std::unique_lock<std::mutex> lock(pool.connPoolMutex);
        pool.connPool.push(&singleConn);
    }
    pool.cvPoolNotEmpty.notify_one();
    EXPECT_EQ(pool.SingleDequeueExec(1), 1);
    EXPECT_EQ(PGConnection::execCount, 2);
    PGConnection::lastTask.reset();
    EXPECT_EQ(FakeMetaServiceJob::doneCount, 3);

    EXPECT_EQ(pool.BatchDequeueExec(1, static_cast<int>(FalconBatchServiceType::MKDIR)), 0);
    EXPECT_EQ(pool.SingleDequeueExec(1), 0);
}

TEST(ConnectionPoolCoverageUT, WorkerTasksRejectInvalidInputs)
{
    TestAllocator allocator(4 * 1024 * 1024);
    flatbuffers::FlatBufferBuilder flatBufferBuilder;
    SerializedData replyBuilder;
    SerializedDataInit(&replyBuilder, nullptr, 0, 0, nullptr);

    SingleWorkerTask singleTask(allocator.get(), nullptr);
    EXPECT_THROW(singleTask.DoWork(nullptr, flatBufferBuilder, replyBuilder), std::runtime_error);

    std::vector<BaseMetaServiceJob *> emptyJobs;
    BatchWorkerTask emptyBatch(allocator.get(), emptyJobs);
    EXPECT_THROW(emptyBatch.DoWork(nullptr, flatBufferBuilder, replyBuilder), std::runtime_error);

    auto *badJob = new FakeMetaServiceJob(FalconMetaServiceType::MKDIR,
                                          true,
                                          false,
                                          1,
                                          std::vector<char>{'b', 'a', 'd'});
    std::vector<BaseMetaServiceJob *> badJobs{badJob};
    {
        BatchWorkerTask badBatch(allocator.get(), badJobs);
        EXPECT_THROW(badBatch.DoWork(nullptr, flatBufferBuilder, replyBuilder), std::runtime_error);
    }

    SerializedDataDestroy(&replyBuilder);
}

TEST(ConnectionPoolCoverageUT, SingleWorkerTaskProcessesPlainCommandResult)
{
    ResetFakePg();
    FakeMetaServiceJob::ResetCounters();
    TestAllocator allocator(4 * 1024 * 1024);
    flatbuffers::FlatBufferBuilder flatBufferBuilder;
    SerializedData replyBuilder;
    SerializedDataInit(&replyBuilder, nullptr, 0, 0, nullptr);

    auto *result = new FakePgResult();
    result->status = PGRES_TUPLES_OK;
    result->rows = 1;
    result->cols = 1;
    result->values = {"plain-result"};
    QueueFakePgResult(result);

    auto *job = new FakeMetaServiceJob(FalconMetaServiceType::PLAIN_COMMAND,
                                       false,
                                       false,
                                       1,
                                       MakeSerializedPlainCommand("select 1"));
    SingleWorkerTask task(allocator.get(), job);
    EXPECT_NO_THROW(task.DoWork(nullptr, flatBufferBuilder, replyBuilder));
    EXPECT_EQ(FakeMetaServiceJob::doneCount, 1);
    EXPECT_EQ(FakeMetaServiceJob::failedCount, 0);
    EXPECT_EQ(FakeMetaServiceJob::responseCount, 1);

    SerializedDataDestroy(&replyBuilder);
    ResetFakePg();
}

TEST(ConnectionPoolCoverageUT, BatchWorkerTaskHandlesZeroReplyShift)
{
    ResetFakePg();
    FakeMetaServiceJob::ResetCounters();
    TestAllocator allocator(4 * 1024 * 1024);
    flatbuffers::FlatBufferBuilder flatBufferBuilder;
    SerializedData replyBuilder;
    SerializedDataInit(&replyBuilder, nullptr, 0, 0, nullptr);

    auto *result = new FakePgResult();
    result->status = PGRES_TUPLES_OK;
    result->rows = 1;
    result->cols = 1;
    result->values = {"0"};
    QueueFakePgResult(result);

    std::vector<BaseMetaServiceJob *> jobs{
        new FakeMetaServiceJob(FalconMetaServiceType::MKDIR),
        new FakeMetaServiceJob(FalconMetaServiceType::MKDIR),
    };
    BatchWorkerTask task(allocator.get(), jobs);
    EXPECT_NO_THROW(task.DoWork(nullptr, flatBufferBuilder, replyBuilder));
    EXPECT_EQ(FakeMetaServiceJob::doneCount, 2);
    EXPECT_EQ(FakeMetaServiceJob::failedCount, 0);

    SerializedDataDestroy(&replyBuilder);
    ResetFakePg();
}

TEST(ConnectionPoolCoverageUT, WorkerTasksConvertPgErrorsToResponses)
{
    ResetFakePg();
    FakeMetaServiceJob::ResetCounters();
    TestAllocator allocator(4 * 1024 * 1024);
    flatbuffers::FlatBufferBuilder flatBufferBuilder;
    SerializedData replyBuilder;
    SerializedDataInit(&replyBuilder, nullptr, 0, 0, nullptr);

    auto *singleResult = new FakePgResult();
    singleResult->status = PGRES_FATAL_ERROR;
    singleResult->error = "fake fatal";
    QueueFakePgResult(singleResult);

    auto *singleJob = new FakeMetaServiceJob(FalconMetaServiceType::PLAIN_COMMAND,
                                             false,
                                             false,
                                             1,
                                             MakeSerializedPlainCommand("select fail"));
    {
        SingleWorkerTask task(allocator.get(), singleJob);
        EXPECT_NO_THROW(task.DoWork(nullptr, flatBufferBuilder, replyBuilder));
    }
    EXPECT_EQ(FakeMetaServiceJob::doneCount, 1);
    EXPECT_EQ(FakeMetaServiceJob::failedCount, 0);
    EXPECT_EQ(FakeMetaServiceJob::responseCount, 1);

    auto *batchResult = new FakePgResult();
    batchResult->status = PGRES_FATAL_ERROR;
    batchResult->error = "fake batch fatal";
    QueueFakePgResult(batchResult);

    std::vector<BaseMetaServiceJob *> jobs{
        new FakeMetaServiceJob(FalconMetaServiceType::MKDIR),
        new FakeMetaServiceJob(FalconMetaServiceType::MKDIR),
    };
    {
        BatchWorkerTask task(allocator.get(), jobs);
        EXPECT_NO_THROW(task.DoWork(nullptr, flatBufferBuilder, replyBuilder));
    }
    EXPECT_EQ(FakeMetaServiceJob::doneCount, 3);
    EXPECT_EQ(FakeMetaServiceJob::failedCount, 0);
    EXPECT_EQ(FakeMetaServiceJob::responseCount, 3);

    SerializedDataDestroy(&replyBuilder);
    ResetFakePg();
}
