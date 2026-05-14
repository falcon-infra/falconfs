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

#include "../../falcon/connection_pool/pg_connection_pool.cpp"
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

class DefaultMarkFailedMetaServiceJob : public BaseMetaServiceJob {
  public:
    void Done() override {}
    bool IsAllowBatchProcess() override { return false; }
    bool IsEmptyRequest() override { return true; }
    int GetReqServiceCnt() override { return 0; }
    size_t GetReqDatasize() override { return 0; }
    size_t CopyOutData(void *, size_t) override { return 0; }
    FalconMetaServiceType GetFalconMetaServiceType(int) override { return NOT_SUPPORTED; }
    void ProcessResponse(void *, size_t, FalDataDeleter) override {}
};

TEST(ConnectionPoolCoverageUT, WorkerTaskDestructorsFailAndCompleteOwnedJobs)
{
    /* Exercise task destructors that mark unfinished jobs failed before calling Done(). */
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

TEST(ConnectionPoolCoverageUT, SerializedDataBoundaryBranches)
{
    /* Validate serialized-buffer rejection paths before appending and clearing owned buffers. */
    EXPECT_TRUE(SystemIsLittleEndian());
    EXPECT_EQ(ConvertBetweenBigAndLittleEndian(0x01020304U), 0x04030201U);

    char raw[16] = {};
    SerializedData external;
    EXPECT_FALSE(SerializedDataInit(&external, raw, 3, 0, nullptr));
    EXPECT_FALSE(SerializedDataInit(&external, raw, sizeof(raw), 2, nullptr));
    ASSERT_TRUE(SerializedDataInit(&external, raw, sizeof(raw), 0, nullptr));
    EXPECT_EQ(SerializedDataApplyForSegment(&external, sizeof(raw)), nullptr);

    SerializedData first;
    ASSERT_TRUE(SerializedDataInit(&first, nullptr, 0, 0, nullptr));
    char *firstSegment = SerializedDataApplyForSegment(&first, 3);
    ASSERT_NE(firstSegment, nullptr);
    std::memcpy(firstSegment, "abc", 3);
    EXPECT_EQ(first.size, 8U);
    EXPECT_EQ(SerializedDataNextSeveralItemSize(&first, 1, 1), static_cast<sd_size_t>(-1));

    SerializedData second;
    ASSERT_TRUE(SerializedDataInit(&second, nullptr, 0, 0, nullptr));
    char *secondSegment = SerializedDataApplyForSegment(&second, 4);
    ASSERT_NE(secondSegment, nullptr);
    std::memcpy(secondSegment, "defg", 4);
    ASSERT_TRUE(SerializedDataAppend(&first, &second));
    EXPECT_EQ(SerializedDataNextSeveralItemSize(&first, 0, 2), first.size);
    EXPECT_EQ(SerializedDataNextSeveralItemSize(&first, 0, 3), static_cast<sd_size_t>(-1));
    SerializedDataClear(&first);
    EXPECT_EQ(first.size, 0U);
    SerializedDataDestroy(&first);
    SerializedDataDestroy(&second);
}

TEST(ConnectionPoolCoverageUT, DefaultJobMarkFailedIsNoop)
{
    /* Call the base default MarkFailed hook used by adapters that do not need failure state. */
    DefaultMarkFailedMetaServiceJob job;
    BaseMetaServiceJob *base = &job;
    EXPECT_NO_THROW(base->MarkFailed());
}

TEST(ConnectionPoolCoverageUT, MapsMetaServiceTypesToBatchServiceTypes)
{
    /* Verify every public meta opcode maps to the batch worker opcode expected by PG replies. */
    EXPECT_EQ(FalconMetaServiceTypeToBatchServiceType(FalconMetaServiceType::MKDIR), FalconBatchServiceType::MKDIR);
    EXPECT_EQ(FalconMetaServiceTypeToBatchServiceType(FalconMetaServiceType::CREATE), FalconBatchServiceType::CREATE);
    EXPECT_EQ(FalconMetaServiceTypeToBatchServiceType(FalconMetaServiceType::STAT), FalconBatchServiceType::STAT);
    EXPECT_EQ(FalconMetaServiceTypeToBatchServiceType(FalconMetaServiceType::UNLINK), FalconBatchServiceType::UNLINK);
    EXPECT_EQ(FalconMetaServiceTypeToBatchServiceType(FalconMetaServiceType::OPEN), FalconBatchServiceType::OPEN);
    EXPECT_EQ(FalconMetaServiceTypeToBatchServiceType(FalconMetaServiceType::CLOSE), FalconBatchServiceType::CLOSE);
    EXPECT_EQ(FalconMetaServiceTypeToBatchServiceType(FalconMetaServiceType::KV_PUT), FalconBatchServiceType::KV_PUT);
    EXPECT_EQ(FalconMetaServiceTypeToBatchServiceType(FalconMetaServiceType::KV_GET), FalconBatchServiceType::KV_GET);
    EXPECT_EQ(FalconMetaServiceTypeToBatchServiceType(FalconMetaServiceType::KV_DEL), FalconBatchServiceType::KV_DEL);
    EXPECT_EQ(FalconMetaServiceTypeToBatchServiceType(FalconMetaServiceType::SLICE_PUT), FalconBatchServiceType::SLICE_PUT);
    EXPECT_EQ(FalconMetaServiceTypeToBatchServiceType(FalconMetaServiceType::SLICE_GET), FalconBatchServiceType::SLICE_GET);
    EXPECT_EQ(FalconMetaServiceTypeToBatchServiceType(FalconMetaServiceType::SLICE_DEL), FalconBatchServiceType::SLICE_DEL);
    EXPECT_EQ(FalconMetaServiceTypeToBatchServiceType(FalconMetaServiceType::PLAIN_COMMAND),
              FalconBatchServiceType::NOT_SUPPORT);
}

TEST(ConnectionPoolCoverageUT, ShmemAllocatorCoversBoundariesAndFreePaths)
{
    /* Exercise Shared Memory Allocator covers Boundaries And Free Paths and assert the relevant success or failure branch. */
    std::vector<char> tooSmall(sizeof(PaddedAtomic64) * (1 + FALCON_SHMEM_ALLOCATOR_FREE_LIST_COUNT));
    FalconShmemAllocator smallAllocator {};
    EXPECT_NE(FalconShmemAllocatorInit(&smallAllocator, tooSmall.data(), tooSmall.size()), 0);

    TestAllocator allocator(2 * 1024 * 1024);
    FalconShmemAllocator *raw = allocator.get();
    EXPECT_EQ(FalconShmemAllocatorGetUniqueSignature(raw), 1);
    EXPECT_EQ(FalconShmemAllocatorGetUniqueSignature(raw), 2);

    EXPECT_EQ(FalconShmemAllocatorMalloc(raw, FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE), 0U);

    uint64_t fullPage = FalconShmemAllocatorMalloc(
        raw, FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE - sizeof(MemoryHdr));
    ASSERT_NE(fullPage, 0U);
    FalconShmemAllocatorFree(raw, fullPage);

    uint64_t first = FalconShmemAllocatorMalloc(raw, 1);
    ASSERT_NE(first, 0U);
    auto *firstHdr = reinterpret_cast<MemoryHdr *>(FALCON_SHMEM_ALLOCATOR_GET_POINTER(raw,
        first - sizeof(MemoryHdr)));
    EXPECT_EQ(firstHdr->size, 1U);
    FalconShmemAllocatorFree(raw, first);

    uint64_t reused = FalconShmemAllocatorMalloc(raw, 1);
    ASSERT_NE(reused, 0U);
    auto *reusedHdr = reinterpret_cast<MemoryHdr *>(FALCON_SHMEM_ALLOCATOR_GET_POINTER(raw,
        reused - sizeof(MemoryHdr)));
    reusedHdr->capacity = 123;
    FalconShmemAllocatorFree(raw, reused);

    uint64_t invalidHigh = FalconShmemAllocatorMalloc(raw, 1);
    ASSERT_NE(invalidHigh, 0U);
    auto *invalidHighHdr = reinterpret_cast<MemoryHdr *>(FALCON_SHMEM_ALLOCATOR_GET_POINTER(raw,
        invalidHigh - sizeof(MemoryHdr)));
    invalidHighHdr->capacity = FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE + 1;
    FalconShmemAllocatorFree(raw, invalidHigh);

    FalconShmemAllocator *globalAllocator = GetFalconConnectionPoolShmemAllocator();
    EXPECT_NE(globalAllocator, nullptr);
}

TEST(ConnectionPoolCoverageUT, ShmemAllocatorCoversFullPageAndHintBranches)
{
    /* Exercise Shared Memory Allocator covers Full Page And Hint branches and assert the relevant success or failure branch. */
    TestAllocator twoPageAllocator(3 * 1024 * 1024);
    FalconShmemAllocator *twoPage = twoPageAllocator.get();

    uint64_t tiny = FalconShmemAllocatorMalloc(twoPage, 1);
    ASSERT_NE(tiny, 0U);
    uint64_t fullPageAfterUsedPage = FalconShmemAllocatorMalloc(
        twoPage, FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE - sizeof(MemoryHdr));
    ASSERT_NE(fullPageAfterUsedPage, 0U);
    FalconShmemAllocatorFree(twoPage, fullPageAfterUsedPage);

    constexpr int minBlockLevel = FALCON_SHMEM_ALLOCATOR_FREE_LIST_COUNT - 1;
    twoPage->freeListHint[minBlockLevel].data.store(twoPage->pageCount, std::memory_order_relaxed);
    uint64_t secondScanAlloc = FalconShmemAllocatorMalloc(twoPage, 1);
    ASSERT_NE(secondScanAlloc, 0U);
    FalconShmemAllocatorFree(twoPage, secondScanAlloc);

    std::vector<uint64_t> blocks;
    blocks.reserve(FALCON_SHMEM_ALLOCATOR_STATE_BIT_COUNT);
    for (int i = 0; i < FALCON_SHMEM_ALLOCATOR_STATE_BIT_COUNT; ++i) {
        uint64_t block = FalconShmemAllocatorMalloc(twoPage, 1);
        ASSERT_NE(block, 0U);
        blocks.push_back(block);
    }
    for (uint64_t block : blocks) {
        FalconShmemAllocatorFree(twoPage, block);
    }

    TestAllocator onePageAllocator(2 * 1024 * 1024);
    FalconShmemAllocator *onePage = onePageAllocator.get();
    uint64_t onlyPage = FalconShmemAllocatorMalloc(
        onePage, FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE - sizeof(MemoryHdr));
    ASSERT_NE(onlyPage, 0U);
    EXPECT_EQ(FalconShmemAllocatorMalloc(
        onePage, FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE - sizeof(MemoryHdr)), 0U);
    FalconShmemAllocatorFree(onePage, onlyPage);
}

TEST(ConnectionPoolCoverageUT, StubbedPoolInitCreatesAndStopsConnections)
{
    /* Exercise Stubbed Pool Init creates And Stops Connections and assert the relevant success or failure branch. */
    PGConnection::Reset();
    FakeMetaServiceJob::ResetCounters();
    PGConnectionPool &pool = PGConnectionPool::GetInstance();

    ASSERT_TRUE(pool.Init(55510, "tester", 1, 20, 40));
    EXPECT_EQ(PGConnection::constructed, 1);

    auto *emptyJob = new FakeMetaServiceJob(FalconMetaServiceType::CREATE, true, true);
    pool.DispatchMetaServiceJob(emptyJob);
    delete emptyJob;

    auto *batchJob = new FakeMetaServiceJob(FalconMetaServiceType::MKDIR, true);
    auto *singleJob = new FakeMetaServiceJob(FalconMetaServiceType::STAT, false);
    pool.DispatchMetaServiceJob(batchJob);
    pool.DispatchMetaServiceJob(singleJob);

    for (int i = 0; i < 200 && PGConnection::execCount < 2; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_GE(PGConnection::execCount, 2);
    EXPECT_NE(PGConnection::lastTask, nullptr);

    PGConnection::lastTask.reset();
    pool.Destroy();
    EXPECT_EQ(PGConnection::stopCount, 1);
    EXPECT_EQ(PGConnection::destructed, 1);
}

TEST(ConnectionPoolCoverageUT, WorkerTasksRejectInvalidInputs)
{
    /* Exercise Worker Tasks Reject invalid Inputs and assert the relevant success or failure branch. */
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

TEST(ConnectionPoolCoverageUT, WorkerTasksCoverAllocatorFailureBranches)
{
    /* Exercise Worker Tasks Cover Allocator Failure branches and assert the relevant success or failure branch. */
    TestAllocator allocator(2 * 1024 * 1024);
    uint64_t fullPage = FalconShmemAllocatorMalloc(
        allocator.get(), FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE - sizeof(MemoryHdr));
    ASSERT_NE(fullPage, 0U);

    flatbuffers::FlatBufferBuilder flatBufferBuilder;
    SerializedData replyBuilder;
    SerializedDataInit(&replyBuilder, nullptr, 0, 0, nullptr);

    {
        auto *job = new FakeMetaServiceJob(FalconMetaServiceType::MKDIR);
        SingleWorkerTask task(allocator.get(), job);
        EXPECT_THROW(task.DoWork(nullptr, flatBufferBuilder, replyBuilder), std::runtime_error);
    }

    {
        std::vector<BaseMetaServiceJob *> jobs{
            new FakeMetaServiceJob(FalconMetaServiceType::MKDIR),
        };
        BatchWorkerTask task(allocator.get(), jobs);
        EXPECT_THROW(task.DoWork(nullptr, flatBufferBuilder, replyBuilder), std::runtime_error);
    }

    FalconShmemAllocatorFree(allocator.get(), fullPage);
    SerializedDataDestroy(&replyBuilder);
}

TEST(ConnectionPoolCoverageUT, BatchWorkerTaskCoversStatIndicesAllocationDrop)
{
    /* Exercise Batch Worker Task covers Stat Indices Allocation Drop and assert the relevant success or failure branch. */
    ResetFakePg();
    FakeMetaServiceJob::ResetCounters();
    TestAllocator allocator(2 * 1024 * 1024);
    flatbuffers::FlatBufferBuilder flatBufferBuilder;
    SerializedData replyBuilder;
    SerializedDataInit(&replyBuilder, nullptr, 0, 0, nullptr);

    size_t almostFullPage = FALCON_SHMEM_ALLOCATOR_MAX_SUPPORT_ALLOC_SIZE - sizeof(MemoryHdr);
    std::vector<char> largeRequest(almostFullPage, 'x');

    auto *result = new FakePgResult();
    result->status = PGRES_TUPLES_OK;
    result->rows = 1;
    result->cols = 1;
    result->values = {"0"};
    QueueFakePgResult(result);

    std::vector<BaseMetaServiceJob *> jobs{
        new FakeMetaServiceJob(FalconMetaServiceType::MKDIR, true, false, 1, std::move(largeRequest)),
    };
    BatchWorkerTask task(allocator.get(), jobs);
    EXPECT_NO_THROW(task.DoWork(nullptr, flatBufferBuilder, replyBuilder));
    EXPECT_EQ(FakeMetaServiceJob::doneCount, 1);

    SerializedDataDestroy(&replyBuilder);
    ResetFakePg();
}

TEST(ConnectionPoolCoverageUT, WorkerTasksRejectCorruptPgReplyShapes)
{
    /* Exercise Worker Tasks Reject Corrupt Pg Reply Shapes and assert the relevant success or failure branch. */
    ResetFakePg();
    TestAllocator allocator(4 * 1024 * 1024);
    flatbuffers::FlatBufferBuilder flatBufferBuilder;
    SerializedData replyBuilder;
    SerializedDataInit(&replyBuilder, nullptr, 0, 0, nullptr);

    {
        auto *result = new FakePgResult();
        result->status = PGRES_TUPLES_OK;
        result->rows = 0;
        result->cols = 1;
        QueueFakePgResult(result);

        auto *job = new FakeMetaServiceJob(FalconMetaServiceType::MKDIR);
        SingleWorkerTask task(allocator.get(), job);
        EXPECT_THROW(task.DoWork(nullptr, flatBufferBuilder, replyBuilder), std::runtime_error);
        ResetFakePg();
    }

    {
        uint64_t replyShift = FalconShmemAllocatorMalloc(allocator.get(), 8);
        ASSERT_NE(replyShift, 0U);
        auto *result = new FakePgResult();
        result->status = PGRES_TUPLES_OK;
        result->rows = 1;
        result->cols = 1;
        result->values = {std::to_string(replyShift)};
        QueueFakePgResult(result);

        auto *job = new FakeMetaServiceJob(FalconMetaServiceType::MKDIR);
        SingleWorkerTask task(allocator.get(), job);
        EXPECT_THROW(task.DoWork(nullptr, flatBufferBuilder, replyBuilder), std::runtime_error);
        ResetFakePg();
    }

    {
        std::vector<BaseMetaServiceJob *> jobs{
            new FakeMetaServiceJob(FalconMetaServiceType::MKDIR),
        };
        BatchWorkerTask task(allocator.get(), jobs);
        EXPECT_THROW(task.DoWork(nullptr, flatBufferBuilder, replyBuilder), std::runtime_error);
    }

    {
        auto *result = new FakePgResult();
        result->status = PGRES_TUPLES_OK;
        result->rows = 0;
        result->cols = 1;
        QueueFakePgResult(result);

        std::vector<BaseMetaServiceJob *> jobs{
            new FakeMetaServiceJob(FalconMetaServiceType::MKDIR),
        };
        BatchWorkerTask task(allocator.get(), jobs);
        EXPECT_THROW(task.DoWork(nullptr, flatBufferBuilder, replyBuilder), std::runtime_error);
        ResetFakePg();
    }

    SerializedDataDestroy(&replyBuilder);
}

TEST(ConnectionPoolCoverageUT, BatchWorkerTaskRejectsCorruptSplitReply)
{
    /* Exercise Batch Worker Task rejects Corrupt Split Reply and assert the relevant success or failure branch. */
    ResetFakePg();
    TestAllocator allocator(4 * 1024 * 1024);
    flatbuffers::FlatBufferBuilder flatBufferBuilder;
    SerializedData replyBuilder;
    SerializedDataInit(&replyBuilder, nullptr, 0, 0, nullptr);

    auto replyData = MakeSerializedBytes("abcd", 4);
    uint64_t replyShift = FalconShmemAllocatorMalloc(allocator.get(), replyData.size());
    ASSERT_NE(replyShift, 0U);
    std::memcpy(FALCON_SHMEM_ALLOCATOR_GET_POINTER(allocator.get(), replyShift), replyData.data(), replyData.size());

    auto *result = new FakePgResult();
    result->status = PGRES_TUPLES_OK;
    result->rows = 1;
    result->cols = 1;
    result->values = {std::to_string(replyShift)};
    QueueFakePgResult(result);

    std::vector<BaseMetaServiceJob *> jobs{
        new FakeMetaServiceJob(FalconMetaServiceType::MKDIR, true, false, 2),
    };
    BatchWorkerTask task(allocator.get(), jobs);
    EXPECT_THROW(task.DoWork(nullptr, flatBufferBuilder, replyBuilder), std::runtime_error);

    SerializedDataDestroy(&replyBuilder);
    ResetFakePg();
}

TEST(ConnectionPoolCoverageUT, SingleWorkerTaskProcessesPlainCommandResult)
{
    /* Exercise Single Worker Task Processes Plain Command Result and assert the relevant success or failure branch. */
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
    /* Exercise Batch Worker Task handles Zero Reply Shift and assert the relevant success or failure branch. */
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
    /* Exercise Worker Tasks Convert Pg Errors To Responses and assert the relevant success or failure branch. */
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
