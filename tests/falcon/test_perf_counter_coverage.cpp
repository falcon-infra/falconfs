#include <gtest/gtest.h>

#include <cstdarg>
#include <cstddef>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

extern "C" {
#include "perf_counter/falcon_per_request_stat.h"
#include "postgres.h"
}

#ifdef vsnprintf
#undef vsnprintf
#endif
#ifdef snprintf
#undef snprintf
#endif

namespace {

std::vector<unsigned char> g_shmem;
bool g_shmemFound = false;
std::vector<std::string> g_logMessages;

void ResetPerfShmem()
{
    g_shmem.clear();
    g_shmemFound = false;
    g_logMessages.clear();
    g_FalconPerRequestStatShmem = nullptr;
    FalconPerfEnabled = true;
}

void InitFreshPerfShmem()
{
    ResetPerfShmem();
    FalconPerRequestStatShmemInit();
    ASSERT_NE(g_FalconPerRequestStatShmem, nullptr);
}

} // namespace

extern "C" {

void *ShmemInitStruct(const char *, Size size, bool *found)
{
    *found = g_shmemFound;
    if (g_shmem.empty()) {
        g_shmem.resize(size);
        std::memset(g_shmem.data(), 0, size);
    }
    return g_shmem.data();
}

bool errstart(int, const char *)
{
    return true;
}

bool errstart_cold(int elevel, const char *domain)
{
    return errstart(elevel, domain);
}

void errfinish(const char *, int, const char *)
{
}

int errmsg(const char *fmt, ...)
{
    char buffer[512];
    va_list args;
    va_start(args, fmt);
    std::vsnprintf(buffer, sizeof(buffer), fmt, args);
    va_end(args);
    g_logMessages.emplace_back(buffer);
    return 0;
}

int pg_vsnprintf(char *str, size_t count, const char *fmt, va_list args)
{
    return std::vsnprintf(str, count, fmt, args);
}

int pg_snprintf(char *str, size_t count, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    int ret = std::vsnprintf(str, count, fmt, args);
    va_end(args);
    return ret;
}

} // extern "C"

TEST(PerfCounterCoverageUT, ShmemInitInitializesDefaultsAndHonorsExistingBlock)
{
    /* Exercise Shared Memory Init initializes Defaults And Honors Existing Block and assert the relevant success or failure branch. */
    InitFreshPerfShmem();

    EXPECT_TRUE(g_FalconPerRequestStatShmem->enabled);
    EXPECT_EQ(g_FalconPerRequestStatShmem->nextIndex, 0);
    EXPECT_EQ(g_FalconPerRequestStatShmem->accum[MKDIR].gaps[0].min_ns, INT64_MAX);
    EXPECT_GE(FalconPerRequestStatShmemSize(), sizeof(FalconPerRequestStatShmem));

    g_FalconPerRequestStatShmem->enabled = false;
    g_shmemFound = true;
    FalconPerRequestStatShmemInit();
    EXPECT_FALSE(g_FalconPerRequestStatShmem->enabled);
}

TEST(PerfCounterCoverageUT, AllocationRejectsDisabledAndFullSlots)
{
    /* Exercise Allocation rejects Disabled And Full Slots and assert the relevant success or failure branch. */
    ResetPerfShmem();
    EXPECT_EQ(PerRequestStatAllocIndex(), -1);

    InitFreshPerfShmem();
    g_FalconPerRequestStatShmem->enabled = false;
    EXPECT_EQ(PerRequestStatAllocIndex(), -1);

    g_FalconPerRequestStatShmem->enabled = true;
    int32_t idx = PerRequestStatAllocIndex();
    ASSERT_EQ(idx, 0);
    RequestStat *slot = &g_FalconPerRequestStatShmem->statArray[idx];
    EXPECT_TRUE(slot->inUse);
    EXPECT_FALSE(slot->completed);
    EXPECT_EQ(slot->checkpointCount, 1);
    EXPECT_GT(slot->timestamps[CKPT_START], 0);

    g_FalconPerRequestStatShmem->nextIndex = STAT_ARRAY_SIZE;
    EXPECT_EQ(PerRequestStatAllocIndex(), -1);
    EXPECT_EQ(g_FalconPerRequestStatShmem->allocDropCount, 1);
}

TEST(PerfCounterCoverageUT, CheckpointsCompleteAndAggregate)
{
    /* Exercise Checkpoints Complete And Aggregate and assert the relevant success or failure branch. */
    InitFreshPerfShmem();
    int32_t idx = PerRequestStatAllocIndex();
    ASSERT_GE(idx, 0);

    StatCheckpoint(-1, CKPT_DISPATCH);
    StatCheckpoint(STAT_ARRAY_SIZE, CKPT_DISPATCH);
    StatCheckpoint(idx, -1);
    StatCheckpoint(idx, STAT_MAX_CHECKPOINTS);

    StatCheckpoint(idx, CKPT_DISPATCH);
    StatCheckpoint(idx, CKPT_ENQUEUE);

    int32_t indices[] = {idx, -1, STAT_ARRAY_SIZE, 2};
    StatCheckpointBroadcast(indices, 4, CKPT_DEQUEUE);

    PerRequestStatComplete(idx, MKDIR);
    EXPECT_FALSE(g_FalconPerRequestStatShmem->statArray[idx].inUse);
    EXPECT_EQ(g_FalconPerRequestStatShmem->accum[MKDIR].requestCount, 1);
    EXPECT_GE(g_FalconPerRequestStatShmem->accum[MKDIR].maxCheckpointCount, CKPT_ENQUEUE + 1);
    EXPECT_EQ(g_FalconPerRequestStatShmem->accum[MKDIR].gaps[0].count, 1);

    g_FalconPerRequestStatShmem->allocDropCount = 2;
    g_FalconPerRequestStatShmem->statIndicesAllocDropCount = 3;
    g_FalconPerRequestStatShmem->accum[KV_GET].requestCount = 1;
    g_FalconPerRequestStatShmem->accum[KV_GET].e2eSumNs = 0;
    g_FalconPerRequestStatShmem->accum[KV_GET].maxCheckpointCount = 3;
    g_FalconPerRequestStatShmem->accum[KV_GET].gaps[0].count = 1;
    g_FalconPerRequestStatShmem->accum[KV_GET].gaps[0].sum_ns = 0;
    g_FalconPerRequestStatShmem->accum[KV_GET].gaps[0].min_ns = INT64_MAX;
    g_FalconPerRequestStatShmem->accum[KV_GET].gaps[0].max_ns = 0;

    PerRequestStatAggregateAndOutput();
    EXPECT_EQ(g_FalconPerRequestStatShmem->allocDropCount, 0);
    EXPECT_EQ(g_FalconPerRequestStatShmem->statIndicesAllocDropCount, 0);
    EXPECT_EQ(g_FalconPerRequestStatShmem->accum[MKDIR].requestCount, 0);
    EXPECT_FALSE(g_logMessages.empty());

    g_logMessages.clear();
    PerRequestStatAggregateAndOutput();
    EXPECT_TRUE(g_logMessages.empty());
}

TEST(PerfCounterCoverageUT, CompleteReleasesInvalidAndShortRequests)
{
    /* Exercise Complete Releases invalid And Short Requests and assert the relevant success or failure branch. */
    InitFreshPerfShmem();
    PerRequestStatComplete(-1, MKDIR);

    int32_t idx = PerRequestStatAllocIndex();
    ASSERT_GE(idx, 0);
    PerRequestStatComplete(idx, -1);
    EXPECT_FALSE(g_FalconPerRequestStatShmem->statArray[idx].inUse);
    EXPECT_EQ(g_FalconPerRequestStatShmem->accum[MKDIR].requestCount, 0);

    idx = PerRequestStatAllocIndex();
    ASSERT_GE(idx, 0);
    PerRequestStatComplete(idx, NOT_SUPPORTED);
    EXPECT_FALSE(g_FalconPerRequestStatShmem->statArray[idx].inUse);

    idx = PerRequestStatAllocIndex();
    ASSERT_GE(idx, 0);
    g_FalconPerRequestStatShmem->statArray[idx].checkpointCount = 1;
    PerRequestStatComplete(idx, MKDIR);
    EXPECT_FALSE(g_FalconPerRequestStatShmem->statArray[idx].inUse);
    EXPECT_EQ(g_FalconPerRequestStatShmem->accum[MKDIR].requestCount, 0);
}

TEST(PerfCounterCoverageUT, BroadcastSkipsWhenDisabledOrOutOfRange)
{
    /* Exercise Broadcast Skips When Disabled Or Out Of Range and assert the relevant success or failure branch. */
    InitFreshPerfShmem();
    int32_t idx = PerRequestStatAllocIndex();
    ASSERT_GE(idx, 0);
    int32_t indices[] = {idx};

    g_FalconPerRequestStatShmem->enabled = false;
    StatCheckpointBroadcast(indices, 1, CKPT_DISPATCH);
    EXPECT_EQ(g_FalconPerRequestStatShmem->statArray[idx].checkpointCount, 1);

    g_FalconPerRequestStatShmem->enabled = true;
    StatCheckpointBroadcast(indices, 1, -1);
    StatCheckpointBroadcast(indices, 1, STAT_MAX_CHECKPOINTS);
    EXPECT_EQ(g_FalconPerRequestStatShmem->statArray[idx].checkpointCount, 1);
}

TEST(PerfCounterCoverageUT, AggregatePrintsEveryOpcodeNameAndFallbackCheckpointName)
{
    /* Exercise Aggregate Prints Every Opcode Name And Fallback Checkpoint Name and assert the relevant success or failure branch. */
    InitFreshPerfShmem();

    for (int op = MKDIR_SUB_MKDIR; op < NOT_SUPPORTED; ++op) {
        g_FalconPerRequestStatShmem->accum[op].requestCount = 1;
        g_FalconPerRequestStatShmem->accum[op].e2eSumNs = 1000;
        g_FalconPerRequestStatShmem->accum[op].maxCheckpointCount = 1;
    }

    g_FalconPerRequestStatShmem->accum[MKDIR_SUB_MKDIR].maxCheckpointCount = STAT_MAX_CHECKPOINTS;
    g_FalconPerRequestStatShmem->accum[MKDIR_SUB_MKDIR].gaps[STAT_MAX_CHECKPOINTS - 2].count = 1;
    g_FalconPerRequestStatShmem->accum[MKDIR_SUB_MKDIR].gaps[STAT_MAX_CHECKPOINTS - 2].sum_ns = 10;
    g_FalconPerRequestStatShmem->accum[MKDIR_SUB_MKDIR].gaps[STAT_MAX_CHECKPOINTS - 2].min_ns = 10;
    g_FalconPerRequestStatShmem->accum[MKDIR_SUB_MKDIR].gaps[STAT_MAX_CHECKPOINTS - 2].max_ns = 10;

    g_FalconPerRequestStatShmem->accum[CREATE].maxCheckpointCount = 2;
    g_FalconPerRequestStatShmem->accum[CREATE].requestCount = 2;
    g_FalconPerRequestStatShmem->accum[CREATE].gaps[0].count = 1;
    g_FalconPerRequestStatShmem->accum[CREATE].gaps[0].sum_ns = 5;
    g_FalconPerRequestStatShmem->accum[CREATE].gaps[0].min_ns = 5;
    g_FalconPerRequestStatShmem->accum[CREATE].gaps[0].max_ns = 5;

    PerRequestStatAggregateAndOutput();
    EXPECT_FALSE(g_logMessages.empty());
}
