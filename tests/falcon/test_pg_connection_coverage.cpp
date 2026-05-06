#include <gtest/gtest.h>

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

#include "connection_pool/falcon_worker_task.h"

namespace {

struct FakePgConn {
    int marker{0};
};

struct FakePgResult {
    ExecStatusType status{PGRES_TUPLES_OK};
    std::string error;
};

FakePgConn g_conn;
ConnStatusType g_connectStatus = CONNECTION_OK;
std::string g_connError;
std::deque<FakePgResult *> g_execResults;
std::deque<FakePgResult *> g_pendingResults;
int g_clearCount = 0;
int g_finishCount = 0;
std::string g_lastConnInfo;
std::string g_lastExecSql;

void ResetFakePg()
{
    for (auto *result : g_execResults) {
        delete result;
    }
    for (auto *result : g_pendingResults) {
        delete result;
    }
    g_execResults.clear();
    g_pendingResults.clear();
    g_connectStatus = CONNECTION_OK;
    g_connError.clear();
    g_clearCount = 0;
    g_finishCount = 0;
    g_lastConnInfo.clear();
    g_lastExecSql.clear();
}

PGresult *AsPgResult(FakePgResult *result)
{
    return reinterpret_cast<PGresult *>(result);
}

FakePgResult *AsFakeResult(const PGresult *result)
{
    return reinterpret_cast<FakePgResult *>(const_cast<PGresult *>(result));
}

class RecordingTask : public BaseWorkerTask {
  public:
    explicit RecordingTask(bool shouldThrow = false) : BaseWorkerTask(nullptr), shouldThrow_(shouldThrow) {}

    void DoWork(PGconn *conn, flatbuffers::FlatBufferBuilder &, SerializedData &) override
    {
        EXPECT_EQ(conn, reinterpret_cast<PGconn *>(&g_conn));
        ran = true;
        if (shouldThrow_) {
            throw std::runtime_error("task failed");
        }
    }

    bool ran{false};

  private:
    bool shouldThrow_;
};

class NotifyState {
  public:
    void Mark()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        notified_ = true;
        cv_.notify_one();
    }

    bool Wait()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::seconds(2), [&] { return notified_; });
    }

  private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool notified_{false};
};

} // namespace

extern "C" {

PGconn *PQconnectdb(const char *conninfo)
{
    g_lastConnInfo = conninfo == nullptr ? "" : conninfo;
    return reinterpret_cast<PGconn *>(&g_conn);
}

ConnStatusType PQstatus(const PGconn *)
{
    return g_connectStatus;
}

char *PQerrorMessage(const PGconn *)
{
    return g_connError.empty() ? const_cast<char *>("fake connection error") : g_connError.data();
}

PGresult *PQexec(PGconn *, const char *query)
{
    g_lastExecSql = query == nullptr ? "" : query;
    if (g_execResults.empty()) {
        return AsPgResult(new FakePgResult());
    }
    FakePgResult *result = g_execResults.front();
    g_execResults.pop_front();
    return AsPgResult(result);
}

PGresult *PQgetResult(PGconn *)
{
    if (g_pendingResults.empty()) {
        return nullptr;
    }
    FakePgResult *result = g_pendingResults.front();
    g_pendingResults.pop_front();
    return AsPgResult(result);
}

ExecStatusType PQresultStatus(const PGresult *result)
{
    return AsFakeResult(result)->status;
}

char *PQresultErrorMessage(const PGresult *result)
{
    return const_cast<char *>(AsFakeResult(result)->error.c_str());
}

void PQclear(PGresult *result)
{
    ++g_clearCount;
    delete AsFakeResult(result);
}

void PQfinish(PGconn *)
{
    ++g_finishCount;
}

}

#include "../../falcon/connection_pool/pg_connection.cpp"

TEST(PGConnectionCoverageUT, ConstructorRejectsConnectionAndPrepareFailures)
{
    ResetFakePg();
    g_connectStatus = CONNECTION_BAD;
    EXPECT_THROW(PGConnection([](PGConnection *) {}, "127.0.0.1", 5432, "hx"), std::runtime_error);

    ResetFakePg();
    auto *prepareFailure = new FakePgResult();
    prepareFailure->status = PGRES_FATAL_ERROR;
    prepareFailure->error = "prepare failed";
    g_execResults.push_back(prepareFailure);
    EXPECT_THROW(PGConnection([](PGConnection *) {}, "127.0.0.1", 5432, "hx"), std::runtime_error);
    EXPECT_EQ(g_lastExecSql, "SELECT falcon_prepare_commands();");
}

TEST(PGConnectionCoverageUT, ExecutesTaskAndStopsFromNotify)
{
    ResetFakePg();
    NotifyState notifyState;
    auto task = std::make_shared<RecordingTask>();

    {
        PGConnection connection(
            [&](PGConnection *conn) {
                conn->Stop();
                notifyState.Mark();
            },
            "127.0.0.1",
            5432,
            "hx");
        EXPECT_NE(g_lastConnInfo.find("hostaddr=127.0.0.1"), std::string::npos);
        EXPECT_NE(g_lastConnInfo.find("port=5432"), std::string::npos);
        EXPECT_NE(g_lastConnInfo.find("user=hx"), std::string::npos);

        connection.Exec(task);
        EXPECT_TRUE(notifyState.Wait());
    }

    EXPECT_TRUE(task->ran);
    EXPECT_EQ(g_finishCount, 1);
}

TEST(PGConnectionCoverageUT, BackgroundWorkerClearsPendingResultsAfterTaskException)
{
    ResetFakePg();
    g_pendingResults.push_back(new FakePgResult());
    g_pendingResults.push_back(new FakePgResult());
    NotifyState notifyState;

    {
        PGConnection connection(
            [&](PGConnection *conn) {
                conn->Stop();
                notifyState.Mark();
            },
            "127.0.0.1",
            5432,
            "hx");
        connection.Exec(std::make_shared<RecordingTask>(true));
        EXPECT_TRUE(notifyState.Wait());
    }

    EXPECT_EQ(g_clearCount, 2);
}
