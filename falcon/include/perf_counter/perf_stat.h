/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_PERF_STAT_H
#define FALCON_PERF_STAT_H

#include <chrono>
#include <cstdint>

/* Forward declarations to avoid including PostgreSQL headers */
struct LatencyData;
struct FalconPerfLatencyShmem;

/* Report latency using atomic operations (safe for C++ std::thread) */
extern "C" void ReportLatencyToShmemAtomic(LatencyData* data, uint64_t latencyNs);

/* Getter functions for LatencyData pointers (to avoid exposing struct layout) */
extern "C" LatencyData* GetTotalRequestLatencyData();
extern "C" LatencyData* GetQueueWaitLatencyData();
extern "C" LatencyData* GetEnqueueDelayLatencyData();
extern "C" LatencyData* GetInQueueLatencyData();
extern "C" LatencyData* GetConnWaitLatencyData();
extern "C" LatencyData* GetWorkerWaitLatencyData();
/* Batch path */
extern "C" LatencyData* GetShmemCopyLatencyData();
extern "C" LatencyData* GetPgExecTotalLatencyData();
extern "C" LatencyData* GetResultProcLatencyData();
/* Single (non-batch) path */
extern "C" LatencyData* GetShmemCopySingleLatencyData();
extern "C" LatencyData* GetPgExecSingleLatencyData();
extern "C" LatencyData* GetResultProcSingleLatencyData();

extern "C" LatencyData* GetOpcodeE2ELatencyData(int opcode);

constexpr uint64_t NS_PER_US = 1000ULL;
constexpr uint64_t NS_PER_MS = 1000000ULL;
constexpr uint64_t NS_PER_SEC = 1000000000ULL;

/*
 * RAII Timer class for automatic latency measurement.
 * Directly operates on LatencyData* in shared memory.
 *
 * Usage:
 *   {
 *       LatencyTimer timer(&g_FalconPerfLatencyShmem->shmemCopyLatency);
 *       // ... code to measure ...
 *   }  // automatically reports latency on destruction
 */
class LatencyTimer {
public:
    explicit LatencyTimer(LatencyData* data = nullptr, bool startNow = true)
        : m_data(data), m_started(false), m_startTime(0), m_latency(0)
    {
        if (data != nullptr && startNow) {
            Start();
        }
    }

    LatencyTimer(const LatencyTimer&) = delete;
    LatencyTimer& operator=(const LatencyTimer&) = delete;

    ~LatencyTimer() {
        if (m_started) {
            End();
        }
    }

    /* Bind to a different LatencyData */
    void Bind(LatencyData* data) noexcept {
        m_data = data;
    }

    /* Start timing */
    void Start() {
        m_startTime = GetCurrentTimeNs();
        m_started = true;
        m_latency = 0;
    }

    /* Bind and start in one call */
    void BindAndStart(LatencyData* data) noexcept {
        Bind(data);
        Start();
    }

    /* End timing and report to shared memory */
    void End(LatencyData* data = nullptr) noexcept {
        if (data != nullptr) {
            Bind(data);
        }

        if (m_started) {
            m_started = false;
            uint64_t endTime = GetCurrentTimeNs();
            m_latency = endTime - m_startTime;
            ReportLatencyToShmemAtomic(m_data, m_latency);
        }
    }

    /* End current timing and restart for next measurement */
    void EndAndRestart(LatencyData* data = nullptr) noexcept {
        End(data);
        Start();
    }

    /* Get the last measured latency */
    uint64_t GetLatencyNs() const { return m_latency; }
    uint64_t GetLatencyUs() const { return m_latency / NS_PER_US; }
    uint64_t GetLatencyMs() const { return m_latency / NS_PER_MS; }

    /* Get current elapsed time without ending */
    uint64_t GetElapsedNs() const {
        if (!m_started) return 0;
        return GetCurrentTimeNs() - m_startTime;
    }
    uint64_t GetElapsedMs() const { return GetElapsedNs() / NS_PER_MS; }

    /* Get current time in nanoseconds (static utility) */
    static uint64_t GetCurrentTimeNs() {
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now().time_since_epoch()).count());
    }

private:
    LatencyData* m_data;
    bool m_started;
    uint64_t m_startTime;
    uint64_t m_latency;
};

#endif /* FALCON_PERF_STAT_H */
