/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_PERF_MACROS_H
#define FALCON_PERF_MACROS_H

#include "postgres.h"
#include "storage/lwlock.h"
#include "perf_counter/falcon_perf_latency_shmem.h"
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Get current time in nanoseconds */
static inline uint64
GetCurrentTimeNs(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64)ts.tv_sec * 1000000000ULL + (uint64)ts.tv_nsec;
}

/* Report latency to shared memory */
static inline void
ReportLatency(LatencyData* data, uint64 latency_ns)
{
    if (data == NULL)
        return;

    LWLockAcquire(&data->lock, LW_EXCLUSIVE);
    data->sum_ns += latency_ns;
    data->count++;
    if (latency_ns < data->min_ns)
        data->min_ns = latency_ns;
    if (latency_ns > data->max_ns)
        data->max_ns = latency_ns;
    LWLockRelease(&data->lock);
}

/* Latency timer structure (stack-allocated, zero overhead) */
typedef struct LatencyTimer
{
    uint64 start_time;
    LatencyData* target;
} LatencyTimer;

/* Start timer macro */
#define PERF_TIMER_START(timer, target_data) \
    do { \
        (timer).start_time = GetCurrentTimeNs(); \
        (timer).target = (target_data); \
    } while(0)

/* End timer and report macro */
#define PERF_TIMER_END(timer) \
    do { \
        if ((timer).target != NULL) { \
            uint64 end_time = GetCurrentTimeNs(); \
            uint64 latency = end_time - (timer).start_time; \
            ReportLatency((timer).target, latency); \
        } \
    } while(0)

/* Auto-named timer macros (manual BEGIN/END) */
#define PERF_LATENCY_BEGIN(name, target_data) \
    LatencyTimer name##_timer; \
    PERF_TIMER_START(name##_timer, target_data)

#define PERF_LATENCY_END(name) \
    PERF_TIMER_END(name##_timer)

/*
 * ============ Auto cleanup (GCC extension) ============
 * Timer automatically reports when leaving scope, like C++ RAII.
 *
 * Usage 1: Auto report on scope exit (whole function)
 *   void foo() {
 *       PERF_SCOPED_TIMER(timer, &perf->xxxLatency);
 *       // ... code ...
 *   }  // automatically reports when function returns
 *
 * Usage 2: Manual end for partial measurement
 *   void foo() {
 *       PERF_SCOPED_TIMER(timer, &perf->xxxLatency);
 *       // ... code to measure ...
 *       PERF_SCOPED_END(timer);  // report now, won't report again on exit
 *       // ... more code not measured ...
 *   }
 */
static inline void
LatencyTimerCleanup(LatencyTimer *timer)
{
    if (timer->target != NULL) {
        uint64 end_time = GetCurrentTimeNs();
        uint64 latency = end_time - timer->start_time;
        ReportLatency(timer->target, latency);
        timer->target = NULL;  /* prevent double report */
    }
}

/* Define scoped timer - auto reports on scope exit */
#define PERF_SCOPED_TIMER(name, target_data) \
    LatencyTimer name __attribute__((cleanup(LatencyTimerCleanup))) = { \
        .start_time = GetCurrentTimeNs(), \
        .target = (target_data) \
    }

/* Optional: manually end and report now (won't report again on scope exit) */
#define PERF_SCOPED_END(name) \
    LatencyTimerCleanup(&(name))

#ifdef __cplusplus
}
#endif

#endif /* FALCON_PERF_MACROS_H */
