/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#ifndef FALCON_UTILS_RWLOCK_H
#define FALCON_UTILS_RWLOCK_H

#include "postgres.h"

#include "port/atomics.h"

typedef struct RWLock
{
    pg_atomic_uint64 state;
} RWLock;

typedef enum RWLockMode {
    RW_EXCLUSIVE,
    RW_SHARED,

    RW_DECLARE,
} RWLockMode;

extern void RWLockInitialize(RWLock *lock);
void RWLockDeclare(RWLock *lock);
void RWLockUndeclare(RWLock *lock);
bool RWLockCheckDestroyable(RWLock *lock);
void RWLockAcquire(RWLock *lock, RWLockMode mode);
void RWLockRelease(RWLock *lock);
void RWLockReleaseAll(bool keepInterruptHoldoffCount);

/*
 * RWLockGetHeldCount - Get current number of held RWLocks
 *
 * Returns the count of locks in held_rwlocks array. Used by sub-transaction
 * callback to record the lock state at sub-transaction start.
 */
int RWLockGetHeldCount(void);

/*
 * RWLockReleaseSince - Release RWLocks acquired after savedCount
 *
 * Parameters:
 *   savedCount - Number of locks held before the critical section (e.g., before
 *                sub-transaction start). Only locks at indices >= savedCount
 *                will be released.
 *   keepInterruptHoldoffCount - If true, preserve InterruptHoldoffCount value
 *                               across the release loop. Critical for error
 *                               handling when errfinish() has reset the count.
 *
 * Context:
 *   This function is designed for sub-transaction abort handling. When a
 *   sub-transaction aborts due to ERROR:
 *   1. errfinish() forcibly resets InterruptHoldoffCount to 0
 *   2. We need to release locks acquired during the sub-transaction
 *   3. But we must preserve parent transaction's locks
 *   4. And we must not disturb the InterruptHoldoffCount = 0 state
 *
 * Implementation:
 *   - Save InterruptHoldoffCount before the loop
 *   - For each lock release, temporarily HOLD_INTERRUPTS() to offset the
 *     RESUME_INTERRUPTS() inside RWLockRelease()
 *   - After the loop, restore InterruptHoldoffCount to its saved value
 *   - This ensures the loop's internal HOLD/RESUME pairs don't affect
 *     external state, even if the loop exits abnormally
 */
void RWLockReleaseSince(int savedCount, bool keepInterruptHoldoffCount);

#endif
