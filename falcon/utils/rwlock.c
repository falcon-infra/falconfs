/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "utils/rwlock.h"

#include "miscadmin.h"
#include "storage/s_lock.h"

#include "utils/error_log.h"

// must be greater than MAX_BACKENDS - which is 2^23 - 1
#define BIT_COUNT_FOR_SHARED 24

/* if there are more than one exclusive waiter, we only make sure one of them will get Lock priot to all other shared
 * waiters */
#define RW_FLAG_HAS_EXCLUSIVE_WAITER ((uint64_t)1ULL << 62)

#define RW_VAL_EXCLUSIVE ((uint64_t)1ULL << BIT_COUNT_FOR_SHARED)
#define RW_VAL_SHARED 1
#define RW_VAL_REF_COUNT ((uint64_t)1ULL << (BIT_COUNT_FOR_SHARED + 1))

#define RW_LOCK_MASK ((uint64_t)((1ULL << (BIT_COUNT_FOR_SHARED + 1)) - 1))
#define RW_SHARED_MASK ((uint64_t)((1ULL << BIT_COUNT_FOR_SHARED) - 1))
#define RW_REF_COUNT_MASK (((uint64_t)((1ULL << (BIT_COUNT_FOR_SHARED * 2 + 2)) - 1)) & ~RW_LOCK_MASK)

typedef struct RWLockHandle
{
    RWLock *lock;
    RWLockMode mode;
} RWLockHandle;

#define MAX_SIMUL_RWLOCKS 8196

static int num_held_rwlocks = 0;
static RWLockHandle held_rwlocks[MAX_SIMUL_RWLOCKS];

static int8_t RWLockAttemptLock(RWLock *lock, RWLockMode mode);

void RWLockInitialize(RWLock *lock) { pg_atomic_init_u64(&lock->state, 0); }

/*
 * return value:
 * 1: acquired
 * 0: not acquired
 */
static int8_t RWLockAttemptLock(RWLock *lock, RWLockMode mode)
{
    uint64_t old_state;

    Assert(mode == RW_EXCLUSIVE || mode == RW_SHARED);

    old_state = pg_atomic_read_u64(&lock->state);
    while (true) {
        uint64_t desired_state;
        bool lock_free;

        desired_state = old_state;

        if (mode == RW_EXCLUSIVE) {
            lock_free = (old_state & RW_LOCK_MASK) == 0;
            if (lock_free) {
                desired_state |= RW_VAL_EXCLUSIVE;
                desired_state &= ~RW_FLAG_HAS_EXCLUSIVE_WAITER;
            } else
                desired_state |= RW_FLAG_HAS_EXCLUSIVE_WAITER;
        } else {
            lock_free = (old_state & (RW_VAL_EXCLUSIVE | RW_FLAG_HAS_EXCLUSIVE_WAITER)) == 0;
            if (lock_free)
                desired_state += RW_VAL_SHARED;
        }

        if (pg_atomic_compare_exchange_u64(&lock->state, &old_state, desired_state)) {
            return lock_free ? 1 : 0;
        }
    }
    pg_unreachable();
}

/*
 * Usually not called directly by user unless you want to control the lifetime of a rwlock.
 * Make sure you know what you are doing, otherwise call RwLockDeclareAndAcquire instead. It's caller's
 * duty to make sure each declared lock must be undeclared.
 *
 * Refcount is increased by 1.
 */
void RWLockDeclare(RWLock *lock)
{
    if (num_held_rwlocks >= MAX_SIMUL_RWLOCKS)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "too many RWLocks taken");
    HOLD_INTERRUPTS();

    pg_atomic_fetch_add_u64(&lock->state, RW_VAL_REF_COUNT);

    held_rwlocks[num_held_rwlocks].lock = lock;
    held_rwlocks[num_held_rwlocks++].mode = RW_DECLARE;
}

/*
 * The caller should make sure RwLockDeclare is called before, otherwise the incorrect undeclare may
 * cause others visit illegal memory or the memory cannot be freed forever.
 *
 * Refcount is subtracted by 1.
 */
void RWLockUndeclare(RWLock *lock)
{
    int i;
    for (i = num_held_rwlocks; --i >= 0;)
        if (lock == held_rwlocks[i].lock && held_rwlocks[i].mode == RW_DECLARE)
            break;
    if (i < 0)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "lock is not declared by RWLockDeclare");
    num_held_rwlocks--;
    for (; i < num_held_rwlocks; i++)
        held_rwlocks[i] = held_rwlocks[i + 1];

    pg_atomic_fetch_sub_u64(&lock->state, RW_VAL_REF_COUNT);

    RESUME_INTERRUPTS();
}

bool RWLockCheckDestroyable(RWLock *lock)
{
    uint64_t state = pg_atomic_read_u64(&lock->state);
    return (state & RW_REF_COUNT_MASK) == 0;
}

void RWLockAcquire(RWLock *lock, RWLockMode mode)
{
    if (mode == RW_DECLARE)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "not supported mode");
    if (num_held_rwlocks >= MAX_SIMUL_RWLOCKS)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "too many RWLocks taken");
    HOLD_INTERRUPTS();

    pg_atomic_fetch_add_u64(&lock->state, RW_VAL_REF_COUNT);

    int8_t getLock;
    SpinDelayStatus delayStatus;
    init_local_spin_delay(&delayStatus);
    for (;;) {
        getLock = RWLockAttemptLock(lock, mode);

        if (getLock != 0) {
            break;
        }
        perform_spin_delay(&delayStatus);
    }
    finish_spin_delay(&delayStatus);

    held_rwlocks[num_held_rwlocks].lock = lock;
    held_rwlocks[num_held_rwlocks++].mode = mode;
}

void RWLockRelease(RWLock *lock)
{
    RWLockMode mode;
    int i;
    /*
     * Remove lock from list of locks held, Usually, but not always, it will
     * be the latest-acquired lock; so search array backwards.
     */
    for (i = num_held_rwlocks; --i >= 0;)
        if (lock == held_rwlocks[i].lock)
            break;
    if (i < 0)
        FALCON_ELOG_ERROR(PROGRAM_ERROR, "lock is not held");
    mode = held_rwlocks[i].mode;
    num_held_rwlocks--;
    for (; i < num_held_rwlocks; i++)
        held_rwlocks[i] = held_rwlocks[i + 1];

    if (mode == RW_EXCLUSIVE)
        pg_atomic_fetch_sub_u64(&lock->state, RW_VAL_EXCLUSIVE);
    else
        pg_atomic_fetch_sub_u64(&lock->state, RW_VAL_SHARED);

    pg_atomic_fetch_sub_u64(&lock->state, RW_VAL_REF_COUNT);

    RESUME_INTERRUPTS();
}

/*
 * RWLockReleaseAll - release all currently-held locks
 *
 * Used to clean up after ereport(ERROR). An important difference between this
 * function and retail RWLockRelease calls is that InterruptHoldoffcount is
 * unchanged by this operation if keepInterruptHoldoffcount is true.
 * This is necessary since InterruptHoldoffCount
 * has been set to an appropriate level earlier in error recovery. We could
 * decrement it below zero if we allow it to drop for each released lock!
 */
void RWLockReleaseAll(bool keepInterruptHoldoffCount)
{
    while (num_held_rwlocks > 0) {
        if (keepInterruptHoldoffCount)
            HOLD_INTERRUPTS();

        if (held_rwlocks[num_held_rwlocks - 1].mode == RW_DECLARE)
            RWLockUndeclare(held_rwlocks[num_held_rwlocks - 1].lock);
        else
            RWLockRelease(held_rwlocks[num_held_rwlocks - 1].lock);
    }
}
