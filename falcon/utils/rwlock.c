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
 * RWLockGetHeldCount - Get current number of held RWLocks
 *
 * Returns the current size of the held_rwlocks array. This represents
 * the number of RWLock acquire operations (RWLockAcquire or RWLockDeclare)
 * that have been performed but not yet released.
 *
 * Used by sub-transaction callback to snapshot the lock state at the
 * beginning of a sub-transaction, so we can restore to this state on abort.
 */
int RWLockGetHeldCount(void)
{
    return num_held_rwlocks;
}

/*
 * RWLockReleaseSince - Release RWLocks acquired after savedCount
 *
 * Releases all locks at indices >= savedCount in the held_rwlocks array,
 * effectively restoring the lock state to what it was when savedCount
 * locks were held.
 *
 * The keepInterruptHoldoffCount parameter controls InterruptHoldoffCount
 * handling during the release loop:
 *
 * When keepInterruptHoldoffCount = true (sub-transaction abort):
 *   - Context: errfinish() has just reset InterruptHoldoffCount to 0
 *   - Each RWLockRelease() call internally does RESUME_INTERRUPTS() (count -1)
 *   - To prevent underflow, we HOLD_INTERRUPTS() (+1) before each release
 *   - But we must ensure the final InterruptHoldoffCount equals the value
 *     before this function was called (usually 0 after errfinish)
 *   - Solution: Save InterruptHoldoffCount at entry, restore it at exit
 *
 * When keepInterruptHoldoffCount = false (main transaction commit):
 *   - Normal HOLD/RESUME pairing is expected
 *   - Each RWLockRelease() decrements InterruptHoldoffCount naturally
 *   - No special handling needed
 *
 * Example scenario (sub-transaction abort):
 *   PathParse: RWLockAcquire(lock1)        // InterruptHoldoffCount: 0 → 1
 *   BeginInternalSubTransaction()          // savedCount = 1 recorded
 *     InsertIntoInodeTable:
 *       RWLockAcquire(lock2)               // InterruptHoldoffCount: 1 → 2
 *       Unique key conflict → ERROR
 *   errfinish():
 *     InterruptHoldoffCount = 0            // ← Forcibly reset!
 *     longjmp to PG_CATCH
 *   SubXactCallback(ABORT_SUB):
 *     RWLockReleaseSince(1, true):
 *       savedInterruptHoldoffCount = 0     // Save current value
 *       Loop iteration 1 (release lock2):
 *         HOLD_INTERRUPTS()                // 0 → 1 (temporarily)
 *         RWLockRelease(lock2)
 *           RESUME_INTERRUPTS()            // 1 → 0 (inside RWLockRelease)
 *       InterruptHoldoffCount = 0          // Restore saved value (no change)
 *   Result: lock1 preserved, InterruptHoldoffCount = 0 ✓
 */
void RWLockReleaseSince(int savedCount, bool keepInterruptHoldoffCount)
{
    /* Validate savedCount is within reasonable range */
    if (savedCount < 0 || savedCount > num_held_rwlocks)
        FALCON_ELOG_ERROR_EXTENDED(PROGRAM_ERROR,
                                    "invalid savedCount %d (num_held_rwlocks = %d)",
                                    savedCount,
                                    num_held_rwlocks);

    /*
     * Save InterruptHoldoffCount before the loop.
     *
     * Why save? Because each iteration does HOLD_INTERRUPTS() followed by
     * RWLockRelease() (which does RESUME_INTERRUPTS()), causing the count
     * to fluctuate. When keepInterruptHoldoffCount = true, we want to
     * guarantee the count returns to its pre-loop value, regardless of
     * how many locks are released or whether the loop exits normally.
     *
     * This is especially critical after errfinish() has reset the count
     * to 0 - we must preserve that 0 value.
     */
    int savedInterruptHoldoffCount = InterruptHoldoffCount;

    /*
     * Use PG_FINALLY to guarantee InterruptHoldoffCount restoration even if
     * an error occurs during lock release.
     *
     * Why PG_FINALLY instead of simple assignment after the loop?
     *   1. Defense in depth: If RWLockRelease() throws an ERROR (e.g., "lock
     *      is not held"), the FINALLY block still executes, preventing count
     *      mismatch from propagating.
     *
     *   2. Clear intent: Using PG_FINALLY makes it explicit that we MUST restore
     *      InterruptHoldoffCount regardless of how the function exits (normal
     *      return, ERROR, or even FATAL).
     *
     *   3. PostgreSQL best practice: This follows the same pattern used
     *      throughout PostgreSQL core for critical state restoration (e.g.,
     *      CurrentMemoryContext, interrupt handling, etc.).
     *
     * Note: While errfinish() will also reset InterruptHoldoffCount to 0 on
     * ERROR, using PG_FINALLY ensures our code is self-contained and doesn't
     * rely on implicit error handling behavior. This makes the code more
     * robust against future PostgreSQL changes.
     */
    PG_TRY();
    {
        /* Release locks in reverse order (most recently acquired first) */
        while (num_held_rwlocks > savedCount) {
            /*
             * Always hold interrupts to offset the RESUME_INTERRUPTS() that will
             * be called inside RWLockRelease() or RWLockUndeclare().
             *
             * Without this, each release would decrement InterruptHoldoffCount,
             * potentially causing underflow. This applies regardless of
             * keepInterruptHoldoffCount value:
             *
             * - When keepInterruptHoldoffCount=true (sub-tx abort after errfinish):
             *   InterruptHoldoffCount may be 0, and we need to prevent underflow.
             *
             * - When keepInterruptHoldoffCount=false (main tx commit):
             *   InterruptHoldoffCount starts at a positive value (from CommitTransaction's
             *   HOLD_INTERRUPTS), and releasing multiple locks would cause underflow
             *   without this offsetting HOLD_INTERRUPTS.
             */
            HOLD_INTERRUPTS();

            if (held_rwlocks[num_held_rwlocks - 1].mode == RW_DECLARE)
                RWLockUndeclare(held_rwlocks[num_held_rwlocks - 1].lock);
            else
                RWLockRelease(held_rwlocks[num_held_rwlocks - 1].lock);
        }
    }
    PG_FINALLY();
    {
        /*
         * Restore InterruptHoldoffCount to its pre-loop value.
         *
         * This is the key to keepInterruptHoldoffCount = true semantics:
         * no matter what happened in the TRY block (normal execution, ERROR,
         * or early exit due to assertion failure), this restore operation
         * ensures InterruptHoldoffCount matches what it was when we entered.
         *
         * The FINALLY block guarantees this restoration happens even if:
         *   - RWLockRelease() throws "lock is not held" error
         *   - An assertion fails inside the loop
         *   - A FATAL error occurs (though in that case the process dies anyway)
         *
         * For keepInterruptHoldoffCount = false, we skip this restore and
         * let the natural RESUME_INTERRUPTS() calls take effect.
         */
        if (keepInterruptHoldoffCount)
            InterruptHoldoffCount = savedInterruptHoldoffCount;
    }
    PG_END_TRY();
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
    RWLockReleaseSince(0, keepInterruptHoldoffCount);
}
