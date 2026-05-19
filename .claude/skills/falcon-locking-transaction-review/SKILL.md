---
name: falcon-locking-transaction-review
description: Use this skill when changing or reviewing FalconFS locking correctness, lock release order, parent path locks, RWLock usage, dir_path_shmem, transaction cleanup, subtransaction abort callbacks, rename/mkdir concurrency, distributed metadata deadlock risks, InterruptHoldoffCount logic, spinlock interactions, or path protection invariants.
---

# Falcon Locking And Transaction Review

Use this as a correctness gate for any lock or transaction change. Prefer finding behavioral regressions over style comments.

## First Reads

1. Read `RWLOCK_SUBTRANSACTION_CORRECTNESS_ANALYSIS.md` before touching path locks or subtransaction cleanup.
2. Read `INTERRUPT_HOLDOFF_COUNT_ANALYSIS.md` when changing interrupt holdoff, PG critical sections, or cleanup interactions.
3. Read `falcon/AGENTS.md` for metadata-engine constraints.
4. For the focused checklist, read `references/locking-review-checklist.md`.

## Core Invariants

- Parent path locks must survive subtransaction abort when the parent operation still needs path protection.
- Do not hold a spinlock while acquiring an RWLock.
- Do not assume backend-local static lock state is shared across PostgreSQL processes.
- Preserve lock acquisition order and release symmetry.
- Do not introduce cleanup that can release another operation's locks.
- Keep transaction/subtransaction nesting within FalconFS limits.

## Review Procedure

1. Identify every lock acquired, released, or transferred by the change.
2. Trace normal path, error path, transaction abort, and subtransaction abort separately.
3. Check whether lock ownership is backend-local, shared-memory, per-request, or per-transaction.
4. Check distributed operation behavior: mkdir, rename, and cross-server metadata paths need 2PC consistency.
5. Look for hidden ordering changes between spinlocks, RWLocks, mutexes, and transaction cleanup.

## Output Format

Lead with findings. For each finding, include:

- File and line when available.
- The broken invariant.
- Why the bug can happen.
- A concrete fix or verification request.

If no issue is found, say that clearly and list any concurrency scenarios that remain untested.
