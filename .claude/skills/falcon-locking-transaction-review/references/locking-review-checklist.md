# Locking Review Checklist

## Path Lock And Subtransaction Rules

- Parent path locks are not ordinary subtransaction-local resources.
- A subtransaction abort must not release path locks still required by the outer operation.
- Cleanup must distinguish locks acquired by the failing subtransaction from locks owned by parent work.
- Review `transaction/transaction.c` and `dir_path_shmem/dir_path_hash.c` together for path-lock changes.

## Spinlock And RWLock Rules

- Never acquire an RWLock while holding a spinlock.
- Keep spinlock-held sections small and deterministic.
- Do not call code that can allocate, block, log heavily, or raise PostgreSQL `ERROR` while holding a spinlock.

## Ownership Questions

- Who owns the lock: request, backend, transaction, subtransaction, or shared memory structure?
- Can an error jump over the matching release?
- Can cleanup run more than once?
- Can cleanup release a lock acquired by a different nesting level?
- Is the lock state stored in backend-local static memory, shared memory, or heap state?

## Distributed Metadata Questions

- Does the change affect mkdir, rename, unlink, or cross-server coordination?
- Can one participant commit while another aborts?
- Does cleanup interact with 2PC recovery or the cleanup daemon?
- Are remote error paths preserving enough information to resolve in-doubt transactions?

## Suggested Stress Scenarios

- Concurrent mkdir and rename under the same parent.
- Rename across metadata servers while one participant fails.
- ERROR raised after child lock acquisition but before parent cleanup.
- Subtransaction abort inside a larger metadata operation.
- Repeated lock acquisition/release under high request batching.
