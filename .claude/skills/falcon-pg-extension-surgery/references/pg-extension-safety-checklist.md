# PG Extension Safety Checklist

Use this checklist before finalizing changes under `falcon/`.

## PostgreSQL Runtime Model

- Code runs inside PostgreSQL backend processes.
- Each backend is single-threaded, but the system is multi-process.
- Backend-local static state is not shared across clients.
- PostgreSQL `ERROR` unwinds control flow and can skip ordinary cleanup unless guarded.

## Memory And Resource Ownership

- Use `palloc()`/`pfree()` for PostgreSQL memory-context allocations.
- Use `malloc()`/`free()` only for non-PG ownership and keep ownership obvious.
- Do not return pointers into short-lived memory contexts.
- Free heap tuples with `heap_freetuple()` when ownership requires it.
- Clear libpq results with `PQclear()` on every path.
- Finish SPI sessions with `SPI_finish()` even when later work errors.

## Error Handling

- Use `PG_TRY`/`PG_CATCH`/`PG_FINALLY` around code that must release locks, finish SPI, clear results, or restore state.
- In `PG_CATCH`, perform cleanup and rethrow unless the caller intentionally converts the error.
- Avoid swallowing PostgreSQL errors without preserving enough diagnostic context.

## Snapshot And PG17 Rules

- If modifying TOAST-backed relations or behavior that touches relation storage in PG17+, verify whether an active snapshot is required.
- Use `PushActiveSnapshot(GetTransactionSnapshot())` immediately before the relation operation that requires an active snapshot, and `PopActiveSnapshot()` in guaranteed cleanup.
- Keep the snapshot scope as small as possible. Do not leave an active snapshot across unrelated SPI, remote RPC, lock acquisition, or long-running work.
- Track whether a snapshot was pushed so error cleanup does not call `PopActiveSnapshot()` without ownership.
- Do not hold snapshots longer than needed.

## Transactions And Subtransactions

- Check `MAX_SUB_XACT_DEPTH` before adding nested subtransaction patterns.
- Never release parent path locks on subtransaction abort.
- Path-lock cleanup must distinguish locks acquired by the failing subtransaction from locks still owned by the outer metadata operation.
- If a child operation fails, cleanup only the child-owned resources; parent path protection must remain until the parent operation finishes or aborts.
- Treat transaction cleanup as correctness logic, not as generic best-effort cleanup.

## Build-System Boundary

- `falcon/` uses PGXS Makefiles.
- `falcon/MakefilePlugin.brpc` and `falcon/MakefilePlugin.hcom` build communication plugins.
- CMake changes do not fix metadata extension build failures unless the failure is in generated/shared dependencies.
