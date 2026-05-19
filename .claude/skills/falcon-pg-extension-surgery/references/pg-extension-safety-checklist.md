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
- Follow `PG17_COMPATIBILITY_FIX.md` for `PushActiveSnapshot()` / `PopActiveSnapshot()` patterns.
- Do not hold snapshots longer than needed.

## Transactions And Subtransactions

- Check `MAX_SUB_XACT_DEPTH` before adding nested subtransaction patterns.
- Never release parent path locks on subtransaction abort.
- Ensure transaction cleanup paths match the lock ownership model in `RWLOCK_SUBTRANSACTION_CORRECTNESS_ANALYSIS.md`.

## Build-System Boundary

- `falcon/` uses PGXS Makefiles.
- `falcon/MakefilePlugin.brpc` and `falcon/MakefilePlugin.hcom` build communication plugins.
- CMake changes do not fix metadata extension build failures unless the failure is in generated/shared dependencies.
