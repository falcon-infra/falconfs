---
name: falcon-pg-extension-surgery
description: Use this skill when modifying FalconFS PostgreSQL extension C code under falcon/, especially metadb/meta_handle.c handlers, PostgreSQL SPI/control functions, PGresult/PQclear cleanup, heap_freetuple cleanup, palloc memory contexts, transaction wrappers, snapshots/TOAST/PG17 behavior, background workers, or PGXS extension build issues.
---

# Falcon PG Extension Surgery

Use this workflow for changes in `falcon/`. This code runs inside PostgreSQL backend processes, so ordinary C service assumptions are often wrong.

## First Reads

1. Read `falcon/AGENTS.md`.
2. If the change touches PG17 snapshot, TOAST, or table rewrite behavior, read `PG17_COMPATIBILITY_FIX.md`.
3. If the change touches transactions, subtransactions, path locks, or cleanup callbacks, use `falcon-locking-transaction-review` too.
4. For detailed PG safety checks, read `references/pg-extension-safety-checklist.md`.

## Classify The Change

Identify the narrowest affected area before editing:

- Metadata CRUD: `falcon/metadb/`, especially `meta_handle.c`.
- SPI/control plane: `falcon/control/`.
- Transactions and cleanup: `falcon/transaction/`.
- Distributed metadata behavior: `falcon/distributed_backend/`.
- Background workers and extension lifecycle: `falcon/falcon_init.c`, `falcon/plugin/`.
- Communication adapter glue: `falcon/brpc_comm_adapter/`, `falcon/hcom_comm_adapter/`, `falcon/plugin_comm_adapter/`.

## Required Checks

- Match PG memory ownership: do not mix `palloc()`/`free()` or `malloc()`/`pfree()`.
- Ensure `SPI_finish()`, `heap_freetuple()`, `PQclear()`, and allocated resources are handled on normal and error paths.
- Use `PG_TRY`/`PG_CATCH`/`PG_FINALLY` when a PG `ERROR` can bypass cleanup.
- Do not let C++ exceptions cross PostgreSQL C boundaries.
- Keep subtransaction nesting below `MAX_SUB_XACT_DEPTH`.
- Do not change lock lifetime or path-lock release rules without running the locking skill.
- Remember `falcon/` is built by PGXS, not CMake.

## Validation

Prefer the smallest meaningful validation:

```bash
./build.sh build falcon
./build.sh test
```

If only core unit tests are relevant:

```bash
./build/tests/falcon/FalconQueueUT
./build/tests/falcon_plugin/PluginFrameworkUT
./build/tests/falcon_plugin/PluginLoaderUT
```

If validation cannot run because the container or dependencies are missing, report that explicitly and explain what remains unverified.
