---
name: falcon-metadata-op-addition
description: Use this skill when adding, removing, or changing a FalconFS metadata operation or wire API across FlatBuffers schemas, falcon_client APIs, FUSE/POSIX mapping, router behavior, connection_pool dispatch, metadb operation handlers, distributed backend behavior, generated serialization code, or metadata service tests.
---

# Falcon Metadata Operation Addition

Use this workflow when a metadata operation changes across layers. FalconFS metadata behavior often spans schema, client, routing, batching, PostgreSQL handlers, error mapping, and tests.

## First Reads

1. Read `AGENTS.md`, `falcon/AGENTS.md`, and `falcon_client/AGENTS.md`.
2. If the operation changes locking or transaction behavior, use `falcon-locking-transaction-review`.
3. If the operation changes PostgreSQL handler code, use `falcon-pg-extension-surgery`.
4. For the layer-by-layer map, read `references/metadata-op-touchpoints.md`.

## Classify The Operation

- File operation: create, open, stat, read metadata, write metadata, truncate, unlink.
- Directory operation: mkdir, rmdir, readdir, rename, path lookup.
- Distributed operation: cross-server mkdir/rename or shard-affecting behavior.
- Control operation: cluster, shard, server, or maintenance metadata.
- Client-only mapping: POSIX/FUSE behavior without wire/schema change.

## Required Trace

Before editing, trace the operation through the relevant layers:

1. Public client API in `falcon_client/src/falcon_meta.cpp`.
2. Internal client/store bridge in `falcon_client/src/inner_falcon_meta.cpp` if data I/O is involved.
3. Routing in `falcon_client/src/router.cpp`.
4. Metadata connection and serialization in `falcon_client/src/connection.cpp`.
5. FlatBuffers schema under `remote_connection_def/fbs/`.
6. Server-side serialization helper in `falcon/metadb/meta_serialize_interface_helper.cpp`.
7. Request batching/dispatch in `falcon/connection_pool/`.
8. PostgreSQL metadata handler in `falcon/metadb/meta_handle.c`.
9. Distributed backend in `falcon/distributed_backend/` if operation spans metadata servers.
10. FUSE mapping in `falcon_client/fuse_main.cpp` if user-visible POSIX behavior changes.

## Required Checks

- Keep request and response schema versions compatible with generated code expectations.
- Map Falcon error codes to POSIX `errno` when the operation reaches FUSE/client APIs.
- Preserve retry behavior on `SERVER_FAULT` unless changing it intentionally.
- Confirm directory operations update replicated directory namespace consistently.
- Confirm file metadata operations use the correct shard/table path.
- Add or update focused tests; if not feasible, name the exact missing test coverage.

## Validation

Use the smallest set that covers the touched layers:

```bash
./build.sh build falcon
./build.sh test
```

For focused checks:

```bash
./build/tests/falcon/FalconQueueUT
./build/tests/falcon_plugin/PluginFrameworkUT
./build/tests/falcon_store/FalconStoreUT
```

Regenerate FlatBuffers or Protobuf outputs if the schema changes, following the existing build flow.
