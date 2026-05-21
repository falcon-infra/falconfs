# Metadata Operation Touchpoints

Use this file as a map, not a requirement to edit every file.

## Client And FUSE

- `falcon_client/fuse_main.cpp`: FUSE callbacks and POSIX-visible return conventions.
- `falcon_client/src/falcon_meta.cpp`: public metadata API and retry behavior.
- `falcon_client/src/inner_falcon_meta.cpp`: internal metadata/data operation bridge.
- `falcon_client/src/router.cpp`: metadata server selection and shard routing.
- `falcon_client/src/connection.cpp`: BRPC channel, FlatBuffers request/response handling.
- `falcon_client/src/include/*.h`: public or internal API shape.

## Wire Format And Generated Code

- `remote_connection_def/fbs/`: metadata request/response schemas.
- `remote_connection_def/proto/`: RPC service definitions when service shape changes.
- `falcon/connection_pool/fbs/`: generated FlatBuffers headers used by metadata service.
- `build/`: generated Protobuf output.

## Metadata Engine

- `falcon/metadb/meta_handle.c`: core metadata operation handlers.
- `falcon/metadb/meta_serialize_interface_helper.cpp`: operation serialization and response helpers.
- `falcon/metadb/*_table.c` and `falcon/include/metadb/*_table.h`: table-level operations.
- `falcon/connection_pool/`: request batching, worker tasks, and dispatch.
- `falcon/distributed_backend/`: 2PC and cross-server operation coordination.
- `falcon/transaction/`: transaction wrappers and cleanup behavior.
- `falcon/dir_path_shmem/`: replicated directory namespace and path locking.

## Error Mapping

- Check Falcon internal error code definitions under `falcon/include/` and `falcon/utils/`.
- Check client-side `ErrorCodeToErrno()` before changing user-visible behavior.
- Keep server errors precise enough for client retry and recovery decisions.

## Test Locations

- `tests/falcon/`: core metadata-adjacent unit tests.
- `tests/falcon_plugin/`: plugin and metadata service framework tests.
- `tests/falcon_meta_service_plugin/`: metadata service end-to-end plugin tests.
- `tests/regress/`: Docker-based multi-node regression.
- `tests/private-directory-test/`: workload and performance scenarios.
