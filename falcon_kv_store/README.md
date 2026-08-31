# falcon_kv_store

Standalone KV store daemon used by FalconFS KV cache distributed tests.

## Topology

- Single-store default: `STORE_COUNT=1` starts one daemon on `KV_STORE_BRPC_PORT` (default `18765`).
- Multi-store topology: set `STORE_COUNT>=2`; daemons bind to `KV_STORE_BRPC_PORT + i`.

## Common workflows

- Start cluster with stores:
  - `STORE_COUNT=2 bash scripts/falcon_distributed_test.sh start`
- Validate distributed routing:
  - `STORE_COUNT=2 bash scripts/falcon_distributed_test.sh kv-topology-test`
- Run failover drill:
  - `bash scripts/falcon_distributed_test.sh kv-cluster-failover-test`
- Run promote-on-read drill:
  - `bash scripts/falcon_distributed_test.sh kv-cluster-promote-test`
