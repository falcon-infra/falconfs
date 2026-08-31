# Falcon KV Pre-Commit Regression Gate

Run this gate before committing Falcon KV-cache, BRPC, metadata/data-service,
OffloadingManager, or mixed-cluster benchmark changes.

The primary wrapper is:

```bash
env REGRESSION_PROFILE=smoke \
  KV_THREE_DNS=1 \
  STORE_COUNT=4 \
  FALCON_KV_STORE_BLOCK_SIZE=1048576 \
  FALCON_MIX_KV_BLOCK_BYTES=1048576 \
  FALCON_KV_STORE_MIN_LOGICAL_SLOTS=512 \
  FALCON_POOL_SHMEM_MB=512 \
  KEEP_LOGS=1 \
  PYTHON_BIN=python3 \
  bash scripts/falcon_kv_regression.sh
```

There is no `falcon_regression_test.sh` in this repo. Use
`scripts/falcon_kv_regression.sh`.

## What Must Pass

The smoke wrapper must pass all of these suites:

1. Build, unless `SKIP_BUILD=1` is intentionally used after a matching build.
2. `ctest -L kv-unit` under `build/tests/falcon_kv`.
3. Cluster start through `scripts/falcon_distributed_test.sh start`.
4. Basic FalconFS distributed FS test through `scripts/falcon_distributed_test.sh test`.
5. C++ KV E2E suites: `kv-test`, `kv-meta-stress-test`, `kv-cluster-test`,
   `kv-fault-test`, and `kv-cluster-fault-test`.
6. Clean cluster restart after fault/failover drills, so Python BRPC/offloading
   E2E tests do not inherit deliberately perturbed epoch or live-region state.
7. Python unittest modules directly under `vllm_kv_cache/test`, including
   metadata E2E coverage, metadata channel-cache reuse/eviction, promote-on-read
   admission, adaptive batching policy, and end-to-end offloading tests.
8. Cluster stop.

For release or broad topology changes, run the full profile:

```bash
env REGRESSION_PROFILE=full \
  FALCON_KV_STORE_BLOCK_SIZE=1048576 \
  FALCON_MIX_KV_BLOCK_BYTES=1048576 \
  FALCON_KV_STORE_MIN_LOGICAL_SLOTS=512 \
  FALCON_POOL_SHMEM_MB=512 \
  KEEP_LOGS=1 \
  PYTHON_BIN=python3 \
  bash scripts/falcon_kv_regression.sh
```

The full profile adds failover, topology, mixed-colocation, and promote tests.
It also requires the 3-DN + 4-store topology used by the mixed offloading E2E.

## Focused Checks

Use these commands when iterating, but do not treat them as a replacement for
the wrapper before committing.

Build:

```bash
cmake --build build -j 8
```

KV unit tests:

```bash
env TMPDIR=/tmp TEMP=/tmp TMP=/tmp \
  ctest --test-dir build/tests/falcon_kv --output-on-failure -L kv-unit -j "$(nproc)"
```

Metadata E2E:

```bash
env FALCON_KV_STORE_BLOCK_SIZE=1048576 \
  build/tests/falcon_kv/FalconKVMetadataBrpcE2E \
  --endpoint 127.0.0.1:55530 \
  --block-hash meta-e2e-precommit

env FALCON_KV_STORE_BLOCK_SIZE=1048576 \
  build/tests/falcon_kv/FalconKVMetaEngineConcurrencyE2E \
  --endpoint 127.0.0.1:55530 \
  --threads 4 \
  --batch-size 4 \
  --waves 2
```

Basic distributed FS test:

```bash
env KV_THREE_DNS=1 STORE_COUNT=4 \
  FALCON_KV_STORE_BLOCK_SIZE=1048576 \
  FALCON_KV_STORE_MIN_LOGICAL_SLOTS=512 \
  FALCON_POOL_SHMEM_MB=512 \
  bash -lc 'bash scripts/falcon_distributed_test.sh restart && bash scripts/falcon_distributed_test.sh test'
```

In the Codex desktop runner, keep `restart` and `test` in the same shell or use
`scripts/falcon_kv_regression.sh`; separate foreground commands can leave stale
FUSE mounts after the command process exits.

Mixed offloading E2E with persisted metrics:

```bash
env PYTHONPATH="$PWD/vllm_kv_cache/python" \
  FALCON_KV_CN_SQL=127.0.0.1:55500 \
  FALCON_KV_STORE_BLOCK_SIZE=1048576 \
  FALCON_MIX_KV_BLOCK_BYTES=1048576 \
  FALCON_MIX_E2E_QUICK=0 \
  FALCON_MIX_THROUGHPUT_KEYS=384 \
  FALCON_MIX_PHASE_KEYS=512 \
  FALCON_MIX_PHASE_CHUNK_KEYS=128 \
  FALCON_MIX_THROUGHPUT_JSON="$PWD/logs/falcon_mix_colocated_1mb_precommit.json" \
  FALCON_MIX_METRICS_DIR="$PWD/logs" \
  FALCON_KV_CLIENT_DATA_PARALLELISM_MAX=64 \
  FALCON_KV_CLIENT_META_PARALLELISM_MAX=16 \
  FALCON_KV_CLIENT_BATCH_READ_MAX_BLOCKS=8 \
  FALCON_KV_CLIENT_BATCH_READ_LOCAL_MAX_BLOCKS=8 \
  FALCON_KV_CLIENT_BATCH_READ_REMOTE_MAX_BLOCKS=8 \
  FALCON_KV_CLIENT_BATCH_WRITE_MAX_BLOCKS=1 \
  FALCON_MIX_FACADE_NODE_NAME=v65mix0 \
  python3 -m unittest -q \
  vllm_kv_cache.test.test_offloading_manager_cluster_mixed_e2e.OffloadingManagerClusterMixedE2E.test_batch_throughput_local_vs_remote_facade_split \
  vllm_kv_cache.test.test_offloading_manager_cluster_mixed_e2e.OffloadingManagerClusterMixedE2E.test_z_two_phase_throughput_store_then_load
```

The metrics JSON must include end-to-end throughput, local/remote ratios,
metadata latency, data-path latency, client parallelism, metadata channel cache
stats, promote-on-read counters, and adaptive batching policy so regressions can
be compared with prior runs.

Store-restart SSD validation smoke:

```bash
build/tests/falcon_kv/FalconKVPrimitivesUT \
  --gtest_filter=FalconKvStoreSmoke.ValidateEvictedPathsChecksStoreLocalSSD
```

Metadata channel-cache regression (requires a running DN endpoint; self-skips if
unavailable):

```bash
env PYTHONPATH="$PWD/vllm_kv_cache/python:$PWD/vllm_kv_cache/test" \
  FALCON_KV_TEST_DN_ENDPOINT=127.0.0.1:55530 \
  python3 -m unittest -q test_metadata_channel_cache
```
