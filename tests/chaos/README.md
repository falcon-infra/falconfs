# FalconFS Chaos Tests

This directory contains the FalconFS chaos and long-run test harness. It drives the Docker Compose test topologies under `tests/regress/docker-compose-*.yaml`, injects service faults, and verifies recovery through health, metadata service, and reliability checks.

## Layout

- `chaos_run.sh`: runs one chaos scenario against one compose topology.
- `chaos_suite.sh`: runs selected stages across single, dual, and triple topologies.
- `run_longrun.sh`: config-driven entry for long-running acceptance runs.
- `longrun_config.sh`: default long-run configuration.
- `chaos_case_matrix_default.txt`: deterministic suite matrix.
- `plans/`: deterministic action order per topology.
- `lib/`: implementation modules sourced by `chaos_lib.sh`.
- `chaos_lib.sh`: compatibility source wrapper for `lib/*.sh`.

The compose files intentionally remain under `tests/regress/` because they are shared with the existing regress environment.

## Actions

- `S01`: restart CN leader.
- `S02`: restart one DN leader.
- `S03`: restart random DN, preferring a follower.
- `S04`: restart the store container.
- `S05`: kill the `falcon_client` process in the store container.
- `S06`: stop CN leader, hold, then start it again.
- `S07`: stop random DN, hold, then start it again.
- `S08`: stop CN leader, wait for a new leader, stop it, hold, then recover both.

## Recovery Checks

Each injected action is expected to recover through these checks:

- ZooKeeper has `/falcon/ready`.
- CN, DN, and Store liveness checks pass.
- Store FUSE mount is ready.
- Metadata service returns to `RW`.
- Multi-replica metadata actions restore the expected replica reliability.

Dual topology can observe degraded states such as `RO`, `RECOVERY`, or `WRITE_BLOCKED`. Triple topology does not require degraded-state observation when one replica is stopped; it may remain `RW`.

## Common Commands

Run one dual chaos scenario:

```bash
export FALCON_FULL_IMAGE=localhost:5000/falconfs-full-ubuntu24.04:v0.1.0
bash tests/chaos/chaos_run.sh \
  --data-path tests/chaos/verify_data_chaos \
  --compose-file tests/regress/docker-compose-dual.yaml \
  --topology dual \
  --duration-min 120 \
  --interval-sec 300 \
  --actions S01,S02,S03,S04,S05,S06,S07 \
  --fresh-start \
  --teardown
```

Run the deterministic all-topology suite:

```bash
export FALCON_FULL_IMAGE=localhost:5000/falconfs-full-ubuntu24.04:v0.1.0
bash tests/chaos/chaos_suite.sh \
  --data-path tests/chaos/verify_all_topologies \
  --case-matrix-file tests/chaos/chaos_case_matrix_default.txt \
  --strict-coverage
```

Run a quicker all-topology smoke with short hold actions:

```bash
export FALCON_FULL_IMAGE=localhost:5000/falconfs-full-ubuntu24.04:v0.1.0
bash tests/chaos/chaos_suite.sh \
  --data-path tests/chaos/verify_all_topologies_once \
  --case-matrix-file tests/chaos/chaos_case_matrix_default.txt \
  --strict-coverage \
  --interval-sec 20 \
  --post-action-grace-sec 10 \
  --stable-window-sec 10 \
  --action-hold-secs S06=60,S07=60,S08=60 \
  --core-once-enable \
  --core-limit-kb 1048576 \
  --fail-fast
```

Run only one suite stage:

```bash
bash tests/chaos/chaos_suite.sh \
  --data-path tests/chaos/verify_triple_only \
  --stages triple \
  --case-matrix-file tests/chaos/chaos_case_matrix_default.txt \
  --strict-coverage
```

Preview the config-driven long-run command:

```bash
bash tests/chaos/run_longrun.sh --dry-run
```

Execute the config-driven long run:

```bash
bash tests/chaos/run_longrun.sh
```

Override long-run stages or quick hold settings from the environment:

```bash
LONGRUN_STAGES=triple \
LONGRUN_ACTION_HOLD_SECS=S06=60,S07=60,S08=60 \
bash tests/chaos/run_longrun.sh --dry-run
```

## Supplement Observation

`S06`, `S07`, and `S08` use a hold window. By default the hold is `wait_replica_time + 30s`, normally `630s`, so the run has enough time to observe `/falcon/need_supplement`.

For quick smoke tests, use `--action-hold-secs S06=60,S07=60,S08=60`. This validates recovery behavior but should not be treated as a dedicated supplement-path proof.

For supplement-specific validation, do not pass `--action-hold-secs`; let the default hold apply.

## Reports And Artifacts

Each `chaos_run.sh` data path contains:

- `chaos_report.jsonl`: one event per action and one final summary event.
- `chaos_coverage.json`: action execution coverage.
- `chaos_run.log`: phase and recovery logs.
- `chaos_diag/`: failure snapshots when startup or actions fail.
- `core_once/`: captured first core when `--core-once-enable` is used.

`chaos_suite.sh` also writes `suite_summary.json` at the suite data path.

Generated `verify_*` and `accept_longrun_*` directories are run artifacts and should not be committed.

## Implementation Modules

- `lib/runtime.sh`: common runtime helpers, topology resolution, option validation.
- `lib/actions.sh`: action metadata, action plan parsing, coverage, and `S01~S08` implementations.
- `lib/recovery_report.sh`: recovery orchestration, action loop, and report writers.
- `lib/diagnostics.sh`: alerting, diagnostics snapshots, logging, and core-once handling.
- `lib/infra_health.sh`: Docker, ZooKeeper, container, health, service, and reliability helpers.
- `lib/data_dirs.sh`: data directory setup, cleanup, mountpoint reset, and container restart helper.

Keep `chaos_lib.sh` as the only file sourced by `chaos_run.sh`. New helper code should go into the module that owns the relevant responsibility.

## Notes

- `--core-once-enable` allows core dumps and captures only the first core, then disables further core dumps.
- Without `--core-once-enable`, the harness expects service core ulimit to be `0` to avoid disk blowup.
- Stale FUSE mountpoints can remain in generated data paths after interrupted runs. Only unmount the specific generated `falcon-data/store-1/data` path before deleting artifacts.
