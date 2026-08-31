#!/usr/bin/env python3
"""End-to-end OffloadingManager tests for 3-DN + multi-store mixed topology (v6.6.x).

Requires ``scripts/falcon_distributed_test.sh start`` with ``KV_THREE_DNS=1`` and
``STORE_COUNT=4`` (full regression profile). Tests are skipped when DN3 or the
fourth store BRPC port is unreachable so smoke / two-DN clusters stay green.

**v6.6.x alignment (design §29.5):** ``FalconFSOffloadingManager`` uses
``_data_stage_pool`` with default ``FALCON_KV_CLIENT_DATA_PARALLELISM_MAX=min(64,hw*2)``
and ``FALCON_KV_CLIENT_META_PARALLELISM_MAX=num_dns`` — **no** legacy
``FALCON_KV_OM_PARALLEL_STORE_IO`` switch. Data plane work is **one unary RPC per
logical block** (``Store.write`` / ``Store.read``) fanned out across the pool so
the KV store engine sees concurrent ``WriteBlock`` / ``ReadBlock`` traffic.

Throughput, facade IPC (local vs remote data-path), ``store_locality``, and
OM latency breakdown are appended to ``throughput_events``. Each run prints::

    FALCON_MIX_RUN_SUMMARY\t{...json...}

immediately followed by a **plain-text block** (``--- FALCON_MIX_DETAIL ---`` …)
with throughput, **aggregated phase latency** (``perf_breakdown_*`` when present),
**last-chunk OM splits** (``om_perf_breakdown`` / ``om_perf``), and SHM vs BRPC facade
counts—so you do not have to parse JSON to see breakdown latency. Disable the block
with ``FALCON_MIX_NO_HUMAN_DETAIL=1``.

Optionally mirror the same events to a file::

    FALCON_MIX_THROUGHPUT_JSON=/path/out.json

Store/load tests call ``set_om_perf_enabled(True)`` for the measured section so each
summary includes ``om_perf_breakdown`` even when ``FALCON_KV_OM_PERF`` is unset; the
manager restores that flag afterward to match the environment default.

**Fast by default:** unless ``FALCON_MIX_E2E_QUICK=0``, the module uses smaller
default key counts (two-phase, batch touch, throughput split) so the suite finishes
quickly while still emitting **full** JSON + human detail. Raise sizes with
``FALCON_MIX_PHASE_KEYS``, ``FALCON_MIX_THROUGHPUT_KEYS``, ``FALCON_MIX_BATCH_TOUCH_KEYS``,
or turn off quick mode for stress runs.

    FALCON_MIX_KV_BLOCK_BYTES       # default vLLM formula (must match catalog block_size)
    FALCON_MIX_THROUGHPUT_KEYS      # default scales with CPU count (stress many concurrent blocks)
    FALCON_MIX_BATCH_TOUCH_KEYS     # default scales with CPU count
    FALCON_MIX_REQUIRE_STORE_MB_S   # optional: fail if ``complete_store`` MB/s below this (decimal MB/s)
    FALCON_MIX_FACADE_NODE_NAME     # default ``v65mix0``: ``NODE_NAME`` + ``client_hostname`` for local SHM on one store
    FALCON_MIX_FACADE_ALL_REMOTE=1  # unset ``NODE_NAME`` here (all stores use remote BRPC facade)

    **Throughput:** §29.8 / §19.2 describe **scaling vs** ``client_data_parallelism_max`` and
    hardware (memcpy / NIC). Python→BRPC on a single host rarely hits 10+ GB/s; for a
    **strict** high-bandwidth gate (e.g. colocated SHM + many keys), set
    ``FALCON_MIX_REQUIRE_STORE_MB_S`` (e.g. ``10000`` for ~10 GB/s in decimal MB/s) in
    addition to raising ``FALCON_MIX_THROUGHPUT_KEYS`` and ``FALCON_KV_CLIENT_DATA_PARALLELISM_MAX``.

**Two-phase throughput (default on):** ``test_z_two_phase_throughput_store_then_load`` runs
last (method name sorts after other tests) whenever mixed topology is up, so shorter
tests validate the cluster before the long store/load phases. It exercises **store phase**
(chunked ``prepare_store`` + ``complete_store``) then **load phase** (chunked
``prepare_load`` + verify + ``complete_load``). Disable only when needed:
``FALCON_MIX_TWO_PHASE_THROUGHPUT=0`` (also ``false`` / ``no`` / ``off``).

Each ``FALCON_MIX_RUN_SUMMARY`` for that test includes **throughput_end_to_end** (phase
wall MB/s), **throughput_breakdown_local_remote_mb_s** (Python/OM data-path timing split by
direct local-SHM vs remote-BRPC operation measurements), **cxx_facade_latency_throughput**
(clean C++ facade timing around ``LocalKVStoreShmFacade`` / ``RemoteKVStoreFacade``),
**latency_breakdown_meta_local_remote_s** (``totals_s`` plus **per_batch_meta_ms** for
chunk-wave metadata and **per_block_meta_ms** / **per_path_data_ms** for per-logical-block
costs), **perf_breakdown_store_phase** / **perf_breakdown_load_phase** (aggregated OM
latency and the same **per_batch** / **per_block** breakdown fields), **om_perf_breakdown**
(last chunk snapshot), **store_locality**,
**facade_ipc_stats** (cumulative counters at end of load), **facade_ipc_after_store_phase**
(write counts after store phase), and **facade_ipc_data_path** (SHM vs BRPC labels).
**store_performance_analysis** / **load_performance_analysis** spell out batch vs per-block
latency and payload MB/s equivalents (including true **batch_lookup per_batch_ms** vs
**per_block_load_data** instrumented read averages).

``FALCON_MIX_PHASE_KEYS`` defaults to **512** when unset and quick mode is on (**3072**
when ``FALCON_MIX_E2E_QUICK=0``). If the catalog reports less total DRAM, the test
**caps** the key count to ``sum(dram_pool_bytes) // block_size`` (minimum **16** keys)
and records ``phase_keys_requested`` vs ``phase_keys_effective``. Use
``FALCON_MIX_PHASE_CHUNK_KEYS`` (default ``256``) to bound Python heap per wave.

DN pool manager ``falcon_connection_pool.shmem_size`` is set from ``FALCON_POOL_SHMEM_MB``
(defaults to **512** MB when ``KV_THREE_DNS=1`` and the pool knob was left at **256**).

The module sets ``FALCON_KV_STORE_BRPC_ENDPOINT`` to ``127.0.0.1:${KV_STORE_BRPC_PORT}``
when unset so store I/O targets ``falcon_kv_store`` BRPC, not the DN pooler.
For the single-node harness, ``psql`` can prime ``falcon_dn_node.healthy`` before
each test (opt out with ``FALCON_MIX_SKIP_DN_PRIME=1``).

**Mixed colocation (client vs store):** ``scripts/falcon_distributed_test.sh`` starts
four ``falcon_kv_store`` processes with ``NODE_NAME=v65mix{0..3}``, so catalog
``host_node_name`` is ``v65mix0`` … ``v65mix3``. The C++ facade registry compares
that to ``NODE_NAME`` (else ``gethostname()``) — **not** to ``client_hostname``.
By default this module sets ``NODE_NAME`` (and ``client_hostname``) to
``v65mix0`` before constructing ``FalconFSOffloadingManager``, so **store 1** can
use **local SHM** when ``shm_open`` succeeds and **stores 2–4** stay **remote BRPC**
(mixed deployment). The store daemon maps DRAM with ``shm_open`` + ``mmap(MAP_SHARED)``
using ``FALCON_KV_STORE_SHM_NAME`` (same string registered in CN); **restart all
``falcon_kv_store`` processes** after upgrading so segments exist for the client.
Override with ``FALCON_MIX_FACADE_NODE_NAME`` (e.g. ``v65mix2``),
or force all-remote behavior with ``FALCON_MIX_FACADE_ALL_REMOTE=1``.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import time
import unittest
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

DN1_POOLER = os.environ.get("FALCON_KV_DN1_POOLER", "127.0.0.1:55530")
DN2_POOLER = os.environ.get("FALCON_KV_DN2_POOLER", "127.0.0.1:55550")
DN3_POOLER = os.environ.get("FALCON_KV_DN3_POOLER", "127.0.0.1:55570")
CN_SQL = os.environ.get("FALCON_KV_CN_SQL", "127.0.0.1:55500")
STORE_BASE = int(os.environ.get("KV_STORE_BRPC_PORT", "18765"))
# BrpcCluster defaults store I/O to DN endpoint if unset; mixed harness writes
# ``/tmp/falcon_kv_store_env.sh`` for daemons only — tests must point at BRPC.
os.environ.setdefault("FALCON_KV_STORE_BRPC_ENDPOINT", f"127.0.0.1:{STORE_BASE}")

# vLLM: bytes per logical GPU KV block ≈ 2 * num_layers * num_kv_heads * head_dim
# * gpu_block_tokens * sizeof(dtype).  Representative fp16 Llama-class slice:
_VLLM_FORMULA_KV_BYTES = 2 * 32 * 8 * 128 * 16 * 2


def _psql_bin() -> str:
    local = Path("/usr/local/pgsql/bin/psql")
    if local.is_file():
        return str(local)
    return shutil.which("psql") or ""


def _mixed_workload_block_bytes() -> int:
    # Default matches ``falcon_kv_store`` / harness vLLM-style block_size (see
    # ``FALCON_KV_STORE_BLOCK_SIZE``). Override with ``FALCON_MIX_KV_BLOCK_BYTES``
    # for smaller catalog rows when running legacy 64 KiB stores.
    raw = int(os.environ.get("FALCON_MIX_KV_BLOCK_BYTES", str(_VLLM_FORMULA_KV_BYTES)))
    return max(4096, min(raw, _VLLM_FORMULA_KV_BYTES))


def _mixed_throughput_num_keys() -> int:
    # §29.8: enough concurrent unary data tasks to stress the engine; cap total payload for CI RAM.
    cpus = os.cpu_count() or 8
    default_nk = max(96, min(384, cpus * 24))
    if _e2e_quick_enabled():
        default_nk = max(32, min(96, default_nk // 3))
    return max(8, int(os.environ.get("FALCON_MIX_THROUGHPUT_KEYS", str(default_nk))))


def _mixed_batch_touch_key_count() -> int:
    cpus = os.cpu_count() or 8
    default_b = max(48, min(256, cpus * 16))
    if _e2e_quick_enabled():
        default_b = max(32, min(64, default_b // 2))
    return max(4, int(os.environ.get("FALCON_MIX_BATCH_TOUCH_KEYS", str(default_b))))


def _require_store_mb_s() -> float:
    """Optional floor on ``complete_store`` payload MB/s (decimal MB = 1e6 bytes)."""
    raw = os.environ.get("FALCON_MIX_REQUIRE_STORE_MB_S", "").strip()
    if not raw:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0


def _mixed_payload(byte_count: int, *, variant: int = 0) -> bytes:
    pat = bytes(((i + variant) & 0xFF) for i in range(256))
    rep = (byte_count + 255) // 256
    return (pat * rep)[:byte_count]


def _can_reach(endpoint: str, timeout_s: float = 1.0) -> bool:
    host, _, port = endpoint.partition(":")
    try:
        with socket.create_connection((host, int(port)), timeout=timeout_s):
            return True
    except OSError:
        return False


def _three_dns_reachable() -> bool:
    return all(
        _can_reach(ep)
        for ep in (CN_SQL, DN1_POOLER, DN2_POOLER, DN3_POOLER)
    )


def _four_kv_stores_listening() -> bool:
    for i in range(4):
        if not _can_reach(f"127.0.0.1:{STORE_BASE + i}"):
            return False
    return True


def mixed_topology_available() -> bool:
    """True when CN + 3 DN BRPC poolers and four ``falcon_kv_store`` ports answer.

    Evaluated at call time (not import time) so harness ``start`` completes before checks.
    """
    return _three_dns_reachable() and _four_kv_stores_listening()


def _om_perf_enabled() -> bool:
    return os.environ.get("FALCON_KV_OM_PERF", "").strip().lower() in ("1", "true", "yes", "on")


def _e2e_quick_enabled() -> bool:
    """Smaller default workloads for fast local runs; disable with ``FALCON_MIX_E2E_QUICK=0``."""
    raw = os.environ.get("FALCON_MIX_E2E_QUICK", "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _facade_local_node_name_for_mixed_e2e() -> str:
    """Return ``host_node_name`` to export as ``NODE_NAME`` for local SHM vs remote BRPC mix.

    Harness ``KV_THREE_DNS=1`` + ``STORE_COUNT=4`` sets ``NODE_NAME=v65mix{si}`` per store row
    (``host_node_name`` in ``falcon_store_node``). The BRPC extension resolves the registry
    with ``ResolveLocalHostNodeName()`` (``NODE_NAME`` or ``gethostname()``), which must match
    exactly one store's ``host_node_name`` for that store to get ``LocalKVStoreShmFacade``;
    other stores use ``RemoteKVStoreFacade``.

    - Default: ``v65mix0`` (colocated with **store_node_id** 1).
    - ``FALCON_MIX_FACADE_NODE_NAME``: choose another ``v65mix*`` identity.
    - ``FALCON_MIX_FACADE_ALL_REMOTE=1``: do not set ``NODE_NAME`` here (legacy all-remote).
    """
    if os.environ.get("FALCON_MIX_FACADE_ALL_REMOTE", "").strip().lower() in ("1", "true", "yes", "on"):
        return ""
    return os.environ.get("FALCON_MIX_FACADE_NODE_NAME", "v65mix0").strip()


def _two_phase_throughput_disabled() -> bool:
    """Opt-out for CI smoke or very small clusters (see module docstring)."""
    raw = os.environ.get("FALCON_MIX_TWO_PHASE_THROUGHPUT", "").strip().lower()
    if not raw:
        return False
    return raw in ("0", "false", "no", "off")


def _phase_keys_requested() -> int:
    """Requested logical block count; env ``FALCON_MIX_PHASE_KEYS`` or quick/stress default."""
    raw = os.environ.get("FALCON_MIX_PHASE_KEYS", "").strip()
    if raw:
        return max(1, int(raw))
    return 512 if _e2e_quick_enabled() else 3072


def _phase_chunk_keys() -> int:
    return max(16, int(os.environ.get("FALCON_MIX_PHASE_CHUNK_KEYS", "256")))


def _phase_pregenerate_payload_max_bytes() -> int:
    """Max bytes to pre-generate outside throughput phase timers.

    The benchmark measures offloading, not Python payload synthesis.  Default to
    a bounded 1 GiB cap so 1 MiB/512-key and 512 KiB/1024-key stress runs keep
    payload generation and expected-payload construction outside the measured
    store/load phases.  Set ``FALCON_MIX_PREGENERATE_PAYLOAD_MAX_BYTES=0`` to
    force the old low-memory streaming mode.
    """
    raw = os.environ.get(
        "FALCON_MIX_PREGENERATE_PAYLOAD_MAX_BYTES", str(1024 * 1024 * 1024)
    ).strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 1024 * 1024 * 1024


def _total_healthy_store_dram_bytes(conninfo: str) -> int:
    """Sum ``dram_pool_bytes`` across healthy ``falcon_store_node`` rows (CN catalog)."""
    psql_bin = _psql_bin()
    if not psql_bin:
        return 0
    sql = (
        "SELECT coalesce(sum(dram_pool_bytes), 0)::bigint "
        "FROM pg_catalog.falcon_store_node WHERE coalesce(healthy, true);"
    )
    r = subprocess.run(
        [psql_bin, conninfo, "-v", "ON_ERROR_STOP=1", "-qtA", "-c", sql],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    if r.returncode != 0:
        return 0
    try:
        return int((r.stdout or "0").strip() or "0")
    except ValueError:
        return 0


def _min_healthy_store_dram_bytes(conninfo: str) -> int:
    psql_bin = _psql_bin()
    if not psql_bin:
        return 0
    sql = (
        "SELECT coalesce(min(dram_pool_bytes), 0)::bigint "
        "FROM pg_catalog.falcon_store_node WHERE coalesce(healthy, true);"
    )
    r = subprocess.run(
        [psql_bin, conninfo, "-v", "ON_ERROR_STOP=1", "-qtA", "-c", sql],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    if r.returncode != 0:
        return 0
    try:
        return int((r.stdout or "0").strip() or "0")
    except ValueError:
        return 0


def _env_store_dram_capacity_fallback(block_size: int) -> Dict[str, int]:
    """Capacity fallback for environments where ``psql`` is absent from PATH.

    The regression harness starts Stores from env knobs before Python tests run.
    If the Python process cannot invoke ``psql`` to inspect ``falcon_store_node``,
    use the same bounded local setup knobs instead of skipping the two-phase E2E.
    """
    try:
        store_count = max(1, int(os.environ.get("STORE_COUNT", "1") or "1"))
    except ValueError:
        store_count = 1
    try:
        slots = max(1, int(os.environ.get("FALCON_KV_STORE_MIN_LOGICAL_SLOTS", "512") or "512"))
    except ValueError:
        slots = 512
    bs = max(1, int(block_size or 1))
    per_store = slots * bs
    return {"sum": store_count * per_store, "min": per_store}


def _perf_store_acc_new() -> Dict[str, Any]:
    return {
        "prepare_store_wall_s": 0.0,
        "meta_allocate_by_dn_s": {},
        "complete_store_wall_s": 0.0,
        "complete_store_data_write_s": 0.0,
        "complete_store_data_write_bytes": 0,
        "complete_store_data_write_rpcs": 0,
        "complete_store_data_write_local_s": 0.0,
        "complete_store_data_write_local_bytes": 0,
        "complete_store_n_writes_local": 0,
        "complete_store_data_write_local_rpcs": 0,
        "complete_store_data_write_remote_s": 0.0,
        "complete_store_data_write_remote_bytes": 0,
        "complete_store_n_writes_remote": 0,
        "complete_store_data_write_remote_rpcs": 0,
        "complete_store_data_write_unknown_s": 0.0,
        "complete_store_data_write_unknown_bytes": 0,
        "complete_store_n_writes_unknown": 0,
        "complete_store_data_write_unknown_rpcs": 0,
        "complete_store_meta_update_status_s": 0.0,
        "complete_store_n_writes": 0,
    }


def _perf_load_acc_new() -> Dict[str, Any]:
    return {
        "prepare_load_wall_s": 0.0,
        "prepare_load_meta_batch_lookup_s": 0.0,
        "prepare_load_data_read_s": 0.0,
        "prepare_load_data_read_bytes": 0,
        "prepare_load_data_read_rpcs": 0,
        "prepare_load_data_read_local_s": 0.0,
        "prepare_load_data_read_local_bytes": 0,
        "prepare_load_n_reads_local": 0,
        "prepare_load_data_read_local_rpcs": 0,
        "prepare_load_data_read_remote_s": 0.0,
        "prepare_load_data_read_remote_bytes": 0,
        "prepare_load_n_reads_remote": 0,
        "prepare_load_data_read_remote_rpcs": 0,
        "prepare_load_data_read_unknown_s": 0.0,
        "prepare_load_data_read_unknown_bytes": 0,
        "prepare_load_n_reads_unknown": 0,
        "prepare_load_data_read_unknown_rpcs": 0,
        "prepare_load_n_reads": 0,
        "complete_load_wall_s": 0.0,
    }


def _perf_merge_store_chunk(acc: Dict[str, Any], bd: Dict[str, Any]) -> None:
    ps = bd.get("prepare_store") or {}
    cs = bd.get("complete_store") or {}
    acc["prepare_store_wall_s"] += float(ps.get("wall_s", 0.0))
    for dn, sec in (ps.get("meta_allocate_by_dn_s") or {}).items():
        k = str(dn)
        m = acc["meta_allocate_by_dn_s"]
        m[k] = m.get(k, 0.0) + float(sec)
    acc["complete_store_wall_s"] += float(cs.get("wall_s", 0.0))
    acc["complete_store_data_write_s"] += float(cs.get("data_write_s", 0.0))
    acc["complete_store_data_write_bytes"] += int(cs.get("data_write_bytes", 0))
    acc["complete_store_data_write_rpcs"] += int(cs.get("data_write_rpcs", 0))
    for label in ("local", "remote", "unknown"):
        acc[f"complete_store_data_write_{label}_s"] += float(cs.get(f"data_write_{label}_s", 0.0))
        acc[f"complete_store_data_write_{label}_bytes"] += int(cs.get(f"data_write_{label}_bytes", 0))
        acc[f"complete_store_n_writes_{label}"] += int(cs.get(f"n_writes_{label}", 0))
        acc[f"complete_store_data_write_{label}_rpcs"] += int(cs.get(f"data_write_{label}_rpcs", 0))
    acc["complete_store_meta_update_status_s"] += float(cs.get("meta_update_status_s", 0.0))
    acc["complete_store_n_writes"] += int(cs.get("n_writes_instrumented", 0))


def _perf_merge_load_chunk(acc: Dict[str, Any], bd: Dict[str, Any]) -> None:
    pl = bd.get("prepare_load") or {}
    cl = bd.get("complete_load") or {}
    acc["prepare_load_wall_s"] += float(pl.get("wall_s", 0.0))
    acc["prepare_load_meta_batch_lookup_s"] += float(pl.get("meta_batch_lookup_s", 0.0))
    acc["prepare_load_data_read_s"] += float(pl.get("data_read_s", 0.0))
    acc["prepare_load_data_read_bytes"] += int(pl.get("data_read_bytes", 0))
    acc["prepare_load_data_read_rpcs"] += int(pl.get("data_read_rpcs", 0))
    for label in ("local", "remote", "unknown"):
        acc[f"prepare_load_data_read_{label}_s"] += float(pl.get(f"data_read_{label}_s", 0.0))
        acc[f"prepare_load_data_read_{label}_bytes"] += int(pl.get(f"data_read_{label}_bytes", 0))
        acc[f"prepare_load_n_reads_{label}"] += int(pl.get(f"n_reads_{label}", 0))
        acc[f"prepare_load_data_read_{label}_rpcs"] += int(pl.get(f"data_read_{label}_rpcs", 0))
    acc["prepare_load_n_reads"] += int(pl.get("n_reads", 0))
    acc["complete_load_wall_s"] += float(cl.get("wall_s", 0.0))


def _finalize_store_breakdown(
    acc: Dict[str, Any],
    nbytes: int,
    *,
    n_batches: Optional[int] = None,
) -> Dict[str, Any]:
    mb = nbytes / 1.0e6
    dws = max(float(acc["complete_store_data_write_s"]), 1e-12)
    mus = max(float(acc["complete_store_meta_update_status_s"]), 1e-12)
    nw = max(int(acc["complete_store_n_writes"]), 1)
    meta_alloc = float(sum((acc.get("meta_allocate_by_dn_s") or {}).values()))
    psw = max(float(acc["prepare_store_wall_s"]), 1e-12)
    csw = max(float(acc["complete_store_wall_s"]), 1e-12)
    out: Dict[str, Any] = {
        "note": (
            "complete_store_data_write_s and complete_store_meta_update_status_s sum per-block "
            "instrumented intervals across threads; they can exceed complete_store_wall_s when "
            "the data pool runs writes in parallel. "
            "per_batch_meta_ms divides prepare_store / allocate by n_batches (one OM prepare_store "
            "wave per chunk). per_block_* divides by complete_store_n_writes (one metadata "
            "update_status + data write path per logical block)."
        ),
        "prepare_store_wall_s": round(float(acc["prepare_store_wall_s"]), 6),
        "meta_allocate_by_dn_s": {k: round(v, 6) for k, v in (acc.get("meta_allocate_by_dn_s") or {}).items()},
        "meta_allocate_total_s": round(meta_alloc, 6),
        "complete_store_wall_s": round(float(acc["complete_store_wall_s"]), 6),
        "complete_store_data_write_s": round(float(acc["complete_store_data_write_s"]), 6),
        "complete_store_meta_update_status_s": round(float(acc["complete_store_meta_update_status_s"]), 6),
        "complete_store_n_writes": int(acc["complete_store_n_writes"]),
        "complete_store_data_write_rpcs": int(acc.get("complete_store_data_write_rpcs", 0)),
        "complete_store_data_write_local_rpcs": int(acc.get("complete_store_data_write_local_rpcs", 0)),
        "complete_store_data_write_remote_rpcs": int(acc.get("complete_store_data_write_remote_rpcs", 0)),
        "complete_store_data_write_unknown_rpcs": int(acc.get("complete_store_data_write_unknown_rpcs", 0)),
        "payload_mb_per_data_write_s": round(mb / dws, 3),
        "payload_mb_per_prepare_store_wall_s": round(mb / psw, 3),
        "payload_mb_per_complete_store_wall_s": round(mb / csw, 3),
        "avg_data_write_ms": round(1000.0 * float(acc["complete_store_data_write_s"]) / nw, 4),
        "avg_data_write_rpc_ms": round(
            1000.0 * float(acc["complete_store_data_write_s"])
            / max(int(acc.get("complete_store_data_write_rpcs", 0)), 1),
            4,
        ),
        "avg_meta_update_ms": round(1000.0 * float(acc["complete_store_meta_update_status_s"]) / nw, 4),
        "meta_updates_per_s": round(nw / mus, 1),
    }
    nb = int(n_batches) if n_batches is not None and int(n_batches) > 0 else 0
    if nb > 0:
        psw_v = float(acc["prepare_store_wall_s"])
        out["n_batches"] = nb
        out["per_batch_meta_ms"] = {
            "prepare_store_wall_ms": round(1000.0 * psw_v / nb, 4),
            "meta_allocate_ms": round(1000.0 * meta_alloc / nb, 4),
        }
    out["per_block_meta_ms"] = {
        "meta_update_status_ms": round(1000.0 * float(acc["complete_store_meta_update_status_s"]) / nw, 4),
    }
    out["per_block_data_ms"] = {
        "data_write_instrumented_ms": round(1000.0 * float(acc["complete_store_data_write_s"]) / nw, 4),
    }
    out["data_path_local_remote"] = _path_data_stats(
        prefix="store write",
        local_s=float(acc.get("complete_store_data_write_local_s", 0.0)),
        local_bytes=int(acc.get("complete_store_data_write_local_bytes", 0)),
        local_ops=int(acc.get("complete_store_n_writes_local", 0)),
        remote_s=float(acc.get("complete_store_data_write_remote_s", 0.0)),
        remote_bytes=int(acc.get("complete_store_data_write_remote_bytes", 0)),
        remote_ops=int(acc.get("complete_store_n_writes_remote", 0)),
        unknown_s=float(acc.get("complete_store_data_write_unknown_s", 0.0)),
        unknown_bytes=int(acc.get("complete_store_data_write_unknown_bytes", 0)),
        unknown_ops=int(acc.get("complete_store_n_writes_unknown", 0)),
    )
    return out


def _finalize_load_breakdown(
    acc: Dict[str, Any],
    nbytes: int,
    *,
    n_batches: Optional[int] = None,
) -> Dict[str, Any]:
    mb = nbytes / 1.0e6
    drs = max(float(acc["prepare_load_data_read_s"]), 1e-12)
    nr = max(int(acc["prepare_load_n_reads"]), 1)
    mls = max(float(acc["prepare_load_meta_batch_lookup_s"]), 1e-12)
    plw = max(float(acc["prepare_load_wall_s"]), 1e-12)
    clw = max(float(acc["complete_load_wall_s"]), 1e-12)
    out: Dict[str, Any] = {
        "note": (
            "prepare_load_data_read_s sums per-read intervals across threads and may exceed "
            "prepare_load_wall_s under parallelism. "
            "per_batch_meta_ms uses n_batches (one prepare_load + complete_load wave per chunk): "
            "meta_batch_lookup and complete_load renew are batch-shaped; prepare_load_wall_ms "
            "includes that batch's data reads. per_block_data_ms divides data_read_s by n_reads."
        ),
        "prepare_load_wall_s": round(float(acc["prepare_load_wall_s"]), 6),
        "prepare_load_meta_batch_lookup_s": round(mls, 6),
        "prepare_load_data_read_s": round(float(acc["prepare_load_data_read_s"]), 6),
        "prepare_load_n_reads": int(acc["prepare_load_n_reads"]),
        "prepare_load_data_read_rpcs": int(acc.get("prepare_load_data_read_rpcs", 0)),
        "prepare_load_data_read_local_rpcs": int(acc.get("prepare_load_data_read_local_rpcs", 0)),
        "prepare_load_data_read_remote_rpcs": int(acc.get("prepare_load_data_read_remote_rpcs", 0)),
        "prepare_load_data_read_unknown_rpcs": int(acc.get("prepare_load_data_read_unknown_rpcs", 0)),
        "complete_load_wall_s": round(float(acc["complete_load_wall_s"]), 6),
        "payload_mb_per_data_read_s": round(mb / drs, 3),
        "payload_mb_per_prepare_load_wall_s": round(mb / plw, 3),
        "avg_data_read_ms": round(1000.0 * float(acc["prepare_load_data_read_s"]) / nr, 4),
        "avg_data_read_rpc_ms": round(1000.0 * float(acc["prepare_load_data_read_s"]) / max(int(acc.get("prepare_load_data_read_rpcs", 0)), 1), 4),
        "avg_meta_batch_lookup_ms": round(1000.0 * mls / nr, 4),
        "note_avg_meta_batch_lookup_ms": (
            "This value divides total batch-lookup time by n_reads (blocks); use "
            "load_performance_analysis.batch_lookup.per_batch_ms for true per-batch lookup latency."
        ),
        "reads_per_prepare_load_s": round(nr / plw, 1),
        "payload_mb_per_complete_load_wall_s": round(mb / clw, 3) if clw > 1e-9 else None,
    }
    nb = int(n_batches) if n_batches is not None and int(n_batches) > 0 else 0
    if nb > 0:
        out["n_batches"] = nb
        out["per_batch_meta_ms"] = {
            "meta_batch_lookup_ms": round(1000.0 * float(acc["prepare_load_meta_batch_lookup_s"]) / nb, 4),
            "complete_load_renew_wall_ms": round(1000.0 * float(acc["complete_load_wall_s"]) / nb, 4),
            "prepare_load_wall_ms": round(1000.0 * float(acc["prepare_load_wall_s"]) / nb, 4),
        }
    out["per_block_data_ms"] = {
        "data_read_instrumented_ms": round(1000.0 * float(acc["prepare_load_data_read_s"]) / nr, 4),
    }
    out["data_path_local_remote"] = _path_data_stats(
        prefix="load read",
        local_s=float(acc.get("prepare_load_data_read_local_s", 0.0)),
        local_bytes=int(acc.get("prepare_load_data_read_local_bytes", 0)),
        local_ops=int(acc.get("prepare_load_n_reads_local", 0)),
        remote_s=float(acc.get("prepare_load_data_read_remote_s", 0.0)),
        remote_bytes=int(acc.get("prepare_load_data_read_remote_bytes", 0)),
        remote_ops=int(acc.get("prepare_load_n_reads_remote", 0)),
        unknown_s=float(acc.get("prepare_load_data_read_unknown_s", 0.0)),
        unknown_bytes=int(acc.get("prepare_load_data_read_unknown_bytes", 0)),
        unknown_ops=int(acc.get("prepare_load_n_reads_unknown", 0)),
    )
    return out


def _performance_analysis_store_phase(
    acc: Dict[str, Any], nbytes: int, n_batches: int
) -> Dict[str, Any]:
    """Structured store-phase latency + throughput for analysts (two-phase / similar)."""
    nb = max(int(n_batches), 1)
    nw = max(int(acc["complete_store_n_writes"]), 1)
    mb = nbytes / 1.0e6
    psw = float(acc["prepare_store_wall_s"])
    meta_alloc = float(sum((acc.get("meta_allocate_by_dn_s") or {}).values()))
    csw = float(acc["complete_store_wall_s"])
    dws = float(acc["complete_store_data_write_s"])
    mus = float(acc["complete_store_meta_update_status_s"])
    return {
        "n_batches": nb,
        "logical_blocks": nw,
        "payload_mb": round(mb, 3),
        "batch_allocate_meta": {
            "note": "One prepare_store(chunk) per batch; times summed over all batches.",
            "prepare_store_wall_total_s": round(psw, 6),
            "meta_allocate_total_s": round(meta_alloc, 6),
            "per_batch_ms": {
                "prepare_store_wall_ms": round(1000.0 * psw / nb, 4),
                "meta_allocate_ms": round(1000.0 * meta_alloc / nb, 4),
            },
        },
        "per_block_complete_store_path": {
            "note": (
                "complete_store runs unary writes + per-block update_status; "
                "instrumented data_write_s sums overlapping thread intervals (can exceed wall_s). "
                "data_path_local_remote below is direct per-operation timing, not ratio attribution."
            ),
            "complete_store_wall_total_s": round(csw, 6),
            "instrumented_data_write_total_s": round(dws, 6),
            "instrumented_meta_update_status_total_s": round(mus, 6),
            "per_block_ms": {
                "avg_complete_store_wall_ms": round(1000.0 * csw / nw, 4),
                "avg_data_write_instrumented_ms": round(1000.0 * dws / nw, 4),
                "avg_meta_update_status_ms": round(1000.0 * mus / nw, 4),
            },
            "data_path_local_remote": _path_data_stats(
                prefix="store write",
                local_s=float(acc.get("complete_store_data_write_local_s", 0.0)),
                local_bytes=int(acc.get("complete_store_data_write_local_bytes", 0)),
                local_ops=int(acc.get("complete_store_n_writes_local", 0)),
                remote_s=float(acc.get("complete_store_data_write_remote_s", 0.0)),
                remote_bytes=int(acc.get("complete_store_data_write_remote_bytes", 0)),
                remote_ops=int(acc.get("complete_store_n_writes_remote", 0)),
                unknown_s=float(acc.get("complete_store_data_write_unknown_s", 0.0)),
                unknown_bytes=int(acc.get("complete_store_data_write_unknown_bytes", 0)),
                unknown_ops=int(acc.get("complete_store_n_writes_unknown", 0)),
            ),
        },
        "throughput_mb_per_s": {
            "payload_per_phase_wall_if_only_complete_store": round(mb / max(csw, 1e-12), 3),
            "payload_per_instrumented_data_write_sum": round(mb / max(dws, 1e-12), 3),
        },
    }


def _performance_analysis_load_phase(
    acc: Dict[str, Any], nbytes: int, n_batches: int
) -> Dict[str, Any]:
    """Structured load-phase: batch lookup vs per-block read + throughput (two-phase / similar).

    In ``FalconFSOffloadingManager.prepare_load``, ``meta_batch_lookup_s`` is wall time for
    the batched metadata lookup for **all keys in that chunk** before any store read starts.
    ``data_read_s`` sums per-block read intervals (parallelism can make sum > prepare_load wall).
    """
    nb = max(int(n_batches), 1)
    nr = max(int(acc["prepare_load_n_reads"]), 1)
    mb = nbytes / 1.0e6
    mbl = float(acc["prepare_load_meta_batch_lookup_s"])
    plw = float(acc["prepare_load_wall_s"])
    drs = float(acc["prepare_load_data_read_s"])
    clw = float(acc["complete_load_wall_s"])
    post_lookup_wall = max(plw - mbl, 0.0)
    return {
        "n_batches": nb,
        "logical_blocks": nr,
        "payload_mb": round(mb, 3),
        "batch_lookup": {
            "note": (
                "Wall time for batched DN metadata lookup for all keys in each prepare_load(chunk); "
                "total_s is summed over batches. per_batch_ms = total / n_batches."
            ),
            "total_s_all_batches": round(mbl, 6),
            "per_batch_ms": round(1000.0 * mbl / nb, 4),
        },
        "per_block_load_data": {
            "note": (
                "Instrumented time inside each store ReadBlock path, summed across blocks/threads; "
                "per_block_ms = total / n_reads (average interval per read, not sequential latency). "
                "data_path_local_remote below is direct per-operation timing, not ratio attribution."
            ),
            "interpretation": (
                "per_block_ms is NOT 'wall seconds to read one 2MiB block sequentially'. Under "
                "FALCON_KV_CLIENT_DATA_PARALLELISM_MAX concurrent unary reads, OM sums each read's "
                "interval, so totals often exceed prepare_load wall; dividing by n_reads yields a "
                "rough per-block *instrumented* average, still dominated by BRPC/serialization when "
                "not using LocalKVStoreShmFacade."
            ),
            "instrumented_read_time_total_s": round(drs, 6),
            "per_block_ms": round(1000.0 * drs / nr, 4),
            "data_path_local_remote": _path_data_stats(
                prefix="load read",
                local_s=float(acc.get("prepare_load_data_read_local_s", 0.0)),
                local_bytes=int(acc.get("prepare_load_data_read_local_bytes", 0)),
                local_ops=int(acc.get("prepare_load_n_reads_local", 0)),
                remote_s=float(acc.get("prepare_load_data_read_remote_s", 0.0)),
                remote_bytes=int(acc.get("prepare_load_data_read_remote_bytes", 0)),
                remote_ops=int(acc.get("prepare_load_n_reads_remote", 0)),
                unknown_s=float(acc.get("prepare_load_data_read_unknown_s", 0.0)),
                unknown_bytes=int(acc.get("prepare_load_data_read_unknown_bytes", 0)),
                unknown_ops=int(acc.get("prepare_load_n_reads_unknown", 0)),
            ),
        },
        "prepare_load_wall": {
            "note": (
                "Full prepare_load wall per batch includes batch_lookup + overlapping data reads. "
                "post_lookup_wall_per_batch_ms is (prepare_load_wall - batch_lookup) / n_batches."
            ),
            "total_s_all_batches": round(plw, 6),
            "per_batch_ms": round(1000.0 * plw / nb, 4),
            "post_batch_lookup_wall_per_batch_ms": round(1000.0 * post_lookup_wall / nb, 4),
        },
        "complete_load_renew_meta": {
            "note": "Lease renew metadata RPCs per prepare_load wave (one complete_load per batch).",
            "total_s_all_batches": round(clw, 6),
            "per_batch_ms": round(1000.0 * clw / nb, 4),
        },
        "throughput_mb_per_s": {
            "payload_per_prepare_load_wall": round(mb / max(plw, 1e-12), 3),
            "payload_per_instrumented_data_read_sum": round(mb / max(drs, 1e-12), 3),
            "batch_lookup_time_fraction_of_prepare_load_wall": round(
                mbl / max(plw, 1e-12), 5
            ),
        },
    }


def _prime_dn_catalog_for_mixed_e2e(conninfo: str) -> None:
    """Single-node harness: CN watchdog can mark DNs unhealthy without live heartbeats.

    Without healthy rows, ``discover_dn_endpoints`` is empty and the facade registry
    skips DN metadata routing. Priming keeps Python E2E aligned with CI clusters
    that run full DN stacks. The helper re-registers harness DN rows instead of
    only flipping ``healthy`` so it also repairs rows deleted by preceding fault
    drills or catalog cleanup. Opt out with ``FALCON_MIX_SKIP_DN_PRIME=1``.
    """
    if os.environ.get("FALCON_MIX_SKIP_DN_PRIME", "").strip().lower() in ("1", "true", "yes", "on"):
        return
    psql_bin = "/usr/local/pgsql/bin/psql"
    if not Path(psql_bin).is_file():
        psql_bin = shutil.which("psql") or ""
    if not psql_bin:
        return

    def _pooler_port(endpoint: str) -> int:
        return int(endpoint.rsplit(":", 1)[1])

    dn_specs = [
        (1, "worker0", _pooler_port(DN1_POOLER)),
        (2, "worker1", _pooler_port(DN2_POOLER)),
    ]
    if _can_reach(DN3_POOLER, timeout_s=0.25):
        dn_specs.append((3, "worker2", _pooler_port(DN3_POOLER)))

    register_sql = []
    for sid, host_node, port in dn_specs:
        register_sql.append(
            "SELECT pg_catalog.falcon_dn_node_register("
            f"{sid}, '{host_node}'::cstring, '127.0.0.1'::cstring, "
            f"{port}::int, {port}::int, "
            f"COALESCE((SELECT dn_epoch FROM pg_catalog.falcon_dn_node WHERE server_id = {sid}), 1)::bigint"
            ");"
        )
    register_sql.append(
        "UPDATE pg_catalog.falcon_dn_node SET healthy = true, "
        "last_heartbeat_ms = (EXTRACT(EPOCH FROM now()) * 1000)::bigint;"
    )
    sql = "\n".join(register_sql)
    for attempt in range(3):
        proc = subprocess.run(
            [psql_bin, conninfo, "-v", "ON_ERROR_STOP=1", "-q", "-c", sql],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        if proc.returncode == 0:
            return
        time.sleep(0.25 * (attempt + 1))


def _mixed_e2e_live_dn_shard_table() -> Dict[int, str]:
    """Known harness DN poolers for tests while CN discovery catches up."""
    candidates = {
        1: DN1_POOLER,
        2: DN2_POOLER,
        3: DN3_POOLER,
    }
    return {
        dn_id: endpoint
        for dn_id, endpoint in candidates.items()
        if _can_reach(endpoint, timeout_s=0.5)
    }


def _parse_child_report(stdout: str) -> Optional[Dict[str, Any]]:
    for line in stdout.splitlines():
        if line.startswith("FALCON_MIX_REPORT\t"):
            return json.loads(line.split("\t", 1)[1])
    return None


def _store_locality_snapshot() -> Dict[int, bool]:
    try:
        from falconfs_kv import falconfs_kv_brpc as b

        return {int(k): bool(v) for k, v in dict(b.store_locality()).items()}
    except Exception:
        return {}


def _facade_ipc_snapshot() -> Dict[str, int]:
    """Cumulative facade data-path counters (see ``falconfs_kv_brpc.facade_ipc_stats``)."""
    try:
        from falconfs_kv import falconfs_kv_brpc as b

        lr, lw, rr, rw = b.facade_ipc_stats()
        return {
            "local_reads": int(lr),
            "local_writes": int(lw),
            "remote_reads": int(rr),
            "remote_writes": int(rw),
        }
    except Exception:
        return {
            "local_reads": 0,
            "local_writes": 0,
            "remote_reads": 0,
            "remote_writes": 0,
        }


def _facade_ipc_data_path_report() -> Dict[str, Any]:
    """Human-readable split: colocated SHM facade vs remote BRPC facade (see design §29.4)."""
    d = _facade_ipc_snapshot()
    return {
        "shared_memory_local_reads": d["local_reads"],
        "shared_memory_local_writes": d["local_writes"],
        "brpc_remote_reads": d["remote_reads"],
        "brpc_remote_writes": d["remote_writes"],
        "note": (
            "From falconfs_kv_brpc counters on facade BatchRead/BatchWrite (and unary paths): "
            "local = LocalKVStoreShmFacade (DRAM memcpy); remote = RemoteKVStoreFacade. "
            "ReadFromSSD on a local facade still increments local_reads in C++."
        ),
    }


def _facade_perf_snapshot() -> Dict[str, Any]:
    """C++ facade timing counters split by local SHM vs remote BRPC."""
    try:
        from falconfs_kv import falconfs_kv_brpc as b

        if hasattr(b, "facade_perf_stats"):
            return dict(b.facade_perf_stats())
    except Exception:
        pass
    return {}


def _om_perf_phases(mgr: Any) -> Dict[str, Any]:
    """Snapshot of ``perf_breakdown()`` keys used by mixed E2E (cluster mode)."""
    bd = mgr.perf_breakdown()
    return {
        "prepare_store": dict(bd.get("prepare_store") or {}),
        "complete_store": dict(bd.get("complete_store") or {}),
        "prepare_load": dict(bd.get("prepare_load") or {}),
        "complete_load": dict(bd.get("complete_load") or {}),
        "metadata_channel_cache": dict(bd.get("metadata_channel_cache") or {}),
        "promote_on_read": dict(bd.get("promote_on_read") or {}),
        "adaptive_batching": dict(bd.get("adaptive_batching") or {}),
        "zero_copy_read": dict(bd.get("zero_copy_read") or {}),
        "prealloc_load_buffers": dict(bd.get("prealloc_load_buffers") or {}),
    }


def _load_json_file(path: Path) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return None


def _extract_two_phase_throughput(obj: Any) -> Dict[str, Optional[float]]:
    events = obj if isinstance(obj, list) else [obj] if isinstance(obj, dict) else []
    for ev in reversed(events):
        if not isinstance(ev, dict):
            continue
        tee = ev.get("throughput_end_to_end")
        if isinstance(tee, dict):
            return {
                "store_mb_s": float(tee.get("mb_s_phase_store") or 0.0) or None,
                "load_mb_s": float(tee.get("mb_s_phase_load") or 0.0) or None,
            }
    return {"store_mb_s": None, "load_mb_s": None}


def _percent_change(cur: Optional[float], base: Optional[float]) -> Optional[float]:
    if cur is None or base is None or base <= 0:
        return None
    return round(100.0 * (cur - base) / base, 3)


def _percent_of(cur: Optional[float], upper: Optional[float]) -> Optional[float]:
    if cur is None or upper is None or upper <= 0:
        return None
    return round(100.0 * cur / upper, 3)


def _phase_mb_s(section: Dict[str, Any], phase: str) -> Optional[float]:
    blk = section.get(phase) if isinstance(section.get(phase), dict) else {}
    val = blk.get("wall_mb_s") if isinstance(blk, dict) else None
    try:
        return float(val) if val is not None else None
    except Exception:
        return None


_MICROBENCH_BOUND_KEYS = (
    "native_memcpy_bound",
    "local_shm_upper_bound",
    "local_shm_prealloc_write_bound",
    "remote_brpc_upper_bound",
    "mixed_facade_bound",
)


def _microbench_json_paths() -> List[Path]:
    raw_many = os.environ.get("FALCON_MIX_MICROBENCH_JSONS", "").strip()
    raw_one = os.environ.get("FALCON_MIX_MICROBENCH_JSON", "").strip()
    default_dir = ROOT.parent / "logs"
    defaults = [
        default_dir / "falcon_kv_store_microbench_native_latest.json",
        default_dir / "falcon_kv_store_microbench_local_read_latest.json",
        default_dir / "falcon_kv_store_microbench_local_write_latest.json",
        default_dir / "falcon_kv_store_microbench_remote_latest.json",
        default_dir / "falcon_kv_store_microbench_latest.json",
    ]
    parts: List[str] = []
    if raw_many:
        parts.extend([x for x in raw_many.replace(",", os.pathsep).split(os.pathsep) if x])
    if raw_one:
        parts.append(raw_one)
    parts.extend(str(x) for x in defaults)
    out: List[Path] = []
    seen = set()
    for item in parts:
        path = str(Path(item))
        if path in seen:
            continue
        seen.add(path)
        out.append(Path(path))
    return out


def _legacy_microbench_section(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    write = obj.get("write") if isinstance(obj.get("write"), dict) else {}
    read = obj.get("read") if isinstance(obj.get("read"), dict) else {}
    if not write and not read:
        return None
    return {
        "path_mode": obj.get("path_mode", "legacy"),
        "write": write,
        "read": read,
        "parallelism": obj.get("parallelism"),
        "batch_size": obj.get("batch_size"),
        "block_bytes": obj.get("block_bytes"),
        "payloads_preallocated": obj.get("payloads_preallocated"),
        "verification": obj.get("verification"),
        "locality": obj.get("store_locality") or obj.get("locality_assertions"),
    }


def _merge_microbench_obj(out: Dict[str, Any], obj: Dict[str, Any], path: Path) -> None:
    out.setdefault("source_files", []).append(str(path))
    for key in ("mode", "path_mode", "allocation", "stores", "blocks", "block_bytes", "parallelism", "batch_size", "verification", "payloads_preallocated", "locality_assertions"):
        if out.get(key) is None and obj.get(key) is not None:
            out[key] = obj.get(key)
    for key in _MICROBENCH_BOUND_KEYS:
        val = obj.get(key)
        if isinstance(val, dict):
            copied = dict(val)
            copied.setdefault("source_path", str(path))
            out[key] = copied
    if any(key in obj for key in _MICROBENCH_BOUND_KEYS):
        return
    section = _legacy_microbench_section(obj)
    if not section:
        return
    section = dict(section)
    section.setdefault("source_path", str(path))
    path_mode = str(obj.get("path_mode") or "")
    if path_mode == "local-shm-prealloc-write":
        out["local_shm_prealloc_write_bound"] = section
        return
    if path_mode == "local-shm-zero-copy-read":
        out["local_shm_upper_bound"] = section
        return
    if path_mode == "remote-brpc-attachment":
        out["remote_brpc_upper_bound"] = section
        return
    loc = obj.get("store_locality") if isinstance(obj.get("store_locality"), dict) else {}
    vals = list(loc.values())
    if vals and all(bool(v) for v in vals):
        out["local_shm_upper_bound"] = section
    elif vals and not any(bool(v) for v in vals):
        out["remote_brpc_upper_bound"] = section
    else:
        out["mixed_facade_bound"] = section


def _microbench_bounds_snapshot() -> Dict[str, Any]:
    paths = _microbench_json_paths()
    out: Dict[str, Any] = {
        "available": False,
        "paths": [str(p) for p in paths],
        "source_files": [],
        "mode": None,
        "path_mode": None,
        "allocation": None,
        "stores": None,
        "blocks": None,
        "block_bytes": None,
        "parallelism": None,
        "batch_size": None,
        "verification": None,
        "payloads_preallocated": None,
        "locality_assertions": None,
    }
    for path in paths:
        obj = _load_json_file(path)
        if isinstance(obj, dict):
            out["available"] = True
            _merge_microbench_obj(out, obj, path)
    out["path"] = out["source_files"][0] if out["source_files"] else str(paths[0]) if paths else ""
    return out


def _microbench_upper_bound_snapshot() -> Dict[str, Any]:
    bounds = _microbench_bounds_snapshot()
    if not bounds.get("available"):
        return bounds
    section = None
    for key in ("mixed_facade_bound", "local_shm_prealloc_write_bound", "local_shm_upper_bound", "remote_brpc_upper_bound"):
        if isinstance(bounds.get(key), dict):
            section = bounds[key]
            break
    section = section if isinstance(section, dict) else {}
    return {
        "available": True,
        "path": bounds.get("path"),
        "paths": bounds.get("source_files") or bounds.get("paths"),
        "mode": bounds.get("mode"),
        "path_mode": bounds.get("path_mode") or section.get("path_mode"),
        "allocation": bounds.get("allocation"),
        "stores": bounds.get("stores"),
        "blocks": bounds.get("blocks"),
        "block_bytes": bounds.get("block_bytes"),
        "parallelism": bounds.get("parallelism"),
        "batch_size": bounds.get("batch_size"),
        "write_wall_mb_s": _phase_mb_s(section, "write"),
        "read_wall_mb_s": _phase_mb_s(section, "read"),
    }


def _path_upper_bound_comparison(ev: Dict[str, Any], bounds: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    bounds = bounds or _microbench_bounds_snapshot()
    perf = ev.get("path_throughput_latency") if isinstance(ev.get("path_throughput_latency"), dict) else {}
    store = perf.get("store_write") if isinstance(perf.get("store_write"), dict) else {}
    load = perf.get("load_read") if isinstance(perf.get("load_read"), dict) else {}
    local_read_bound = bounds.get("local_shm_upper_bound") if isinstance(bounds.get("local_shm_upper_bound"), dict) else {}
    local_write_bound = bounds.get("local_shm_prealloc_write_bound") if isinstance(bounds.get("local_shm_prealloc_write_bound"), dict) else local_read_bound
    remote_bound = bounds.get("remote_brpc_upper_bound") if isinstance(bounds.get("remote_brpc_upper_bound"), dict) else {}

    def path_mb_s(phase: Dict[str, Any], label: str) -> Optional[float]:
        row = phase.get(label) if isinstance(phase.get(label), dict) else {}
        try:
            return float(row.get("instrumented_mb_s") or 0.0) or None
        except Exception:
            return None

    return {
        "available": bool(bounds.get("available")),
        "microbench_path": bounds.get("path"),
        "microbench_paths": bounds.get("source_files") or bounds.get("paths"),
        "local_store_pct_of_local_shm_upper": _percent_of(path_mb_s(store, "local_shm"), _phase_mb_s(local_write_bound, "write")),
        "local_load_pct_of_local_shm_upper": _percent_of(path_mb_s(load, "local_shm"), _phase_mb_s(local_read_bound, "read")),
        "remote_store_pct_of_remote_brpc_upper": _percent_of(path_mb_s(store, "remote_brpc"), _phase_mb_s(remote_bound, "write")),
        "remote_load_pct_of_remote_brpc_upper": _percent_of(path_mb_s(load, "remote_brpc"), _phase_mb_s(remote_bound, "read")),
        "note": "Local writes compare against local_shm_prealloc_write_bound when present; local reads compare against local_shm_upper_bound; remote paths compare against remote_brpc_upper_bound.",
    }


def _baseline_comparison_for_event(ev: Dict[str, Any]) -> Dict[str, Any]:
    tee = ev.get("throughput_end_to_end") if isinstance(ev.get("throughput_end_to_end"), dict) else {}
    current_store = float(tee.get("mb_s_phase_store") or 0.0) or None
    current_load = float(tee.get("mb_s_phase_load") or 0.0) or None
    baseline_env = os.environ.get("FALCON_MIX_BASELINE_JSON", "").strip()
    baseline_path = Path(baseline_env or str(ROOT.parent / "logs" / "falcon_mix_metrics_baseline.json"))
    baseline_exists = baseline_path.exists()
    baseline = _extract_two_phase_throughput(_load_json_file(baseline_path)) if baseline_exists else {"store_mb_s": None, "load_mb_s": None}
    bounds = _microbench_bounds_snapshot()
    upper = _microbench_upper_bound_snapshot()
    upper_store = upper.get("write_wall_mb_s") if upper.get("available") else None
    upper_load = upper.get("read_wall_mb_s") if upper.get("available") else None
    return {
        "current_store_mb_s": current_store,
        "current_load_mb_s": current_load,
        "saved_baseline_path": str(baseline_path),
        "saved_baseline_exists": bool(baseline_exists),
        "saved_baseline_store_mb_s": baseline.get("store_mb_s"),
        "saved_baseline_load_mb_s": baseline.get("load_mb_s"),
        "store_pct_change_from_baseline": _percent_change(current_store, baseline.get("store_mb_s")),
        "load_pct_change_from_baseline": _percent_change(current_load, baseline.get("load_mb_s")),
        "store_pct_of_upper_bound": _percent_of(current_store, upper_store),
        "load_pct_of_upper_bound": _percent_of(current_load, upper_load),
        "path_upper_bound_comparison": _path_upper_bound_comparison(ev, bounds),
        "gating_enabled": bool(baseline_env and baseline_exists and os.environ.get("FALCON_MIX_ALLOW_PERF_REGRESSION", "0").strip().lower() not in ("1", "true", "yes", "on")),
    }


def _truthy_env(name: str, default: str = "0") -> bool:
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


def _assert_required_upper_bound_comparison(ev: Dict[str, Any], bounds: Dict[str, Any], cmp_obj: Dict[str, Any]) -> None:
    if not _truthy_env("FALCON_MIX_REQUIRE_LOCAL_UPPER_BOUND", "1"):
        return
    if not bounds.get("available"):
        return
    locality = ev.get("store_locality") if isinstance(ev.get("store_locality"), dict) else {}
    has_local_store = any(bool(v) for v in locality.values())
    ratio = ev.get("local_remote_io_ratio") if isinstance(ev.get("local_remote_io_ratio"), dict) else {}
    store_counts = ratio.get("store_writes") if isinstance(ratio.get("store_writes"), dict) else {}
    load_counts = ratio.get("load_reads") if isinstance(ratio.get("load_reads"), dict) else {}
    has_local_io = int(store_counts.get("local", 0) or 0) > 0 or int(load_counts.get("local", 0) or 0) > 0
    if not (has_local_store or has_local_io):
        return
    missing: List[str] = []
    if not isinstance(bounds.get("local_shm_upper_bound"), dict):
        missing.append("local_shm_upper_bound")
    if not isinstance(bounds.get("local_shm_prealloc_write_bound"), dict):
        missing.append("local_shm_prealloc_write_bound")
    if int(store_counts.get("local", 0) or 0) > 0 and cmp_obj.get("local_store_pct_of_local_shm_upper") is None:
        missing.append("local_store_pct_of_local_shm_upper")
    if int(load_counts.get("local", 0) or 0) > 0 and cmp_obj.get("local_load_pct_of_local_shm_upper") is None:
        missing.append("local_load_pct_of_local_shm_upper")
    if missing:
        raise AssertionError(f"local Store exists but required local upper-bound metrics are missing/null: {missing}; microbench_paths={bounds.get('source_files') or bounds.get('paths')}")


def _enforce_baseline_gate(event: Dict[str, Any], comparison: Dict[str, Any]) -> None:
    baseline_env = os.environ.get("FALCON_MIX_BASELINE_JSON", "").strip()
    if not baseline_env or not comparison.get("saved_baseline_exists"):
        return
    if _truthy_env("FALCON_MIX_ALLOW_PERF_REGRESSION", "0"):
        return
    try:
        threshold = abs(float(os.environ.get("FALCON_MIX_BASELINE_REGRESSION_PCT", "10")))
    except ValueError:
        threshold = 10.0
    failures: List[str] = []
    for label, key in (("store", "store_pct_change_from_baseline"), ("load", "load_pct_change_from_baseline")):
        val = comparison.get(key)
        if val is not None and float(val) < -threshold:
            failures.append(f"{label} {val}%")
    if failures:
        raise AssertionError(
            "mixed E2E throughput regressed beyond "
            f"{threshold:.1f}% versus {baseline_env}: " + ", ".join(failures)
        )


def _facade_ipc_sub(
    a: Dict[str, int], b: Dict[str, int]
) -> Dict[str, int]:
    keys = ("local_reads", "local_writes", "remote_reads", "remote_writes")
    return {k: int(a.get(k, 0)) - int(b.get(k, 0)) for k in keys}


def _count_ratio(local: int, remote: int) -> Dict[str, Any]:
    local = max(int(local), 0)
    remote = max(int(remote), 0)
    total = local + remote
    return {
        "local": local,
        "remote": remote,
        "total": total,
        "local_pct": round(100.0 * local / total, 3) if total else 0.0,
        "remote_pct": round(100.0 * remote / total, 3) if total else 0.0,
        "local_to_remote": round(local / remote, 4) if remote else None,
        "remote_to_local": round(remote / local, 4) if local else None,
    }


def _derive_local_remote_io_ratio(ev: Dict[str, Any]) -> Dict[str, Any]:
    loc = ev.get("store_locality") if isinstance(ev.get("store_locality"), dict) else {}
    local_store_ids = sorted(int(k) for k, v in loc.items() if bool(v)) if isinstance(loc, dict) else []
    remote_store_ids = sorted(int(k) for k, v in loc.items() if not bool(v)) if isinstance(loc, dict) else []

    st = ev.get("facade_ipc_stats") if isinstance(ev.get("facade_ipc_stats"), dict) else {}
    after_write = ev.get("facade_after_write") if isinstance(ev.get("facade_after_write"), dict) else {}
    after_read = ev.get("facade_after_read") if isinstance(ev.get("facade_after_read"), dict) else {}
    two_phase_after_store = ev.get("facade_ipc_after_store_phase") if isinstance(ev.get("facade_ipc_after_store_phase"), dict) else {}

    if after_write:
        sw_local = int(after_write.get("lw", 0))
        sw_remote = int(after_write.get("rw", 0))
    elif two_phase_after_store:
        sw_local = int(two_phase_after_store.get("local_writes", 0))
        sw_remote = int(two_phase_after_store.get("remote_writes", 0))
    else:
        sw_local = int(st.get("local_writes", 0))
        sw_remote = int(st.get("remote_writes", 0))

    if after_read and after_write:
        lr_local = int(after_read.get("lr", 0)) - int(after_write.get("lr", 0))
        lr_remote = int(after_read.get("rr", 0)) - int(after_write.get("rr", 0))
    elif two_phase_after_store:
        lr_local = int(st.get("local_reads", 0)) - int(two_phase_after_store.get("local_reads", 0))
        lr_remote = int(st.get("remote_reads", 0)) - int(two_phase_after_store.get("remote_reads", 0))
    else:
        lr_local = int(st.get("local_reads", 0))
        lr_remote = int(st.get("remote_reads", 0))

    block_local = int(ev.get("blocks_local_store", 0) or 0)
    block_remote = int(ev.get("blocks_remote_store", 0) or 0)
    return {
        "note": "Local means colocated LocalKVStoreShmFacade; remote means RemoteKVStoreFacade over BRPC.",
        "colocated_local_store_ids": local_store_ids,
        "remote_store_ids": remote_store_ids,
        "block_placement": _count_ratio(block_local, block_remote) if block_local or block_remote else {},
        "store_writes": _count_ratio(sw_local, sw_remote),
        "load_reads": _count_ratio(lr_local, lr_remote),
    }


def _one_path_perf(*, ops: int, rpcs: int, byte_count: int, seconds: float) -> Dict[str, Any]:
    ops = max(int(ops), 0)
    rpcs = max(int(rpcs), 0)
    byte_count = max(int(byte_count), 0)
    seconds = max(float(seconds), 0.0)
    mb = byte_count / 1.0e6
    return {
        "ops": ops,
        "rpcs": rpcs,
        "bytes": byte_count,
        "total_s": round(seconds, 6),
        "avg_ms_per_block": round(1000.0 * seconds / ops, 4) if ops else 0.0,
        "avg_ms_per_rpc": round(1000.0 * seconds / rpcs, 4) if rpcs else 0.0,
        "instrumented_mb_s": round(mb / max(seconds, 1e-12), 3) if byte_count else 0.0,
        "blocks_per_rpc": round(ops / rpcs, 4) if rpcs else 0.0,
    }


def _derive_path_throughput_latency(ev: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "note": "Per-path timing from OffloadingManager data boundary. Timers are summed across concurrent tasks, so use phase wall MB/s for end-to-end throughput and these numbers for local-vs-remote latency/throughput comparison.",
    }

    agg_store = ev.get("perf_breakdown_store_phase") if isinstance(ev.get("perf_breakdown_store_phase"), dict) else {}
    agg_load = ev.get("perf_breakdown_load_phase") if isinstance(ev.get("perf_breakdown_load_phase"), dict) else {}
    if agg_store:
        sdp = agg_store.get("data_path_local_remote") if isinstance(agg_store.get("data_path_local_remote"), dict) else {}
        loc = sdp.get("local_shm") if isinstance(sdp.get("local_shm"), dict) else {}
        rem = sdp.get("remote_brpc") if isinstance(sdp.get("remote_brpc"), dict) else {}
        out["store_write"] = {
            "local_shm": _one_path_perf(
                ops=int(loc.get("ops", 0) or 0),
                rpcs=int(agg_store.get("complete_store_data_write_local_rpcs", 0) or 0),
                byte_count=int(loc.get("bytes", 0) or 0),
                seconds=float(loc.get("total_s", 0.0) or 0.0),
            ),
            "remote_brpc": _one_path_perf(
                ops=int(rem.get("ops", 0) or 0),
                rpcs=int(agg_store.get("complete_store_data_write_remote_rpcs", 0) or 0),
                byte_count=int(rem.get("bytes", 0) or 0),
                seconds=float(rem.get("total_s", 0.0) or 0.0),
            ),
            "overall": _one_path_perf(
                ops=int(agg_store.get("complete_store_n_writes", 0) or 0),
                rpcs=int(agg_store.get("complete_store_data_write_rpcs", 0) or 0),
                byte_count=int((loc.get("bytes", 0) or 0) + (rem.get("bytes", 0) or 0)),
                seconds=float(agg_store.get("complete_store_data_write_s", 0.0) or 0.0),
            ),
        }
    if agg_load:
        ldp = agg_load.get("data_path_local_remote") if isinstance(agg_load.get("data_path_local_remote"), dict) else {}
        loc = ldp.get("local_shm") if isinstance(ldp.get("local_shm"), dict) else {}
        rem = ldp.get("remote_brpc") if isinstance(ldp.get("remote_brpc"), dict) else {}
        out["load_read"] = {
            "local_shm": _one_path_perf(
                ops=int(loc.get("ops", 0) or 0),
                rpcs=int(agg_load.get("prepare_load_data_read_local_rpcs", 0) or 0),
                byte_count=int(loc.get("bytes", 0) or 0),
                seconds=float(loc.get("total_s", 0.0) or 0.0),
            ),
            "remote_brpc": _one_path_perf(
                ops=int(rem.get("ops", 0) or 0),
                rpcs=int(agg_load.get("prepare_load_data_read_remote_rpcs", 0) or 0),
                byte_count=int(rem.get("bytes", 0) or 0),
                seconds=float(rem.get("total_s", 0.0) or 0.0),
            ),
            "overall": _one_path_perf(
                ops=int(agg_load.get("prepare_load_n_reads", 0) or 0),
                rpcs=int(agg_load.get("prepare_load_data_read_rpcs", 0) or 0),
                byte_count=int((loc.get("bytes", 0) or 0) + (rem.get("bytes", 0) or 0)),
                seconds=float(agg_load.get("prepare_load_data_read_s", 0.0) or 0.0),
            ),
        }
    if "store_write" in out or "load_read" in out:
        return out

    om = ev.get("om_perf") if isinstance(ev.get("om_perf"), dict) else ev.get("om_perf_breakdown")
    if not isinstance(om, dict):
        om = {}
    cs = om.get("complete_store") if isinstance(om.get("complete_store"), dict) else {}
    pl = om.get("prepare_load") if isinstance(om.get("prepare_load"), dict) else {}
    if cs:
        out["store_write"] = {
            "local_shm": _one_path_perf(
                ops=int(cs.get("n_writes_local", 0) or 0),
                rpcs=int(cs.get("data_write_local_rpcs", 0) or 0),
                byte_count=int(cs.get("data_write_local_bytes", 0) or 0),
                seconds=float(cs.get("data_write_local_s", 0.0) or 0.0),
            ),
            "remote_brpc": _one_path_perf(
                ops=int(cs.get("n_writes_remote", 0) or 0),
                rpcs=int(cs.get("data_write_remote_rpcs", 0) or 0),
                byte_count=int(cs.get("data_write_remote_bytes", 0) or 0),
                seconds=float(cs.get("data_write_remote_s", 0.0) or 0.0),
            ),
            "overall": _one_path_perf(
                ops=int(cs.get("n_writes_instrumented", cs.get("n_writes", 0)) or 0),
                rpcs=int(cs.get("data_write_rpcs", 0) or 0),
                byte_count=int(cs.get("data_write_bytes", 0) or 0),
                seconds=float(cs.get("data_write_s", 0.0) or 0.0),
            ),
        }
    if pl:
        out["load_read"] = {
            "local_shm": _one_path_perf(
                ops=int(pl.get("n_reads_local", 0) or 0),
                rpcs=int(pl.get("data_read_local_rpcs", 0) or 0),
                byte_count=int(pl.get("data_read_local_bytes", 0) or 0),
                seconds=float(pl.get("data_read_local_s", 0.0) or 0.0),
            ),
            "remote_brpc": _one_path_perf(
                ops=int(pl.get("n_reads_remote", 0) or 0),
                rpcs=int(pl.get("data_read_remote_rpcs", 0) or 0),
                byte_count=int(pl.get("data_read_remote_bytes", 0) or 0),
                seconds=float(pl.get("data_read_remote_s", 0.0) or 0.0),
            ),
            "overall": _one_path_perf(
                ops=int(pl.get("n_reads", 0) or 0),
                rpcs=int(pl.get("data_read_rpcs", 0) or 0),
                byte_count=int(pl.get("data_read_bytes", 0) or 0),
                seconds=float(pl.get("data_read_s", 0.0) or 0.0),
            ),
        }
    return out


def _path_data_stats(
    *,
    prefix: str,
    local_s: float,
    local_bytes: int,
    local_ops: int,
    remote_s: float,
    remote_bytes: int,
    remote_ops: int,
    unknown_s: float = 0.0,
    unknown_bytes: int = 0,
    unknown_ops: int = 0,
) -> Dict[str, Any]:
    def _one(seconds: float, byte_count: int, ops: int) -> Dict[str, Any]:
        mb = byte_count / 1.0e6
        return {
            "ops": int(ops),
            "bytes": int(byte_count),
            "total_s": round(seconds, 6),
            "avg_ms_per_op": round(1000.0 * seconds / ops, 4) if ops else 0.0,
            "instrumented_mb_s": round(mb / max(seconds, 1e-12), 3) if byte_count else 0.0,
        }

    return {
        "note": (
            f"{prefix}: direct per-operation timing tagged by the facade used for that store. "
            "These are summed instrumented intervals, not phase wall time; local is SHM, "
            "remote is BRPC."
        ),
        "local_shm": _one(local_s, local_bytes, local_ops),
        "remote_brpc": _one(remote_s, remote_bytes, remote_ops),
        "unknown": _one(unknown_s, unknown_bytes, unknown_ops),
    }


def _two_phase_mixed_breakdown(
    acc_store: Dict[str, Any],
    acc_load: Dict[str, Any],
    ipc_after_store: Dict[str, int],
    ipc_end: Dict[str, int],
    cxx_facade_after_store: Dict[str, Any],
    cxx_facade_end: Dict[str, Any],
    *,
    mb: float,
    dt_store_s: float,
    dt_load_s: float,
    n_batches: int,
    n_blocks: int,
) -> Dict[str, Any]:
    """Wall MB/s plus directly measured local-SHM vs remote-BRPC data timings."""
    nb = max(int(n_batches), 1)
    lw = max(int(ipc_after_store.get("local_writes", 0)), 0)
    rw = max(int(ipc_after_store.get("remote_writes", 0)), 0)
    dr = _facade_ipc_sub(ipc_end, ipc_after_store)
    lr = max(int(dr.get("local_reads", 0)), 0)
    rr = max(int(dr.get("remote_reads", 0)), 0)

    store_mb_s = mb / max(dt_store_s, 1e-12)
    load_mb_s = mb / max(dt_load_s, 1e-12)

    dwt = float(acc_store.get("complete_store_data_write_s", 0.0))
    drt = float(acc_load.get("prepare_load_data_read_s", 0.0))
    loc_dw = float(acc_store.get("complete_store_data_write_local_s", 0.0))
    rem_dw = float(acc_store.get("complete_store_data_write_remote_s", 0.0))
    unk_dw = float(acc_store.get("complete_store_data_write_unknown_s", 0.0))
    loc_dr = float(acc_load.get("prepare_load_data_read_local_s", 0.0))
    rem_dr = float(acc_load.get("prepare_load_data_read_remote_s", 0.0))
    unk_dr = float(acc_load.get("prepare_load_data_read_unknown_s", 0.0))
    meta_alloc = float(sum((acc_store.get("meta_allocate_by_dn_s") or {}).values()))
    meta_upd = float(acc_store.get("complete_store_meta_update_status_s", 0.0))
    prep_store_wall = float(acc_store.get("prepare_store_wall_s", 0.0))
    meta_lookup = float(acc_load.get("prepare_load_meta_batch_lookup_s", 0.0))
    meta_renew = float(acc_load.get("complete_load_wall_s", 0.0))
    prep_load_wall = float(acc_load.get("prepare_load_wall_s", 0.0))

    nw = max(int(acc_store.get("complete_store_n_writes", 0)), 1)
    nr = max(int(acc_load.get("prepare_load_n_reads", 0)), 1)
    nw_loc = int(acc_store.get("complete_store_n_writes_local", 0))
    nw_rem = int(acc_store.get("complete_store_n_writes_remote", 0))
    nr_loc = int(acc_load.get("prepare_load_n_reads_local", 0))
    nr_rem = int(acc_load.get("prepare_load_n_reads_remote", 0))

    return {
        "throughput_breakdown_local_remote_mb_s": {
            "note": (
                "wall_end_to_end_mb_s is payload divided by full phase wall clock. "
                "local_shm / remote_brpc instrumented_mb_s are direct per-operation data-path "
                "throughput from Python's store read/write boundary, tagged by facade locality."
            ),
            "store_phase": {
                "wall_end_to_end_mb_s": round(store_mb_s, 3),
                "facade_writes": {"local": lw, "remote": rw},
                "data_path": _path_data_stats(
                    prefix="store write",
                    local_s=loc_dw,
                    local_bytes=int(acc_store.get("complete_store_data_write_local_bytes", 0)),
                    local_ops=nw_loc,
                    remote_s=rem_dw,
                    remote_bytes=int(acc_store.get("complete_store_data_write_remote_bytes", 0)),
                    remote_ops=nw_rem,
                    unknown_s=unk_dw,
                    unknown_bytes=int(acc_store.get("complete_store_data_write_unknown_bytes", 0)),
                    unknown_ops=int(acc_store.get("complete_store_n_writes_unknown", 0)),
                ),
            },
            "load_phase": {
                "wall_end_to_end_mb_s": round(load_mb_s, 3),
                "facade_reads_in_load_phase": {"local": lr, "remote": rr},
                "data_path": _path_data_stats(
                    prefix="load read",
                    local_s=loc_dr,
                    local_bytes=int(acc_load.get("prepare_load_data_read_local_bytes", 0)),
                    local_ops=nr_loc,
                    remote_s=rem_dr,
                    remote_bytes=int(acc_load.get("prepare_load_data_read_remote_bytes", 0)),
                    remote_ops=nr_rem,
                    unknown_s=unk_dr,
                    unknown_bytes=int(acc_load.get("prepare_load_data_read_unknown_bytes", 0)),
                    unknown_ops=int(acc_load.get("prepare_load_n_reads_unknown", 0)),
                ),
            },
        },
        "cxx_facade_latency_throughput": {
            "note": (
                "Lower-level timing inside falconfs_kv_brpc DoFacadeCall around "
                "LocalKVStoreShmFacade / RemoteKVStoreFacade. This excludes Python protobuf "
                "decode and OffloadingManager scheduling, so it is the cleaner SHM-vs-BRPC "
                "facade comparison."
            ),
            "store_phase": {
                "local_writes": cxx_facade_after_store.get("local_writes", {}),
                "remote_writes": cxx_facade_after_store.get("remote_writes", {}),
            },
            "load_phase": {
                "local_reads": cxx_facade_end.get("local_reads", {}),
                "remote_reads": cxx_facade_end.get("remote_reads", {}),
            },
        },
        "latency_breakdown_meta_local_remote_s": {
            "n_batches": nb,
            "n_blocks": int(n_blocks),
            "note": (
                "per_batch_meta_ms: averages over n_batches chunk waves (prepare_store / "
                "prepare_load + complete_load). per_block_meta_ms: meta_update_status (one DN "
                "update per stored block). per_path_data_ms is direct per-operation timing "
                "tagged by facade locality; no count-ratio time attribution is used."
            ),
            "store_phase": {
                "totals_s": {
                    "prepare_store_wall_s": round(prep_store_wall, 6),
                    "meta_allocate_s": round(meta_alloc, 6),
                    "meta_update_status_s": round(meta_upd, 6),
                    "data_write_total_s": round(dwt, 6),
                    "data_write_local_s": round(loc_dw, 6),
                    "data_write_remote_s": round(rem_dw, 6),
                    "data_write_unknown_s": round(unk_dw, 6),
                },
                "per_batch_meta_ms": {
                    "prepare_store_wall_ms": round(1000.0 * prep_store_wall / nb, 4),
                    "meta_allocate_ms": round(1000.0 * meta_alloc / nb, 4),
                },
                "per_block_meta_ms": {
                    "meta_update_status_ms": round(1000.0 * meta_upd / max(nw, 1), 4),
                },
                "per_block_data_ms": {
                    "data_write_instrumented_ms": round(1000.0 * dwt / max(nw, 1), 4),
                },
                "per_path_data_ms": {
                    "local_shm_avg_ms": round(1000.0 * loc_dw / max(nw_loc, 1), 4) if nw_loc else 0.0,
                    "remote_brpc_avg_ms": round(1000.0 * rem_dw / max(nw_rem, 1), 4) if nw_rem else 0.0,
                    "unknown_avg_ms": (
                        round(1000.0 * unk_dw / max(int(acc_store.get("complete_store_n_writes_unknown", 0)), 1), 4)
                        if int(acc_store.get("complete_store_n_writes_unknown", 0))
                        else 0.0
                    ),
                },
            },
            "load_phase": {
                "totals_s": {
                    "prepare_load_wall_s": round(prep_load_wall, 6),
                    "meta_batch_lookup_s": round(meta_lookup, 6),
                    "complete_load_renew_meta_wall_s": round(meta_renew, 6),
                    "data_read_total_s": round(drt, 6),
                    "data_read_local_s": round(loc_dr, 6),
                    "data_read_remote_s": round(rem_dr, 6),
                    "data_read_unknown_s": round(unk_dr, 6),
                },
                "per_batch_meta_ms": {
                    "meta_batch_lookup_ms": round(1000.0 * meta_lookup / nb, 4),
                    "complete_load_renew_wall_ms": round(1000.0 * meta_renew / nb, 4),
                    "prepare_load_wall_ms": round(1000.0 * prep_load_wall / nb, 4),
                },
                "per_block_data_ms": {
                    "data_read_instrumented_ms": round(1000.0 * drt / max(nr, 1), 4),
                },
                "per_path_data_ms": {
                    "local_shm_avg_ms": round(1000.0 * loc_dr / max(nr_loc, 1), 4) if nr_loc else 0.0,
                    "remote_brpc_avg_ms": round(1000.0 * rem_dr / max(nr_rem, 1), 4) if nr_rem else 0.0,
                    "unknown_avg_ms": (
                        round(1000.0 * unk_dr / max(int(acc_load.get("prepare_load_n_reads_unknown", 0)), 1), 4)
                        if int(acc_load.get("prepare_load_n_reads_unknown", 0))
                        else 0.0
                    ),
                },
            },
        },
    }


def _human_detail_enabled() -> bool:
    return os.environ.get("FALCON_MIX_NO_HUMAN_DETAIL", "").strip().lower() not in (
        "1",
        "true",
        "yes",
        "on",
    )


def _metric_value(d: Any, key: str, default: float = 0.0) -> float:
    if not isinstance(d, dict):
        return default
    try:
        return float(d.get(key, default) or default)
    except (TypeError, ValueError):
        return default


def _fmt_num(value: Any, width: int = 12, precision: int = 3) -> str:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return str(value if value is not None else "-").rjust(width)
    if abs(v) >= 1000:
        s = f"{v:.1f}"
    elif abs(v) >= 100:
        s = f"{v:.2f}"
    else:
        s = f"{v:.{precision}f}"
    return s.rjust(width)


def _fmt_int(value: Any, width: int = 8) -> str:
    try:
        return str(int(value)).rjust(width)
    except (TypeError, ValueError):
        return str(value if value is not None else "-").rjust(width)


def _visual_row(label: str, ops: Any, avg_ms: Any, mb_s: Any, seconds: Any) -> str:
    return (
        f"  {label:<20}"
        f"{_fmt_int(ops)}"
        f"{_fmt_num(avg_ms)}"
        f"{_fmt_num(mb_s)}"
        f"{_fmt_num(seconds)}"
    )


def _append_ratio_report(lines: List[str], ratio: Dict[str, Any]) -> None:
    if not isinstance(ratio, dict) or not ratio:
        return
    lines.append("local_remote_ratio:")
    ids = ratio.get("colocated_local_store_ids") or []
    rem_ids = ratio.get("remote_store_ids") or []
    lines.append(f"  stores: local={ids} remote={rem_ids}")
    for label, key in (("block_placement", "block_placement"), ("store_writes", "store_writes"), ("load_reads", "load_reads")):
        r = ratio.get(key)
        if not isinstance(r, dict) or not r:
            continue
        lines.append(
            f"  {label}: local={r.get('local')} ({r.get('local_pct')}%) "
            f"remote={r.get('remote')} ({r.get('remote_pct')}%) "
            f"local/remote={r.get('local_to_remote')}"
        )


def _append_path_perf_report(lines: List[str], perf: Dict[str, Any]) -> None:
    if not isinstance(perf, dict) or not perf:
        return
    for phase, title in (("store_write", "path_store_write"), ("load_read", "path_load_read")):
        blk = perf.get(phase)
        if not isinstance(blk, dict) or not blk:
            continue
        lines.append(f"{title}:")
        lines.append("  path                     ops     rpcs   blk/rpc   avg_ms/block    avg_ms/rpc        MB/s")
        lines.append("  -------------------- -------- -------- --------- -------------- ------------- ------------")
        for label in ("local_shm", "remote_brpc", "overall"):
            row = blk.get(label)
            if not isinstance(row, dict):
                continue
            lines.append(
                f"  {label:<20}"
                f"{_fmt_int(row.get('ops'))}"
                f"{_fmt_int(row.get('rpcs'))}"
                f"{_fmt_num(row.get('blocks_per_rpc'), width=10)}"
                f"{_fmt_num(row.get('avg_ms_per_block'), width=15)}"
                f"{_fmt_num(row.get('avg_ms_per_rpc'), width=14)}"
                f"{_fmt_num(row.get('instrumented_mb_s'))}"
            )
        loc = blk.get("local_shm") if isinstance(blk.get("local_shm"), dict) else {}
        rem = blk.get("remote_brpc") if isinstance(blk.get("remote_brpc"), dict) else {}
        lms = _metric_value(loc, "avg_ms_per_block")
        rms = _metric_value(rem, "avg_ms_per_block")
        if lms > 0 and rms > 0:
            lines.append(f"  remote/local avg block latency: {rms / lms:.2f}x")


def _append_path_table(lines: List[str], title: str, local: Dict[str, Any], remote: Dict[str, Any]) -> None:
    lines.append(title)
    lines.append("  path                     ops   avg_ms/op        MB/s     total_s")
    lines.append("  -------------------- -------- ------------ ------------ ------------")
    lines.append(
        _visual_row(
            "local_shm",
            local.get("ops"),
            local.get("avg_ms_per_op"),
            local.get("instrumented_mb_s"),
            local.get("total_s"),
        )
    )
    lines.append(
        _visual_row(
            "remote_brpc",
            remote.get("ops"),
            remote.get("avg_ms_per_op"),
            remote.get("instrumented_mb_s"),
            remote.get("total_s"),
        )
    )
    local_ms = _metric_value(local, "avg_ms_per_op")
    remote_ms = _metric_value(remote, "avg_ms_per_op")
    if local_ms > 0 and remote_ms > 0:
        lines.append(f"  remote/local latency ratio: {remote_ms / local_ms:.2f}x")


def _append_visual_perf_dashboard(lines: List[str], ev: Dict[str, Any]) -> None:
    tee = ev.get("throughput_end_to_end")
    cxx = ev.get("cxx_facade_latency_throughput")
    tlr = ev.get("throughput_breakdown_local_remote_mb_s")
    latm = ev.get("latency_breakdown_meta_local_remote_s")
    if not any(isinstance(x, dict) and x for x in (tee, cxx, tlr, latm)):
        return

    lines.append("performance_dashboard:")
    if isinstance(tee, dict) and tee:
        lines.append(
            "  end_to_end_wall: "
            f"store={tee.get('mb_s_phase_store')} MB/s over {tee.get('seconds_phase_store')}s, "
            f"load={tee.get('mb_s_phase_load')} MB/s over {tee.get('seconds_phase_load')}s"
        )

    if isinstance(cxx, dict) and cxx:
        sp = cxx.get("store_phase") if isinstance(cxx.get("store_phase"), dict) else {}
        lp = cxx.get("load_phase") if isinstance(cxx.get("load_phase"), dict) else {}
        if isinstance(sp, dict) and (sp.get("local_writes") or sp.get("remote_writes")):
            _append_path_table(
                lines,
                "  cxx_facade_store_write (clean SHM vs BRPC call timing):",
                sp.get("local_writes") or {},
                sp.get("remote_writes") or {},
            )
        if isinstance(lp, dict) and (lp.get("local_reads") or lp.get("remote_reads")):
            _append_path_table(
                lines,
                "  cxx_facade_load_read (clean SHM vs BRPC call timing):",
                lp.get("local_reads") or {},
                lp.get("remote_reads") or {},
            )

    if isinstance(tlr, dict) and tlr:
        sp = tlr.get("store_phase") if isinstance(tlr.get("store_phase"), dict) else {}
        lp = tlr.get("load_phase") if isinstance(tlr.get("load_phase"), dict) else {}
        sd = sp.get("data_path") if isinstance(sp.get("data_path"), dict) else {}
        ld = lp.get("data_path") if isinstance(lp.get("data_path"), dict) else {}
        if isinstance(sd, dict) and (sd.get("local_shm") or sd.get("remote_brpc")):
            _append_path_table(
                lines,
                "  om_store_write (Python OM boundary, includes client-side overhead):",
                sd.get("local_shm") or {},
                sd.get("remote_brpc") or {},
            )
        if isinstance(ld, dict) and (ld.get("local_shm") or ld.get("remote_brpc")):
            _append_path_table(
                lines,
                "  om_load_read (Python OM boundary, includes client-side overhead):",
                ld.get("local_shm") or {},
                ld.get("remote_brpc") or {},
            )

    if isinstance(latm, dict) and latm:
        store = latm.get("store_phase") if isinstance(latm.get("store_phase"), dict) else {}
        load = latm.get("load_phase") if isinstance(latm.get("load_phase"), dict) else {}
        smeta_b = store.get("per_batch_meta_ms") if isinstance(store.get("per_batch_meta_ms"), dict) else {}
        smeta_blk = store.get("per_block_meta_ms") if isinstance(store.get("per_block_meta_ms"), dict) else {}
        lmeta_b = load.get("per_batch_meta_ms") if isinstance(load.get("per_batch_meta_ms"), dict) else {}
        lines.append("  metadata_latency:")
        lines.append(
            "    store_prepare_batch_ms="
            f"{smeta_b.get('prepare_store_wall_ms', 0)} "
            f"meta_allocate_batch_ms={smeta_b.get('meta_allocate_ms', 0)} "
            f"update_status_block_ms={smeta_blk.get('meta_update_status_ms', 0)}"
        )
        lines.append(
            "    load_lookup_batch_ms="
            f"{lmeta_b.get('meta_batch_lookup_ms', 0)} "
            f"lease_renew_batch_ms={lmeta_b.get('complete_load_renew_wall_ms', 0)} "
            f"prepare_load_batch_wall_ms={lmeta_b.get('prepare_load_wall_ms', 0)}"
        )


def _format_mixed_human_report(ev: Dict[str, Any]) -> str:
    """Multi-line summary: throughput, phase latency aggregates, OM last-chunk, facade IPC."""
    lines: List[str] = []
    name = ev.get("test", "?")
    lines.append(f"--- FALCON_MIX_DETAIL test={name} ---")

    mid = ev.get("mixed_facade_identity")
    if isinstance(mid, dict) and any(mid.values()):
        lines.append("mixed_facade_identity: " + json.dumps(mid, sort_keys=True))

    tee = ev.get("throughput_end_to_end")
    if isinstance(tee, dict) and tee:
        lines.append(
            "throughput_end_to_end: "
            f"write_store_phase_s={tee.get('seconds_phase_store')} read_load_phase_s={tee.get('seconds_phase_load')} "
            f"write_mb_s={tee.get('mb_s_phase_store')} read_mb_s={tee.get('mb_s_phase_load')}"
        )
    elif ev.get("mb_s_complete_store") is not None or ev.get("mb_s_prepare_load") is not None:
        lines.append(
            "throughput_end_to_end: "
            f"write_complete_store_mb_s={ev.get('mb_s_complete_store')} "
            f"read_prepare_load_mb_s={ev.get('mb_s_prepare_load')} "
            f"write_s={ev.get('seconds_complete_store')} read_s={ev.get('seconds_prepare_load')}"
        )
    if (isinstance(tee, dict) and tee) or ev.get("mb_s_complete_store") is not None or ev.get("mb_s_prepare_load") is not None:
        if "keys" in ev or "bytes_total" in ev:
            lines.append(
                f"  payload: keys={ev.get('keys')} bytes_total={ev.get('bytes_total')} "
                f"kv_block_bytes={ev.get('kv_block_bytes')}"
            )

    ps = ev.get("perf_breakdown_store_phase")
    if isinstance(ps, dict) and ps.get("complete_store_wall_s") is not None:
        lines.append(
            "latency_store_phase (aggregated across chunks): "
            f"prepare_store_wall_s={ps.get('prepare_store_wall_s')} "
            f"meta_allocate_total_s={ps.get('meta_allocate_total_s')} "
            f"complete_store_wall_s={ps.get('complete_store_wall_s')} "
            f"complete_store_data_write_s={ps.get('complete_store_data_write_s')} "
            f"complete_store_meta_update_status_s={ps.get('complete_store_meta_update_status_s')} "
            f"n_writes={ps.get('complete_store_n_writes')} "
            f"write_rpcs={ps.get('complete_store_data_write_rpcs')} "
            f"avg_data_write_ms={ps.get('avg_data_write_ms')} "
            f"avg_data_write_rpc_ms={ps.get('avg_data_write_rpc_ms')} "
            f"avg_meta_update_ms={ps.get('avg_meta_update_ms')}"
        )
        pbm = ps.get("per_batch_meta_ms")
        if isinstance(pbm, dict) and pbm:
            lines.append(
                f"  store per_batch_meta_ms (n_batches={ps.get('n_batches')}): "
                + json.dumps(pbm, sort_keys=True)
            )
        pbm_blk = ps.get("per_block_meta_ms")
        if isinstance(pbm_blk, dict) and pbm_blk:
            lines.append("  store per_block_meta_ms: " + json.dumps(pbm_blk, sort_keys=True))
        pbd = ps.get("per_block_data_ms")
        if isinstance(pbd, dict) and pbd:
            lines.append("  store per_block_data_ms: " + json.dumps(pbd, sort_keys=True))

    pl = ev.get("perf_breakdown_load_phase")
    if isinstance(pl, dict) and pl.get("prepare_load_wall_s") is not None:
        lines.append(
            "latency_load_phase (aggregated across chunks): "
            f"prepare_load_wall_s={pl.get('prepare_load_wall_s')} "
            f"prepare_load_meta_batch_lookup_s={pl.get('prepare_load_meta_batch_lookup_s')} "
            f"prepare_load_data_read_s={pl.get('prepare_load_data_read_s')} "
            f"n_reads={pl.get('prepare_load_n_reads')} "
            f"read_rpcs={pl.get('prepare_load_data_read_rpcs')} "
            f"complete_load_wall_s={pl.get('complete_load_wall_s')} "
            f"avg_data_read_ms={pl.get('avg_data_read_ms')} "
            f"avg_data_read_rpc_ms={pl.get('avg_data_read_rpc_ms')}"
        )
        pbm = pl.get("per_batch_meta_ms")
        if isinstance(pbm, dict) and pbm:
            lines.append(
                f"  load per_batch_meta_ms (n_batches={pl.get('n_batches')}): "
                + json.dumps(pbm, sort_keys=True)
            )
        pbd = pl.get("per_block_data_ms")
        if isinstance(pbd, dict) and pbd:
            lines.append("  load per_block_data_ms: " + json.dumps(pbd, sort_keys=True))

    om = ev.get("om_perf_breakdown")
    if not isinstance(om, dict) or not om:
        om = ev.get("om_perf")
    if isinstance(om, dict) and om:
        lines.append("om_perf (last chunk or per-test snapshot):")
        for phase in ("prepare_store", "complete_store", "prepare_load", "complete_load"):
            blk = om.get(phase)
            if not isinstance(blk, dict) or not blk:
                continue
            keys = (
                "wall_s",
                "data_write_s",
                "data_read_s",
                "meta_batch_lookup_s",
                "meta_update_status_s",
                "n_writes_instrumented",
                "data_write_rpcs",
                "data_write_remote_rpcs",
                "write_batching_enabled",
                "write_batch_max_blocks",
                "write_batch_local_max_blocks",
                "write_batch_remote_max_blocks",
                "adaptive_batching_enabled",
                "n_reads",
                "data_read_rpcs",
                "data_read_remote_rpcs",
                "read_batching_enabled",
                "read_batch_max_blocks",
                "read_batch_local_max_blocks",
                "read_batch_remote_max_blocks",
                "n_keys",
                "n_specs",
                "waves",
                "wave_chunk_size",
                "data_parallelism_max",
            )
            parts = [f"{k}={blk.get(k)}" for k in keys if blk.get(k) is not None]
            mad = blk.get("meta_allocate_by_dn_s")
            if isinstance(mad, dict) and mad:
                parts.append("meta_allocate_by_dn_s=" + json.dumps(mad, sort_keys=True))
            lines.append(f"  [{phase}] " + " ".join(parts))
        for section in ("metadata_channel_cache", "promote_on_read", "adaptive_batching"):
            blk = om.get(section)
            if isinstance(blk, dict) and blk:
                lines.append(f"  [{section}] " + json.dumps(blk, sort_keys=True))

    ipc = ev.get("facade_ipc_data_path")
    ipc_ok = isinstance(ipc, dict) and "shared_memory_local_reads" in ipc
    if ipc_ok:
        lines.append(
            "facade_data_path (SHM local vs BRPC remote): "
            f"shm_r={ipc.get('shared_memory_local_reads')} shm_w={ipc.get('shared_memory_local_writes')} "
            f"brpc_r={ipc.get('brpc_remote_reads')} brpc_w={ipc.get('brpc_remote_writes')}"
        )
    st = ev.get("facade_ipc_stats")
    if isinstance(st, dict) and not ipc_ok:
        lines.append(
            "facade_ipc_stats: "
            f"local_reads={st.get('local_reads')} local_writes={st.get('local_writes')} "
            f"remote_reads={st.get('remote_reads')} remote_writes={st.get('remote_writes')}"
        )

    _append_ratio_report(lines, ev.get("local_remote_io_ratio") or {})
    _append_path_perf_report(lines, ev.get("path_throughput_latency") or {})

    loc = ev.get("store_locality")
    if isinstance(loc, dict) and loc:
        lines.append("store_locality: " + json.dumps({str(k): v for k, v in sorted(loc.items())}))

    _append_visual_perf_dashboard(lines, ev)

    tlr = ev.get("throughput_breakdown_local_remote_mb_s")
    if isinstance(tlr, dict) and tlr:
        lines.append("throughput_breakdown_local_remote_mb_s: " + json.dumps(tlr, sort_keys=True))
    cxx = ev.get("cxx_facade_latency_throughput")
    if isinstance(cxx, dict) and cxx:
        lines.append("cxx_facade_latency_throughput: " + json.dumps(cxx, sort_keys=True))
    for key in ("local_shm_upper_bound", "remote_brpc_upper_bound", "native_memcpy_bound", "mixed_facade_bound"):
        blk = ev.get(key)
        if isinstance(blk, dict) and blk:
            lines.append(f"{key}: " + json.dumps(blk, sort_keys=True))
    puc = ev.get("path_upper_bound_comparison")
    if isinstance(puc, dict) and puc:
        lines.append("path_upper_bound_comparison: " + json.dumps(puc, sort_keys=True))
    mbu = ev.get("microbench_upper_bound")
    if isinstance(mbu, dict) and mbu:
        lines.append("microbench_upper_bound_legacy: " + json.dumps(mbu, sort_keys=True))
    bc = ev.get("baseline_comparison")
    if isinstance(bc, dict) and bc:
        lines.append("baseline_comparison: " + json.dumps(bc, sort_keys=True))
    latm = ev.get("latency_breakdown_meta_local_remote_s") or ev.get("latency_breakdown_meta_local_remote_est_s")
    if isinstance(latm, dict) and latm:
        lines.append(
            f"latency_breakdown_meta_local_remote_s: n_batches={latm.get('n_batches')} "
            f"n_blocks={latm.get('n_blocks')}"
        )
        for phase in ("store_phase", "load_phase"):
            blk = latm.get(phase)
            if not isinstance(blk, dict):
                continue
            lines.append(f"  [{phase}] totals_s={json.dumps(blk.get('totals_s'), sort_keys=True)}")
            pbm = blk.get("per_batch_meta_ms")
            if isinstance(pbm, dict) and pbm:
                lines.append(f"    per_batch_meta_ms: {json.dumps(pbm, sort_keys=True)}")
            pbm2 = blk.get("per_block_meta_ms")
            if isinstance(pbm2, dict) and pbm2:
                lines.append(f"    per_block_meta_ms: {json.dumps(pbm2, sort_keys=True)}")
            pbd = blk.get("per_block_data_ms")
            if isinstance(pbd, dict) and pbd:
                lines.append(f"    per_block_data_ms: {json.dumps(pbd, sort_keys=True)}")
            ppd = blk.get("per_path_data_ms")
            if isinstance(ppd, dict) and ppd:
                lines.append(f"    per_path_data_ms: {json.dumps(ppd, sort_keys=True)}")

    spa = ev.get("store_performance_analysis")
    if isinstance(spa, dict) and spa:
        lines.append("store_performance_analysis (latency + throughput):")
        lines.append("  " + json.dumps(spa, sort_keys=True))

    # Keep a simple target/gap line in the persistent human report. A 2 MiB local
    # copy is usually far below 1 ms on this host, so multi-ms per-block data
    # latency should be read as RPC/queueing/Python allocation overhead, not DRAM
    # bandwidth. The exact local bound is measured by kv_store_microbench.py.
    e2e = ev.get("throughput_end_to_end")
    if isinstance(e2e, dict) and e2e:
        st = float(e2e.get("mb_s_phase_store") or 0.0)
        ld = float(e2e.get("mb_s_phase_load") or 0.0)
        lines.append(
            "target_gap: "
            f"store_mb_s={st:.3f}/1500.000 load_mb_s={ld:.3f}/1000.000 "
            f"store_gap_x={(1500.0 / st) if st > 0 else 0.0:.2f} "
            f"load_gap_x={(1000.0 / ld) if ld > 0 else 0.0:.2f}"
        )
    lpa = ev.get("load_performance_analysis")
    if isinstance(lpa, dict) and lpa:
        lines.append("load_performance_analysis (batch lookup vs per-block data + throughput):")
        lines.append("  " + json.dumps(lpa, sort_keys=True))

    lines.append("--- end FALCON_MIX_DETAIL ---")
    return "\n".join(lines)


def _persist_latest_metric_display(events: List[Dict[str, Any]]) -> None:
    if not events:
        return
    out_dir = Path(os.environ.get("FALCON_MIX_METRICS_DIR", str(ROOT.parent / "logs")))
    out_dir.mkdir(parents=True, exist_ok=True)
    latest_json = out_dir / "falcon_mix_metrics_latest.json"
    latest_txt = out_dir / "falcon_mix_metrics_latest.txt"
    with latest_json.open("w", encoding="utf-8") as f:
        json.dump(events, f, indent=2, ensure_ascii=False)
    chunks = []
    for ev in events:
        chunks.append(_format_mixed_human_report(ev))
    latest_txt.write_text("\n\n".join(chunks) + "\n", encoding="utf-8")


class OffloadingManagerClusterMixedE2E(unittest.TestCase):
    throughput_events: ClassVar[List[Dict[str, Any]]] = []
    _current_facade_node: ClassVar[str] = ""
    _current_client_hostname: ClassVar[str] = ""

    @classmethod
    def setUpClass(cls) -> None:
        if not mixed_topology_available():
            raise unittest.SkipTest(
                "Need CN + 3 DN poolers + 4 store BRPC ports (KV_THREE_DNS=1, STORE_COUNT=4 cluster)"
            )
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        out = os.environ.get("FALCON_MIX_THROUGHPUT_JSON", "").strip()
        if out and cls.throughput_events:
            with open(out, "w", encoding="utf-8") as f:
                json.dump(cls.throughput_events, f, indent=2, ensure_ascii=False)
        _persist_latest_metric_display(cls.throughput_events)
        super().tearDownClass()

    @classmethod
    def _record(cls, event: Dict[str, Any], *, persist_metrics: bool = True) -> None:
        event["ts_wall_ms"] = int(time.time() * 1000)
        event.setdefault("e2e_quick", _e2e_quick_enabled())
        event.setdefault(
            "mixed_facade_identity",
            {
                "NODE_NAME_for_registry": getattr(cls, "_current_facade_node", "") or None,
                "client_hostname": getattr(cls, "_current_client_hostname", "") or None,
            },
        )
        if "throughput_end_to_end" in event and isinstance(event.get("throughput_end_to_end"), dict):
            tee = event["throughput_end_to_end"]
            event.setdefault("write_throughput_mb_s", tee.get("mb_s_phase_store"))
            event.setdefault("read_throughput_mb_s", tee.get("mb_s_phase_load"))
        else:
            event.setdefault("write_throughput_mb_s", event.get("mb_s_complete_store"))
            event.setdefault("read_throughput_mb_s", event.get("mb_s_prepare_load"))
        event.setdefault("local_remote_io_ratio", _derive_local_remote_io_ratio(event))
        event.setdefault("path_throughput_latency", _derive_path_throughput_latency(event))
        if "throughput_end_to_end" in event and isinstance(event.get("throughput_end_to_end"), dict):
            bounds = _microbench_bounds_snapshot()
            event.setdefault("microbench_bounds", bounds)
            for key in _MICROBENCH_BOUND_KEYS:
                if isinstance(bounds.get(key), dict):
                    event.setdefault(key, bounds[key])
            event.setdefault("microbench_upper_bound", _microbench_upper_bound_snapshot())
            path_cmp = _path_upper_bound_comparison(event, bounds)
            event.setdefault("path_upper_bound_comparison", path_cmp)
            _assert_required_upper_bound_comparison(event, bounds, path_cmp)
            baseline_cmp = _baseline_comparison_for_event(event)
            event.setdefault("baseline_comparison", baseline_cmp)
            _enforce_baseline_gate(event, baseline_cmp)
        event.setdefault(
            "recovery_stress",
            {
                "large_dn_recovery_unit": "KVMetadataRecovery.LargeDnRecoveryStressRestoresRowsAndBitmap",
                "late_store_registration_unit": "KVMetadataRecovery.LateStoreRegistrationParksAndReplaysRows",
                "dn_restart_evicting_rollback_unit": "KVMetadataRecovery.RestoresRowsAndReconcilesEvicting",
                "store_restart_validation_cluster": "kv-cluster-fault-test store-restart-reconcile",
                "store_restart_matrix": [
                    "ALLOCATED_deleted",
                    "STORED_deleted",
                    "EVICTING_deleted",
                    "EVICTED_empty_path_deleted",
                    "EVICTED_missing_path_deleted_after_validation",
                    "EVICTED_valid_path_preserved",
                ],
                "scanned": None,
                "parked": None,
                "replayed": None,
                "evicting_rolled_back": None,
                "invalid_deleted": None,
                "validation_failed": None,
                "profile": os.environ.get("REGRESSION_PROFILE", ""),
                "note": "Cluster scenario prints concrete Store restart counters; unit tests cover large DN recovery and late Store registration.",
            },
        )
        event.setdefault(
            "hot_path_integrity_policy",
            {
                "store_compute_checksums": os.environ.get("FALCON_KV_STORE_COMPUTE_CHECKSUMS", "0(default:off)"),
                "reason": "DRAM hot-path checksums are opt-in; explicit verify_checksum still computes and validates.",
            },
        )
        event.setdefault(
            "copy_optimization_policy",
            {
                "batch_read_response": "BRPC attachment frames are appended directly from protobuf result payloads.",
                "batch_write_request": "BRPC attachment payloads are passed directly to KVStoreEngine without rebuilding protobuf payloads.",
            },
        )
        if not persist_metrics:
            print(
                "FALCON_MIX_CHECK\t" + json.dumps(event, ensure_ascii=False),
                flush=True,
            )
            return
        cls.throughput_events.append(event)
        print(
            "FALCON_MIX_RUN_SUMMARY\t" + json.dumps(event, ensure_ascii=False),
            flush=True,
        )
        if _human_detail_enabled():
            print(_format_mixed_human_report(event), flush=True)

    def setUp(self) -> None:
        from falconfs_kv.offloading_manager import FalconFSOffloadingManager  # noqa: WPS433

        self.cn_conninfo = os.environ.get(
            "FALCON_KV_CN_CONNINFO",
            f"host=127.0.0.1 port=55500 dbname=postgres user={os.environ.get('USER', 'postgres')}",
        )
        _prime_dn_catalog_for_mixed_e2e(self.cn_conninfo)
        self._prefix = f"py_omix_{int(time.time() * 1000)}_{id(self)}"
        self._kv_block_bytes = _mixed_workload_block_bytes()
        self._facade_node = _facade_local_node_name_for_mixed_e2e()
        if self._facade_node:
            os.environ["NODE_NAME"] = self._facade_node
        else:
            os.environ.pop("NODE_NAME", None)
        self._client_hostname = self._facade_node or "py-mixed-e2e-host"
        type(self)._current_facade_node = self._facade_node
        type(self)._current_client_hostname = self._client_hostname
        try:
            self.mgr = FalconFSOffloadingManager(
                client_id=5150,
                client_hostname=self._client_hostname,
                mode="cluster",
                block_size=self._kv_block_bytes,
                timeout_ms=45000,
                cn_conninfo=self.cn_conninfo,
            )
        except RuntimeError as exc:
            if "discovered no DN endpoints" not in str(exc):
                raise
            fallback_shards = _mixed_e2e_live_dn_shard_table()
            if len(fallback_shards) < 3:
                raise
            self.mgr = FalconFSOffloadingManager(
                client_id=5150,
                client_hostname=self._client_hostname,
                shard_table=fallback_shards,
                mode="cluster",
                block_size=self._kv_block_bytes,
                timeout_ms=45000,
                cn_conninfo=self.cn_conninfo,
            )
        self.shard_table = dict(self.mgr.shard_table)
        self.assertGreaterEqual(
            len(self.shard_table),
            3,
            f"expected ≥3 DNs in shard_table, got {self.shard_table!r}",
        )

    def _key(self, name: str) -> str:
        return f"{self._prefix}_{name}"

    def _cleanup(self, keys: List[str]) -> None:
        for k in keys:
            loc = self.mgr.local_cache.get(k)
            if loc is None:
                continue
            cluster = self.mgr._clusters.get(loc.dn_id)
            if cluster is None:
                continue
            try:
                cluster.metadata.free_allocated(
                    k,
                    expected_version=loc.version,
                    force=True,
                    request_id=f"{self._prefix}_cleanup_{k}",
                    client_id=self.mgr.client_id,
                )
            except Exception:
                pass

    def test_routing_touches_at_least_three_dns(self) -> None:
        keys = [self._key(f"r{i}") for i in range(72)]
        grouping = self.mgr._group_by_dn(keys)
        self.assertGreaterEqual(
            len(grouping),
            3,
            f"expected keys to span ≥3 DNs; got dn_ids={sorted(grouping.keys())}",
        )
        type(self)._record(
            {
                "test": "routing_three_dns",
                "distinct_dns": len(grouping),
                "store_locality": _store_locality_snapshot(),
                "facade_ipc_stats": _facade_ipc_snapshot(),
            },
            persist_metrics=False,
        )

    def test_batch_store_load_touch_full_block_three_dns(self) -> None:
        n = _mixed_batch_touch_key_count()
        keys = [self._key(f"b{i}") for i in range(n)]
        bs = self.mgr.block_size
        payload = {k: _mixed_payload(bs, variant=i) for i, k in enumerate(keys)}
        nbytes = sum(len(v) for v in payload.values())
        from falconfs_kv import falconfs_kv_brpc as _b_rpc

        self.mgr.set_om_perf_enabled(True)
        _b_rpc.facade_ipc_stats_reset()
        try:
            bs = self.mgr.block_size
            miss = self.mgr.batch_lookup(keys, None)
            for k in keys:
                self.assertFalse(miss[k])

            t0 = time.perf_counter()
            spec = self.mgr.prepare_store(keys, None)
            t_prepare = time.perf_counter() - t0
            self.assertIsNotNone(spec)
            self.assertEqual({s["block_hash"] for s in spec.specs}, set(keys))

            t1 = time.perf_counter()
            self.mgr.complete_store(keys, payload, None, success=True)
            t_store = time.perf_counter() - t1

            hits = self.mgr.batch_lookup(keys, None)
            for k in keys:
                self.assertTrue(hits.get(k), k)

            t2 = time.perf_counter()
            ld = self.mgr.prepare_load(keys, None)
            t_load = time.perf_counter() - t2
            for k in keys:
                self.assertEqual(ld.data.get(k), payload[k], k)

            self.mgr.complete_load(keys, None)
            self.mgr.touch(keys, None)

            mb = nbytes / 1.0e6
            ipc_after = _facade_ipc_snapshot()
            type(self)._record(
                {
                    "test": "batch_store_load_touch_multi",
                    "keys": len(keys),
                    "kv_block_bytes": bs,
                    "vllm_formula_kv_bytes": _VLLM_FORMULA_KV_BYTES,
                    "bytes_total": nbytes,
                    "payloads_pregenerated_outside_phase_timers": True,
                    "seconds_prepare_store": round(t_prepare, 6),
                    "seconds_complete_store": round(t_store, 6),
                    "seconds_prepare_load": round(t_load, 6),
                    "mb_s_complete_store": round(mb / t_store, 3) if t_store > 0 else None,
                    "mb_s_prepare_load": round(mb / t_load, 3) if t_load > 0 else None,
                    "store_locality": _store_locality_snapshot(),
                    "facade_ipc_stats": ipc_after,
                    "facade_ipc_data_path": _facade_ipc_data_path_report(),
                    "om_perf_breakdown": _om_perf_phases(self.mgr),
                },
                persist_metrics=False,
            )
            floor = _require_store_mb_s()
            if floor > 0 and t_store > 0:
                self.assertGreaterEqual(
                    mb / t_store,
                    floor,
                    f"complete_store MB/s below FALCON_MIX_REQUIRE_STORE_MB_S={floor}",
                )
        finally:
            self.mgr.set_om_perf_enabled(_om_perf_enabled())
            self._cleanup(keys)

    def test_facade_ipc_stats_track_batch_store_and_load(self) -> None:
        """Each successful facade read/write bumps C++ counters (local or remote)."""
        from falconfs_kv import falconfs_kv_brpc as b

        loc0 = _store_locality_snapshot()
        self.mgr.set_om_perf_enabled(True)
        b.facade_ipc_stats_reset()
        keys = [self._key(f"ipc{i}") for i in range(3)]
        bs = self.mgr.block_size
        payload = {k: _mixed_payload(bs, variant=i) for i, k in enumerate(keys)}
        nbytes = sum(len(v) for v in payload.values())
        try:
            self.assertFalse(any(self.mgr.batch_lookup(keys, None).values()))
            t0 = time.perf_counter()
            self.assertIsNotNone(self.mgr.prepare_store(keys, None))
            t_ps = time.perf_counter() - t0
            t1 = time.perf_counter()
            self.mgr.complete_store(keys, payload, None, success=True)
            t_cs = time.perf_counter() - t1
            lr0, lw0, rr0, rw0 = b.facade_ipc_stats()
            self.assertGreaterEqual(lw0 + rw0, 3, "three facade writes expected")
            t2 = time.perf_counter()
            ld = self.mgr.prepare_load(keys, None)
            t_pl = time.perf_counter() - t2
            for k in keys:
                self.assertEqual(ld.data.get(k), payload[k])
            lr1, lw1, rr1, rw1 = b.facade_ipc_stats()
            self.assertGreaterEqual((lr1 - lr0) + (rr1 - rr0), 3, "three facade reads expected")
            self.mgr.complete_load(keys, None)
            mb = nbytes / 1.0e6
            ipc = _facade_ipc_snapshot()
            type(self)._record(
                {
                    "test": "facade_ipc_batch_3keys",
                    "kv_block_bytes": bs,
                    "vllm_formula_kv_bytes": _VLLM_FORMULA_KV_BYTES,
                    "store_locality": loc0,
                    "facade_ipc_stats": ipc,
                    "facade_ipc_data_path": _facade_ipc_data_path_report(),
                    "facade_after_write": {"lr": int(lr0), "lw": int(lw0), "rr": int(rr0), "rw": int(rw0)},
                    "facade_after_read": {"lr": int(lr1), "lw": int(lw1), "rr": int(rr1), "rw": int(rw1)},
                    "local_writes": int(lw0),
                    "remote_writes": int(rw0),
                    "local_read_delta": int(lr1 - lr0),
                    "remote_read_delta": int(rr1 - rr0),
                    "seconds_prepare_store": round(t_ps, 6),
                    "seconds_complete_store": round(t_cs, 6),
                    "seconds_prepare_load": round(t_pl, 6),
                    "mb_s_complete_store": round(mb / t_cs, 3) if t_cs > 0 else None,
                    "mb_s_prepare_load": round(mb / t_pl, 3) if t_pl > 0 else None,
                    "om_perf_breakdown": _om_perf_phases(self.mgr),
                },
                persist_metrics=False,
            )
        finally:
            self.mgr.set_om_perf_enabled(_om_perf_enabled())
            self._cleanup(keys)

    def test_batch_throughput_local_vs_remote_facade_split(self) -> None:
        """Larger batch: classify blocks by local vs remote store from ``store_locality()``."""
        from falconfs_kv import falconfs_kv_brpc as b

        loc = _store_locality_snapshot()
        nk = _mixed_throughput_num_keys()
        keys = [self._key(f"tp{i}") for i in range(nk)]
        bs = self.mgr.block_size
        payload = {k: _mixed_payload(bs, variant=i) for i, k in enumerate(keys)}
        nbytes = sum(len(v) for v in payload.values())
        b.facade_ipc_stats_reset()
        self.mgr.set_om_perf_enabled(True)
        try:
            self.assertFalse(any(self.mgr.batch_lookup(keys, None).values()))
            t0 = time.perf_counter()
            spec = self.mgr.prepare_store(keys, None)
            t_ps = time.perf_counter() - t0
            om_ps = dict(self.mgr.perf_breakdown().get("prepare_store", {}))
            self.assertIsNotNone(spec)
            assert spec is not None
            by_sid: Dict[str, int] = {}
            for s in spec.specs:
                h = s["block_hash"]
                by_sid[h] = int(s["store_id"])
            store_id_distribution: Dict[str, int] = {}
            for sid in by_sid.values():
                store_id_distribution[str(sid)] = store_id_distribution.get(str(sid), 0) + 1
            t1 = time.perf_counter()
            self.mgr.complete_store(keys, payload, None, success=True)
            t_cs = time.perf_counter() - t1
            om_cs = dict(self.mgr.perf_breakdown().get("complete_store", {}))
            lr0, lw0, rr0, rw0 = b.facade_ipc_stats()
            t2 = time.perf_counter()
            ld = self.mgr.prepare_load(keys, None)
            t_pl = time.perf_counter() - t2
            om_pl = dict(self.mgr.perf_breakdown().get("prepare_load", {}))
            for k in keys:
                self.assertEqual(ld.data[k], payload[k])
            lr1, lw1, rr1, rw1 = b.facade_ipc_stats()
            self.mgr.complete_load(keys, None)
            om_cl = dict(self.mgr.perf_breakdown().get("complete_load", {}))

            local_blocks = 0
            remote_blocks = 0
            for k in keys:
                sid = by_sid.get(k, 0)
                is_local = bool(loc.get(sid))
                if is_local:
                    local_blocks += 1
                else:
                    remote_blocks += 1
            self.assertGreater(local_blocks + remote_blocks, 0)
            self.assertGreaterEqual(lw0 + rw0, len(keys))
            self.assertGreaterEqual((lr1 - lr0) + (rr1 - rr0), len(keys))

            mb = nbytes / 1.0e6
            ipc_final = _facade_ipc_snapshot()
            ev: Dict[str, Any] = {
                "test": "throughput_local_remote_split",
                "keys": len(keys),
                "kv_block_bytes": bs,
                "vllm_formula_kv_bytes": _VLLM_FORMULA_KV_BYTES,
                "client_data_parallelism_max": int(self.mgr._data_parallelism_max),
                "client_meta_parallelism_max": int(self.mgr._meta_parallelism_max),
                "blocks_local_store": local_blocks,
                "blocks_remote_store": remote_blocks,
                "store_id_distribution": store_id_distribution,
                "store_locality": loc,
                "facade_ipc_stats": ipc_final,
                "facade_ipc_data_path": _facade_ipc_data_path_report(),
                "facade_after_write": {"lr": int(lr0), "lw": int(lw0), "rr": int(rr0), "rw": int(rw0)},
                "facade_after_read": {"lr": int(lr1), "lw": int(lw1), "rr": int(rr1), "rw": int(rw1)},
                "seconds_prepare_store": round(t_ps, 6),
                "seconds_complete_store": round(t_cs, 6),
                "seconds_prepare_load": round(t_pl, 6),
                "mb_s_complete_store": round(mb / t_cs, 3) if t_cs > 0 else None,
                "mb_s_prepare_load": round(mb / t_pl, 3) if t_pl > 0 else None,
                "bytes_total": nbytes,
                "payloads_pregenerated_outside_phase_timers": True,
                "om_perf": {
                    "prepare_store": om_ps,
                    "complete_store": om_cs,
                    "prepare_load": om_pl,
                    "complete_load": om_cl,
                },
                "om_perf_breakdown": _om_perf_phases(self.mgr),
            }
            type(self)._record(ev)
            floor = _require_store_mb_s()
            if floor > 0 and t_cs > 0:
                self.assertGreaterEqual(
                    mb / t_cs,
                    floor,
                    f"complete_store MB/s below FALCON_MIX_REQUIRE_STORE_MB_S={floor}",
                )
        finally:
            self.mgr.set_om_perf_enabled(_om_perf_enabled())
            self._cleanup(keys)

    def test_y_flagged_adaptive_zero_copy_small_e2e(self) -> None:
        """Small mixed-cluster E2E with adaptive batching and zero-copy reads enabled."""
        from falconfs_kv.offloading_manager import FalconFSOffloadingManager  # noqa: WPS433

        old_env = {
            k: os.environ.get(k)
            for k in (
                "FALCON_KV_CLIENT_ADAPTIVE_BATCHING",
                "FALCON_KV_CLIENT_ZERO_COPY_READS",
            )
        }
        os.environ["FALCON_KV_CLIENT_ADAPTIVE_BATCHING"] = "1"
        os.environ["FALCON_KV_CLIENT_ZERO_COPY_READS"] = "1"
        _prime_dn_catalog_for_mixed_e2e(self.cn_conninfo)
        mgr_kwargs = dict(
            client_id=5151,
            client_hostname=self._client_hostname,
            mode="cluster",
            block_size=self._kv_block_bytes,
            timeout_ms=45000,
            cn_conninfo=self.cn_conninfo,
        )
        try:
            mgr = FalconFSOffloadingManager(**mgr_kwargs)
        except RuntimeError as exc:
            if "discovered no DN endpoints" not in str(exc):
                raise
            fallback_shards = _mixed_e2e_live_dn_shard_table()
            if len(fallback_shards) < 3:
                raise
            mgr = FalconFSOffloadingManager(shard_table=fallback_shards, **mgr_kwargs)
        mgr.set_om_perf_enabled(True)
        keys = [self._key(f"flagged{i}") for i in range(24)]
        try:
            payload = {k: _mixed_payload(self._kv_block_bytes, variant=i) for i, k in enumerate(keys)}
            spec = mgr.prepare_store(keys, None)
            self.assertIsNotNone(spec)
            mgr.complete_store(keys, payload, None, success=True)
            ld = mgr.prepare_load(keys, None)
            for k in keys:
                self.assertEqual(bytes(ld.data[k]), payload[k])
            bd = mgr.perf_breakdown()
            zc = bd.get("zero_copy_read") or {}
            ab = bd.get("adaptive_batching") or {}
            self.assertTrue(zc.get("enabled"), zc)
            self.assertGreaterEqual(int(zc.get("zero_copy_blocks", 0)), len(keys))
            self.assertTrue(ab.get("enabled"), ab)
            mgr.complete_load(keys, None)
            type(self)._record(
                {
                    "test": "flagged_adaptive_zero_copy_small_e2e",
                    "keys": len(keys),
                    "kv_block_bytes": self._kv_block_bytes,
                    "om_perf_breakdown": _om_perf_phases(mgr),
                    "store_locality": _store_locality_snapshot(),
                    "facade_ipc_stats": _facade_ipc_snapshot(),
                },
                persist_metrics=False,
            )
        finally:
            for k in keys:
                loc = mgr.local_cache.get(k)
                if loc is None:
                    continue
                cluster = mgr._clusters.get(loc.dn_id)
                if cluster is None:
                    continue
                try:
                    cluster.metadata.free_allocated(
                        k,
                        expected_version=loc.version,
                        force=True,
                        request_id=f"{self._prefix}_cleanup_{k}",
                        client_id=mgr.client_id,
                    )
                except Exception:
                    pass
            mgr._data_stage_pool.shutdown(wait=False, cancel_futures=True)
            mgr._meta_stage_pool.shutdown(wait=False, cancel_futures=True)
            for k, v in old_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_z_two_phase_throughput_store_then_load(self) -> None:
        """Store phase then load phase; throughput + OM breakdown + SHM vs BRPC facade counts.

        Runs by default when mixed topology is available. Opt out with
        ``FALCON_MIX_TWO_PHASE_THROUGHPUT=0`` (or ``false`` / ``no`` / ``off``).
        """
        if _two_phase_throughput_disabled():
            self.skipTest(
                "two-phase throughput disabled (set FALCON_MIX_TWO_PHASE_THROUGHPUT=0/false/off to opt out)"
            )
        nk_req = _phase_keys_requested()
        chunk = _phase_chunk_keys()
        bs = self.mgr.block_size
        sum_dram = _total_healthy_store_dram_bytes(self.cn_conninfo)
        min_dram = _min_healthy_store_dram_bytes(self.cn_conninfo)
        dram_capacity_source = "catalog"
        if sum_dram <= 0:
            fallback = _env_store_dram_capacity_fallback(bs)
            sum_dram = int(fallback["sum"])
            min_dram = int(fallback["min"])
            dram_capacity_source = "env_fallback"
        nk_cap = sum_dram // bs if bs > 0 else 0
        if nk_cap < 1:
            self.skipTest(
                f"sum(dram_pool_bytes)={sum_dram} cannot hold one block at block_size={bs}"
            )
        nk = min(nk_req, nk_cap)
        if nk < 16:
            self.skipTest(
                f"effective phase keys {nk} too small after capping "
                f"requested={nk_req} to catalog cap={nk_cap}"
            )
        keys = [self._key(f"2ph{i}") for i in range(nk)]
        pregen_cap = _phase_pregenerate_payload_max_bytes()
        pregen_payloads: Optional[Dict[str, bytes]] = None
        pregen_required_bytes = nk * bs if bs > 0 else 0
        if bs > 0 and pregen_required_bytes <= pregen_cap:
            pregen_payloads = {k: _mixed_payload(bs, variant=i) for i, k in enumerate(keys)}
        pool_mb_raw = os.environ.get("FALCON_POOL_SHMEM_MB", "").strip()
        pool_ev: Any = int(pool_mb_raw) if pool_mb_raw.isdigit() else (pool_mb_raw or None)
        dram_floor = int(os.environ.get("FALCON_KV_STORE_MIN_LOGICAL_SLOTS", "0") or 0)
        self.mgr.set_om_perf_enabled(True)
        acc_store = _perf_store_acc_new()
        acc_load = _perf_load_acc_new()
        from falconfs_kv import falconfs_kv_brpc as b2

        b2.facade_ipc_stats_reset()
        if hasattr(b2, "facade_perf_stats_reset"):
            b2.facade_perf_stats_reset()
        try:
            nbytes_total = (sum(len(v) for v in pregen_payloads.values()) if pregen_payloads is not None else 0)
            t_store0 = time.perf_counter()
            for off in range(0, nk, chunk):
                sub = keys[off : off + chunk]
                if pregen_payloads is not None:
                    payload = {k: pregen_payloads[k] for k in sub}
                else:
                    payload = {k: _mixed_payload(bs, variant=off + j) for j, k in enumerate(sub)}
                    nbytes_total += sum(len(v) for v in payload.values())
                miss = self.mgr.batch_lookup(sub, None)
                self.assertFalse(any(miss.get(k) for k in sub))
                spec = self.mgr.prepare_store(sub, None)
                self.assertIsNotNone(spec)
                self.mgr.complete_store(sub, payload, None, success=True)
                _perf_merge_store_chunk(acc_store, self.mgr.perf_breakdown())
            t_store1 = time.perf_counter()
            ipc_after_store = _facade_ipc_snapshot()
            cxx_facade_after_store = _facade_perf_snapshot()

            # Brief pause + membership refresh after a long store phase: avoids rare
            # BRPC ``Not connected yet`` on the first load RPC when other tests have
            # already stressed the same client process.
            time.sleep(0.5)
            try:
                self.mgr.refresh_membership(timeout_ms=20000)
            except Exception:
                pass

            t_load0 = time.perf_counter()
            load_measured_s = 0.0
            load_verify_s = 0.0
            for off in range(0, nk, chunk):
                sub = keys[off : off + chunk]
                if pregen_payloads is not None:
                    expected = {k: pregen_payloads[k] for k in sub}
                else:
                    expected = {k: _mixed_payload(bs, variant=off + j) for j, k in enumerate(sub)}
                t_prepare_load0 = time.perf_counter()
                ld = self.mgr.prepare_load(sub, None)
                t_prepare_load1 = time.perf_counter()
                t_verify0 = time.perf_counter()
                for k in sub:
                    self.assertEqual(ld.data.get(k), expected[k], k)
                load_verify_s += time.perf_counter() - t_verify0
                t_complete_load0 = time.perf_counter()
                self.mgr.complete_load(sub, None)
                load_measured_s += (t_prepare_load1 - t_prepare_load0) + (
                    time.perf_counter() - t_complete_load0
                )
                _perf_merge_load_chunk(acc_load, self.mgr.perf_breakdown())
            t_load1 = time.perf_counter()

            mb = nbytes_total / 1.0e6
            dt_s = max(t_store1 - t_store0, 1e-9)
            dl_wall_s = max(t_load1 - t_load0, 1e-9)
            dl_s = max(load_measured_s, 1e-9)
            ipc_end = _facade_ipc_snapshot()
            cxx_facade_end = _facade_perf_snapshot()
            n_batches_e = (nk + chunk - 1) // chunk if nk > 0 else 1
            extra = _two_phase_mixed_breakdown(
                acc_store,
                acc_load,
                ipc_after_store,
                ipc_end,
                cxx_facade_after_store,
                cxx_facade_end,
                mb=mb,
                dt_store_s=dt_s,
                dt_load_s=dl_s,
                n_batches=n_batches_e,
                n_blocks=nk,
            )
            ev: Dict[str, Any] = {
                "test": "two_phase_throughput_store_then_load",
                "phase_keys_requested": nk_req,
                "phase_keys_effective": nk,
                "phase_keys_capped_to_fleet_dram": nk < nk_req,
                "keys": nk,
                "chunk_keys": chunk,
                "kv_block_bytes": bs,
                "vllm_formula_kv_bytes": _VLLM_FORMULA_KV_BYTES,
                "bytes_total": nbytes_total,
                "payloads_pregenerated_outside_phase_timers": pregen_payloads is not None,
                "payload_pregenerate_cap_bytes": pregen_cap,
                "payload_pregenerate_required_bytes": pregen_required_bytes,
                "payload_generation_timed": pregen_payloads is None,
                "catalog_sum_dram_pool_bytes": sum_dram,
                "catalog_min_dram_pool_bytes": min_dram,
                "dram_capacity_source": dram_capacity_source,
                "falcon_pool_shmem_mb_env": pool_ev,
                "falcon_kv_store_min_logical_slots_env": dram_floor,
                "throughput_end_to_end": {
                    "seconds_phase_store": round(dt_s, 6),
                    "seconds_phase_load": round(dl_s, 6),
                    "seconds_phase_load_wall_including_verify": round(dl_wall_s, 6),
                    "seconds_phase_load_verify": round(load_verify_s, 6),
                    "mb_s_phase_store": round(mb / dt_s, 3),
                    "mb_s_phase_load": round(mb / dl_s, 3),
                    "mb_s_phase_load_wall_including_verify": round(mb / dl_wall_s, 3),
                },
                "throughput_breakdown_local_remote_mb_s": extra[
                    "throughput_breakdown_local_remote_mb_s"
                ],
                "cxx_facade_latency_throughput": extra[
                    "cxx_facade_latency_throughput"
                ],
                "latency_breakdown_meta_local_remote_s": extra[
                    "latency_breakdown_meta_local_remote_s"
                ],
                "perf_breakdown_store_phase": _finalize_store_breakdown(
                    acc_store, nbytes_total, n_batches=n_batches_e
                ),
                "perf_breakdown_load_phase": _finalize_load_breakdown(
                    acc_load, nbytes_total, n_batches=n_batches_e
                ),
                "store_performance_analysis": _performance_analysis_store_phase(
                    acc_store, nbytes_total, n_batches_e
                ),
                "load_performance_analysis": _performance_analysis_load_phase(
                    acc_load, nbytes_total, n_batches_e
                ),
                "client_data_parallelism_max": int(self.mgr._data_parallelism_max),
                "client_meta_parallelism_max": int(self.mgr._meta_parallelism_max),
                "store_locality": _store_locality_snapshot(),
                "facade_ipc_stats": ipc_end,
                "facade_ipc_after_store_phase": ipc_after_store,
                "facade_ipc_data_path": _facade_ipc_data_path_report(),
                "facade_perf_stats": cxx_facade_end,
                "facade_perf_after_store_phase": cxx_facade_after_store,
                "om_perf_breakdown": _om_perf_phases(self.mgr),
            }
            type(self)._record(ev)
        finally:
            self.mgr.set_om_perf_enabled(_om_perf_enabled())
            self._cleanup(keys)

    def test_zz_refresh_membership_sees_multiple_stores(self) -> None:
        """Run last: catalog row count can lag briefly after heavy subprocess / two-phase."""
        num_dns, num_stores, gen = 0, 0, 0
        for _attempt in range(25):
            stats = self.mgr.refresh_membership(timeout_ms=12000)
            self.assertEqual(len(stats), 3)
            num_dns, num_stores, gen = int(stats[0]), int(stats[1]), int(stats[2])
            if num_stores >= 4 and num_dns >= 3:
                break
            time.sleep(1.0)
        self.assertGreaterEqual(num_dns, 3)
        self.assertGreaterEqual(num_stores, 4)
        type(self)._record(
            {
                "test": "refresh_membership",
                "num_dns": num_dns,
                "num_stores": num_stores,
                "store_locality": _store_locality_snapshot(),
                "facade_ipc_stats": _facade_ipc_snapshot(),
            },
            persist_metrics=False,
        )

    def test_four_node_name_clients_subprocess_roundtrip(self) -> None:
        """Four processes × ``NODE_NAME=v65mix{0..3}`` — store/read + throughput JSON."""
        child = Path(__file__).resolve().parent / "mixed_topology_offloader_child.py"
        self.assertTrue(child.is_file(), f"missing helper {child}")
        env_base = os.environ.copy()
        env_base["PYTHONPATH"] = f"{ROOT / 'python'}:{ROOT / 'test'}"
        env_base.setdefault("FALCON_KV_STORE_BRPC_ENDPOINT", f"127.0.0.1:{STORE_BASE}")
        env_base["FALCON_MIX_KV_BLOCK_BYTES"] = str(self._kv_block_bytes)
        env_base["FALCON_KV_CN_CONNINFO"] = self.cn_conninfo
        for i in range(4):
            _prime_dn_catalog_for_mixed_e2e(self.cn_conninfo)
            key = self._key(f"subc{i}")
            env = env_base.copy()
            env["NODE_NAME"] = f"v65mix{i}"
            env["FALCON_MIX_KEY"] = key
            env["FALCON_MIX_CLIENT_ID"] = str(9200 + i)
            rc = subprocess.run(
                [sys.executable, str(child)],
                env=env,
                capture_output=True,
                text=True,
                timeout=120,
            )
            self.assertEqual(
                rc.returncode,
                0,
                f"child i={i} failed rc={rc.returncode} stderr={rc.stderr!r} stdout={rc.stdout!r}",
            )
            rep = _parse_child_report(rc.stdout)
            self.assertIsNotNone(rep, f"no FALCON_MIX_REPORT line in stdout={rc.stdout!r}")
            assert rep is not None
            self.assertTrue(rep.get("ok"), rep)
            # Child already validates SHM vs BRPC counters for colocated vs fallback alloc.
            self.assertGreater(rep.get("mb_s_complete_store") or 0, 0, rep)
            self.assertGreater(rep.get("mb_s_prepare_load") or 0, 0, rep)
            rep["test"] = f"subprocess_client_v65mix{i}"
            type(self)._record(rep, persist_metrics=False)


if __name__ == "__main__":
    unittest.main()
