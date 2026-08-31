#!/usr/bin/env python3
"""Standalone helper for ``test_offloading_manager_cluster_mixed_e2e``.

Emits a single machine-readable line on success::

    FALCON_MIX_REPORT\\t{json}

Errors are one JSON object per line on stderr (no ``print()``).
"""

from __future__ import annotations

import json
import os
import re
import socket
import sys
import time
from pathlib import Path


def _fail(msg: str) -> int:
    sys.stderr.write(json.dumps({"ok": False, "error": msg}, ensure_ascii=False) + "\n")
    return 1


def _emit_report(obj: dict) -> int:
    sys.stdout.write("FALCON_MIX_REPORT\t" + json.dumps(obj, ensure_ascii=False) + "\n")
    return 0


def _can_reach(endpoint: str, timeout_s: float = 0.5) -> bool:
    try:
        host, port = endpoint.rsplit(":", 1)
        with socket.create_connection((host, int(port)), timeout=timeout_s):
            return True
    except OSError:
        return False


def _fallback_dn_shard_table() -> dict[int, str]:
    candidates = {
        1: os.environ.get("FALCON_VLLM_SMOKE_DN1_ENDPOINT", "127.0.0.1:55530"),
        2: os.environ.get("FALCON_VLLM_SMOKE_DN2_ENDPOINT", "127.0.0.1:55550"),
        3: os.environ.get("FALCON_VLLM_SMOKE_DN3_ENDPOINT", "127.0.0.1:55570"),
    }
    return {dn_id: ep for dn_id, ep in candidates.items() if _can_reach(ep)}


def main() -> int:
    key = os.environ.get("FALCON_MIX_KEY")
    cn = os.environ.get("FALCON_KV_CN_CONNINFO")
    if not key or not cn:
        return _fail("FALCON_MIX_KEY and FALCON_KV_CN_CONNINFO are required")
    node = os.environ.get("NODE_NAME", "").strip()
    if not node:
        return _fail("NODE_NAME must be set (e.g. v65mix0)")
    try:
        cid = int(os.environ.get("FALCON_MIX_CLIENT_ID", "9000"))
    except ValueError:
        cid = 9000

    store_base = int(os.environ.get("KV_STORE_BRPC_PORT", "18765"))
    os.environ.setdefault("FALCON_KV_STORE_BRPC_ENDPOINT", f"127.0.0.1:{store_base}")
    _default_kv_blk = 2 * 32 * 8 * 128 * 16 * 2
    blk = max(4096, int(os.environ.get("FALCON_MIX_KV_BLOCK_BYTES", str(_default_kv_blk))))

    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root / "python"))

    from falconfs_kv import falconfs_kv_brpc as brpc  # noqa: WPS433
    from falconfs_kv.offloading_manager import FalconFSOffloadingManager  # noqa: WPS433

    t0 = time.perf_counter()
    brpc.facade_ipc_stats_reset()
    mgr_kwargs = dict(
        client_id=cid,
        client_hostname=node,
        mode="cluster",
        block_size=blk,
        timeout_ms=45000,
        cn_conninfo=cn,
    )
    try:
        mgr = FalconFSOffloadingManager(**mgr_kwargs)
    except RuntimeError as exc:
        if "discovered no DN endpoints" not in str(exc):
            raise
        fallback_shards = _fallback_dn_shard_table()
        if len(fallback_shards) < 3:
            raise
        mgr = FalconFSOffloadingManager(shard_table=fallback_shards, **mgr_kwargs)
    t_init = time.perf_counter() - t0

    m = re.fullmatch(r"v65mix(\d+)", node)
    expected_local_store = int(m.group(1)) + 1 if m else 1

    locmap = brpc.store_locality()
    has_local_facade = any(locmap.values())
    # DN-colocated processes see LocalKVStoreShmFacade for their store; standalone
    # Python only has RemoteKVStoreFacade — skip SHM-only gates in that case.
    if has_local_facade and not locmap.get(expected_local_store):
        return _fail(
            f"store_locality: expected store {expected_local_store} local for NODE_NAME={node}, got {locmap!r}"
        )

    miss = mgr.batch_lookup([key], None)
    if miss.get(key):
        return _fail(f"expected miss for {key}")

    t_ps0 = time.perf_counter()
    spec = mgr.prepare_store([key], None)
    t_prepare_store_s = time.perf_counter() - t_ps0
    if not spec or not spec.specs:
        return _fail("prepare_store returned empty")
    sid = int(spec.specs[0]["store_id"])
    payload = bytes([(i + cid) % 256 for i in range(blk)])
    nbytes = len(payload)

    t_cs0 = time.perf_counter()
    mgr.complete_store([key], {key: payload}, None, success=True)
    t_complete_store_s = time.perf_counter() - t_cs0

    lr0, lw0, rr0, rw0 = brpc.facade_ipc_stats()
    if lw0 + rw0 < 1:
        return _fail(f"expected at least one facade write, got lr={lr0} lw={lw0} rr={rr0} rw={rw0}")
    if has_local_facade:
        if sid == expected_local_store and rw0 != 0:
            return _fail(
                f"colocated store {sid}: expected remote_writes=0, got lr={lr0} lw={lw0} rr={rr0} rw={rw0}"
            )
        if sid != expected_local_store and rw0 < 1:
            return _fail(
                f"non-colocated store {sid}: expected remote_writes>=1, got lr={lr0} lw={lw0} rr={rr0} rw={rw0}"
            )

    hit = mgr.batch_lookup([key], None)
    if not hit.get(key):
        return _fail("expected hit after store")

    t_pl0 = time.perf_counter()
    ld = mgr.prepare_load([key], None)
    t_prepare_load_s = time.perf_counter() - t_pl0
    got = ld.data.get(key)
    if got != payload:
        return _fail("load payload mismatch")

    lr1, lw1, rr1, rw1 = brpc.facade_ipc_stats()
    if has_local_facade and sid == expected_local_store:
        if lr1 < lr0 + 1:
            return _fail(
                f"colocated read: expected local_reads to increase, before={(lr0, lw0, rr0, rw0)} after={(lr1, lw1, rr1, rw1)}"
            )
        if rr1 != rr0 or rw1 != rw0:
            return _fail(
                f"colocated read: expected only local read path, before={(lr0, lw0, rr0, rw0)} after={(lr1, lw1, rr1, rw1)}"
            )
    elif has_local_facade:
        if (rr1 - rr0) + (rw1 - rw0) < 1:
            return _fail(
                f"remote store read: expected remote read counters to increase, before={(lr0, lw0, rr0, rw0)} after={(lr1, lw1, rr1, rw1)}"
            )
    else:
        if (lr1 - lr0) + (rr1 - rr0) < 1:
            return _fail(
                f"read: expected facade read counters to increase, before={(lr0, lw0, rr0, rw0)} after={(lr1, lw1, rr1, rw1)}"
            )

    mgr.complete_load([key], None)
    mgr.touch([key], None)

    loc = mgr.local_cache.get(key)
    if loc is not None:
        cluster = mgr._clusters.get(loc.dn_id)
        if cluster is not None:
            cluster.metadata.free_allocated(
                key,
                expected_version=loc.version,
                force=True,
                request_id=f"mixed_child_free_{key}",
                client_id=mgr.client_id,
            )
        mgr.local_cache.pop(key, None)

    mb = nbytes / 1.0e6
    report = {
        "ok": True,
        "node_name": node,
        "client_id": cid,
        "expected_local_store": expected_local_store,
        "allocated_store_id": sid,
        "colocated_alloc": sid == expected_local_store,
        "has_local_facade": has_local_facade,
        "seconds_init": round(t_init, 6),
        "seconds_prepare_store": round(t_prepare_store_s, 6),
        "seconds_complete_store": round(t_complete_store_s, 6),
        "seconds_prepare_load": round(t_prepare_load_s, 6),
        "bytes_per_block": nbytes,
        "kv_block_bytes": blk,
        "vllm_formula_kv_bytes": 2 * 32 * 8 * 128 * 16 * 2,
        "mb_s_complete_store": round(mb / t_complete_store_s, 3) if t_complete_store_s > 0 else None,
        "mb_s_prepare_load": round(mb / t_prepare_load_s, 3) if t_prepare_load_s > 0 else None,
        "facade_ipc_after_write": {"lr": int(lr0), "lw": int(lw0), "rr": int(rr0), "rw": int(rw0)},
        "facade_ipc_after_read": {"lr": int(lr1), "lw": int(lw1), "rr": int(rr1), "rw": int(rw1)},
        "store_locality": {int(k): bool(v) for k, v in locmap.items()},
    }
    return _emit_report(report)


if __name__ == "__main__":
    raise SystemExit(0 if main() == 0 else 1)
