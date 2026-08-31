#!/usr/bin/env python3
"""Direct KV Store data-path micro-benchmark.

This intentionally bypasses OffloadingManager metadata and leases. It writes and
reads fixed Store offsets to estimate path-specific upper bounds: local SHM,
remote BRPC attachment, mixed facade, or pure native memcpy. Payload generation,
metadata allocation, warmup, cleanup, and optional read verification are kept out
of measured windows.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

from falconfs_kv import falconfs_kv_brpc  # noqa: E402
from falconfs_kv.dn_client import BrpcMetadataService  # noqa: E402
from falconfs_kv.store_client import BrpcKVStore, StoreBlockWrite  # noqa: E402

Assignment = Tuple[int, int, int]


def _payload(n: int, variant: int) -> bytes:
    pat = bytes(((i + variant) & 0xFF) for i in range(256))
    return (pat * ((n + 255) // 256))[:n]


def _copy_bound(block_bytes: int, iters: int = 256) -> Dict[str, float]:
    src = bytearray((i & 0xFF) for i in range(block_bytes))
    dst = bytearray(block_bytes)
    for _ in range(min(16, iters)):
        dst[:] = src
    t0 = time.perf_counter()
    for _ in range(iters):
        dst[:] = src
    copy_s = time.perf_counter() - t0
    one = bytes(src)
    t1 = time.perf_counter()
    for _ in range(iters):
        _ = bytes(bytearray(one))
    alloc_s = time.perf_counter() - t1
    bytes_total = float(block_bytes * iters)
    return {
        "copy_ms_per_block": round(1000.0 * copy_s / max(iters, 1), 4),
        "copy_mb_s": round((bytes_total / 1.0e6) / max(copy_s, 1e-12), 3),
        "alloc_copy_ms_per_block": round(1000.0 * alloc_s / max(iters, 1), 4),
        "alloc_copy_mb_s": round((bytes_total / 1.0e6) / max(alloc_s, 1e-12), 3),
    }


def _native_memcpy_bound(block_bytes: int, iters: int) -> Dict[str, Any]:
    if hasattr(falconfs_kv_brpc, "native_memcpy_bound"):
        try:
            out = dict(falconfs_kv_brpc.native_memcpy_bound(int(block_bytes), int(max(1, iters))))
            return {k: round(v, 3) if isinstance(v, float) else v for k, v in out.items()}
        except Exception as exc:
            return {"error": str(exc), **_copy_bound(block_bytes, iters)}
    return {"source": "python_fallback", **_copy_bound(block_bytes, iters)}


def _percentile(vals: List[float], pct: float) -> float:
    if not vals:
        return 0.0
    vals = sorted(vals)
    idx = min(len(vals) - 1, max(0, int(round((pct / 100.0) * (len(vals) - 1)))))
    return vals[idx]


def _summarize_phase(name: str, seconds: List[float], byte_count: int, wall_s: float) -> Dict[str, Any]:
    return {
        "phase": name,
        "ops": len(seconds),
        "bytes": byte_count,
        "wall_s": round(wall_s, 6),
        "wall_mb_s": round((byte_count / 1.0e6) / max(wall_s, 1e-12), 3),
        "avg_ms": round(1000.0 * statistics.mean(seconds), 4) if seconds else 0.0,
        "p50_ms": round(1000.0 * _percentile(seconds, 50), 4),
        "p95_ms": round(1000.0 * _percentile(seconds, 95), 4),
        "p99_ms": round(1000.0 * _percentile(seconds, 99), 4),
        "instrumented_mb_s": round((byte_count / 1.0e6) / max(sum(seconds), 1e-12), 3),
    }


def _phase_from_bound(name: str, blocks: int, block_bytes: int, bound: Dict[str, Any]) -> Dict[str, Any]:
    mb_s = float(bound.get("copy_mb_s", 0.0) or 0.0)
    bytes_total = blocks * block_bytes
    wall_s = (bytes_total / 1.0e6) / max(mb_s, 1e-12)
    per_block = wall_s / max(1, blocks)
    return _summarize_phase(name, [per_block] * max(1, blocks), bytes_total, wall_s)


def _normalize_path_mode(raw: str) -> str:
    mode = (raw or "mixed").strip().lower().replace("_", "-")
    aliases = {
        "local-shm-zero-copy-read": "local-shm-zero-copy-read",
        "local-zero-copy-read": "local-shm-zero-copy-read",
        "local-shm-prealloc-write": "local-shm-prealloc-write",
        "local-prealloc-write": "local-shm-prealloc-write",
        "remote-brpc-attachment": "remote-brpc-attachment",
        "remote-attachment": "remote-brpc-attachment",
        "pure-native-memcpy": "pure-native-memcpy",
        "native-memcpy": "pure-native-memcpy",
        "mixed": "mixed",
    }
    if mode not in aliases:
        raise SystemExit(f"unknown --path-mode {raw!r}")
    return aliases[mode]


def _path_section_name(path_mode: str, locality_summary: Dict[str, Any]) -> str:
    if path_mode == "local-shm-zero-copy-read":
        return "local_shm_upper_bound"
    if path_mode == "local-shm-prealloc-write":
        return "local_shm_prealloc_write_bound"
    if path_mode == "remote-brpc-attachment":
        return "remote_brpc_upper_bound"
    if path_mode == "pure-native-memcpy":
        return "native_memcpy_bound"
    if locality_summary.get("all_local"):
        return "local_shm_upper_bound"
    if locality_summary.get("all_remote"):
        return "remote_brpc_upper_bound"
    return "mixed_facade_bound"


def _chunks_by_store(assignments: Sequence[Assignment], batch_size: int) -> List[List[Assignment]]:
    by_store: Dict[int, List[Assignment]] = {}
    for item in assignments:
        by_store.setdefault(int(item[0]), []).append(item)
    chunks: List[List[Assignment]] = []
    n = max(1, int(batch_size))
    for sid in sorted(by_store):
        items = by_store[sid]
        for i in range(0, len(items), n):
            chunks.append(items[i:i + n])
    return chunks


def _merge_counts(dst: Dict[str, int], src: Dict[str, int]) -> None:
    for k, v in src.items():
        dst[k] = int(dst.get(k, 0)) + int(v)


def _verification_indices(items: Sequence[Assignment], mode: str) -> Iterable[Assignment]:
    if mode == "none":
        return []
    if mode == "sampled":
        if not items:
            return []
        step = max(1, len(items) // 16)
        return [item for pos, item in enumerate(items) if pos % step == 0]
    return items


def _verify_payloads(items: Sequence[Assignment], payloads: Sequence[Any], expected: Sequence[bytes], mode: str) -> Tuple[float, int]:
    verify_items = list(_verification_indices(items, mode))
    if not verify_items:
        return 0.0, 0
    by_idx = {idx: payload for (_sid, _off, idx), payload in zip(items, payloads)}
    t0 = time.perf_counter()
    for sid, off, idx in verify_items:
        payload = by_idx.get(idx, b"")
        if payload != expected[idx]:
            if memoryview(payload) != memoryview(expected[idx]):
                raise RuntimeError(f"read mismatch sid={sid} off={off} idx={idx}")
    return time.perf_counter() - t0, len(verify_items)


def _run_phase(tasks: Sequence[List[Assignment]], fn, parallelism: int) -> Tuple[List[float], float, Dict[str, int], float, int]:
    t0 = time.perf_counter()
    latencies: List[float] = []
    counts: Dict[str, int] = {}
    verify_s = 0.0
    verify_count = 0
    with ThreadPoolExecutor(max_workers=parallelism) as ex:
        futs = [ex.submit(fn, task) for task in tasks]
        for fut in as_completed(futs):
            secs, extra, vs, vc = fut.result()
            latencies.extend(float(x) for x in secs)
            _merge_counts(counts, extra)
            verify_s += float(vs)
            verify_count += int(vc)
    return latencies, time.perf_counter() - t0, counts, verify_s, verify_count


def _locality_summary(stores: Sequence[int], store_locality: Dict[int, bool]) -> Dict[str, Any]:
    selected = {int(s): store_locality.get(int(s)) for s in stores}
    known = [v for v in selected.values() if v is not None]
    return {
        "selected": selected,
        "all_local": bool(known) and len(known) == len(selected) and all(bool(v) for v in known),
        "all_remote": bool(known) and len(known) == len(selected) and not any(bool(v) for v in known),
        "known_count": len(known),
    }


def _enforce_locality(path_mode: str, stores: Sequence[int], locality: Dict[str, Any], require_local_store: int) -> None:
    selected = locality.get("selected") or {}
    if require_local_store:
        if require_local_store not in selected:
            raise SystemExit(f"required local store {require_local_store} is not in --stores")
        if selected.get(require_local_store) is not True:
            raise SystemExit(f"required local store {require_local_store} is not local according to facade registry")
    if os.environ.get("FALCON_KV_MICRO_REQUIRE_LOCAL", "0") == "1":
        if len(stores) != 1 or not locality.get("all_local"):
            raise SystemExit("FALCON_KV_MICRO_REQUIRE_LOCAL=1 requires exactly one selected local Store")
    if path_mode in ("local-shm-zero-copy-read", "local-shm-prealloc-write") and not locality.get("all_local"):
        raise SystemExit(f"{path_mode} requires all selected Stores to be local; locality={selected}")
    if path_mode == "remote-brpc-attachment" and not locality.get("all_remote"):
        raise SystemExit(f"remote-brpc-attachment requires all selected Stores to be remote; locality={selected}")


def _target_analysis(result: Dict[str, Any]) -> Dict[str, Any]:
    native = result.get("native_memcpy_bound") or result.get("copy_bound") or {}
    read = result.get("read") or {}
    write = result.get("write") or {}
    native_mb_s = float(native.get("copy_mb_s", 0.0) or 0.0)
    return {
        "local_shm_target_mb_s": 10000.0,
        "remote_brpc_target_mb_s": 1500.0,
        "read_pct_native_memcpy": round(100.0 * float(read.get("wall_mb_s", 0.0) or 0.0) / native_mb_s, 2) if native_mb_s > 0 else None,
        "write_pct_native_memcpy": round(100.0 * float(write.get("wall_mb_s", 0.0) or 0.0) / native_mb_s, 2) if native_mb_s > 0 else None,
        "measurement_hint": "path-specific; do not compare remote BRPC against DDR memcpy bandwidth",
    }


def _print_table(result: Dict[str, Any]) -> None:
    print("--- KV_STORE_MICROBENCH ---")
    print(
        f"mode={result['mode']} path_mode={result['path_mode']} allocation={result.get('allocation', 'fixed')} "
        f"stores={result['stores']} blocks={result['blocks']} block_bytes={result['block_bytes']} "
        f"parallelism={result['parallelism']} batch_size={result.get('batch_size', 1)} verify={result.get('verification', {}).get('mode', 'full')}"
    )
    for phase in ("write", "read"):
        p = result[phase]
        print(
            f"{phase:>5}: wall={p['wall_s']:.6f}s wall_MB/s={p['wall_mb_s']:.3f} "
            f"avg={p['avg_ms']:.4f}ms p50={p['p50_ms']:.4f}ms "
            f"p95={p['p95_ms']:.4f}ms p99={p['p99_ms']:.4f}ms "
            f"instr_MB/s={p['instrumented_mb_s']:.3f}"
        )
    for key in ("local_shm_upper_bound", "local_shm_prealloc_write_bound", "remote_brpc_upper_bound", "mixed_facade_bound", "native_memcpy_bound"):
        if key in result:
            print(f"{key}=" + json.dumps(result[key], sort_keys=True))
    if result.get("zero_copy_read"):
        print("zero_copy_read=" + json.dumps(result["zero_copy_read"], sort_keys=True))
    if result.get("facade_perf_stats"):
        print("facade_perf_stats=" + json.dumps(result["facade_perf_stats"], sort_keys=True))
    print("--- end KV_STORE_MICROBENCH ---")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cn", default=os.environ.get("FALCON_KV_CN_CONNINFO", "host=127.0.0.1 port=55500 dbname=postgres"))
    ap.add_argument("--endpoint", default=os.environ.get("FALCON_KV_STORE_BRPC_ENDPOINT", "127.0.0.1:18765"))
    ap.add_argument("--metadata-endpoint", default=os.environ.get("FALCON_KV_MICRO_METADATA_ENDPOINT", "127.0.0.1:55530"))
    ap.add_argument("--stores", default=os.environ.get("FALCON_KV_MICRO_STORES", "1,2,3,4"))
    ap.add_argument("--blocks", type=int, default=int(os.environ.get("FALCON_KV_MICRO_BLOCKS", "256")))
    ap.add_argument("--block-bytes", type=int, default=int(os.environ.get("FALCON_KV_MICRO_BLOCK_BYTES", str(2 * 1024 * 1024))))
    ap.add_argument("--parallelism", type=int, default=int(os.environ.get("FALCON_KV_MICRO_PARALLELISM", str((os.cpu_count() or 8) * 2))))
    ap.add_argument("--batch-size", type=int, default=int(os.environ.get("FALCON_KV_MICRO_BATCH_SIZE", "0")), help="0 selects a path-specific default")
    ap.add_argument("--epoch", type=int, default=int(os.environ.get("FALCON_KV_MICRO_STORE_EPOCH", "1")))
    ap.add_argument("--mode", choices=("facade", "direct"), default=os.environ.get("FALCON_KV_MICRO_MODE", "facade"))
    ap.add_argument("--path-mode", default=os.environ.get("FALCON_KV_MICRO_PATH_MODE", "mixed"), help="mixed|local-shm-zero-copy-read|local-shm-prealloc-write|remote-brpc-attachment|pure-native-memcpy")
    ap.add_argument("--require-local-store", type=int, default=int(os.environ.get("FALCON_KV_MICRO_REQUIRE_LOCAL_STORE", "0") or "0"))
    ap.add_argument("--allocation", choices=("metadata", "fixed"), default=os.environ.get("FALCON_KV_MICRO_ALLOCATION", "metadata"), help="metadata allocates valid Store offsets first; fixed writes synthetic offsets")
    ap.add_argument("--key-prefix", default=os.environ.get("FALCON_KV_MICRO_KEY_PREFIX", ""))
    ap.add_argument("--keep-allocations", action="store_true")
    ap.add_argument("--json", default=os.environ.get("FALCON_KV_MICRO_JSON", ""))
    ap.add_argument("--copy-bound-iters", type=int, default=int(os.environ.get("FALCON_KV_MICRO_COPY_BOUND_ITERS", "256")))
    ap.add_argument("--verify", choices=("none", "sampled", "full"), default=os.environ.get("FALCON_KV_MICRO_VERIFY", "full"))
    ap.add_argument("--no-verify", action="store_true", help="deprecated alias for --verify none")
    args = ap.parse_args()

    path_mode = _normalize_path_mode(args.path_mode)
    verify_mode = "none" if args.no_verify else args.verify
    stores = [int(x) for x in args.stores.split(",") if x.strip()]
    if not stores and path_mode != "pure-native-memcpy":
        raise SystemExit("no stores selected")
    batch_size = args.batch_size if args.batch_size > 0 else (8 if path_mode == "local-shm-zero-copy-read" else 1)

    if path_mode == "pure-native-memcpy":
        native = _native_memcpy_bound(args.block_bytes, max(1, args.copy_bound_iters))
        result: Dict[str, Any] = {
            "mode": args.mode,
            "path_mode": path_mode,
            "stores": stores,
            "blocks": args.blocks,
            "block_bytes": args.block_bytes,
            "parallelism": args.parallelism,
            "batch_size": batch_size,
            "bytes_total": args.blocks * args.block_bytes,
            "payloads_preallocated": True,
            "verification": {"mode": verify_mode, "verify_s": 0.0, "verified_blocks": 0, "inside_measured_window": False},
            "native_memcpy_bound": native,
            "copy_bound": _copy_bound(args.block_bytes, max(1, args.copy_bound_iters)),
            "write": _phase_from_bound("write", args.blocks, args.block_bytes, native),
            "read": _phase_from_bound("read", args.blocks, args.block_bytes, native),
            "ts_wall_ms": int(time.time() * 1000),
        }
        result["target_analysis"] = _target_analysis(result)
        _print_table(result)
        out = args.json or str(ROOT.parent / "logs" / "falcon_kv_store_microbench_latest.json")
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        Path(out).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return 0

    if args.mode == "facade":
        falconfs_kv_brpc.membership_start(args.cn)
        falconfs_kv_brpc.membership_refresh(20000)
        if hasattr(falconfs_kv_brpc, "facade_perf_stats_reset"):
            falconfs_kv_brpc.facade_perf_stats_reset()

    store = BrpcKVStore(args.endpoint, timeout_ms=30000, use_facade_registry=(args.mode == "facade"))
    total_bytes = args.blocks * args.block_bytes
    key_prefix = args.key_prefix or f"micro-{int(time.time() * 1000)}-{os.getpid()}"
    block_hashes = [f"{key_prefix}-{i}" for i in range(args.blocks)]
    versions = [0 for _ in range(args.blocks)]
    leases = [None for _ in range(args.blocks)]

    if args.allocation == "metadata":
        old_pref = os.environ.get("FALCON_KV_PREFERRED_STORE_IDS")
        if old_pref is None:
            os.environ["FALCON_KV_PREFERRED_STORE_IDS"] = ",".join(str(s) for s in stores)
        meta = BrpcMetadataService(args.metadata_endpoint, timeout_ms=30000, block_size=args.block_bytes)
        alloc = meta.allocate(block_hashes, request_id=f"{key_prefix}-alloc")
        assignments: List[Assignment] = []
        allocated_for_cleanup = []
        try:
            for i, h in enumerate(block_hashes):
                ir, row, lease = alloc.get(h, (None, None, None))
                if ir is None or not ir.success or row is None:
                    msg = ir.error_message if ir is not None else "missing allocation result"
                    raise RuntimeError(f"metadata allocation failed idx={i} hash={h}: {msg}")
                assignments.append((int(row.location.store_node_id), int(row.location.pool_offset), i))
                versions[i] = int(row.version)
                leases[i] = lease
                allocated_for_cleanup.append((h, versions[i], True))
        except Exception:
            if allocated_for_cleanup:
                try:
                    meta.batch_free_allocated(allocated_for_cleanup, request_id=f"{key_prefix}-alloc-fail-free")
                except Exception:
                    pass
            raise
        finally:
            if old_pref is None:
                os.environ.pop("FALCON_KV_PREFERRED_STORE_IDS", None)
    else:
        meta = None
        per_store_slots: Dict[int, int] = {sid: 0 for sid in stores}
        assignments = []
        for i in range(args.blocks):
            sid = stores[i % len(stores)]
            slot = per_store_slots[sid]
            per_store_slots[sid] = slot + 1
            assignments.append((sid, slot * args.block_bytes, i))

    store_locality: Dict[int, bool] = {}
    if args.mode == "facade" and hasattr(falconfs_kv_brpc, "store_locality"):
        try:
            store_locality = {int(k): bool(v) for k, v in dict(falconfs_kv_brpc.store_locality()).items()}
        except Exception:
            store_locality = {}
    assigned_stores = sorted({sid for sid, _off, _idx in assignments})
    locality = _locality_summary(assigned_stores, store_locality)
    _enforce_locality(path_mode, assigned_stores, locality, args.require_local_store)

    payloads = [_payload(args.block_bytes, i) for i in range(args.blocks)]
    payload_views = [memoryview(p) for p in payloads]
    use_prealloc_write = path_mode in ("local-shm-prealloc-write", "local-shm-zero-copy-read")
    use_zero_copy_read = path_mode == "local-shm-zero-copy-read"
    write_tasks = _chunks_by_store(assignments, batch_size if batch_size > 1 else 1)
    read_tasks = _chunks_by_store(assignments, batch_size if use_zero_copy_read else 1)

    def do_write_chunk(chunk: List[Assignment]) -> Tuple[List[float], Dict[str, int], float, int]:
        sid = int(chunk[0][0])
        if len(chunk) > 1:
            blocks = [
                StoreBlockWrite(
                    pool_offset=off,
                    payload=payload_views[idx] if use_prealloc_write else payloads[idx],
                    block_hash=block_hashes[idx].encode(),
                    block_size=args.block_bytes,
                    expected_store_epoch=args.epoch,
                    expected_version=versions[idx],
                )
                for _sid, off, idx in chunk
            ]
            t0 = time.perf_counter()
            results = store.batch_write_blocks(sid, blocks)
            dt = time.perf_counter() - t0
            for r in results:
                if not r.success:
                    raise RuntimeError(f"batch write failed sid={sid}: {r.error_message}")
            return [dt / max(1, len(chunk))] * len(chunk), {"preallocated_writes": len(chunk) if use_prealloc_write else 0}, 0.0, 0
        sid, off, idx = chunk[0]
        t0 = time.perf_counter()
        r = store.write(
            sid,
            off,
            payload_views[idx] if use_prealloc_write else payloads[idx],
            args.epoch,
            args.block_bytes,
            expected_version=versions[idx],
            block_hash=block_hashes[idx].encode(),
        )
        dt = time.perf_counter() - t0
        if not r.success:
            raise RuntimeError(f"write failed sid={sid} off={off}: {r.error_message}")
        return [dt], {"preallocated_writes": 1 if use_prealloc_write else 0}, 0.0, 0

    def do_read_chunk(chunk: List[Assignment]) -> Tuple[List[float], Dict[str, int], float, int]:
        sid = int(chunk[0][0])
        if use_zero_copy_read:
            offsets = [off for _sid, off, _idx in chunk]
            epochs = [args.epoch for _sid, _off, _idx in chunk]
            hashes = [block_hashes[idx].encode() for _sid, _off, idx in chunk]
            t0 = time.perf_counter()
            ok_flags, views, kinds = store.batch_read_payload_views(sid, offsets, epochs, hashes, args.block_bytes)
            dt = time.perf_counter() - t0
            if len(ok_flags) != len(chunk) or not all(ok_flags):
                raise RuntimeError(f"zero-copy batch read failed sid={sid}: ok={ok_flags}")
            counts = {"zero_copy_blocks": len(chunk)}
            for kind in kinds:
                if kind == "local_shm_view":
                    counts["local_shm_views"] = counts.get("local_shm_views", 0) + 1
                elif kind in ("remote_attachment_view", "remote_attachment_buffer"):
                    counts["remote_attachment_views"] = counts.get("remote_attachment_views", 0) + 1
                elif kind in ("local_native_buffer", "bytes_fallback"):
                    counts["bytes_materialized_blocks"] = counts.get("bytes_materialized_blocks", 0) + 1
                elif kind:
                    counts[f"kind_{kind}"] = counts.get(f"kind_{kind}", 0) + 1
            vs, vc = _verify_payloads(chunk, views, payloads, verify_mode)
            return [dt / max(1, len(chunk))] * len(chunk), counts, vs, vc
        sid, off, idx = chunk[0]
        t0 = time.perf_counter()
        r, data = store.read(sid, off, args.epoch, expected_version=versions[idx], block_size=args.block_bytes, block_hash=block_hashes[idx].encode())
        dt = time.perf_counter() - t0
        if not r.success:
            raise RuntimeError(f"read failed sid={sid} off={off}: {r.error_message}")
        vs, vc = _verify_payloads(chunk, [data], payloads, verify_mode)
        return [dt], {"bytes_materialized_blocks": 1}, vs, vc

    # Warm pooled BRPC channels and facade mappings outside the measured window.
    seen = set()
    for item in assignments:
        sid, _off, _idx = item
        if sid in seen:
            continue
        seen.add(sid)
        do_write_chunk([item])
        do_read_chunk([item])

    write_s, write_wall, write_counts, write_verify_s, write_verified = _run_phase(write_tasks, do_write_chunk, args.parallelism)
    read_s, read_wall, read_counts, read_verify_s, read_verified = _run_phase(read_tasks, do_read_chunk, args.parallelism)

    result: Dict[str, Any] = {
        "mode": args.mode,
        "path_mode": path_mode,
        "stores": stores,
        "assigned_stores": assigned_stores,
        "blocks": args.blocks,
        "block_bytes": args.block_bytes,
        "parallelism": args.parallelism,
        "batch_size": batch_size,
        "allocation": args.allocation,
        "metadata_endpoint": args.metadata_endpoint if args.allocation == "metadata" else "",
        "bytes_total": total_bytes,
        "payloads_preallocated": True,
        "write": _summarize_phase("write", write_s, total_bytes, write_wall),
        "read": _summarize_phase("read", read_s, total_bytes, read_wall),
        "copy_bound": _copy_bound(args.block_bytes, max(1, args.copy_bound_iters)),
        "native_memcpy_bound": _native_memcpy_bound(args.block_bytes, max(1, args.copy_bound_iters)),
        "verification": {
            "mode": verify_mode,
            "verify_s": round(read_verify_s + write_verify_s, 6),
            "verified_blocks": int(read_verified + write_verified),
            "inside_measured_window": False,
        },
        "ts_wall_ms": int(time.time() * 1000),
        "store_locality": store_locality,
        "locality_assertions": {
            "require_local_store": args.require_local_store,
            "env_require_local": os.environ.get("FALCON_KV_MICRO_REQUIRE_LOCAL", "0") == "1",
            **locality,
        },
        "zero_copy_read": {
            "enabled": use_zero_copy_read,
            "zero_copy_blocks": int(read_counts.get("zero_copy_blocks", 0)),
            "local_shm_views": int(read_counts.get("local_shm_views", 0)),
            "remote_attachment_views": int(read_counts.get("remote_attachment_views", 0)),
            "bytes_materialized_blocks": int(read_counts.get("bytes_materialized_blocks", 0)),
            "fallback_reasons": {k: v for k, v in read_counts.items() if k.startswith("kind_")},
        },
        "preallocated_store_buffers": {
            "enabled": use_prealloc_write,
            "preallocated_writes": int(write_counts.get("preallocated_writes", 0)),
            "python_bytes_materialized_in_measured_write": not use_prealloc_write,
        },
    }

    section = _path_section_name(path_mode, locality)
    if section != "native_memcpy_bound":
        result[section] = {
            "path_mode": path_mode,
            "write": result["write"],
            "read": result["read"],
            "parallelism": args.parallelism,
            "batch_size": batch_size,
            "block_bytes": args.block_bytes,
            "payloads_preallocated": True,
            "verification": result["verification"],
            "locality": locality,
            "zero_copy_read": result["zero_copy_read"],
            "preallocated_store_buffers": result["preallocated_store_buffers"],
        }
    result["target_analysis"] = _target_analysis(result)

    if args.mode == "facade" and hasattr(falconfs_kv_brpc, "facade_perf_stats"):
        result["facade_perf_stats"] = dict(falconfs_kv_brpc.facade_perf_stats())

    store_counts: Dict[int, int] = {}
    for sid, _off, _idx in assignments:
        store_counts[sid] = store_counts.get(sid, 0) + 1
    result["store_distribution"] = store_counts

    try:
        if args.allocation == "metadata" and not args.keep_allocations and meta is not None:
            frees = [(h, versions[i], True) for i, h in enumerate(block_hashes)]
            free_results = meta.batch_free_allocated(frees, request_id=f"{key_prefix}-free")
            result["free_allocated_success"] = sum(1 for ir, _ in free_results if ir.success)
    except Exception as e:
        result["free_allocated_error"] = str(e)

    _print_table(result)
    out = args.json or str(ROOT.parent / "logs" / "falcon_kv_store_microbench_latest.json")
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    Path(out).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
