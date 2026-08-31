from __future__ import annotations

import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from .reference import BlockStatus, ErrorCode, ReferenceCluster
from .router import Router
from .promote_worker import PromoteWorker

try:
    from . import falconfs_kv_brpc
except ImportError:  # pragma: no cover - only in no-extension unit envs
    falconfs_kv_brpc = None


LEASE_DURATION_MS = 5000
BLOCK_SIZE = 65536

STATUS_ALLOCATED = int(BlockStatus.ALLOCATED)
STATUS_STORED = int(BlockStatus.STORED)
STATUS_EVICTING = int(BlockStatus.EVICTING)
STATUS_EVICTED = int(BlockStatus.EVICTED)
STATUS_FAILED = int(BlockStatus.FAILED)


def _v66_default_data_parallelism_max() -> int:
    """``falcon_kv.client_data_parallelism_max``: min(64, hw_concurrency*2)."""
    n = os.cpu_count() or 1
    return min(64, max(1, n * 2))


def _om_perf_env_enabled() -> bool:
    return os.environ.get("FALCON_KV_OM_PERF", "").strip().lower() in ("1", "true", "yes", "on")



def _env_bool(env_name: str, default: bool = False) -> bool:
    raw = os.environ.get(env_name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _env_int(env_name: str, default: int, lo: int = 1, hi: int = 1 << 30) -> int:
    raw = os.environ.get(env_name, "").strip()
    if not raw:
        return default
    try:
        return max(lo, min(int(raw), hi))
    except ValueError:
        return default

def _env_parallelism_max(env_name: str, default: int) -> int:
    """Positive int from env, clamped to [1, 4096]; invalid or empty → default."""
    raw = os.environ.get(env_name, "").strip()
    if not raw:
        return default
    try:
        v = int(raw)
        return max(1, min(v, 4096))
    except ValueError:
        return default


@dataclass
class LoadStoreSpec:
    data: Dict[str, Any] = field(default_factory=dict)
    specs: List[dict] = field(default_factory=list)


@dataclass
class KVBlockLocation:
    block_hash: str
    status: int
    store_id: int
    pool_offset: int
    lease_expire_ms: int
    evicted_path: Optional[str] = None
    lease_token: int = 0
    version: int = 0
    dn_epoch: int = 1
    store_epoch: int = 1
    dn_id: int = 0


def _route_dn_id(shard_table: Dict[int, str], block_hash: str) -> int:
    """Pick a DN id for the given block hash.

    The reference router uses a stable hash modulo of the shard-table keys
    so the same block always lands on the same DN. The native client will
    eventually replace this with the real FalconFS shard table lookup.
    """
    if not shard_table:
        raise RuntimeError("empty shard_table; at least one DN must be registered")
    keys = sorted(shard_table.keys())
    return keys[hash(block_hash) % len(keys)]


class FalconFSOffloadingManager:
    """Reference OffloadingManager with upstream public API names.

    The implementation uses one in-memory reference cluster per DN until C++
    BRPC clients are wired in. The `data` parameter on `complete_store` is a
    test scaffold that lets reference tests push payload bytes through the
    Python in-memory store; the native (cluster-mode) client receives bytes
    via the Store BRPC service and does not pass them through this method.

    **Cluster store I/O:** ``complete_store`` and ``prepare_load`` group DRAM
    writes/reads by ``(dn_id, store_id)`` within each CPU-aware wave, bounded by
    ``FALCON_KV_CLIENT_BATCH_WRITE_MAX_BLOCKS`` and
    ``FALCON_KV_CLIENT_BATCH_READ_MAX_BLOCKS``.
    ``(dn_id, store_id)`` is only a routing index — concurrency is capped by
    ``FALCON_KV_CLIENT_DATA_PARALLELISM_MAX`` (default ``min(64, hw*2)``) and
    ``FALCON_KV_CLIENT_META_PARALLELISM_MAX`` (default ``num_dns`` in cluster
    mode). Metadata updates after each write use per-key ``update_status`` unless
    the DN client exposes ``batch_update_status``.

    **Latency instrumentation:** set ``FALCON_KV_OM_PERF=1`` before constructing the
    manager (cluster mode). Then call ``perf_breakdown()`` for the last
    ``prepare_store`` / ``complete_store`` / ``prepare_load`` / ``complete_load``
    wall times and summed meta vs store-BRPC seconds (``data_write_s``,
    ``meta_update_status_s``, ``data_read_s``, per-DN allocate times, wave counts).
    Adds a small amount of locking around counters; disable for apples-to-apples
    bandwidth runs.
    """

    def __init__(
        self,
        client_id: int,
        client_hostname: str,
        shard_table: Optional[Dict[int, str]] = None,
        cluster=None,
        clusters: Optional[Dict[int, ReferenceCluster]] = None,
        cluster_factory: Optional[Callable[[int, str], ReferenceCluster]] = None,
        block_size: int = BLOCK_SIZE,
        mode: str = "reference",
        timeout_ms: int = 30000,
        cn_conninfo: Optional[str] = None,
    ):
        if mode == "cluster":
            if not shard_table and not cn_conninfo:
                raise RuntimeError("mode=cluster requires shard_table or cn_conninfo")
            if falconfs_kv_brpc is None:
                raise RuntimeError("falconfs_kv_brpc extension is required for cluster mode")
            if cn_conninfo:
                falconfs_kv_brpc.membership_start(cn_conninfo)
                # Prime Store facade registry. Also discover DN endpoints when
                # the caller did not provide an explicit shard table. CN/DN rows
                # can lag briefly after restart and fault drills, so use the
                # caller's timeout as a discovery budget instead of failing after
                # one empty snapshot.
                deadline = time.monotonic() + max(0.1, float(timeout_ms) / 1000.0)
                while True:
                    falconfs_kv_brpc.membership_refresh(timeout_ms)
                    if shard_table:
                        break
                    discovered = falconfs_kv_brpc.discover_dn_endpoints()
                    if discovered or time.monotonic() >= deadline:
                        shard_table = discovered
                        break
                    time.sleep(0.25)
        self.shard_table = dict(shard_table or {})
        if mode == "cluster" and not self.shard_table:
            raise RuntimeError("cluster mode discovered no DN endpoints")
        self.client_id = client_id
        self.client_hostname = client_hostname
        self.local_cache: Dict[str, KVBlockLocation] = {}
        self.block_size = block_size
        self.mode = mode
        self._cn_conninfo = cn_conninfo
        self.timeout_ms = timeout_ms
        self._cluster_factory = cluster_factory
        self._cluster_singleton = cluster
        if clusters:
            self._clusters: Dict[int, ReferenceCluster] = clusters
        elif cluster_factory is not None:
            self._clusters = {
                dn_id: cluster_factory(dn_id, endpoint)
                for dn_id, endpoint in sorted(self.shard_table.items())
            }
        elif cluster is not None:
            keys = sorted(self.shard_table.keys()) or [0]
            self._clusters = {dn_id: cluster for dn_id in keys}
        elif mode == "cluster":
            # v6 §16: BRPC client per DN. Each DN endpoint is addressed by a
            # single `BrpcCluster` that exposes `metadata` + `store` shims so
            # the rest of this manager stays mode-agnostic.
            from .store_client import BrpcCluster
            self._clusters = {
                dn_id: BrpcCluster(endpoint, dn_id=dn_id, client_id=client_id,
                                   timeout_ms=timeout_ms, block_size=block_size,
                                   use_facade_registry=bool(cn_conninfo))
                for dn_id, endpoint in sorted(self.shard_table.items())
            }
        else:
            keys = sorted(self.shard_table.keys()) or [0]
            self._clusters = {dn_id: ReferenceCluster(block_size=block_size) for dn_id in keys}
        # Stable per-DN routing.
        self._router = Router(self.shard_table) if self.shard_table else None
        self._routing: Dict[str, int] = {}
        self._promote_worker = PromoteWorker(self) if mode == "cluster" else None
        _dns = max(1, len(self.shard_table)) if self.shard_table else 1
        _data_def = _v66_default_data_parallelism_max()
        _meta_def = _dns if mode == "cluster" else min(_dns, _v66_default_data_parallelism_max())
        self._data_parallelism_max = _env_parallelism_max(
            "FALCON_KV_CLIENT_DATA_PARALLELISM_MAX", _data_def
        )
        self._meta_parallelism_max = _env_parallelism_max(
            "FALCON_KV_CLIENT_META_PARALLELISM_MAX", _meta_def
        )
        self._data_stage_pool = ThreadPoolExecutor(
            max_workers=self._data_parallelism_max, thread_name_prefix="kv-om-data"
        )
        self._read_grouping_enabled = _env_bool("FALCON_KV_CLIENT_READ_GROUPING", True)
        self._write_grouping_enabled = _env_bool("FALCON_KV_CLIENT_WRITE_GROUPING", True)
        self._adaptive_batching_enabled = _env_bool("FALCON_KV_CLIENT_ADAPTIVE_BATCHING", False)
        self._read_batch_max_blocks = _env_parallelism_max(
            "FALCON_KV_CLIENT_BATCH_READ_MAX_BLOCKS", 8
        )
        self._read_batch_local_max_blocks = _env_parallelism_max(
            "FALCON_KV_CLIENT_BATCH_READ_LOCAL_MAX_BLOCKS", self._read_batch_max_blocks
        )
        self._read_batch_remote_max_blocks = _env_parallelism_max(
            "FALCON_KV_CLIENT_BATCH_READ_REMOTE_MAX_BLOCKS", self._read_batch_max_blocks
        )
        self._write_batch_max_blocks = _env_parallelism_max(
            "FALCON_KV_CLIENT_BATCH_WRITE_MAX_BLOCKS", 1
        )
        self._write_batch_local_max_blocks = _env_parallelism_max(
            "FALCON_KV_CLIENT_BATCH_WRITE_LOCAL_MAX_BLOCKS", self._write_batch_max_blocks
        )
        self._write_batch_remote_max_blocks = _env_parallelism_max(
            "FALCON_KV_CLIENT_BATCH_WRITE_REMOTE_MAX_BLOCKS", self._write_batch_max_blocks
        )
        self._adaptive_batching_rollbacks = 0
        self._read_batch_target_bytes = _env_int(
            "FALCON_KV_CLIENT_BATCH_READ_TARGET_BYTES", 8 * 1024 * 1024
        )
        self._write_batch_target_bytes = _env_int(
            "FALCON_KV_CLIENT_BATCH_WRITE_TARGET_BYTES", 4 * 1024 * 1024
        )
        self._adaptive_batching_cooldown = _env_int("FALCON_KV_CLIENT_ADAPTIVE_COOLDOWN_WINDOWS", 1)
        self._adaptive_batching_max_inflight_bytes = _env_int(
            "FALCON_KV_CLIENT_ADAPTIVE_MAX_INFLIGHT_BYTES",
            max(1, int(self._data_parallelism_max)) * max(1, int(self.block_size)) * 8,
        )
        self._adaptive_batching_state = self._init_adaptive_batching_state()
        self._zero_copy_reads_enabled = _env_bool("FALCON_KV_CLIENT_ZERO_COPY_READS", False)
        self._zero_copy_stats = {
            "enabled": bool(self._zero_copy_reads_enabled),
            "zero_copy_blocks": 0,
            "bytes_materialized_blocks": 0,
            "local_shm_views": 0,
            "remote_attachment_views": 0,
            "fallback_reasons": {},
        }
        self._zero_copy_lock = threading.Lock()
        self._prealloc_load_buffers_enabled = _env_bool("FALCON_KV_CLIENT_PREALLOC_LOAD_BUFFERS", False)
        self._prealloc_load_pool_capacity = _env_int("FALCON_KV_CLIENT_PREALLOC_LOAD_BUFFER_POOL", 1024)
        self._prealloc_load_lock = threading.Lock()
        self._prealloc_load_pool: List[bytearray] = []
        self._prealloc_load_outstanding: Dict[str, Tuple[bytearray, int]] = {}
        self._prealloc_load_stats = {
            "enabled": bool(self._prealloc_load_buffers_enabled),
            "blocks": 0,
            "bytes": 0,
            "buffers_created": 0,
            "buffers_reused": 0,
            "buffers_returned": 0,
            "fallback_reasons": {},
        }
        self._meta_stage_pool = ThreadPoolExecutor(
            max_workers=self._meta_parallelism_max, thread_name_prefix="kv-om-meta"
        )
        self._om_perf_enabled = bool(mode == "cluster" and _om_perf_env_enabled())
        self._om_perf_lock = threading.Lock()
        self._om_perf_last: Dict[str, Any] = {}
        # Populated during complete_store / prepare_load when _om_perf_enabled.
        self._om_perf_cs_acc: Optional[Dict[str, float]] = None
        self._om_perf_ld_acc: Optional[Dict[str, float]] = None

    def _store_is_local(self, store_id: int) -> Optional[bool]:
        """Return whether the active facade for ``store_id`` is local SHM."""
        if self.mode != "cluster" or falconfs_kv_brpc is None:
            return None
        try:
            loc = falconfs_kv_brpc.store_locality()
            if store_id in loc:
                return bool(loc[store_id])
            s = str(store_id)
            if s in loc:
                return bool(loc[s])
        except Exception:
            return None
        return None

    def _record_data_io(
        self,
        acc: Optional[Dict[str, float]],
        *,
        prefix: str,
        store_id: int,
        elapsed_s: float,
        byte_count: int,
    ) -> None:
        self._record_data_batch_io(
            acc,
            prefix=prefix,
            store_id=store_id,
            elapsed_s=elapsed_s,
            byte_count=byte_count,
            logical_ops=1,
            rpc_ops=1,
        )

    def _record_data_batch_io(
        self,
        acc: Optional[Dict[str, float]],
        *,
        prefix: str,
        store_id: int,
        elapsed_s: float,
        byte_count: int,
        logical_ops: int,
        rpc_ops: int = 1,
    ) -> None:
        if acc is None:
            return
        locality = self._store_is_local(store_id)
        if locality is True:
            label = "local"
        elif locality is False:
            label = "remote"
        else:
            label = "unknown"
        logical_ops = max(0, int(logical_ops))
        rpc_ops = max(0, int(rpc_ops))
        with self._om_perf_lock:
            acc[f"{prefix}_s"] = float(acc.get(f"{prefix}_s", 0.0)) + elapsed_s
            acc[f"{prefix}_bytes"] = float(acc.get(f"{prefix}_bytes", 0.0)) + float(byte_count)
            acc[f"n_{prefix}s"] = float(acc.get(f"n_{prefix}s", 0.0)) + float(logical_ops)
            acc[f"{prefix}_rpcs"] = float(acc.get(f"{prefix}_rpcs", 0.0)) + float(rpc_ops)
            acc[f"{prefix}_{label}_s"] = float(acc.get(f"{prefix}_{label}_s", 0.0)) + elapsed_s
            acc[f"{prefix}_{label}_bytes"] = float(acc.get(f"{prefix}_{label}_bytes", 0.0)) + float(byte_count)
            acc[f"n_{prefix}s_{label}"] = float(acc.get(f"n_{prefix}s_{label}", 0.0)) + float(logical_ops)
            acc[f"{prefix}_{label}_rpcs"] = float(acc.get(f"{prefix}_{label}_rpcs", 0.0)) + float(rpc_ops)
            if rpc_ops > 0:
                samples = acc.setdefault(f"{prefix}_{label}_latency_ms", [])
                samples.append(1000.0 * elapsed_s / float(rpc_ops))

    def perf_breakdown(self) -> Dict[str, Any]:
        """Last high-level latency split (``FALCON_KV_OM_PERF=1`` cluster mode only).

        Keys may include ``prepare_store``, ``complete_store``, ``prepare_load``,
        ``complete_load`` with wall-clock seconds and summed BRPC sub-phases.
        Stable observability sections are also included so benchmark JSON can be
        compared across runs: metadata channel cache, promote-on-read, and the
        active adaptive/static batching policy.
        """
        with self._om_perf_lock:
            out = dict(self._om_perf_last)
        out["adaptive_batching"] = self._adaptive_batching_snapshot()
        if self._promote_worker is not None:
            out["promote_on_read"] = self._promote_worker.stats()
        out["zero_copy_read"] = self._zero_copy_snapshot()
        out["prealloc_load_buffers"] = self._prealloc_load_snapshot()
        if falconfs_kv_brpc is not None:
            try:
                out["metadata_channel_cache"] = dict(falconfs_kv_brpc.metadata_channel_stats())
            except Exception:
                pass
        return out

    def set_om_perf_enabled(self, enabled: bool) -> None:
        """Turn cluster-side BRPC latency counters on or off (e.g. two-phase E2E).

        Only ``mode=cluster`` collects; reference mode is always off.
        """
        self._om_perf_enabled = bool(self.mode == "cluster" and enabled)

    def lookup(self, key: str, req_context=None) -> bool | None:
        result = self._batch_lookup_impl([key], req_context, renew_lease_on_hit=False)
        return result.get(key)

    def prepare_load(self, keys: List[str], req_context=None) -> LoadStoreSpec:
        if not self._om_perf_enabled:
            lookup = self._batch_lookup_impl(keys, req_context, renew_lease_on_hit=True)
            missing = [key for key, hit in lookup.items() if not hit]
            if missing:
                raise RuntimeError(f"Blocks {missing} not found in cache")
            return self._batch_load_impl(keys)
        t_wall0 = time.perf_counter()
        self._om_perf_ld_lookup_dn = {}
        lookup = self._batch_lookup_impl(keys, req_context, renew_lease_on_hit=True)
        t_lookup1 = time.perf_counter()
        missing = [key for key, hit in lookup.items() if not hit]
        if missing:
            raise RuntimeError(f"Blocks {missing} not found in cache")
        self._om_perf_ld_acc = {
            "data_read_s": 0.0,
            "data_read_bytes": 0.0,
            "n_data_reads": 0.0,
            "data_read_rpcs": 0.0,
            "data_read_local_s": 0.0,
            "data_read_local_bytes": 0.0,
            "n_data_reads_local": 0.0,
            "data_read_local_rpcs": 0.0,
            "data_read_remote_s": 0.0,
            "data_read_remote_bytes": 0.0,
            "n_data_reads_remote": 0.0,
            "data_read_remote_rpcs": 0.0,
            "data_read_unknown_s": 0.0,
            "data_read_unknown_bytes": 0.0,
            "n_data_reads_unknown": 0.0,
            "data_read_unknown_rpcs": 0.0,
            "read_batching_enabled": 0.0,
            "zero_copy_blocks": 0.0,
            "bytes_materialized_blocks": 0.0,
            "local_shm_views": 0.0,
            "remote_attachment_views": 0.0,
            "preallocated_load_blocks": 0.0,
            "preallocated_load_bytes": 0.0,
        }
        try:
            return self._batch_load_impl(keys)
        finally:
            t_wall1 = time.perf_counter()
            acc = self._om_perf_ld_acc or {}
            nr = int(acc.get("n_data_reads", acc.get("n_reads", 0.0)))
            dr = float(acc.get("data_read_s", 0.0))
            with self._om_perf_lock:
                self._om_perf_last["prepare_load"] = {
                    "wall_s": t_wall1 - t_wall0,
                    "meta_batch_lookup_s": t_lookup1 - t_wall0,
                    "meta_lookup_by_dn_s": dict(getattr(self, "_om_perf_ld_lookup_dn", {})),
                    "data_read_s": dr,
                    "n_reads": nr,
                    "data_read_rpcs": int(acc.get("data_read_rpcs", 0.0)),
                    "data_read_local_rpcs": int(acc.get("data_read_local_rpcs", 0.0)),
                    "data_read_remote_rpcs": int(acc.get("data_read_remote_rpcs", 0.0)),
                    "data_read_unknown_rpcs": int(acc.get("data_read_unknown_rpcs", 0.0)),
                    "read_batching_enabled": bool(acc.get("read_batching_enabled", 0.0)),
                    "data_read_bytes": int(acc.get("data_read_bytes", 0.0)),
                    "data_read_local_s": float(acc.get("data_read_local_s", 0.0)),
                    "data_read_local_bytes": int(acc.get("data_read_local_bytes", 0.0)),
                    "n_reads_local": int(acc.get("n_data_reads_local", 0.0)),
                    "data_read_remote_s": float(acc.get("data_read_remote_s", 0.0)),
                    "data_read_remote_bytes": int(acc.get("data_read_remote_bytes", 0.0)),
                    "n_reads_remote": int(acc.get("n_data_reads_remote", 0.0)),
                    "data_read_unknown_s": float(acc.get("data_read_unknown_s", 0.0)),
                    "data_read_unknown_bytes": int(acc.get("data_read_unknown_bytes", 0.0)),
                    "n_reads_unknown": int(acc.get("n_data_reads_unknown", 0.0)),
                    "avg_data_read_ms": (1000.0 * dr / nr) if nr else 0.0,
                    "avg_data_read_local_ms": (
                        1000.0 * float(acc.get("data_read_local_s", 0.0))
                        / int(acc.get("n_data_reads_local", 0.0))
                    )
                    if int(acc.get("n_data_reads_local", 0.0))
                    else 0.0,
                    "avg_data_read_remote_ms": (
                        1000.0 * float(acc.get("data_read_remote_s", 0.0))
                        / int(acc.get("n_data_reads_remote", 0.0))
                    )
                    if int(acc.get("n_data_reads_remote", 0.0))
                    else 0.0,
                    "n_keys": len(keys),
                    "wave_chunk_size": float(acc.get("wave_chunk_size", self._store_io_wave_chunk_size(is_write=False, batch_max=getattr(self, "_read_batch_max_blocks", 1)))),
                    "read_batch_max_blocks": int(acc.get("read_batch_max_blocks", getattr(self, "_read_batch_max_blocks", 16))),
                    "read_batch_local_max_blocks": int(acc.get("read_batch_local_max_blocks", getattr(self, "_read_batch_local_max_blocks", getattr(self, "_read_batch_max_blocks", 16)))),
                    "read_batch_remote_max_blocks": int(acc.get("read_batch_remote_max_blocks", getattr(self, "_read_batch_remote_max_blocks", getattr(self, "_read_batch_max_blocks", 16)))),
                    "adaptive_batching_enabled": bool(acc.get("adaptive_batching_enabled", 0.0)),
                    "waves": int(acc.get("waves", 0.0)),
                    "data_parallelism_max": float(self._data_parallelism_max),
                    "zero_copy_blocks": int(acc.get("zero_copy_blocks", 0.0)),
                    "bytes_materialized_blocks": int(acc.get("bytes_materialized_blocks", 0.0)),
                    "local_shm_views": int(acc.get("local_shm_views", 0.0)),
                    "remote_attachment_views": int(acc.get("remote_attachment_views", 0.0)),
                    "preallocated_load_blocks": int(acc.get("preallocated_load_blocks", 0.0)),
                    "preallocated_load_bytes": int(acc.get("preallocated_load_bytes", 0.0)),
                    "prealloc_load_buffers_enabled": bool(getattr(self, "_prealloc_load_buffers_enabled", False)),
                }
            self._adaptive_observe_from_acc(acc, is_write=False)
            self._om_perf_ld_acc = None
            self._om_perf_ld_lookup_dn = {}

    def complete_load(self, keys: List[str], req_context=None):
        try:
            if self._om_perf_enabled:
                t0 = time.perf_counter()
                self._om_perf_renew_dn = {}
                self._batch_renew_impl(keys)
                renew_by_dn = dict(getattr(self, "_om_perf_renew_dn", {}))
                with self._om_perf_lock:
                    self._om_perf_last["complete_load"] = {
                        "wall_s": time.perf_counter() - t0,
                        "meta_renew_s": float(sum(renew_by_dn.values())),
                        "meta_renew_by_dn_s": renew_by_dn,
                        "n_keys": len(keys),
                        "meta_parallelism_max": float(self._meta_parallelism_max),
                    }
                self._om_perf_renew_dn = {}
                return
            self._batch_renew_impl(keys)
        finally:
            self._prealloc_load_release(keys)

    def prepare_store(self, keys: List[str], req_context=None) -> Optional[LoadStoreSpec]:
        if not self._om_perf_enabled:
            spec = self._batch_prepare_store_impl(keys, req_context)
            if not spec.specs:
                return None
            return spec
        t0 = time.perf_counter()
        self._om_perf_ps_dn = {}
        spec = self._batch_prepare_store_impl(keys, req_context)
        t1 = time.perf_counter()
        with self._om_perf_lock:
            self._om_perf_last["prepare_store"] = {
                "wall_s": t1 - t0,
                "meta_allocate_by_dn_s": dict(self._om_perf_ps_dn),
                "n_keys": len(keys),
                "n_specs": len(spec.specs),
            }
        if not spec.specs:
            return None
        return spec

    def complete_store(
        self,
        keys: List[str],
        data: Optional[Dict[str, bytes]] = None,
        req_context=None,
        success: bool = True,
    ):
        if not success:
            self._free_allocated_for_keys(keys)
            return
        successful = self._batch_complete_store_impl(keys, data or {}, req_context)
        failed = [key for key in keys if key not in successful]
        if failed:
            self._free_allocated_for_keys(failed)

    def touch(self, keys: List[str], req_context=None):
        self._batch_renew_impl(keys)

    def refresh_membership(self, timeout_ms: int = 2000):
        if self.mode != "cluster":
            return (0, 0, 0)
        if falconfs_kv_brpc is None:
            raise RuntimeError("falconfs_kv_brpc extension is required for cluster mode")
        stats = falconfs_kv_brpc.membership_refresh(timeout_ms)
        discovered = dict(falconfs_kv_brpc.discover_dn_endpoints())
        if discovered and discovered != self.shard_table:
            self.shard_table = discovered
            if self._cluster_factory is not None:
                self._clusters = {
                    dn_id: self._cluster_factory(dn_id, endpoint)
                    for dn_id, endpoint in sorted(self.shard_table.items())
                }
            elif self._cluster_singleton is not None:
                self._clusters = {dn_id: self._cluster_singleton for dn_id in self.shard_table.keys()}
            else:
                from .store_client import BrpcCluster
                self._clusters = {
                    dn_id: BrpcCluster(endpoint, dn_id=dn_id, client_id=self.client_id,
                                       timeout_ms=self.timeout_ms, block_size=self.block_size,
                                       use_facade_registry=bool(self._cn_conninfo))
                    for dn_id, endpoint in sorted(self.shard_table.items())
                }
            self._router = Router(self.shard_table)
            self._routing.clear()
        return stats

    def batch_lookup(self, keys: List[str], req_context) -> Dict[str, bool]:
        return self._batch_lookup_impl(keys, req_context, renew_lease_on_hit=True)

    def batch_prepare_store(self, keys: List[str], req_context) -> LoadStoreSpec:
        spec = self.prepare_store(keys, req_context)
        return spec or LoadStoreSpec()

    def batch_complete_store(self, keys: List[str], data: Dict[str, bytes], req_context):
        self.complete_store(keys, data, req_context)

    def batch_prepare_load(self, keys: List[str], req_context) -> LoadStoreSpec:
        return self.prepare_load(keys, req_context)

    def batch_complete_load(self, keys: List[str], req_context):
        self.complete_load(keys, req_context)

    def batch_touch(self, keys: List[str], req_context):
        self.touch(keys, req_context)

    def _cluster_for(self, block_hash: str) -> ReferenceCluster:
        dn_id = self._routing.get(block_hash)
        if dn_id is None:
            dn_id = _route_dn_id(self.shard_table, block_hash)
            self._routing[block_hash] = dn_id
        if dn_id not in self._clusters:
            # fallback: use any available cluster
            dn_id = next(iter(self._clusters.keys()))
            self._routing[block_hash] = dn_id
        return self._clusters[dn_id]

    def _group_by_dn(self, keys: List[str]) -> Dict[int, List[str]]:
        grouped: Dict[int, List[str]] = {}
        for key in keys:
            dn_id = self._routing.get(key) or _route_dn_id(self.shard_table, key)
            self._routing[key] = dn_id
            grouped.setdefault(dn_id, []).append(key)
        return grouped

    def _batch_lookup_impl(self, keys: List[str], req_context, renew_lease_on_hit: bool) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        groups = self._group_by_dn(keys)

        def lookup_dn(dn_id: int, dn_keys: List[str]):
            cluster = self._clusters[dn_id]
            t0 = time.perf_counter() if self._om_perf_enabled else 0.0
            rows = cluster.metadata.lookup(
                dn_keys, renew_lease_on_hit=renew_lease_on_hit
            )
            if self._om_perf_enabled and hasattr(self, "_om_perf_ld_lookup_dn"):
                dt = time.perf_counter() - t0
                kdn = str(int(dn_id))
                with self._om_perf_lock:
                    self._om_perf_ld_lookup_dn[kdn] = (
                        float(self._om_perf_ld_lookup_dn.get(kdn, 0.0)) + dt
                    )
            return dn_id, rows

        if len(groups) > 1:
            futs = {
                self._meta_stage_pool.submit(lookup_dn, dn_id, dn_keys): dn_id
                for dn_id, dn_keys in groups.items()
            }
            dn_results = [fut.result() for fut in as_completed(futs)]
        else:
            dn_results = [lookup_dn(dn_id, dn_keys) for dn_id, dn_keys in groups.items()]

        for dn_id, rows in dn_results:
            for key, (result, row, lease) in rows.items():
                if result.success and row:
                    if lease:
                        self._cache_location(key, row, lease, dn_id)
                    elif int(row.status) == STATUS_EVICTED:
                        self.local_cache[key] = KVBlockLocation(
                            block_hash=key,
                            status=int(row.status),
                            store_id=row.location.store_node_id,
                            pool_offset=row.location.pool_offset,
                            lease_expire_ms=self._now_ms(),
                            evicted_path=row.location.evicted_path,
                            lease_token=0,
                            version=row.version,
                            dn_epoch=1,
                            store_epoch=row.location.store_epoch,
                            dn_id=dn_id,
                        )
                    results[key] = True
                elif result.error_code in (ErrorCode.CAS_CONFLICT, ErrorCode.THROTTLED):
                    results[key] = None  # type: ignore[assignment]
                else:
                    results[key] = False
        return results

    def _batch_prepare_store_impl(self, keys: List[str], req_context) -> LoadStoreSpec:
        request_id = self._mk_request_id()
        specs: List[dict] = []
        groups = self._group_by_dn(keys)

        def allocate_dn(dn_id: int, dn_keys: List[str]):
            cluster = self._clusters[dn_id]
            if self._om_perf_enabled:
                ta = time.perf_counter()
            allocations = cluster.metadata.allocate(
                dn_keys,
                request_id=request_id,
                client_id=self.client_id,
            )
            if self._om_perf_enabled:
                dt = time.perf_counter() - ta
                kdn = str(int(dn_id))
                self._om_perf_ps_dn[kdn] = self._om_perf_ps_dn.get(kdn, 0.0) + dt
            return dn_id, allocations

        if len(groups) > 1:
            futs = {
                self._meta_stage_pool.submit(allocate_dn, dn_id, dn_keys): dn_id
                for dn_id, dn_keys in groups.items()
            }
            dn_allocations = [fut.result() for fut in as_completed(futs)]
        else:
            dn_allocations = [allocate_dn(dn_id, dn_keys) for dn_id, dn_keys in groups.items()]

        for dn_id, allocations in dn_allocations:
            for key, (result, row, lease) in allocations.items():
                if not result.success or not row or not lease:
                    continue
                self._cache_location(key, row, lease, dn_id)
                specs.append(
                    {
                        "block_hash": row.block_hash,
                        "store_id": row.location.store_node_id,
                        "pool_offset": row.location.pool_offset,
                        "dn_id": dn_id,
                    }
                )
        return LoadStoreSpec(specs=specs)

    def _complete_store_write_one_key(
        self, key: str, data: Dict[str, bytes], batch_id: str, req_context
    ) -> bool:
        del req_context
        payload = data.get(key)
        if payload is None:
            return False
        loc = self.local_cache.get(key)
        if not loc:
            return False
        cluster = self._clusters.get(loc.dn_id) or self._cluster_for(key)
        acc = self._om_perf_cs_acc
        t_dw0 = time.perf_counter() if acc is not None else 0.0
        write = cluster.store.write(
            loc.store_id,
            loc.pool_offset,
            payload,
            loc.store_epoch,
            self.block_size,
            block_hash=key.encode("utf-8"),
        )
        t_mid = time.perf_counter() if acc is not None else 0.0
        if not write.success:
            return False
        if acc is not None:
            self._record_data_io(
                acc,
                prefix="data_write",
                store_id=loc.store_id,
                elapsed_s=t_mid - t_dw0,
                byte_count=len(payload),
            )
        return True


    def _complete_store_write_key_group(
        self, dn_id: int, store_id: int, keys: List[str], data: Dict[str, bytes], batch_id: str
    ) -> Dict[str, bool]:
        del batch_id
        from .store_client import StoreBlockWrite

        cluster = self._clusters.get(dn_id) or self._cluster_for(keys[0])
        blocks = []
        ordered_keys = []
        total_bytes = 0
        for key in keys:
            loc = self.local_cache.get(key)
            payload = data.get(key)
            if not loc or payload is None:
                continue
            ordered_keys.append(key)
            total_bytes += len(payload)
            blocks.append(
                StoreBlockWrite(
                    pool_offset=loc.pool_offset,
                    payload=payload,
                    block_hash=key.encode("utf-8"),
                    block_size=self.block_size,
                    expected_store_epoch=loc.store_epoch,
                    expected_version=0,
                )
            )
        if not blocks:
            return {}
        acc = self._om_perf_cs_acc
        t0 = time.perf_counter() if acc is not None else 0.0
        results = cluster.store.batch_write_blocks(store_id, blocks)
        elapsed = time.perf_counter() - t0 if acc is not None else 0.0
        if len(results) != len(ordered_keys):
            raise RuntimeError(
                f"Store {store_id} returned {len(results)} write results for {len(ordered_keys)} requested blocks"
            )
        out: Dict[str, bool] = {}
        for key, result in zip(ordered_keys, results):
            out[key] = bool(result.success)
        if acc is not None:
            self._record_data_batch_io(
                acc,
                prefix="data_write",
                store_id=store_id,
                elapsed_s=elapsed,
                byte_count=total_bytes,
                logical_ops=sum(1 for ok in out.values() if ok),
                rpc_ops=1,
            )
        return out

    def _batch_mark_stored(self, keys: List[str], batch_id: str) -> Dict[str, bool]:
        grouped: Dict[int, List[str]] = {}
        for key in keys:
            loc = self.local_cache.get(key)
            if loc:
                grouped.setdefault(loc.dn_id, []).append(key)

        def update_dn(dn_id: int, dn_keys: List[str]):
            cluster = self._clusters[dn_id]
            updates = []
            for key in dn_keys:
                loc = self.local_cache.get(key)
                if loc:
                    updates.append((key, BlockStatus.ALLOCATED, BlockStatus.STORED, loc.version, ""))
            t0 = time.perf_counter() if self._om_perf_cs_acc is not None else 0.0
            if hasattr(cluster.metadata, "batch_update_status"):
                rows = cluster.metadata.batch_update_status(
                    updates,
                    request_id=f"{batch_id}:stored:{dn_id}",
                    client_id=self.client_id,
                )
            else:
                rows = [
                    cluster.metadata.update_status(
                        key,
                        BlockStatus.ALLOCATED,
                        BlockStatus.STORED,
                        expected_version,
                        request_id=f"{batch_id}:{key}",
                        client_id=self.client_id,
                    )
                    for key, _from, _to, expected_version, _path in updates
                ]
            if self._om_perf_cs_acc is not None:
                dt = time.perf_counter() - t0
                with self._om_perf_lock:
                    self._om_perf_cs_acc["meta_update_status_s"] = (
                        float(self._om_perf_cs_acc.get("meta_update_status_s", 0.0))
                        + dt
                    )
                    by_dn = self._om_perf_cs_acc.setdefault("meta_update_status_by_dn_s", {})
                    kdn = str(int(dn_id))
                    by_dn[kdn] = float(by_dn.get(kdn, 0.0)) + dt
            out: Dict[str, bool] = {}
            for key, (result, row) in zip(dn_keys, rows):
                if result.success and row:
                    loc = self.local_cache.get(key)
                    if loc:
                        loc.status = int(row.status)
                        loc.version = row.version
                    out[key] = True
                else:
                    out[key] = False
            return out

        if not grouped:
            return {}
        if len(grouped) > 1:
            futs = {
                self._meta_stage_pool.submit(update_dn, dn_id, dn_keys): dn_id
                for dn_id, dn_keys in grouped.items()
            }
            parts = [fut.result() for fut in as_completed(futs)]
        else:
            parts = [update_dn(dn_id, dn_keys) for dn_id, dn_keys in grouped.items()]
        merged: Dict[str, bool] = {}
        for part in parts:
            merged.update(part)
        return merged


    def _configured_batch_for_path(self, path: str) -> int:
        return int({
            "local_read": getattr(self, "_read_batch_local_max_blocks", 1),
            "remote_read": getattr(self, "_read_batch_remote_max_blocks", 1),
            "local_write": getattr(self, "_write_batch_local_max_blocks", 1),
            "remote_write": getattr(self, "_write_batch_remote_max_blocks", 1),
        }.get(path, 1))

    def _path_is_write(self, path: str) -> bool:
        return path.endswith("write")

    def _target_bound_for_path(self, path: str, configured: int) -> int:
        is_write = self._path_is_write(path)
        target = self._write_batch_target_bytes if is_write else self._read_batch_target_bytes
        by_bytes = max(1, int(target) // max(1, int(self.block_size)))
        by_inflight = max(1, int(self._adaptive_batching_max_inflight_bytes) // max(1, int(self.block_size)) // max(1, int(self._data_parallelism_max)))
        return max(1, min(int(configured), int(by_bytes), int(by_inflight), 4096))

    def _init_adaptive_batching_state(self) -> Dict[str, Dict[str, Any]]:
        state: Dict[str, Dict[str, Any]] = {}
        for path in ("local_read", "remote_read", "local_write", "remote_write"):
            configured = self._configured_batch_for_path(path)
            selected = self._target_bound_for_path(path, configured)
            state[path] = {
                "configured": configured,
                "selected": selected,
                "previous": selected,
                "last_good": selected,
                "last_good_mb_s": 0.0,
                "last_good_p95_ms": 0.0,
                "pending_probe": False,
                "cooldown": 0,
                "rollback_count": 0,
                "last_decision_reason": "static_defaults_active" if not self._adaptive_batching_enabled else "initial_policy",
                "last_observed_mb_s": 0.0,
                "last_p50_ms": 0.0,
                "last_p95_ms": 0.0,
                "last_error_rate": 0.0,
                "throughput_before": 0.0,
                "throughput_after": 0.0,
            }
        return state

    def _effective_batch_max_blocks(self, *, is_write: bool) -> int:
        path = "local_write" if is_write else "local_read"
        configured = self._write_batch_max_blocks if is_write else self._read_batch_max_blocks
        if self._adaptive_batching_enabled:
            return self._adaptive_selected_for_path(path, configured)
        return max(1, int(configured))

    def _effective_batch_max_from_configured(self, configured: int, *, is_write: bool) -> int:
        if not self._adaptive_batching_enabled:
            return max(1, int(configured))
        path = "local_write" if is_write else "local_read"
        return self._adaptive_selected_for_path(path, configured)

    def _adaptive_selected_for_path(self, path: str, configured: int) -> int:
        if not self._adaptive_batching_enabled:
            return max(1, int(configured))
        st = self._adaptive_batching_state.get(path)
        bound = self._target_bound_for_path(path, configured)
        if st is None:
            return bound
        return max(1, min(int(st.get("selected", bound)), bound))

    def _effective_read_batch_max_for_store(self, store_id: int) -> int:
        locality = self._store_is_local(store_id)
        if locality is True:
            return self._adaptive_selected_for_path("local_read", self._read_batch_local_max_blocks)
        if locality is False:
            return self._adaptive_selected_for_path("remote_read", self._read_batch_remote_max_blocks)
        return self._effective_batch_max_blocks(is_write=False)

    def _effective_write_batch_max_for_store(self, store_id: int) -> int:
        locality = self._store_is_local(store_id)
        if locality is True:
            return self._adaptive_selected_for_path("local_write", self._write_batch_local_max_blocks)
        if locality is False:
            return self._adaptive_selected_for_path("remote_write", self._write_batch_remote_max_blocks)
        return self._effective_batch_max_blocks(is_write=True)

    def _percentile(self, values: Any, pct: float) -> float:
        vals = sorted(float(v) for v in (values or []) if v is not None)
        if not vals:
            return 0.0
        idx = min(len(vals) - 1, max(0, int(round((len(vals) - 1) * pct))))
        return vals[idx]

    def _adaptive_observe_from_acc(self, acc: Dict[str, Any], *, is_write: bool) -> None:
        if not self._adaptive_batching_enabled:
            return
        prefix = "data_write" if is_write else "data_read"
        suffix = "write" if is_write else "read"
        for label in ("local", "remote"):
            bytes_v = float(acc.get(f"{prefix}_{label}_bytes", 0.0))
            seconds = float(acc.get(f"{prefix}_{label}_s", 0.0))
            if bytes_v <= 0 or seconds <= 0:
                continue
            samples = acc.get(f"{prefix}_{label}_latency_ms", [])
            self._adaptive_observe_path(
                f"{label}_{suffix}",
                mb_s=(bytes_v / 1.0e6) / seconds,
                p50_ms=self._percentile(samples, 0.50),
                p95_ms=self._percentile(samples, 0.95),
                error_rate=0.0,
            )

    def _adaptive_observe_path(self, path: str, *, mb_s: float, p50_ms: float, p95_ms: float, error_rate: float) -> None:
        st = self._adaptive_batching_state.get(path)
        if st is None:
            return
        st["last_observed_mb_s"] = float(mb_s)
        st["last_p50_ms"] = float(p50_ms)
        st["last_p95_ms"] = float(p95_ms)
        st["last_error_rate"] = float(error_rate)
        configured = self._configured_batch_for_path(path)
        max_batch = self._target_bound_for_path(path, configured)
        selected = max(1, min(int(st.get("selected", 1)), max_batch))
        st["previous"] = selected
        if st.get("cooldown", 0) > 0:
            st["cooldown"] = int(st.get("cooldown", 0)) - 1
            st["last_decision_reason"] = "cooldown"
            return
        last_good_mb = float(st.get("last_good_mb_s", 0.0))
        last_good_p95 = float(st.get("last_good_p95_ms", 0.0))
        if last_good_mb <= 0.0:
            st["last_good_mb_s"] = float(mb_s)
            st["last_good_p95_ms"] = float(p95_ms)
            st["last_good"] = selected
            if selected < max_batch:
                st["selected"] = selected + 1
                st["pending_probe"] = True
                st["last_decision_reason"] = "initial_probe_up"
            else:
                st["last_decision_reason"] = "initial_at_max"
            return
        regressed = error_rate > 0.0 or mb_s < (last_good_mb * 0.97) or (last_good_p95 > 0 and p95_ms > last_good_p95 * 1.20)
        if st.get("pending_probe") and regressed:
            st["selected"] = int(st.get("last_good", selected))
            st["rollback_count"] = int(st.get("rollback_count", 0)) + 1
            self._adaptive_batching_rollbacks += 1
            st["pending_probe"] = False
            st["cooldown"] = int(self._adaptive_batching_cooldown)
            st["throughput_before"] = last_good_mb
            st["throughput_after"] = float(mb_s)
            st["last_decision_reason"] = "rollback_on_regression"
            return
        if mb_s >= last_good_mb and (last_good_p95 <= 0 or p95_ms <= last_good_p95 * 1.10):
            st["last_good_mb_s"] = float(mb_s)
            st["last_good_p95_ms"] = float(p95_ms)
            st["last_good"] = selected
            st["pending_probe"] = False
            st["throughput_before"] = last_good_mb
            st["throughput_after"] = float(mb_s)
            if selected < max_batch:
                st["selected"] = selected + 1
                st["pending_probe"] = True
                st["last_decision_reason"] = "accepted_probe_then_probe_up"
            else:
                st["last_decision_reason"] = "accepted_at_max"
            return
        if selected > 1 and p95_ms > last_good_p95 * 1.10:
            st["selected"] = max(1, selected - 1)
            st["pending_probe"] = True
            st["last_decision_reason"] = "probe_down_on_latency"
            return
        st["last_decision_reason"] = "kept_last_good"

    def _adaptive_batching_snapshot(self) -> Dict[str, Any]:
        enabled = bool(getattr(self, "_adaptive_batching_enabled", False))
        state = getattr(self, "_adaptive_batching_state", {})
        selected = {path: int(st.get("selected", 1)) for path, st in state.items()}
        controller = {}
        for path, st in state.items():
            controller[path] = {
                "selected": int(st.get("selected", 1)),
                "previous": int(st.get("previous", st.get("selected", 1))),
                "configured": int(st.get("configured", self._configured_batch_for_path(path))),
                "rollback_count": int(st.get("rollback_count", 0)),
                "last_decision_reason": str(st.get("last_decision_reason", "static_defaults_active")),
                "p50_ms": float(st.get("last_p50_ms", 0.0)),
                "p95_ms": float(st.get("last_p95_ms", 0.0)),
                "error_rate": float(st.get("last_error_rate", 0.0)),
                "throughput_before_mb_s": float(st.get("throughput_before", 0.0)),
                "throughput_after_mb_s": float(st.get("throughput_after", st.get("last_observed_mb_s", 0.0))),
                "last_observed_mb_s": float(st.get("last_observed_mb_s", 0.0)),
            }
        return {
            "enabled": enabled,
            "selected_batch_size": {
                "local_read": int(selected.get("local_read", self._read_batch_local_max_blocks if not enabled else 1)),
                "remote_read": int(selected.get("remote_read", self._read_batch_remote_max_blocks if not enabled else 1)),
                "local_write": int(selected.get("local_write", self._write_batch_local_max_blocks if not enabled else 1)),
                "remote_write": int(selected.get("remote_write", self._write_batch_remote_max_blocks if not enabled else 1)),
            },
            "configured_batch_size": {
                "read": int(getattr(self, "_read_batch_max_blocks", 1)),
                "read_local": int(getattr(self, "_read_batch_local_max_blocks", 1)),
                "read_remote": int(getattr(self, "_read_batch_remote_max_blocks", 1)),
                "write": int(getattr(self, "_write_batch_max_blocks", 1)),
                "write_local": int(getattr(self, "_write_batch_local_max_blocks", 1)),
                "write_remote": int(getattr(self, "_write_batch_remote_max_blocks", 1)),
            },
            "target_bytes": {"read": int(self._read_batch_target_bytes), "write": int(self._write_batch_target_bytes)},
            "cpu_count": int(os.cpu_count() or 1),
            "data_parallelism": int(getattr(self, "_data_parallelism_max", 1)),
            "max_inflight_bytes": int(getattr(self, "_adaptive_batching_max_inflight_bytes", 0)),
            "block_size": int(self.block_size),
            "rollback_count": int(getattr(self, "_adaptive_batching_rollbacks", 0)),
            "last_decision_reason": ";".join(sorted({str(v.get("last_decision_reason", "")) for v in state.values()})) if enabled else "static_defaults_active",
            "controller": controller,
        }

    def _zero_copy_bump(self, key: str, count: int = 1, reason: str = "") -> None:
        with self._zero_copy_lock:
            if key == "fallback":
                reasons = self._zero_copy_stats.setdefault("fallback_reasons", {})
                reasons[reason or "unknown"] = int(reasons.get(reason or "unknown", 0)) + int(count)
            else:
                self._zero_copy_stats[key] = int(self._zero_copy_stats.get(key, 0)) + int(count)

    def _zero_copy_snapshot(self) -> Dict[str, Any]:
        with self._zero_copy_lock:
            out = dict(self._zero_copy_stats)
            out["fallback_reasons"] = dict(self._zero_copy_stats.get("fallback_reasons", {}))
        out["enabled"] = bool(getattr(self, "_zero_copy_reads_enabled", False))
        return out

    def _prealloc_load_bump(self, key: str, count: int = 1, reason: str = "") -> None:
        with self._prealloc_load_lock:
            if key == "fallback":
                reasons = self._prealloc_load_stats.setdefault("fallback_reasons", {})
                reasons[reason or "unknown"] = int(reasons.get(reason or "unknown", 0)) + int(count)
            else:
                self._prealloc_load_stats[key] = int(self._prealloc_load_stats.get(key, 0)) + int(count)

    def _prealloc_load_snapshot(self) -> Dict[str, Any]:
        with self._prealloc_load_lock:
            out = dict(self._prealloc_load_stats)
            out["fallback_reasons"] = dict(self._prealloc_load_stats.get("fallback_reasons", {}))
            out["pool_size"] = len(self._prealloc_load_pool)
            out["outstanding"] = len(self._prealloc_load_outstanding)
            out["pool_capacity"] = int(getattr(self, "_prealloc_load_pool_capacity", 0))
        out["enabled"] = bool(getattr(self, "_prealloc_load_buffers_enabled", False))
        return out

    def _prealloc_load_acquire(self, size: int) -> bytearray:
        n = max(0, int(size))
        with self._prealloc_load_lock:
            while self._prealloc_load_pool:
                buf = self._prealloc_load_pool.pop()
                if len(buf) >= n:
                    self._prealloc_load_stats["buffers_reused"] = int(self._prealloc_load_stats.get("buffers_reused", 0)) + 1
                    return buf
            self._prealloc_load_stats["buffers_created"] = int(self._prealloc_load_stats.get("buffers_created", 0)) + 1
        return bytearray(max(n, int(self.block_size)))

    def _prealloc_load_materialize(self, key: str, payload: Any) -> memoryview:
        view = memoryview(payload)
        buf = self._prealloc_load_acquire(len(view))
        buf_view = memoryview(buf)
        buf_view[:len(view)] = view
        with self._prealloc_load_lock:
            old = self._prealloc_load_outstanding.pop(key, None)
            if old is not None and len(self._prealloc_load_pool) < int(self._prealloc_load_pool_capacity):
                self._prealloc_load_pool.append(old[0])
                self._prealloc_load_stats["buffers_returned"] = int(self._prealloc_load_stats.get("buffers_returned", 0)) + 1
            self._prealloc_load_outstanding[key] = (buf, len(view))
            self._prealloc_load_stats["blocks"] = int(self._prealloc_load_stats.get("blocks", 0)) + 1
            self._prealloc_load_stats["bytes"] = int(self._prealloc_load_stats.get("bytes", 0)) + int(len(view))
        return memoryview(buf)[:len(view)]

    def _prealloc_load_release(self, keys: List[str]) -> None:
        if not getattr(self, "_prealloc_load_buffers_enabled", False):
            return
        with self._prealloc_load_lock:
            for key in keys:
                item = self._prealloc_load_outstanding.pop(key, None)
                if item is None:
                    continue
                if len(self._prealloc_load_pool) < int(self._prealloc_load_pool_capacity):
                    self._prealloc_load_pool.append(item[0])
                    self._prealloc_load_stats["buffers_returned"] = int(self._prealloc_load_stats.get("buffers_returned", 0)) + 1

    def _store_io_wave_chunk_size(self, *, is_write: bool = True, batch_max: int = 1) -> int:
        """CPU-aware logical blocks per Store I/O wave.

        ``batch_max`` is accepted so callers can report/tune read and write waves
        with one helper. The measured default is CPU-bounded in logical blocks:
        larger batch-scaled waves reduce RPC count, but raise BRPC attachment
        latency enough to hurt remote read throughput on the 1 MiB policy.
        """
        specific = (
            "FALCON_KV_CLIENT_WRITE_WAVE_CHUNK_SIZE"
            if is_write
            else "FALCON_KV_CLIENT_READ_WAVE_CHUNK_SIZE"
        )
        for env_name in (specific, "FALCON_KV_CLIENT_DATA_WAVE_CHUNK_SIZE"):
            explicit = os.environ.get(env_name, "").strip()
            if explicit:
                try:
                    return max(1, min(int(explicit), 4096))
                except ValueError:
                    pass
        raw = os.environ.get("FALCON_KV_CLIENT_DATA_CPU_FACTOR", "2").strip()
        try:
            factor = max(1, min(int(raw), 16))
        except ValueError:
            factor = 2
        logical = max(1, min(self._data_parallelism_max, (os.cpu_count() or 1) * factor))
        cap = _env_int("FALCON_KV_CLIENT_DATA_WAVE_MAX_BLOCKS", 512)
        return max(1, min(int(logical), int(cap), 4096))

    def _reference_batch_complete_store(
        self, keys: List[str], data: Dict[str, bytes], req_context
    ) -> Dict[str, bool]:
        batch_id = self._mk_request_id()
        successful: Dict[str, bool] = {}
        work = [k for k in keys if k in data]
        acc = self._om_perf_cs_acc
        if len(work) <= 1:
            if acc is not None:
                acc["waves"] = 1.0
                acc["wave_chunk_size"] = float(self._store_io_wave_chunk_size(is_write=True, batch_max=1))
            written = [
                key
                for key in work
                if self._complete_store_write_one_key(key, data, batch_id, req_context)
            ]
            return {k: True for k, ok in self._batch_mark_stored(written, batch_id).items() if ok}

        step = self._store_io_wave_chunk_size(is_write=True, batch_max=1)
        if acc is not None:
            acc["wave_chunk_size"] = float(step)
        waves = 0
        for off in range(0, len(work), step):
            waves += 1
            chunk = work[off : off + step]
            futs = {
                self._data_stage_pool.submit(self._complete_store_write_one_key, k, data, batch_id, req_context): k
                for k in chunk
            }
            for fut in as_completed(futs):
                k = futs[fut]
                if fut.result():
                    successful[k] = True
        successful = {k: True for k, ok in self._batch_mark_stored(list(successful), batch_id).items() if ok}
        if acc is not None:
            acc["waves"] = float(waves)
        return successful

    def _cluster_batch_complete_store(
        self, keys: List[str], data: Dict[str, bytes], req_context
    ) -> Dict[str, bool]:
        batch_id = self._mk_request_id()
        work = [k for k in keys if k in data and self.local_cache.get(k)]
        successful: Dict[str, bool] = {}
        if not work:
            return successful
        acc = self._om_perf_cs_acc
        if len(work) == 1:
            if acc is not None:
                acc["waves"] = 1.0
                acc["wave_chunk_size"] = float(self._store_io_wave_chunk_size(is_write=True, batch_max=1))
            k0 = work[0]
            if self._complete_store_write_one_key(k0, data, batch_id, req_context):
                successful = {
                    k: True
                    for k, ok in self._batch_mark_stored([k0], batch_id).items()
                    if ok
                }
            return successful

        raw_batch_max = self._effective_batch_max_blocks(is_write=True)
        local_batch_max = self._effective_batch_max_from_configured(
            self._write_batch_local_max_blocks, is_write=True
        )
        remote_batch_max = self._effective_batch_max_from_configured(
            self._write_batch_remote_max_blocks, is_write=True
        )
        if not getattr(self, "_write_grouping_enabled", True):
            raw_batch_max = 1
            local_batch_max = 1
            remote_batch_max = 1
        max_configured_batch = max(raw_batch_max, local_batch_max, remote_batch_max)
        step = self._store_io_wave_chunk_size(is_write=True, batch_max=max_configured_batch)
        if acc is not None:
            acc["wave_chunk_size"] = float(step)
            acc["write_batching_enabled"] = 1.0 if max_configured_batch > 1 else 0.0
            acc["write_batch_max_blocks"] = float(raw_batch_max)
            acc["write_batch_local_max_blocks"] = float(local_batch_max)
            acc["write_batch_remote_max_blocks"] = float(remote_batch_max)
            acc["adaptive_batching_enabled"] = 1.0 if getattr(self, "_adaptive_batching_enabled", False) else 0.0
        waves = 0
        for off in range(0, len(work), step):
            waves += 1
            chunk = work[off : off + step]
            if max_configured_batch <= 1:
                futs_one = {
                    self._data_stage_pool.submit(self._complete_store_write_one_key, k, data, batch_id, req_context): k
                    for k in chunk
                }
                for fut in as_completed(futs_one):
                    k = futs_one[fut]
                    if fut.result():
                        successful[k] = True
                continue
            grouped: Dict[Tuple[int, int], List[str]] = {}
            for k in chunk:
                loc = self.local_cache.get(k)
                if loc:
                    grouped.setdefault((loc.dn_id, loc.store_id), []).append(k)
            futs = {}
            for (dn_id, store_id), group_keys in grouped.items():
                group_batch_max = (
                    self._effective_write_batch_max_for_store(store_id)
                    if getattr(self, "_write_grouping_enabled", True)
                    else 1
                )
                for group_off in range(0, len(group_keys), group_batch_max):
                    part = group_keys[group_off : group_off + group_batch_max]
                    futs[self._data_stage_pool.submit(
                        self._complete_store_write_key_group, dn_id, store_id, part, data, batch_id
                    )] = None
            for fut in as_completed(futs):
                for k, ok in fut.result().items():
                    if ok:
                        successful[k] = True
        successful = {k: True for k, ok in self._batch_mark_stored(list(successful), batch_id).items() if ok}
        if acc is not None:
            acc["waves"] = float(waves)
        return successful

    def _batch_complete_store_impl(
        self, keys: List[str], data: Dict[str, bytes], req_context
    ) -> Dict[str, bool]:
        if not self._om_perf_enabled:
            if self.mode == "cluster":
                return self._cluster_batch_complete_store(keys, data, req_context)
            return self._reference_batch_complete_store(keys, data, req_context)
        self._om_perf_cs_acc = {
            "data_write_s": 0.0,
            "data_write_bytes": 0.0,
            "data_write_rpcs": 0.0,
            "meta_update_status_s": 0.0,
            "meta_update_status_by_dn_s": {},
            "n_data_writes": 0.0,
            "data_write_local_s": 0.0,
            "data_write_local_bytes": 0.0,
            "n_data_writes_local": 0.0,
            "data_write_local_rpcs": 0.0,
            "data_write_remote_s": 0.0,
            "data_write_remote_bytes": 0.0,
            "n_data_writes_remote": 0.0,
            "data_write_remote_rpcs": 0.0,
            "data_write_unknown_s": 0.0,
            "data_write_unknown_bytes": 0.0,
            "n_data_writes_unknown": 0.0,
            "data_write_unknown_rpcs": 0.0,
            "write_batching_enabled": 0.0,
            "write_batch_max_blocks": 0.0,
            "waves": 0.0,
            "wave_chunk_size": 0.0,
        }
        t_wall0 = time.perf_counter()
        try:
            if self.mode == "cluster":
                return self._cluster_batch_complete_store(keys, data, req_context)
            return self._reference_batch_complete_store(keys, data, req_context)
        finally:
            t_wall1 = time.perf_counter()
            acc = self._om_perf_cs_acc or {}
            n_inst = int(acc.get("n_data_writes", acc.get("n_writes_instrumented", 0.0)))
            with self._om_perf_lock:
                self._om_perf_last["complete_store"] = {
                    "wall_s": t_wall1 - t_wall0,
                    "data_write_s": float(acc.get("data_write_s", 0.0)),
                    "data_write_bytes": int(acc.get("data_write_bytes", 0.0)),
                    "data_write_rpcs": int(acc.get("data_write_rpcs", 0.0)),
                    "data_write_local_rpcs": int(acc.get("data_write_local_rpcs", 0.0)),
                    "data_write_remote_rpcs": int(acc.get("data_write_remote_rpcs", 0.0)),
                    "data_write_unknown_rpcs": int(acc.get("data_write_unknown_rpcs", 0.0)),
                    "write_batching_enabled": bool(acc.get("write_batching_enabled", 0.0)),
                    "write_batch_max_blocks": int(acc.get("write_batch_max_blocks", getattr(self, "_write_batch_max_blocks", 1))),
                    "write_batch_local_max_blocks": int(acc.get("write_batch_local_max_blocks", getattr(self, "_write_batch_local_max_blocks", getattr(self, "_write_batch_max_blocks", 1)))),
                    "write_batch_remote_max_blocks": int(acc.get("write_batch_remote_max_blocks", getattr(self, "_write_batch_remote_max_blocks", getattr(self, "_write_batch_max_blocks", 1)))),
                    "adaptive_batching_enabled": bool(acc.get("adaptive_batching_enabled", 0.0)),
                    "meta_update_status_s": float(acc.get("meta_update_status_s", 0.0)),
                    "meta_update_status_by_dn_s": dict(acc.get("meta_update_status_by_dn_s", {})),
                    "n_writes_instrumented": n_inst,
                    "n_writes_local": int(acc.get("n_data_writes_local", 0.0)),
                    "data_write_local_s": float(acc.get("data_write_local_s", 0.0)),
                    "data_write_local_bytes": int(acc.get("data_write_local_bytes", 0.0)),
                    "n_writes_remote": int(acc.get("n_data_writes_remote", 0.0)),
                    "data_write_remote_s": float(acc.get("data_write_remote_s", 0.0)),
                    "data_write_remote_bytes": int(acc.get("data_write_remote_bytes", 0.0)),
                    "n_writes_unknown": int(acc.get("n_data_writes_unknown", 0.0)),
                    "data_write_unknown_s": float(acc.get("data_write_unknown_s", 0.0)),
                    "data_write_unknown_bytes": int(acc.get("data_write_unknown_bytes", 0.0)),
                    "avg_data_write_ms": (1000.0 * float(acc.get("data_write_s", 0.0)) / n_inst) if n_inst else 0.0,
                    "avg_data_write_rpc_ms": (
                        1000.0 * float(acc.get("data_write_s", 0.0))
                        / int(acc.get("data_write_rpcs", 0.0))
                    )
                    if int(acc.get("data_write_rpcs", 0.0))
                    else 0.0,
                    "avg_data_write_local_ms": (
                        1000.0 * float(acc.get("data_write_local_s", 0.0))
                        / int(acc.get("n_data_writes_local", 0.0))
                    )
                    if int(acc.get("n_data_writes_local", 0.0))
                    else 0.0,
                    "avg_data_write_remote_ms": (
                        1000.0 * float(acc.get("data_write_remote_s", 0.0))
                        / int(acc.get("n_data_writes_remote", 0.0))
                    )
                    if int(acc.get("n_data_writes_remote", 0.0))
                    else 0.0,
                    "avg_meta_update_ms": (1000.0 * float(acc.get("meta_update_status_s", 0.0)) / n_inst)
                    if n_inst
                    else 0.0,
                    "waves": float(acc.get("waves", 0.0)),
                    "wave_chunk_size": float(acc.get("wave_chunk_size", 0.0)),
                    "data_parallelism_max": float(self._data_parallelism_max),
                }
            self._adaptive_observe_from_acc(acc, is_write=True)
            self._om_perf_cs_acc = None

    def _free_allocated_for_keys(self, keys: List[str]) -> None:
        batch_id = self._mk_request_id()
        grouped: Dict[int, List[str]] = {}
        for key in keys:
            loc = self.local_cache.get(key)
            if not loc:
                continue
            grouped.setdefault(loc.dn_id, []).append(key)

        def free_dn(dn_id: int, dn_keys: List[str]) -> List[str]:
            cluster = self._clusters.get(dn_id)
            if cluster is None:
                return []
            freed: List[str] = []
            batch_items = []
            for key in dn_keys:
                loc = self.local_cache.get(key)
                if loc:
                    batch_items.append((key, loc.version, False))
            if hasattr(cluster.metadata, "batch_free_allocated"):
                results = cluster.metadata.batch_free_allocated(
                    batch_items,
                    request_id=f"{batch_id}:free:{dn_id}",
                    client_id=self.client_id,
                )
                for key, (result, _new_version) in zip([x[0] for x in batch_items], results):
                    if result.success:
                        freed.append(key)
                return freed
            for key, expected_version, force in batch_items:
                result, _new_version = cluster.metadata.free_allocated(
                    key,
                    expected_version=expected_version,
                    force=force,
                    request_id=f"{batch_id}:{key}",
                    client_id=self.client_id,
                )
                if result.success:
                    freed.append(key)
            return freed

        if len(grouped) > 1:
            futs = {
                self._meta_stage_pool.submit(free_dn, dn_id, dn_keys): dn_id
                for dn_id, dn_keys in grouped.items()
            }
            freed_keys = [key for fut in as_completed(futs) for key in fut.result()]
        else:
            freed_keys = [key for dn_id, dn_keys in grouped.items() for key in free_dn(dn_id, dn_keys)]

        for key in freed_keys:
            self.local_cache.pop(key, None)
            self._routing.pop(key, None)

    def _load_one_key_bytes(self, key: str) -> bytes:
        loc = self.local_cache.get(key)
        if not loc:
            raise RuntimeError(f"Block {key} not found in local cache")
        cluster = self._clusters.get(loc.dn_id) or self._cluster_for(key)
        if loc.status == STATUS_EVICTED and loc.evicted_path:
            acc_ld = self._om_perf_ld_acc
            t_r0 = time.perf_counter() if acc_ld is not None else 0.0
            result, payload = cluster.store.read_from_ssd(
                loc.store_id,
                loc.evicted_path,
                loc.store_epoch,
                expected_version=loc.version,
                block_size=self.block_size,
            )
            if acc_ld is not None:
                self._record_data_io(
                    acc_ld,
                    prefix="data_read",
                    store_id=loc.store_id,
                    elapsed_s=time.perf_counter() - t_r0,
                    byte_count=len(payload),
                )
            if result.success and getattr(self, "_zero_copy_reads_enabled", False):
                self._zero_copy_bump("fallback", 1, "ssd_read")
                self._zero_copy_bump("bytes_materialized_blocks")
            if result.success and self._promote_worker is not None:
                self._promote_worker.enqueue(
                    key,
                    payload,
                    loc.store_id,
                    loc.pool_offset,
                    loc.version,
                    loc.store_epoch,
                    loc.dn_id,
                )
        else:
            # Store DRAM versioning is engine-local (see KVStoreEngine::Write); metadata
            # block version after STORED can diverge. Use 0 to read latest payload at
            # (store_id, pool_offset) once metadata already validated the lease.
            acc_ld = self._om_perf_ld_acc
            t_r0 = time.perf_counter() if acc_ld is not None else 0.0
            result, payload = cluster.store.read(
                loc.store_id,
                loc.pool_offset,
                loc.store_epoch,
                0,
                self.block_size,
                block_hash=key.encode("utf-8"),
            )
            if acc_ld is not None:
                self._record_data_io(
                    acc_ld,
                    prefix="data_read",
                    store_id=loc.store_id,
                    elapsed_s=time.perf_counter() - t_r0,
                    byte_count=len(payload),
                )
        if not result.success:
            raise RuntimeError(f"Failed to read {key}: {result.error_code}")
        return payload

    def _reference_batch_load(self, keys: List[str]) -> LoadStoreSpec:
        if len(keys) <= 1:
            loaded = {key: self._load_one_key_bytes(key) for key in keys}
            return LoadStoreSpec(data=loaded)

        loaded: Dict[str, bytes] = {}
        step = self._store_io_wave_chunk_size(is_write=False, batch_max=1)
        for off in range(0, len(keys), step):
            chunk = keys[off : off + step]
            futs = {self._data_stage_pool.submit(self._load_one_key_bytes, k): k for k in chunk}
            for fut in as_completed(futs):
                k = futs[fut]
                loaded[k] = fut.result()
        return LoadStoreSpec(data=loaded)

    def _cluster_batch_load(self, keys: List[str]) -> LoadStoreSpec:
        work = [k for k in keys if self.local_cache.get(k)]
        if not work:
            return LoadStoreSpec(data={})
        if len(work) == 1:
            k0 = work[0]
            return LoadStoreSpec(data={k0: self._load_one_key_bytes(k0)})

        loaded: Dict[str, bytes] = {}
        batch_max = self._effective_batch_max_blocks(is_write=False)
        local_batch_max = self._effective_batch_max_from_configured(
            self._read_batch_local_max_blocks, is_write=False
        )
        remote_batch_max = self._effective_batch_max_from_configured(
            self._read_batch_remote_max_blocks, is_write=False
        )
        if not getattr(self, "_read_grouping_enabled", True):
            batch_max = 1
            local_batch_max = 1
            remote_batch_max = 1
        step = self._store_io_wave_chunk_size(is_write=False, batch_max=batch_max)
        if self._om_perf_ld_acc is not None:
            self._om_perf_ld_acc["read_batching_enabled"] = 1.0 if max(local_batch_max, remote_batch_max, batch_max) > 1 else 0.0
            self._om_perf_ld_acc["wave_chunk_size"] = float(step)
            self._om_perf_ld_acc["read_batch_max_blocks"] = float(batch_max)
            self._om_perf_ld_acc["read_batch_local_max_blocks"] = float(local_batch_max)
            self._om_perf_ld_acc["read_batch_remote_max_blocks"] = float(remote_batch_max)
            self._om_perf_ld_acc["adaptive_batching_enabled"] = 1.0 if getattr(self, "_adaptive_batching_enabled", False) else 0.0
        waves = 0
        for off in range(0, len(work), step):
            waves += 1
            chunk = work[off : off + step]
            grouped: Dict[Tuple[int, int], List[str]] = {}
            fallback: List[str] = []
            for k in chunk:
                loc = self.local_cache.get(k)
                if not loc:
                    continue
                if loc.status == STATUS_EVICTED and loc.evicted_path:
                    fallback.append(k)
                else:
                    grouped.setdefault((loc.dn_id, loc.store_id), []).append(k)

            futs = {self._data_stage_pool.submit(self._load_one_key_bytes, k): ("one", k) for k in fallback}
            for (dn_id, store_id), group_keys in grouped.items():
                group_batch_max = (
                    self._effective_read_batch_max_for_store(store_id)
                    if getattr(self, "_read_grouping_enabled", True)
                    else 1
                )
                for group_off in range(0, len(group_keys), group_batch_max):
                    part = group_keys[group_off : group_off + group_batch_max]
                    futs[self._data_stage_pool.submit(self._load_dram_key_group, dn_id, store_id, part)] = ("batch", None)
            for fut in as_completed(futs):
                kind, key = futs[fut]
                if kind == "one":
                    loaded[str(key)] = fut.result()
                else:
                    loaded.update(fut.result())
        if self._om_perf_ld_acc is not None:
            self._om_perf_ld_acc["waves"] = float(waves)
        return LoadStoreSpec(data=loaded)

    def _load_dram_key_group(self, dn_id: int, store_id: int, keys: List[str]) -> Dict[str, bytes]:
        from .store_client import StoreBlockRead

        cluster = self._clusters.get(dn_id) or self._cluster_for(keys[0])
        ordered_keys: List[str] = []
        pool_offsets: List[int] = []
        store_epochs: List[int] = []
        block_hashes: List[bytes] = []
        blocks: List[StoreBlockRead] = []
        for key in keys:
            loc = self.local_cache.get(key)
            if not loc:
                continue
            key_bytes = key.encode("utf-8")
            ordered_keys.append(key)
            pool_offsets.append(loc.pool_offset)
            store_epochs.append(loc.store_epoch)
            block_hashes.append(key_bytes)
            blocks.append(
                StoreBlockRead(
                    pool_offset=loc.pool_offset,
                    block_hash=key_bytes,
                    block_size=self.block_size,
                    expected_store_epoch=loc.store_epoch,
                    expected_version=0,
                )
            )
        if not ordered_keys:
            return {}
        acc_ld = self._om_perf_ld_acc
        t0 = time.perf_counter() if acc_ld is not None else 0.0
        view_kinds: List[str] = []
        prealloc_enabled = bool(getattr(self, "_prealloc_load_buffers_enabled", False))
        use_zero_copy = bool(getattr(self, "_zero_copy_reads_enabled", False))
        use_view_source = bool(
            (use_zero_copy or prealloc_enabled)
            and hasattr(cluster.store, "batch_read_payload_views")
        )
        if use_view_source:
            try:
                ok_flags, payloads, view_kinds = cluster.store.batch_read_payload_views(
                    store_id, pool_offsets, store_epochs, block_hashes, self.block_size
                )
            except Exception:
                if use_zero_copy:
                    self._zero_copy_bump("fallback", len(ordered_keys), "view_api_failed")
                if prealloc_enabled:
                    self._prealloc_load_bump("fallback", len(ordered_keys), "view_api_failed")
                use_view_source = False
        if not use_view_source and hasattr(cluster.store, "batch_read_payloads_fast"):
            ok_flags, payloads = cluster.store.batch_read_payloads_fast(
                store_id, pool_offsets, store_epochs, block_hashes, self.block_size
            )
            view_kinds = ["bytes_fallback"] * len(payloads)
            if use_zero_copy:
                self._zero_copy_bump("fallback", len(payloads), "view_api_unavailable")
            if prealloc_enabled:
                self._prealloc_load_bump("fallback", len(payloads), "view_api_unavailable")
        elif not use_view_source:
            results = cluster.store.batch_read_blocks(store_id, blocks)
            ok_flags = [result.success for result, _ in results]
            payloads = [payload if result.success else b"" for result, payload in results]
            view_kinds = ["bytes_fallback"] * len(payloads)
            if use_zero_copy:
                self._zero_copy_bump("fallback", len(payloads), "batch_fast_unavailable")
            if prealloc_enabled:
                self._prealloc_load_bump("fallback", len(payloads), "batch_fast_unavailable")
        if len(ok_flags) != len(ordered_keys) or len(payloads) != len(ordered_keys):
            raise RuntimeError(
                f"Store {store_id} returned {len(payloads)} read payloads for {len(ordered_keys)} requested blocks"
            )
        out: Dict[str, Any] = {}
        result_kinds: List[str] = []
        total_bytes = 0
        for key, ok, payload in zip(ordered_keys, ok_flags, payloads):
            if not ok:
                raise RuntimeError(f"Failed to read {key}: {ErrorCode.INTERNAL_ERROR}")
            kind = view_kinds[len(out)] if len(view_kinds) > len(out) else "bytes_fallback"
            if prealloc_enabled and not use_zero_copy and kind in ("local_native_buffer", "local_shm_view", "remote_attachment_buffer"):
                payload = self._prealloc_load_materialize(key, payload)
                kind = "preallocated_load_buffer"
            out[key] = payload
            result_kinds.append(kind)
            total_bytes += len(payload)
            if kind in ("local_native_buffer", "local_shm_view", "remote_attachment_buffer"):
                self._zero_copy_bump("zero_copy_blocks")
                if kind.startswith("local"):
                    self._zero_copy_bump("local_shm_views")
                else:
                    self._zero_copy_bump("remote_attachment_views")
            elif kind == "preallocated_load_buffer":
                self._zero_copy_bump("bytes_materialized_blocks")
            else:
                self._zero_copy_bump("bytes_materialized_blocks")
        elapsed = time.perf_counter() - t0 if acc_ld is not None else 0.0
        if acc_ld is not None:
            for kind in result_kinds:
                if kind in ("local_native_buffer", "local_shm_view", "remote_attachment_buffer"):
                    acc_ld["zero_copy_blocks"] = float(acc_ld.get("zero_copy_blocks", 0.0)) + 1.0
                    if kind.startswith("local"):
                        acc_ld["local_shm_views"] = float(acc_ld.get("local_shm_views", 0.0)) + 1.0
                    else:
                        acc_ld["remote_attachment_views"] = float(acc_ld.get("remote_attachment_views", 0.0)) + 1.0
                elif kind == "preallocated_load_buffer":
                    acc_ld["bytes_materialized_blocks"] = float(acc_ld.get("bytes_materialized_blocks", 0.0)) + 1.0
                    acc_ld["preallocated_load_blocks"] = float(acc_ld.get("preallocated_load_blocks", 0.0)) + 1.0
                    acc_ld["preallocated_load_bytes"] = float(acc_ld.get("preallocated_load_bytes", 0.0)) + float(self.block_size)
                else:
                    acc_ld["bytes_materialized_blocks"] = float(acc_ld.get("bytes_materialized_blocks", 0.0)) + 1.0
            self._record_data_batch_io(
                acc_ld,
                prefix="data_read",
                store_id=store_id,
                elapsed_s=elapsed,
                byte_count=total_bytes,
                logical_ops=len(out),
                rpc_ops=1,
            )
        return out

    def _batch_load_impl(self, keys: List[str]) -> LoadStoreSpec:
        if self.mode == "cluster":
            return self._cluster_batch_load(keys)
        return self._reference_batch_load(keys)

    def _renew_one_key(self, key: str, now_ms: int) -> None:
        loc = self.local_cache.get(key)
        if not loc:
            return
        if loc.lease_token <= 0:
            return
        cluster = self._clusters.get(loc.dn_id) or self._cluster_for(key)
        result, lease = cluster.metadata.lease_manager.renew(
            key,
            loc.lease_token,
            loc.dn_epoch,
            loc.store_epoch,
            now_ms,
        )
        if result.success and lease:
            loc.lease_expire_ms = lease.lease_expire_ms

    def _batch_renew_impl(self, keys: List[str]) -> None:
        now = self._now_ms()
        grouped: Dict[int, List[KVBlockLocation]] = {}
        for key in keys:
            loc = self.local_cache.get(key)
            if loc and loc.lease_token > 0:
                grouped.setdefault(loc.dn_id, []).append(loc)

        def renew_dn(dn_id: int, locs: List[KVBlockLocation]) -> None:
            cluster = self._clusters.get(dn_id)
            if cluster is None:
                return
            t0 = time.perf_counter() if self._om_perf_enabled else 0.0
            if hasattr(cluster.metadata, "renew_many"):
                renewals = [
                    (loc.block_hash, loc.lease_token, loc.dn_epoch, loc.store_epoch)
                    for loc in locs
                ]
                renewed = cluster.metadata.renew_many(renewals)
                for loc in locs:
                    result, lease = renewed.get(loc.block_hash, (None, None))
                    if result is not None and result.success and lease:
                        loc.lease_expire_ms = lease.lease_expire_ms
                if self._om_perf_enabled and hasattr(self, "_om_perf_renew_dn"):
                    dt = time.perf_counter() - t0
                    kdn = str(int(dn_id))
                    with self._om_perf_lock:
                        self._om_perf_renew_dn[kdn] = float(self._om_perf_renew_dn.get(kdn, 0.0)) + dt
                return
            for loc in locs:
                self._renew_one_key(loc.block_hash, now)
            if self._om_perf_enabled and hasattr(self, "_om_perf_renew_dn"):
                dt = time.perf_counter() - t0
                kdn = str(int(dn_id))
                with self._om_perf_lock:
                    self._om_perf_renew_dn[kdn] = float(self._om_perf_renew_dn.get(kdn, 0.0)) + dt

        if len(grouped) > 1:
            futs = {
                self._meta_stage_pool.submit(renew_dn, dn_id, locs): dn_id
                for dn_id, locs in grouped.items()
            }
            for fut in as_completed(futs):
                fut.result()
            return
        for dn_id, locs in grouped.items():
            renew_dn(dn_id, locs)

    def _cache_location(self, key, row, lease, dn_id: int) -> None:
        self.local_cache[key] = KVBlockLocation(
            block_hash=key,
            status=int(row.status),
            store_id=row.location.store_node_id,
            pool_offset=row.location.pool_offset,
            lease_expire_ms=lease.lease_expire_ms,
            evicted_path=row.location.evicted_path,
            lease_token=lease.lease_token,
            version=row.version,
            dn_epoch=lease.dn_epoch,
            store_epoch=lease.store_epoch,
            dn_id=dn_id,
        )

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _mk_request_id() -> str:
        return uuid.uuid4().hex
