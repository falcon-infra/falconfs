from __future__ import annotations

import os
import queue
import threading
from dataclasses import dataclass
from typing import Dict

from . import kv_metadata_service_pb2 as _kvmeta
from .reference import BlockStatus


@dataclass(frozen=True)
class _PromoteTask:
    block_hash: str
    payload: bytes
    store_id: int
    pool_offset: int
    expected_version: int
    expected_store_epoch: int
    dn_id: int


class PromoteWorker:
    """Best-effort asynchronous promote-on-read helper for evicted blocks.

    Foreground SSD reads call :meth:`enqueue`, which is intentionally
    non-blocking. Background daemon workers perform the DN allocate -> Store
    write -> DN STORED publish sequence. Dropping promote work is always safe:
    the foreground read already returned bytes from SSD and the block remains
    EVICTED until a later access reconsiders promotion.
    """

    def __init__(self, manager):
        self._manager = manager
        self._enabled = os.environ.get("FALCON_KV_ENABLE_PROMOTE_ON_READ", "1") != "0"
        self._capacity = _env_int("FALCON_KV_PROMOTE_QUEUE_CAPACITY", 1024)
        self._max_inflight = _env_int("FALCON_KV_PROMOTE_MAX_INFLIGHT", 4)
        self._min_access_count = _env_int("FALCON_KV_PROMOTE_MIN_ACCESS_COUNT", 1)
        self._pressure_below = _env_float("FALCON_KV_PROMOTE_WHEN_PRESSURE_BELOW", 1.0)
        self._q: "queue.Queue[_PromoteTask]" = queue.Queue(maxsize=max(1, self._capacity))
        self._stop = threading.Event()
        self._access_lock = threading.Lock()
        self._access_counts: Dict[str, int] = {}
        self._stats_lock = threading.Lock()
        self._inflight = 0
        self._stats = {
            "enqueued": 0,
            "dropped_disabled": 0,
            "dropped_admission": 0,
            "dropped_pressure": 0,
            "dropped_queue_full": 0,
            "dropped_full": 0,
            "promoted": 0,
            "failed": 0,
            "failed_alloc": 0,
            "failed_write": 0,
            "failed_status_update": 0,
        }
        self._threads = []
        if self._enabled:
            for i in range(max(1, self._max_inflight)):
                t = threading.Thread(
                    target=self._run,
                    name=f"kv-promote-{i}",
                    daemon=True,
                )
                t.start()
                self._threads.append(t)

    def close(self, timeout: float = 1.0) -> None:
        self._stop.set()
        for t in list(self._threads):
            t.join(timeout=timeout)

    def stats(self) -> Dict[str, int]:
        with self._stats_lock:
            out = dict(self._stats)
        out["queue_size"] = self._q.qsize()
        out["queue_capacity"] = max(1, self._capacity)
        out["max_inflight"] = max(1, self._max_inflight)
        out["min_access_count"] = max(1, self._min_access_count)
        out["pressure_threshold"] = self._pressure_below
        out["inflight"] = self._inflight
        return out

    def enqueue(
        self,
        block_hash: str,
        payload: bytes,
        store_id: int,
        pool_offset: int,
        expected_version: int,
        expected_store_epoch: int,
        dn_id: int,
    ) -> None:
        if not self._enabled:
            self._bump("dropped_disabled")
            return
        if not self._admit_pressure():
            self._bump("dropped_pressure")
            return
        if not self._admit(block_hash):
            self._bump("dropped_admission")
            return
        task = _PromoteTask(
            block_hash=block_hash,
            payload=payload,
            store_id=store_id,
            pool_offset=pool_offset,
            expected_version=expected_version,
            expected_store_epoch=expected_store_epoch,
            dn_id=dn_id,
        )
        try:
            self._q.put_nowait(task)
            self._bump("enqueued")
        except queue.Full:
            self._bump("dropped_queue_full")
            self._bump("dropped_full")

    def _admit_pressure(self) -> bool:
        probe = getattr(self._manager, "promote_pressure_ratio", None)
        if not callable(probe):
            return True
        try:
            ratio = float(probe())
        except Exception:
            return True
        return ratio <= float(self._pressure_below)

    def _admit(self, block_hash: str) -> bool:
        need = max(1, self._min_access_count)
        if need <= 1:
            return True
        with self._access_lock:
            n = self._access_counts.get(block_hash, 0) + 1
            self._access_counts[block_hash] = n
            return n >= need

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                task = self._q.get(timeout=0.1)
            except queue.Empty:
                continue
            ok = False
            try:
                self._set_inflight(1)
                ok = self._promote_one(task)
            except Exception:
                ok = False
            finally:
                self._set_inflight(-1)
                self._q.task_done()
                self._bump("promoted" if ok else "failed")

    def _promote_one(self, task: _PromoteTask) -> bool:
        cluster = self._manager._clusters.get(task.dn_id)
        if cluster is None:
            return False
        try:
            alloc_map = cluster.metadata.allocate(
                [task.block_hash],
                request_id=f"promote_alloc:{task.block_hash}",
                client_id=self._manager.client_id,
                allocate_hint=_kvmeta.ALLOCATE_HINT_PROMOTE_FROM_EVICTED,
            )
        except TypeError:
            alloc_map = cluster.metadata.allocate(
                [task.block_hash],
                request_id=f"promote_alloc:{task.block_hash}",
                client_id=self._manager.client_id,
            )
        alloc_result = alloc_map.get(task.block_hash) if alloc_map else None
        if not alloc_result:
            self._bump("failed_alloc")
            return False
        alloc_status, alloc_row, _ = alloc_result
        if not alloc_status.success or alloc_row is None:
            self._bump("failed_alloc")
            return False
        write = cluster.store.write(
            alloc_row.location.store_node_id,
            alloc_row.location.pool_offset,
            task.payload,
            alloc_row.location.store_epoch,
            self._manager.block_size,
            expected_version=alloc_row.version,
            block_hash=task.block_hash.encode("utf-8"),
        )
        if not write.success:
            self._bump("failed_write")
            return False
        result, row = cluster.metadata.update_status(
            task.block_hash,
            BlockStatus.ALLOCATED,
            BlockStatus.STORED,
            alloc_row.version,
            request_id=f"promote:{task.block_hash}",
            client_id=self._manager.client_id,
        )
        if not (result.success and row):
            self._bump("failed_status_update")
            return False
        loc = self._manager.local_cache.get(task.block_hash)
        if loc is not None:
            loc.status = int(BlockStatus.STORED)
            loc.version = row.version
            loc.store_id = alloc_row.location.store_node_id
            loc.pool_offset = alloc_row.location.pool_offset
            loc.store_epoch = alloc_row.location.store_epoch
        return True

    def _bump(self, key: str) -> None:
        with self._stats_lock:
            self._stats[key] = int(self._stats.get(key, 0)) + 1

    def _set_inflight(self, delta: int) -> None:
        with self._stats_lock:
            self._inflight = max(0, int(self._inflight) + int(delta))


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return max(1, int(raw))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default
