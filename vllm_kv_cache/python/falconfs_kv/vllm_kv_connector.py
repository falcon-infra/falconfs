from __future__ import annotations

import hashlib
import json
import os
import socket
import threading
import time
from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

import torch

from .offloading_manager import (
    FalconFSOffloadingManager,
    KVBlockLocation,
    STATUS_ALLOCATED,
    STATUS_EVICTED,
    STATUS_STORED,
)

from vllm.config import VllmConfig
from vllm.distributed.kv_events import KVCacheEvent
from vllm.distributed.kv_transfer.kv_connector.v1 import (
    KVConnectorBase_V1,
    KVConnectorRole,
)
from vllm.distributed.kv_transfer.kv_connector.v1.base import KVConnectorMetadata
from vllm.distributed.kv_transfer.kv_connector.v1.metrics import (
    KVConnectorPromMetrics,
    KVConnectorStats,
    PromMetric,
    PromMetricT,
)
from vllm.distributed.kv_transfer.kv_connector.v1.offloading.metrics import (
    OffloadingConnectorStats,
    OffloadPromMetrics,
)
from vllm.distributed.kv_transfer.kv_connector.v1.offloading.scheduler import (
    OffloadingConnectorScheduler,
)
from vllm.distributed.kv_transfer.kv_connector.v1.offloading.worker import (
    OffloadingConnectorWorker,
)
from vllm.forward_context import ForwardContext
from vllm.v1.attention.backend import AttentionBackend, AttentionMetadata
from vllm.v1.core.kv_cache_manager import KVCacheBlocks
from vllm.v1.core.sched.output import SchedulerOutput
from vllm.v1.kv_cache_interface import KVCacheConfig
from vllm.v1.kv_offload.abstract import (
    LoadStoreSpec,
    OffloadingEvent,
    OffloadingManager,
    OffloadKey,
    PrepareStoreOutput,
    ReqContext,
    get_offload_block_hash,
    get_offload_group_idx,
)
from vllm.v1.kv_offload.mediums import GPULoadStoreSpec
from vllm.v1.kv_offload.spec import (
    CanonicalKVCaches,
    OffloadingSpec,
)
from vllm.v1.kv_offload.worker.worker import (
    OffloadingHandler,
    TransferResult,
    TransferSpec,
)
from vllm.v1.outputs import KVConnectorOutput
from vllm.v1.request import Request


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _config_bool(
    extra: dict[str, Any],
    key: str,
    env_name: str,
    default: bool,
) -> bool:
    if key in extra:
        return _truthy(extra.get(key))
    raw = os.environ.get(env_name)
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return _truthy(raw)


def _config_int(
    extra: dict[str, Any],
    key: str,
    env_name: str,
    default: int,
) -> int:
    raw = extra.get(key, os.environ.get(env_name, default))
    try:
        return int(raw)
    except (TypeError, ValueError):
        return int(default)


def _config_attr(obj: Any, name: str, default: Any = "") -> Any:
    if obj is None:
        return default
    return getattr(obj, name, default)


@dataclass(frozen=True)
class FalconFSKeyNamespace:
    key_version: str = "v1"
    namespace: str = "default"
    tenant: str = "default"
    model_id: str = "unknown-model"
    hash_algo: str = "sha256"
    cache_salt: str = ""
    kv_layout_version: str = "vllm-v1"
    block_size: str = ""
    lora_name: str = ""
    legacy_unnamespaced_keys: bool = False

    @classmethod
    def from_vllm_config(
        cls,
        vllm_config: VllmConfig | None,
        extra_config: dict[str, Any] | None,
    ) -> "FalconFSKeyNamespace":
        extra = extra_config or {}
        cache_config = _config_attr(vllm_config, "cache_config", None)
        model_config = _config_attr(vllm_config, "model_config", None)
        model_id = (
            extra.get("falconfs_model_id")
            or _config_attr(model_config, "model", "")
            or _config_attr(model_config, "served_model_name", "")
            or _config_attr(model_config, "model_name", "")
            or "unknown-model"
        )
        hash_algo = (
            extra.get("falconfs_hash_algo")
            or _config_attr(cache_config, "prefix_caching_hash_algo", "")
            or "sha256"
        )
        block_size = extra.get("falconfs_block_size") or extra.get("block_size")
        if block_size is None or block_size == "":
            block_size = _config_attr(cache_config, "block_size", "")
        return cls(
            key_version=str(extra.get("falconfs_key_version", "v1")),
            namespace=str(extra.get("falconfs_namespace", "default")),
            tenant=str(extra.get("falconfs_tenant", "default")),
            model_id=str(model_id),
            hash_algo=str(hash_algo),
            cache_salt=str(extra.get("falconfs_cache_salt", "")),
            kv_layout_version=str(extra.get("falconfs_kv_layout_version", "vllm-v1")),
            block_size=str(block_size),
            lora_name=str(
                extra.get("falconfs_lora_name", extra.get("falconfs_lora_id", ""))
            ),
            legacy_unnamespaced_keys=_truthy(
                extra.get("falconfs_legacy_unnamespaced_keys", False)
            ),
        )

    def to_falcon_key(self, key: OffloadKey | bytes | str) -> str:
        if isinstance(key, str):
            return key
        if self.legacy_unnamespaced_keys:
            return offload_key_to_falcon_key(key)
        raw = bytes(key)
        try:
            block_hash = get_offload_block_hash(OffloadKey(raw)).hex()
            group_idx = get_offload_group_idx(OffloadKey(raw))
        except Exception:
            block_hash = raw.hex()
            group_idx = 0
        payload = {
            "key_version": self.key_version,
            "namespace": self.namespace,
            "tenant": self.tenant,
            "model_id": self.model_id,
            "hash_algo": self.hash_algo,
            "cache_salt": self.cache_salt,
            "kv_layout_version": self.kv_layout_version,
            "block_size": self.block_size,
            "lora_name": self.lora_name,
            "group_idx": int(group_idx),
            "block_hash": block_hash,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        digest = hashlib.sha256(encoded).hexdigest()
        return f"ffkv:{self.key_version}:{self.namespace}:{self.tenant}:{digest}"

    def metric_labels(self) -> dict[str, str]:
        return {
            "falconfs_key_version": self.key_version,
            "falconfs_namespace": self.namespace,
            "falconfs_tenant": self.tenant,
            "falconfs_model_id": self.model_id,
            "falconfs_hash_algo": self.hash_algo,
            "falconfs_kv_layout_version": self.kv_layout_version,
            "falconfs_block_size": self.block_size,
            "falconfs_lora_name": self.lora_name,
            "falconfs_legacy_unnamespaced_keys": str(self.legacy_unnamespaced_keys),
        }


def offload_key_to_falcon_key(key: OffloadKey | bytes | str) -> str:
    """Encode a vLLM OffloadKey into FalconFS's string block-hash namespace."""
    if isinstance(key, str):
        return key
    return bytes(key).hex()


def _metadata_block_hash_to_str(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _append_metrics_record(record: dict[str, Any]) -> None:
    path = os.environ.get("FALCON_KV_VLLM_METRICS_JSONL", "").strip()
    if not path:
        return
    try:
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(record)
        payload.setdefault("pid", os.getpid())
        payload.setdefault("time_ns", time.time_ns())
        with out.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, sort_keys=True) + "\n")
    except Exception:
        # Metrics export is test/diagnostic-only and must never break serving.
        return


@dataclass(frozen=True)
class FalconFSBlockLocationSpec:
    offload_key: OffloadKey
    falcon_key: str
    dn_id: int
    store_id: int
    pool_offset: int
    store_epoch: int
    version: int = 0
    status: int = STATUS_STORED
    evicted_path: str | None = None


class FalconFSLoadStoreSpec(LoadStoreSpec):
    """vLLM LoadStoreSpec carrying prepared FalconFS block locations."""

    def __init__(
        self,
        keys: Iterable[OffloadKey],
        locations: Iterable[FalconFSBlockLocationSpec] = (),
    ):
        self.keys = list(keys)
        self.locations = list(locations)
        self.location_by_key = {loc.offload_key: loc for loc in self.locations}
        self.falcon_keys = [
            self.location_by_key[key].falcon_key
            if key in self.location_by_key
            else offload_key_to_falcon_key(key)
            for key in self.keys
        ]

    @staticmethod
    def medium() -> str:
        return "FalconFS"

    def __repr__(self) -> str:
        return f"FalconFSLoadStoreSpec(keys={len(self.keys)}, locations={len(self.locations)})"


class FalconFSOffloadingManagerAdapter(OffloadingManager):
    """Adapter from vLLM kv_offload.OffloadingManager to FalconFS metadata APIs.

    The critical method is ``lookup_many``. vLLM's generic scheduler probes a
    prefix by calling ``lookup`` one block at a time; FalconFS must batch those
    probes into DN-level BatchLookupWithLease calls.
    """

    def __init__(
        self,
        vllm_config: VllmConfig | None = None,
        kv_cache_config: KVCacheConfig | None = None,
        inner_manager: FalconFSOffloadingManager | None = None,
        metrics_label: str = "adapter",
    ):
        self.vllm_config = vllm_config
        self.kv_cache_config = kv_cache_config
        self.extra_config: dict[str, Any] = {}
        kv_transfer_config = None
        if vllm_config is not None and vllm_config.kv_transfer_config is not None:
            kv_transfer_config = vllm_config.kv_transfer_config
            self.extra_config = dict(kv_transfer_config.kv_connector_extra_config)
        self.key_namespace = FalconFSKeyNamespace.from_vllm_config(
            vllm_config, self.extra_config
        )
        self.kv_role = str(getattr(kv_transfer_config, "kv_role", None) or "kv_both")
        self.load_failure_policy = str(
            self.extra_config.get(
                "falconfs_load_failure_policy",
                self.extra_config.get("load_failure_policy", "recompute"),
            )
        ).strip().lower()
        if self.load_failure_policy not in ("recompute", "fail"):
            raise ValueError(
                "falconfs_load_failure_policy must be 'recompute' or 'fail', "
                f"got {self.load_failure_policy!r}"
            )
        self.async_load_enabled = _config_bool(
            self.extra_config,
            "falconfs_async_load",
            "FALCON_KV_VLLM_ASYNC_LOAD",
            True,
        )
        self.layerwise_load_enabled = _config_bool(
            self.extra_config,
            "falconfs_layerwise_load",
            "FALCON_KV_VLLM_LAYERWISE_LOAD",
            False,
        )
        self.layer_prefetch_depth = max(
            1,
            _config_int(
                self.extra_config,
                "falconfs_layer_prefetch_depth",
                "FALCON_KV_VLLM_LAYER_PREFETCH_DEPTH",
                2,
            ),
        )
        self.manager = inner_manager or self._build_manager()
        self.metrics_label = metrics_label
        self.medium = FalconFSLoadStoreSpec.medium()
        self._lookup_cache: OrderedDict[Any, dict[OffloadKey, bool | None]] = OrderedDict()
        self._lookup_cache_lock = threading.Lock()
        self._lookup_cache_capacity = int(self.extra_config.get("lookup_cache_capacity", 1024))
        self._events: list[OffloadingEvent] = []
        self._stats_lock = threading.Lock()
        self._stats: dict[str, int] = {
            "lookup_calls": 0,
            "lookup_many_calls": 0,
            "lookup_many_keys": 0,
            "lookup_cache_hits": 0,
            "lookup_cache_misses": 0,
            "lookup_batches": 0,
            "prepare_load_keys": 0,
            "prepare_store_keys": 0,
            "data_read_blocks": 0,
            "data_write_blocks": 0,
            "data_read_bytes": 0,
            "data_write_bytes": 0,
            "forbidden_single_key_lookup_count": 0,
            "role_producer_load_skips": 0,
            "role_consumer_store_skips": 0,
            "producer_store_blocks": 0,
            "consumer_load_hits": 0,
            "consumer_load_misses": 0,
            "gpu_d2h_copies": 0,
            "gpu_h2d_copies": 0,
            "gpu_d2h_bytes": 0,
            "gpu_h2d_bytes": 0,
            "pinned_staging_allocations": 0,
            "pinned_staging_reuses": 0,
            "pinned_staging_fallbacks": 0,
            "transfer_failures": 0,
            "load_failures": 0,
            "store_failures": 0,
            "whole_block_loads": 0,
            "layerwise_loads": 0,
            "layerwise_async_jobs": 0,
            "layerwise_sync_waits": 0,
            "payload_fetch_ms": 0,
            "layer_copy_ms": 0,
            "layer_wait_ms": 0,
            "payload_cache_bytes": 0,
            "layerwise_fallbacks": 0,
            "fallback_invalid_layout": 0,
            "fallback_unknown_layer": 0,
        }

    def _build_manager(self) -> FalconFSOffloadingManager:
        block_bytes = int(
            self.extra_config.get(
                "falconfs_block_bytes",
                self.extra_config.get("block_bytes", self.extra_config.get("kv_block_bytes", 2 * 1024 * 1024)),
            )
        )
        cn_conninfo = self.extra_config.get("cn_conninfo")
        shard_table = self.extra_config.get("shard_table")
        if isinstance(shard_table, dict):
            shard_table = {int(k): str(v) for k, v in shard_table.items()}
        if not shard_table and not cn_conninfo:
            shard_table = {1: self.extra_config.get("dn_endpoint", "unused")}
        mode = "cluster" if cn_conninfo else str(self.extra_config.get("mode", "reference"))
        client_id = int(self.extra_config.get("client_id", os.getpid() & 0x7fffffff))
        client_hostname = str(self.extra_config.get("client_hostname", socket.gethostname()))
        return FalconFSOffloadingManager(
            client_id=client_id,
            client_hostname=client_hostname,
            shard_table=shard_table,
            mode=mode,
            cn_conninfo=cn_conninfo,
            block_size=block_bytes,
        )

    def can_load(self) -> bool:
        return self.kv_role in ("kv_both", "kv_consumer")

    def can_store(self) -> bool:
        return self.kv_role in ("kv_both", "kv_producer")

    def _to_falcon_key(self, key: OffloadKey | bytes | str) -> str:
        return self.key_namespace.to_falcon_key(key)

    def _to_falcon_keys(self, keys: Iterable[OffloadKey | bytes | str]) -> list[str]:
        return [self._to_falcon_key(key) for key in keys]

    def _metric_context(self) -> dict[str, Any]:
        labels: dict[str, Any] = self.key_namespace.metric_labels()
        labels["kv_role"] = self.kv_role
        labels["load_failure_policy"] = self.load_failure_policy
        labels["async_load_enabled"] = self.async_load_enabled
        labels["layerwise_load_enabled"] = self.layerwise_load_enabled
        labels["layer_prefetch_depth"] = self.layer_prefetch_depth
        return labels

    def _bump(self, key: str, count: int = 1) -> None:
        snapshot: dict[str, Any]
        with self._stats_lock:
            self._stats[key] = int(self._stats.get(key, 0)) + int(count)
            snapshot = dict(self._stats)
        if os.environ.get("FALCON_KV_VLLM_METRICS_EAGER", "1") != "0":
            _append_metrics_record({
                "event": "counter",
                "label": self.metrics_label,
                "counter": key,
                "delta": int(count),
                "stats": snapshot,
                **self._metric_context(),
            })

    def export_metrics(self, event: str = "snapshot") -> None:
        _append_metrics_record({
            "event": event,
            "label": self.metrics_label,
            "stats": self.stats_snapshot(),
            **self._metric_context(),
        })

    def stats_snapshot(self) -> dict[str, Any]:
        with self._stats_lock:
            out: dict[str, Any] = dict(self._stats)
        out.update(self._metric_context())
        return out

    def _request_cache_key(self, keys: list[OffloadKey], req_context: ReqContext | None) -> Any:
        params = getattr(req_context, "kv_transfer_params", None) if req_context is not None else None
        if isinstance(params, dict):
            for name in ("request_id", "req_id", "request", "transfer_id"):
                value = params.get(name)
                if value is not None:
                    return ("request", str(value))
        return ("keys", tuple(bytes(key) for key in keys))

    def clear_lookup_cache(
        self,
        keys: Iterable[OffloadKey] | None = None,
        req_context: ReqContext | None = None,
    ) -> None:
        with self._lookup_cache_lock:
            if keys is None and req_context is None:
                self._lookup_cache.clear()
                return
            key_set = set(keys or ())
            if req_context is not None and not key_set:
                marker = self._request_cache_key([], req_context)
                self._lookup_cache.pop(marker, None)
                return
            for cache_key in list(self._lookup_cache.keys()):
                cached = self._lookup_cache[cache_key]
                for key in key_set:
                    cached.pop(key, None)
                if not cached:
                    self._lookup_cache.pop(cache_key, None)

    def lookup_many(
        self,
        keys: Iterable[OffloadKey],
        req_context: ReqContext | None = None,
    ) -> dict[OffloadKey, bool | None]:
        key_list = list(keys)
        self._bump("lookup_many_calls")
        self._bump("lookup_many_keys", len(key_list))
        if not key_list:
            return {}
        if not self.can_load():
            self._bump("role_producer_load_skips", len(key_list))
            return {key: False for key in key_list}
        cache_key = self._request_cache_key(key_list, req_context)
        with self._lookup_cache_lock:
            cached = self._lookup_cache.setdefault(cache_key, {})
            out = {key: cached[key] for key in key_list if key in cached}
            missing = [key for key in key_list if key not in cached]
            self._bump("lookup_cache_hits", len(out))
            self._bump("lookup_cache_misses", len(missing))
        if missing:
            falcon_keys = self._to_falcon_keys(missing)
            self._bump("lookup_batches")
            raw = self.manager.batch_lookup(falcon_keys, req_context)
            fresh = {key: raw.get(falcon_key, False) for key, falcon_key in zip(missing, falcon_keys)}
            with self._lookup_cache_lock:
                cached = self._lookup_cache.setdefault(cache_key, {})
                cached.update(fresh)
                self._lookup_cache.move_to_end(cache_key)
                while len(self._lookup_cache) > self._lookup_cache_capacity:
                    self._lookup_cache.popitem(last=False)
            out.update(fresh)
        return {key: out.get(key, False) for key in key_list}

    def lookup(self, key: OffloadKey, req_context: ReqContext) -> bool | None:
        self._bump("lookup_calls")
        self._bump("forbidden_single_key_lookup_count")
        if _truthy(os.environ.get("FALCON_KV_VLLM_FORBID_SINGLE_LOOKUP", False)):
            raise RuntimeError("FalconFSConnector requires batched lookup_many(); single-key lookup() is disabled")
        return self.lookup_many([key], req_context).get(key, False)

    def _locations_for(self, keys: list[OffloadKey]) -> list[FalconFSBlockLocationSpec]:
        locations: list[FalconFSBlockLocationSpec] = []
        for key in keys:
            falcon_key = self._to_falcon_key(key)
            loc = self.manager.local_cache.get(falcon_key)
            if loc is None:
                continue
            locations.append(
                FalconFSBlockLocationSpec(
                    offload_key=key,
                    falcon_key=falcon_key,
                    dn_id=int(getattr(loc, "dn_id", 0)),
                    store_id=int(loc.store_id),
                    pool_offset=int(loc.pool_offset),
                    store_epoch=int(getattr(loc, "store_epoch", 0)),
                    version=int(getattr(loc, "version", 0)),
                    status=int(getattr(loc, "status", STATUS_STORED)),
                    evicted_path=getattr(loc, "evicted_path", None),
                )
            )
        return locations

    def _seed_locations(self, spec: FalconFSLoadStoreSpec) -> None:
        for loc in spec.locations:
            self.manager.local_cache[loc.falcon_key] = KVBlockLocation(
                block_hash=loc.falcon_key,
                status=int(loc.status),
                store_id=int(loc.store_id),
                pool_offset=int(loc.pool_offset),
                lease_expire_ms=0,
                evicted_path=loc.evicted_path,
                lease_token=0,
                version=int(loc.version),
                store_epoch=int(loc.store_epoch),
                dn_id=int(loc.dn_id),
            )

    def prepare_load(self, keys: Iterable[OffloadKey], req_context: ReqContext) -> FalconFSLoadStoreSpec:
        key_list = list(keys)
        if not self.can_load():
            self._bump("role_producer_load_skips", len(key_list))
            raise RuntimeError("FalconFS kv_producer role cannot prepare loads")
        self._bump("prepare_load_keys", len(key_list))
        falcon_keys = self._to_falcon_keys(key_list)
        lookup = self.manager._batch_lookup_impl(falcon_keys, req_context, renew_lease_on_hit=True)
        missing = [key for key, falcon_key in zip(key_list, falcon_keys) if not lookup.get(falcon_key)]
        if missing:
            self._bump("consumer_load_misses", len(missing))
            self._bump("load_failures", len(missing))
            self.clear_lookup_cache(missing, req_context=req_context)
            if self.load_failure_policy == "recompute":
                return FalconFSLoadStoreSpec([], [])
            raise RuntimeError(f"FalconFS blocks not found for load: {missing!r}")
        self._bump("consumer_load_hits", len(key_list))
        return FalconFSLoadStoreSpec(key_list, self._locations_for(key_list))

    def complete_load(self, keys: Iterable[OffloadKey]) -> None:
        key_list = list(keys)
        self.manager.complete_load(self._to_falcon_keys(key_list))
        self.clear_lookup_cache(key_list)

    def prepare_store(self, keys: Iterable[OffloadKey], req_context: ReqContext) -> PrepareStoreOutput | None:
        key_list = list(keys)
        if not self.can_store():
            self._bump("role_consumer_store_skips", len(key_list))
            return PrepareStoreOutput([], FalconFSLoadStoreSpec([]), [])
        self._bump("prepare_store_keys", len(key_list))
        if not key_list:
            return PrepareStoreOutput([], FalconFSLoadStoreSpec([]), [])
        falcon_keys = self._to_falcon_keys(key_list)
        spec = self.manager._batch_prepare_store_impl(falcon_keys, req_context)
        by_falcon = {key: offload for key, offload in zip(falcon_keys, key_list)}
        keys_to_store: list[OffloadKey] = []
        for item in spec.specs:
            falcon_key = _metadata_block_hash_to_str(item.get("block_hash"))
            offload_key = by_falcon.get(falcon_key)
            if offload_key is not None:
                keys_to_store.append(offload_key)
        store_spec = FalconFSLoadStoreSpec(keys_to_store, self._locations_for(keys_to_store))
        return PrepareStoreOutput(keys_to_store=keys_to_store, store_spec=store_spec, evicted_keys=[])

    def complete_store(self, keys: Iterable[OffloadKey], success: bool = True) -> None:
        key_list = list(keys)
        falcon_keys = self._to_falcon_keys(key_list)
        if success:
            if falcon_keys and self.can_store():
                self.manager._batch_mark_stored(falcon_keys, f"falconfs-vllm:{time.time_ns()}")
                self._bump("producer_store_blocks", len(falcon_keys))
                self._events.append(OffloadingEvent(keys=key_list, medium=self.medium, removed=False))
        else:
            self.manager._free_allocated_for_keys(falcon_keys)
            if key_list:
                self._events.append(OffloadingEvent(keys=key_list, medium=self.medium, removed=True))
        self.clear_lookup_cache(key_list)

    def touch(self, keys: Iterable[OffloadKey]) -> None:
        key_list = list(keys)
        if key_list:
            self.manager.touch(self._to_falcon_keys(key_list))

    def take_events(self) -> Iterable[OffloadingEvent]:
        yield from self._events
        self._events.clear()

    def read_prepared_payloads(self, spec: FalconFSLoadStoreSpec) -> dict[OffloadKey, bytes | memoryview]:
        self._seed_locations(spec)
        loaded = self.manager._batch_load_impl(spec.falcon_keys).data
        out = {key: loaded[falcon_key] for key, falcon_key in zip(spec.keys, spec.falcon_keys) if falcon_key in loaded}
        self._bump("data_read_blocks", len(out))
        self._bump("data_read_bytes", sum(len(memoryview(v)) for v in out.values()))
        return out

    def write_prepared_payloads(
        self,
        spec: FalconFSLoadStoreSpec,
        payloads: dict[OffloadKey, bytes | bytearray | memoryview],
    ) -> bool:
        if not self.can_store():
            self._bump("role_consumer_store_skips", len(spec.locations))
            return True
        self._seed_locations(spec)
        data: dict[str, Any] = {
            loc.falcon_key: memoryview(payloads[loc.offload_key])
            for loc in spec.locations
            if loc.offload_key in payloads
        }
        if len(data) != len(spec.locations):
            self._bump("store_failures")
            return False
        batch_id = f"falconfs-vllm-data:{time.time_ns()}"
        ok: dict[str, bool] = {}
        grouped: dict[tuple[int, int], list[str]] = {}
        for loc in spec.locations:
            grouped.setdefault((loc.dn_id, loc.store_id), []).append(loc.falcon_key)
        if hasattr(self.manager, "_complete_store_write_key_group"):
            for (dn_id, store_id), keys in grouped.items():
                ok.update(
                    self.manager._complete_store_write_key_group(
                        dn_id, store_id, keys, data, batch_id
                    )
                )
        else:
            for loc in spec.locations:
                ok[loc.falcon_key] = self.manager._complete_store_write_one_key(
                    loc.falcon_key, data, batch_id, None
                )
        success = all(ok.get(loc.falcon_key, False) for loc in spec.locations)
        if success:
            self._bump("data_write_blocks", len(spec.locations))
            self._bump("data_write_bytes", sum(len(memoryview(v)) for v in payloads.values()))
        else:
            self._bump("store_failures")
        return success

    def shutdown(self) -> None:
        self.export_metrics("shutdown")
        self.clear_lookup_cache()
        for pool_name in ("_data_stage_pool", "_meta_stage_pool"):
            pool = getattr(self.manager, pool_name, None)
            if pool is not None:
                pool.shutdown(wait=False, cancel_futures=True)


class FalconFSConnectorScheduler(OffloadingConnectorScheduler):
    """Offloading scheduler with batch-shaped FalconFS prefix lookup."""

    def __init__(self, spec: OffloadingSpec):
        super().__init__(spec)
        if not hasattr(self.manager, "lookup_many"):
            raise TypeError("FalconFSConnectorScheduler requires manager.lookup_many")

    def _maximal_prefix_lookup(self, keys: Iterable[OffloadKey], req_context: ReqContext) -> int | None:
        key_list = list(keys)
        results = self.manager.lookup_many(key_list, req_context)  # type: ignore[attr-defined]
        hit_count = 0
        defer_lookup = False
        for key in key_list:
            result = results.get(key, False)
            if result is None:
                defer_lookup = True
                result = True
            if not result:
                break
            hit_count += 1
        return hit_count if not defer_lookup else None

    def _sliding_window_lookup(
        self,
        keys: list[OffloadKey],
        sliding_window_size: int,
        req_context: ReqContext,
    ) -> int | None:
        results = self.manager.lookup_many(keys, req_context)  # type: ignore[attr-defined]
        defer_lookup = False
        consecutive_hits = 0
        for idx in range(len(keys) - 1, -1, -1):
            result = results.get(keys[idx], False)
            if result is None:
                defer_lookup = True
                result = False
            if not result:
                consecutive_hits = 0
            else:
                consecutive_hits += 1
                if consecutive_hits == sliding_window_size:
                    return idx + sliding_window_size if not defer_lookup else None
        return consecutive_hits if not defer_lookup else None

    def _can_load(self) -> bool:
        return bool(getattr(self.manager, "can_load", lambda: True)())

    def _can_store(self) -> bool:
        return bool(getattr(self.manager, "can_store", lambda: True)())

    def get_num_new_matched_tokens(
        self, request: Request, num_computed_tokens: int
    ) -> tuple[int | None, bool]:
        if not self._can_load():
            bump = getattr(self.manager, "_bump", None)
            if callable(bump):
                bump("role_producer_load_skips")
            return 0, False
        return super().get_num_new_matched_tokens(request, num_computed_tokens)

    def update_state_after_alloc(
        self, request: Request, blocks: KVCacheBlocks, num_external_tokens: int
    ):
        if not self._can_load():
            return None
        return super().update_state_after_alloc(request, blocks, num_external_tokens)

    def _get_reqs_to_store(self, scheduler_output: SchedulerOutput):
        if not self._can_store():
            bump = getattr(self.manager, "_bump", None)
            if callable(bump):
                bump("role_consumer_store_skips")
            return {}
        return super()._get_reqs_to_store(scheduler_output)

    def request_finished(
        self,
        request: Request,
        block_ids: list[int],
    ) -> tuple[bool, dict[str, Any] | None]:
        if not self._can_store():
            self._req_status.pop(request.request_id, None)
            clear = getattr(self.manager, "clear_lookup_cache", None)
            if callable(clear):
                clear(req_context=ReqContext(kv_transfer_params=getattr(request, "kv_transfer_params", None)))
            return False, None
        return super().request_finished(request, block_ids)


class _TorchHostBufferPool:
    def __init__(self, adapter: FalconFSOffloadingManagerAdapter):
        self.adapter = adapter
        self._free: dict[tuple[int, bool], list[torch.Tensor]] = {}
        self._lock = threading.Lock()

    def acquire(self, nbytes: int, prefer_pinned: bool) -> torch.Tensor:
        nbytes = int(nbytes)
        key = (nbytes, bool(prefer_pinned))
        with self._lock:
            bucket = self._free.get(key)
            if bucket:
                bump = getattr(self.adapter, "_bump", None)
                if callable(bump):
                    bump("pinned_staging_reuses")
                return bucket.pop()
        try:
            buf = torch.empty(nbytes, dtype=torch.uint8, device="cpu", pin_memory=bool(prefer_pinned))
        except Exception:
            if prefer_pinned:
                bump = getattr(self.adapter, "_bump", None)
                if callable(bump):
                    bump("pinned_staging_fallbacks")
            buf = torch.empty(nbytes, dtype=torch.uint8, device="cpu")
        bump = getattr(self.adapter, "_bump", None)
        if callable(bump):
            bump("pinned_staging_allocations")
        return buf

    def release(self, buf: torch.Tensor) -> None:
        key = (int(buf.numel()), bool(getattr(buf, "is_pinned", lambda: False)()))
        with self._lock:
            self._free.setdefault(key, []).append(buf)


@dataclass(frozen=True)
class _LayerCopySpan:
    layer_name: str
    payload_offset: int
    tensor_idx: int
    tensor_offset: int
    nbytes: int


@dataclass
class _LayerLoadJob:
    job_id: int
    transfer_spec: TransferSpec
    layer_names: list[str]
    start_time: float
    payload_future: Future[dict[OffloadKey, bytes | memoryview]] | None = None
    payloads: dict[OffloadKey, bytes | memoryview] | None = None
    payload_size: int = 0
    copied_layers: set[str] = field(default_factory=set)
    copy_futures: dict[str, Future[None]] = field(default_factory=dict)
    completed_result: TransferResult | None = None
    failed: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)


class _FalconFSTransferHandler(OffloadingHandler):
    def __init__(
        self,
        adapter: FalconFSOffloadingManagerAdapter,
        kv_caches: CanonicalKVCaches,
        block_size_factor: int,
        gpu_to_falconfs: bool,
    ):
        self.adapter = adapter
        self.kv_caches = kv_caches
        self.block_size_factor = int(block_size_factor)
        self.gpu_to_falconfs = bool(gpu_to_falconfs)
        self.transfer_type = ("GPU", "FalconFS") if self.gpu_to_falconfs else ("FalconFS", "GPU")
        self.executor = ThreadPoolExecutor(
            max_workers=_env_int("FALCON_KV_VLLM_TRANSFER_WORKERS", 4),
            thread_name_prefix="falconfs-vllm-xfer",
        )
        self._futures: dict[int, Future[TransferResult]] = {}
        self._completed_results: list[TransferResult] = []
        self._layer_jobs: dict[int, _LayerLoadJob] = {}
        self._layer_layout: dict[str, list[_LayerCopySpan]] | None | bool = None
        self._failed_load_block_ids: set[int] = set()
        self._lock = threading.Lock()
        self._host_pool = _TorchHostBufferPool(adapter)

    def _bump(self, key: str, count: int = 1) -> None:
        bump = getattr(self.adapter, "_bump", None)
        if callable(bump):
            bump(key, count)

    def _bump_ms(self, key: str, start: float) -> None:
        self._bump(key, max(1, int((time.perf_counter() - start) * 1000)))

    def _group_refs(self):
        if len(self.kv_caches.group_data_refs) != 1:
            raise NotImplementedError(
                "FalconFSConnector currently supports one KV cache group; "
                "multi-group/HMA models must be gated before serving."
            )
        return self.kv_caches.group_data_refs[0]

    def _layer_names(self) -> list[str]:
        groups = getattr(self.adapter.kv_cache_config, "kv_cache_groups", None)
        if not groups:
            return []
        return list(getattr(groups[0], "layer_names", ()) or ())

    def _payload_bytes_per_gpu_block(self) -> int:
        return sum(int(ref.page_size_bytes) for ref in self._group_refs())

    def _get_layer_layout(self) -> dict[str, list[_LayerCopySpan]] | None:
        if self._layer_layout is False:
            return None
        if isinstance(self._layer_layout, dict):
            return self._layer_layout

        layer_names = self._layer_names()
        refs = self._group_refs()
        if not layer_names or not refs:
            self._layer_layout = False
            return None

        layout: dict[str, list[_LayerCopySpan]] = {name: [] for name in layer_names}
        if len(refs) == 1 and len(layer_names) > 1:
            # Cross-layer cache: one row contains all layers contiguously.
            total = int(refs[0].page_size_bytes)
            if total <= 0 or total % len(layer_names) != 0:
                self._layer_layout = False
                return None
            per_layer = total // len(layer_names)
            for idx, layer_name in enumerate(layer_names):
                offset = idx * per_layer
                layout[layer_name].append(
                    _LayerCopySpan(
                        layer_name=layer_name,
                        payload_offset=offset,
                        tensor_idx=int(refs[0].tensor_idx),
                        tensor_offset=offset,
                        nbytes=per_layer,
                    )
                )
            self._layer_layout = layout
            return layout

        if len(refs) % len(layer_names) != 0:
            self._layer_layout = False
            return None
        refs_per_layer = len(refs) // len(layer_names)
        payload_offset = 0
        for layer_name in layer_names:
            for ref in refs[:refs_per_layer]:
                layout[layer_name].append(
                    _LayerCopySpan(
                        layer_name=layer_name,
                        payload_offset=payload_offset,
                        tensor_idx=int(ref.tensor_idx),
                        tensor_offset=0,
                        nbytes=int(ref.page_size_bytes),
                    )
                )
                payload_offset += int(ref.page_size_bytes)
            refs = refs[refs_per_layer:]
        self._layer_layout = layout
        return layout

    @staticmethod
    def _sync_device(device: torch.device) -> None:
        try:
            if device.type == "cuda" and torch.cuda.is_available():
                torch.cuda.current_stream(device).synchronize()
            elif device.type == "xpu" and hasattr(torch, "xpu"):
                torch.xpu.current_stream(device).synchronize()  # type: ignore[attr-defined]
        except Exception:
            # Correctness first: copy_ without stream sync is still ordered for CPU-only tests.
            return

    @staticmethod
    def _as_byte_tensor(tensor: torch.Tensor, *, copy_if_needed: bool = True) -> torch.Tensor:
        view = tensor.detach()
        if copy_if_needed and not view.is_contiguous():
            view = view.contiguous()
        try:
            return view.view(torch.uint8).flatten()
        except Exception:
            if not copy_if_needed:
                raise
            return view.view(torch.int8).flatten().to(torch.uint8)

    def _payloads_from_gpu(
        self, gpu_spec: GPULoadStoreSpec, keys: list[OffloadKey]
    ) -> tuple[dict[OffloadKey, memoryview], list[torch.Tensor]]:
        refs = self._group_refs()
        block_ids = list(int(x) for x in gpu_spec.block_ids.tolist())
        expected = len(keys) * self.block_size_factor
        if len(block_ids) != expected:
            raise ValueError(f"Expected {expected} GPU block ids for {len(keys)} FalconFS blocks, got {len(block_ids)}")
        payload_size = self.block_size_factor * sum(int(ref.page_size_bytes) for ref in refs)
        out: dict[OffloadKey, memoryview] = {}
        acquired: list[torch.Tensor] = []
        pos = 0
        for key in keys:
            host = self._host_pool.acquire(payload_size, prefer_pinned=True)
            acquired.append(host)
            offset = 0
            source_devices: set[torch.device] = set()
            for _ in range(self.block_size_factor):
                block_id = block_ids[pos]
                pos += 1
                for ref in refs:
                    tensor = self.kv_caches.tensors[ref.tensor_idx].tensor
                    src = self._as_byte_tensor(tensor[int(block_id)])
                    n = int(ref.page_size_bytes)
                    host[offset: offset + n].copy_(src[:n], non_blocking=bool(getattr(host, "is_pinned", lambda: False)()))
                    source_devices.add(src.device)
                    offset += n
            for device in source_devices:
                if device.type != "cpu":
                    self._sync_device(device)
                    self._bump("gpu_d2h_copies")
                    self._bump("gpu_d2h_bytes", payload_size)
            out[key] = memoryview(host[:payload_size].numpy())
        return out, acquired

    def _copy_payload_view_to_tensor(
        self,
        payload: memoryview,
        offset: int,
        tensor: torch.Tensor,
        block_id: int,
        nbytes: int,
        tensor_offset: int = 0,
    ) -> None:
        view = payload[offset: offset + nbytes]
        try:
            if getattr(view, "readonly", False):
                src = torch.tensor(bytearray(view), dtype=torch.uint8)
            else:
                src = torch.frombuffer(view, dtype=torch.uint8)
        except Exception:
            src = torch.tensor(bytearray(view), dtype=torch.uint8)
        dst_row = self._as_byte_tensor(tensor[int(block_id)], copy_if_needed=False)
        dst = dst_row[int(tensor_offset): int(tensor_offset) + nbytes]
        if dst.device.type == "cpu":
            dst.copy_(src[:nbytes])
            return
        staging = self._host_pool.acquire(nbytes, prefer_pinned=True)
        try:
            staging[:nbytes].copy_(src[:nbytes])
            dst.copy_(staging[:nbytes], non_blocking=bool(getattr(staging, "is_pinned", lambda: False)()))
            self._sync_device(dst.device)
            self._bump("gpu_h2d_copies")
            self._bump("gpu_h2d_bytes", nbytes)
        finally:
            self._host_pool.release(staging)

    def _payloads_to_gpu(self, falcon_spec: FalconFSLoadStoreSpec, gpu_spec: GPULoadStoreSpec, payloads: dict[OffloadKey, bytes | memoryview]) -> None:
        refs = self._group_refs()
        block_ids = list(int(x) for x in gpu_spec.block_ids.tolist())
        expected = len(falcon_spec.keys) * self.block_size_factor
        if len(block_ids) != expected:
            raise ValueError(f"Expected {expected} GPU block ids for {len(falcon_spec.keys)} FalconFS blocks, got {len(block_ids)}")
        block_pos = 0
        for key in falcon_spec.keys:
            payload = memoryview(payloads[key])
            offset = 0
            for _ in range(self.block_size_factor):
                block_id = block_ids[block_pos]
                block_pos += 1
                for ref in refs:
                    n = int(ref.page_size_bytes)
                    tensor = self.kv_caches.tensors[ref.tensor_idx].tensor
                    self._copy_payload_view_to_tensor(payload, offset, tensor, block_id, n)
                    offset += n

    def _copy_layer_payloads_to_gpu(
        self,
        falcon_spec: FalconFSLoadStoreSpec,
        gpu_spec: GPULoadStoreSpec,
        payloads: dict[OffloadKey, bytes | memoryview],
        layer_name: str,
    ) -> None:
        layout = self._get_layer_layout()
        if layout is None or layer_name not in layout:
            self._bump("fallback_unknown_layer")
            raise KeyError(f"Unknown FalconFS layer layout for {layer_name!r}")
        block_ids = list(int(x) for x in gpu_spec.block_ids.tolist())
        expected = len(falcon_spec.keys) * self.block_size_factor
        if len(block_ids) != expected:
            raise ValueError(f"Expected {expected} GPU block ids for {len(falcon_spec.keys)} FalconFS blocks, got {len(block_ids)}")
        bytes_per_gpu_block = self._payload_bytes_per_gpu_block()
        block_pos = 0
        for key in falcon_spec.keys:
            payload = memoryview(payloads[key])
            for factor_idx in range(self.block_size_factor):
                block_id = block_ids[block_pos]
                block_pos += 1
                base = factor_idx * bytes_per_gpu_block
                for span in layout[layer_name]:
                    tensor = self.kv_caches.tensors[span.tensor_idx].tensor
                    self._copy_payload_view_to_tensor(
                        payload,
                        base + span.payload_offset,
                        tensor,
                        block_id,
                        span.nbytes,
                        tensor_offset=span.tensor_offset,
                    )

    def _record_failed_load_spec(self, transfer_spec: TransferSpec) -> None:
        if self.gpu_to_falconfs:
            return
        try:
            _, dst = transfer_spec
            if isinstance(dst, GPULoadStoreSpec):
                with self._lock:
                    self._failed_load_block_ids.update(int(x) for x in dst.block_ids.tolist())
        except Exception:
            return

    def _read_payloads_for_layer_job(
        self, state: _LayerLoadJob
    ) -> dict[OffloadKey, bytes | memoryview]:
        with state.lock:
            if state.payloads is not None:
                return state.payloads
            future = state.payload_future
        if future is None:
            start = time.perf_counter()
            src, _ = state.transfer_spec
            assert isinstance(src, FalconFSLoadStoreSpec)
            payloads = self.adapter.read_prepared_payloads(src)
            self._bump_ms("payload_fetch_ms", start)
        else:
            start = time.perf_counter()
            payloads = future.result()
            self._bump_ms("payload_fetch_ms", start)
        payload_size = sum(len(memoryview(v)) for v in payloads.values())
        with state.lock:
            if state.payloads is None:
                state.payloads = payloads
                state.payload_size = payload_size
                self._bump("payload_cache_bytes", payload_size)
            return state.payloads

    def _copy_layer_for_state(self, state: _LayerLoadJob, layer_name: str) -> None:
        with state.lock:
            if state.failed or layer_name in state.copied_layers:
                return
        start = time.perf_counter()
        src, dst = state.transfer_spec
        assert isinstance(src, FalconFSLoadStoreSpec)
        assert isinstance(dst, GPULoadStoreSpec)
        payloads = self._read_payloads_for_layer_job(state)
        if len(payloads) != len(src.keys):
            raise RuntimeError("FalconFS layer-wise load did not return all requested payloads")
        self._copy_layer_payloads_to_gpu(src, dst, payloads, layer_name)
        self._bump_ms("layer_copy_ms", start)
        with state.lock:
            state.copied_layers.add(layer_name)

    def _finalize_layer_job_if_ready(self, state: _LayerLoadJob) -> TransferResult | None:
        with state.lock:
            futures = list(state.copy_futures.values())
        for future in futures:
            if future.done():
                try:
                    exc = future.exception()
                except BaseException as err:  # cancelled futures report failure here.
                    exc = err
                if exc is not None:
                    self._mark_layer_job_failed(state, exc)
                    break
        with state.lock:
            if state.completed_result is not None:
                return state.completed_result
            if state.failed:
                state.completed_result = TransferResult(
                    job_id=state.job_id,
                    success=False,
                    transfer_type=self.transfer_type,
                )
                return state.completed_result
            for future in state.copy_futures.values():
                if not future.done():
                    return None
            if set(state.layer_names) - state.copied_layers:
                return None
            state.completed_result = TransferResult(
                job_id=state.job_id,
                success=True,
                transfer_size=int(state.payload_size),
                transfer_time=time.perf_counter() - state.start_time,
                transfer_type=self.transfer_type,
            )
            return state.completed_result

    def _mark_layer_job_failed(self, state: _LayerLoadJob, exc: BaseException | None = None) -> None:
        del exc
        with state.lock:
            state.failed = True
        self._bump("load_failures")
        self._bump("transfer_failures")
        self._record_failed_load_spec(state.transfer_spec)

    def _schedule_layer_prefetch_locked(self, state: _LayerLoadJob, through_layer: str | None = None) -> None:
        if not bool(getattr(self.adapter, "async_load_enabled", True)):
            return
        depth = max(1, int(getattr(self.adapter, "layer_prefetch_depth", 2)))
        target_idx = 0
        if through_layer in state.layer_names:
            target_idx = state.layer_names.index(through_layer) + 1
        scheduled = len(state.copy_futures)
        limit = min(len(state.layer_names), max(scheduled, target_idx) + depth)
        for idx in range(scheduled, limit):
            layer_name = state.layer_names[idx]
            if layer_name not in state.copy_futures:
                state.copy_futures[layer_name] = self.executor.submit(
                    self._copy_layer_for_state, state, layer_name
                )

    def _start_layerwise_load(self, job_id: int, transfer_spec: TransferSpec) -> bool:
        layout = self._get_layer_layout()
        if layout is None:
            self._bump("layerwise_fallbacks")
            self._bump("fallback_invalid_layout")
            return False
        state = _LayerLoadJob(
            job_id=job_id,
            transfer_spec=transfer_spec,
            layer_names=list(layout.keys()),
            start_time=time.perf_counter(),
        )
        if bool(getattr(self.adapter, "async_load_enabled", True)):
            src, _ = transfer_spec
            assert isinstance(src, FalconFSLoadStoreSpec)
            state.payload_future = self.executor.submit(self.adapter.read_prepared_payloads, src)
            self._bump("layerwise_async_jobs")
        with self._lock:
            if job_id in self._layer_jobs or job_id in self._futures:
                return False
            if state.payload_future is not None:
                self._schedule_layer_prefetch_locked(state)
            self._layer_jobs[job_id] = state
        self._bump("layerwise_loads")
        return True

    def _run_transfer(self, job_id: int, transfer_spec: TransferSpec) -> TransferResult:
        start = time.perf_counter()
        acquired: list[torch.Tensor] = []
        try:
            src, dst = transfer_spec
            if self.gpu_to_falconfs:
                assert isinstance(src, GPULoadStoreSpec)
                assert isinstance(dst, FalconFSLoadStoreSpec)
                payloads, acquired = self._payloads_from_gpu(src, dst.keys)
                success = self.adapter.write_prepared_payloads(dst, payloads)
                size = sum(len(memoryview(v)) for v in payloads.values())
                if not success:
                    self._bump("store_failures")
            else:
                assert isinstance(src, FalconFSLoadStoreSpec)
                assert isinstance(dst, GPULoadStoreSpec)
                self._bump("whole_block_loads")
                payloads = self.adapter.read_prepared_payloads(src)
                self._payloads_to_gpu(src, dst, payloads)
                success = len(payloads) == len(src.keys)
                size = sum(len(memoryview(v)) for v in payloads.values())
                if not success:
                    self._bump("load_failures")
        except Exception:
            self._bump("transfer_failures")
            if not self.gpu_to_falconfs:
                self._record_failed_load_spec(transfer_spec)
                if getattr(self.adapter, "load_failure_policy", "recompute") == "fail":
                    raise
            return TransferResult(job_id=job_id, success=False, transfer_type=self.transfer_type)
        finally:
            for buf in acquired:
                self._host_pool.release(buf)
        return TransferResult(
            job_id=job_id,
            success=bool(success),
            transfer_size=int(size),
            transfer_time=time.perf_counter() - start,
            transfer_type=self.transfer_type,
        )

    def transfer_async(self, job_id: int, spec: TransferSpec) -> bool:
        if not self.gpu_to_falconfs and bool(getattr(self.adapter, "layerwise_load_enabled", False)):
            if self._start_layerwise_load(job_id, spec):
                return True
        if not self.gpu_to_falconfs and not bool(getattr(self.adapter, "async_load_enabled", True)):
            with self._lock:
                if job_id in self._futures or job_id in self._layer_jobs:
                    return False
            result = self._run_transfer(job_id, spec)
            with self._lock:
                self._completed_results.append(result)
            return True
        with self._lock:
            if job_id in self._futures or job_id in self._layer_jobs:
                return False
            self._futures[job_id] = self.executor.submit(self._run_transfer, job_id, spec)
        return True

    def get_finished(self) -> list[TransferResult]:
        finished: list[TransferResult] = []
        with self._lock:
            finished.extend(self._completed_results)
            self._completed_results.clear()
            for job_id, fut in list(self._futures.items()):
                if fut.done():
                    finished.append(fut.result())
                    del self._futures[job_id]
            for job_id, state in list(self._layer_jobs.items()):
                result = self._finalize_layer_job_if_ready(state)
                if result is not None:
                    finished.append(result)
                    del self._layer_jobs[job_id]
        return finished

    def take_failed_load_block_ids(self) -> set[int]:
        with self._lock:
            out = set(self._failed_load_block_ids)
            self._failed_load_block_ids.clear()
            return out

    def wait(self, job_ids: set[int]) -> None:
        for job_id in list(job_ids):
            fut = self._futures.get(job_id)
            if fut is not None:
                fut.result()
            state = self._layer_jobs.get(job_id)
            if state is not None:
                for layer_name in state.layer_names:
                    self.wait_for_layer_load(layer_name)

    def wait_for_layer_load(self, layer_name: str) -> None:
        if self.gpu_to_falconfs:
            return
        wait_start = time.perf_counter()
        states: list[_LayerLoadJob]
        with self._lock:
            states = list(self._layer_jobs.values())
        for state in states:
            if layer_name not in state.layer_names:
                self._bump("fallback_unknown_layer")
                continue
            with state.lock:
                future = state.copy_futures.get(layer_name)
                if future is None:
                    if bool(getattr(self.adapter, "async_load_enabled", True)):
                        future = self.executor.submit(self._copy_layer_for_state, state, layer_name)
                        state.copy_futures[layer_name] = future
                    else:
                        self._bump("layerwise_sync_waits")
            try:
                if future is None:
                    self._copy_layer_for_state(state, layer_name)
                else:
                    future.result()
            except Exception as exc:
                self._mark_layer_job_failed(state, exc)
                if getattr(self.adapter, "load_failure_policy", "recompute") == "fail":
                    raise
            with state.lock:
                self._schedule_layer_prefetch_locked(state, through_layer=layer_name)
            self._finalize_layer_job_if_ready(state)
        self._bump_ms("layer_wait_ms", wait_start)

    def shutdown(self) -> None:
        self.executor.shutdown(wait=False, cancel_futures=True)


class FalconFSConnectorWorker(OffloadingConnectorWorker):
    def __init__(self, spec: OffloadingSpec):
        super().__init__(spec)
        self._failed_load_block_ids: set[int] = set()

    def handle_preemptions(self, kv_connector_metadata):
        super().handle_preemptions(kv_connector_metadata)
        for req_id in kv_connector_metadata.reqs_to_flush or ():
            job_id = self._load_job.get(req_id)
            if job_id is not None:
                self.worker.wait({job_id})

    def wait_for_layer_load(self, layer_name: str) -> None:
        for handler in self.worker.handlers:
            method = getattr(handler, "wait_for_layer_load", None)
            if callable(method):
                method(layer_name)

    def get_finished(self, finished_req_ids: set[str]) -> tuple[set[str], set[str]]:
        finished_sending = set()
        finished_recving = set()
        for transfer_result in self.worker.get_finished():
            job_id = transfer_result.job_id
            req_id, store = self._jobs.pop(job_id)
            if transfer_result.success:
                if (
                    transfer_result.transfer_time
                    and transfer_result.transfer_size is not None
                    and transfer_result.transfer_type is not None
                ):
                    self.kv_connector_stats.record_transfer(
                        num_bytes=transfer_result.transfer_size,
                        time=transfer_result.transfer_time,
                        transfer_type=transfer_result.transfer_type,
                    )
            else:
                for handler in self.worker.handlers:
                    method = getattr(handler, "take_failed_load_block_ids", None)
                    if callable(method):
                        self._failed_load_block_ids.update(method())
            if store:
                req_jobs = self._store_jobs[req_id]
                req_jobs.remove(job_id)
                if req_jobs:
                    continue
                if req_id in self._finished_reqs_waiting_for_store:
                    self._finished_reqs_waiting_for_store.remove(req_id)
                    finished_sending.add(req_id)
                    del self._store_jobs[req_id]
            else:
                req_job = self._load_job[req_id]
                assert job_id == req_job
                del self._load_job[req_id]
                finished_recving.add(req_id)

        for req_id in finished_req_ids:
            pending_req_jobs = self._store_jobs.get(req_id)
            if pending_req_jobs:
                self._finished_reqs_waiting_for_store.add(req_id)
            elif pending_req_jobs is not None:
                finished_sending.add(req_id)
                del self._store_jobs[req_id]

        return finished_sending, finished_recving

    def get_block_ids_with_load_errors(self) -> set[int]:
        for handler in self.worker.handlers:
            method = getattr(handler, "take_failed_load_block_ids", None)
            if callable(method):
                self._failed_load_block_ids.update(method())
        out = set(self._failed_load_block_ids)
        self._failed_load_block_ids.clear()
        return out


class FalconFSOffloadingSpec(OffloadingSpec):
    def __init__(self, vllm_config: VllmConfig, kv_cache_config: KVCacheConfig):
        super().__init__(vllm_config, kv_cache_config)
        if len(self.gpu_block_size) != 1:
            raise NotImplementedError(
                "FalconFSConnector production path currently supports one KV cache group; "
                "multi-group/HMA models must fail early until generalized."
            )
        self._manager: FalconFSOffloadingManagerAdapter | None = None
        self._worker_manager: FalconFSOffloadingManagerAdapter | None = None
        self._handlers: tuple[_FalconFSTransferHandler, _FalconFSTransferHandler] | None = None

    def get_manager(self) -> FalconFSOffloadingManagerAdapter:
        if self._manager is None:
            self._manager = FalconFSOffloadingManagerAdapter(
                self.vllm_config, self.kv_cache_config, metrics_label="scheduler"
            )
        return self._manager

    def _get_worker_manager(self) -> FalconFSOffloadingManagerAdapter:
        if self._worker_manager is None:
            self._worker_manager = FalconFSOffloadingManagerAdapter(
                self.vllm_config, self.kv_cache_config, metrics_label="worker"
            )
        return self._worker_manager

    def get_handlers(self, kv_caches: CanonicalKVCaches):
        if self._handlers is None:
            adapter = self._get_worker_manager()
            store = _FalconFSTransferHandler(adapter, kv_caches, self.block_size_factor, gpu_to_falconfs=True)
            load = _FalconFSTransferHandler(adapter, kv_caches, self.block_size_factor, gpu_to_falconfs=False)
            self._handlers = (store, load)
        store, load = self._handlers
        yield GPULoadStoreSpec, FalconFSLoadStoreSpec, store
        yield FalconFSLoadStoreSpec, GPULoadStoreSpec, load


class FalconFSConnector(KVConnectorBase_V1):
    @property
    def prefer_cross_layer_blocks(self) -> bool:
        return True

    def __init__(
        self,
        vllm_config: VllmConfig,
        role: KVConnectorRole,
        kv_cache_config: KVCacheConfig | None = None,
    ):
        super().__init__(vllm_config, role, kv_cache_config)
        if kv_cache_config is None:
            raise ValueError("FalconFSConnector requires kv_cache_config")
        self.spec = FalconFSOffloadingSpec(vllm_config, kv_cache_config)
        self.connector_scheduler: FalconFSConnectorScheduler | None = None
        self.connector_worker: FalconFSConnectorWorker | None = None
        if role == KVConnectorRole.SCHEDULER:
            self.connector_scheduler = FalconFSConnectorScheduler(self.spec)
        elif role == KVConnectorRole.WORKER:
            self.connector_worker = FalconFSConnectorWorker(self.spec)

    def shutdown(self) -> None:
        if self.connector_worker is not None:
            self.connector_worker.shutdown()
        if self.connector_scheduler is not None:
            self.connector_scheduler.shutdown()

    def register_kv_caches(self, kv_caches: dict[str, torch.Tensor]):
        assert self.connector_worker is not None
        self.connector_worker.register_kv_caches(kv_caches)

    def register_cross_layers_kv_cache(self, kv_cache: torch.Tensor, attn_backend: type[AttentionBackend]):
        assert self.connector_worker is not None
        self.connector_worker.register_cross_layers_kv_cache(kv_cache, attn_backend)

    def handle_preemptions(self, kv_connector_metadata: KVConnectorMetadata):
        assert self.connector_worker is not None
        self.connector_worker.handle_preemptions(kv_connector_metadata)  # type: ignore[arg-type]

    def start_load_kv(self, forward_context: ForwardContext, **kwargs: Any) -> None:
        del forward_context, kwargs
        assert self.connector_worker is not None
        self.connector_worker.start_kv_transfers(self._get_connector_metadata())  # type: ignore[arg-type]

    def wait_for_layer_load(self, layer_name: str) -> None:
        assert self.connector_worker is not None
        self.connector_worker.wait_for_layer_load(layer_name)

    def save_kv_layer(self, layer_name: str, kv_layer: torch.Tensor, attn_metadata: AttentionMetadata, **kwargs: Any) -> None:
        del layer_name, kv_layer, attn_metadata, kwargs
        return None

    def wait_for_save(self):
        assert self.connector_worker is not None
        self.connector_worker.prepare_store_kv(self._get_connector_metadata())  # type: ignore[arg-type]

    def get_finished(self, finished_req_ids: set[str]) -> tuple[set[str], set[str]]:
        assert self.connector_worker is not None
        return self.connector_worker.get_finished(finished_req_ids)

    def get_block_ids_with_load_errors(self) -> set[int]:
        if self.connector_worker is None:
            return set()
        return self.connector_worker.get_block_ids_with_load_errors()

    def get_num_new_matched_tokens(self, request: Request, num_computed_tokens: int) -> tuple[int | None, bool]:
        assert self.connector_scheduler is not None
        return self.connector_scheduler.get_num_new_matched_tokens(request, num_computed_tokens)

    def update_state_after_alloc(self, request: Request, blocks: KVCacheBlocks, num_external_tokens: int):
        assert self.connector_scheduler is not None
        return self.connector_scheduler.update_state_after_alloc(request, blocks, num_external_tokens)

    def build_connector_meta(self, scheduler_output: SchedulerOutput) -> KVConnectorMetadata:
        assert self.connector_scheduler is not None
        return self.connector_scheduler.build_connector_meta(scheduler_output)

    def update_connector_output(self, connector_output: KVConnectorOutput):
        assert self.connector_scheduler is not None
        self.connector_scheduler.update_connector_output(connector_output)

    def request_finished(self, request: Request, block_ids: list[int]) -> tuple[bool, dict[str, Any] | None]:
        assert self.connector_scheduler is not None
        handled = self.connector_scheduler.request_finished(request, block_ids)
        manager = getattr(self.connector_scheduler, "manager", None)
        if hasattr(manager, "clear_lookup_cache"):
            manager.clear_lookup_cache(req_context=ReqContext(kv_transfer_params=getattr(request, "kv_transfer_params", None)))
        return handled

    def take_events(self) -> Iterable[KVCacheEvent]:
        assert self.connector_scheduler is not None
        return self.connector_scheduler.take_events()

    def get_kv_connector_stats(self) -> KVConnectorStats | None:
        if self.connector_worker is None:
            return None
        return self.connector_worker.get_kv_connector_stats()

    @classmethod
    def build_kv_connector_stats(cls, data: dict[str, Any] | None = None) -> KVConnectorStats | None:
        return OffloadingConnectorStats(data=data) if data is not None else OffloadingConnectorStats()

    @classmethod
    def build_prom_metrics(
        cls,
        vllm_config: VllmConfig,
        metric_types: dict[type[PromMetric], type[PromMetricT]],
        labelnames: list[str],
        per_engine_labelvalues: dict[int, list[object]],
    ) -> KVConnectorPromMetrics:
        return OffloadPromMetrics(vllm_config, metric_types, labelnames, per_engine_labelvalues)
