from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[2]
for path in (ROOT / "third_party" / "vllm", ROOT / "vllm_kv_cache" / "python"):
    s = str(path)
    if s not in sys.path:
        sys.path.insert(0, s)

try:
    import torch  # noqa: F401
    import transformers  # noqa: F401
    import vllm  # noqa: F401
except Exception as exc:  # pragma: no cover - optional deployment smoke
    IMPORT_ERROR = exc
else:
    IMPORT_ERROR = None


def _env_enabled(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _can_connect(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


def _live_block_size_candidates() -> list[int]:
    explicit = os.environ.get("FALCON_VLLM_SMOKE_BLOCK_BYTES")
    if explicit:
        return [int(explicit)]
    candidates: list[int] = []
    for value in (
        os.environ.get("FALCON_KV_STORE_BLOCK_SIZE"),
        os.environ.get("FALCON_MIX_KV_BLOCK_BYTES"),
        "1048576",
        "2097152",
        "524288",
        "65536",
    ):
        if not value:
            continue
        size = int(value)
        if size > 0 and size not in candidates:
            candidates.append(size)
    return candidates


def _tail(path: Path, max_chars: int = 12000) -> str:
    try:
        data = path.read_text(errors="replace")
    except OSError as exc:
        return f"<unable to read {path}: {exc}>"
    return data[-max_chars:]


def _wait_http_ok(url: str, proc: subprocess.Popen, log_path: Path, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise AssertionError(
                f"vLLM server exited early with rc={proc.returncode}\n{_tail(log_path)}"
            )
        try:
            with urllib.request.urlopen(url, timeout=2.0) as rsp:
                if 200 <= int(rsp.status) < 300:
                    return
        except BaseException as exc:  # noqa: BLE001 - preserve startup context
            last_error = exc
        time.sleep(0.5)
    raise AssertionError(f"Timed out waiting for {url}: {last_error}\n{_tail(log_path)}")


def _post_json(url: str, payload: dict, timeout_s: float = 30.0) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as rsp:
        return json.loads(rsp.read().decode("utf-8"))


def _read_metrics_records(path: Path) -> list[dict]:
    records: list[dict] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return records
    for line in lines:
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


def _merged_metric_stats(path: Path) -> dict[str, int]:
    merged: dict[str, int] = {}
    for record in _read_metrics_records(path):
        stats = record.get("stats")
        if not isinstance(stats, dict):
            continue
        for key, value in stats.items():
            try:
                merged[key] = max(int(merged.get(key, 0)), int(value))
            except (TypeError, ValueError):
                continue
    return merged


def _wait_for_metric(path: Path, key: str, minimum: int, timeout_s: float = 45.0) -> dict[str, int]:
    deadline = time.monotonic() + timeout_s
    stats: dict[str, int] = {}
    while time.monotonic() < deadline:
        stats = _merged_metric_stats(path)
        if int(stats.get(key, 0)) >= minimum:
            return stats
        time.sleep(0.5)
    return stats


def _make_tiny_opt_model(model_dir: Path) -> None:
    from tokenizers import Tokenizer
    from tokenizers.models import WordLevel
    from tokenizers.pre_tokenizers import Whitespace
    from transformers import OPTConfig, OPTForCausalLM, PreTrainedTokenizerFast

    model_dir.mkdir(parents=True, exist_ok=True)
    config = OPTConfig(
        vocab_size=64,
        hidden_size=64,
        num_hidden_layers=1,
        ffn_dim=128,
        num_attention_heads=1,
        max_position_embeddings=128,
        word_embed_proj_dim=64,
        bos_token_id=1,
        eos_token_id=2,
        pad_token_id=0,
    )
    OPTForCausalLM(config).save_pretrained(model_dir)
    vocab = {
        "<pad>": 0,
        "<s>": 1,
        "</s>": 2,
        "<unk>": 3,
        "Hello": 4,
        ",": 5,
        "world": 6,
        "FalconFS": 7,
        "KV": 8,
        "cache": 9,
        "test": 10,
        ".": 11,
        "my": 12,
        "name": 13,
        "is": 14,
    }
    for i in range(15, 64):
        vocab[f"tok{i}"] = i
    tokenizer = Tokenizer(WordLevel(vocab=vocab, unk_token="<unk>"))
    tokenizer.pre_tokenizer = Whitespace()
    hf_tokenizer = PreTrainedTokenizerFast(
        tokenizer_object=tokenizer,
        unk_token="<unk>",
        pad_token="<pad>",
        bos_token="<s>",
        eos_token="</s>",
    )
    hf_tokenizer.save_pretrained(model_dir)


@unittest.skipIf(IMPORT_ERROR is not None, f"vLLM/torch deployment unavailable: {IMPORT_ERROR}")
class FalconFSVllmServeSmokeTest(unittest.TestCase):
    """Live dynamic-connector smoke using a tiny generated local model."""

    @unittest.skipUnless(
        _env_enabled("FALCON_VLLM_LIVE_SMOKE"),
        "set FALCON_VLLM_LIVE_SMOKE=1 to run the live vLLM/FalconFS serve smoke",
    )
    def test_vllm_serve_with_falconfs_connector(self):
        cn_port = int(os.environ.get("FALCON_VLLM_SMOKE_CN_PORT", "55500"))
        store_port = int(
            os.environ.get(
                "FALCON_VLLM_SMOKE_STORE_PORT",
                os.environ.get("KV_STORE_BRPC_PORT", "18765"),
            )
        )
        if not _can_connect("127.0.0.1", cn_port):
            self.skipTest(f"FalconFS CN not reachable on 127.0.0.1:{cn_port}")
        if not _can_connect("127.0.0.1", store_port):
            self.skipTest(f"FalconFS Store not reachable on 127.0.0.1:{store_port}")

        tmp = Path(tempfile.mkdtemp(prefix="falconfs_vllm_live_"))
        self.addCleanup(lambda: shutil.rmtree(tmp, ignore_errors=True))
        model_dir = tmp / "tiny-opt"
        _make_tiny_opt_model(model_dir)

        port = int(os.environ.get("FALCON_VLLM_SMOKE_PORT", "18080"))
        model_name = "falconfs-tiny-opt"
        conninfo = os.environ.get(
            "FALCON_KV_CN_CONNINFO",
            f"hostaddr=127.0.0.1 port={cn_port} user={os.environ.get('USER', 'junbin')} "
            "dbname=postgres application_name=FalconFSVllmServeSmoke",
        )
        block_bytes = _live_block_size_candidates()[0]
        shard_table = {
            "1": os.environ.get("FALCON_VLLM_SMOKE_DN1_ENDPOINT", "127.0.0.1:55530"),
            "2": os.environ.get("FALCON_VLLM_SMOKE_DN2_ENDPOINT", "127.0.0.1:55550"),
        }
        kv_transfer_config = {
            "kv_connector": "FalconFSConnector",
            "kv_role": "kv_both",
            "kv_connector_module_path": "falconfs_kv.vllm_kv_connector",
            "kv_connector_extra_config": {
                "cn_conninfo": conninfo,
                "shard_table": shard_table,
                "block_bytes": block_bytes,
                "lookup_cache_capacity": 64,
                "client_id": int(os.environ.get("FALCON_VLLM_SMOKE_CLIENT_ID", "9091")),
                "client_hostname": os.environ.get("NODE_NAME", "falconfs-vllm-smoke"),
            },
        }

        env = os.environ.copy()
        env["TMPDIR"] = env["TEMP"] = env["TMP"] = "/tmp"
        py_paths = [str(ROOT / "vllm_kv_cache" / "python"), str(ROOT / "third_party" / "vllm"), str(ROOT)]
        if env.get("PYTHONPATH"):
            py_paths.append(env["PYTHONPATH"])
        env["PYTHONPATH"] = os.pathsep.join(py_paths)
        env.setdefault("FALCON_KV_CLIENT_BATCH_READ_MAX_BLOCKS", "8")
        env.setdefault("FALCON_KV_CLIENT_BATCH_WRITE_MAX_BLOCKS", "1")
        env.setdefault("FALCON_KV_VLLM_TRANSFER_WORKERS", "2")

        vllm_bin = os.environ.get("VLLM_BIN")
        if not vllm_bin:
            candidate = ROOT / ".venv-vllm" / "bin" / "vllm"
            vllm_bin = str(candidate if candidate.exists() else "vllm")
        log_path = tmp / "vllm_serve.log"
        metrics_path = tmp / "falconfs_vllm_metrics.jsonl"
        env["FALCON_KV_VLLM_METRICS_JSONL"] = str(metrics_path)
        env["FALCON_KV_VLLM_METRICS_EAGER"] = "1"
        cmd = [
            vllm_bin,
            "serve",
            str(model_dir),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--dtype",
            "float32",
            "--max-model-len",
            "64",
            "--max-num-seqs",
            "2",
            "--enforce-eager",
            "--served-model-name",
            model_name,
            "--disable-log-stats",
            "--disable-uvicorn-access-log",
            "--gpu-memory-utilization",
            os.environ.get("FALCON_VLLM_SMOKE_CPU_MEMORY_UTILIZATION", "0.2"),
            "--kv-transfer-config",
            json.dumps(kv_transfer_config),
        ]
        with log_path.open("w", encoding="utf-8") as logf:
            proc = subprocess.Popen(cmd, stdout=logf, stderr=subprocess.STDOUT, env=env, text=True)
        self.addCleanup(self._stop_process, proc)
        _wait_http_ok(f"http://127.0.0.1:{port}/health", proc, log_path, timeout_s=90.0)

        prompt = " ".join(f"tok{15 + (i % 49)}" for i in range(48))
        for _ in range(3):
            body = _post_json(
                f"http://127.0.0.1:{port}/v1/completions",
                {"model": model_name, "prompt": prompt, "max_tokens": 2, "temperature": 0},
            )
            self.assertIn("choices", body)
            self.assertEqual(len(body["choices"]), 1)
            self.assertIn("text", body["choices"][0])
            if proc.poll() is not None:
                self.fail(f"vLLM server exited after completion request\n{_tail(log_path)}")


    def test_connector_store_and_load_blocks_against_falconfs(self):
        cn_port = int(os.environ.get("FALCON_VLLM_SMOKE_CN_PORT", "55500"))
        store_port = int(
            os.environ.get(
                "FALCON_VLLM_SMOKE_STORE_PORT",
                os.environ.get("KV_STORE_BRPC_PORT", "18765"),
            )
        )
        if not _can_connect("127.0.0.1", cn_port):
            self.skipTest(f"FalconFS CN not reachable on 127.0.0.1:{cn_port}")
        if not _can_connect("127.0.0.1", store_port):
            self.skipTest(f"FalconFS Store not reachable on 127.0.0.1:{store_port}")

        from falconfs_kv.offloading_manager import FalconFSOffloadingManager
        from falconfs_kv.vllm_kv_connector import (
            FalconFSLoadStoreSpec,
            FalconFSOffloadingManagerAdapter,
            _FalconFSTransferHandler,
        )
        from vllm.v1.kv_offload.abstract import ReqContext, make_offload_key
        from vllm.v1.kv_offload.mediums import GPULoadStoreSpec
        from vllm.v1.kv_offload.spec import (
            CanonicalKVCaches,
            CanonicalKVCacheRef,
            CanonicalKVCacheTensor,
        )

        conninfo = os.environ.get(
            "FALCON_KV_CN_CONNINFO",
            f"hostaddr=127.0.0.1 port={cn_port} user={os.environ.get('USER', 'junbin')} "
            "dbname=postgres application_name=FalconFSVllmBlockOffloadE2E",
        )
        block_candidates = _live_block_size_candidates()
        old_store_endpoint = os.environ.get("FALCON_KV_STORE_BRPC_ENDPOINT")
        os.environ["FALCON_KV_STORE_BRPC_ENDPOINT"] = f"127.0.0.1:{store_port}"
        adapter = None
        store_handler = None
        load_handlers = []
        try:
            nonce = time.time_ns() & 0xFFFFFFFFFFFF
            keys = [make_offload_key(f"lv{nonce:012x}{i}".encode(), 0) for i in range(2)]
            store_output = None
            last_prepare = ""
            for block_bytes in block_candidates:
                inner = FalconFSOffloadingManager(
                    client_id=int(os.environ.get("FALCON_VLLM_SMOKE_CLIENT_ID", "9092")),
                    client_hostname=os.environ.get("NODE_NAME", "falconfs-vllm-block-e2e"),
                    shard_table={
                        1: os.environ.get("FALCON_VLLM_SMOKE_DN1_ENDPOINT", "127.0.0.1:55530"),
                        2: os.environ.get("FALCON_VLLM_SMOKE_DN2_ENDPOINT", "127.0.0.1:55550"),
                    },
                    mode="cluster",
                    cn_conninfo=None,
                    block_size=block_bytes,
                )
                candidate_adapter = FalconFSOffloadingManagerAdapter(inner_manager=inner, metrics_label="live-e2e")
                candidate_adapter.kv_cache_config = SimpleNamespace(
                    kv_cache_groups=[SimpleNamespace(layer_names=["layer.0", "layer.1"])]
                )
                candidate_output = candidate_adapter.prepare_store(
                    keys, ReqContext(kv_transfer_params={"request_id": f"live-store-{block_bytes}"})
                )
                if candidate_output is not None and set(candidate_output.keys_to_store) == set(keys):
                    adapter = candidate_adapter
                    store_output = candidate_output
                    break
                if candidate_output is not None and candidate_output.keys_to_store:
                    candidate_adapter.complete_store(candidate_output.keys_to_store, success=False)
                last_prepare = f"block_bytes={block_bytes} keys_to_store={getattr(candidate_output, 'keys_to_store', None)!r}"
                candidate_adapter.shutdown()
            if adapter is None or store_output is None:
                self.fail(
                    "FalconFS live connector could not allocate test blocks; "
                    f"tried block sizes {block_candidates}. Last result: {last_prepare}"
                )
            tensor = torch.zeros((4, block_bytes), dtype=torch.int8)
            caches = CanonicalKVCaches(
                tensors=[CanonicalKVCacheTensor(tensor=tensor, page_size_bytes=block_bytes)],
                group_data_refs=[[CanonicalKVCacheRef(tensor_idx=0, page_size_bytes=block_bytes)]],
            )
            self.assertIsInstance(store_output.store_spec, FalconFSLoadStoreSpec)

            expected_by_key = {}
            for row, key in enumerate(store_output.keys_to_store):
                values = ((torch.arange(block_bytes, dtype=torch.int32) + 17 + row * 66) % 127).to(torch.int8)
                tensor[row].copy_(values)
                expected_by_key[key] = values.clone()

            store_handler = _FalconFSTransferHandler(adapter, caches, block_size_factor=1, gpu_to_falconfs=True)
            self.assertTrue(
                store_handler.transfer_async(
                    1,
                    (
                        GPULoadStoreSpec(list(range(len(store_output.keys_to_store))), group_sizes=(len(store_output.keys_to_store),)),
                        store_output.store_spec,
                    ),
                )
            )
            store_handler.wait({1})
            store_results = store_handler.get_finished()
            self.assertEqual(len(store_results), 1)
            self.assertTrue(store_results[0].success)
            adapter.complete_store(store_output.keys_to_store, success=True)

            lookup = adapter.lookup_many(keys, ReqContext(kv_transfer_params={"request_id": "live-load"}))
            self.assertEqual(lookup, {key: True for key in keys})
            modes = [
                ("whole-sync", False, False),
                ("whole-async", True, False),
                ("layer-sync", False, True),
                ("layer-async", True, True),
            ]
            for idx, (mode_name, async_load, layerwise_load) in enumerate(modes, start=2):
                adapter.async_load_enabled = async_load
                adapter.layerwise_load_enabled = layerwise_load
                adapter.layer_prefetch_depth = 1
                load_spec = adapter.prepare_load(keys, ReqContext(kv_transfer_params={"request_id": f"live-load-{mode_name}"}))
                tensor[2:].zero_()
                load_handler = _FalconFSTransferHandler(adapter, caches, block_size_factor=1, gpu_to_falconfs=False)
                load_handlers.append(load_handler)
                self.assertTrue(
                    load_handler.transfer_async(idx, (load_spec, GPULoadStoreSpec([2, 3], group_sizes=(2,))))
                )
                if layerwise_load:
                    load_handler.wait_for_layer_load("layer.0")
                    load_handler.wait_for_layer_load("layer.1")
                else:
                    load_handler.wait({idx})
                load_results = load_handler.get_finished()
                self.assertEqual(len(load_results), 1, mode_name)
                self.assertTrue(load_results[0].success, mode_name)
                for row, key in enumerate(load_spec.keys, start=2):
                    self.assertTrue(torch.equal(tensor[row], expected_by_key[key]), mode_name)

            stats = adapter.stats_snapshot()
            self.assertGreaterEqual(stats.get("prepare_store_keys", 0), 2)
            self.assertGreaterEqual(stats.get("data_write_blocks", 0), 2)
            self.assertGreaterEqual(stats.get("lookup_many_keys", 0), 2)
            self.assertGreaterEqual(stats.get("prepare_load_keys", 0), 2)
            self.assertGreaterEqual(stats.get("data_read_blocks", 0), 8)
            self.assertGreaterEqual(stats.get("whole_block_loads", 0), 2)
            self.assertGreaterEqual(stats.get("layerwise_loads", 0), 2)
        finally:
            if store_handler is not None:
                store_handler.shutdown()
            for load_handler in load_handlers:
                load_handler.shutdown()
            if adapter is not None:
                adapter.shutdown()
            if old_store_endpoint is None:
                os.environ.pop("FALCON_KV_STORE_BRPC_ENDPOINT", None)
            else:
                os.environ["FALCON_KV_STORE_BRPC_ENDPOINT"] = old_store_endpoint

    @staticmethod
    def _stop_process(proc: subprocess.Popen) -> None:
        if proc.poll() is not None:
            return
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=15)


if __name__ == "__main__":
    unittest.main()
