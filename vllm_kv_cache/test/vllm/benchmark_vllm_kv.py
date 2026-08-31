#!/usr/bin/env python3
import importlib.util
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "python"))

from falconfs_kv import FalconFSOffloadingManager  # noqa: E402


def main():
    if importlib.util.find_spec("vllm") is None:
        print("vLLM is not installed; running OffloadingManager micro-benchmark only")

    manager = FalconFSOffloadingManager(
        shard_table={0: "127.0.0.1:55520"},
        client_id=1,
        client_hostname="localhost",
    )
    keys = [f"bench-{i}" for i in range(100)]
    payload = {key: b"x" * 1024 for key in keys}

    start = time.perf_counter()
    manager.prepare_store(keys, None)
    manager.complete_store(keys, payload, None)
    store_s = time.perf_counter() - start

    start = time.perf_counter()
    for key in keys:
        manager.lookup(key, None)
    lookup_s = time.perf_counter() - start

    start = time.perf_counter()
    manager.prepare_load(keys, None)
    load_s = time.perf_counter() - start

    print(f"store_seconds={store_s:.6f}")
    print(f"lookup_seconds={lookup_s:.6f}")
    print(f"load_seconds={load_s:.6f}")
    print(f"blocks={len(keys)}")


if __name__ == "__main__":
    main()
