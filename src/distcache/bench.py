"""Workload + benchmark.

Generates a Zipf-distributed mix of GET/SET ops over ``n_keys`` and
measures throughput + hit rate for the in-process DistributedCache. If
``redis_url`` is provided, runs the SAME workload against Redis for a
head-to-head comparison.
"""

from __future__ import annotations

import random
import time
import tracemalloc
from typing import Any

from distcache.distributed import DistributedCache


def _generate_workload(n_ops: int, n_keys: int, read_ratio: float, seed: int = 42):
    rnd = random.Random(seed)
    # Use a Zipf-ish skew: pick from a small "hot" set 70% of the time.
    hot = list(range(min(200, n_keys)))
    cold = list(range(200, n_keys))
    ops = []
    for _ in range(n_ops):
        if cold and rnd.random() > 0.7:
            kid = rnd.choice(cold)
        else:
            kid = rnd.choice(hot)
        kind = "get" if rnd.random() < read_ratio else "set"
        ops.append((kind, f"k{kid}"))
    return ops


def run_bench(
    *,
    n_nodes: int,
    n_ops: int,
    n_keys: int,
    read_ratio: float,
    per_node_size: int,
    redis_url: str | None = None,
) -> dict:
    ops = _generate_workload(n_ops, n_keys, read_ratio)

    # --- distcache ---
    nodes = [f"n{i}" for i in range(n_nodes)]
    dc = DistributedCache(nodes, per_node_max_size=per_node_size)

    tracemalloc.start()
    t0 = time.perf_counter()
    for kind, key in ops:
        if kind == "set":
            dc.set(key, key.encode("ascii"))
        else:
            dc.get(key)
    dc_seconds = time.perf_counter() - t0
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    dc_total = dc.total_stats()
    distribution = dc.key_distribution([f"k{i}" for i in range(n_keys)])

    report: dict[str, Any] = {
        "config": {
            "n_nodes": n_nodes, "n_ops": n_ops, "n_keys": n_keys,
            "read_ratio": read_ratio, "per_node_size": per_node_size,
        },
        "distcache": {
            "seconds": round(dc_seconds, 3),
            "ops_per_sec": int(n_ops / max(dc_seconds, 1e-9)),
            "hits": dc_total.hits,
            "misses": dc_total.misses,
            "hit_rate": round(dc_total.hit_rate, 4),
            "evictions": dc_total.evicted_lru,
            "peak_memory_bytes": peak,
            "peak_memory_kb": round(peak / 1024, 1),
            "key_distribution_min": min(distribution.values()),
            "key_distribution_max": max(distribution.values()),
            "key_distribution_imbalance": round(
                (max(distribution.values()) - min(distribution.values()))
                / max(sum(distribution.values()) / max(len(distribution), 1), 1),
                3,
            ),
        },
    }

    if redis_url:
        try:
            import redis  # type: ignore

            r = redis.from_url(redis_url, decode_responses=False)
            r.flushdb()
            t0 = time.perf_counter()
            for kind, key in ops:
                if kind == "set":
                    r.set(key, key.encode("ascii"))
                else:
                    r.get(key)
            redis_seconds = time.perf_counter() - t0
            info = r.info("memory")
            report["redis"] = {
                "seconds": round(redis_seconds, 3),
                "ops_per_sec": int(n_ops / max(redis_seconds, 1e-9)),
                "memory_used_bytes": info.get("used_memory", 0),
                "memory_used_kb": round(info.get("used_memory", 0) / 1024, 1),
            }
            report["distcache_vs_redis"] = {
                "speedup_factor": round(redis_seconds / max(dc_seconds, 1e-9), 2),
            }
        except Exception as exc:  # noqa: BLE001
            report["redis"] = {"error": str(exc)}

    return report
