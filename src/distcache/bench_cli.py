"""``distcache-bench`` — run the benchmark from CLI."""

from __future__ import annotations

import argparse
import sys

from distcache.bench import run_bench


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=4)
    ap.add_argument("--ops", type=int, default=200_000)
    ap.add_argument("--keys", type=int, default=50_000)
    ap.add_argument("--read-ratio", type=float, default=0.8)
    ap.add_argument("--per-node-size", type=int, default=10_000)
    ap.add_argument("--redis-url", default=None, help="optional redis URL; if set, run the same workload against redis")
    args = ap.parse_args()
    report = run_bench(
        n_nodes=args.nodes,
        n_ops=args.ops,
        n_keys=args.keys,
        read_ratio=args.read_ratio,
        per_node_size=args.per_node_size,
        redis_url=args.redis_url,
    )
    import json
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
