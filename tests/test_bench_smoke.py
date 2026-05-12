"""Smoke test for the benchmark function with tiny config."""

from __future__ import annotations

from distcache.bench import run_bench


def test_bench_produces_well_formed_report():
    rpt = run_bench(
        n_nodes=3, n_ops=2_000, n_keys=500, read_ratio=0.8, per_node_size=200
    )
    assert "distcache" in rpt
    dc = rpt["distcache"]
    assert dc["ops_per_sec"] > 0
    assert 0.0 <= dc["hit_rate"] <= 1.0
    assert dc["key_distribution_imbalance"] >= 0
