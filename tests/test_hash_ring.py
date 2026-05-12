"""ConsistentHashRing tests."""

from __future__ import annotations

import pytest

from distcache import ConsistentHashRing


def test_route_returns_none_when_empty():
    r = ConsistentHashRing()
    assert r.route("anything") is None


def test_route_is_deterministic():
    r = ConsistentHashRing(virtual_replicas=50)
    for n in ["a", "b", "c"]:
        r.add_node(n)
    target = r.route("key1")
    for _ in range(10):
        assert r.route("key1") == target


def test_distribution_is_roughly_even():
    r = ConsistentHashRing(virtual_replicas=200)
    nodes = ["n0", "n1", "n2", "n3"]
    for n in nodes:
        r.add_node(n)
    counts: dict[str, int] = {n: 0 for n in nodes}
    for i in range(20_000):
        counts[r.route(f"k{i}")] += 1
    expected = 20_000 / 4
    # within 20% of even
    for n, c in counts.items():
        assert 0.8 * expected <= c <= 1.2 * expected, counts


def test_removing_a_node_only_re_homes_its_fraction():
    r = ConsistentHashRing(virtual_replicas=200)
    for n in ["a", "b", "c", "d"]:
        r.add_node(n)
    keys = [f"k{i}" for i in range(10_000)]
    stats = r.rebalance_stats(keys, mutator=lambda: r.remove_node("a"))
    # Removing 1/4 of the ring should re-home roughly 25% of keys.
    assert 0.18 <= stats.fraction_moved <= 0.32, stats.fraction_moved


def test_adding_node_only_steals_fraction():
    r = ConsistentHashRing(virtual_replicas=200)
    for n in ["a", "b", "c"]:
        r.add_node(n)
    keys = [f"k{i}" for i in range(10_000)]
    stats = r.rebalance_stats(keys, mutator=lambda: r.add_node("d"))
    # Adding 1 of 4 nodes should re-home ~25% of keys onto it.
    assert 0.18 <= stats.fraction_moved <= 0.32, stats.fraction_moved


def test_duplicate_add_is_noop():
    r = ConsistentHashRing()
    r.add_node("x")
    r.add_node("x")
    assert r.nodes == ["x"]


def test_remove_missing_is_noop():
    r = ConsistentHashRing()
    r.add_node("a")
    r.remove_node("z")  # no error
    assert r.nodes == ["a"]


def test_rejects_zero_virtual_replicas():
    with pytest.raises(ValueError):
        ConsistentHashRing(virtual_replicas=0)
