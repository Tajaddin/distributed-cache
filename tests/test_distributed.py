"""DistributedCache tests."""

from __future__ import annotations

from distcache import DistributedCache


def test_set_then_get_through_cluster():
    dc = DistributedCache(["a", "b", "c", "d"], per_node_max_size=100)
    for i in range(50):
        dc.set(f"k{i}", i)
    assert dc.get("k7") == 7
    assert dc.get("missing") is None


def test_key_lands_on_same_node_consistently():
    dc = DistributedCache(["a", "b", "c"], per_node_max_size=100)
    dc.set("hot", "value-1")
    # Repeated gets stay on same node; check via total_stats hits
    for _ in range(10):
        assert dc.get("hot") == "value-1"
    assert dc.total_stats().hits == 10


def test_remove_node_orphans_some_keys():
    dc = DistributedCache(["a", "b", "c"], per_node_max_size=100)
    for i in range(60):
        dc.set(f"k{i}", i)
    dc.remove_node("a")
    # Some keys are now orphaned (their original node is gone). They map to
    # surviving nodes which don't have them. So we get misses for ~1/3 of keys.
    misses_before = dc.total_stats().misses
    for i in range(60):
        dc.get(f"k{i}")
    new_misses = dc.total_stats().misses - misses_before
    # Roughly 1/3 should miss (a's share)
    assert 10 <= new_misses <= 30, new_misses


def test_add_node_does_not_lose_pre_existing_keys_when_their_owner_unchanged():
    """The ring is consistent: most keys keep their home node after a join."""
    dc = DistributedCache(["a", "b", "c"], per_node_max_size=1000)
    for i in range(300):
        dc.set(f"k{i}", i)
    dc.add_node("d", per_node_max_size=1000)
    keep = sum(1 for i in range(300) if dc.get(f"k{i}") == i)
    # ~3/4 of keys keep their old home (the other ~1/4 move onto d and miss).
    assert keep >= 200, keep


def test_key_distribution_is_balanced():
    dc = DistributedCache(["a", "b", "c", "d"], per_node_max_size=10, virtual_replicas=200)
    keys = [f"k{i}" for i in range(10_000)]
    dist = dc.key_distribution(keys)
    expected = 10_000 / 4
    for n, c in dist.items():
        assert 0.8 * expected <= c <= 1.2 * expected, dist


def test_aggregate_stats_sum_across_nodes():
    dc = DistributedCache(["a", "b"], per_node_max_size=10)
    dc.set("x", 1); dc.set("y", 2); dc.set("z", 3)
    dc.get("x"); dc.get("y"); dc.get("z"); dc.get("missing")
    s = dc.total_stats()
    assert s.set_count == 3
    assert s.hits == 3
    assert s.misses == 1
