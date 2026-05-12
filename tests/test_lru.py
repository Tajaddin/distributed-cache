"""LRUCache unit tests."""

from __future__ import annotations

import time

import pytest

from distcache import LRUCache


def test_set_and_get_returns_value():
    c = LRUCache(max_size=10)
    c.set("a", 1)
    assert c.get("a") == 1
    assert c.stats.hits == 1


def test_missing_key_returns_none_and_records_miss():
    c = LRUCache(max_size=10)
    assert c.get("missing") is None
    assert c.stats.misses == 1


def test_lru_eviction_drops_oldest():
    c = LRUCache(max_size=3)
    c.set("a", 1); c.set("b", 2); c.set("c", 3); c.set("d", 4)
    assert "a" not in c
    assert c.stats.evicted_lru == 1


def test_get_promotes_to_recent():
    c = LRUCache(max_size=3)
    c.set("a", 1); c.set("b", 2); c.set("c", 3)
    c.get("a")           # promote a
    c.set("d", 4)        # should evict b, not a
    assert "a" in c
    assert "b" not in c


def test_set_existing_key_does_not_grow_store():
    c = LRUCache(max_size=2)
    c.set("a", 1); c.set("b", 2)
    c.set("a", 99)
    assert len(c) == 2
    assert c.get("a") == 99


def test_ttl_expiry_on_get():
    c = LRUCache(max_size=10, default_ttl=0.05)
    c.set("a", 1)
    time.sleep(0.07)
    assert c.get("a") is None
    assert c.stats.expired == 1


def test_per_set_ttl_overrides_default():
    c = LRUCache(max_size=10, default_ttl=0.05)
    c.set("a", 1, ttl=1.0)
    time.sleep(0.07)
    assert c.get("a") == 1


def test_ttl_none_disables_expiry():
    c = LRUCache(max_size=10, default_ttl=0.05)
    c.set("a", 1, ttl=None)
    time.sleep(0.07)
    assert c.get("a") == 1


def test_delete_removes_key():
    c = LRUCache(max_size=10)
    c.set("a", 1)
    assert c.delete("a") is True
    assert c.delete("a") is False
    assert "a" not in c


def test_sweep_expired_bulk_collects():
    c = LRUCache(max_size=100, default_ttl=0.02)
    for i in range(20):
        c.set(f"k{i}", i)
    time.sleep(0.03)
    removed = c.sweep_expired()
    assert removed == 20
    assert len(c) == 0


def test_rejects_bad_config():
    with pytest.raises(ValueError):
        LRUCache(max_size=0)
    with pytest.raises(ValueError):
        LRUCache(max_size=1, default_ttl=0)


def test_hit_rate_property():
    c = LRUCache(max_size=10)
    c.set("a", 1)
    c.get("a"); c.get("a"); c.get("missing")
    assert c.stats.hit_rate == pytest.approx(2 / 3)
