"""Multi-node distributed cache — in-process simulation.

Each :class:`Node` wraps an :class:`LRUCache`. The :class:`DistributedCache`
client routes every key through a :class:`ConsistentHashRing` so the same
key always lands on the same node (until the ring is rebalanced).

In production each node is a separate process. The interface is identical;
swap the in-process Node for an HTTP/gRPC client and the routing logic is
unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from distcache.hash_ring import ConsistentHashRing
from distcache.lru import LRUCache, Stats


@dataclass
class Node:
    node_id: str
    cache: LRUCache

    @property
    def stats(self) -> Stats:
        return self.cache.stats


class DistributedCache:
    def __init__(
        self,
        node_ids: list[str],
        *,
        per_node_max_size: int = 1024,
        default_ttl: float | None = None,
        virtual_replicas: int = 100,
    ) -> None:
        self.ring = ConsistentHashRing(virtual_replicas=virtual_replicas)
        self.nodes: dict[str, Node] = {}
        for n in node_ids:
            self.add_node(n, per_node_max_size=per_node_max_size, default_ttl=default_ttl)

    def add_node(self, node_id: str, *, per_node_max_size: int, default_ttl: float | None = None) -> None:
        self.ring.add_node(node_id)
        self.nodes[node_id] = Node(
            node_id=node_id,
            cache=LRUCache(max_size=per_node_max_size, default_ttl=default_ttl),
        )

    def remove_node(self, node_id: str) -> None:
        self.ring.remove_node(node_id)
        self.nodes.pop(node_id, None)

    def _route(self, key: str) -> Node:
        nid = self.ring.route(key)
        if nid is None:
            raise RuntimeError("no nodes available")
        return self.nodes[nid]

    # -------------------------------------------------------------- API

    def get(self, key: str) -> Any:
        return self._route(key).cache.get(key)

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        self._route(key).cache.set(key, value, ttl=ttl) if ttl is not None else self._route(key).cache.set(key, value)

    def delete(self, key: str) -> bool:
        return self._route(key).cache.delete(key)

    def __contains__(self, key: str) -> bool:
        return key in self._route(key).cache

    # ---------------------------------------------------------- aggregate

    def total_stats(self) -> Stats:
        agg = Stats()
        for n in self.nodes.values():
            agg.hits += n.stats.hits
            agg.misses += n.stats.misses
            agg.expired += n.stats.expired
            agg.evicted_lru += n.stats.evicted_lru
            agg.set_count += n.stats.set_count
        return agg

    def key_distribution(self, keys: list[str]) -> dict[str, int]:
        out: dict[str, int] = {n: 0 for n in self.nodes}
        for k in keys:
            out[self.ring.route(k)] += 1  # type: ignore[index]
        return out
