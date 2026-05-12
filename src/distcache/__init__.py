"""distributed-cache — LRU + TTL nodes behind a consistent-hashing client.

Exports:

* :class:`LRUCache` — single-node store with bounded size + TTL eviction
* :class:`ConsistentHashRing` — virtual-node hash ring with rebalance metrics
* :class:`DistributedCache` — multi-node client (in-process simulation) using the ring
* :class:`Stats` — hit/miss/eviction counters
"""

from distcache.hash_ring import ConsistentHashRing, RebalanceStats
from distcache.lru import LRUCache, Stats
from distcache.distributed import DistributedCache, Node

__all__ = [
    "ConsistentHashRing",
    "DistributedCache",
    "LRUCache",
    "Node",
    "RebalanceStats",
    "Stats",
]
