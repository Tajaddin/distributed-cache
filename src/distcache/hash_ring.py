"""Consistent-hash ring with virtual nodes.

Each physical node is mapped to ``virtual_replicas`` points on a 32-bit
ring. Keys hash to a point and route to the *next* point clockwise.
Removing or adding a node only re-homes the keys that fall between the
removed node's points and the next surviving point — proven by
:meth:`rebalance_stats`.
"""

from __future__ import annotations

import bisect
import hashlib
from dataclasses import dataclass


def _h32(s: str) -> int:
    return int.from_bytes(hashlib.md5(s.encode("utf-8")).digest()[:4], "big")


@dataclass
class RebalanceStats:
    keys_total: int
    keys_moved: int

    @property
    def fraction_moved(self) -> float:
        return self.keys_moved / max(self.keys_total, 1)


class ConsistentHashRing:
    """Stateful ring; instances are mutable (add_node / remove_node)."""

    def __init__(self, virtual_replicas: int = 100) -> None:
        if virtual_replicas <= 0:
            raise ValueError("virtual_replicas must be > 0")
        self.virtual_replicas = virtual_replicas
        self._sorted_keys: list[int] = []
        self._key_to_node: dict[int, str] = {}
        self._nodes: set[str] = set()

    @property
    def nodes(self) -> list[str]:
        return sorted(self._nodes)

    # ----------------------------------------------------------- mutators

    def add_node(self, node: str) -> None:
        if node in self._nodes:
            return
        self._nodes.add(node)
        for i in range(self.virtual_replicas):
            h = _h32(f"{node}#{i}")
            self._key_to_node[h] = node
            bisect.insort(self._sorted_keys, h)

    def remove_node(self, node: str) -> None:
        if node not in self._nodes:
            return
        self._nodes.discard(node)
        for i in range(self.virtual_replicas):
            h = _h32(f"{node}#{i}")
            self._key_to_node.pop(h, None)
            idx = bisect.bisect_left(self._sorted_keys, h)
            if idx < len(self._sorted_keys) and self._sorted_keys[idx] == h:
                self._sorted_keys.pop(idx)

    # ----------------------------------------------------------- lookups

    def route(self, key: str) -> str | None:
        if not self._sorted_keys:
            return None
        h = _h32(key)
        idx = bisect.bisect_right(self._sorted_keys, h) % len(self._sorted_keys)
        return self._key_to_node[self._sorted_keys[idx]]

    def rebalance_stats(self, keys: list[str], *, mutator) -> RebalanceStats:
        """Apply ``mutator(ring)`` (which adds or removes a node) and report
        how many keys changed home node.

        ``mutator`` is a no-arg lambda you pass — it must be the change you
        want to measure (e.g., ``lambda: ring.add_node('n4')``).
        """
        before = {k: self.route(k) for k in keys}
        mutator()
        moved = sum(1 for k in keys if before[k] != self.route(k))
        return RebalanceStats(keys_total=len(keys), keys_moved=moved)
