"""Single-node LRU cache with TTL eviction.

Backed by ``collections.OrderedDict`` so move-to-end and popitem-from-front
are both O(1). TTL is stored per-entry as an absolute monotonic deadline;
eviction is lazy on read AND opportunistic via :meth:`sweep_expired`.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any


_SENTINEL = object()


@dataclass
class Stats:
    hits: int = 0
    misses: int = 0
    expired: int = 0
    evicted_lru: int = 0
    set_count: int = 0

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total else 0.0


@dataclass
class _Entry:
    value: Any
    expires_at: float | None  # monotonic seconds, or None for no TTL


class LRUCache:
    """Bounded-size LRU cache with optional per-key TTL.

    * ``max_size`` — number of entries before LRU eviction kicks in.
    * ``default_ttl`` — seconds; ``None`` means no automatic TTL.
    """

    def __init__(self, max_size: int = 1024, default_ttl: float | None = None) -> None:
        if max_size <= 0:
            raise ValueError("max_size must be > 0")
        if default_ttl is not None and default_ttl <= 0:
            raise ValueError("default_ttl must be > 0 (or None)")
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._store: OrderedDict[str, _Entry] = OrderedDict()
        self.stats = Stats()

    # ------------------------------------------------------------------ ops

    def get(self, key: str) -> Any:
        entry = self._store.get(key)
        if entry is None:
            self.stats.misses += 1
            return None
        if entry.expires_at is not None and entry.expires_at <= time.monotonic():
            del self._store[key]
            self.stats.expired += 1
            self.stats.misses += 1
            return None
        self._store.move_to_end(key)
        self.stats.hits += 1
        return entry.value

    def set(self, key: str, value: Any, ttl: float | None = _SENTINEL) -> None:  # type: ignore[assignment]
        effective_ttl = self.default_ttl if ttl is _SENTINEL else ttl
        deadline = (time.monotonic() + effective_ttl) if effective_ttl else None
        if key in self._store:
            self._store.move_to_end(key)
            self._store[key] = _Entry(value=value, expires_at=deadline)
        else:
            self._store[key] = _Entry(value=value, expires_at=deadline)
            self._evict_if_needed()
        self.stats.set_count += 1

    def delete(self, key: str) -> bool:
        if key in self._store:
            del self._store[key]
            return True
        return False

    def __contains__(self, key: str) -> bool:
        entry = self._store.get(key)
        if entry is None:
            return False
        if entry.expires_at is not None and entry.expires_at <= time.monotonic():
            return False
        return True

    def __len__(self) -> int:
        return len(self._store)

    # ----------------------------------------------------------- internals

    def _evict_if_needed(self) -> None:
        while len(self._store) > self.max_size:
            self._store.popitem(last=False)
            self.stats.evicted_lru += 1

    def sweep_expired(self) -> int:
        """Hard-scan for expired entries; returns the number deleted. Cheap
        enough to call from a periodic task; cost is O(N)."""
        now = time.monotonic()
        keys = [k for k, e in self._store.items() if e.expires_at is not None and e.expires_at <= now]
        for k in keys:
            del self._store[k]
        self.stats.expired += len(keys)
        return len(keys)
