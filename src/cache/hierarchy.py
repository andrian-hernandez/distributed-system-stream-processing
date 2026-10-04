from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from src.cache.l1_cache import L1Cache


@dataclass
class CacheMetrics:
    l1_hits: int = 0
    l1_misses: int = 0
    l2_hits: int = 0
    l2_misses: int = 0
    source_loads: int = 0
    redis_errors: int = 0


class CacheHierarchy:
    """
    Two-level cache hierarchy:

        L1 memory -> L2 Redis -> source

    L2 values are promoted into L1 on an L2 hit.
    """

    def __init__(
        self,
        l1: L1Cache,
        l2: Any,
        default_ttl_seconds: float | None = 60,
    ) -> None:
        if default_ttl_seconds is not None and default_ttl_seconds <= 0:
            raise ValueError("default_ttl_seconds must be greater than zero.")

        self.l1 = l1
        self.l2 = l2
        self.default_ttl_seconds = default_ttl_seconds
        self.metrics = CacheMetrics()

    def get(
        self,
        key: str,
        loader: Callable[[], Any],
        ttl_seconds: float | None = None,
    ) -> Any:
        value = self.l1.get(key)

        if value is not None:
            self.metrics.l1_hits += 1
            return value

        self.metrics.l1_misses += 1

        try:
            value = self.l2.get(key)
        except Exception:
            self.metrics.redis_errors += 1
            value = None

        if value is not None:
            self.metrics.l2_hits += 1
            self.l1.set(
                key,
                value,
                ttl_seconds or self.default_ttl_seconds,
            )
            return value

        self.metrics.l2_misses += 1

        value = loader()
        self.metrics.source_loads += 1

        effective_ttl = ttl_seconds or self.default_ttl_seconds

        self.l1.set(key, value, effective_ttl)

        try:
            self.l2.set(key, value, effective_ttl)
        except Exception:
            self.metrics.redis_errors += 1

        return value

    def invalidate(self, key: str) -> None:
        self.l1.delete(key)

        try:
            self.l2.delete(key)
        except Exception:
            self.metrics.redis_errors += 1

    def clear_l1(self) -> None:
        self.l1.clear()
