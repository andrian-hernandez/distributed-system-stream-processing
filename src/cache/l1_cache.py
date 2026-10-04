from __future__ import annotations

import time
from dataclasses import dataclass
from threading import RLock
from typing import Any


@dataclass
class CacheEntry:
    value: Any
    expires_at: float | None


class L1Cache:
    """Thread-safe in-memory L1 cache with optional TTL."""

    def __init__(self, max_size: int = 1000) -> None:
        if max_size <= 0:
            raise ValueError("max_size must be greater than zero.")

        self.max_size = max_size
        self._entries: dict[str, CacheEntry] = {}
        self._lock = RLock()

    def get(self, key: str) -> Any | None:
        with self._lock:
            entry = self._entries.get(key)

            if entry is None:
                return None

            if entry.expires_at is not None and time.monotonic() >= entry.expires_at:
                del self._entries[key]
                return None

            return entry.value

    def set(self, key: str, value: Any, ttl_seconds: float | None = None) -> None:
        if ttl_seconds is not None and ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be greater than zero.")

        with self._lock:
            if key not in self._entries and len(self._entries) >= self.max_size:
                oldest_key = next(iter(self._entries))
                del self._entries[oldest_key]

            expires_at = (
                time.monotonic() + ttl_seconds
                if ttl_seconds is not None
                else None
            )

            self._entries[key] = CacheEntry(
                value=value,
                expires_at=expires_at,
            )

    def delete(self, key: str) -> None:
        with self._lock:
            self._entries.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)
