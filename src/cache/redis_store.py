from __future__ import annotations

import json
from typing import Any


class RedisStore:
    """Small Redis adapter used as the distributed L2 cache."""

    def __init__(self, client: Any) -> None:
        self.client = client

    def get(self, key: str) -> Any | None:
        raw = self.client.get(key)

        if raw is None:
            return None

        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")

        return json.loads(raw)

    def set(self, key: str, value: Any, ttl_seconds: float | None = None) -> None:
        payload = json.dumps(value)

        if ttl_seconds is None:
            self.client.set(key, payload)
        else:
            if ttl_seconds <= 0:
                raise ValueError("ttl_seconds must be greater than zero.")

            self.client.set(key, payload, ex=max(1, int(ttl_seconds)))

    def delete(self, key: str) -> None:
        self.client.delete(key)
