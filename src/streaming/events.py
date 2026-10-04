from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


@dataclass(frozen=True)
class Event:
    event_id: str
    key: str
    payload: dict[str, Any]
    created_at: datetime

    @classmethod
    def create(
        cls,
        key: str,
        payload: dict[str, Any],
    ) -> "Event":
        if not key:
            raise ValueError("Event key cannot be empty.")

        if not isinstance(payload, dict):
            raise ValueError("Event payload must be a dictionary.")

        return cls(
            event_id=str(uuid4()),
            key=key,
            payload=payload,
            created_at=datetime.now(timezone.utc),
        )
