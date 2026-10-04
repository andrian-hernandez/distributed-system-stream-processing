from __future__ import annotations

import hashlib
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable

from src.streaming.events import Event


@dataclass
class ProcessedEvent:
    event_id: str
    partition_id: int
    latency_ms: float


class EventPipeline:
    """
    Local event-streaming pipeline with deterministic partition routing,
    duplicate protection, retries, and latency-SLA measurement.
    """

    def __init__(
        self,
        partition_count: int,
        latency_sla_ms: float,
        max_retries: int = 2,
    ) -> None:
        if partition_count <= 0:
            raise ValueError("partition_count must be greater than zero.")

        if latency_sla_ms <= 0:
            raise ValueError("latency_sla_ms must be greater than zero.")

        if max_retries < 0:
            raise ValueError("max_retries cannot be negative.")

        self.partition_count = partition_count
        self.latency_sla_ms = latency_sla_ms
        self.max_retries = max_retries

        self.queues = {
            partition_id: deque()
            for partition_id in range(partition_count)
        }

        self.processed_ids: set[str] = set()
        self.processed_events: list[ProcessedEvent] = []
        self.failed_events: list[str] = []
        self.retry_counts: defaultdict[str, int] = defaultdict(int)

    def partition_for(self, key: str) -> int:
        if not key:
            raise ValueError("Partition key cannot be empty.")

        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()

        return int(digest, 16) % self.partition_count

    def publish(self, event: Event) -> int:
        if not isinstance(event, Event):
            raise ValueError("publish expects an Event.")

        partition_id = self.partition_for(event.key)

        self.queues[partition_id].append(event)

        return partition_id

    def consume(
        self,
        processor: Callable[[Event], None],
        *,
        partition_id: int | None = None,
    ) -> int:
        if partition_id is not None and partition_id not in self.queues:
            raise ValueError(f"Unknown partition: {partition_id}")

        partition_ids = (
            [partition_id]
            if partition_id is not None
            else list(self.queues.keys())
        )

        processed_count = 0

        for current_partition in partition_ids:
            queue = self.queues[current_partition]

            while queue:
                event = queue.popleft()

                if event.event_id in self.processed_ids:
                    continue

                success = False

                for attempt in range(self.max_retries + 1):
                    try:
                        processor(event)
                        success = True
                        break
                    except Exception:
                        self.retry_counts[event.event_id] += 1

                        if attempt == self.max_retries:
                            self.failed_events.append(event.event_id)

                if not success:
                    continue

                consumed_at = datetime.now(timezone.utc)

                latency_ms = (
                    consumed_at - event.created_at
                ).total_seconds() * 1000

                self.processed_ids.add(event.event_id)

                self.processed_events.append(
                    ProcessedEvent(
                        event_id=event.event_id,
                        partition_id=current_partition,
                        latency_ms=latency_ms,
                    )
                )

                processed_count += 1

        return processed_count

    def latency_statistics(self) -> dict[str, float | int | bool]:
        latencies = [
            event.latency_ms
            for event in self.processed_events
        ]

        if not latencies:
            return {
                "count": 0,
                "average_ms": 0.0,
                "p95_ms": 0.0,
                "sla_ms": self.latency_sla_ms,
                "sla_passed": True,
            }

        sorted_latencies = sorted(latencies)

        index = max(
            0,
            min(
                len(sorted_latencies) - 1,
                int(0.95 * len(sorted_latencies)) - 1,
            ),
        )

        p95 = sorted_latencies[index]

        return {
            "count": len(latencies),
            "average_ms": round(statistics.mean(latencies), 3),
            "p95_ms": round(p95, 3),
            "sla_ms": self.latency_sla_ms,
            "sla_passed": p95 <= self.latency_sla_ms,
        }
