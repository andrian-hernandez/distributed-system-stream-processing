from __future__ import annotations

import json
import statistics
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.cache.hierarchy import CacheHierarchy
from src.cache.l1_cache import L1Cache
from src.partitioning.rebalancer import Partition, PartitionRebalancer
from src.streaming.events import Event
from src.streaming.pipeline import EventPipeline


class FakeRedis:
    """Deterministic Redis test double for local benchmarking."""

    def __init__(self) -> None:
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, ttl_seconds=None):
        self.data[key] = value

    def delete(self, key):
        self.data.pop(key, None)


def benchmark_cache() -> dict:
    redis = FakeRedis()

    cache = CacheHierarchy(
        L1Cache(max_size=1000),
        redis,
        default_ttl_seconds=60,
    )

    source_calls = {"count": 0}

    def loader():
        source_calls["count"] += 1
        return {
            "customer_id": 1001,
            "status": "active",
        }

    start = time.perf_counter()

    for _ in range(1000):
        cache.get(
            "customer:1001",
            loader,
        )

    elapsed_ms = (
        time.perf_counter() - start
    ) * 1000

    return {
        "operations": 1000,
        "elapsed_ms": round(elapsed_ms, 3),
        "average_operation_ms": round(
            elapsed_ms / 1000,
            6,
        ),
        "l1_hits": cache.metrics.l1_hits,
        "l1_misses": cache.metrics.l1_misses,
        "l2_hits": cache.metrics.l2_hits,
        "l2_misses": cache.metrics.l2_misses,
        "source_loads": cache.metrics.source_loads,
        "redis_errors": cache.metrics.redis_errors,
        "source_loader_calls": source_calls["count"],
    }


def benchmark_rebalancing() -> dict:
    partitions = [
        Partition(partition_id=index)
        for index in range(24)
    ]

    rebalancer = PartitionRebalancer(
        nodes=[
            "node-a",
            "node-b",
            "node-c",
            "node-d",
        ],
        partitions=partitions,
        imbalance_threshold=0.20,
    )

    # Create an intentionally imbalanced ownership state.
    rebalancer.ownership = {
        "node-a": list(range(18)),
        "node-b": [18, 19],
        "node-c": [20],
        "node-d": [21, 22, 23],
    }

    before_loads = rebalancer.loads()
    before_valid = rebalancer.validate_ownership()
    before_balanced = rebalancer.is_balanced()

    start = time.perf_counter()

    after_ownership = rebalancer.rebalance()

    elapsed_ms = (
        time.perf_counter() - start
    ) * 1000

    after_loads = rebalancer.loads()

    return {
        "partition_count": len(partitions),
        "node_count": len(rebalancer.nodes),
        "before_loads": before_loads,
        "before_ownership_valid": before_valid,
        "before_balanced": before_balanced,
        "after_ownership": after_ownership,
        "after_loads": after_loads,
        "after_ownership_valid": rebalancer.validate_ownership(),
        "after_balanced": rebalancer.is_balanced(),
        "rebalance_elapsed_ms": round(
            elapsed_ms,
            3,
        ),
    }


def benchmark_streaming() -> dict:
    pipeline = EventPipeline(
        partition_count=4,
        latency_sla_ms=1000,
        max_retries=2,
    )

    events = [
        Event.create(
            key=f"customer-{index % 100}",
            payload={"sequence": index},
        )
        for index in range(1000)
    ]

    publish_start = time.perf_counter()

    for event in events:
        pipeline.publish(event)

    publish_elapsed_ms = (
        time.perf_counter() - publish_start
    ) * 1000

    process_start = time.perf_counter()

    processed = pipeline.consume(
        lambda event: None
    )

    process_elapsed_ms = (
        time.perf_counter() - process_start
    ) * 1000

    stats = pipeline.latency_statistics()

    latencies = [
        item.latency_ms
        for item in pipeline.processed_events
    ]

    total_elapsed_ms = (
        publish_elapsed_ms
        + process_elapsed_ms
    )

    return {
        "published_events": len(events),
        "processed_events": processed,
        "failed_events": len(
            pipeline.failed_events
        ),
        "partition_count": (
            pipeline.partition_count
        ),
        "publish_elapsed_ms": round(
            publish_elapsed_ms,
            3,
        ),
        "process_elapsed_ms": round(
            process_elapsed_ms,
            3,
        ),
        "total_elapsed_ms": round(
            total_elapsed_ms,
            3,
        ),
        "events_per_second": round(
            processed
            / max(
                total_elapsed_ms / 1000,
                0.000001,
            ),
            2,
        ),
        "latency_statistics": stats,
        "latency_min_ms": round(
            min(latencies),
            3,
        ),
        "latency_max_ms": round(
            max(latencies),
            3,
        ),
        "latency_mean_ms": round(
            statistics.mean(latencies),
            3,
        ),
    }


def main() -> None:
    output_dir = Path("benchmarks")

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    results = {
        "benchmark": (
            "Distributed System Stream Processing "
            "& Cache Optimization"
        ),
        "status": "completed",
        "cache": benchmark_cache(),
        "partition_rebalancing": (
            benchmark_rebalancing()
        ),
        "streaming": benchmark_streaming(),
    }

    output_path = (
        output_dir / "benchmark_results.json"
    )

    output_path.write_text(
        json.dumps(
            results,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(
        json.dumps(
            results,
            indent=2,
        )
    )

    print(
        f"\nResults written to: {output_path}"
    )


if __name__ == "__main__":
    main()