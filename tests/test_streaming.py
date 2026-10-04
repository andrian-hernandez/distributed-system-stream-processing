import time

import pytest

from src.streaming.events import Event
from src.streaming.pipeline import EventPipeline


def test_event_requires_key():
    with pytest.raises(ValueError, match="Event key cannot be empty"):
        Event.create("", {"value": 1})


def test_event_requires_dictionary_payload():
    with pytest.raises(ValueError, match="payload must be a dictionary"):
        Event.create("user-1", ["invalid"])


def test_same_key_routes_deterministically():
    pipeline = EventPipeline(
        partition_count=4,
        latency_sla_ms=100,
    )

    assert pipeline.partition_for("customer-1") == pipeline.partition_for(
        "customer-1"
    )


def test_publish_and_consume_event():
    pipeline = EventPipeline(
        partition_count=2,
        latency_sla_ms=100,
    )

    event = Event.create("customer-1", {"value": 10})

    partition = pipeline.publish(event)

    processed = []

    count = pipeline.consume(
        lambda item: processed.append(item.payload)
    )

    assert count == 1
    assert processed == [{"value": 10}]
    assert pipeline.processed_events[0].partition_id == partition


def test_duplicate_event_is_not_processed_twice():
    pipeline = EventPipeline(
        partition_count=2,
        latency_sla_ms=100,
    )

    event = Event.create("customer-1", {"value": 10})

    pipeline.publish(event)
    pipeline.publish(event)

    count = pipeline.consume(lambda item: None)

    assert count == 1
    assert len(pipeline.processed_events) == 1


def test_failed_processing_is_retried():
    pipeline = EventPipeline(
        partition_count=1,
        latency_sla_ms=100,
        max_retries=2,
    )

    event = Event.create("customer-1", {"value": 10})
    pipeline.publish(event)

    attempts = {"count": 0}

    def processor(_):
        attempts["count"] += 1

        if attempts["count"] < 3:
            raise RuntimeError("temporary failure")

    assert pipeline.consume(processor) == 1
    assert attempts["count"] == 3
    assert pipeline.retry_counts[event.event_id] == 2


def test_permanently_failed_event_is_recorded():
    pipeline = EventPipeline(
        partition_count=1,
        latency_sla_ms=100,
        max_retries=2,
    )

    event = Event.create("customer-1", {"value": 10})
    pipeline.publish(event)

    def processor(_):
        raise RuntimeError("permanent failure")

    assert pipeline.consume(processor) == 0
    assert event.event_id in pipeline.failed_events
    assert pipeline.retry_counts[event.event_id] == 3


def test_latency_statistics_and_sla():
    pipeline = EventPipeline(
        partition_count=1,
        latency_sla_ms=1000,
    )

    event = Event.create("customer-1", {"value": 10})
    pipeline.publish(event)

    time.sleep(0.001)

    assert pipeline.consume(lambda _: None) == 1

    stats = pipeline.latency_statistics()

    assert stats["count"] == 1
    assert stats["average_ms"] >= 0
    assert stats["p95_ms"] >= 0
    assert stats["sla_passed"] is True


def test_unknown_partition_is_rejected():
    pipeline = EventPipeline(
        partition_count=2,
        latency_sla_ms=100,
    )

    with pytest.raises(ValueError, match="Unknown partition"):
        pipeline.consume(lambda _: None, partition_id=99)
