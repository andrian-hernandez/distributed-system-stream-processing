# Distributed System Stream Processing & Cache Optimization

A focused Python prototype demonstrating three core distributed-system concerns:

1. A two-level cache hierarchy using in-memory L1 caching and a Redis-backed L2 adapter.
2. Deterministic partition ownership and partition rebalancing across nodes.
3. An event-streaming pipeline with partition routing, duplicate protection, retries, failure tracking, and configurable latency-SLA measurement.

The implementation is intentionally local and testable. It demonstrates the core mechanisms without claiming to be a production Kubernetes, Kafka, or Redis Cluster deployment.

---

## Scope

The implementation addresses the following requirements:

- Distributed Redis caching hierarchy
- Partition ownership and rebalancing
- Event streaming and partition routing
- Failure handling and retry behavior
- Duplicate-event protection
- Latency measurement against a configurable SLA
- Automated tests
- Local performance benchmarking

---

## Architecture

### Cache path

```text
                         ┌─────────────────────┐
                         │       Source        │
                         │  DB / API / Loader  │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │    L2 Redis Cache   │
                         │   RedisStore API    │
                         └──────────┬──────────┘
                                    │
                                    ▼
                         ┌─────────────────────┐
                         │    L1 Memory Cache  │
                         │   TTL + max size    │
                         └──────────┬──────────┘
                                    │
                                    ▼
                              Application
```

### Streaming path

```text
Event
  │
  ▼
Deterministic Partition Routing
  │
  ├── Partition 0 ──┐
  ├── Partition 1 ──┤
  ├── Partition 2 ──┤──► Consumer / Processor
  └── Partition 3 ──┘
                         │
                         ├── Success
                         ├── Retry
                         └── Failed Event
```

Partition ownership can be redistributed when node load becomes imbalanced.

---

## Project Structure

```text
distributed-system-stream-processing/
│
├── src/
│   ├── cache/
│   │   ├── __init__.py
│   │   ├── l1_cache.py
│   │   ├── redis_store.py
│   │   └── hierarchy.py
│   │
│   ├── partitioning/
│   │   ├── __init__.py
│   │   └── rebalancer.py
│   │
│   └── streaming/
│       ├── __init__.py
│       ├── events.py
│       └── pipeline.py
│
├── tests/
│   ├── test_cache.py
│   ├── test_rebalancing.py
│   └── test_streaming.py
│
├── benchmarks/
│   ├── run_benchmarks.py
│   └── benchmark_results.json
│
├── examples/
├── requirements.txt
└── pyproject.toml
```

---

## 1. Cache Hierarchy

The cache implements a two-level lookup strategy:

```text
L1 memory cache
      ↓ miss
L2 Redis
      ↓ miss
Source loader
```

### L1 Cache

The L1 cache provides:

- In-memory access
- Thread-safe operations
- Optional TTL expiration
- Maximum cache size
- Eviction of the oldest entry when capacity is reached

### L2 Redis

`RedisStore` provides a small adapter around a Redis client.

Values are serialized as JSON and can be stored with an optional TTL.

### Cache Failure Handling

Redis failures do not cause an unhandled application exception.

If Redis is unavailable:

1. L1 is checked.
2. Redis failure is recorded.
3. The source loader is used.
4. The value is returned to the caller.
5. Metrics record the Redis error.

This provides graceful degradation when the distributed cache is unavailable.

---

## 2. Partition Rebalancing

`PartitionRebalancer` maintains ownership of partitions across nodes.

Example initial imbalance:

```text
node-a: 18 partitions
node-b:  2 partitions
node-c:  1 partition
node-d:  3 partitions
```

After deterministic rebalancing:

```text
node-a: 6
node-b: 6
node-c: 6
node-d: 6
```

The implementation validates that:

- Every known partition has an owner.
- No partition is duplicated.
- No partition is dropped.
- Unknown partitions are rejected.
- Unknown nodes are rejected.
- Duplicate node names are rejected.
- Duplicate partition IDs are rejected.

The rebalance operation produces deterministic ownership based on sorted partition IDs.

---

## 3. Event Streaming Pipeline

The streaming pipeline provides:

- Event creation and validation
- Deterministic partition routing
- Per-partition queues
- Duplicate-event protection
- Configurable retries
- Permanent failure tracking
- Processing latency measurement
- Configurable SLA evaluation

### Partition Routing

An event key is hashed using SHA-256 and mapped to a partition.

The same key therefore consistently maps to the same partition for a fixed partition count.

### Retry Handling

The processor can retry failed events up to the configured retry limit.

For example:

```text
Attempt 1 → failure
Attempt 2 → failure
Attempt 3 → success
```

The event is then recorded as successfully processed.

If all attempts fail, the event is added to the failed-event collection.

### Duplicate Protection

Successfully processed event IDs are recorded.

If the same event is published more than once, subsequent occurrences are skipped after the first successful processing.

---

## 4. Latency SLA

The pipeline measures the elapsed time between event creation and successful processing.

The SLA is configurable:

```python
EventPipeline(
    partition_count=4,
    latency_sla_ms=1000,
)
```

The pipeline reports:

- Event count
- Average latency
- P95 latency
- Configured SLA
- SLA pass/fail status

No fixed production SLA is assumed because the task specification does not define a specific threshold.

---

## 5. Running the Project

### Create and activate the virtual environment

Windows PowerShell:

```powershell
python -m venv .venv
.venv\\Scripts\\Activate.ps1
```

### Install dependencies

```powershell
pip install -r requirements.txt
```

---

## 6. Run Tests

```powershell
pytest
```

Latest local verification:

```text
22 passed
0 failed
```

Test coverage includes:

### Cache

- L1 cache hits
- L2 cache hits
- Cache misses
- Source loading
- Invalidation
- Redis failure handling

### Partitioning

- Initial assignment
- Balance validation
- Rebalancing
- Ownership integrity
- Invalid partitions
- Invalid nodes
- Duplicate configuration validation

### Streaming

- Event validation
- Deterministic routing
- Publishing and consumption
- Duplicate protection
- Retry handling
- Permanent failures
- Latency statistics
- Unknown partition handling

---

## 7. Run Benchmarks

```powershell
python benchmarks\\run_benchmarks.py
```

Results are written to:

```text
benchmarks/benchmark_results.json
```

### Latest Local Benchmark

#### Cache

```text
Operations:              1,000
L1 hits:                    999
L1 misses:                    1
Source loads:                 1
Redis errors:                 0
Average operation:     0.000895 ms
```

#### Partition Rebalancing

```text
Partitions:                 24
Nodes:                       4

Before:
node-a: 18
node-b:  2
node-c:  1
node-d:  3

After:
node-a: 6
node-b: 6
node-c: 6
node-d: 6

Ownership valid:           Yes
Balanced after rebalance:  Yes
Rebalance time:       0.016 ms
```

#### Streaming

```text
Published events:       1,000
Processed events:       1,000
Failed events:              0
Partitions:                  4

Total local time:       5.950 ms
Measured throughput: 168,078.53 events/sec

Average latency:        8.560 ms
P95 latency:           11.868 ms
Maximum latency:       13.039 ms

Configured SLA:      1,000 ms
SLA result:              PASS
```

> The throughput figure is a local Python benchmark using in-process queues and a local test double. It is not a production Kafka/Redis throughput claim.

---

## 8. Redis Integration

The production-facing cache abstraction is implemented through `RedisStore`.

A Redis client can be supplied to the adapter:

```python
from src.cache.redis_store import RedisStore

store = RedisStore(redis_client)
```

The automated tests use a deterministic fake Redis implementation so that the test suite does not require an external Redis server.

This keeps the test suite reproducible and offline.

---

## 9. Failure and Quality Controls

The implementation explicitly handles:

- Invalid cache configuration
- Invalid TTL values
- Redis connection failures
- Empty event keys
- Invalid event payloads
- Invalid partition counts
- Invalid node configurations
- Duplicate partitions
- Unknown partitions
- Unknown target nodes
- Processor failures
- Retry exhaustion
- Duplicate events

The test suite verifies these paths where applicable.

---

## 10. Scope and Limitations

This repository is a focused implementation prototype.

It does **not** claim to provide:

- A production Redis Cluster
- A Kafka-compatible broker
- Kubernetes deployment
- Cross-machine network orchestration
- Persistent event storage
- Automatic node discovery
- Production-grade consensus
- Distributed locking
- Cloud infrastructure

Those components are outside the stated implementation scope.

The repository instead demonstrates the core algorithms, interfaces, failure handling, testing, and measurable behavior required by the project specification.

---

## 11. Verification Summary

Latest verification completed locally:

```text
Test suite:
22 passed
0 failed

Benchmark:
completed successfully

Cache:
999/1000 L1 hits
0 Redis errors

Rebalancing:
24 partitions
4 nodes
18/2/1/3 → 6/6/6/6

Streaming:
1000/1000 processed
0 failures
P95: 11.868 ms
SLA: PASS
```

The implementation is locally tested and benchmarked against the defined acceptance criteria.

---

## Verification Commands

From the project root:

```powershell
pytest
python benchmarks\\run_benchmarks.py
```

Both commands should complete successfully before submitting a new evaluation/review version.
