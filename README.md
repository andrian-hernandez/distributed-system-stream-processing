# Distributed System Stream Processing & Cache Optimization

A focused Python prototype demonstrating three core distributed-system concerns:

1. A two-level cache hierarchy using in-memory L1 caching and a Redis-backed L2 adapter.
2. Deterministic partition ownership and partition rebalancing across nodes.
3. An event-streaming pipeline with partition routing, duplicate protection, retries, failure tracking, and configurable latency-SLA measurement.

The implementation is intentionally local and testable. It demonstrates the core mechanisms without claiming to be a production Kubernetes, Kafka, or Redis Cluster deployment.

## Scope

- Distributed Redis caching hierarchy
- Partition ownership and rebalancing
- Event streaming and partition routing
- Failure handling and retry behavior
- Duplicate-event protection
- Configurable latency-SLA measurement
- Automated tests
- Local performance benchmarking

## Architecture

### Cache path

```text
Source / Loader
      |
      v
L2 Redis Adapter
      |
      v
L1 In-Memory Cache
      |
      v
Application
```

### Streaming path

```text
Event
  |
  v
Deterministic Partition Routing
  |
  +-- Partition 0 --+
  +-- Partition 1 --+--> Consumer / Processor
  +-- Partition 2 --+
  +-- Partition 3 --+
                         |
                         +-- Success
                         +-- Retry
                         +-- Failed Event
```

## Project Structure

```text
distributed-system-stream-processing/
├── src/
│   ├── cache/
│   │   ├── __init__.py
│   │   ├── l1_cache.py
│   │   ├── redis_store.py
│   │   └── hierarchy.py
│   ├── partitioning/
│   │   ├── __init__.py
│   │   └── rebalancer.py
│   └── streaming/
│       ├── __init__.py
│       ├── events.py
│       └── pipeline.py
├── tests/
│   ├── test_cache.py
│   ├── test_rebalancing.py
│   └── test_streaming.py
├── benchmarks/
│   ├── run_benchmarks.py
│   └── benchmark_results.json
├── examples/
├── requirements.txt
└── pyproject.toml
```

## 1. Cache Hierarchy

The cache uses:

```text
L1 memory cache
      |
    miss
      v
L2 Redis
      |
    miss
      v
Source loader
```

### L1

- Thread-safe in-memory cache
- Optional TTL expiration
- Maximum cache size
- Eviction when capacity is reached

### L2 Redis

`RedisStore` provides the Redis adapter. Values are JSON serialized and may use TTL.

### Failure handling

Redis failures are handled without an unhandled application exception. The source loader can provide the value when Redis is unavailable, and the error is recorded in cache metrics.

## 2. Partition Rebalancing

`PartitionRebalancer` tracks partition ownership across nodes and deterministically redistributes partitions.

Example:

```text
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
```

Ownership validation ensures that partitions are neither duplicated nor dropped.

The implementation also rejects invalid node names, partition IDs, and duplicate configuration.

## 3. Event Streaming Pipeline

The pipeline provides:

- Event validation
- Deterministic partition routing
- Per-partition queues
- Duplicate protection
- Configurable retries
- Permanent failure tracking
- Latency measurement
- Configurable SLA evaluation

Event keys are routed using SHA-256 hashing. A fixed key maps consistently to the same partition for a fixed partition count.

## 4. Latency SLA

The pipeline measures event creation-to-processing latency and reports:

- Event count
- Average latency
- P95 latency
- Configured SLA
- SLA pass/fail

No production SLA is assumed because the task specification does not provide a fixed threshold. The benchmark uses a configurable 1,000 ms SLA for verification.

## 5. Running the Project

### Create and activate the virtual environment

Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### Install dependencies

```powershell
pip install -r requirements.txt
```

## 6. Run Tests

```powershell
pytest
```

Latest verified result:

```text
22 passed
0 failed
```

Coverage includes cache behavior, Redis failure handling, partition assignment/rebalancing, ownership integrity, event routing, duplicate protection, retries, permanent failures, latency statistics, and invalid configuration paths.

## 7. Run Benchmarks

```powershell
python benchmarks\run_benchmarks.py
```

Results are written to:

```text
benchmarks/benchmark_results.json
```

### Latest Verified Local Benchmark

#### Cache

```text
Operations:              1,000
L1 hits:                    999
L1 misses:                    1
L2 hits:                     0
L2 misses:                   1
Source loads:                1
Redis errors:                0
Average operation:     0.000976 ms
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

Ownership valid before:     Yes
Ownership valid after:      Yes
Balanced before:            No
Balanced after:             Yes
Rebalance time:         0.028 ms
```

#### Streaming

```text
Published events:       1,000
Processed events:       1,000
Failed events:              0
Partitions:                  4

Publish time:            2.906 ms
Process time:            2.590 ms
Total local time:        5.496 ms
Measured throughput: 181,963.75 events/sec

Average latency:         8.710 ms
P95 latency:            12.947 ms
Maximum latency:        14.813 ms

Configured SLA:       1,000 ms
SLA result:               PASS
```

> The throughput figure is a local Python benchmark using in-process queues and a local test double. It is not a production Kafka/Redis throughput claim.

## 8. Redis Integration

The production-facing cache abstraction is implemented through `RedisStore`:

```python
from src.cache.redis_store import RedisStore

store = RedisStore(redis_client)
```

Automated tests use a deterministic fake Redis implementation, so the test suite does not require an external Redis server.

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

## 10. Scope and Limitations

This is a focused implementation prototype. It does not claim to provide:

- A production Redis Cluster
- A Kafka-compatible broker
- Kubernetes deployment
- Cross-machine network orchestration
- Persistent event storage
- Automatic node discovery
- Production-grade consensus
- Distributed locking
- Cloud infrastructure

The repository demonstrates the core algorithms, interfaces, failure handling, testing, and measurable behavior required by the project scope.

## 11. Verification Summary

Latest local verification:

```text
Test suite:
22 passed
0 failed

Cache:
999/1000 L1 hits
1 source load
0 Redis errors

Rebalancing:
24 partitions
4 nodes
18/2/1/3 -> 6/6/6/6
Ownership valid
Balanced after rebalance

Streaming:
1000/1000 processed
0 failures
P95: 12.947 ms
SLA: PASS
```

## Verification Commands

From the project root:

```powershell
pytest
python benchmarks\run_benchmarks.py
```

Both commands should complete successfully before submitting a new evaluation/review version.
