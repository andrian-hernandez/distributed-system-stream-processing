from src.cache.hierarchy import CacheHierarchy
from src.cache.l1_cache import L1Cache


class FakeRedis:
    def __init__(self):
        self.data = {}
        self.fail = False

    def get(self, key):
        if self.fail:
            raise ConnectionError("Redis unavailable")
        return self.data.get(key)

    def set(self, key, value, ttl_seconds=None):
        if self.fail:
            raise ConnectionError("Redis unavailable")
        self.data[key] = value

    def delete(self, key):
        if self.fail:
            raise ConnectionError("Redis unavailable")
        self.data.pop(key, None)


def test_l1_hit_avoids_l2_and_source():
    redis = FakeRedis()
    cache = CacheHierarchy(L1Cache(), redis)

    calls = {"count": 0}

    def loader():
        calls["count"] += 1
        return {"name": "item"}

    assert cache.get("item", loader) == {"name": "item"}
    assert cache.get("item", loader) == {"name": "item"}

    assert calls["count"] == 1
    assert cache.metrics.l1_hits == 1
    assert cache.metrics.source_loads == 1


def test_l2_hit_promotes_value_to_l1():
    redis = FakeRedis()
    redis.data["item"] = {"name": "redis-item"}

    cache = CacheHierarchy(L1Cache(), redis)

    calls = {"count": 0}

    def loader():
        calls["count"] += 1
        return {"name": "source-item"}

    assert cache.get("item", loader) == {"name": "redis-item"}
    assert cache.metrics.l2_hits == 1
    assert calls["count"] == 0

    assert cache.get("item", loader) == {"name": "redis-item"}
    assert cache.metrics.l1_hits == 1


def test_cache_miss_loads_source_and_populates_both_levels():
    redis = FakeRedis()
    cache = CacheHierarchy(L1Cache(), redis)

    def loader():
        return {"id": 123}

    result = cache.get("user:123", loader)

    assert result == {"id": 123}
    assert redis.data["user:123"] == {"id": 123}
    assert cache.l1.get("user:123") == {"id": 123}
    assert cache.metrics.l2_misses == 1
    assert cache.metrics.source_loads == 1


def test_invalidation_removes_both_levels():
    redis = FakeRedis()
    cache = CacheHierarchy(L1Cache(), redis)

    cache.get("item", lambda: "value")
    cache.invalidate("item")

    assert cache.l1.get("item") is None
    assert redis.get("item") is None


def test_redis_failure_does_not_raise_unhandled_error():
    redis = FakeRedis()
    cache = CacheHierarchy(L1Cache(), redis)

    redis.fail = True

    result = cache.get("item", lambda: "source-value")

    assert result == "source-value"
    assert cache.metrics.redis_errors == 2
