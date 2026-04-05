"""Redis integration tests."""

from typing import cast

import pytest
import redis
from redis import ConnectionPool as RedisConnectionPool

from .conftest import (
    PIPELINE_BATCH_SIZE,
    TOP_POSTS_LIMIT,
    TOTAL_FEED_ITEMS,
    RedisClient,
)


@pytest.mark.smoke
@pytest.mark.integration
def test_redis_connectivity(redis_client_from_pool: redis.Redis) -> None:
    """Test Redis connection via connection pool."""
    client = cast("RedisClient", redis_client_from_pool)
    assert client.ping() is True
    client.set("test:embedding", "vector:0.1,0.2")
    assert client.get("test:embedding") == "vector:0.1,0.2"
    print("✅ Redis: connection via pool + set/get work")


@pytest.mark.integration
def test_redis_sorted_sets(redis_client_from_pool: redis.Redis) -> None:
    """Test working with Redis Sorted Sets (for feeds) via connection pool."""
    # Create user feed
    user_id = 123
    feed_key = f"feed:{user_id}"

    # ZADD: post_id -> score
    client = cast("RedisClient", redis_client_from_pool)
    client.zadd(feed_key, {"101": 0.95, "102": 0.87, "103": 0.76, "104": 0.65})

    # Get top 3 posts
    top_posts = client.zrevrange(feed_key, 0, TOP_POSTS_LIMIT - 1, withscores=True)

    assert len(top_posts) == TOP_POSTS_LIMIT
    assert top_posts[0] == ("101", 0.95)  # Highest score
    assert top_posts[1] == ("102", 0.87)
    assert top_posts[2] == ("103", 0.76)

    # Verify element count
    count = client.zcard(feed_key)
    assert count == TOTAL_FEED_ITEMS

    # TTL
    client.expire(feed_key, 3600)
    ttl = client.ttl(feed_key)
    assert ttl > 0

    print("✅ Redis: Sorted Sets for feeds work via connection pool")


@pytest.mark.integration
def test_redis_pipeline_operations(redis_client_from_pool: redis.Redis) -> None:
    """Test batch operations in Redis via pipeline with connection pool."""
    # Create pipeline for batch operations
    client = cast("RedisClient", redis_client_from_pool)
    pipe = client.pipeline()

    # Add multiple keys
    for i in range(PIPELINE_BATCH_SIZE):
        pipe.set(f"test:key:{i}", f"value_{i}")

    # Execute batch
    results = pipe.execute()
    assert len(results) == PIPELINE_BATCH_SIZE
    assert all(r is True for r in results)

    # Verify data
    assert client.get("test:key:50") == "value_50"

    # Cleanup via pipeline
    pipe = client.pipeline()
    for i in range(PIPELINE_BATCH_SIZE):
        pipe.delete(f"test:key:{i}")
    pipe.execute()

    print("✅ Redis: pipeline for batch operations works via connection pool")


@pytest.mark.integration
def test_redis_connection_pool_usage(redis_pool: RedisConnectionPool) -> None:
    """Test Redis connection pool functionality."""
    # Create multiple clients using one pool
    clients: list[redis.Redis] = []
    for i in range(5):
        client = redis.Redis(connection_pool=redis_pool)
        clients.append(client)

        # Each client performs operation
        client.set(f"pool:test:{i}", f"value_{i}")
        value = client.get(f"pool:test:{i}")
        assert value == f"value_{i}"

    print(f"Redis: {len(clients)} clients use one connection pool")

    # Cleanup
    for i in range(5):
        clients[0].delete(f"pool:test:{i}")

    print("Redis: connection pool works correctly")
