"""TTL and feed trimming tests.

Covers:
- Feed TTL (Time To Live) management
- Feed trimming to MAX_FEED_SIZE
- Mastodon-style trimming patterns
- Pipeline performance for batch operations
"""

import time

import pytest
import redis

from .conftest import (
    BASE_USER_ID,
    FEED_TTL_SECONDS,
    MAX_FEED_SIZE,
    feed_key,
    worker_user_id,
)


@pytest.mark.integration
def test_redis_feed_ttl(redis_client: redis.Redis, worker_id: str) -> None:
    """Test that feed has TTL (Time To Live) for automatic expiration."""
    user_id = worker_user_id(BASE_USER_ID, worker_id)
    key = feed_key(user_id)

    # Clear and add data
    redis_client.delete(key)
    redis_client.zadd(key, {101: 0.95, 102: 0.87})

    # Set TTL (24 hours)
    redis_client.expire(key, FEED_TTL_SECONDS)

    # Verify TTL is set
    ttl = redis_client.ttl(key)
    assert ttl > 0, "TTL should be positive"
    assert ttl <= FEED_TTL_SECONDS, f"TTL {ttl} exceeds {FEED_TTL_SECONDS}"

    print(f"✅ Feed TTL set: {ttl}s (~{ttl / 3600:.1f}h)")


@pytest.mark.integration
def test_redis_feed_trimming(redis_client: redis.Redis, worker_id: str) -> None:
    """Test feed trimming to MAX_FEED_SIZE (Mastodon pattern)."""
    user_id = worker_user_id(BASE_USER_ID, worker_id)
    key = feed_key(user_id)

    redis_client.delete(key)

    # Add more posts than MAX_FEED_SIZE
    excess_count = 50
    total_posts = MAX_FEED_SIZE + excess_count

    for i in range(1, total_posts + 1):
        # Score = normalized value (higher post_id = higher score)
        score = i / total_posts
        redis_client.zadd(key, {i: score})

    # Verify all added
    count_before = redis_client.zcard(key)
    assert count_before == total_posts

    # Trim: keep only top MAX_FEED_SIZE by score
    # ZREMRANGEBYRANK removes items by rank (0-indexed)
    # Rank 0 = lowest score, rank -1 = highest score
    # We want to keep ranks from -(MAX_FEED_SIZE) to -1 (top MAX_FEED_SIZE)
    # So remove ranks from 0 to -(MAX_FEED_SIZE + 1)
    redis_client.zremrangebyrank(key, 0, -(MAX_FEED_SIZE + 1))

    # Verify trimmed
    count_after = redis_client.zcard(key)
    assert count_after == MAX_FEED_SIZE, (
        f"Expected {MAX_FEED_SIZE} posts after trim, got {count_after}"
    )

    # Verify we kept the highest-scored posts
    top_post = redis_client.zrevrange(key, 0, 0, withscores=True)[0]
    top_id = int(top_post[0])
    top_score = float(top_post[1])

    # Top post should be the last one added (highest score)
    assert top_id == total_posts
    assert abs(top_score - 1.0) < 0.01

    print(f"✅ Feed trimming: {count_before} → {count_after} posts (removed {excess_count})")
    print(f"   Top post retained: ID={top_id}, score={top_score:.3f}")


@pytest.mark.integration
def test_redis_feed_pipeline_performance(redis_client: redis.Redis, worker_id: str) -> None:
    """Test Redis pipelining for batch operations (Mastodon optimization)."""
    user_id = worker_user_id(BASE_USER_ID, worker_id)
    key = feed_key(user_id)

    redis_client.delete(key)

    # Generate large batch
    batch_size = 100
    recommendations = {i: i / batch_size for i in range(1, batch_size + 1)}

    # Method 1: Without pipeline (slow)
    start = time.time()
    for post_id, score in recommendations.items():
        redis_client.zadd(key, {post_id: score})
    no_pipeline_time = time.time() - start

    redis_client.delete(key)

    # Method 2: With pipeline (fast)
    start = time.time()
    pipe = redis_client.pipeline()
    for post_id, score in recommendations.items():
        pipe.zadd(key, {post_id: score})
    pipe.execute()
    pipeline_time = time.time() - start

    # Verify same result
    count = redis_client.zcard(key)
    assert count == batch_size

    speedup = no_pipeline_time / pipeline_time if pipeline_time > 0 else float("inf")

    print("✅ Pipeline performance:")
    print(f"   Without pipeline: {no_pipeline_time * 1000:.1f}ms")
    print(f"   With pipeline: {pipeline_time * 1000:.1f}ms")
    print(f"   Speedup: {speedup:.1f}x faster")

    # Pipeline should be faster (at least 2x for 100 items)
    assert pipeline_time < no_pipeline_time, "Pipeline should be faster"


@pytest.mark.integration
def test_mastodon_style_feed_trimming(redis_client: redis.Redis, worker_id: str) -> None:
    """Test Mastodon-style feed trimming pattern from feed_manager.rb.

    Keeps top MAX_FEED_SIZE posts, removes older ones.
    """
    user_id = worker_user_id(BASE_USER_ID, worker_id)
    key = feed_key(user_id)

    redis_client.delete(key)

    # Simulate feed growth beyond MAX_FEED_SIZE
    # In Mastodon: MAX_ITEMS = 800
    total_posts = 900
    mastodon_max = 800

    # Add posts with timestamps as scores (newer = higher score)
    for i in range(1, total_posts + 1):
        # Score represents post ID (monotonically increasing)
        redis_client.zadd(key, {i: float(i)})

    count_before = redis_client.zcard(key)
    assert count_before == total_posts

    # Mastodon trimming pattern (from feed_manager.rb:404)
    # Remove any items past the MAX_ITEMS'th entry
    # zremrangebyrank(key, 0, -(MAX_ITEMS + 1))
    redis_client.zremrangebyrank(key, 0, -(mastodon_max + 1))

    count_after = redis_client.zcard(key)
    assert count_after == mastodon_max

    # Verify we kept the newest posts (highest IDs)
    top_posts = redis_client.zrevrange(key, 0, 9)
    top_ids = [int(p) for p in top_posts]

    # Newest posts should be at top
    assert top_ids[0] == total_posts, f"Top post should be {total_posts}, got {top_ids[0]}"
    assert all(top_ids[i] > top_ids[i + 1] for i in range(len(top_ids) - 1)), "Should be descending"

    # Verify we removed oldest posts
    oldest_remaining = redis_client.zrange(key, 0, 0)[0]
    assert int(oldest_remaining) > 100, "Oldest posts should be removed"

    print(f"✅ Mastodon-style trimming: {count_before} → {count_after} posts")
    print(f"   Top post: {top_ids[0]}")
    print(f"   Oldest remaining: {int(oldest_remaining)}")
