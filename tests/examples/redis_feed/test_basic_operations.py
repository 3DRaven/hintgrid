"""Basic Redis feed operations tests.

Covers:
- ZADD, ZREVRANGE, ZCARD operations
- Feed pagination
- Empty feed handling
- Score updates
- Multi-user isolation
"""

import pytest
import redis

from hintgrid.utils.coercion import coerce_float

from .conftest import BASE_USER_ID, BASE_USER_ID_2, feed_key, worker_user_id


@pytest.mark.integration
@pytest.mark.smoke
def test_redis_feed_basic_operations(redis_client: redis.Redis, worker_id: str) -> None:
    """Test basic Redis operations for feed management."""
    user_id = worker_user_id(BASE_USER_ID, worker_id)
    key = feed_key(user_id)

    # Clear any existing data
    redis_client.delete(key)

    # Add posts to feed with scores (post_id -> score mapping)
    # Score determines ordering (higher = more relevant)
    recommendations = [
        (101, 0.95),  # post_id=101, score=0.95
        (102, 0.87),
        (103, 0.76),
        (104, 0.65),
        (105, 0.54),
    ]

    # ZADD: Add to sorted set (score -> member)
    for post_id, score in recommendations:
        redis_client.zadd(key, {post_id: score})

    # Verify count
    count = redis_client.zcard(key)
    assert count == len(recommendations), f"Expected {len(recommendations)} posts, got {count}"

    # Get top 3 posts (highest scores first)
    top_posts = redis_client.zrevrange(key, 0, 2, withscores=True)
    assert len(top_posts) == 3
    assert int(top_posts[0][0]) == 101  # Highest score
    assert float(top_posts[0][1]) == 0.95

    # Get all posts with scores
    all_posts = redis_client.zrevrange(key, 0, -1, withscores=True)
    assert len(all_posts) == len(recommendations)

    # Verify ordering (descending by score)
    scores = [float(score) for _, score in all_posts]
    assert scores == sorted(scores, reverse=True), "Posts should be ordered by score DESC"

    print(f"✅ Basic feed operations: {count} posts stored and retrieved correctly")
    print(f"   Top post: ID={int(top_posts[0][0])}, score={float(top_posts[0][1]):.2f}")


@pytest.mark.integration
def test_redis_feed_multi_user(redis_client: redis.Redis, worker_id: str) -> None:
    """Test multiple users with separate feeds."""
    users = [
        worker_user_id(BASE_USER_ID, worker_id),
        worker_user_id(BASE_USER_ID_2, worker_id),
        worker_user_id(99999, worker_id),
    ]

    # Clear all feeds
    for user_id in users:
        redis_client.delete(feed_key(user_id))

    # Each user gets different recommendations
    for idx, user_id in enumerate(users):
        key = feed_key(user_id)
        # User 1 gets posts 1-5, User 2 gets posts 6-10, etc.
        start_id = idx * 5 + 1
        for i in range(5):
            post_id = start_id + i
            score = 1.0 - (i * 0.1)  # Decreasing scores
            redis_client.zadd(key, {post_id: score})

    # Verify isolation
    for idx, user_id in enumerate(users):
        key = feed_key(user_id)
        posts = redis_client.zrevrange(key, 0, -1)
        posts_ids = [int(p) for p in posts]

        expected_start = idx * 5 + 1
        expected_ids = list(range(expected_start, expected_start + 5))

        assert len(posts_ids) == 5, f"User {user_id} should have 5 posts"
        assert sorted(posts_ids) == sorted(expected_ids), (
            f"User {user_id} should have posts {expected_ids}, got {posts_ids}"
        )

    print(f"✅ Multi-user feeds: {len(users)} users with isolated feeds")
    for _idx, user_id in enumerate(users):
        key = feed_key(user_id)
        count = redis_client.zcard(key)
        print(f"   User {user_id}: {count} posts")


@pytest.mark.integration
def test_redis_feed_pagination(redis_client: redis.Redis, worker_id: str) -> None:
    """Test paginated feed retrieval (like Mastodon API: max_id, since_id)."""
    user_id = worker_user_id(BASE_USER_ID, worker_id)
    key = feed_key(user_id)

    redis_client.delete(key)

    # Create feed with 20 posts
    posts_count = 20
    for i in range(1, posts_count + 1):
        score = 1.0 - (i * 0.01)  # Decreasing scores
        redis_client.zadd(key, {i: score})

    # Page 1: First 10 posts (highest scores)
    page_1 = redis_client.zrevrange(key, 0, 9, withscores=True)
    assert len(page_1) == 10
    page_1_ids = [int(p) for p, _ in page_1]
    assert page_1_ids[0] == 1  # Highest score

    # Page 2: Next 10 posts
    page_2 = redis_client.zrevrange(key, 10, 19, withscores=True)
    assert len(page_2) == 10
    page_2_ids = [int(p) for p, _ in page_2]
    assert page_2_ids[0] == 11  # Next after page 1

    # Verify no overlap
    assert set(page_1_ids).isdisjoint(set(page_2_ids))

    # Get by score range (Mastodon max_id pattern)
    # Get posts with score < 0.95 (older posts)
    older_posts = redis_client.zrevrangebyscore(key, 0.95, 0.0, start=0, num=5)
    assert len(older_posts) <= 5

    print("✅ Pagination:")
    print(f"   Page 1: posts {page_1_ids[:3]}... (count: {len(page_1)})")
    print(f"   Page 2: posts {page_2_ids[:3]}... (count: {len(page_2)})")
    print(f"   By score < 0.95: {len(older_posts)} posts")


@pytest.mark.integration
def test_redis_feed_empty_handling(redis_client: redis.Redis, worker_id: str) -> None:
    """Test handling empty feeds."""
    user_id = worker_user_id(11111, worker_id)
    key = feed_key(user_id)

    redis_client.delete(key)

    # Check empty feed
    count = redis_client.zcard(key)
    assert count == 0

    # Try to get posts from empty feed
    posts = redis_client.zrevrange(key, 0, -1)
    assert len(posts) == 0
    assert posts == []

    # TTL on non-existent key
    ttl = redis_client.ttl(key)
    assert ttl == -2, "TTL should be -2 for non-existent key"

    print("✅ Empty feed handled correctly")


@pytest.mark.integration
def test_redis_feed_score_updates(redis_client: redis.Redis, worker_id: str) -> None:
    """Test updating scores for existing posts (like re-scoring recommendations)."""
    user_id = worker_user_id(BASE_USER_ID, worker_id)
    key = feed_key(user_id)

    redis_client.delete(key)

    # Initial recommendations
    redis_client.zadd(key, {101: 0.5, 102: 0.6, 103: 0.7})

    # Get initial scores
    score_101_old = redis_client.zscore(key, 101)
    assert score_101_old is not None
    assert abs(coerce_float(score_101_old) - 0.5) < 0.01

    # Update scores (ZADD with existing members updates scores)
    redis_client.zadd(key, {101: 0.95, 102: 0.85})  # Boost scores

    # Verify updates
    score_101_new = redis_client.zscore(key, 101)
    score_102_new = redis_client.zscore(key, 102)
    score_103_same = redis_client.zscore(key, 103)

    assert score_101_new is not None
    assert score_102_new is not None
    assert score_103_same is not None

    assert abs(coerce_float(score_101_new) - 0.95) < 0.01, "Score 101 should be updated"
    assert abs(coerce_float(score_102_new) - 0.85) < 0.01, "Score 102 should be updated"
    assert abs(coerce_float(score_103_same) - 0.7) < 0.01, "Score 103 should remain unchanged"

    # Verify count unchanged (no duplicates)
    count = redis_client.zcard(key)
    assert count == 3

    # Verify new ordering
    top_post = redis_client.zrevrange(key, 0, 0, withscores=True)[0]
    assert int(top_post[0]) == 101, "Post 101 should be top after score update"

    print("✅ Score updates:")
    print(f"   Post 101: {score_101_old:.2f} → {score_101_new:.2f}")
    print(f"   Post 102: 0.60 → {score_102_new:.2f}")
    print(f"   Post 103: {score_103_same:.2f} (unchanged)")


@pytest.mark.integration
def test_multiple_feed_types(redis_client: redis.Redis, worker_id: str) -> None:
    """Test different feed types like in Mastodon: home, list, mentions, tags.

    Each has separate Redis keys.
    """
    user_id = worker_user_id(BASE_USER_ID, worker_id)

    # Different feed types (Mastodon pattern)
    feeds = {
        f"feed:home:{user_id}": [101, 102, 103],
        f"feed:list:{user_id}:1": [201, 202],  # List feed
        f"feed:mentions:{user_id}": [301],  # Mentions
        f"feed:tags:{user_id}:python": [401, 402],  # Tag feed
    }

    # Clear and populate
    for key, post_ids in feeds.items():
        redis_client.delete(key)
        for idx, post_id in enumerate(post_ids):
            score = 1.0 - (idx * 0.1)
            redis_client.zadd(key, {post_id: score})
        redis_client.expire(key, 86400)

    # Verify isolation
    for key, expected_ids in feeds.items():
        actual_ids = [int(p) for p in redis_client.zrevrange(key, 0, -1)]
        assert sorted(actual_ids) == sorted(expected_ids), f"Feed {key} mismatch"

    print(f"✅ Multiple feed types: {len(feeds)} feeds with isolation")
    for key, _post_ids in feeds.items():
        count = redis_client.zcard(key)
        print(f"   {key}: {count} posts")
