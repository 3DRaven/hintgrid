"""Feed management tests.

Covers:
- Incremental feed updates
- Clear and repopulate
- Feed rebuild with idempotency
- WAS_RECOMMENDED tracking

All tests use worker-isolated labels via neo4j.label() for parallel execution.
"""


import pytest
import redis

from hintgrid.utils.coercion import coerce_float, coerce_int
from tests.conftest import EmbeddingServiceConfig
from tests.parallel import IsolatedNeo4jClient

from .conftest import (
    BASE_USER_ID,
    FEED_TTL_SECONDS,
    feed_key,
    worker_user_id,
)


@pytest.mark.integration
def test_redis_feed_incremental_update(redis_client: redis.Redis, worker_id: str) -> None:
    """Test incremental feed updates (add new recommendations without clearing old ones)."""
    user_id = worker_user_id(BASE_USER_ID, worker_id)
    key = feed_key(user_id)

    redis_client.delete(key)

    # Initial batch
    initial_posts = {101: 0.95, 102: 0.87, 103: 0.76}
    redis_client.zadd(key, initial_posts)

    initial_count = redis_client.zcard(key)
    assert initial_count == 3

    # New recommendations (some overlap, some new)
    new_posts = {103: 0.80, 104: 0.85, 105: 0.70}  # 103 updated score
    redis_client.zadd(key, new_posts)

    # Verify count (103 is updated, not duplicated)
    final_count = redis_client.zcard(key)
    assert final_count == 5, f"Expected 5 unique posts, got {final_count}"

    # Verify 103 has new score
    score_103 = redis_client.zscore(key, 103)
    assert score_103 is not None
    assert abs(coerce_float(score_103) - 0.80) < 0.01, (
        f"Post 103 should have updated score 0.80, got {score_103}"
    )

    # Get all posts sorted
    all_posts = redis_client.zrevrange(key, 0, -1, withscores=True)

    print(f"✅ Incremental update: {initial_count} → {final_count} posts")
    print("   Updated feed:")
    for post_id, score in all_posts:
        print(f"     Post {int(post_id)}: score={float(score):.2f}")


@pytest.mark.integration
def test_redis_feed_clear_and_repopulate(redis_client: redis.Redis, worker_id: str) -> None:
    """Test clearing old feed and repopulating with fresh recommendations."""
    user_id = worker_user_id(BASE_USER_ID, worker_id)
    key = feed_key(user_id)

    # Create old feed
    redis_client.delete(key)
    old_posts = {i: i / 10.0 for i in range(1, 11)}
    redis_client.zadd(key, old_posts)
    redis_client.expire(key, FEED_TTL_SECONDS)

    old_count = redis_client.zcard(key)
    assert old_count == 10

    # Clear feed (DELETE operation)
    redis_client.delete(key)

    # Verify cleared
    count_after_clear = redis_client.zcard(key)
    assert count_after_clear == 0

    # Repopulate with new recommendations
    new_posts = {i: i / 5.0 for i in range(11, 16)}
    redis_client.zadd(key, new_posts)
    redis_client.expire(key, FEED_TTL_SECONDS)

    new_count = redis_client.zcard(key)
    assert new_count == 5

    # Verify no overlap with old posts
    all_ids = [int(p) for p in redis_client.zrevrange(key, 0, -1)]
    assert all(post_id >= 11 for post_id in all_ids), "Should only have new posts"

    print(f"✅ Feed cleared and repopulated: {old_count} old → 0 → {new_count} new posts")


@pytest.mark.integration
def test_feed_rebuild_with_idempotency(
    isolated_neo4j: IsolatedNeo4jClient,
    redis_client: redis.Redis,
    fasttext_embedding_service: EmbeddingServiceConfig,
    worker_id: str,
) -> None:
    """Test full feed rebuild: clear old recommendations, recalculate, ensure idempotency.

    Demonstrates WAS_RECOMMENDED cleanup and BELONGS_TO idempotency.
    """
    from tests.examples.embeddings.conftest import generate_embeddings_via_service

    user_id = worker_user_id(88888, worker_id)
    neo4j = isolated_neo4j.client
    key = feed_key(user_id)


    # 1. Setup graph
    neo4j.execute_labeled(
        "CREATE (:__user__ {id: $id})",
        {"user": "User"},
        {"id": user_id},
    )
    neo4j.execute_labeled(
        "CREATE (:__uc__ {id: 1})",
        {"uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: $uid}), (uc:__uc__ {id: 1}) "
        "CREATE (u)-[:BELONGS_TO]->(uc)",
        {"user": "User", "uc": "UserCommunity"},
        {"uid": user_id},
    )

    # Create posts
    sample_posts = [
        {"id": 501, "text": "First post"},
        {"id": 502, "text": "Second post"},
        {"id": 503, "text": "Third post"},
    ]

    posts_with_embeddings = generate_embeddings_via_service(
        fasttext_embedding_service, sample_posts  # type: ignore[arg-type]
    )

    for post in posts_with_embeddings:
        neo4j.execute_labeled(
            "CREATE (p:__post__ {"
            "id: $id, "
            "text: $text, "
            "embedding: $emb, "
            "createdAt: datetime()"
            "})",
            {"post": "Post"},
            {"id": post["id"], "text": post["text"], "emb": post["embedding"]},
        )

    neo4j.execute_labeled(
        "CREATE (:__pc__ {id: 10})",
        {"pc": "PostCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (p:__post__), (pc:__pc__ {id: 10}) "
        "CREATE (p)-[:BELONGS_TO]->(pc)",
        {"post": "Post", "pc": "PostCommunity"},
    )

    neo4j.execute_labeled(
        "MATCH (uc:__uc__ {id: 1}), (pc:__pc__ {id: 10}) "
        "CREATE (uc)-[i:INTERESTED_IN]->(pc) "
        "SET i.score = 0.8, "
        "i.last_updated = datetime(), "
        "i.expires_at = datetime() + duration('P30D')",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )

    # 2. First feed generation
    feed_query = (
        "MATCH (u:__user__ {id: $user_id})-[:BELONGS_TO]->(uc:__uc__) "
        "MATCH (uc)-[i:INTERESTED_IN]->(pc:__pc__) "
        "MATCH (p:__post__)-[:BELONGS_TO]->(pc) "
        "RETURN p.id AS post_id, i.score AS score "
        "ORDER BY score DESC "
        "LIMIT 500"
    )

    recs_v1 = list(
        neo4j.execute_and_fetch_labeled(
            feed_query,
            {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity", "post": "Post"},
            {"user_id": user_id},
        )
    )

    # Store in Redis
    redis_client.delete(key)
    pipe = redis_client.pipeline()
    for rec in recs_v1:
        pipe.zadd(key, {coerce_int(rec["post_id"]): coerce_float(rec["score"])})
    pipe.expire(key, FEED_TTL_SECONDS)
    pipe.execute()

    # Mark as recommended
    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: $user_id}) "
        "UNWIND $post_ids AS post_id "
        "MATCH (p:__post__ {id: post_id}) "
        "MERGE (u)-[r:WAS_RECOMMENDED]->(p) "
        "ON CREATE SET r.at = datetime(), r.score = 0.8",
        {"user": "User", "post": "Post"},
        {"user_id": user_id, "post_ids": [coerce_int(r["post_id"]) for r in recs_v1]},
    )

    was_recommended_v1 = next(iter(neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__ {id: $uid})-[r:WAS_RECOMMENDED]->(p:__post__) "
            "RETURN count(p) AS count",
            {"user": "User", "post": "Post"},
            {"uid": user_id},
        )))["count"]

    print(f"✅ First generation: {was_recommended_v1} posts marked as WAS_RECOMMENDED")

    # 3. REBUILD: Clear old WAS_RECOMMENDED (idempotency)
    deleted = next(iter(neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__ {id: $uid})-[r:WAS_RECOMMENDED]->() "
            "DELETE r "
            "RETURN count(r) AS deleted",
            {"user": "User"},
            {"uid": user_id},
        )))["deleted"]

    assert deleted == was_recommended_v1

    print(f"✅ Cleanup: deleted {deleted} old WAS_RECOMMENDED")

    # 4. Regenerate feed (simulating score update)
    neo4j.execute_labeled(
        "MATCH (uc:__uc__ {id: 1})-[i:INTERESTED_IN]->(pc:__pc__ {id: 10}) "
        "SET i.score = 0.95",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )

    recs_v2 = list(
        neo4j.execute_and_fetch_labeled(
            feed_query,
            {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity", "post": "Post"},
            {"user_id": user_id},
        )
    )

    # Verify score updated
    assert recs_v2[0]["score"] == 0.95

    # Store new feed (overwrite)
    redis_client.delete(key)
    pipe = redis_client.pipeline()
    for rec in recs_v2:
        pipe.zadd(key, {coerce_int(rec["post_id"]): coerce_float(rec["score"])})
    pipe.expire(key, FEED_TTL_SECONDS)
    pipe.execute()

    # Mark as recommended again
    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: $user_id}) "
        "UNWIND $post_ids AS post_id "
        "MATCH (p:__post__ {id: post_id}) "
        "MERGE (u)-[r:WAS_RECOMMENDED]->(p) "
        "ON CREATE SET r.at = datetime(), r.score = 0.95",
        {"user": "User", "post": "Post"},
        {"user_id": user_id, "post_ids": [coerce_int(r["post_id"]) for r in recs_v2]},
    )

    was_recommended_v2 = next(iter(neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__ {id: $uid})-[r:WAS_RECOMMENDED]->(p:__post__) "
            "RETURN count(p) AS count",
            {"user": "User", "post": "Post"},
            {"uid": user_id},
        )))["count"]

    assert was_recommended_v2 == len(recs_v2)

    print(f"✅ Second generation: {was_recommended_v2} posts (idempotent)")
    print("✅ Feed rebuild completed successfully!")
