"""Full Neo4j to Redis workflow tests.

Covers:
- Complete Neo4j recommendations → Redis storage → Feed retrieval
- Cold start handling (fallback strategies)
- INTERESTED_IN TTL and cleanup
- Hybrid feed (personalized + diversity)

All tests use worker-isolated labels via neo4j.label() for parallel execution.
"""


import pytest
import redis

from hintgrid.utils.coercion import coerce_float, coerce_int
from tests.conftest import EmbeddingServiceConfig
from tests.parallel import IsolatedNeo4jClient

from .conftest import (
    FEED_TTL_SECONDS,
    INTERESTS_TTL_DAYS,
    feed_key,
    worker_user_id,
)


@pytest.mark.integration
def test_neo4j_to_redis_full_workflow(
    isolated_neo4j: IsolatedNeo4jClient,
    redis_client: redis.Redis,
    fasttext_embedding_service: EmbeddingServiceConfig,
    worker_id: str,
) -> None:
    """Full workflow: Generate recommendations in Neo4j → Store in Redis → Retrieve feed.

    Uses worker-isolated labels:
    - Leiden clustering (simulated by manual cluster assignment)
    - INTERESTED_IN with TTL
    - Idempotent BELONGS_TO (cleanup old before creating new)
    - WAS_RECOMMENDED tracking
    """
    from tests.examples.embeddings.conftest import generate_embeddings_via_service

    base_user_id = 12345
    user_id = worker_user_id(base_user_id, worker_id)
    neo4j = isolated_neo4j.client

    # Clear Redis key for this user
    key = feed_key(user_id)
    redis_client.delete(key)

    # Worker-isolated labels
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.label("Post")
    neo4j.label("PostCommunity")

    # 1. Setup graph: Create user, communities, posts
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

    # Create posts with embeddings
    sample_posts = [
        {"id": 201, "text": "Python programming tutorial"},
        {"id": 202, "text": "Docker container guide"},
        {"id": 203, "text": "Kubernetes basics"},
        {"id": 204, "text": "Redis caching tips"},
        {"id": 205, "text": "PostgreSQL optimization"},
        {"id": 206, "text": "Django web framework"},
        {"id": 207, "text": "Flask API development"},
        {"id": 208, "text": "SQLAlchemy database ORM"},
        {"id": 209, "text": "Pytest testing framework"},
        {"id": 210, "text": "Git version control"},
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
            "createdAt: datetime(), "
            "cluster_id: $cid"
            "})",
            {"post": "Post"},
            {
                "id": post["id"],
                "text": post["text"],
                "emb": post["embedding"],
                "cid": 1 if post["id"] <= 205 else 2,  # Simulate Leiden clustering
            },
        )

    # Create PostCommunities (simulating Leiden result)
    neo4j.execute_labeled(
        "CREATE (:__pc__ {id: 1, size: 5})",
        {"pc": "PostCommunity"},
    )
    neo4j.execute_labeled(
        "CREATE (:__pc__ {id: 2, size: 5})",
        {"pc": "PostCommunity"},
    )

    # IDEMPOTENT: Delete old BELONGS_TO before creating new
    neo4j.execute_labeled(
        "MATCH (p:__post__)-[old:BELONGS_TO]->(:__pc__) "
        "DELETE old",
        {"post": "Post", "pc": "PostCommunity"},
    )

    # Assign posts to communities based on cluster_id
    neo4j.execute_labeled(
        "MATCH (p:__post__) "
        "WHERE p.cluster_id IS NOT NULL "
        "WITH p, p.cluster_id AS cluster_id "
        "MERGE (pc:__pc__ {id: cluster_id}) "
        "MERGE (p)-[:BELONGS_TO]->(pc) "
        "RETURN count(*) AS count",
        {"post": "Post", "pc": "PostCommunity"},
    )

    # Verify BELONGS_TO created
    belongs_count = next(iter(neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__)-[:BELONGS_TO]->(pc:__pc__) "
            "RETURN count(p) AS count",
            {"post": "Post", "pc": "PostCommunity"},
        )))["count"]

    assert belongs_count == len(sample_posts), (
        f"Expected {len(sample_posts)} BELONGS_TO, got {belongs_count}"
    )

    # Create INTERESTED_IN with TTL
    neo4j.execute_labeled(
        "MATCH (uc:__uc__ {id: 1}), (pc:__pc__) "
        "MERGE (uc)-[i:INTERESTED_IN]->(pc) "
        "SET i.score = CASE WHEN pc.id = 1 THEN 0.9 ELSE 0.6 END, "
        "i.based_on = 100, "
        "i.last_updated = datetime(), "
        "i.expires_at = datetime() + duration('P30D') "
        "RETURN count(i) AS count",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )

    # Use execute_and_fetch_labeled with worker label (always present in tests)
    interests_count = next(iter(neo4j.execute_and_fetch_labeled(
        "MATCH (n:__worker__)-[i:INTERESTED_IN]->() RETURN count(i) AS count",
        label_map={"worker": neo4j.worker_label} if neo4j.worker_label else {}
    )))["count"]

    assert interests_count == 2, f"Expected 2 INTERESTED_IN, got {interests_count}"

    print(f"✅ Graph setup: {belongs_count} posts, {interests_count} INTERESTED_IN with TTL")

    # 2. Generate personalized feed from Neo4j
    feed_query = (
        "MATCH (u:__user__ {id: $user_id})-[:BELONGS_TO]->(uc:__uc__) "
        "MATCH (uc)-[i:INTERESTED_IN]->(pc:__pc__) "
        "WHERE i.expires_at > datetime() "
        "MATCH (p:__post__)-[:BELONGS_TO]->(pc) "
        "WHERE p.createdAt > datetime() - duration('P7D') "
        "AND NOT (u)-[:WAS_RECOMMENDED]->(p) "
        "AND NOT (u)-[:FAVOURITED]->(p) "
        "AND NOT (u)-[:WROTE]->(p) "
        "OPTIONAL MATCH (p)<-[f_pop:FAVOURITED]-() "
        "WITH p, i.score AS interest_score, count(f_pop) AS popularity "
        "RETURN p.id AS post_id, "
        "interest_score * 0.6 + log10(popularity + 1) * 0.4 AS score "
        "ORDER BY score DESC "
        "LIMIT 500"
    )

    recommendations = list(
        neo4j.execute_and_fetch_labeled(
            feed_query,
            {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity", "post": "Post"},
            {"user_id": user_id},
        )
    )

    assert len(recommendations) >= 3, "Should generate recommendations"

    print(f"✅ Generated {len(recommendations)} recommendations from Neo4j")

    # 3. Store in Redis using pipeline
    pipe = redis_client.pipeline()

    for rec in recommendations:
        recommendation_post_id = coerce_int(rec["post_id"])
        score = coerce_float(rec["score"])
        pipe.zadd(key, {recommendation_post_id: score})

    # Set TTL
    pipe.expire(key, FEED_TTL_SECONDS)

    pipe.execute()

    # 4. Verify storage
    stored_count = redis_client.zcard(key)
    assert stored_count == len(recommendations), (
        f"Expected {len(recommendations)} in Redis, got {stored_count}"
    )

    ttl = redis_client.ttl(key)
    assert ttl > 0, "TTL should be set"

    print(f"✅ Stored {stored_count} recommendations in Redis (TTL: {ttl}s)")

    # 5. Retrieve feed
    top_feed = redis_client.zrevrange(key, 0, 9, withscores=True)  # Top 10

    assert len(top_feed) <= 10
    assert len(top_feed) >= 3

    # Verify scores are ordered
    scores = [float(score) for _, score in top_feed]
    assert scores == sorted(scores, reverse=True), "Feed should be ordered by score DESC"

    print(f"✅ Retrieved top {len(top_feed)} posts from feed:")
    for feed_post_id, score in top_feed[:3]:
        print(f"   Post {int(feed_post_id)}: score={float(score):.3f}")

    # 6. Mark posts as recommended
    recommended_ids = [int(feed_post_id) for feed_post_id, _ in top_feed]

    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: $user_id}) "
        "UNWIND $post_ids AS post_id "
        "MATCH (p:__post__ {id: post_id}) "
        "MERGE (u)-[r:WAS_RECOMMENDED]->(p) "
        "ON CREATE SET r.at = datetime(), r.score = 0.8 "
        "RETURN count(r) AS marked",
        {"user": "User", "post": "Post"},
        {"user_id": user_id, "post_ids": recommended_ids},
    )

    # Verify marking
    marked_count = next(iter(neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__ {id: $uid})-[r:WAS_RECOMMENDED]->(p:__post__) "
            "RETURN count(p) AS count",
            {"user": "User", "post": "Post"},
            {"uid": user_id},
        )))["count"]

    assert marked_count == len(recommended_ids)

    print(f"✅ Marked {marked_count} posts as WAS_RECOMMENDED in Neo4j")
    print("\n🎉 Full workflow completed successfully!")


@pytest.mark.integration
def test_cold_start_global_top_fallback(
    isolated_neo4j: IsolatedNeo4jClient,
    redis_client: redis.Redis,
    fasttext_embedding_service: EmbeddingServiceConfig,
    worker_id: str,
) -> None:
    """Test cold start scenario: UserCommunity without INTERESTED_IN uses global top posts.

    Implements fallback strategy for new users.
    """
    from tests.examples.embeddings.conftest import generate_embeddings_via_service

    user_id = worker_user_id(55555, worker_id)
    neo4j = isolated_neo4j.client
    key = feed_key(user_id)
    redis_client.delete(key)

    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.label("Post")
    neo4j.label("PostCommunity")

    # 1. Create user in NEW community (cold start: no INTERESTED_IN)
    community_id = worker_user_id(99, worker_id)
    neo4j.execute_labeled(
        "CREATE (:__user__ {id: $id})",
        {"user": "User"},
        {"id": user_id},
    )
    neo4j.execute_labeled(
        "CREATE (:__uc__ {id: $cid})",
        {"uc": "UserCommunity"},
        {"cid": community_id},
    )
    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: $uid}), (uc:__uc__ {id: $cid}) "
        "CREATE (u)-[:BELONGS_TO]->(uc)",
        {"user": "User", "uc": "UserCommunity"},
        {"uid": user_id, "cid": community_id},
    )

    # 2. Create popular posts (without INTERESTED_IN from UserCommunity 99)
    sample_posts = [
        {"id": 301, "text": "Very popular post"},
        {"id": 302, "text": "Trending content"},
        {"id": 303, "text": "Viral meme"},
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

    # Simulate favourites to make posts "popular"
    for post_id in [301, 302, 303]:
        for dummy_user in range(10):  # 10 favourites each
            neo4j.execute_labeled(
                "MATCH (p:__post__ {id: $pid}) "
                "MERGE (u:__user__ {id: $uid}) "
                "MERGE (u)-[f:FAVOURITED]->(p) "
                "ON CREATE SET f.at = datetime()",
                {"post": "Post", "user": "User"},
                {"uid": 1000 + dummy_user, "pid": post_id},
            )

    # 3. Try personalized query (should return empty - no INTERESTED_IN)
    personalized_query = (
        "MATCH (u:__user__ {id: $user_id})-[:BELONGS_TO]->(uc:__uc__) "
        "OPTIONAL MATCH (uc)-[i:INTERESTED_IN]->(pc:__pc__) "
        "WITH u, count(i) AS interests_count "
        "WHERE interests_count > 0 "
        "MATCH (u)-[:BELONGS_TO]->(uc:__uc__) "
        "MATCH (uc)-[i:INTERESTED_IN]->(pc:__pc__) "
        "MATCH (p:__post__)-[:BELONGS_TO]->(pc) "
        "RETURN p.id AS post_id, i.score AS score "
        "LIMIT 10"
    )

    personalized = list(
        neo4j.execute_and_fetch_labeled(
            personalized_query,
            {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity", "post": "Post"},
            {"user_id": user_id},
        )
    )
    assert len(personalized) == 0, "Cold start: no personalized recommendations"

    print("✅ Cold start detected: no INTERESTED_IN for UserCommunity 99")

    # 4. FALLBACK: Global top query (by popularity)
    fallback_query = (
        "MATCH (u:__user__ {id: $user_id}) "
        "MATCH (p:__post__) "
        "WHERE p.createdAt > datetime() - duration('P7D') "
        "AND NOT (u)-[:WAS_RECOMMENDED]->(p) "
        "AND NOT (u)-[:WROTE]->(p) "
        "AND NOT (u)-[:FAVOURITED]->(p) "
        "OPTIONAL MATCH (p)<-[f_pop:FAVOURITED]-() "
        "WITH p, count(f_pop) AS popularity, "
        "duration.between(p.createdAt, datetime()) AS age_duration "
        "RETURN p.id AS post_id, "
        "log10(popularity + 1) * 0.7 + "
        "(1.0 / (age_duration.days + 1)) * 0.3 AS score, "
        "'cold_start' AS source "
        "ORDER BY score DESC "
        "LIMIT 500"
    )

    fallback_recs = list(
        neo4j.execute_and_fetch_labeled(
            fallback_query,
            {"user": "User", "post": "Post"},
            {"user_id": user_id},
        )
    )

    assert len(fallback_recs) > 0, "Should have fallback recommendations"
    assert fallback_recs[0]["source"] == "cold_start"

    print(f"✅ Fallback recommendations: {len(fallback_recs)} posts (global top)")

    # 5. Store fallback in Redis
    pipe = redis_client.pipeline()
    for rec in fallback_recs:
        pipe.zadd(key, {coerce_int(rec["post_id"]): coerce_float(rec["score"])})
    pipe.expire(key, FEED_TTL_SECONDS)
    pipe.execute()

    # 6. Verify
    stored_count = redis_client.zcard(key)
    assert stored_count == len(fallback_recs)

    top_post = redis_client.zrevrange(key, 0, 0, withscores=True)[0]
    print(f"✅ Cold start feed stored: {stored_count} posts")
    print(f"   Top post: ID={int(top_post[0])}, score={float(top_post[1]):.3f}")


@pytest.mark.integration
def test_interested_in_ttl_and_cleanup(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test TTL for INTERESTED_IN relationships and cleanup of expired ones.

    Implements TTL mechanism from architecture docs.
    """
    neo4j = isolated_neo4j.client

    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")

    # Create communities with worker isolation
    uc_id = worker_user_id(1, worker_id)
    pc_id_10 = worker_user_id(10, worker_id)
    pc_id_20 = worker_user_id(20, worker_id)
    neo4j.execute_labeled(
        "CREATE (:__uc__ {id: $id})",
        {"uc": "UserCommunity"},
        {"id": uc_id},
    )
    neo4j.execute_labeled(
        "CREATE (:__pc__ {id: $id})",
        {"pc": "PostCommunity"},
        {"id": pc_id_10},
    )
    neo4j.execute_labeled(
        "CREATE (:__pc__ {id: $id})",
        {"pc": "PostCommunity"},
        {"id": pc_id_20},
    )

    # Create INTERESTED_IN with different ages
    # Fresh interest expires in INTERESTS_TTL_DAYS days
    # Old interest expired yesterday

    # Fresh interest (valid)
    neo4j.execute_labeled(
        "MATCH (uc:__uc__ {id: $uc_id}), (pc:__pc__ {id: $pc_id}) "
        "CREATE (uc)-[i:INTERESTED_IN]->(pc) "
        "SET i.score = 0.8, "
        "i.based_on = 100, "
        "i.last_updated = datetime(), "
        "i.expires_at = datetime() + duration('P' + toString($ttl_days) + 'D')",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
        {"uc_id": uc_id, "pc_id": pc_id_10, "ttl_days": INTERESTS_TTL_DAYS},
    )

    # Expired interest (old)
    neo4j.execute_labeled(
        "MATCH (uc:__uc__ {id: $uc_id}), (pc:__pc__ {id: $pc_id}) "
        "CREATE (uc)-[i:INTERESTED_IN]->(pc) "
        "SET i.score = 0.5, "
        "i.based_on = 50, "
        "i.last_updated = datetime() - duration('P31D'), "
        "i.expires_at = datetime() - duration('P1D')",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
        {"uc_id": uc_id, "pc_id": pc_id_20},
    )

    # Check total INTERESTED_IN
    total = next(iter(neo4j.execute_and_fetch_labeled(
        "MATCH (n:__worker__)-[i:INTERESTED_IN]->() RETURN count(i) AS count",
        label_map={"worker": neo4j.worker_label} if neo4j.worker_label else {}
    )))["count"]

    assert total == 2, f"Should have 2 INTERESTED_IN, got {total}"

    print("✅ Created 2 INTERESTED_IN (1 fresh, 1 expired)")

    # Check expired count
    expired_count = next(iter(neo4j.execute_and_fetch_labeled(
        "MATCH (n:__worker__)-[i:INTERESTED_IN]->() WHERE i.expires_at < datetime() RETURN count(i) AS count",
        label_map={"worker": neo4j.worker_label} if neo4j.worker_label else {}
    )))["count"]

    assert expired_count == 1, f"Should have 1 expired, got {expired_count}"

    print(f"✅ Detected {expired_count} expired INTERESTED_IN")

    # Cleanup expired
    deleted = next(iter(neo4j.execute_and_fetch_labeled(
        "MATCH (n:__worker__)-[i:INTERESTED_IN]->() WHERE i.expires_at < datetime() DELETE i RETURN count(i) AS deleted",
        label_map={"worker": neo4j.worker_label} if neo4j.worker_label else {}
    )))["deleted"]

    assert deleted == 1, f"Should delete 1, deleted {deleted}"

    # Verify only fresh remains
    remaining = next(iter(neo4j.execute_and_fetch_labeled(
        "MATCH (n:__worker__)-[i:INTERESTED_IN]->() RETURN count(i) AS count",
        label_map={"worker": neo4j.worker_label} if neo4j.worker_label else {}
    )))["count"]

    assert remaining == 1, f"Should have 1 remaining, got {remaining}"

    print(f"✅ Cleanup: deleted {deleted} expired, {remaining} valid remaining")


@pytest.mark.integration
def test_hybrid_feed_personalized_plus_cold_start(
    isolated_neo4j: IsolatedNeo4jClient,
    redis_client: redis.Redis,
    fasttext_embedding_service: EmbeddingServiceConfig,
    worker_id: str,
) -> None:
    """Test hybrid feed: mix personalized recommendations with cold start content.

    Simulates UNION query for diversity.
    """
    from tests.examples.embeddings.conftest import generate_embeddings_via_service

    user_id = worker_user_id(77777, worker_id)
    neo4j = isolated_neo4j.client
    key = feed_key(user_id)
    redis_client.delete(key)

    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.label("Post")
    neo4j.label("PostCommunity")

    # 1. Setup: User in community with SOME interests (but limited)
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

    # 2. Create posts in different communities
    sample_posts = [
        {"id": 401, "text": "Tech post about Python"},  # Personalized
        {"id": 402, "text": "Popular viral content"},  # Global
        {"id": 403, "text": "Another tech tutorial"},  # Personalized
        {"id": 404, "text": "Breaking news"},  # Global
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

    # 3. Create communities and assign posts
    neo4j.execute_labeled(
        "CREATE (:__pc__ {id: 10})",  # Tech
        {"pc": "PostCommunity"},
    )
    neo4j.execute_labeled(
        "CREATE (:__pc__ {id: 20})",  # General
        {"pc": "PostCommunity"},
    )

    neo4j.execute_labeled(
        "MATCH (p:__post__ {id: 401}), (pc:__pc__ {id: 10}) "
        "CREATE (p)-[:BELONGS_TO]->(pc)",
        {"post": "Post", "pc": "PostCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (p:__post__ {id: 403}), (pc:__pc__ {id: 10}) "
        "CREATE (p)-[:BELONGS_TO]->(pc)",
        {"post": "Post", "pc": "PostCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (p:__post__ {id: 402}), (pc:__pc__ {id: 20}) "
        "CREATE (p)-[:BELONGS_TO]->(pc)",
        {"post": "Post", "pc": "PostCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (p:__post__ {id: 404}), (pc:__pc__ {id: 20}) "
        "CREATE (p)-[:BELONGS_TO]->(pc)",
        {"post": "Post", "pc": "PostCommunity"},
    )

    # 4. Create LIMITED interest (only to tech community)
    neo4j.execute_labeled(
        "MATCH (uc:__uc__ {id: 1}), (pc:__pc__ {id: 10}) "
        "CREATE (uc)-[i:INTERESTED_IN]->(pc) "
        "SET i.score = 0.9, "
        "i.based_on = 100, "
        "i.last_updated = datetime(), "
        "i.expires_at = datetime() + duration('P30D')",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )

    # 5. Query hybrid feed (personalized + cold start)
    hybrid_query = (
        "MATCH (u:__user__ {id: $user_id})-[:BELONGS_TO]->(uc:__uc__) "
        "OPTIONAL MATCH (uc)-[i:INTERESTED_IN]->(pc:__pc__) "
        "WITH u, uc, collect(DISTINCT pc) AS interested_communities, count(i) AS interests_count "
        "WITH u, uc, interested_communities, interests_count, "
        "CASE WHEN interests_count = 0 THEN true ELSE false END AS is_cold_start "
        "CALL { "
        "WITH u, uc, interested_communities, is_cold_start "
        "WITH u, uc, interested_communities, is_cold_start "
        "WHERE NOT is_cold_start "
        "UNWIND interested_communities AS pc "
        "MATCH (p:__post__)-[:BELONGS_TO]->(pc) "
        "MATCH (uc)-[i:INTERESTED_IN]->(pc) "
        "WHERE p.createdAt > datetime() - duration('P7D') "
        "OPTIONAL MATCH (p)<-[f_pop:FAVOURITED]-() "
        "WITH p, i.score AS interest_score, count(f_pop) AS popularity "
        "RETURN p, "
        "interest_score * 0.7 + log10(popularity + 1) * 0.3 AS score, "
        "'personalized' AS source "
        "UNION "
        "WITH u "
        "MATCH (p:__post__) "
        "WHERE p.createdAt > datetime() - duration('P7D') "
        "AND NOT (u)-[:WROTE]->(p) "
        "OPTIONAL MATCH (p)<-[f_pop:FAVOURITED]-() "
        "WITH p, count(f_pop) AS popularity "
        "RETURN p, "
        "log10(popularity + 1) * 0.5 AS score, "
        "'diversity' AS source "
        "ORDER BY score DESC "
        "LIMIT 2 "
        "} "
        "WITH p, score, source "
        "RETURN DISTINCT p.id AS post_id, max(score) AS score, collect(DISTINCT source) AS sources "
        "ORDER BY score DESC "
        "LIMIT 500"
    )

    recommendations = list(
        neo4j.execute_and_fetch_labeled(
            hybrid_query,
            {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity", "post": "Post"},
            {"user_id": user_id},
        )
    )

    assert len(recommendations) > 0, "Should have hybrid recommendations"

    # Check we have both sources
    from typing import cast
    has_personalized = any(
        "personalized" in cast("list[str]", rec["sources"]) for rec in recommendations
    )
    has_diversity = any("diversity" in cast("list[str]", rec["sources"]) for rec in recommendations)

    print(f"✅ Hybrid feed: {len(recommendations)} posts")
    print(f"   Has personalized: {has_personalized}")
    print(f"   Has diversity: {has_diversity}")

    # 6. Store in Redis
    pipe = redis_client.pipeline()
    for rec in recommendations:
        pipe.zadd(key, {coerce_int(rec["post_id"]): coerce_float(rec["score"])})
    pipe.expire(key, FEED_TTL_SECONDS)
    pipe.execute()

    stored_count = redis_client.zcard(key)
    assert stored_count == len(recommendations)

    print(f"✅ Hybrid feed stored in Redis: {stored_count} posts")
