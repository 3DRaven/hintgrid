"""Integration tests for public timeline feed generation.

Covers:
- generate_public_feed with local_communities strategy
- generate_public_feed with all_communities strategy
- generate_public_feed with local_only_authors filter
- write_public_feed_to_redis with rank-based scoring
- namespaced_key helper
"""

from __future__ import annotations


import pytest

from hintgrid.clients.redis import RedisClient
from hintgrid.config import HintGridSettings
from hintgrid.pipeline.feed import (
    generate_public_feed,
    namespaced_key,
    write_public_feed_to_redis,
)
from hintgrid.utils.coercion import coerce_float
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    import redis


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_public_graph(neo4j: Neo4jClient) -> None:
    """Create a graph for public feed tests.

    Creates:
    - 2 user communities (uc1 with local user, uc2 with remote user)
    - 2 post communities
    - INTERESTED_IN relationships
    - Posts in communities (some by local authors, some by remote)
    """
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")
    neo4j.label("Post")

    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 60001, isLocal: true}) "
        "CREATE (u2:__user__ {id: 60002, isLocal: false}) "
        "CREATE (uc1:__uc__ {id: 'pub_uc_1'}) "
        "CREATE (uc2:__uc__ {id: 'pub_uc_2'}) "
        "CREATE (pc1:__pc__ {id: 'pub_pc_1'}) "
        "CREATE (pc2:__pc__ {id: 'pub_pc_2'}) "
        "CREATE (u1)-[:BELONGS_TO]->(uc1) "
        "CREATE (u2)-[:BELONGS_TO]->(uc2) "
        "CREATE (uc1)-[:INTERESTED_IN {score: 0.9}]->(pc1) "
        "CREATE (uc2)-[:INTERESTED_IN {score: 0.7}]->(pc2) "
        "CREATE (p1:__post__ {id: 60101, createdAt: datetime() - duration({hours: 1})})-[:BELONGS_TO]->(pc1) "
        "CREATE (p2:__post__ {id: 60102, createdAt: datetime() - duration({hours: 2})})-[:BELONGS_TO]->(pc2) "
        "CREATE (p3:__post__ {id: 60103, createdAt: datetime() - duration({hours: 3})})-[:BELONGS_TO]->(pc1) "
        "CREATE (u1)-[:WROTE]->(p1) "
        "CREATE (u2)-[:WROTE]->(p2) "
        "CREATE (u1)-[:WROTE]->(p3)",
        {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity", "post": "Post"},
    )


# ---------------------------------------------------------------------------
# Tests: namespaced_key
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_namespaced_key_with_namespace() -> None:
    """namespaced_key prepends namespace when configured."""
    settings = HintGridSettings(redis_namespace="cache")
    assert namespaced_key("timeline:public", settings) == "cache:timeline:public"
    assert namespaced_key("feed:home:123", settings) == "cache:feed:home:123"


@pytest.mark.integration
def test_namespaced_key_without_namespace() -> None:
    """namespaced_key returns key as-is when namespace is None."""
    settings = HintGridSettings(redis_namespace=None)
    assert namespaced_key("timeline:public", settings) == "timeline:public"


@pytest.mark.integration
def test_namespaced_key_empty_string_namespace() -> None:
    """namespaced_key with empty string namespace returns key as-is."""
    settings = HintGridSettings(redis_namespace="")
    assert namespaced_key("timeline:public", settings) == "timeline:public"


# ---------------------------------------------------------------------------
# Tests: generate_public_feed with local_communities strategy
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_generate_public_feed_local_communities_strategy(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """local_only_interests=True uses only communities with local users.

    Posts in pc2 (only remote user community) should not appear because
    the query follows local user → user community → INTERESTED_IN.
    """
    _setup_public_graph(neo4j)

    test_settings = HintGridSettings(
        feed_pc_share_weight=1.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        public_feed_size=100,
        feed_days=7,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_public_feed(
        neo4j, test_settings, local_only_interests=True, local_only_authors=False
    )

    post_ids = {int(r["post_id"]) for r in recs}

    # pc1 posts should appear (local user uc1 → INTERESTED_IN → pc1)
    assert 60101 in post_ids, "Post 60101 (pc1) should appear"
    assert 60103 in post_ids, "Post 60103 (pc1) should appear"

    # pc2 posts should NOT appear (only remote user uc2 → INTERESTED_IN → pc2)
    assert 60102 not in post_ids, (
        "Post 60102 (pc2, remote community only) should not appear "
        "in local_communities strategy"
    )


@pytest.mark.integration
def test_generate_public_feed_all_communities_strategy(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """local_only_interests=False uses ALL community interests.

    Posts from both pc1 and pc2 should appear.
    """
    _setup_public_graph(neo4j)

    test_settings = HintGridSettings(
        feed_pc_share_weight=1.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        public_feed_size=100,
        feed_days=7,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_public_feed(
        neo4j, test_settings, local_only_interests=False, local_only_authors=False
    )

    post_ids = {int(r["post_id"]) for r in recs}

    # All posts should appear
    assert 60101 in post_ids
    assert 60102 in post_ids
    assert 60103 in post_ids


@pytest.mark.integration
def test_generate_public_feed_local_only_authors(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """local_only_authors=True filters to posts written by local users only.

    Post 60102 is by remote user u2 — should be excluded.
    """
    _setup_public_graph(neo4j)

    test_settings = HintGridSettings(
        feed_pc_share_weight=1.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        public_feed_size=100,
        feed_days=7,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_public_feed(
        neo4j,
        test_settings,
        local_only_interests=False,
        local_only_authors=True,
    )

    post_ids = {int(r["post_id"]) for r in recs}

    # Only posts by local author u1
    assert 60101 in post_ids
    assert 60103 in post_ids
    assert 60102 not in post_ids, (
        "Post 60102 (by remote user) should be excluded with local_only_authors"
    )


@pytest.mark.integration
def test_generate_public_feed_returns_empty_when_no_data(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """generate_public_feed returns empty list with no graph data."""
    test_settings = HintGridSettings(
        public_feed_size=100,
        feed_days=7,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_public_feed(neo4j, test_settings)
    assert recs == []


@pytest.mark.integration
def test_generate_public_feed_scores_are_sorted_desc(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """generate_public_feed returns recommendations sorted by score DESC."""
    _setup_public_graph(neo4j)

    test_settings = HintGridSettings(
        feed_pc_share_weight=1.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=1.0,
        public_feed_size=100,
        feed_days=7,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_public_feed(
        neo4j, test_settings, local_only_interests=False, local_only_authors=False
    )

    if len(recs) >= 2:
        scores = [r["score"] for r in recs]
        for i in range(len(scores) - 1):
            assert scores[i] >= scores[i + 1], (
                f"Scores should be sorted DESC: {scores}"
            )


# ---------------------------------------------------------------------------
# Tests: write_public_feed_to_redis
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_write_public_feed_to_redis_rank_based_scoring(
    redis_client: redis.Redis,
) -> None:
    """write_public_feed_to_redis stores posts with rank-based scores.

    Most interesting post (rank 0) gets highest Redis score.
    All scores > max(post_id) to outrank Mastodon entries.
    """
    key = "timeline:public:test"

    recs: list[dict[str, float]] = [
        {"post_id": 1000, "score": 0.95},  # rank 0 — most interesting
        {"post_id": 2000, "score": 0.80},  # rank 1
        {"post_id": 3000, "score": 0.65},  # rank 2 — least interesting
    ]

    test_settings = HintGridSettings(
        feed_score_multiplier=10,
        redis_namespace=None,
    )

    redis_wrapper = RedisClient(redis_client)
    write_public_feed_to_redis(redis_wrapper, key, recs, test_settings)

    # Check scores are in Redis
    entries = redis_client.zrevrange(key, 0, -1, withscores=True)
    assert len(entries) == 3

    score_map = {int(member): score for member, score in entries}

    # All scores should be > max_post_id (3000)
    for post_id, redis_score in score_map.items():
        assert redis_score > 3000, (
            f"Post {post_id} score {redis_score} should be > max_post_id 3000"
        )

    # Most interesting (1000) should have highest Redis score
    assert score_map[1000] > score_map[2000] > score_map[3000], (
        f"Rank order should be preserved: {score_map}"
    )


@pytest.mark.integration
def test_write_public_feed_to_redis_with_namespace(
    redis_client: redis.Redis,
) -> None:
    """write_public_feed_to_redis respects redis_namespace.

    The function calls namespaced_key internally, so the raw key
    gets prefixed with the namespace.
    """
    recs: list[dict[str, float]] = [
        {"post_id": 5000, "score": 0.90},
    ]

    test_settings = HintGridSettings(
        feed_score_multiplier=10,
        redis_namespace="cache",
    )

    redis_wrapper = RedisClient(redis_client)
    write_public_feed_to_redis(redis_wrapper, "timeline:public", recs, test_settings)

    # Check key has the namespace prefix
    count_namespaced = redis_client.zcard("cache:timeline:public")
    count_raw = redis_client.zcard("timeline:public")

    assert count_namespaced == 1, "Key should have namespace prefix"
    assert count_raw == 0, "Raw key should not have data"


@pytest.mark.integration
def test_write_public_feed_to_redis_empty_recs(
    redis_client: redis.Redis,
) -> None:
    """write_public_feed_to_redis does nothing for empty recommendations."""
    key = "timeline:public:empty_test"

    test_settings = HintGridSettings(feed_score_multiplier=10)
    redis_wrapper = RedisClient(redis_client)
    write_public_feed_to_redis(redis_wrapper, key, [], test_settings)

    assert redis_client.zcard(key) == 0


@pytest.mark.integration
def test_write_public_feed_outranks_mastodon_entries(
    redis_client: redis.Redis,
) -> None:
    """HintGrid entries in public feed outrank native Mastodon entries.

    Mastodon adds entries with score = post_id.
    HintGrid entries should all have score > any post_id.
    """
    key = "timeline:public:outrank_test"

    # Simulate Mastodon native entries (score = post_id)
    redis_client.zadd(key, {"8000": 8000, "8001": 8001, "8002": 8002})

    # Add HintGrid recommendations
    recs: list[dict[str, float]] = [
        {"post_id": 9000, "score": 0.95},
        {"post_id": 9001, "score": 0.80},
    ]

    test_settings = HintGridSettings(
        feed_score_multiplier=10,
        redis_namespace=None,
    )

    redis_wrapper = RedisClient(redis_client)
    write_public_feed_to_redis(redis_wrapper, key, recs, test_settings)

    # Get all entries sorted by score DESC
    entries = redis_client.zrevrange(key, 0, -1, withscores=True)

    # HintGrid entries should be first (highest scores)
    hintgrid_ids = {int(member) for member, _score in entries[:2]}
    assert 9000 in hintgrid_ids
    assert 9001 in hintgrid_ids

    # All HintGrid scores should be > all Mastodon scores
    hintgrid_min = min(
        coerce_float(score) for member, score in entries if int(member) >= 9000
    )
    mastodon_max = max(
        coerce_float(score) for member, score in entries if int(member) < 9000
    )
    assert hintgrid_min > mastodon_max, (
        f"HintGrid min score ({hintgrid_min}) should exceed "
        f"Mastodon max score ({mastodon_max})"
    )
