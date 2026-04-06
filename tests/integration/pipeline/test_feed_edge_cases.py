"""Integration tests for feed generation edge cases.

Covers:
- Empty results and cold start fallback
- Redis connection errors and timeouts
- Scoring edge cases (zero weights, negative values)
- Namespace handling edge cases
- Public feed edge cases
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.clients.redis import RedisClient
from hintgrid.config import HintGridSettings
from hintgrid.pipeline.feed import (
    generate_public_feed,
    generate_user_feed,
    get_detailed_recommendations,
    namespaced_key,
    write_feed_to_redis,
    write_public_feed_to_redis,
)

if TYPE_CHECKING:
    import redis

    from hintgrid.clients.neo4j import Neo4jClient
else:
    import redis
    from hintgrid.clients.neo4j import Neo4jClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_user_without_communities(neo4j: Neo4jClient, user_id: int) -> None:
    """Create a user with no communities (for cold start testing)."""
    # Explicit runtime use of Neo4jClient
    assert isinstance(neo4j, Neo4jClient)
    neo4j.execute_labeled(
        "CREATE (:__user__ {id: $user_id})",
        {"user": "User"},
        {"user_id": user_id},
    )


def _setup_user_with_empty_communities(neo4j: Neo4jClient, user_id: int) -> None:
    """Create a user with communities but no INTERESTED_IN relationships."""
    # Explicit runtime use of Neo4jClient
    assert isinstance(neo4j, Neo4jClient)
    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: $user_id})\n"
        "CREATE (uc:__uc__ {id: 'empty_uc'})\n"
        "CREATE (u)-[:BELONGS_TO]->(uc)",
        {"user": "User", "uc": "UserCommunity"},
        {"user_id": user_id},
    )


def _setup_posts_for_cold_start(neo4j: Neo4jClient) -> None:
    """Create posts for cold start fallback (no community relationships)."""
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {\n"
        "    id: 9001,\n"
        "    createdAt: datetime() - duration({hours: 1}),\n"
        "    embedding: [0.1, 0.2, 0.3]\n"
        "})\n"
        "CREATE (p2:__post__ {\n"
        "    id: 9002,\n"
        "    createdAt: datetime() - duration({hours: 2}),\n"
        "    embedding: [0.4, 0.5, 0.6]\n"
        "})",
        {"post": "Post"},
    )


# ---------------------------------------------------------------------------
# Tests: Empty results and cold start
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_generate_user_feed_empty_results_triggers_cold_start(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that empty personalized results trigger cold start fallback."""
    user_id = 80001
    _setup_user_without_communities(neo4j, user_id)
    _setup_posts_for_cold_start(neo4j)

    test_settings = HintGridSettings(
        feed_size=100,
        feed_days=7,
        cold_start_limit=50,
        feed_pc_share_weight=1.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        cold_start_popularity_weight=1.0,
        cold_start_recency_weight=0.0,
        popularity_smoothing=1,
        recency_smoothing=1,
        recency_numerator=1.0,
        language_match_weight=0.0,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_user_feed(neo4j, user_id, test_settings)

    # Cold start should return posts even without communities
    assert len(recs) > 0, "Cold start should return posts when personalized feed is empty"
    assert all("post_id" in r and "score" in r for r in recs), (
        "All recommendations should have post_id and score"
    )


@pytest.mark.integration
def test_generate_user_feed_empty_communities_triggers_cold_start(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that user with communities but no INTERESTED_IN triggers cold start."""
    user_id = 80002
    _setup_user_with_empty_communities(neo4j, user_id)
    _setup_posts_for_cold_start(neo4j)

    test_settings = HintGridSettings(
        feed_size=100,
        feed_days=7,
        cold_start_limit=50,
        feed_pc_share_weight=1.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        cold_start_popularity_weight=1.0,
        cold_start_recency_weight=0.0,
        popularity_smoothing=1,
        recency_smoothing=1,
        recency_numerator=1.0,
        language_match_weight=0.0,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_user_feed(neo4j, user_id, test_settings)

    # Should fall back to cold start
    assert len(recs) > 0, "Should use cold start when no INTERESTED_IN relationships"


@pytest.mark.integration
def test_generate_user_feed_cold_start_respects_limit(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that cold start respects cold_start_limit setting."""
    user_id = 80003
    _setup_user_without_communities(neo4j, user_id)
    _setup_posts_for_cold_start(neo4j)

    # Create more posts than cold_start_limit
    for i in range(10):
        neo4j.execute_labeled(
            "CREATE (:__post__ {\n"
            "    id: $post_id,\n"
            "    createdAt: datetime() - duration({hours: $hours}),\n"
            "    embedding: [0.1, 0.2, 0.3]\n"
            "})",
            {"post": "Post"},
            {"post_id": 9003 + i, "hours": i + 1},
        )

    test_settings = HintGridSettings(
        feed_size=100,
        feed_days=7,
        cold_start_limit=5,  # Limit to 5 posts
        feed_pc_share_weight=1.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        cold_start_popularity_weight=1.0,
        cold_start_recency_weight=0.0,
        popularity_smoothing=1,
        recency_smoothing=1,
        recency_numerator=1.0,
        language_match_weight=0.0,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_user_feed(neo4j, user_id, test_settings)

    # Should not exceed cold_start_limit
    assert len(recs) <= test_settings.cold_start_limit, (
        f"Cold start should respect limit of {test_settings.cold_start_limit}, "
        f"got {len(recs)}"
    )


@pytest.mark.integration
def test_generate_user_feed_cold_start_no_posts_returns_empty(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that cold start returns empty when no posts exist."""
    user_id = 80004
    _setup_user_without_communities(neo4j, user_id)
    # No posts created

    test_settings = HintGridSettings(
        feed_size=100,
        feed_days=7,
        cold_start_limit=50,
        feed_pc_share_weight=1.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        cold_start_popularity_weight=1.0,
        cold_start_recency_weight=0.0,
        popularity_smoothing=1,
        recency_smoothing=1,
        recency_numerator=1.0,
        language_match_weight=0.0,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_user_feed(neo4j, user_id, test_settings)

    # Should return empty list when no posts exist
    assert recs == [], "Should return empty list when no posts exist for cold start"


# ---------------------------------------------------------------------------
# Tests: Redis connection errors
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_write_feed_to_redis_handles_closed_connection(
    redis_client: redis.Redis,
) -> None:
    # Explicit runtime use of redis
    assert isinstance(redis_client, redis.Redis)
    """Test that write_feed_to_redis handles closed Redis connection gracefully.
    
    Note: redis-py automatically reconnects on connection loss, so we need to
    simulate a connection error by patching the pipeline.execute method.
    """
    from unittest.mock import patch
    
    recs: list[dict[str, float]] = [
        {"post_id": 10000, "score": 0.95},
        {"post_id": 10001, "score": 0.80},
    ]

    test_settings = HintGridSettings(feed_score_multiplier=10)
    redis_wrapper = RedisClient(redis_client)

    # Mock pipeline.execute to raise ConnectionError
    # This simulates what would happen if Redis connection was lost
    with (
        patch.object(
            redis_client, "pipeline", return_value=type("MockPipeline", (), {
                "zadd": lambda self, *args, **kwargs: self,
                "execute": lambda self: (_ for _ in ()).throw(ConnectionError("Connection lost")),
            })()
        ),
        pytest.raises(ConnectionError),
    ):
        # Should raise ConnectionError when trying to execute pipeline
        write_feed_to_redis(redis_wrapper, 80010, recs, test_settings)


@pytest.mark.integration
def test_write_public_feed_to_redis_handles_closed_connection(
    redis_client: redis.Redis,
) -> None:
    # Explicit runtime use of redis
    assert isinstance(redis_client, redis.Redis)
    """Test that write_public_feed_to_redis handles closed Redis connection.
    
    Note: redis-py automatically reconnects on connection loss, so we need to
    simulate a connection error by patching the pipeline.execute method.
    """
    from unittest.mock import patch
    
    recs: list[dict[str, float]] = [
        {"post_id": 10002, "score": 0.90},
    ]

    test_settings = HintGridSettings(feed_score_multiplier=10)
    redis_wrapper = RedisClient(redis_client)

    # Mock pipeline.execute to raise ConnectionError
    # This simulates what would happen if Redis connection was lost
    with (
        patch.object(
            redis_client, "pipeline", return_value=type("MockPipeline", (), {
                "zadd": lambda self, *args, **kwargs: self,
                "execute": lambda self: (_ for _ in ()).throw(ConnectionError("Connection lost")),
            })()
        ),
        pytest.raises(ConnectionError),
    ):
        # Should raise ConnectionError when trying to execute pipeline
        write_public_feed_to_redis(redis_wrapper, "timeline:public:test", recs, test_settings)


# ---------------------------------------------------------------------------
# Tests: Scoring edge cases
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_generate_user_feed_zero_weights(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test feed generation with all weights set to zero."""
    user_id = 80005
    _setup_user_without_communities(neo4j, user_id)
    _setup_posts_for_cold_start(neo4j)

    test_settings = HintGridSettings(
        feed_size=100,
        feed_days=7,
        cold_start_limit=50,
        feed_pc_share_weight=0.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        cold_start_popularity_weight=0.0,
        cold_start_recency_weight=0.0,
        popularity_smoothing=1,
        recency_smoothing=1,
        recency_numerator=1.0,
        language_match_weight=0.0,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_user_feed(neo4j, user_id, test_settings)

    # With all weights zero, should still return posts (language boost might apply)
    # or return empty if language_match_weight is also zero
    # The exact behavior depends on implementation, but should not crash
    assert isinstance(recs, list), "Should return a list even with zero weights"


@pytest.mark.integration
def test_generate_user_feed_negative_scores_handled(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that negative scores are handled correctly (should not crash)."""
    # Create graph with very old posts (negative recency score possible)
    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: 80006})\n"
        "CREATE (uc:__uc__ {id: 'neg_uc'})\n"
        "CREATE (pc:__pc__ {id: 'neg_pc'})\n"
        "CREATE (p:__post__ {\n"
        "    id: 10003,\n"
        "    createdAt: datetime() - duration({days: 365})\n"
        "})\n"
        "CREATE (u)-[:BELONGS_TO]->(uc)\n"
        "CREATE (uc)-[:INTERESTED_IN {score: 0.1}]->(pc)\n"
        "CREATE (p)-[:BELONGS_TO]->(pc)",
        {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity", "post": "Post"},
    )

    test_settings = HintGridSettings(
        feed_size=100,
        feed_days=400,  # Include very old posts
        feed_pc_share_weight=0.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=1.0,  # High recency weight
        popularity_smoothing=1,
        recency_smoothing=1.0,  # Small smoothing might cause issues
        recency_numerator=1.0,
        language_match_weight=0.0,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    # Should not crash even if scores are negative or very small
    recs = generate_user_feed(neo4j, 80006, test_settings)
    assert isinstance(recs, list), "Should handle negative or very small scores gracefully"


# ---------------------------------------------------------------------------
# Tests: Namespace handling edge cases
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_namespaced_key_with_colon_in_key() -> None:
    """Test namespaced_key with keys that already contain colons."""
    settings = HintGridSettings(redis_namespace="cache")
    result = namespaced_key("feed:home:123", settings)
    assert result == "cache:feed:home:123"


@pytest.mark.integration
def test_namespaced_key_with_empty_key() -> None:
    """Test namespaced_key with empty string key."""
    settings = HintGridSettings(redis_namespace="cache")
    result = namespaced_key("", settings)
    assert result == "cache:"


@pytest.mark.integration
def test_write_feed_to_redis_with_namespace(
    redis_client: redis.Redis,
) -> None:
    """Test write_feed_to_redis with namespace configured.
    
    Note: Namespace is NOT applied to feed:home:* keys (personal feeds).
    Namespace is only used for public timelines (timeline:public).
    This is intentional to avoid conflicts with Mastodon FeedManager.
    """
    recs: list[dict[str, float]] = [
        {"post_id": 10004, "score": 0.95},
    ]

    test_settings = HintGridSettings(
        feed_score_multiplier=10,
        redis_namespace="test_namespace",
    )
    redis_wrapper = RedisClient(redis_client)
    write_feed_to_redis(redis_wrapper, 80011, recs, test_settings)

    # feed:home:* keys do NOT use namespace (by design, to work with Mastodon)
    raw_key = "feed:home:80011"
    count = redis_client.zcard(raw_key)
    assert count == 1, (
        f"Feed should be written to raw key {raw_key} "
        f"(namespace is not applied to feed:home:* keys)"
    )

    # Namespaced key should be empty (namespace not used for personal feeds)
    namespaced_key = "test_namespace:feed:home:80011"
    namespaced_count = redis_client.zcard(namespaced_key)
    assert namespaced_count == 0, (
        f"Namespaced key {namespaced_key} should be empty "
        f"(namespace is not applied to feed:home:* keys)"
    )


# ---------------------------------------------------------------------------
# Tests: Public feed edge cases
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_generate_public_feed_all_weights_zero(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test public feed generation with all weights set to zero."""
    test_settings = HintGridSettings(
        public_feed_size=100,
        feed_days=7,
        feed_pc_share_weight=0.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        popularity_smoothing=1,
        recency_smoothing=1,
        recency_numerator=1.0,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_public_feed(neo4j, test_settings)

    # Should return list (possibly empty) without crashing
    assert isinstance(recs, list), "Should return a list even with zero weights"


@pytest.mark.integration
def test_generate_public_feed_no_communities_returns_empty(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test public feed generation when no communities exist."""
    test_settings = HintGridSettings(
        public_feed_size=100,
        feed_days=7,
        feed_pc_share_weight=1.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_public_feed(neo4j, test_settings)

    assert recs == [], "Should return empty list when no communities exist"


@pytest.mark.integration
def test_write_public_feed_to_redis_empty_list(
    redis_client: redis.Redis,
) -> None:
    """Test write_public_feed_to_redis with empty recommendations list."""
    test_settings = HintGridSettings(
        feed_score_multiplier=10,
        redis_namespace=None,
    )
    redis_wrapper = RedisClient(redis_client)
    write_public_feed_to_redis(redis_wrapper, "timeline:public:empty", [], test_settings)

    # Should not create any entries
    count = redis_client.zcard("timeline:public:empty")
    assert count == 0, "Should not write anything for empty recommendations"


@pytest.mark.integration
def test_write_feed_to_redis_empty_list(
    redis_client: redis.Redis,
) -> None:
    """Test write_feed_to_redis with empty recommendations list."""
    test_settings = HintGridSettings(feed_score_multiplier=10)
    redis_wrapper = RedisClient(redis_client)
    write_feed_to_redis(redis_wrapper, 80012, [], test_settings)

    # Should not create any entries
    key = "feed:home:80012"
    count = redis_client.zcard(key)
    assert count == 0, "Should not write anything for empty recommendations"


@pytest.mark.integration
def test_generate_public_feed_local_only_combinations(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test public feed with various combinations of local_only flags."""
    # Setup: mix of local and remote users/posts
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 80007, isLocal: true})\n"
        "CREATE (u2:__user__ {id: 80008, isLocal: false})\n"
        "CREATE (uc1:__uc__ {id: 'combo_uc1'})\n"
        "CREATE (uc2:__uc__ {id: 'combo_uc2'})\n"
        "CREATE (pc1:__pc__ {id: 'combo_pc1'})\n"
        "CREATE (p1:__post__ {id: 10005, createdAt: datetime()})\n"
        "CREATE (p2:__post__ {id: 10006, createdAt: datetime()})\n"
        "CREATE (u1)-[:BELONGS_TO]->(uc1)\n"
        "CREATE (u2)-[:BELONGS_TO]->(uc2)\n"
        "CREATE (uc1)-[:INTERESTED_IN {score: 0.9}]->(pc1)\n"
        "CREATE (uc2)-[:INTERESTED_IN {score: 0.8}]->(pc1)\n"
        "CREATE (p1)-[:BELONGS_TO]->(pc1)\n"
        "CREATE (p2)-[:BELONGS_TO]->(pc1)\n"
        "CREATE (u1)-[:WROTE]->(p1)\n"
        "CREATE (u2)-[:WROTE]->(p2)",
        {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity", "post": "Post"},
    )

    test_settings = HintGridSettings(
        public_feed_size=100,
        feed_days=7,
        feed_pc_share_weight=1.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    # Test all combinations
    combinations = [
        (True, True),   # local_only_interests=True, local_only_authors=True
        (True, False),  # local_only_interests=True, local_only_authors=False
        (False, True),  # local_only_interests=False, local_only_authors=True
        (False, False), # local_only_interests=False, local_only_authors=False
    ]

    for local_interests, local_authors in combinations:
        recs = generate_public_feed(
            neo4j,
            test_settings,
            local_only_interests=local_interests,
            local_only_authors=local_authors,
        )
        assert isinstance(recs, list), (
            f"Should return list for combination "
            f"local_interests={local_interests}, local_authors={local_authors}"
        )


# ---------------------------------------------------------------------------
# Tests: get_detailed_recommendations
# ---------------------------------------------------------------------------


def _setup_detailed_recommendations_graph(neo4j: Neo4jClient) -> None:
    # Explicit runtime use of Neo4jClient
    assert isinstance(neo4j, Neo4jClient)
    """Create graph for detailed recommendations tests."""
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")
    neo4j.label("Post")

    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: 70001, languages: ['en', 'ru']})\n"
        "CREATE (uc:__uc__ {id: 'detail_uc1'})\n"
        "CREATE (pc:__pc__ {id: 'detail_pc1'})\n"
        "CREATE (p1:__post__ {\n"
        "    id: 20001,\n"
        "    text: 'First post with interesting content',\n"
        "    language: 'en',\n"
        "    authorId: 80001,\n"
        "    createdAt: datetime() - duration({hours: 2}),\n"
        "    pagerank: 0.5\n"
        "})\n"
        "CREATE (p2:__post__ {\n"
        "    id: 20002,\n"
        "    text: 'Second post with different content',\n"
        "    language: 'ru',\n"
        "    authorId: 80002,\n"
        "    createdAt: datetime() - duration({hours: 5}),\n"
        "    pagerank: 0.3\n"
        "})\n"
        "CREATE (u)-[:BELONGS_TO]->(uc)\n"
        "CREATE (uc)-[:INTERESTED_IN {score: 0.8}]->(pc)\n"
        "CREATE (p1)-[:BELONGS_TO]->(pc)\n"
        "CREATE (p2)-[:BELONGS_TO]->(pc)\n"
        "CREATE (author1:__user__ {id: 80001})-[:WROTE]->(p1)\n"
        "CREATE (author2:__user__ {id: 80002})-[:WROTE]->(p2)\n"
        "CREATE (:__user__)-[:FAVORITED]->(p1)\n"
        "CREATE (:__user__)-[:FAVORITED]->(p1)\n"
        "CREATE (:__user__)-[:FAVORITED]->(p2)",
        {
            "user": "User",
            "uc": "UserCommunity",
            "pc": "PostCommunity",
            "post": "Post",
        },
    )


@pytest.mark.integration
def test_get_detailed_recommendations_returns_full_info(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test get_detailed_recommendations returns complete recommendation details."""
    _setup_detailed_recommendations_graph(neo4j)

    test_settings = HintGridSettings(
        feed_size=10,
        feed_days=7,
        feed_pc_share_weight=1.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.5,
        personalized_recency_weight=0.3,
        pagerank_weight=0.2,
        pagerank_enabled=True,
        popularity_smoothing=1,
        recency_smoothing=1,
        recency_numerator=1.0,
        language_match_weight=0.3,
        neo4j_worker_label=settings.neo4j_worker_label,
    )

    detailed_recs = get_detailed_recommendations(neo4j, 70001, test_settings)

    assert len(detailed_recs) > 0, "Should return at least one recommendation"

    # Check first recommendation has all required fields
    first_rec = detailed_recs[0]
    assert "post_id" in first_rec
    assert "post_text" in first_rec
    assert "post_language" in first_rec
    assert "post_created_at" in first_rec
    assert "author_id" in first_rec
    assert "interest_score" in first_rec
    assert "share_i" in first_rec
    assert "norm_pc_size" in first_rec
    assert "local_raw" in first_rec
    assert "global_raw" in first_rec
    assert "popularity_contrib" in first_rec
    assert "age_hours" in first_rec
    assert "pagerank" in first_rec
    assert "language_match" in first_rec
    assert "final_score" in first_rec

    # Verify data types and values
    assert isinstance(first_rec["post_id"], int)
    assert isinstance(first_rec["post_text"], str)
    assert isinstance(first_rec["author_id"], int)
    assert isinstance(first_rec["interest_score"], float)
    assert isinstance(first_rec["share_i"], float)
    assert isinstance(first_rec["norm_pc_size"], float)
    assert isinstance(first_rec["local_raw"], float)
    assert isinstance(first_rec["global_raw"], float)
    assert isinstance(first_rec["popularity_contrib"], float)
    assert isinstance(first_rec["age_hours"], float)
    assert isinstance(first_rec["pagerank"], float)
    assert isinstance(first_rec["language_match"], float)
    assert isinstance(first_rec["final_score"], float)
    assert first_rec["final_score"] >= 0.0, "Final score should be non-negative"

    # Verify post text is present
    assert len(first_rec["post_text"]) > 0, "Post text should not be empty"


@pytest.mark.integration
def test_get_detailed_recommendations_includes_score_components(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test get_detailed_recommendations includes all score components."""
    _setup_detailed_recommendations_graph(neo4j)

    test_settings = HintGridSettings(
        feed_size=10,
        feed_days=7,
        feed_pc_share_weight=0.25,
        feed_pc_size_weight=0.25,
        personalized_popularity_weight=0.25,
        personalized_recency_weight=0.25,
        pagerank_weight=1.0,
        pagerank_enabled=True,
        popularity_smoothing=1,
        recency_smoothing=1,
        recency_numerator=1.0,
        language_match_weight=0.3,
        neo4j_worker_label=settings.neo4j_worker_label,
    )

    detailed_recs = get_detailed_recommendations(neo4j, 70001, test_settings)

    assert len(detailed_recs) >= 2, "Should return at least 2 recommendations"

    # Check that score components are present and reasonable
    for rec in detailed_recs:
        assert rec["interest_score"] >= 0.0, "Interest score should be non-negative"
        assert rec["local_raw"] >= 0.0, "local_raw should be non-negative"
        assert rec["global_raw"] >= 0.0, "global_raw should be non-negative"
        assert rec["popularity_contrib"] >= 0.0, "popularity_contrib should be non-negative"
        assert rec["age_hours"] >= 0.0, "Age hours should be non-negative"
        assert rec["pagerank"] >= 0.0, "PageRank should be non-negative"
        assert rec["language_match"] >= 0.0, "Language match should be non-negative"
        assert rec["final_score"] >= 0.0, "Final score should be non-negative"


@pytest.mark.integration
def test_get_detailed_recommendations_cold_start_has_zero_interest(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test get_detailed_recommendations in cold start mode has interest_score=0."""
    user_id = 70002
    _setup_user_without_communities(neo4j, user_id)
    _setup_posts_for_cold_start(neo4j)

    test_settings = HintGridSettings(
        feed_size=10,
        feed_days=7,
        cold_start_limit=5,
        feed_pc_share_weight=1.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        cold_start_popularity_weight=1.0,
        cold_start_recency_weight=0.0,
        popularity_smoothing=1,
        recency_smoothing=1,
        recency_numerator=1.0,
        language_match_weight=0.0,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    detailed_recs = get_detailed_recommendations(neo4j, user_id, test_settings)

    assert len(detailed_recs) > 0, "Cold start should return recommendations"

    # In cold start, interest_score should be 0.0
    for rec in detailed_recs:
        assert rec["interest_score"] == 0.0, (
            "Cold start recommendations should have interest_score=0.0"
        )


@pytest.mark.integration
def test_get_detailed_recommendations_empty_returns_empty_list(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test get_detailed_recommendations returns empty list when no posts available."""
    user_id = 70003
    _setup_user_without_communities(neo4j, user_id)
    # No posts created

    test_settings = HintGridSettings(
        feed_size=10,
        feed_days=7,
        cold_start_limit=5,
        feed_pc_share_weight=1.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        cold_start_popularity_weight=1.0,
        cold_start_recency_weight=0.0,
        popularity_smoothing=1,
        recency_smoothing=1,
        recency_numerator=1.0,
        language_match_weight=0.0,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    detailed_recs = get_detailed_recommendations(neo4j, user_id, test_settings)

    assert detailed_recs == [], "Should return empty list when no posts available"
