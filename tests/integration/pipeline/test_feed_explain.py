"""Integration tests for feed inclusion explanation (explain_feed_inclusion)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.clients.redis import RedisClient
from hintgrid.config import HintGridSettings
from hintgrid.pipeline.feed import generate_user_feed, write_feed_to_redis
from hintgrid.pipeline.feed_explain import explain_feed_inclusion, feed_explain_rel_types

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient


def _setup_personalized_pair(neo4j: Neo4jClient) -> None:
    """Minimal graph: one viewer, INTERESTED_IN to post's PostCommunity."""
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")
    neo4j.label("Post")
    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: 71001, languages: ['en'], uiLanguage: 'en'})\n"
        "CREATE (uc:__uc__ {id: 'uc_explain'})\n"
        "CREATE (pc:__pc__ {id: 'pc_explain'})\n"
        "CREATE (p:__post__ {\n"
        "    id: 21001,\n"
        "    language: 'en',\n"
        "    createdAt: datetime() - duration({hours: 1}),\n"
        "    authorId: 99001,\n"
        "    pagerank: 0.2\n"
        "})\n"
        "CREATE (u)-[:BELONGS_TO]->(uc)\n"
        "CREATE (uc)-[:INTERESTED_IN {score: 0.55, based_on: 7, serendipity: false}]->(pc)\n"
        "CREATE (p)-[:BELONGS_TO]->(pc)",
        {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity", "post": "Post"},
    )


def _setup_cold_only(neo4j: Neo4jClient) -> None:
    """User without communities; one embedded post."""
    neo4j.execute_labeled(
        "CREATE (:__user__ {id: 71002})",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "CREATE (:__post__ {\n"
        "    id: 21002,\n"
        "    createdAt: datetime() - duration({hours: 2}),\n"
        "    embedding: [0.1, 0.2, 0.3],\n"
        "    authorId: 1,\n"
        "    language: 'en'\n"
        "})",
        {"post": "Post"},
    )


@pytest.mark.integration
def test_explain_feed_inclusion_personalized_path(
    neo4j: Neo4jClient,
    redis_client: object,
    settings: HintGridSettings,
) -> None:
    """Personalized path returns score components and INTERESTED_IN context."""
    _setup_personalized_pair(neo4j)
    redis_wrapper = RedisClient(redis_client)

    test_settings = HintGridSettings(
        feed_size=20,
        feed_days=7,
        feed_pc_share_weight=0.7,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.2,
        personalized_recency_weight=0.1,
        pagerank_enabled=True,
        pagerank_weight=0.1,
        popularity_smoothing=1.0,
        recency_smoothing=1.0,
        recency_numerator=1.0,
        language_match_weight=0.1,
        ui_language_match_weight=0.2,
        neo4j_worker_label=settings.neo4j_worker_label,
        noise_community_id=-1,
    )

    ex = explain_feed_inclusion(
        neo4j,
        redis_wrapper,
        71001,
        21001,
        test_settings,
        rel_types=neo4j.get_existing_rel_types(),
    )
    assert ex is not None
    assert ex["path"] == "personalized"
    assert ex["score_components"] is not None
    sc = ex["score_components"]
    assert sc["interest_score"] > 0.0
    assert sc["final_cypher_score"] > 0.0
    edge = ex["interest_edge"]
    assert edge is not None
    assert edge.get("interest_rel_score", 0.0) > 0.0
    assert ex["settings_snapshot"]["feed_size"] == 20


@pytest.mark.integration
def test_explain_feed_inclusion_cold_start_path(
    neo4j: Neo4jClient,
    redis_client: object,
    settings: HintGridSettings,
) -> None:
    """Cold start path when no INTERESTED_IN chain exists."""
    _setup_cold_only(neo4j)
    redis_wrapper = RedisClient(redis_client)

    test_settings = HintGridSettings(
        feed_size=20,
        feed_days=7,
        cold_start_limit=10,
        feed_pc_share_weight=1.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        cold_start_popularity_weight=0.5,
        cold_start_recency_weight=0.5,
        popularity_smoothing=1.0,
        recency_smoothing=1.0,
        recency_numerator=1.0,
        language_match_weight=0.0,
        ui_language_match_weight=0.0,
        pagerank_enabled=False,
        neo4j_worker_label=settings.neo4j_worker_label,
    )

    ex = explain_feed_inclusion(
        neo4j,
        redis_wrapper,
        71002,
        21002,
        test_settings,
        rel_types=neo4j.get_existing_rel_types(),
    )
    assert ex is not None
    assert ex["path"] == "cold_start"
    assert ex["score_components"] is not None
    assert ex["score_components"]["interest_score"] == 0.0
    assert ex["interest_edge"] is None


@pytest.mark.integration
def test_explain_feed_inclusion_redis_rank_after_write(
    neo4j: Neo4jClient,
    redis_client: object,
    settings: HintGridSettings,
) -> None:
    """Top recommendation has zrevrank 0 after write_feed_to_redis."""
    _setup_personalized_pair(neo4j)
    # Second post so batch has max_post_id for formula
    neo4j.execute_labeled(
        "MATCH (pc:__pc__ {id: 'pc_explain'}) "
        "CREATE (p2:__post__ {\n"
        "    id: 21003,\n"
        "    language: 'en',\n"
        "    createdAt: datetime() - duration({hours: 3}),\n"
        "    authorId: 99002,\n"
        "    pagerank: 0.1\n"
        "}) "
        "CREATE (p2)-[:BELONGS_TO]->(pc)",
        {"pc": "PostCommunity", "post": "Post"},
    )

    redis_wrapper = RedisClient(redis_client)
    test_settings = HintGridSettings(
        feed_size=10,
        feed_days=7,
        feed_pc_share_weight=1.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        popularity_smoothing=1.0,
        recency_smoothing=1.0,
        recency_numerator=1.0,
        language_match_weight=0.0,
        ui_language_match_weight=0.0,
        feed_score_multiplier=2,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_user_feed(neo4j, 71001, test_settings)
    assert len(recs) >= 2
    write_feed_to_redis(redis_wrapper, 71001, recs, test_settings)

    top_post = int(recs[0]["post_id"])
    ex = explain_feed_inclusion(
        neo4j,
        redis_wrapper,
        71001,
        top_post,
        test_settings,
        rel_types=neo4j.get_existing_rel_types(),
    )
    assert ex is not None
    rr = ex["redis"]
    assert rr.get("zrevrank_0_is_top") == 0
    assert rr.get("redis_score") is not None
    assert float(rr["redis_score"]) > float(top_post)


@pytest.mark.integration
def test_explain_feed_inclusion_respect_was_recommended_filter(
    neo4j: Neo4jClient,
    redis_client: object,
    settings: HintGridSettings,
) -> None:
    """With WAS_RECOMMENDED edge, strict rel_types yields not_scored; omitting type restores path."""
    _setup_personalized_pair(neo4j)
    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: $uid}), (p:__post__ {id: $pid}) "
        "MERGE (u)-[r:WAS_RECOMMENDED]->(p) ON CREATE SET r.at = datetime()",
        {"user": "User", "post": "Post"},
        {"uid": 71001, "pid": 21001},
    )
    neo4j.invalidate_rel_types_cache()

    redis_wrapper = RedisClient(redis_client)
    test_settings = HintGridSettings(
        feed_size=20,
        feed_days=7,
        feed_pc_share_weight=0.7,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=0.2,
        personalized_recency_weight=0.1,
        pagerank_enabled=True,
        pagerank_weight=0.1,
        popularity_smoothing=1.0,
        recency_smoothing=1.0,
        recency_numerator=1.0,
        language_match_weight=0.1,
        ui_language_match_weight=0.2,
        neo4j_worker_label=settings.neo4j_worker_label,
        noise_community_id=-1,
    )

    existing = neo4j.get_existing_rel_types()
    assert "WAS_RECOMMENDED" in existing

    ex_strict = explain_feed_inclusion(
        neo4j,
        redis_wrapper,
        71001,
        21001,
        test_settings,
        rel_types=existing,
    )
    assert ex_strict is not None
    assert ex_strict["path"] == "not_scored"

    ex_loose = explain_feed_inclusion(
        neo4j,
        redis_wrapper,
        71001,
        21001,
        test_settings,
        rel_types=feed_explain_rel_types(existing, respect_was_recommended=False),
    )
    assert ex_loose is not None
    assert ex_loose["path"] == "personalized"
    assert ex_loose["score_components"] is not None
