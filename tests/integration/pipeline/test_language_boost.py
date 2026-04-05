"""Integration tests for language boost scoring in feed generation.

Covers:
- Language match adds a boost to recommendation score
- Language mismatch does NOT filter posts (soft boost, not hard filter)
- Users without languages property get the boost for all posts
- Posts without language property get the boost for all users
- Both personalized and cold-start feeds apply language boost
"""

from __future__ import annotations


import pytest

from hintgrid.config import HintGridSettings
from hintgrid.pipeline.feed import generate_user_feed
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_language_graph(neo4j: Neo4jClient) -> None:
    """Create a graph for language boost tests.

    Creates:
    - User 40001 with languages=["en","de"]
    - User 40002 with no languages
    - UserCommunity uc1, PostCommunity pc1 with INTERESTED_IN
    - Post 40101 language="en" (matches user 40001)
    - Post 40102 language="fr" (doesn't match user 40001)
    - Post 40103 language=null (should get boost for everyone)
    """
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")
    neo4j.label("Post")

    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 40001, isLocal: true, languages: ['en', 'de']}) "
        "CREATE (u2:__user__ {id: 40002, isLocal: true}) "
        "CREATE (uc1:__uc__ {id: 'lang_uc_1'}) "
        "CREATE (pc1:__pc__ {id: 'lang_pc_1'}) "
        "CREATE (u1)-[:BELONGS_TO]->(uc1) "
        "CREATE (u2)-[:BELONGS_TO]->(uc1) "
        "CREATE (uc1)-[:INTERESTED_IN {score: 1.0}]->(pc1) "
        "CREATE (p1:__post__ {id: 40101, language: 'en', createdAt: datetime() - duration({hours: 1}), embedding: [0.1]})-[:BELONGS_TO]->(pc1) "
        "CREATE (p2:__post__ {id: 40102, language: 'fr', createdAt: datetime() - duration({hours: 1}), embedding: [0.2]})-[:BELONGS_TO]->(pc1) "
        "CREATE (p3:__post__ {id: 40103, createdAt: datetime() - duration({hours: 1}), embedding: [0.3]})-[:BELONGS_TO]->(pc1)",
        {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity", "post": "Post"},
    )


# ---------------------------------------------------------------------------
# Tests: Language boost (personalized feed)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_language_match_boosts_score(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Posts in user's preferred languages get a score boost.

    Post 40101 (en) should score higher than post 40102 (fr) for user 40001
    who speaks en+de. Both posts must still appear (soft boost, not filter).
    """
    _setup_language_graph(neo4j)

    test_settings = HintGridSettings(
        personalized_interest_weight=1.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        language_match_weight=0.5,  # Significant boost
        feed_size=100,
        feed_days=7,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_user_feed(neo4j, 40001, test_settings)
    rec_map = {int(r["post_id"]): r["score"] for r in recs}

    # Both posts must appear (language is a boost, not a filter)
    assert 40101 in rec_map, "English post should appear for en-speaker"
    assert 40102 in rec_map, "French post should still appear (not filtered)"

    # English post should have higher score due to language boost
    assert rec_map[40101] > rec_map[40102], (
        f"English post score ({rec_map[40101]}) should be higher than "
        f"French post score ({rec_map[40102]}) due to language boost"
    )


@pytest.mark.integration
def test_language_boost_not_filter(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Language preference is a soft boost, not a hard filter.

    Even with very high language_match_weight, non-matching posts
    must still appear in the feed.
    """
    _setup_language_graph(neo4j)

    test_settings = HintGridSettings(
        personalized_interest_weight=1.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        language_match_weight=10.0,  # Very high boost
        feed_size=100,
        feed_days=7,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_user_feed(neo4j, 40001, test_settings)
    post_ids = {int(r["post_id"]) for r in recs}

    assert 40101 in post_ids, "Matching language post present"
    assert 40102 in post_ids, "Non-matching language post also present"
    assert 40103 in post_ids, "Post without language present"


@pytest.mark.integration
def test_null_post_language_gets_boost(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Posts without language property receive the language boost.

    Post 40103 has no language — per Cypher CASE logic, it gets the boost.
    Its score should be equal to post 40101 (en, also gets boost).
    """
    _setup_language_graph(neo4j)

    test_settings = HintGridSettings(
        personalized_interest_weight=1.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        language_match_weight=0.5,
        feed_size=100,
        feed_days=7,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_user_feed(neo4j, 40001, test_settings)
    rec_map = {int(r["post_id"]): r["score"] for r in recs}

    assert 40103 in rec_map, "Post without language should appear"
    # Post 40103 (no lang) and 40101 (en) should have same boost
    assert abs(rec_map[40103] - rec_map[40101]) < 0.01, (
        f"Post without language ({rec_map[40103]}) should have same score as "
        f"matching language post ({rec_map[40101]})"
    )


@pytest.mark.integration
def test_user_without_languages_gets_boost_for_all(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Users without languages property get boost for ALL posts.

    User 40002 has no languages set — CASE returns boost for all posts.
    """
    _setup_language_graph(neo4j)

    test_settings = HintGridSettings(
        personalized_interest_weight=1.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        language_match_weight=0.5,
        feed_size=100,
        feed_days=7,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_user_feed(neo4j, 40002, test_settings)
    rec_map = {int(r["post_id"]): r["score"] for r in recs}

    assert len(rec_map) >= 3, "User without languages should see all posts"

    # All posts should have the same score (all get the boost)
    scores = list(rec_map.values())
    for score in scores:
        assert abs(score - scores[0]) < 0.01, (
            f"All posts should have equal score for user without languages, "
            f"got scores: {rec_map}"
        )


@pytest.mark.integration
def test_zero_language_weight_disables_boost(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Setting language_match_weight=0 effectively disables the boost.

    Posts with matching and non-matching languages should have equal scores.
    """
    _setup_language_graph(neo4j)

    test_settings = HintGridSettings(
        personalized_interest_weight=1.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        language_match_weight=0.0,  # Disabled
        feed_size=100,
        feed_days=7,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_user_feed(neo4j, 40001, test_settings)
    rec_map = {int(r["post_id"]): r["score"] for r in recs}

    assert 40101 in rec_map
    assert 40102 in rec_map

    # With zero weight, matching and non-matching should be equal
    assert abs(rec_map[40101] - rec_map[40102]) < 0.01, (
        f"With zero language weight, scores should be equal: "
        f"en={rec_map[40101]}, fr={rec_map[40102]}"
    )


# ---------------------------------------------------------------------------
# Tests: Language boost in cold start
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_cold_start_applies_language_boost(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Cold start feed also applies language boost.

    User 40010 has no community (triggers cold start).
    Language boost should still affect scoring.
    """
    neo4j.label("User")
    neo4j.label("Post")

    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: 40010, isLocal: true, languages: ['en']}) "
        "CREATE (p1:__post__ {id: 40201, language: 'en', createdAt: datetime() - duration({hours: 1}), embedding: [0.1]}) "
        "CREATE (p2:__post__ {id: 40202, language: 'ja', createdAt: datetime() - duration({hours: 1}), embedding: [0.2]})",
        {"user": "User", "post": "Post"},
    )

    test_settings = HintGridSettings(
        cold_start_popularity_weight=0.0,
        cold_start_recency_weight=0.0,
        language_match_weight=1.0,  # Only language matters
        feed_size=100,
        feed_days=7,
        cold_start_limit=100,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_user_feed(neo4j, 40010, test_settings)
    rec_map = {int(r["post_id"]): r["score"] for r in recs}

    assert 40201 in rec_map, "English post should appear in cold start"
    assert 40202 in rec_map, "Japanese post should appear (boost, not filter)"

    assert rec_map[40201] > rec_map[40202], (
        f"English post ({rec_map[40201]}) should score higher than "
        f"Japanese post ({rec_map[40202]}) in cold start"
    )
