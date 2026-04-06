"""Integration tests for soft PostCommunity diversification (share_i, norm_pc_size)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.config import HintGridSettings
from hintgrid.pipeline.feed import get_detailed_recommendations
from hintgrid.pipeline.feed_personalized_queries import public_feed_interest_weight

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient


def _seed_equal_interest_two_pc(neo4j: Neo4jClient) -> None:
    """One user with two INTERESTED_IN edges (same i.score); two posts; different pc.size."""
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")
    neo4j.label("Post")
    neo4j.execute_labeled(
        "CREATE (u:__user__ {id: 93001})\n"
        "CREATE (uc:__uc__ {id: 'uc_soft_div'})\n"
        "CREATE (a:__pc__ {id: 'pc_large', size: 1000})\n"
        "CREATE (b:__pc__ {id: 'pc_small', size: 100})\n"
        "CREATE (u)-[:BELONGS_TO]->(uc)\n"
        "CREATE (uc)-[:INTERESTED_IN {score: 1.0}]->(a)\n"
        "CREATE (uc)-[:INTERESTED_IN {score: 1.0}]->(b)\n"
        "CREATE (pa:__post__ {\n"
        "  id: 94001,\n"
        "  createdAt: datetime() - duration({hours: 1}),\n"
        "  embedding: [0.1],\n"
        "  authorId: 1\n"
        "})-[:BELONGS_TO]->(a)\n"
        "CREATE (pb:__post__ {\n"
        "  id: 94002,\n"
        "  createdAt: datetime() - duration({hours: 1}),\n"
        "  embedding: [0.1],\n"
        "  authorId: 1\n"
        "})-[:BELONGS_TO]->(b)",
        {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity", "post": "Post"},
    )


@pytest.mark.integration
def test_two_equal_interest_scores_yield_equal_share_i(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """When sum_i = 2.0 and both i.score = 1.0, share_i = 0.5 for each candidate row."""
    _seed_equal_interest_two_pc(neo4j)
    test_settings = HintGridSettings(
        feed_size=10,
        feed_days=7,
        feed_pc_share_weight=0.5,
        feed_pc_size_weight=0.5,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        pagerank_enabled=False,
        language_match_weight=0.0,
        ui_language_match_weight=0.0,
        popularity_smoothing=1.0,
        recency_smoothing=1.0,
        recency_numerator=1.0,
        neo4j_worker_label=settings.neo4j_worker_label,
        noise_community_id=settings.noise_community_id,
    )
    rows = get_detailed_recommendations(neo4j, 93001, test_settings)
    assert len(rows) == 2
    by_id = {r["post_id"]: r for r in rows}
    assert by_id[94001]["share_i"] == pytest.approx(0.5)
    assert by_id[94002]["share_i"] == pytest.approx(0.5)
    assert by_id[94001]["norm_pc_size"] == pytest.approx(1.0)
    assert by_id[94002]["norm_pc_size"] == pytest.approx(0.1)


@pytest.mark.integration
def test_norm_pc_size_breaks_tie_toward_larger_pc_when_share_equal(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Larger PC gets higher final_score when share_i ties and size weight is positive."""
    _seed_equal_interest_two_pc(neo4j)
    test_settings = HintGridSettings(
        feed_size=10,
        feed_days=7,
        feed_pc_share_weight=0.5,
        feed_pc_size_weight=0.5,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=0.0,
        pagerank_enabled=False,
        language_match_weight=0.0,
        ui_language_match_weight=0.0,
        popularity_smoothing=1.0,
        recency_smoothing=1.0,
        recency_numerator=1.0,
        neo4j_worker_label=settings.neo4j_worker_label,
        noise_community_id=settings.noise_community_id,
    )
    rows = get_detailed_recommendations(neo4j, 93001, test_settings)
    assert rows[0]["post_id"] == 94001
    assert rows[0]["final_score"] >= rows[1]["final_score"]


def test_public_feed_interest_weight_sums_pc_weights() -> None:
    """Public timeline uses scalar weight = feed_pc_share_weight + feed_pc_size_weight."""
    s = HintGridSettings(
        feed_pc_share_weight=0.45,
        feed_pc_size_weight=0.05,
        personalized_popularity_weight=0.3,
        personalized_recency_weight=0.2,
    )
    assert public_feed_interest_weight(s) == pytest.approx(0.5)
