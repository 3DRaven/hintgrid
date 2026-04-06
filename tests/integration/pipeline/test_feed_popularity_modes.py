"""Integration tests for feed popularity modes (local/global/blended) and rel_types."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.config import HintGridSettings
from hintgrid.pipeline.feed import generate_user_feed
from hintgrid.pipeline.feed_queries import (
    build_local_raw_expression,
    build_popularity_contrib_expr,
)

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient


def _seed_post_with_edges(neo4j: Neo4jClient) -> None:
    """One post with two FAVORITED edges and global counters on Post; viewer without interests."""
    neo4j.label("User")
    neo4j.label("Post")
    neo4j.execute_labeled(
        "CREATE (viewer:__user__ {id: 92003})\n"
        "CREATE (u1:__user__ {id: 92001})\n"
        "CREATE (u2:__user__ {id: 92002})\n"
        "CREATE (p:__post__ {\n"
        "  id: 92099,\n"
        "  createdAt: datetime() - duration({hours: 1}),\n"
        "  embedding: [0.1],\n"
        "  totalFavourites: 100,\n"
        "  totalReblogs: 0,\n"
        "  totalReplies: 0\n"
        "})\n"
        "CREATE (u1)-[:FAVORITED]->(p)\n"
        "CREATE (u2)-[:FAVORITED]->(p)",
        {"user": "User", "post": "Post"},
    )


@pytest.mark.integration
def test_generate_user_feed_global_mode_uses_post_counters(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Global mode: popularity_contrib from Post totals dominates when local graph is sparse."""
    _seed_post_with_edges(neo4j)
    test_settings = HintGridSettings(
        feed_size=5,
        feed_days=7,
        feed_popularity_mode="global",
        cold_start_limit=5,
        feed_pc_share_weight=0.0,
        feed_pc_size_weight=0.0,
        personalized_popularity_weight=1.0,
        personalized_recency_weight=0.0,
        cold_start_popularity_weight=1.0,
        cold_start_recency_weight=0.0,
        pagerank_enabled=False,
        global_popularity_favourites_weight=1.0,
        global_popularity_reblogs_weight=0.0,
        global_popularity_replies_weight=0.0,
        global_popularity_smoothing=1.0,
        popularity_smoothing=1.0,
        neo4j_worker_label=settings.neo4j_worker_label,
        noise_community_id=settings.noise_community_id,
    )
    rows = generate_user_feed(neo4j, 92003, test_settings, rel_types=None)
    assert len(rows) >= 1
    post_ids = {int(r["post_id"]) for r in rows}
    assert 92099 in post_ids


@pytest.mark.unit
def test_local_raw_expression_omits_disabled_rel_type() -> None:
    """When FAVORITED is excluded from rel_types, local_raw expr has no FAVORITED term."""
    expr = build_local_raw_expression(frozenset({"REBLOGGED", "REPLIED", "BOOKMARKED"}))
    assert "FAVORITED" not in expr
    assert "REBLOGGED" in expr


@pytest.mark.unit
def test_popularity_contrib_expr_modes_differ() -> None:
    """build_popularity_contrib_expr reflects feed_popularity_mode."""
    base = HintGridSettings()
    loc = build_popularity_contrib_expr(base.model_copy(update={"feed_popularity_mode": "local"}))
    glb = build_popularity_contrib_expr(base.model_copy(update={"feed_popularity_mode": "global"}))
    bld = build_popularity_contrib_expr(base.model_copy(update={"feed_popularity_mode": "blended"}))
    assert "local_raw" in loc
    assert "global_raw" in glb
    assert "blend_local" in bld and "blend_global" in bld
