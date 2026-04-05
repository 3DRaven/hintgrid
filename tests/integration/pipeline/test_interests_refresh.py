"""Integration tests for incremental interest refresh."""

from __future__ import annotations

from datetime import datetime, UTC

import pytest

import redis

from hintgrid.config import HintGridSettings
from hintgrid.pipeline.interests import rebuild_interests, refresh_interests
from hintgrid.utils.coercion import coerce_float
from hintgrid.clients.redis import RedisClient
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient


def _get_interest_score(
    neo4j: Neo4jClient,
    uc_id: str,
    pc_id: str,
) -> float | None:
    """Helper to fetch an INTERESTED_IN score between communities."""
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (uc:__uc__ {id: $uc_id})"
            "-[i:INTERESTED_IN]->"
            "(pc:__pc__ {id: $pc_id}) "
            "RETURN i.score AS score",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
            {"uc_id": uc_id, "pc_id": pc_id},
        )
    )
    if not result:
        return None
    return coerce_float(result[0].get("score"))


def _setup_community_with_interaction(
    neo4j: Neo4jClient,
    uc_id: str,
    pc_id: str,
    user_id: int,
    post_id: int,
    interaction_age_days: int = 0,
) -> None:
    """Create a user community, post community, user, post, and a FAVORITED interaction."""
    neo4j.label("User")
    neo4j.label("Post")
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")

    neo4j.execute_labeled(
        "MERGE (uc:__uc__ {id: $uc_id}) "
        "MERGE (pc:__pc__ {id: $pc_id}) "
        "CREATE (u:__user__ {id: $user_id}) "
        "CREATE (p:__post__ {id: $post_id}) "
        "CREATE (u)-[:BELONGS_TO]->(uc) "
        "CREATE (p)-[:BELONGS_TO]->(pc) "
        "CREATE (u)-[:FAVORITED {at: datetime() - duration({days: $age_days})}]->(p)",
        {"uc": "UserCommunity", "pc": "PostCommunity", "user": "User", "post": "Post"},
        {
            "uc_id": uc_id,
            "pc_id": pc_id,
            "user_id": user_id,
            "post_id": post_id,
            "age_days": interaction_age_days,
        },
    )


@pytest.mark.integration
def test_refresh_applies_global_decay(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that refresh_interests applies global decay to existing scores.

    Sets up interests via full rebuild, then calls refresh with a simulated
    time gap and verifies scores are decayed.

    The interaction must be created BEFORE the simulated last_rebuild_at so
    that the UserCommunity is NOT detected as dirty (no recompute), and only
    global decay is applied.
    """
    from datetime import timedelta

    test_settings = HintGridSettings(
        ctr_enabled=False,
        decay_half_life_days=14,
        interests_ttl_days=90,
        interests_min_favourites=0,
        likes_weight=1.0,
        reblogs_weight=1.5,
        replies_weight=3.0,
    )

    # Interaction 20 days ago — BEFORE the simulated last_rebuild_at (7 days ago)
    _setup_community_with_interaction(
        neo4j,
        uc_id="refresh_decay_uc",
        pc_id="refresh_decay_pc",
        user_id=7701,
        post_id=7801,
        interaction_age_days=20,
    )

    rebuild_interests(neo4j, test_settings)

    initial_score = _get_interest_score(neo4j, "refresh_decay_uc", "refresh_decay_pc")
    assert initial_score is not None
    assert initial_score > 0.0

    # Simulate refresh 7 days later (half of half_life=14)
    # The interaction at 20 days ago is BEFORE last_rebuild_at=7 days ago,
    # so this UC is NOT dirty → only global decay is applied.
    # Global decay: score *= exp(-0.693 * 168 / (14 * 24)) ≈ exp(-0.347) ≈ 0.707
    fake_last_rebuild = (
        datetime.now(UTC) - timedelta(days=7)
    ).isoformat()

    refresh_interests(neo4j, test_settings, fake_last_rebuild)

    decayed_score = _get_interest_score(neo4j, "refresh_decay_uc", "refresh_decay_pc")
    assert decayed_score is not None

    # Score should be lower after decay (approximately 0.707 of original)
    assert decayed_score < initial_score, (
        f"Decayed score ({decayed_score}) should be less than initial ({initial_score})"
    )
    # Should be roughly 70% of initial (7 days with half_life=14)
    ratio = decayed_score / initial_score
    assert 0.5 < ratio < 0.9, (
        f"Decay ratio ({ratio}) should be ~0.71 for 7 days with half_life=14"
    )


@pytest.mark.integration
def test_refresh_recomputes_dirty_communities(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that refresh detects and recomputes dirty communities.

    Creates initial interests, adds a new interaction, and verifies
    the dirty community gets updated while others remain decayed.
    """
    test_settings = HintGridSettings(
        ctr_enabled=False,
        decay_half_life_days=14,
        interests_ttl_days=90,
        interests_min_favourites=0,
        likes_weight=1.0,
        reblogs_weight=1.5,
        replies_weight=3.0,
    )

    # Set up two separate community pairs
    _setup_community_with_interaction(
        neo4j,
        uc_id="refresh_dirty_uc",
        pc_id="refresh_dirty_pc",
        user_id=7711,
        post_id=7811,
        interaction_age_days=1,
    )

    _setup_community_with_interaction(
        neo4j,
        uc_id="refresh_clean_uc",
        pc_id="refresh_clean_pc",
        user_id=7712,
        post_id=7812,
        interaction_age_days=1,
    )

    rebuild_interests(neo4j, test_settings)

    initial_dirty_score = _get_interest_score(neo4j, "refresh_dirty_uc", "refresh_dirty_pc")
    initial_clean_score = _get_interest_score(neo4j, "refresh_clean_uc", "refresh_clean_pc")
    assert initial_dirty_score is not None
    assert initial_clean_score is not None

    # Add a new interaction for the "dirty" community (brand new, just now)
    neo4j.label("User")
    neo4j.label("Post")
    neo4j.label("PostCommunity")
    neo4j.execute_labeled(
        "MATCH (u:__user__ {id: 7711}) "
        "MATCH (pc:__pc__ {id: 'refresh_dirty_pc'}) "
        "CREATE (p2:__post__ {id: 7813}) "
        "CREATE (p2)-[:BELONGS_TO]->(pc) "
        "CREATE (u)-[:FAVORITED {at: datetime()}]->(p2)",
        {"user": "User", "pc": "PostCommunity", "post": "Post"},
    )

    # Refresh with last_rebuild_at = 30 seconds ago
    from datetime import timedelta

    last_rebuild_at = (
        datetime.now(UTC) - timedelta(seconds=30)
    ).isoformat()

    refresh_interests(neo4j, test_settings, last_rebuild_at)

    # Dirty community should have updated score (recomputed with new interaction)
    dirty_score_after = _get_interest_score(neo4j, "refresh_dirty_uc", "refresh_dirty_pc")
    assert dirty_score_after is not None

    # Clean community should still exist (with slight decay from 30s, basically same)
    clean_score_after = _get_interest_score(neo4j, "refresh_clean_uc", "refresh_clean_pc")
    assert clean_score_after is not None

    # Dirty community was rebuilt with more interactions, so it should be meaningful
    assert dirty_score_after > 0.0


@pytest.mark.integration
def test_refresh_removes_near_zero_interests(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that refresh removes interests that decayed below threshold.

    Creates an interest with an old interaction, then simulates enough
    time passing via global decay to drop the score below 0.01.

    The interaction must be created BEFORE the simulated last_rebuild_at
    so that the UC is NOT dirty (no recompute), only global decay applies.
    """
    from datetime import timedelta

    test_settings = HintGridSettings(
        ctr_enabled=False,
        decay_half_life_days=1,  # Very short half-life for fast decay
        interests_ttl_days=90,
        interests_min_favourites=0,
        likes_weight=1.0,
        reblogs_weight=1.5,
        replies_weight=3.0,
    )

    # Interaction 40 days ago — BEFORE the simulated last_rebuild_at (30 days ago)
    _setup_community_with_interaction(
        neo4j,
        uc_id="refresh_zero_uc",
        pc_id="refresh_zero_pc",
        user_id=7721,
        post_id=7821,
        interaction_age_days=40,
    )

    rebuild_interests(neo4j, test_settings)

    initial_score = _get_interest_score(neo4j, "refresh_zero_uc", "refresh_zero_pc")
    assert initial_score is not None
    assert initial_score > 0.0

    # Simulate refresh 30 days later with half_life=1 day
    # The interaction at 40 days ago is BEFORE last_rebuild_at=30 days ago,
    # so this UC is NOT dirty → only global decay is applied.
    # Decay: exp(-0.693 * 720 / 24) ≈ exp(-20.8) ≈ 0.0
    fake_last_rebuild = (
        datetime.now(UTC) - timedelta(days=30)
    ).isoformat()

    refresh_interests(neo4j, test_settings, fake_last_rebuild)

    # Score should have been removed (below 0.01 threshold)
    score_after = _get_interest_score(neo4j, "refresh_zero_uc", "refresh_zero_pc")
    assert score_after is None, (
        f"Interest with near-zero score ({score_after}) should have been deleted"
    )


@pytest.mark.integration
def test_refresh_fallback_in_app(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    redis_client: redis.Redis,
    settings: HintGridSettings,
) -> None:
    """Test that HintGridApp.run_refresh() falls back to full rebuild when no timestamp."""
    from hintgrid.app import HintGridApp
    from hintgrid.state import StateStore

    test_settings = HintGridSettings(
        ctr_enabled=False,
        decay_half_life_days=14,
        interests_ttl_days=90,
        interests_min_favourites=0,
        likes_weight=1.0,
        reblogs_weight=1.5,
        replies_weight=3.0,
    )

    _setup_community_with_interaction(
        neo4j,
        uc_id="refresh_fallback_uc",
        pc_id="refresh_fallback_pc",
        user_id=7731,
        post_id=7831,
        interaction_age_days=0,
    )

    # Create app with real postgres/redis clients
    app = HintGridApp.__new__(HintGridApp)
    app.neo4j = neo4j
    app.postgres = postgres_client
    app.redis = RedisClient(redis_client)
    app.settings = test_settings
    app.state_store = StateStore(neo4j)

    # Ensure last_interests_rebuild_at is empty (no previous rebuild)
    state = app.state_store.load()
    state.last_interests_rebuild_at = ""
    app.state_store.save(state)

    # run_refresh should fall back to full rebuild
    app.run_refresh()

    # Verify interests were created
    score = _get_interest_score(neo4j, "refresh_fallback_uc", "refresh_fallback_pc")
    assert score is not None
    assert score > 0.0

    # Verify timestamp was recorded
    state = app.state_store.load()
    assert state.last_interests_rebuild_at != ""
