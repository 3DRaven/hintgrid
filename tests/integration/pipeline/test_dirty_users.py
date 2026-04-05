"""Integration tests for dirty-user detection, recency scoring, and indexes.

Covers:
- stream_dirty_user_ids selective filtering logic
- Hourly recency scoring precision in feed generation
- User.feedGeneratedAt index creation
"""

from __future__ import annotations

from typing import cast, TYPE_CHECKING

import pytest

from hintgrid.config import HintGridSettings
from hintgrid.pipeline.feed import generate_user_feed
from hintgrid.pipeline.graph import ensure_graph_indexes

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient


# ============================================================================
# Tests: stream_dirty_user_ids
# ============================================================================


@pytest.mark.integration
def test_dirty_users_no_feed_generated_at(
    neo4j: Neo4jClient,
) -> None:
    """Users without feedGeneratedAt are always considered dirty."""
    neo4j.label("User")

    # Create local users without feedGeneratedAt (they should be dirty)
    for uid in [20001, 20002, 20003]:
        neo4j.execute_labeled(
            "CREATE (:__user__ {id: $uid, isLocal: true})",
            {"user": "User"},
            {"uid": uid},
        )

    dirty_ids = list(neo4j.stream_dirty_user_ids(active_days=30, feed_size=100))

    assert 20001 in dirty_ids
    assert 20002 in dirty_ids
    assert 20003 in dirty_ids


@pytest.mark.integration
def test_dirty_users_recent_feed_not_dirty(
    neo4j: Neo4jClient,
) -> None:
    """Users with fresh feedGeneratedAt and no changes are NOT dirty."""
    neo4j.label("User")

    # Create user with recent feedGeneratedAt and no graph changes
    neo4j.execute_labeled(
        "CREATE (:__user__ {id: $uid, feedGeneratedAt: datetime()})",
        {"user": "User"},
        {"uid": 20010},
    )

    dirty_ids = list(neo4j.stream_dirty_user_ids(active_days=30, feed_size=100))

    assert 20010 not in dirty_ids


@pytest.mark.integration
def test_dirty_users_new_posts_in_community(
    neo4j: Neo4jClient,
) -> None:
    """User becomes dirty when new posts appear in their PostCommunity."""
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")
    neo4j.label("Post")

    # Create user with feedGeneratedAt set to 1 hour ago
    # Must be isLocal = true and have lastActive to be considered active
    neo4j.execute_labeled(
        """
        CREATE (u:__user__ {
            id: $uid,
            isLocal: true,
            lastActive: datetime(),
            feedGeneratedAt: datetime() - duration({hours: 1})
        })
        CREATE (uc:__uc__ {id: 'dirty_uc_1'})
        CREATE (pc:__pc__ {id: 'dirty_pc_1'})
        CREATE (u)-[:BELONGS_TO]->(uc)
        CREATE (uc)-[:INTERESTED_IN {
            score: 0.8,
            last_updated: datetime() - duration({hours: 2})
        }]->(pc)
        """,
        {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity"},
        {"uid": 20020},
    )

    # Add a NEW post (createdAt = now, which is AFTER feedGeneratedAt)
    neo4j.execute_labeled(
        """
        MATCH (pc:__pc__ {id: 'dirty_pc_1'})
        CREATE (p:__post__ {id: 90001, createdAt: datetime()})
        CREATE (p)-[:BELONGS_TO]->(pc)
        """,
        {"pc": "PostCommunity", "post": "Post"},
    )

    dirty_ids = list(neo4j.stream_dirty_user_ids(active_days=30, feed_size=100))

    assert 20020 in dirty_ids


@pytest.mark.integration
def test_dirty_users_updated_interests(
    neo4j: Neo4jClient,
) -> None:
    """User becomes dirty when INTERESTED_IN is updated after feedGeneratedAt."""
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")

    # Create user with feedGeneratedAt set to 1 hour ago
    # Must be isLocal = true and have lastActive to be considered active
    neo4j.execute_labeled(
        """
        CREATE (u:__user__ {
            id: $uid,
            isLocal: true,
            lastActive: datetime(),
            feedGeneratedAt: datetime() - duration({hours: 1})
        })
        CREATE (uc:__uc__ {id: 'dirty_uc_2'})
        CREATE (pc:__pc__ {id: 'dirty_pc_2'})
        CREATE (u)-[:BELONGS_TO]->(uc)
        """,
        {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity"},
        {"uid": 20030},
    )

    # Create INTERESTED_IN with last_updated = now (AFTER feedGeneratedAt)
    neo4j.execute_labeled(
        """
        MATCH (uc:__uc__ {id: 'dirty_uc_2'})
        MATCH (pc:__pc__ {id: 'dirty_pc_2'})
        CREATE (uc)-[:INTERESTED_IN {
            score: 0.7,
            last_updated: datetime()
        }]->(pc)
        """,
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )

    dirty_ids = list(neo4j.stream_dirty_user_ids(active_days=30, feed_size=100))

    assert 20030 in dirty_ids


@pytest.mark.integration
def test_dirty_users_high_consumption(
    neo4j: Neo4jClient,
) -> None:
    """User becomes dirty when they consumed >= 80% of their feed."""
    neo4j.label("User")
    neo4j.label("Post")

    feed_size = 10
    consumption_threshold = int(feed_size * 0.8)  # 8

    # Create user with feedGeneratedAt set to 1 hour ago
    # Must be isLocal = true and have lastActive to be considered active
    neo4j.execute_labeled(
        """
        CREATE (:__user__ {
            id: $uid,
            isLocal: true,
            lastActive: datetime(),
            feedGeneratedAt: datetime() - duration({hours: 1})
        })
        """,
        {"user": "User"},
        {"uid": 20040},
    )

    # Create posts and WAS_RECOMMENDED to simulate consumption
    for i in range(consumption_threshold):
        neo4j.execute_labeled(
            "CREATE (p:__post__ {id: $pid, createdAt: datetime() - duration({hours: 2})})",
            {"post": "Post"},
            {"pid": 90100 + i},
        )
        neo4j.execute_labeled(
            """
            MATCH (u:__user__ {id: $uid})
            MATCH (p:__post__ {id: $pid})
            CREATE (u)-[:WAS_RECOMMENDED {at: datetime()}]->(p)
            """,
            {"user": "User", "post": "Post"},
            {"uid": 20040, "pid": 90100 + i},
        )

    dirty_ids = list(
        neo4j.stream_dirty_user_ids(active_days=30, feed_size=feed_size)
    )

    assert 20040 in dirty_ids


@pytest.mark.integration
def test_dirty_users_inactive_excluded(
    neo4j: Neo4jClient,
) -> None:
    """Inactive users (lastActive too old) are NOT considered dirty."""
    neo4j.label("User")

    # Create user whose lastActive is 60 days ago (outside 30-day window)
    neo4j.execute_labeled(
        "CREATE (:__user__ {id: $uid, lastActive: datetime() - duration({days: 60})})",
        {"user": "User"},
        {"uid": 20050},
    )

    dirty_ids = list(neo4j.stream_dirty_user_ids(active_days=30, feed_size=100))

    assert 20050 not in dirty_ids


@pytest.mark.integration
def test_dirty_users_ordered_by_id(
    neo4j: Neo4jClient,
) -> None:
    """stream_dirty_user_ids returns user IDs in ascending order."""
    neo4j.label("User")

    # Create users in non-sequential order (without feedGeneratedAt → all dirty)
    for uid in [20063, 20061, 20062]:
        neo4j.execute_labeled(
            "CREATE (:__user__ {id: $uid})",
            {"user": "User"},
            {"uid": uid},
        )

    dirty_ids = list(neo4j.stream_dirty_user_ids(active_days=30, feed_size=100))

    # Filter to only our test IDs
    test_ids = [uid for uid in dirty_ids if uid in {20061, 20062, 20063}]

    assert test_ids == sorted(test_ids)


# ============================================================================
# Tests: Recency scoring precision (hours)
# ============================================================================


@pytest.mark.integration
def test_recency_scoring_uses_hours(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Feed generation uses hour-based recency, distinguishing sub-day posts.

    Two posts created at different hours on the same day should
    receive different recency scores.
    """
    neo4j.label("User")
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")
    neo4j.label("Post")

    # Set up user with community graph
    neo4j.execute_labeled(
        """
        CREATE (u:__user__ {id: $uid})
        CREATE (uc:__uc__ {id: 'recency_uc'})
        CREATE (pc:__pc__ {id: 'recency_pc'})
        CREATE (u)-[:BELONGS_TO]->(uc)
        CREATE (uc)-[:INTERESTED_IN {score: 1.0}]->(pc)
        """,
        {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity"},
        {"uid": 30001},
    )

    # Create a post from 1 hour ago and one from 12 hours ago
    neo4j.execute_labeled(
        """
        MATCH (pc:__pc__ {id: 'recency_pc'})
        CREATE (p1:__post__ {
            id: 91001,
            createdAt: datetime() - duration({hours: 1})
        })-[:BELONGS_TO]->(pc)
        CREATE (p2:__post__ {
            id: 91002,
            createdAt: datetime() - duration({hours: 12})
        })-[:BELONGS_TO]->(pc)
        """,
        {"pc": "PostCommunity", "post": "Post"},
    )

    test_settings = HintGridSettings(
        personalized_interest_weight=0.0,
        personalized_popularity_weight=0.0,
        personalized_recency_weight=1.0,
        recency_numerator=24.0,
        recency_smoothing=1,
        feed_size=10,
        feed_days=7,
        neo4j_worker_label=settings.neo4j_worker_label,
        pagerank_enabled=False,
    )

    recs = generate_user_feed(neo4j, 30001, test_settings)

    assert len(recs) == 2

    # Find scores for each post
    score_map = {int(r["post_id"]): r["score"] for r in recs}
    score_1h = score_map[91001]
    score_12h = score_map[91002]

    # Post from 1 hour ago should score higher than post from 12 hours ago
    assert score_1h > score_12h


# ============================================================================
# Tests: User.feedGeneratedAt index
# ============================================================================


@pytest.mark.integration
def test_feed_generated_at_index_created(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """ensure_graph_indexes creates index on User.feedGeneratedAt.

    Indexes use the base label (e.g. ``User``), not the composite
    ``User:worker_gw0``.  A global index on ``:User`` covers nodes
    with additional worker labels used for test isolation.
    """
    ensure_graph_indexes(neo4j, settings)

    rows = list(
        neo4j.execute_and_fetch(
            "SHOW INDEXES YIELD name, properties "
            "RETURN name, properties"
        )
    )

    # Find index whose name contains "feed_generated_at"
    # or whose properties include "feedGeneratedAt".
    feed_index_found = False
    for row in rows:
        name = str(row.get("name", ""))
        raw_props = row.get("properties")
        if "feed_generated_at" in name:
            feed_index_found = True
            break
        if raw_props is not None:
            props = cast("list[str]", raw_props)
            if "feedGeneratedAt" in props:
                feed_index_found = True
                break

    assert feed_index_found, (
        "Index on User.feedGeneratedAt should exist "
        f"after ensure_graph_indexes. "
        f"Found indexes: {[str(r.get('name', '')) for r in rows]}"
    )
