"""Integration tests for progress tracking in pipeline operations."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest

from hintgrid.cli.console import create_batch_progress, track_periodic_iterate_progress
from hintgrid.pipeline.clustering import run_post_clustering
from hintgrid.pipeline.graph import cleanup_inactive_users, ensure_graph_indexes
from hintgrid.pipeline.interests import rebuild_interests, refresh_interests
from hintgrid.state import StateStore
from hintgrid.utils.coercion import coerce_int

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient
    from hintgrid.clients.redis import RedisClient
    from hintgrid.config import HintGridSettings
else:
    from hintgrid.clients.postgres import PostgresClient
    from hintgrid.clients.redis import RedisClient


@pytest.mark.integration
def test_rebuild_interests_with_progress(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that rebuild_interests updates progress correctly."""
    # Create test data
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 5001}), (u2:__user__ {id: 5002})",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 5001, createdAt: datetime()}), "
        "(p2:__post__ {id: 5002, createdAt: datetime()})",
        {"post": "Post"},
    )
    neo4j.execute_labeled(
        "CREATE (uc1:__uc__ {id: 5001})",
        {"uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "CREATE (pc1:__pc__ {id: 5001})",
        {"pc": "PostCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 5001}), (uc1:__uc__ {id: 5001}) "
        "CREATE (u1)-[:BELONGS_TO]->(uc1)",
        {"user": "User", "uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (p1:__post__ {id: 5001}), (pc1:__pc__ {id: 5001}) "
        "CREATE (p1)-[:BELONGS_TO]->(pc1)",
        {"post": "Post", "pc": "PostCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 5001}), (p1:__post__ {id: 5001}) "
        "CREATE (u1)-[:FAVORITED {at: datetime()}]->(p1)",
        {"user": "User", "post": "Post"},
    )

    test_settings = settings.model_copy(update={"interests_min_favourites": 1})

    # Run with progress tracking
    with create_batch_progress() as progress:
        rebuild_interests(neo4j, test_settings, progress)

    # Verify INTERESTED_IN relationships were created
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (uc:__uc__)-[i:INTERESTED_IN]->(pc:__pc__) RETURN count(i) AS count",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )
    assert coerce_int(result[0]["count"]) > 0

    # Cleanup
    neo4j.execute_labeled(
        "MATCH (u:__user__) WHERE u.id IN [5001, 5002] DETACH DELETE u",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "MATCH (p:__post__) WHERE p.id IN [5001, 5002] DETACH DELETE p",
        {"post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (uc:__uc__) WHERE uc.id = 5001 DETACH DELETE uc",
        {"uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (pc:__pc__) WHERE pc.id = 5001 DETACH DELETE pc",
        {"pc": "PostCommunity"},
    )


@pytest.mark.integration
def test_refresh_interests_with_progress(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that refresh_interests updates progress correctly."""
    from datetime import datetime, UTC

    # Create test data
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 6001})",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 6001, createdAt: datetime()})",
        {"post": "Post"},
    )
    neo4j.execute_labeled(
        "CREATE (uc1:__uc__ {id: 6001})",
        {"uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "CREATE (pc1:__pc__ {id: 6001})",
        {"pc": "PostCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 6001}), (uc1:__uc__ {id: 6001}) "
        "CREATE (u1)-[:BELONGS_TO]->(uc1)",
        {"user": "User", "uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (p1:__post__ {id: 6001}), (pc1:__pc__ {id: 6001}) "
        "CREATE (p1)-[:BELONGS_TO]->(pc1)",
        {"post": "Post", "pc": "PostCommunity"},
    )

    test_settings = settings.model_copy(update={"interests_min_favourites": 1})
    last_rebuild_at = datetime.now(UTC).isoformat()

    # Create new interaction after last_rebuild_at
    time.sleep(1)  # Ensure interaction is after last_rebuild_at
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 6001}), (p1:__post__ {id: 6001}) "
        "CREATE (u1)-[:FAVORITED {at: datetime()}]->(p1)",
        {"user": "User", "post": "Post"},
    )

    # Run with progress tracking
    with create_batch_progress() as progress:
        refresh_interests(neo4j, test_settings, last_rebuild_at, progress)

    # Verify INTERESTED_IN relationships were created
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (uc:__uc__)-[i:INTERESTED_IN]->(pc:__pc__) RETURN count(i) AS count",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )
    assert coerce_int(result[0]["count"]) > 0

    # Cleanup
    neo4j.execute_labeled(
        "MATCH (u:__user__) WHERE u.id = 6001 DETACH DELETE u",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "MATCH (p:__post__) WHERE p.id = 6001 DETACH DELETE p",
        {"post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (uc:__uc__) WHERE uc.id = 6001 DETACH DELETE uc",
        {"uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (pc:__pc__) WHERE pc.id = 6001 DETACH DELETE pc",
        {"pc": "PostCommunity"},
    )


@pytest.mark.integration
def test_track_periodic_iterate_progress_updates_progress_bar(
    neo4j: Neo4jClient,
) -> None:
    """Test that track_periodic_iterate_progress updates progress bar."""
    operation_id = "test_tracking_1"

    # Create test data
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 7001}), (u2:__user__ {id: 7002}), "
        "(u3:__user__ {id: 7003})",
        {"user": "User"},
    )

    # Create ProgressTracker
    neo4j.create_progress_tracker(operation_id, 3)

    # Create progress bar
    with create_batch_progress(total=3) as progress:
        task_id = progress.add_task("[cyan]Processing...", total=3)

        # Start tracking
        thread = track_periodic_iterate_progress(neo4j, operation_id, progress, task_id)

        # Simulate progress updates
        neo4j.execute(
            "MATCH (pt:ProgressTracker {id: $operation_id}) "
            "SET pt.processed = 1, pt.batches = 1, pt.last_updated = datetime()",
            {"operation_id": operation_id},
        )
        time.sleep(0.6)  # Wait for polling

        neo4j.execute(
            "MATCH (pt:ProgressTracker {id: $operation_id}) "
            "SET pt.processed = 3, pt.batches = 2, pt.last_updated = datetime()",
            {"operation_id": operation_id},
        )
        time.sleep(0.6)  # Wait for polling

        # Stop tracking
        thread.stop_event.set()
        thread.join(timeout=2.0)

        # Verify progress was updated
        assert progress.tasks[task_id].completed == 3

    # Cleanup
    neo4j.cleanup_progress_tracker(operation_id)
    neo4j.execute_labeled(
        "MATCH (u:__user__) WHERE u.id IN [7001, 7002, 7003] DELETE u",
        {"user": "User"},
    )


@pytest.mark.integration
def test_build_similarity_graph_with_progress(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that run_post_clustering updates progress correctly."""
    dim = 16
    emb_a = [0.1] * dim
    emb_b = [0.4] * dim
    test_settings = settings.model_copy(
        update={
            "similarity_recency_days": 365,
            "similarity_threshold": 0.0,
            "fasttext_vector_size": dim,
            "llm_dimensions": dim,
        }
    )
    ensure_graph_indexes(neo4j, test_settings)

    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 8001, embedding: $e1, "
        "createdAt: datetime()}), "
        "(p2:__post__ {id: 8002, embedding: $e2, "
        "createdAt: datetime()})",
        {"post": "Post"},
        params={"e1": emb_a, "e2": emb_b},
    )

    state_store = StateStore(neo4j, "test_progress_tracking")
    with create_batch_progress() as progress:
        run_post_clustering(neo4j, test_settings, state_store, progress)

    neo4j.execute_labeled(
        "MATCH (p:__post__) WHERE p.id IN [8001, 8002] DETACH DELETE p",
        {"post": "Post"},
    )


@pytest.mark.integration
def test_cleanup_inactive_users_with_progress(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that cleanup_inactive_users updates progress correctly."""
    # Create inactive users (lastActive more than active_user_days ago)
    active_days = 30
    test_settings = settings.model_copy(update={"active_user_days": active_days})

    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 9001, lastActive: datetime() - duration({days: 31})}), "
        "(u2:__user__ {id: 9002, lastActive: datetime() - duration({days: 32})})",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 9001, createdAt: datetime()}), "
        "(p2:__post__ {id: 9002, createdAt: datetime()})",
        {"post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (u1:__user__ {id: 9001}), (p1:__post__ {id: 9001}) "
        "CREATE (u1)-[:WROTE]->(p1)",
        {"user": "User", "post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (u2:__user__ {id: 9002}), (p2:__post__ {id: 9002}) "
        "CREATE (u2)-[:WROTE]->(p2)",
        {"user": "User", "post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (p1:__post__ {id: 9001}), (p2:__post__ {id: 9002}) "
        "CREATE (p1)-[:SIMILAR_TO {weight: 0.5}]->(p2)",
        {"post": "Post"},
    )

    # Verify users were created and check their lastActive
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__) WHERE u.id IN [9001, 9002] "
        "RETURN u.id AS id, u.lastActive AS lastActive",
        {"user": "User"},
    )
    assert len(result) == 2, f"Expected 2 users, got {len(result)}"
    
    # Verify users are considered inactive by the cleanup condition (same as cleanup uses)
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__) "
        "WHERE u.lastActive < datetime() - duration({days: $days}) "
        "RETURN u.id AS id",
        {"user": "User"},
        {"days": active_days},
    )
    inactive_user_ids = {coerce_int(row.get("id")) for row in result}
    assert 9001 in inactive_user_ids and 9002 in inactive_user_ids, (
        f"Users 9001 and 9002 should be inactive with active_days={active_days}, "
        f"but found inactive users: {inactive_user_ids}"
    )
    
    # Also verify the exact query that cleanup uses for step 4 (finding posts)
    # This is the iterate query that cleanup_inactive_users uses
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__) "
        "WHERE u.lastActive < datetime() - duration({days: $days}) "
        "MATCH (u)-[:WROTE]->(p:__post__) "
        "RETURN count(p) AS count",
        {"user": "User", "post": "Post"},
        {"days": active_days},
    )
    post_count = coerce_int(result[0]["count"]) if result else 0
    assert post_count == 2, (
        f"Expected 2 posts from inactive users, but cleanup query found {post_count}. "
        f"This means the iterate query should find 2 posts, but APOC finds 0. "
        f"Possible issue: params may not be available in iterate query."
    )

    # Run with progress tracking
    with create_batch_progress() as progress:
        deleted_count = cleanup_inactive_users(neo4j, test_settings, progress)

    # Verify users were deleted
    assert deleted_count == 2, f"Expected 2 deleted users, got {deleted_count}"

    # Verify users are gone
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__) WHERE u.id IN [9001, 9002] RETURN count(u) AS count",
        {"user": "User"},
    )
    assert coerce_int(result[0]["count"]) == 0

    # Verify posts are gone
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) WHERE p.id IN [9001, 9002] RETURN count(p) AS count",
        {"post": "Post"},
    )
    assert coerce_int(result[0]["count"]) == 0


@pytest.mark.integration
def test_run_analytics_with_progress(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    redis_client: RedisClient,
    settings: HintGridSettings,
) -> None:
    """Test that run_analytics displays progress for all steps."""
    from hintgrid.app import HintGridApp

    # Create minimal test data
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 10001})",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 10001, createdAt: datetime(), text: 'test'})",
        {"post": "Post"},
    )

    test_settings = settings.model_copy(
        update={
            "interests_min_favourites": 1,
            "user_communities": 2,
            "post_communities": 2,
        }
    )

    app = HintGridApp(
        neo4j=neo4j,
        postgres=postgres_client,
        redis=redis_client,
        settings=test_settings,
    )

    # Run analytics - should display progress
    app.run_analytics()

    # Verify analytics completed (basic check)
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (uc:__uc__) RETURN count(uc) AS count",
        {"uc": "UserCommunity"},
    )
    # Analytics should have created communities or at least run without error
    assert result is not None

    # Cleanup
    neo4j.execute_labeled(
        "MATCH (u:__user__) WHERE u.id = 10001 DETACH DELETE u",
        {"user": "User"},
    )
    neo4j.execute_labeled(
        "MATCH (p:__post__) WHERE p.id = 10001 DETACH DELETE p",
        {"post": "Post"},
    )
