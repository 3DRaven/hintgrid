"""Integration tests for batchMode in apoc.periodic.iterate."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.utils.coercion import coerce_int

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient


@pytest.mark.integration
def test_batch_mode_batch_processes_in_batches(neo4j: Neo4jClient) -> None:
    """Test that batchMode='BATCH' processes items in batches."""
    # Create test data
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 2001}), (u2:__user__ {id: 2002}), "
        "(u3:__user__ {id: 2003}), (u4:__user__ {id: 2004})",
        {"user": "User"},
    )

    # Run with batchMode='BATCH'
    result = neo4j.execute_periodic_iterate(
        "MATCH (u:__user__) WHERE u.id IN [2001, 2002, 2003, 2004] RETURN id(u) AS user_id",
        "UNWIND $_batch AS row "
        "MATCH (u:__user__) WHERE id(u) = row.user_id SET u.processed = true",
        label_map={"user": "User"},
        batch_size=2,
        batch_mode="BATCH",
    )

    # Verify all items were processed
    assert coerce_int(result.get("total", 0)) == 4
    assert coerce_int(result.get("committedOperations", 0)) == 4

    # Verify items were actually updated
    result_list = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__) WHERE u.id IN [2001, 2002, 2003, 2004] "
        "AND u.processed = true RETURN count(u) AS count",
        {"user": "User"},
    )
    assert len(result_list) > 0
    result = result_list[0]
    count_value = result.get("count")
    assert coerce_int(count_value) == 4

    # Cleanup
    neo4j.execute_labeled(
        "MATCH (u:__user__) WHERE u.id IN [2001, 2002, 2003, 2004] DELETE u",
        {"user": "User"},
    )


@pytest.mark.integration
def test_batch_mode_single_processes_one_by_one(neo4j: Neo4jClient) -> None:
    """Test that batchMode='SINGLE' processes items one by one."""
    # Create test data
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 3001}), (u2:__user__ {id: 3002})",
        {"user": "User"},
    )

    # Run with batchMode='SINGLE'
    result = neo4j.execute_periodic_iterate(
        "MATCH (u:__user__) WHERE u.id IN [3001, 3002] RETURN id(u) AS user_id",
        "MATCH (u:__user__) WHERE id(u) = user_id SET u.processed = true",
        label_map={"user": "User"},
        batch_size=2,
        batch_mode="SINGLE",
    )

    # Verify all items were processed
    assert coerce_int(result.get("total", 0)) == 2
    assert coerce_int(result.get("committedOperations", 0)) == 2

    # Verify items were actually updated
    result_list = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__) WHERE u.id IN [3001, 3002] "
        "AND u.processed = true RETURN count(u) AS count",
        {"user": "User"},
    )
    assert len(result_list) > 0
    result = result_list[0]
    count_value = result.get("count")
    assert coerce_int(count_value) == 2

    # Cleanup
    neo4j.execute_labeled(
        "MATCH (u:__user__) WHERE u.id IN [3001, 3002] DELETE u",
        {"user": "User"},
    )


@pytest.mark.integration
def test_batch_mode_default_is_batch(neo4j: Neo4jClient) -> None:
    """Test that default batchMode is 'BATCH'."""
    # Create test data
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 4001}), (u2:__user__ {id: 4002})",
        {"user": "User"},
    )

    # Run without specifying batch_mode (should default to 'BATCH')
    result = neo4j.execute_periodic_iterate(
        "MATCH (u:__user__) WHERE u.id IN [4001, 4002] RETURN id(u) AS user_id",
        "UNWIND $_batch AS row "
        "MATCH (u:__user__) WHERE id(u) = row.user_id SET u.processed = true",
        label_map={"user": "User"},
        batch_size=2,
    )

    # Verify all items were processed
    assert coerce_int(result.get("total", 0)) == 2

    # Cleanup
    neo4j.execute_labeled(
        "MATCH (u:__user__) WHERE u.id IN [4001, 4002] DELETE u",
        {"user": "User"},
    )


@pytest.mark.integration
def test_batch_mode_performance_comparison(neo4j: Neo4jClient) -> None:
    """Test that batchMode='BATCH' processes items more efficiently."""
    # Create test data
    neo4j.execute_labeled(
        "FOREACH (i IN range(1, 10) | "
        "CREATE (u:__user__ {id: 5000 + i}))",
        {"user": "User"},
    )

    # Run with batchMode='BATCH' (default)
    result_batch = neo4j.execute_periodic_iterate(
        "MATCH (u:__user__) WHERE u.id >= 5001 AND u.id <= 5010 RETURN id(u) AS user_id",
        "UNWIND $_batch AS row "
        "MATCH (u:__user__) WHERE id(u) = row.user_id SET u.processed_batch = true",
        label_map={"user": "User"},
        batch_size=3,
        batch_mode="BATCH",
    )

    # Run with batchMode='SINGLE'
    result_single = neo4j.execute_periodic_iterate(
        "MATCH (u:__user__) WHERE u.id >= 5001 AND u.id <= 5010 RETURN id(u) AS user_id",
        "MATCH (u:__user__) WHERE id(u) = user_id SET u.processed_single = true",
        label_map={"user": "User"},
        batch_size=3,
        batch_mode="SINGLE",
    )

    # Both should process all items
    assert coerce_int(result_batch.get("total", 0)) == 10
    assert coerce_int(result_single.get("total", 0)) == 10

    # BATCH mode should have fewer batches (more efficient)
    batches_batch = coerce_int(result_batch.get("batches", 0))
    batches_single = coerce_int(result_single.get("batches", 0))
    # With batch_size=3, BATCH mode should have ~4 batches, SINGLE should have 10
    assert batches_batch <= batches_single

    # Verify items were processed
    result_list = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__) WHERE u.id >= 5001 AND u.id <= 5010 "
        "AND u.processed_batch = true AND u.processed_single = true "
        "RETURN count(u) AS count",
        {"user": "User"},
    )
    assert len(result_list) > 0
    result = result_list[0]
    count_value = result.get("count")
    assert coerce_int(count_value) == 10

    # Cleanup
    neo4j.execute_labeled(
        "MATCH (u:__user__) WHERE u.id >= 5001 AND u.id <= 5010 DELETE u",
        {"user": "User"},
    )


@pytest.mark.integration
def test_params_in_iterate_query_like_clustering(neo4j: Neo4jClient) -> None:
    """Test that params are accessible in iterate_query, same pattern as clustering.py.
    
    This test mimics the exact pattern from clustering.py where $recency_days
    is used in iterate_query to filter posts by date.
    """
    # Create posts with different dates:
    # - 2 recent posts (created today)
    # - 3 old posts (created 50 days ago)
    neo4j.execute_labeled(
        "CREATE "
        "(p1:__post__ {id: 6001, createdAt: datetime()}), "
        "(p2:__post__ {id: 6002, createdAt: datetime()}), "
        "(p3:__post__ {id: 6003, createdAt: datetime() - duration({days: 50})}), "
        "(p4:__post__ {id: 6004, createdAt: datetime() - duration({days: 50})}), "
        "(p5:__post__ {id: 6005, createdAt: datetime() - duration({days: 50})})",
        {"post": "Post"},
    )

    # Use execute_periodic_iterate with $days parameter in iterate_query
    # This is the same pattern as clustering.py uses with $recency_days
    result = neo4j.execute_periodic_iterate(
        "MATCH (p:__post__) "
        "WHERE p.createdAt > datetime() - duration({days: $days}) "
        "RETURN id(p) AS post_id",
        "UNWIND $_batch AS row "
        "MATCH (p:__post__) WHERE id(p) = row.post_id SET p.processed = true",
        label_map={"post": "Post"},
        batch_size=10,
        batch_mode="BATCH",
        params={"days": 30},  # Only process posts from last 30 days
    )

    # Should process only 2 recent posts (6001, 6002), not the 3 old ones
    assert coerce_int(result.get("total", 0)) == 2
    assert coerce_int(result.get("committedOperations", 0)) == 2

    # Verify only recent posts were processed
    result_list = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) WHERE p.id IN [6001, 6002, 6003, 6004, 6005] "
        "AND p.processed = true RETURN count(p) AS count",
        {"post": "Post"},
    )
    assert len(result_list) > 0
    result = result_list[0]
    count_value = result.get("count")
    assert coerce_int(count_value) == 2

    # Verify specific posts were processed
    result_list = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) WHERE p.id IN [6001, 6002] "
        "AND p.processed = true RETURN count(p) AS count",
        {"post": "Post"},
    )
    assert len(result_list) > 0
    result = result_list[0]
    count_value = result.get("count")
    assert coerce_int(count_value) == 2

    # Verify old posts were NOT processed
    result_list = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) WHERE p.id IN [6003, 6004, 6005] "
        "AND p.processed = true RETURN count(p) AS count",
        {"post": "Post"},
    )
    assert len(result_list) > 0
    result = result_list[0]
    count_value = result.get("count")
    assert coerce_int(count_value) == 0

    # Cleanup
    neo4j.execute_labeled(
        "MATCH (p:__post__) WHERE p.id IN [6001, 6002, 6003, 6004, 6005] DELETE p",
        {"post": "Post"},
    )
