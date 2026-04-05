"""Integration tests for Neo4j ProgressTracker functionality."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.utils.coercion import coerce_int

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient


@pytest.mark.integration
def test_create_progress_tracker(neo4j: Neo4jClient) -> None:
    """Test creating a ProgressTracker node."""
    operation_id = "test_operation_1"
    total = 100

    neo4j.create_progress_tracker(operation_id, total)

    # Verify tracker was created
    result = neo4j.execute_and_fetch(
        "MATCH (pt:ProgressTracker {id: $operation_id}) "
        "RETURN pt.processed AS processed, pt.batches AS batches, "
        "       pt.total AS total, pt.started_at AS started_at",
        {"operation_id": operation_id},
    )
    assert len(result) == 1
    assert coerce_int(result[0]["processed"]) == 0
    assert coerce_int(result[0]["batches"]) == 0
    assert coerce_int(result[0]["total"]) == total
    assert result[0]["started_at"] is not None

    # Cleanup
    neo4j.cleanup_progress_tracker(operation_id)


@pytest.mark.integration
def test_create_progress_tracker_without_total(neo4j: Neo4jClient) -> None:
    """Test creating a ProgressTracker node without total."""
    operation_id = "test_operation_2"

    neo4j.create_progress_tracker(operation_id, None)

    # Verify tracker was created
    result = neo4j.execute_and_fetch(
        "MATCH (pt:ProgressTracker {id: $operation_id}) "
        "RETURN pt.total AS total",
        {"operation_id": operation_id},
    )
    assert len(result) == 1
    assert result[0]["total"] is None

    # Cleanup
    neo4j.cleanup_progress_tracker(operation_id)


@pytest.mark.integration
def test_get_progress(neo4j: Neo4jClient) -> None:
    """Test getting progress from ProgressTracker."""
    operation_id = "test_operation_3"
    total = 50

    neo4j.create_progress_tracker(operation_id, total)

    # Get initial progress
    progress = neo4j.get_progress(operation_id)
    assert coerce_int(progress["processed"]) == 0
    assert coerce_int(progress["batches"]) == 0
    assert coerce_int(progress["total"]) == total

    # Update progress manually
    neo4j.execute(
        "MATCH (pt:ProgressTracker {id: $operation_id}) "
        "SET pt.processed = 25, pt.batches = 2, pt.last_updated = datetime()",
        {"operation_id": operation_id},
    )

    # Get updated progress
    progress = neo4j.get_progress(operation_id)
    assert coerce_int(progress["processed"]) == 25
    assert coerce_int(progress["batches"]) == 2

    # Cleanup
    neo4j.cleanup_progress_tracker(operation_id)


@pytest.mark.integration
def test_get_progress_not_found(neo4j: Neo4jClient) -> None:
    """Test getting progress for non-existent tracker."""
    operation_id = "non_existent_operation"

    progress = neo4j.get_progress(operation_id)
    assert progress == {}


@pytest.mark.integration
def test_cleanup_progress_tracker(neo4j: Neo4jClient) -> None:
    """Test cleaning up ProgressTracker node."""
    operation_id = "test_operation_4"

    neo4j.create_progress_tracker(operation_id, 100)

    # Verify tracker exists
    result = neo4j.execute_and_fetch(
        "MATCH (pt:ProgressTracker {id: $operation_id}) RETURN count(pt) AS count",
        {"operation_id": operation_id},
    )
    assert coerce_int(result[0]["count"]) == 1

    # Cleanup
    neo4j.cleanup_progress_tracker(operation_id)

    # Verify tracker was deleted
    result = neo4j.execute_and_fetch(
        "MATCH (pt:ProgressTracker {id: $operation_id}) RETURN count(pt) AS count",
        {"operation_id": operation_id},
    )
    assert coerce_int(result[0]["count"]) == 0


@pytest.mark.integration
def test_progress_tracker_updates_during_iterate(neo4j: Neo4jClient) -> None:
    """Test that ProgressTracker is updated during apoc.periodic.iterate."""
    operation_id = "test_operation_5"

    # Create test data
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 1001}), (u2:__user__ {id: 1002}), "
        "(u3:__user__ {id: 1003}), (u4:__user__ {id: 1004}), (u5:__user__ {id: 1005})",
        {"user": "User"},
    )

    # Create ProgressTracker
    neo4j.create_progress_tracker(operation_id, 5)

    # Run periodic iterate with progress tracking
    result = neo4j.execute_periodic_iterate(
        "MATCH (u:__user__) WHERE u.id IN [1001, 1002, 1003, 1004, 1005] RETURN id(u) AS user_id",
        "UNWIND $_batch AS row "
        "MATCH (u:__user__) WHERE id(u) = row.user_id SET u.processed = true",
        label_map={"user": "User"},
        batch_size=2,  # Small batch size to ensure multiple batches
        batch_mode="BATCH",
        progress_tracker_id=operation_id,
    )

    # Verify iterate completed
    assert coerce_int(result.get("total", 0)) == 5

    # Verify ProgressTracker was updated
    progress = neo4j.get_progress(operation_id)
    assert coerce_int(progress["processed"]) == 5
    assert coerce_int(progress["batches"]) > 0

    # Cleanup
    neo4j.cleanup_progress_tracker(operation_id)
    neo4j.execute_labeled(
        "MATCH (u:__user__) WHERE u.id IN [1001, 1002, 1003, 1004, 1005] DELETE u",
        {"user": "User"},
    )


@pytest.mark.integration
def test_progress_tracker_multiple_operations(neo4j: Neo4jClient) -> None:
    """Test that multiple ProgressTracker operations can run concurrently."""
    operation_id_1 = "test_operation_6"
    operation_id_2 = "test_operation_7"

    # Create two trackers
    neo4j.create_progress_tracker(operation_id_1, 10)
    neo4j.create_progress_tracker(operation_id_2, 20)

    # Update them independently
    neo4j.execute(
        "MATCH (pt:ProgressTracker {id: $operation_id}) "
        "SET pt.processed = 5, pt.batches = 1, pt.last_updated = datetime()",
        {"operation_id": operation_id_1},
    )
    neo4j.execute(
        "MATCH (pt:ProgressTracker {id: $operation_id}) "
        "SET pt.processed = 15, pt.batches = 2, pt.last_updated = datetime()",
        {"operation_id": operation_id_2},
    )

    # Verify they are independent
    progress_1 = neo4j.get_progress(operation_id_1)
    progress_2 = neo4j.get_progress(operation_id_2)

    assert coerce_int(progress_1["processed"]) == 5
    assert coerce_int(progress_1["batches"]) == 1
    assert coerce_int(progress_2["processed"]) == 15
    assert coerce_int(progress_2["batches"]) == 2

    # Cleanup
    neo4j.cleanup_progress_tracker(operation_id_1)
    neo4j.cleanup_progress_tracker(operation_id_2)

    # Verify both are deleted
    result = neo4j.execute_and_fetch(
        "MATCH (pt:ProgressTracker) "
        "WHERE pt.id IN [$id1, $id2] RETURN count(pt) AS count",
        {"id1": operation_id_1, "id2": operation_id_2},
    )
    assert coerce_int(result[0]["count"]) == 0
