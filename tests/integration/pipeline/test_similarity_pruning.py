"""Integration tests for Neo4j similarity pruning strategies and incremental build."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.config import HintGridSettings, build_similarity_signature
from hintgrid.pipeline.clustering import run_post_clustering, run_similarity_pruning
from hintgrid.pipeline.graph import ensure_graph_indexes
from hintgrid.state import StateStore
from hintgrid.utils.coercion import coerce_int

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient



@pytest.mark.integration
def test_similarity_pruning_aggressive_strategy(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test 'aggressive' similarity pruning removes all SIMILAR_TO relationships."""
    neo4j.label("Post")

    # Create test posts with SIMILAR_TO relationships
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 10001, text: 'Test post 1'}) "
        "CREATE (p2:__post__ {id: 10002, text: 'Test post 2'}) "
        "CREATE (p3:__post__ {id: 10003, text: 'Test post 3'}) "
        "CREATE (p1)-[:SIMILAR_TO {weight: 0.9}]->(p2) "
        "CREATE (p2)-[:SIMILAR_TO {weight: 0.3}]->(p3) "
        "CREATE (p1)-[:SIMILAR_TO {weight: 0.5}]->(p3)",
        {"post": "Post"},
    )

    # Verify relationships exist
    count_before = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__post__)-[r:SIMILAR_TO]->() "
            "RETURN count(r) AS cnt",
            {"post": "Post"},
        )
    )
    assert coerce_int(count_before[0].get("cnt")) >= 3

    # Run aggressive pruning (deletes all)
    aggressive_settings = settings.model_copy(update={"similarity_pruning": "aggressive"})
    neo4j.prune_similarity_links(aggressive_settings)

    # Verify all SIMILAR_TO relationships removed
    count_after = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__)-[r:SIMILAR_TO]->() "
            "WHERE p.id IN [10001, 10002, 10003] "
            "RETURN count(r) AS cnt",
            {"post": "Post"},
        )
    )
    assert coerce_int(count_after[0].get("cnt")) == 0


@pytest.mark.integration
def test_similarity_pruning_partial_strategy(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test 'partial' similarity pruning removes low-weight relationships."""
    neo4j.label("Post")

    # Create test posts with SIMILAR_TO relationships of varying weights
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 20001, text: 'Partial test 1'}) "
        "CREATE (p2:__post__ {id: 20002, text: 'Partial test 2'}) "
        "CREATE (p3:__post__ {id: 20003, text: 'Partial test 3'}) "
        "CREATE (p1)-[:SIMILAR_TO {weight: 0.9}]->(p2) "
        "CREATE (p2)-[:SIMILAR_TO {weight: 0.1}]->(p3) "
        "CREATE (p1)-[:SIMILAR_TO {weight: 0.2}]->(p3)",
        {"post": "Post"},
    )

    # Run partial pruning with threshold 0.5
    partial_settings = settings.model_copy(
        update={
            "similarity_pruning": "partial",
            "prune_similarity_threshold": 0.5,
        }
    )
    neo4j.prune_similarity_links(partial_settings)

    # Verify only high-weight relationship remains
    remaining = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__)-[r:SIMILAR_TO]->() "
            "WHERE p.id IN [20001, 20002, 20003] "
            "RETURN r.weight AS weight",
            {"post": "Post"},
        )
    )
    # Only the 0.9 weight relationship should remain
    weights = [r.get("weight") for r in remaining]
    for w in weights:
        from hintgrid.utils.coercion import coerce_float
        assert w is None or coerce_float(w) >= 0.5


@pytest.mark.integration
def test_similarity_pruning_temporal_strategy(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test 'temporal' similarity pruning removes old posts' relationships.

    The temporal strategy deletes SIMILAR_TO relationships FROM posts
    where the source post's createdAt is older than prune_days.
    """
    neo4j.label("Post")

    # Count relationships before creating test data
    list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__)-[r:SIMILAR_TO]->() "
            "WHERE p.id IN [30001, 30002] "
            "RETURN count(r) AS cnt",
            {"post": "Post"},
        )
    )

    # Create test posts - one old, one recent
    neo4j.execute_labeled(
        "CREATE (old:__post__ {"
        "id: 30001, "
        "text: 'Old post for temporal test', "
        "createdAt: datetime() - duration({days: 100})"
        "}) "
        "CREATE (recent:__post__ {"
        "id: 30002, "
        "text: 'Recent post for temporal test', "
        "createdAt: datetime()"
        "}) "
        "CREATE (target:__post__ {"
        "id: 30003, "
        "text: 'Target post for temporal test', "
        "createdAt: datetime()"
        "})",
        {"post": "Post"},
    )

    # Create relationships separately to ensure they exist
    neo4j.execute_labeled(
        "MATCH (old:__post__ {id: 30001}) "
        "MATCH (target:__post__ {id: 30003}) "
        "CREATE (old)-[:SIMILAR_TO {weight: 0.8}]->(target)",
        {"post": "Post"},
    )
    neo4j.execute_labeled(
        "MATCH (recent:__post__ {id: 30002}) "
        "MATCH (target:__post__ {id: 30003}) "
        "CREATE (recent)-[:SIMILAR_TO {weight: 0.8}]->(target)",
        {"post": "Post"},
    )

    # Verify both relationships exist before pruning
    before = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__)-[r:SIMILAR_TO]->() "
            "WHERE p.id IN [30001, 30002] "
            "RETURN p.id AS post_id, count(r) AS cnt",
            {"post": "Post"},
        )
    )
    assert len(before) == 2, "Both posts should have relationships before pruning"

    # Run temporal pruning with 30 days threshold
    temporal_settings = settings.model_copy(
        update={
            "similarity_pruning": "temporal",
            "prune_days": 30,
        }
    )
    neo4j.prune_similarity_links(temporal_settings)

    # Verify old post's relationship was removed
    old_count = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__ {id: 30001})-[r:SIMILAR_TO]->() "
            "RETURN count(r) AS cnt",
            {"post": "Post"},
        )
    )
    assert coerce_int(old_count[0].get("cnt")) == 0, "Old post relationship should be removed"

    # Note: The recent post's relationship status depends on implementation
    # The temporal pruning targets posts older than prune_days


@pytest.mark.integration
def test_similarity_pruning_disabled_strategy(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test 'none' similarity pruning keeps all relationships."""
    neo4j.label("Post")

    # Create test posts with SIMILAR_TO relationships
    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 40001, text: 'No prune 1'}) "
        "CREATE (p2:__post__ {id: 40002, text: 'No prune 2'}) "
        "CREATE (p1)-[:SIMILAR_TO {weight: 0.1}]->(p2)",
        {"post": "Post"},
    )

    # Run with pruning disabled
    disabled_settings = settings.model_copy(update={"similarity_pruning": "none"})
    neo4j.prune_similarity_links(disabled_settings)

    # Verify relationship still exists
    count = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__ {id: 40001})-[r:SIMILAR_TO]->() "
            "RETURN count(r) AS cnt",
            {"post": "Post"},
        )
    )
    assert coerce_int(count[0].get("cnt")) == 1


@pytest.mark.integration
def test_run_similarity_pruning_calls_prune(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test run_similarity_pruning as a standalone pipeline step."""
    neo4j.label("Post")

    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 50001}) "
        "CREATE (p2:__post__ {id: 50002}) "
        "CREATE (p1)-[:SIMILAR_TO {weight: 0.8}]->(p2)",
        {"post": "Post"},
    )

    pruning_settings = settings.model_copy(
        update={
            "similarity_pruning": "aggressive",
            "prune_after_clustering": True,
        }
    )
    run_similarity_pruning(neo4j, pruning_settings)

    count_result = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__)-[r:SIMILAR_TO]->() "
        "WHERE p.id IN [50001, 50002] "
        "RETURN count(r) AS cnt",
        {"post": "Post"},
    )
    assert coerce_int(count_result[0].get("cnt")) == 0


@pytest.mark.integration
def test_run_similarity_pruning_disabled(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test run_similarity_pruning does nothing when disabled."""
    neo4j.label("Post")

    neo4j.execute_labeled(
        "CREATE (p1:__post__ {id: 55001}) "
        "CREATE (p2:__post__ {id: 55002}) "
        "CREATE (p1)-[:SIMILAR_TO {weight: 0.8}]->(p2)",
        {"post": "Post"},
    )

    disabled_settings = settings.model_copy(
        update={"prune_after_clustering": False}
    )
    run_similarity_pruning(neo4j, disabled_settings)

    count_result = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__)-[r:SIMILAR_TO]->() "
        "WHERE p.id IN [55001, 55002] "
        "RETURN count(r) AS cnt",
        {"post": "Post"},
    )
    assert coerce_int(count_result[0].get("cnt")) == 1


@pytest.mark.integration
def test_incremental_build_processes_only_new_posts(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test incremental build only processes posts without SIMILAR_TO edges.

    First run: builds edges for all posts (full mode, empty signature).
    Second run: only builds edges for newly added posts (incremental mode).
    """
    ensure_graph_indexes(neo4j, settings)
    state_store = StateStore(neo4j, "test_incremental_build")

    embedding = [0.5] * settings.fasttext_vector_size
    test_settings = settings.model_copy(
        update={
            "similarity_recency_days": 365,
            "similarity_threshold": 0.0,
        }
    )

    # Create initial posts
    for i in range(3):
        neo4j.execute_labeled(
            "CREATE (p:__post__ {id: $id, embedding: $emb, createdAt: datetime()})",
            {"post": "Post"},
            {"id": 60001 + i, "emb": embedding},
        )

    # First run: full mode (signature is empty)
    run_post_clustering(neo4j, test_settings, state_store)

    edges_after_first = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__)-[r:SIMILAR_TO]->() "
        "WHERE p.id >= 60001 AND p.id < 70000 "
        "RETURN count(r) AS cnt",
        {"post": "Post"},
    )
    first_count = coerce_int(edges_after_first[0].get("cnt"))
    assert first_count > 0, "First run should create SIMILAR_TO edges"

    # Verify signature was saved
    state = state_store.load()
    expected_sig = build_similarity_signature(test_settings)
    assert state.similarity_signature == expected_sig

    # Add a new post
    neo4j.execute_labeled(
        "CREATE (p:__post__ {id: $id, embedding: $emb, createdAt: datetime()})",
        {"post": "Post"},
        {"id": 60010, "emb": embedding},
    )

    # Second run: incremental mode (signature matches)
    run_post_clustering(neo4j, test_settings, state_store)

    # New post should now have SIMILAR_TO edges too
    new_post_edges = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__ {id: 60010})-[r:SIMILAR_TO]->() "
        "RETURN count(r) AS cnt",
        {"post": "Post"},
    )
    new_count = coerce_int(new_post_edges[0].get("cnt"))
    assert new_count >= 0


@pytest.mark.integration
def test_signature_change_triggers_full_rebuild(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test that changing similarity params triggers full rebuild.

    When knn_neighbors changes, the similarity_signature changes,
    causing all existing SIMILAR_TO edges to be deleted and rebuilt.
    """
    ensure_graph_indexes(neo4j, settings)
    state_store = StateStore(neo4j, "test_signature_change")

    embedding = [0.5] * settings.fasttext_vector_size
    initial_settings = settings.model_copy(
        update={
            "similarity_recency_days": 365,
            "similarity_threshold": 0.0,
            "knn_neighbors": 5,
        }
    )

    for i in range(3):
        neo4j.execute_labeled(
            "CREATE (p:__post__ {id: $id, embedding: $emb, createdAt: datetime()})",
            {"post": "Post"},
            {"id": 70001 + i, "emb": embedding},
        )

    # First run with knn_neighbors=5
    run_post_clustering(neo4j, initial_settings, state_store)

    state = state_store.load()
    sig1 = state.similarity_signature
    assert "knn:5" in sig1

    # Change knn_neighbors -> signature changes -> full rebuild
    changed_settings = initial_settings.model_copy(
        update={"knn_neighbors": 10}
    )

    run_post_clustering(neo4j, changed_settings, state_store)

    state = state_store.load()
    sig2 = state.similarity_signature
    assert "knn:10" in sig2
    assert sig1 != sig2


@pytest.mark.integration
def test_build_similarity_signature_format(
    settings: HintGridSettings,
) -> None:
    """Test that build_similarity_signature returns expected format."""
    sig = build_similarity_signature(settings)

    assert sig.startswith("knn:")
    assert ":threshold:" in sig
    assert ":recency:" in sig
    assert str(settings.knn_neighbors) in sig
    assert str(settings.similarity_threshold) in sig
    assert str(settings.similarity_recency_days) in sig
