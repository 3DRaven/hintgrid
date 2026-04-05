"""Integration tests for Neo4j client validation and edge cases."""

from __future__ import annotations


import pytest

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient



@pytest.mark.integration
def test_create_vector_index_invalid_name(neo4j: Neo4jClient) -> None:
    """create_vector_index rejects invalid index names."""
    with pytest.raises(ValueError, match="Invalid index name"):
        neo4j.create_vector_index(
            index_name="invalid-name!",
            label="Post",
            property_name="embedding",
            dimensions=64,
        )


@pytest.mark.integration
def test_create_vector_index_invalid_label(neo4j: Neo4jClient) -> None:
    """create_vector_index rejects invalid labels."""
    with pytest.raises(ValueError, match="Invalid label"):
        neo4j.create_vector_index(
            index_name="test_index",
            label="Invalid-Label!",
            property_name="embedding",
            dimensions=64,
        )


@pytest.mark.integration
def test_create_vector_index_invalid_property(neo4j: Neo4jClient) -> None:
    """create_vector_index rejects invalid property names."""
    with pytest.raises(ValueError, match="Invalid property name"):
        neo4j.create_vector_index(
            index_name="test_index",
            label="Post",
            property_name="invalid-prop",
            dimensions=64,
        )


@pytest.mark.integration
def test_create_vector_index_invalid_dimensions(neo4j: Neo4jClient) -> None:
    """create_vector_index rejects out-of-range dimensions."""
    with pytest.raises(ValueError, match="Invalid dimensions"):
        neo4j.create_vector_index(
            index_name="test_index",
            label="Post",
            property_name="embedding",
            dimensions=0,
        )

    with pytest.raises(ValueError, match="Invalid dimensions"):
        neo4j.create_vector_index(
            index_name="test_index",
            label="Post",
            property_name="embedding",
            dimensions=5000,
        )


@pytest.mark.integration
def test_create_vector_index_invalid_similarity(neo4j: Neo4jClient) -> None:
    """create_vector_index rejects invalid similarity functions."""
    with pytest.raises(ValueError, match="Invalid similarity function"):
        neo4j.create_vector_index(
            index_name="test_index",
            label="Post",
            property_name="embedding",
            dimensions=64,
            similarity_function="manhattan",
        )


@pytest.mark.integration
def test_label_validation_rejects_special_chars(neo4j: Neo4jClient) -> None:
    """label() rejects labels with special characters."""
    with pytest.raises(ValueError, match="Invalid label"):
        neo4j.label("Invalid-Label!")


@pytest.mark.integration
def test_label_validation_rejects_empty(neo4j: Neo4jClient) -> None:
    """label() rejects empty labels."""
    with pytest.raises(ValueError, match="Invalid label"):
        neo4j.label("")


@pytest.mark.integration
def test_match_all_nodes_with_worker_label(
    neo4j: Neo4jClient,
) -> None:
    """match_all_nodes returns correct pattern based on worker_label."""
    if neo4j.worker_label:
        pattern = neo4j.match_all_nodes("x")
        assert neo4j.worker_label in pattern
        assert "(x:" in pattern
    else:
        pattern = neo4j.match_all_nodes("x")
        assert pattern == "(x)"


@pytest.mark.integration
def test_stream_user_ids_with_invalid_values(neo4j: Neo4jClient) -> None:
    """stream_user_ids skips invalid user IDs gracefully."""
    # Create users with valid and null IDs
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 70001, name: 'valid1'}) "
        "CREATE (u2:__user__ {name: 'no_id'}) "
        "CREATE (u3:__user__ {id: 70002, name: 'valid2'})",
        {"user": "User"},
    )

    ids = list(neo4j.stream_user_ids())
    # Should include valid IDs and skip null
    assert 70001 in ids
    assert 70002 in ids


@pytest.mark.integration
def test_stream_user_ids_handles_missing_properties(neo4j: Neo4jClient) -> None:
    """stream_user_ids handles missing properties gracefully."""
    # Create users with valid IDs
    neo4j.execute_labeled(
        "CREATE (u1:__user__ {id: 80001, name: 'fetch1'}) "
        "CREATE (u2:__user__ {name: 'no_id_fetch'}) "
        "CREATE (u3:__user__ {id: 80002, name: 'fetch2'})",
        {"user": "User"},
    )

    ids = list(neo4j.stream_user_ids())
    assert 80001 in ids
    assert 80002 in ids
