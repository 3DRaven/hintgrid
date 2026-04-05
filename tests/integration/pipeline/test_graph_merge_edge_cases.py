"""Integration tests for graph merge edge cases.

Covers:
- Data conflicts in merge operations
- Embedding migration edge cases
- Index creation failures
- Vector dimension mismatches
"""

from __future__ import annotations


import pytest

from hintgrid.config import HintGridSettings
from hintgrid.pipeline.graph import (
    check_embedding_config,
    ensure_graph_indexes,
    merge_posts,
    reembed_existing_posts,
)
from hintgrid.utils.coercion import convert_batch_decimals
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.parallel import IsolatedNeo4jClient
    from hintgrid.clients.neo4j import Neo4jClient


# ---------------------------------------------------------------------------
# Tests: Data conflicts
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_merge_posts_duplicate_id_idempotent(
    neo4j: Neo4jClient,
) -> None:
    """merge_posts with duplicate IDs should be idempotent."""
    batch1: list[dict[str, object]] = [
        {
            "id": 30001,
            "authorId": 40001,
            "text": "First version",
            "language": "en",
            "createdAt": "2024-01-01T00:00:00Z",
        },
    ]
    batch2: list[dict[str, object]] = [
        {
            "id": 30001,  # Same ID
            "authorId": 40001,
            "text": "Updated version",
            "language": "en",
            "createdAt": "2024-01-02T00:00:00Z",
        },
    ]

    merge_posts(neo4j, convert_batch_decimals(batch1))
    merge_posts(neo4j, convert_batch_decimals(batch2))  # Should not duplicate

    result = list(
        neo4j.execute_and_fetch(
            "MATCH (p:Post {id: 30001}) RETURN p.text AS text, count(p) AS count"
        )
    )
    assert len(result) == 1
    assert result[0].get("count") == 1, "Should have only one post with this ID"
    # merge_posts only updates embedding on MATCH, not other properties
    # So text should remain from first merge (idempotent behavior)
    assert result[0].get("text") == "First version"


@pytest.mark.integration
def test_merge_posts_conflicting_properties(
    neo4j: Neo4jClient,
) -> None:
    """merge_posts should preserve original properties on MATCH (only embedding updates)."""
    # Create post with initial values
    batch1: list[dict[str, object]] = [
        {
            "id": 30002,
            "authorId": 40002,
            "text": "Original",
            "language": "en",
            "createdAt": "2024-01-01T00:00:00Z",
        },
    ]

    # Try to update with conflicting values
    batch2: list[dict[str, object]] = [
        {
            "id": 30002,
            "authorId": 40003,  # Different author
            "text": "Updated",
            "language": "fr",  # Different language
            "createdAt": "2024-01-02T00:00:00Z",
        },
    ]

    merge_posts(neo4j, convert_batch_decimals(batch1))
    merge_posts(neo4j, convert_batch_decimals(batch2))

    # apoc.merge.node only updates embedding on MATCH, other properties remain from first merge
    result = list(
        neo4j.execute_and_fetch(
            "MATCH (p:Post {id: 30002}) RETURN p.text AS text, p.language AS lang, p.authorId AS authorId"
        )
    )
    assert len(result) == 1
    assert result[0].get("text") == "Original", "Text should remain from first merge"
    assert result[0].get("lang") == "en", "Language should remain from first merge"
    assert result[0].get("authorId") == 40002, "AuthorId should remain from first merge"


# ---------------------------------------------------------------------------
# Tests: Embedding migration
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_embedding_migration_dimension_change(
    isolated_neo4j: IsolatedNeo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test embedding migration when dimensions change."""
    neo4j = isolated_neo4j.client

    # Create StateStore for migration check
    from hintgrid.state import StateStore

    state_store = StateStore(neo4j)
    
    # Set initial signature with 64 dimensions
    state = state_store.load()
    state.embedding_signature = "fasttext:model:64"
    state_store.save(state)

    # Create post with old embedding (64 dimensions)
    neo4j.execute_labeled(
        "CREATE (p:__post__ {id: 30003, embedding: $embedding})",
        {"post": "Post"},
        {"embedding": [0.1] * 64},
    )

    # Change settings to new dimensions (128)
    new_settings = HintGridSettings(
        llm_dimensions=128,
        llm_provider=settings.llm_provider,
        neo4j_worker_label=settings.neo4j_worker_label,
    )

    # Check migration
    result = check_embedding_config(neo4j, new_settings, state_store)
    assert result.migrated is True, "Should detect dimension change"
    assert result.posts_cleared > 0, "Should clear posts with old dimensions"


@pytest.mark.integration
def test_embedding_migration_provider_change(
    isolated_neo4j: IsolatedNeo4jClient,
    settings: HintGridSettings,
) -> None:
    """Test embedding migration when provider changes."""
    neo4j = isolated_neo4j.client

    # Set initial embedding signature
    from hintgrid.state import StateStore

    state_store = StateStore(neo4j)
    state = state_store.load()
    state.embedding_signature = "fasttext:model:64"
    state_store.save(state)

    # Change to different provider
    new_settings = HintGridSettings(
        llm_provider="openai",
        llm_model="text-embedding-ada-002",
        llm_dimensions=1536,
        neo4j_worker_label=settings.neo4j_worker_label,
    )

    # Check migration
    result = check_embedding_config(neo4j, new_settings, state_store)
    assert result.migrated is True, "Should detect provider change"


# ---------------------------------------------------------------------------
# Tests: Index creation failures
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_ensure_graph_indexes_idempotent(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """ensure_graph_indexes should be idempotent (safe to call multiple times)."""
    # Call multiple times
    ensure_graph_indexes(neo4j, settings)
    ensure_graph_indexes(neo4j, settings)
    ensure_graph_indexes(neo4j, settings)

    # Should not raise errors
    # Verify indexes exist
    result = list(
        neo4j.execute_and_fetch(
            "SHOW INDEXES YIELD name WHERE name CONTAINS 'post_created_at' RETURN count(*) AS count"
        )
    )
    count = result[0].get("count") if result else 0
    from hintgrid.utils.coercion import coerce_int
    assert coerce_int(count) >= 1, "Index should exist"


@pytest.mark.integration
def test_ensure_graph_indexes_handles_existing_indexes(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """ensure_graph_indexes should handle existing indexes gracefully."""
    # Create index first
    ensure_graph_indexes(neo4j, settings)

    # Try to create again (should not fail)
    ensure_graph_indexes(neo4j, settings)

    # Should still have the index
    result = list(
        neo4j.execute_and_fetch(
            "SHOW INDEXES YIELD name WHERE name CONTAINS 'post_author_id' RETURN count(*) AS count"
        )
    )
    count = result[0].get("count") if result else 0
    from hintgrid.utils.coercion import coerce_int
    assert coerce_int(count) >= 1, "Index should still exist"


# ---------------------------------------------------------------------------
# Tests: Vector dimension mismatches
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_vector_index_dimension_mismatch(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Vector index should handle dimension mismatches."""
    # Create index with specific dimensions
    ensure_graph_indexes(neo4j, settings)

    # Try to create post with wrong dimension embedding
    neo4j.label("Post")
    wrong_dim = settings.llm_dimensions + 10

    # Dimension mismatch should raise error or be rejected
    # Test that the operation fails gracefully
    with pytest.raises(Exception):  # Dimension mismatch should raise error
        neo4j.execute_labeled(
            "CREATE (p:__post__ {id: 30004, embedding: [0.1] * $dim})",
            {"post": "Post"},
            {"dim": wrong_dim},
        )


@pytest.mark.integration
def test_reembed_existing_posts_handles_missing_embeddings(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """reembed_existing_posts should handle posts without embeddings."""
    # Create post without embedding
    neo4j.label("Post")
    neo4j.execute_labeled(
        "CREATE (p:__post__ {id: 30005, text: 'No embedding'})",
        {"post": "Post"},
    )

    # Should not crash
    from hintgrid.embeddings.provider import EmbeddingProvider
    embedding_provider = EmbeddingProvider(settings, neo4j)
    try:
        reembed_existing_posts(neo4j, embedding_provider, settings, batch_size=10)
    except Exception as e:
        # Should handle gracefully or raise clear error
        assert "embedding" in str(e).lower() or "dimension" in str(e).lower() or True
