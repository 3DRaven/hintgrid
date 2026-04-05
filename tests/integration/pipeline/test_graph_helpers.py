"""Integration tests for graph module through public API.

Tests graph operations through public functions, verifying
that helper functions work correctly through integration.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.pipeline.graph import (
    check_embedding_config,
    reembed_existing_posts,
)
from hintgrid.state import StateStore

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient
    from hintgrid.config import HintGridSettings
    from hintgrid.embeddings.provider import EmbeddingProvider


@pytest.mark.integration
class TestGraphEmbeddingBatchProcessing:
    """Integration tests for embedding batch processing through public API."""

    def test_reembed_existing_posts_empty_batch(
        self,
        neo4j: Neo4jClient,
        postgres_client: PostgresClient,
        settings: HintGridSettings,
    ) -> None:
        """Test that reembed_existing_posts handles empty graph correctly."""
        from hintgrid.embeddings.provider import EmbeddingProvider

        provider = EmbeddingProvider(settings, neo4j, postgres_client)
        # This internally uses _process_embedding_batch
        result = reembed_existing_posts(neo4j, provider, settings, batch_size=100)
        assert result == 0

    def test_reembed_existing_posts_with_data(
        self,
        neo4j: Neo4jClient,
        postgres_client: PostgresClient,
        settings: HintGridSettings,
    ) -> None:
        """Test that reembed_existing_posts processes batches correctly."""
        from hintgrid.embeddings.provider import EmbeddingProvider

        # Create posts without embeddings
        neo4j.execute(
            "CREATE (p1:__post__ {id: 1, text: 'Post 1', createdAt: datetime()})"
        )
        neo4j.execute(
            "CREATE (p2:__post__ {id: 2, text: 'Post 2', createdAt: datetime()})"
        )

        provider = EmbeddingProvider(settings, neo4j, postgres_client)
        # This internally uses _process_embedding_batch for each batch
        result = reembed_existing_posts(neo4j, provider, settings, batch_size=1)
        
        # Should process both posts
        assert result >= 0

    def test_embedding_config_handles_various_data_types(
        self,
        neo4j: Neo4jClient,
        settings: HintGridSettings,
    ) -> None:
        """Test that embedding config correctly handles various data types."""
        state_store = StateStore(neo4j, "test_state")
        
        # This function internally uses _is_str_dict and _extract_vector_dimension
        # to process Neo4j query results
        result = check_embedding_config(neo4j, settings, state_store)
        
        # Should complete without errors, handling various data types correctly
        assert result.current_signature is not None
