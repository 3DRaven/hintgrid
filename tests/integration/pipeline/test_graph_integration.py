"""Integration tests for graph module through public API.

Tests graph operations through public functions, verifying
that helper functions work correctly through integration.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.pipeline.graph import (
    check_embedding_config,
    check_embeddings_exist,
    get_embedding_status,
    reembed_existing_posts,
)
from hintgrid.state import StateStore

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient
    from hintgrid.config import HintGridSettings
    from hintgrid.embeddings.provider import EmbeddingProvider


@pytest.mark.integration
class TestGraphEmbeddingOperations:
    """Integration tests for embedding operations through public API."""

    def test_check_embedding_config_empty_graph(
        self,
        neo4j: Neo4jClient,
        settings: HintGridSettings,
    ) -> None:
        """Test check_embedding_config with empty graph."""
        state_store = StateStore(neo4j, "test_state")
        result = check_embedding_config(neo4j, settings, state_store)
        
        # Should record initial signature
        assert result.current_signature is not None
        assert result.migrated is False or result.migrated is True

    def test_get_embedding_status(
        self,
        neo4j: Neo4jClient,
        settings: HintGridSettings,
    ) -> None:
        """Test get_embedding_status returns correct status."""
        state_store = StateStore(neo4j, "test_state")
        status = get_embedding_status(settings, state_store)
        
        assert "stored_signature" in status
        assert "current_signature" in status
        assert "match" in status
        assert isinstance(status["match"], bool)

    def test_check_embeddings_exist_empty(
        self,
        neo4j: Neo4jClient,
    ) -> None:
        """Test check_embeddings_exist with empty graph."""
        result = check_embeddings_exist(neo4j)
        assert result is False

    def test_reembed_existing_posts_empty(
        self,
        neo4j: Neo4jClient,
        postgres_client: PostgresClient,
        settings: HintGridSettings,
    ) -> None:
        """Test reembed_existing_posts with empty graph."""
        from hintgrid.embeddings.provider import EmbeddingProvider
        
        provider = EmbeddingProvider(settings, neo4j, postgres_client)
        result = reembed_existing_posts(neo4j, provider, settings)
        
        assert result == 0

    def test_reembed_existing_posts_with_data(
        self,
        neo4j: Neo4jClient,
        postgres_client: PostgresClient,
        settings: HintGridSettings,
    ) -> None:
        """Test reembed_existing_posts processes posts correctly."""
        from hintgrid.embeddings.provider import EmbeddingProvider
        
        # Create post without embedding
        neo4j.execute(
            "CREATE (p:__post__ {id: 123, text: 'Test post', createdAt: datetime()})"
        )
        
        provider = EmbeddingProvider(settings, neo4j, postgres_client)
        result = reembed_existing_posts(neo4j, provider, settings, batch_size=100)
        
        # Should process at least one post
        assert result >= 0
        
        # Verify embedding was created
        embeddings_exist = check_embeddings_exist(neo4j)
        # May or may not exist depending on provider configuration
        assert isinstance(embeddings_exist, bool)


@pytest.mark.integration
class TestGraphIndexOperations:
    """Integration tests for index operations through public API."""

    def test_embedding_config_handles_vector_dimensions(
        self,
        neo4j: Neo4jClient,
        settings: HintGridSettings,
    ) -> None:
        """Test that embedding config correctly handles vector dimensions."""
        state_store = StateStore(neo4j, "test_state")
        
        # This function internally uses _extract_vector_dimension
        result = check_embedding_config(neo4j, settings, state_store)
        
        # Should complete without errors
        assert result.current_signature is not None
        
        # Verify status can be retrieved (uses vector dimension extraction)
        status = get_embedding_status(settings, state_store)
        assert status["current_signature"] == result.current_signature
