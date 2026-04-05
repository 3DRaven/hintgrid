"""Integration tests for EmbeddingProvider training failure paths.

Tests the error logging branches when training returns failed results
with real FastTextEmbeddingService.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest

from hintgrid.embeddings.provider import TrainableEmbeddingProvider

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient
    from hintgrid.config import HintGridSettings


@pytest.mark.integration
class TestTrainableEmbeddingProviderFailure:
    """Tests for TrainableEmbeddingProvider training failure branches."""

    def test_train_full_failure_logs_error(
        self,
        settings: HintGridSettings,
        neo4j: Neo4jClient,
        postgres_client: PostgresClient,
        mastodon_schema: None,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that failed full training returns False and logs error."""
        caplog.set_level(logging.ERROR)

        provider = TrainableEmbeddingProvider(settings, neo4j, postgres_client)

        result = provider.train_full()

        assert result is False
        assert "Training failed" in caplog.text

    def test_train_incremental_failure_logs_error(
        self,
        settings: HintGridSettings,
        neo4j: Neo4jClient,
        postgres_client: PostgresClient,
        mastodon_schema: None,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Incremental with no model falls back to full; empty corpus still fails and logs."""
        caplog.set_level(logging.ERROR)

        provider = TrainableEmbeddingProvider(settings, neo4j, postgres_client)

        result = provider.train_incremental()

        assert result is False
        assert "Training failed" in caplog.text
