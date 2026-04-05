"""Integration tests for app.py train error handling.

Tests verify that train_full and train_incremental return False
when training is not supported (e.g., LiteLLM backend).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient
    from hintgrid.clients.redis import RedisClient
    from hintgrid.config import HintGridSettings
    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig


@pytest.mark.integration
def test_train_full_failure_when_not_supported(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    redis_client: RedisClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    docker_compose: DockerComposeInfo,
    settings: HintGridSettings,
) -> None:
    """Test that train_full returns False when training is not supported."""
    from hintgrid.app import HintGridApp

    # Use LiteLLM backend which doesn't support training
    test_settings = settings.model_copy(
        update={
            "llm_provider": "openai",
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_dimensions": settings.fasttext_vector_size,
        }
    )

    app = HintGridApp(
        neo4j=neo4j,
        postgres=postgres_client,
        redis=redis_client,
        settings=test_settings,
    )

    # Call train_full - should return False for LiteLLM backend
    result = app.train_full()

    assert result is False, "Should return False when training is not supported"


@pytest.mark.integration
def test_train_incremental_failure_when_not_supported(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    redis_client: RedisClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    docker_compose: DockerComposeInfo,
    settings: HintGridSettings,
) -> None:
    """Test that train_incremental returns False when training is not supported."""
    from hintgrid.app import HintGridApp

    # Use LiteLLM backend which doesn't support training
    test_settings = settings.model_copy(
        update={
            "llm_provider": "openai",
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_dimensions": settings.fasttext_vector_size,
        }
    )

    app = HintGridApp(
        neo4j=neo4j,
        postgres=postgres_client,
        redis=redis_client,
        settings=test_settings,
    )

    # Call train_incremental - should return False for LiteLLM backend
    result = app.train_incremental()

    assert result is False, "Should return False when training is not supported"
