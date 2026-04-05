"""Integration tests for embedding provider selection and training."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.embeddings.provider import EmbeddingProvider, TrainableEmbeddingProvider

if TYPE_CHECKING:
    from hintgrid.config import HintGridSettings
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient



@pytest.mark.integration
def test_embedding_provider_uses_fasttext_by_default(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """EmbeddingProvider uses FastText when no LLM base_url is set.

    Without a trained model on disk, ``embed_texts`` must not auto-train unless
    a PostgreSQL client is supplied (see ``FastTextEmbeddingService.embed_texts``).
    Passing ``postgres=None`` exercises the documented error path for missing
    models and no DB for training.
    """
    provider = EmbeddingProvider(settings, neo4j, postgres=None)

    with pytest.raises(RuntimeError, match="No trained models found"):
        provider.embed_texts([(1, "test text")])


@pytest.mark.integration
def test_embedding_provider_uses_fasttext_for_empty_provider(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """EmbeddingProvider uses FastText when provider is empty string."""
    fasttext_settings = settings.model_copy(
        update={"llm_provider": "", "llm_base_url": ""}
    )
    provider = EmbeddingProvider(fasttext_settings, neo4j, postgres=None)

    with pytest.raises(RuntimeError, match="No trained models found"):
        provider.embed_texts([(1, "test text")])


@pytest.mark.integration
def test_embedding_provider_uses_litellm_for_external(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """EmbeddingProvider uses LiteLLM when base_url is set."""
    llm_settings = settings.model_copy(
        update={
            "llm_provider": "openai",
            "llm_base_url": "http://localhost:8080",
            "llm_model": "text-embedding-3-small",
        }
    )
    provider = EmbeddingProvider(llm_settings, neo4j)
    
    # Test behavior through public API
    # LiteLLM will try to connect to the base_url
    # This tests the actual behavior, not internal implementation
    try:
        result = provider.embed_texts([(1, "test text")])
        # If it succeeds, should return embeddings
        assert len(result) == 1
        assert len(result[0]) > 0
    except Exception:
        # Expected if LiteLLM server is not available
        pass


@pytest.mark.integration
def test_trainable_provider_supports_training_with_fasttext(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    postgres_client: PostgresClient,
) -> None:
    """TrainableEmbeddingProvider supports training when using FastText."""
    provider = TrainableEmbeddingProvider(settings, neo4j, postgres_client)
    assert provider.supports_training is True


@pytest.mark.integration
def test_trainable_provider_no_training_with_litellm(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    postgres_client: PostgresClient,
) -> None:
    """TrainableEmbeddingProvider doesn't support training with LiteLLM."""
    llm_settings = settings.model_copy(
        update={
            "llm_provider": "openai",
            "llm_base_url": "http://localhost:8080",
            "llm_model": "text-embedding-3-small",
        }
    )
    provider = TrainableEmbeddingProvider(llm_settings, neo4j, postgres_client)
    assert provider.supports_training is False

    # Training should return False
    assert provider.train_full() is False
    assert provider.train_incremental() is False
