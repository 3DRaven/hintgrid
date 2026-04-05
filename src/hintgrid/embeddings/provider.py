"""Embedding provider selection between LiteLLM and FastText."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Protocol
from collections.abc import Iterable

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient

from hintgrid.cli.console import print_info
from hintgrid.config import HintGridSettings
from hintgrid.embeddings.fasttext_service import FastTextEmbeddingService
from hintgrid.embeddings.litellm_client import EmbeddingClient

logger = logging.getLogger(__name__)


class EmbeddingBackend(Protocol):
    def embed_texts(self, texts: Iterable[tuple[int, str]]) -> list[list[float]]: ...


class EmbeddingProvider:
    """Select embedding backend based on configuration."""

    def __init__(
        self,
        settings: HintGridSettings,
        neo4j: Neo4jClient,
        postgres: PostgresClient | None = None,
        since_date: datetime | None = None,
    ) -> None:
        self._backend = self._select_backend(settings, neo4j, postgres, since_date)

    def embed_texts(self, texts: Iterable[tuple[int, str]]) -> list[list[float]]:
        return self._backend.embed_texts(texts)

    def _select_backend(
        self,
        settings: HintGridSettings,
        neo4j: Neo4jClient,
        postgres: PostgresClient | None,
        since_date: datetime | None,
    ) -> EmbeddingBackend:
        provider = settings.llm_provider.lower().strip() if settings.llm_provider else ""
        base_url = (settings.llm_base_url or "").strip()

        # Use FastText for local embeddings (default)
        if provider in {"", "none", "fasttext", "tfidf"} or not base_url:
            print_info("Using FastText embeddings (built-in)")
            return FastTextEmbeddingService(neo4j, settings, postgres, since_date=since_date)

        # Use LiteLLM for external providers
        print_info(f"Using LiteLLM embeddings: {provider}/{settings.llm_model}")
        return EmbeddingClient(settings)


class TrainableEmbeddingProvider(EmbeddingProvider):
    """Embedding provider with training capabilities."""

    def __init__(
        self,
        settings: HintGridSettings,
        neo4j: Neo4jClient,
        postgres: PostgresClient,
    ) -> None:
        super().__init__(settings, neo4j, postgres)
        self._fasttext: FastTextEmbeddingService | None = None

        # Keep reference to FastText service if used
        # Use type name check instead of isinstance
        # Runtime guarantee: if type name matches, it's FastTextEmbeddingService
        if self._backend is not None and type(self._backend).__name__ == "FastTextEmbeddingService":
            self._fasttext = self._backend  # type: ignore[assignment]

    def train_full(self, since_date: datetime | None = None) -> bool:
        """Perform full training.

        Args:
            since_date: Optional date filter

        Returns:
            True if training succeeded
        """
        if self._fasttext is None:
            logger.warning("Training not supported for LiteLLM backend")
            return False

        result = self._fasttext.train_full(since_date)
        if result.success:
            logger.info(
                "Full training completed: vocab=%d, corpus=%d, version=%d",
                result.vocab_size,
                result.corpus_size,
                result.version,
            )
        else:
            logger.error("Training failed: %s", result.message)

        return result.success

    def train_incremental(self) -> bool:
        """Perform incremental training.

        Returns:
            True if training succeeded
        """
        if self._fasttext is None:
            logger.warning("Training not supported for LiteLLM backend")
            return False

        result = self._fasttext.train_incremental()
        if result.success:
            logger.info(
                "Incremental training completed: vocab=%d, corpus=%d, version=%d",
                result.vocab_size,
                result.corpus_size,
                result.version,
            )
        else:
            logger.error("Training failed: %s", result.message)

        return result.success

    @property
    def supports_training(self) -> bool:
        """Check if training is supported."""
        return self._fasttext is not None
