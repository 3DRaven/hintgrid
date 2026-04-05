"""LiteLLM embedding client with sub-batching and per-batch retries."""

from __future__ import annotations

import logging
from collections.abc import Iterable

import litellm

from hintgrid.config import HintGridSettings

RETRY_START = 1

logger = logging.getLogger(__name__)


class EmbeddingClient:
    """Embedding client using LiteLLM compatible API.

    Splits large input lists into sub-batches of ``llm_batch_size``
    texts each to respect provider token limits (e.g. OpenAI max 2048).
    Each sub-batch has its own retry loop so a transient failure does
    not discard results from already-completed batches.
    """

    def __init__(self, settings: HintGridSettings) -> None:
        self._settings = settings

    def embed_texts(self, texts: Iterable[tuple[int, str]]) -> list[list[float]]:
        """Generate embeddings for *texts* using LiteLLM.

        Args:
            texts: Iterable of (id, text) pairs.

        Returns:
            List of embedding vectors in the same order as *texts*.

        Raises:
            RuntimeError: When retries are exhausted for any sub-batch.
        """
        inputs = [text for _, text in texts]
        if not inputs:
            return []

        batch_size = self._settings.llm_batch_size
        total_batches = (len(inputs) + batch_size - 1) // batch_size

        all_embeddings: list[list[float]] = []
        for batch_idx in range(total_batches):
            start = batch_idx * batch_size
            end = start + batch_size
            chunk = inputs[start:end]

            if total_batches > 1:
                logger.info(
                    "Processing embedding batch %d/%d (%d texts)",
                    batch_idx + 1,
                    total_batches,
                    len(chunk),
                )

            embeddings = self._embed_chunk(chunk)
            all_embeddings.extend(embeddings)

        return all_embeddings

    def _embed_chunk(self, inputs: list[str]) -> list[list[float]]:
        """Send a single sub-batch to the LLM API with retries.

        Args:
            inputs: List of texts for one sub-batch.

        Returns:
            List of embedding vectors.

        Raises:
            RuntimeError: When retries are exhausted.
        """
        last_error: Exception | None = None

        # LiteLLM requires model format as "provider/model" for custom endpoints
        provider = self._settings.llm_provider or "openai"
        model = f"{provider}/{self._settings.llm_model}"

        for attempt in range(RETRY_START, self._settings.llm_max_retries + 1):
            try:
                response = litellm.embedding(
                    model=model,
                    input=inputs,
                    api_base=self._settings.llm_base_url,
                    api_key=self._settings.llm_api_key,
                    timeout=self._settings.llm_timeout,
                )
                data = list(response.get("data") or [])
                return [item["embedding"] for item in data]
            except Exception as exc:  # pragma: no cover - network variability
                last_error = exc
                if attempt >= self._settings.llm_max_retries:
                    raise
        raise RuntimeError("Embedding retries exhausted") from last_error
