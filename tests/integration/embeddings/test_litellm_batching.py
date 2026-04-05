"""Integration tests for LiteLLM EmbeddingClient sub-batching.

Verifies that EmbeddingClient correctly splits large input lists
into sub-batches of ``llm_batch_size``, calls the real API for each,
and concatenates results preserving order.

Uses FastText embedding service as a real OpenAI-compatible API endpoint.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest

from hintgrid.embeddings.litellm_client import EmbeddingClient

if TYPE_CHECKING:
    from hintgrid.config import HintGridSettings
    from collections.abc import Generator
    from tests.conftest import EmbeddingServiceConfig


SAMPLE_TEXTS: list[tuple[int, str]] = [
    (1, "Python is a great programming language"),
    (2, "Machine learning with Python is powerful"),
    (3, "Data science requires statistical knowledge"),
    (4, "Deep learning neural networks are fascinating"),
    (5, "Natural language processing with transformers"),
    (6, "Web development with modern frameworks"),
    (7, "GraphDB is awesome for connected data"),
    (8, "Distributed systems scale horizontally"),
    (9, "Functional programming reduces side effects"),
    (10, "Cloud computing enables elastic infrastructure"),
]


def _make_settings(
    service: EmbeddingServiceConfig,
    base: HintGridSettings,
    llm_batch_size: int,
) -> HintGridSettings:
    """Create settings pointing to a real embedding service with given batch size."""
    return base.model_copy(
        update={
            "llm_provider": "openai",
            "llm_base_url": service["api_base"],
            "llm_model": service["model"],
            "llm_dimensions": base.fasttext_vector_size,
            "llm_batch_size": llm_batch_size,
        },
    )


@pytest.mark.integration
def test_single_batch_returns_all_embeddings(
    fasttext_embedding_service: EmbeddingServiceConfig,
    settings: HintGridSettings,
) -> None:
    """When batch_size >= input size, all texts go in one API call."""
    test_settings = _make_settings(
        fasttext_embedding_service, settings, llm_batch_size=100,
    )
    client = EmbeddingClient(test_settings)

    result = client.embed_texts(SAMPLE_TEXTS)

    assert len(result) == len(SAMPLE_TEXTS)
    for embedding in result:
        assert isinstance(embedding, list)
        assert len(embedding) == settings.fasttext_vector_size
        assert all(isinstance(v, float) for v in embedding)


@pytest.mark.integration
def test_multi_batch_returns_all_embeddings(
    fasttext_embedding_service: EmbeddingServiceConfig,
    settings: HintGridSettings,
) -> None:
    """When batch_size < input size, texts are split into multiple API calls."""
    test_settings = _make_settings(
        fasttext_embedding_service, settings, llm_batch_size=3,
    )
    client = EmbeddingClient(test_settings)

    result = client.embed_texts(SAMPLE_TEXTS)

    # 10 texts / batch_size 3 = 4 batches (3+3+3+1)
    assert len(result) == len(SAMPLE_TEXTS)
    for embedding in result:
        assert len(embedding) == settings.fasttext_vector_size


@pytest.mark.integration
def test_batch_size_one_processes_each_text_separately(
    fasttext_embedding_service: EmbeddingServiceConfig,
    settings: HintGridSettings,
) -> None:
    """batch_size=1 sends each text as a separate API call."""
    test_settings = _make_settings(
        fasttext_embedding_service, settings, llm_batch_size=1,
    )
    client = EmbeddingClient(test_settings)
    texts = SAMPLE_TEXTS[:3]

    result = client.embed_texts(texts)

    assert len(result) == 3
    for embedding in result:
        assert len(embedding) == settings.fasttext_vector_size


@pytest.mark.integration
def test_batching_preserves_order(
    fasttext_embedding_service: EmbeddingServiceConfig,
    settings: HintGridSettings,
) -> None:
    """Embeddings from batched call match the same order as single-batch call."""
    single_settings = _make_settings(
        fasttext_embedding_service, settings, llm_batch_size=100,
    )
    batched_settings = _make_settings(
        fasttext_embedding_service, settings, llm_batch_size=2,
    )
    single_client = EmbeddingClient(single_settings)
    batched_client = EmbeddingClient(batched_settings)

    single_result = single_client.embed_texts(SAMPLE_TEXTS[:6])
    batched_result = batched_client.embed_texts(SAMPLE_TEXTS[:6])

    assert len(single_result) == len(batched_result)
    for single_emb, batched_emb in zip(single_result, batched_result, strict=False):
        assert len(single_emb) == len(batched_emb)
        # Same text with same model should produce identical embeddings
        for s_val, b_val in zip(single_emb, batched_emb, strict=False):
            assert abs(s_val - b_val) < 1e-6, (
                f"Embedding values differ: {s_val} vs {b_val}"
            )


@pytest.mark.integration
def test_empty_input_returns_empty_list(
    fasttext_embedding_service: EmbeddingServiceConfig,
    settings: HintGridSettings,
) -> None:
    """Empty input should return empty list without any API calls."""
    test_settings = _make_settings(
        fasttext_embedding_service, settings, llm_batch_size=10,
    )
    client = EmbeddingClient(test_settings)

    result = client.embed_texts([])

    assert result == []


@pytest.mark.integration
def test_exact_batch_size_boundary(
    fasttext_embedding_service: EmbeddingServiceConfig,
    settings: HintGridSettings,
) -> None:
    """When input size equals batch_size, exactly one batch is sent."""
    texts = SAMPLE_TEXTS[:5]
    test_settings = _make_settings(
        fasttext_embedding_service, settings, llm_batch_size=5,
    )
    client = EmbeddingClient(test_settings)

    result = client.embed_texts(texts)

    assert len(result) == 5
    for embedding in result:
        assert len(embedding) == settings.fasttext_vector_size


@pytest.mark.integration
def test_batch_size_larger_than_input(
    fasttext_embedding_service: EmbeddingServiceConfig,
    settings: HintGridSettings,
) -> None:
    """When batch_size > input size, all texts go in one batch."""
    texts = SAMPLE_TEXTS[:2]
    test_settings = _make_settings(
        fasttext_embedding_service, settings, llm_batch_size=1000,
    )
    client = EmbeddingClient(test_settings)

    result = client.embed_texts(texts)

    assert len(result) == 2
    for embedding in result:
        assert len(embedding) == settings.fasttext_vector_size


@pytest.mark.integration
def test_single_text_input(
    fasttext_embedding_service: EmbeddingServiceConfig,
    settings: HintGridSettings,
) -> None:
    """Single text input works correctly with any batch_size."""
    test_settings = _make_settings(
        fasttext_embedding_service, settings, llm_batch_size=3,
    )
    client = EmbeddingClient(test_settings)

    result = client.embed_texts([(1, "Hello world")])

    assert len(result) == 1
    assert len(result[0]) == settings.fasttext_vector_size


@pytest.mark.integration
def test_multi_batch_logs_progress(
    fasttext_embedding_service: EmbeddingServiceConfig,
    settings: HintGridSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Multi-batch processing logs progress for each sub-batch."""
    test_settings = _make_settings(
        fasttext_embedding_service, settings, llm_batch_size=3,
    )
    client = EmbeddingClient(test_settings)

    with caplog.at_level(logging.INFO, logger="hintgrid.embeddings.litellm_client"):
        client.embed_texts(SAMPLE_TEXTS)

    # 10 texts / batch 3 = 4 batches → 4 log messages
    batch_messages = [r for r in caplog.records if "Processing embedding batch" in r.message]
    assert len(batch_messages) == 4
    assert "1/4" in batch_messages[0].message
    assert "4/4" in batch_messages[3].message


@pytest.mark.integration
def test_single_batch_does_not_log_progress(
    fasttext_embedding_service: EmbeddingServiceConfig,
    settings: HintGridSettings,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Single-batch processing does not log batch progress."""
    test_settings = _make_settings(
        fasttext_embedding_service, settings, llm_batch_size=100,
    )
    client = EmbeddingClient(test_settings)

    with caplog.at_level(logging.INFO, logger="hintgrid.embeddings.litellm_client"):
        client.embed_texts(SAMPLE_TEXTS)

    batch_messages = [r for r in caplog.records if "Processing embedding batch" in r.message]
    assert len(batch_messages) == 0


@pytest.mark.integration
def test_iterable_input_consumed_correctly(
    fasttext_embedding_service: EmbeddingServiceConfig,
    settings: HintGridSettings,
) -> None:
    """embed_texts accepts any Iterable, not just lists."""
    test_settings = _make_settings(
        fasttext_embedding_service, settings, llm_batch_size=2,
    )
    client = EmbeddingClient(test_settings)

    # Pass a generator instead of a list
    def text_generator() -> Generator[tuple[int, str], None, None]:
        yield from SAMPLE_TEXTS[:4]

    result = client.embed_texts(text_generator())

    assert len(result) == 4
    for embedding in result:
        assert len(embedding) == settings.fasttext_vector_size
