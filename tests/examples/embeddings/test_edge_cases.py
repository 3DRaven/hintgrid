"""Edge case tests for FastText embedding service.

Tests handling of empty strings, punctuation, emojis, whitespace, etc.
"""

import pytest

from tests.conftest import EmbeddingServiceConfig

from .conftest import EMBEDDING_DIM


@pytest.mark.integration
def test_fasttext_empty_string_handling(
    fasttext_embedding_service: EmbeddingServiceConfig,
) -> None:
    """Test that FastText service handles empty strings with deterministic fallback."""
    from litellm import embedding

    config = fasttext_embedding_service

    # Test empty string - should return deterministic non-zero fallback vector
    response = embedding(
        model=config["model"],
        input=[""],
        api_base=config["api_base"],
    )

    emb = response["data"][0]["embedding"]
    assert len(emb) == EMBEDDING_DIM, f"Expected {EMBEDDING_DIM}-dim embedding"
    assert any(val != 0.0 for val in emb), "Empty string should return non-zero fallback vector"

    print("✅ Empty string handled correctly with deterministic fallback")


@pytest.mark.integration
def test_fasttext_punctuation_only_handling(
    fasttext_embedding_service: EmbeddingServiceConfig,
) -> None:
    """Test that FastText service handles punctuation-only strings with fallback."""
    from litellm import embedding

    config = fasttext_embedding_service

    # Test various punctuation-only strings
    punctuation_tests = ["!!!", "???", "...", "---", "***", "@#$%^&*()"]

    for punct in punctuation_tests:
        response = embedding(
            model=config["model"],
            input=[punct],
            api_base=config["api_base"],
        )

        emb = response["data"][0]["embedding"]
        assert len(emb) == EMBEDDING_DIM, (
            f"Expected {EMBEDDING_DIM}-dim embedding for '{punct}'"
        )
        assert any(val != 0.0 for val in emb), (
            f"Punctuation '{punct}' should return non-zero fallback"
        )

    print(f"✅ Punctuation-only strings handled with fallback ({len(punctuation_tests)} cases)")


@pytest.mark.integration
def test_fasttext_emoji_only_handling(
    fasttext_embedding_service: EmbeddingServiceConfig,
) -> None:
    """Test that FastText service handles emoji-only strings with fallback."""
    from litellm import embedding

    config = fasttext_embedding_service

    # Test various emoji-only strings
    emoji_tests = ["😀", "😀😁😂", "🎉🎊🎈", "❤️💙💚", "🚀🛸🌟"]

    for emoji_str in emoji_tests:
        response = embedding(
            model=config["model"],
            input=[emoji_str],
            api_base=config["api_base"],
        )

        emb = response["data"][0]["embedding"]
        assert len(emb) == EMBEDDING_DIM, (
            f"Expected {EMBEDDING_DIM}-dim embedding for '{emoji_str}'"
        )
        assert any(val != 0.0 for val in emb), (
            f"Emoji '{emoji_str}' should return non-zero fallback"
        )

    print(f"✅ Emoji-only strings handled with fallback ({len(emoji_tests)} cases)")


@pytest.mark.integration
def test_fasttext_whitespace_only_handling(
    fasttext_embedding_service: EmbeddingServiceConfig,
) -> None:
    """Test that FastText service handles whitespace-only strings with fallback."""
    from litellm import embedding

    config = fasttext_embedding_service

    # Test various whitespace-only strings
    whitespace_tests = [" ", "   ", "\t", "\n", "\r\n", "  \t\n  "]

    for ws in whitespace_tests:
        response = embedding(
            model=config["model"],
            input=[ws],
            api_base=config["api_base"],
        )

        emb = response["data"][0]["embedding"]
        assert len(emb) == EMBEDDING_DIM, (
            f"Expected {EMBEDDING_DIM}-dim embedding for whitespace"
        )
        assert any(val != 0.0 for val in emb), "Whitespace should return non-zero fallback"

    print(f"✅ Whitespace-only strings handled with fallback ({len(whitespace_tests)} cases)")


@pytest.mark.integration
def test_fasttext_first_request_empty(
    fasttext_embedding_service: EmbeddingServiceConfig,
) -> None:
    """Test that FastText service handles empty string as first request (cold start)."""
    from litellm import embedding

    config = fasttext_embedding_service

    # Send empty string as FIRST request (vectorizer is None initially)
    response = embedding(
        model=config["model"],
        input=[""],
        api_base=config["api_base"],
    )

    assert "data" in response, "Response should have 'data' field"
    assert len(response["data"]) == 1, "Should return 1 embedding"

    emb = response["data"][0]["embedding"]
    assert len(emb) == EMBEDDING_DIM, f"Expected {EMBEDDING_DIM}-dim embedding"
    assert any(val != 0.0 for val in emb), "Embedding should not be all zeros"

    # Now send valid request to ensure service recovered
    response2 = embedding(
        model=config["model"],
        input=["hello world"],
        api_base=config["api_base"],
    )

    assert response2["data"][0]["embedding"], "Service should work after empty string"

    print("✅ Empty string as first request handled correctly (cold start)")
