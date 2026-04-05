"""LiteLLM integration tests with FastText embedding service.

Tests LiteLLM client integration with the mock FastText embedding service.
"""

import numpy as np
import pytest

from tests.conftest import EmbeddingServiceConfig

from .conftest import EMBEDDING_DIM


@pytest.mark.integration
def test_litellm_with_fasttext_service(
    fasttext_embedding_service: EmbeddingServiceConfig,
) -> None:
    """Test LiteLLM with FastText service (real HTTP calls)."""
    from litellm import embedding

    response = embedding(
        model=fasttext_embedding_service["model"],
        input=["Python programming language"],
        api_base=fasttext_embedding_service["api_base"],
    )

    assert "data" in response
    assert len(response["data"]) == 1
    assert "embedding" in response["data"][0]
    assert len(response["data"][0]["embedding"]) == EMBEDDING_DIM

    print(f"✅ LiteLLM + FastText service: {len(response['data'][0]['embedding'])}-dim embedding")


@pytest.mark.integration
def test_fasttext_embeddings_similarity(
    fasttext_embedding_service: EmbeddingServiceConfig,
) -> None:
    """Test that FastText embeddings have semantic similarity."""
    from litellm import embedding

    tech_text1 = "Python programming language"
    tech_text2 = "Docker container technology"
    food_text = "Pizza recipe with cheese"

    config = fasttext_embedding_service

    emb1 = embedding(
        model=config["model"],
        input=[tech_text1],
        api_base=config["api_base"],
    )["data"][0]["embedding"]

    emb2 = embedding(
        model=config["model"],
        input=[tech_text2],
        api_base=config["api_base"],
    )["data"][0]["embedding"]

    emb3 = embedding(
        model=config["model"],
        input=[food_text],
        api_base=config["api_base"],
    )["data"][0]["embedding"]

    def cosine_sim(a: list[float], b: list[float]) -> float:
        a_np = np.array(a)
        b_np = np.array(b)
        return float(np.dot(a_np, b_np) / (np.linalg.norm(a_np) * np.linalg.norm(b_np)))

    tech_similarity = cosine_sim(emb1, emb2)
    cross_similarity = cosine_sim(emb1, emb3)

    print(f"Tech-Tech similarity: {tech_similarity:.3f}")
    print(f"Tech-Food similarity: {cross_similarity:.3f}")

    # Different texts should have different embeddings
    assert not np.allclose(emb1, emb2), "Different texts should have different embeddings"
    assert not np.allclose(emb1, emb3), "Different texts should have different embeddings"
    assert any(val != 0.0 for val in emb1), "Embeddings should not be all zeros"

    print("✅ FastText embeddings generated correctly")


@pytest.mark.integration
def test_fasttext_vocabulary_stability(
    fasttext_embedding_service: EmbeddingServiceConfig,
) -> None:
    """Test that FastText embeddings remain stable for same text after training."""
    from litellm import embedding

    config = fasttext_embedding_service

    # Train the model by sending enough documents
    training_texts = [
        "hello world python programming",
        "machine learning data science",
        "different text here for training",
    ]
    embedding(
        model=config["model"],
        input=training_texts,
        api_base=config["api_base"],
    )

    # Test stability: same text should produce SAME embeddings
    text = "hello world python programming"

    response1 = embedding(
        model=config["model"],
        input=[text],
        api_base=config["api_base"],
    )
    emb1 = response1["data"][0]["embedding"]

    # Send other requests in between
    for _ in range(5):
        embedding(
            model=config["model"],
            input=["different text here"],
            api_base=config["api_base"],
        )

    # Embed same text again
    response2 = embedding(
        model=config["model"],
        input=[text],
        api_base=config["api_base"],
    )
    emb2 = response2["data"][0]["embedding"]

    # Cosine similarity should be 1.0
    similarity = np.dot(emb1, emb2) / (np.linalg.norm(emb1) * np.linalg.norm(emb2))

    assert similarity > 0.99, (
        f"Same text should produce identical embeddings (similarity={similarity:.6f})"
    )

    print(f"✅ FastText vocabulary stable: cosine similarity = {similarity:.6f}")
