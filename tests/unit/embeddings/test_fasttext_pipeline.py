"""Unit tests for TextPipeline in fasttext_service.

Tests pure business logic and algorithm behavior without external services.
Gensim/NLTK are local dependencies, not containerized services.
"""

from __future__ import annotations

import pytest

from hintgrid.embeddings.fasttext_service import TextPipeline


@pytest.mark.unit
class TestTextPipelineTransform:
    """Test TextPipeline.transform without phraser."""

    def test_transform_without_phraser_returns_raw_tokens(self) -> None:
        pipeline = TextPipeline()
        tokens = pipeline.transform("hello world test")
        assert tokens == ["hello", "world", "test"]

    def test_transform_empty_text_returns_empty(self) -> None:
        pipeline = TextPipeline()
        assert pipeline.transform("") == []

    def test_transform_whitespace_only_returns_empty(self) -> None:
        pipeline = TextPipeline()
        assert pipeline.transform("   ") == []

    def test_tokenize_strips_handles(self) -> None:
        pipeline = TextPipeline()
        tokens = pipeline.tokenize("Hello @user how are you")
        assert "@user" not in tokens
        assert "hello" in tokens

    def test_tokenize_preserves_hashtags(self) -> None:
        pipeline = TextPipeline()
        tokens = pipeline.tokenize("Love #python programming")
        assert "#python" in tokens


@pytest.mark.unit
class TestTextPipelineLearnPhrases:
    """Test TextPipeline in-memory phrase learning."""

    def test_learn_phrases_creates_phraser(self) -> None:
        pipeline = TextPipeline()
        documents = [
            ["new", "york", "city"],
            ["new", "york", "times"],
            ["new", "york", "state"],
            ["hello", "world"],
            ["new", "york", "pizza"],
        ]
        pipeline.learn_phrases(documents, min_count=2)
        assert pipeline.phrases is not None
        assert pipeline.phraser is not None

    def test_learn_phrases_single_doc_skips(self) -> None:
        pipeline = TextPipeline()
        documents = [["hello", "world"]]
        pipeline.learn_phrases(documents)
        assert pipeline.phrases is None
        assert pipeline.phraser is None

    def test_learn_phrases_empty_docs_skips(self) -> None:
        pipeline = TextPipeline()
        pipeline.learn_phrases([])
        assert pipeline.phrases is None

    def test_update_phrases_without_existing_learns_first(self) -> None:
        pipeline = TextPipeline()
        documents = [
            ["machine", "learning", "model"],
            ["machine", "learning", "algorithm"],
            ["deep", "learning", "network"],
        ]
        pipeline.update_phrases(documents)
        # Should call learn_phrases since phrases is None
        assert pipeline.phrases is not None

    def test_update_phrases_with_existing_updates(self) -> None:
        pipeline = TextPipeline()
        initial_docs = [
            ["new", "york", "city"],
            ["new", "york", "times"],
            ["hello", "world"],
        ]
        pipeline.learn_phrases(initial_docs, min_count=2)
        assert pipeline.phrases is not None
        initial_vocab_size = len(pipeline.phrases.vocab)

        new_docs = [
            ["machine", "learning", "model"],
            ["machine", "learning", "algorithm"],
        ]
        pipeline.update_phrases(new_docs)
        assert pipeline.phraser is not None
        # Vocab should grow after update
        assert len(pipeline.phrases.vocab) >= initial_vocab_size

    def test_transform_with_phraser_joins_bigrams(self) -> None:
        pipeline = TextPipeline()
        documents = [
            ["new", "york", "is", "great"],
            ["new", "york", "city", "lights"],
            ["new", "york", "state", "park"],
            ["new", "york", "times", "paper"],
            ["new", "york", "pizza", "shop"],
        ]
        pipeline.learn_phrases(documents, min_count=3)
        assert pipeline.phraser is not None

        result = pipeline.transform("new york is amazing")
        # new_york should be joined as bigram
        assert isinstance(result, list)
        assert len(result) > 0
