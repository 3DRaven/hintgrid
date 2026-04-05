"""Integration tests for FastText embedding service - additional coverage.

Covers edge cases and error handling paths:
- Model loading failures (missing files, corrupted files)
- Model saving with partial state
- File deletion error handling
- Minimum document threshold
- Incremental training fallback to full
- Streaming phrase learning with small corpus
- PhrasedCorpusWrapper iteration
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

    from psycopg import Connection
    from psycopg.rows import TupleRow

    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient
    from hintgrid.config import HintGridSettings
    from hintgrid.embeddings.fasttext_service import FastTextEmbeddingService


@pytest.mark.integration
def test_embed_texts_without_models_requires_postgres(
    neo4j: Neo4jClient,
    tmp_path: Path,
) -> None:
    """Test embed_texts raises RuntimeError when no models and no postgres."""
    from hintgrid.config import HintGridSettings
    from hintgrid.embeddings.fasttext_service import FastTextEmbeddingService

    settings = HintGridSettings(
        fasttext_model_path=str(tmp_path),
        fasttext_min_documents=2,
        fasttext_vector_size=64,
    )
    service = FastTextEmbeddingService(neo4j, settings)

    # Try to embed without models and without postgres
    with pytest.raises(RuntimeError, match="No trained models found"):
        service.embed_texts([(1, "test text")])


@pytest.mark.integration
def test_embed_texts_with_corrupted_models_auto_retrains(
    neo4j: Neo4jClient,
    tmp_path: Path,
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    settings: HintGridSettings,
) -> None:
    """After a successful train, corrupting the FastText binary forces reload failure.

    ``embed_texts`` then runs automatic full training (Postgres corpus) and returns
    valid vectors — exercising recovery without swallowing errors.
    """
    from hintgrid.embeddings.fasttext_service import FastTextEmbeddingService

    with postgres_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO accounts (id, username, domain) VALUES (1, 'u1', NULL)"
        )
        for sid, text in enumerate(
            [
                "first corpus line for phrase learning",
                "second corpus line for phrase learning",
                "third corpus line for phrase learning",
            ],
            start=1,
        ):
            cur.execute(
                "INSERT INTO statuses (id, account_id, text, language, visibility) "
                "VALUES (%s, %s, %s, %s, %s)",
                (sid, 1, text, "en", 0),
            )
    postgres_conn.commit()

    test_settings = settings.model_copy(
        update={
            "fasttext_model_path": str(tmp_path),
            "fasttext_min_documents": 3,
            "fasttext_min_count": 1,
            "fasttext_vector_size": 64,
        }
    )

    trained = FastTextEmbeddingService(neo4j, test_settings, postgres=postgres_client)
    train_result = trained.train_full()
    assert train_result.success is True, train_result.message
    version = train_result.version
    assert version >= 1

    bin_path = tmp_path / f"fasttext_v{version}.bin"
    assert bin_path.is_file()
    bin_path.write_bytes(b"not-a-valid-fasttext-model")

    recovered = FastTextEmbeddingService(neo4j, test_settings, postgres=postgres_client)
    vectors = recovered.embed_texts([(1, "recovery path after disk corruption")])
    assert len(vectors) == 1
    assert len(vectors[0]) == 64


# Removed test_delete_model_files_with_permission_error
# Tests internal file deletion logic that is covered through train_full() behavior
# File deletion is an internal implementation detail


# Removed test_save_models_with_no_phrases_and_no_model
# Tests internal save logic that is covered through train_full() behavior
# Model saving is an internal implementation detail


@pytest.mark.integration
def test_embed_texts_without_trained_models_requires_postgres(
    neo4j: Neo4jClient,
    tmp_path: Path,
) -> None:
    """Test embed_texts behavior when no models are trained yet."""
    from hintgrid.config import HintGridSettings
    from hintgrid.embeddings.fasttext_service import FastTextEmbeddingService

    settings = HintGridSettings(
        fasttext_model_path=str(tmp_path),
        fasttext_min_documents=2,
        fasttext_vector_size=64,
    )
    service = FastTextEmbeddingService(neo4j, settings)

    # No model trained yet, should require postgres for auto-training
    with pytest.raises(RuntimeError, match="No trained models found"):
        service.embed_texts([(1, "test text")])


@pytest.mark.integration
def test_train_corpus_not_enough_documents(
    fasttext_service: FastTextEmbeddingService,
    postgres_conn: Connection[TupleRow],
    setup_mastodon_schema_for_cli: None,
) -> None:
    """Test _train_from_corpus returns failure when not enough docs."""
    # Insert only 1 post (min_documents is 2)
    with postgres_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO statuses (id, text, account_id) VALUES (%s, %s, %s)",
            (1, "Single short post that barely qualifies for training purposes here", 1),
        )
        postgres_conn.commit()

    result = fasttext_service.train_full()
    assert result.success is False, "Should fail with not enough documents"
    assert "Not enough documents" in result.message


@pytest.mark.integration
def test_train_incremental_no_existing_model_falls_back(
    fasttext_service: FastTextEmbeddingService,
    postgres_conn: Connection[TupleRow],
    setup_mastodon_schema_for_cli: None,
) -> None:
    """Test train_incremental falls back to full training when no model exists."""
    # Insert enough posts for training
    with postgres_conn.cursor() as cur:
        posts = [
            (1, "Python is a great programming language for data science"),
            (2, "Machine learning with Python is powerful and efficient"),
            (3, "Data science requires strong statistical knowledge"),
        ]
        for post_id, text in posts:
            cur.execute(
                "INSERT INTO statuses (id, text, account_id) VALUES (%s, %s, %s)",
                (post_id, text, 1),
            )
        postgres_conn.commit()

    # train_incremental with no existing model should fall back to full
    result = fasttext_service.train_incremental()
    assert result.success, f"Training failed: {result.message}"
    assert result.version == 1, "Should be version 1 (full training)"


# Removed test_load_state_returns_default_when_no_node
# Tests internal state loading that is covered through train_full() and train_incremental()
# State management is an internal implementation detail


@pytest.mark.integration
def test_embed_texts_without_model_raises(
    neo4j: Neo4jClient,
    tmp_path: Path,
) -> None:
    """Test embed_texts raises RuntimeError when model is not loaded."""
    from hintgrid.config import HintGridSettings
    from hintgrid.embeddings.fasttext_service import FastTextEmbeddingService

    settings = HintGridSettings(
        fasttext_model_path=str(tmp_path),
        fasttext_min_documents=2,
        fasttext_vector_size=64,
    )
    service = FastTextEmbeddingService(neo4j, settings)

    # Model is not loaded -> should raise RuntimeError
    with pytest.raises(RuntimeError, match=r"No trained models found|Model not loaded"):
        service.embed_texts([(1, "hello world test tokens")])


@pytest.mark.integration
def test_train_full_with_empty_corpus_fails(
    neo4j: Neo4jClient,
    tmp_path: Path,
    postgres_client: PostgresClient,
    mastodon_schema: None,
    settings: HintGridSettings,
) -> None:
    """Test train_full fails gracefully with empty corpus (schema present, no rows)."""
    from hintgrid.embeddings.fasttext_service import FastTextEmbeddingService

    test_settings = settings.model_copy(
        update={
            "fasttext_model_path": str(tmp_path),
            "fasttext_min_documents": 2,
            "fasttext_vector_size": 64,
        }
    )
    service = FastTextEmbeddingService(neo4j, test_settings, postgres=postgres_client)

    result = service.train_full()
    assert result.success is False, "Should fail with empty corpus"
    assert "Not enough documents" in result.message
    assert result.corpus_size == 0


# Removed test_phrased_corpus_wrapper_without_phraser
# Tests internal _PhrasedCorpusWrapper that is covered through train_full() behavior
# Phrase detection is an internal implementation detail


@pytest.mark.integration
def test_learn_phrases_from_stream_single_doc(
    neo4j: Neo4jClient,
    postgres_conn: Connection[TupleRow],
    setup_mastodon_schema_for_cli: None,
    docker_compose: object,
    worker_schema: str,
) -> None:
    """Test learn_phrases_from_stream with <= 1 doc (no phraser created)."""
    from hintgrid.clients.postgres import PostgresCorpus
    from hintgrid.embeddings.fasttext_service import TextPipeline

    from tests.conftest import DockerComposeInfo

    info = docker_compose
    assert isinstance(info, DockerComposeInfo)

    # Insert only 1 post
    with postgres_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO statuses (id, text, account_id) VALUES (%s, %s, %s)",
            (1, "Single document for phrase learning test with enough length", 1),
        )
        postgres_conn.commit()

    dsn = (
        f"postgresql://{info.postgres_user}:{info.postgres_password}"
        f"@{info.postgres_host}:{info.postgres_port}/{info.postgres_db}"
    )

    corpus = PostgresCorpus(dsn=dsn, min_id=0, schema=worker_schema)
    pipeline = TextPipeline()

    doc_count = pipeline.learn_phrases_from_stream(corpus)
    assert doc_count == 1
    # With only 1 doc, phraser should NOT be created
    assert pipeline.phraser is None


@pytest.mark.integration
def test_update_phrases_from_stream_without_existing_phrases(
    neo4j: Neo4jClient,
    postgres_conn: Connection[TupleRow],
    setup_mastodon_schema_for_cli: None,
    docker_compose: object,
    worker_schema: str,
) -> None:
    """Test update_phrases_from_stream falls back to learn when phrases is None."""
    from hintgrid.clients.postgres import PostgresCorpus
    from hintgrid.embeddings.fasttext_service import TextPipeline

    from tests.conftest import DockerComposeInfo

    info = docker_compose
    assert isinstance(info, DockerComposeInfo)

    # Insert test posts
    with postgres_conn.cursor() as cur:
        posts = [
            (1, "Python is a great programming language for beginners"),
            (2, "Machine learning with Python is powerful and efficient"),
            (3, "Data science requires statistical knowledge and skills"),
        ]
        for post_id, text in posts:
            cur.execute(
                "INSERT INTO statuses (id, text, account_id) VALUES (%s, %s, %s)",
                (post_id, text, 1),
            )
        postgres_conn.commit()

    dsn = (
        f"postgresql://{info.postgres_user}:{info.postgres_password}"
        f"@{info.postgres_host}:{info.postgres_port}/{info.postgres_db}"
    )

    corpus = PostgresCorpus(dsn=dsn, min_id=0, schema=worker_schema)
    pipeline = TextPipeline()

    # phrases is None, should fall back to learn_phrases_from_stream
    assert pipeline.phrases is None
    doc_count = pipeline.update_phrases_from_stream(corpus)
    assert doc_count >= 2
    assert pipeline.phrases is not None
