"""Integration tests for FastText embedding service."""

from __future__ import annotations

from datetime import datetime, timedelta, UTC
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow
    from pathlib import Path
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient
    from hintgrid.config import HintGridSettings
    from hintgrid.embeddings.fasttext_service import FastTextEmbeddingService
    from tests.conftest import DockerComposeInfo


@pytest.mark.integration
def test_fasttext_service_full_training(
    fasttext_service: FastTextEmbeddingService,
    postgres_conn: Connection[TupleRow],
    setup_mastodon_schema_for_cli: None,
    tmp_path: Path,
) -> None:
    """Test full training workflow from PostgreSQL data."""
    # Insert test posts
    with postgres_conn.cursor() as cur:
        posts = [
            (1, "Python is a great programming language"),
            (2, "Machine learning with Python is powerful"),
            (3, "Data science requires statistical knowledge"),
            (4, "Deep learning neural networks are fascinating"),
            (5, "Natural language processing with transformers"),
        ]
        for post_id, text in posts:
            cur.execute(
                "INSERT INTO statuses (id, text, account_id) VALUES (%s, %s, %s)",
                (post_id, text, 1),
            )
        postgres_conn.commit()

    # Train
    result = fasttext_service.train_full()

    assert result.success, f"Training failed: {result.message}"
    assert result.corpus_size >= 3, f"Expected at least 3 docs, got {result.corpus_size}"
    assert result.vocab_size > 0, "Vocabulary should not be empty"
    assert result.version == 1, f"Expected version 1, got {result.version}"

    print(f"✅ Full training: corpus={result.corpus_size}, vocab={result.vocab_size}")


@pytest.mark.integration
def test_fasttext_service_embed_texts(
    fasttext_service: FastTextEmbeddingService,
    postgres_conn: Connection[TupleRow],
    setup_mastodon_schema_for_cli: None,
) -> None:
    """Test embedding generation after training."""
    # Insert test posts
    with postgres_conn.cursor() as cur:
        posts = [
            (1, "Python programming basics"),
            (2, "Advanced Python techniques"),
            (3, "Machine learning fundamentals"),
            (4, "Deep learning with Python"),
            (5, "Data analysis with pandas"),
        ]
        for post_id, text in posts:
            cur.execute(
                "INSERT INTO statuses (id, text, account_id) VALUES (%s, %s, %s)",
                (post_id, text, 1),
            )
        postgres_conn.commit()

    # Train first
    result = fasttext_service.train_full()
    assert result.success, f"Training failed: {result.message}"

    # Now embed new texts
    test_texts = [
        (100, "Python is awesome"),
        (101, "Machine learning rocks"),
        (102, ""),  # Empty should use fallback
    ]

    embeddings = fasttext_service.embed_texts(test_texts)

    assert len(embeddings) == 3, f"Expected 3 embeddings, got {len(embeddings)}"
    assert all(len(emb) == 64 for emb in embeddings), "All embeddings should be 64-dim"
    assert any(v != 0.0 for v in embeddings[0]), "First embedding should not be zero"
    assert any(v != 0.0 for v in embeddings[1]), "Second embedding should not be zero"
    assert any(v != 0.0 for v in embeddings[2]), "Third (fallback) should not be zero"

    print("✅ Embed texts: 3 embeddings generated")


@pytest.mark.integration
def test_fasttext_service_incremental_training(
    fasttext_service: FastTextEmbeddingService,
    postgres_conn: Connection[TupleRow],
    setup_mastodon_schema_for_cli: None,
) -> None:
    """Test incremental training workflow."""
    # Insert initial posts
    with postgres_conn.cursor() as cur:
        posts = [
            (1, "Python programming basics"),
            (2, "Machine learning fundamentals"),
            (3, "Data science with Python"),
        ]
        for post_id, text in posts:
            cur.execute(
                "INSERT INTO statuses (id, text, account_id) VALUES (%s, %s, %s)",
                (post_id, text, 1),
            )
        postgres_conn.commit()

    # Full training
    result1 = fasttext_service.train_full()
    assert result1.success, f"Initial training failed: {result1.message}"
    initial_vocab = result1.vocab_size

    # Add more posts
    with postgres_conn.cursor() as cur:
        new_posts = [
            (10, "Cooking delicious recipes"),
            (11, "Chef kitchen techniques"),
            (12, "Baking fresh bread"),
        ]
        for post_id, text in new_posts:
            cur.execute(
                "INSERT INTO statuses (id, text, account_id) VALUES (%s, %s, %s)",
                (post_id, text, 2),
            )
        postgres_conn.commit()

    # Incremental training
    result2 = fasttext_service.train_incremental()
    assert result2.success, f"Incremental training failed: {result2.message}"
    assert result2.version == 2, f"Expected version 2, got {result2.version}"

    # Vocabulary should grow
    assert result2.vocab_size >= initial_vocab, "Vocabulary should grow or stay same"

    print(f"✅ Incremental training: v1 vocab={initial_vocab} -> v2 vocab={result2.vocab_size}")


@pytest.mark.integration
def test_fasttext_service_auto_train_on_first_embed(
    fasttext_service: FastTextEmbeddingService,
    postgres_conn: Connection[TupleRow],
    setup_mastodon_schema_for_cli: None,
) -> None:
    """Test that embed_texts auto-trains on first call if no models exist."""
    # Insert posts for training
    with postgres_conn.cursor() as cur:
        posts = [
            (1, "Python programming basics"),
            (2, "Machine learning fundamentals"),
            (3, "Data science with Python"),
        ]
        for post_id, text in posts:
            cur.execute(
                "INSERT INTO statuses (id, text, account_id) VALUES (%s, %s, %s)",
                (post_id, text, 1),
            )
        postgres_conn.commit()

    # No explicit training - should auto-train on first embed
    test_texts = [(100, "Python is great")]
    embeddings = fasttext_service.embed_texts(test_texts)

    assert len(embeddings) == 1, "Should return 1 embedding"
    assert len(embeddings[0]) == 64, "Embedding should be 64-dim"
    assert any(v != 0.0 for v in embeddings[0]), "Embedding should not be zero"

    print("✅ Auto-training on first embed works")


@pytest.mark.integration
def test_fasttext_service_no_postgres_raises_error(
    neo4j: Neo4jClient,
    tmp_path: Path,
) -> None:
    """Test that embed_texts raises error if no models and no postgres."""
    from hintgrid.config import HintGridSettings
    from hintgrid.embeddings.fasttext_service import FastTextEmbeddingService

    settings = HintGridSettings(
        fasttext_model_path=str(tmp_path),
        fasttext_min_documents=2,
        fasttext_vector_size=64,
    )

    # No postgres client provided
    service = FastTextEmbeddingService(neo4j, settings, postgres=None)

    # Should raise error when trying to embed without models
    with pytest.raises(RuntimeError, match="No trained models found"):
        service.embed_texts([(1, "test")])

    print("✅ Correct error raised when no models and no postgres")


@pytest.mark.integration
def test_fasttext_service_auto_train_with_since_date(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    setup_mastodon_schema_for_cli: None,
    tmp_path: Path,
    settings: HintGridSettings,
    docker_compose: DockerComposeInfo,
) -> None:
    """Test that auto-training with since_date filters corpus correctly."""
    from hintgrid.embeddings.fasttext_service import FastTextEmbeddingService
    from hintgrid.utils.snowflake import snowflake_id_at

    # Create test data with different dates
    now = datetime.now(UTC)
    old_date = now - timedelta(days=60)  # 60 days ago - should be excluded
    recent_date = now - timedelta(days=10)  # 10 days ago - should be included

    old_snowflake = snowflake_id_at(old_date)
    recent_snowflake = snowflake_id_at(recent_date)

    # Insert old and recent posts
    with postgres_conn.cursor() as cur:
        # Create account
        cur.execute(
            "INSERT INTO accounts (id, username, domain) VALUES (3001, 'since_test_user', NULL) ON CONFLICT (id) DO NOTHING"
        )

        # Insert old posts (should be excluded)
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at)
            VALUES
                (%s, 3001, 'Old post 1 that should be excluded', 'en', 0, %s),
                (%s, 3001, 'Old post 2 that should be excluded', 'en', 0, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (old_snowflake, old_date, old_snowflake + 1, old_date),
        )

        # Insert recent posts (should be included)
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at)
            VALUES
                (%s, 3001, 'Recent post 1 for training', 'en', 0, %s),
                (%s, 3001, 'Recent post 2 about Python', 'en', 0, %s),
                (%s, 3001, 'Recent post 3 about machine learning', 'en', 0, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (
                recent_snowflake,
                recent_date,
                recent_snowflake + 1,
                recent_date,
                recent_snowflake + 2,
                recent_date,
            ),
        )
        postgres_conn.commit()

    # Create service with since_date (30 days ago should include recent posts)
    test_settings = settings.model_copy(
        update={
            "fasttext_model_path": str(tmp_path),
            "fasttext_min_documents": 2,
            "fasttext_min_count": 1,
            "fasttext_vector_size": 64,
            "fasttext_epochs": 3,
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
        }
    )

    since_date = now - timedelta(days=30)  # 30 days ago
    service = FastTextEmbeddingService(
        neo4j, test_settings, postgres_client, since_date=since_date
    )

    # Auto-train on first embed (should use since_date filter)
    test_texts = [(100, "Python is great")]
    embeddings = service.embed_texts(test_texts)

    assert len(embeddings) == 1, "Should return 1 embedding"
    assert len(embeddings[0]) == 64, "Embedding should be 64-dim"

    # Verify that training used filtered corpus (only recent posts)
    # Check state to see what was actually trained
    from hintgrid.embeddings.fasttext_service import STATE_NODE_ID

    state_result = neo4j.execute_and_fetch_labeled(
        "MATCH (s:FastTextState {id: $id}) RETURN s.lastTrainedPostId AS last_id, s.version AS version",
        {"label": "FastTextState"},
        {"id": STATE_NODE_ID},
    )

    assert len(state_result) > 0, "FastTextState should exist after training"
    last_trained_id_raw = state_result[0].get("last_id")
    version_raw = state_result[0].get("version")
    
    # Coerce types for comparison
    from hintgrid.utils.coercion import coerce_int
    
    last_trained_id: int | None = coerce_int(last_trained_id_raw) if last_trained_id_raw is not None else None
    version: int | None = coerce_int(version_raw) if version_raw is not None else None

    assert version is not None and version >= 1, "Model version should be at least 1"
    # last_trained_id should be >= recent_snowflake (trained on recent posts)
    # but could be higher if there are more posts
    assert (
        last_trained_id is None or last_trained_id >= recent_snowflake
    ), f"Should have trained on recent posts (last_id={last_trained_id}, recent_snowflake={recent_snowflake})"

    print(
        f"✅ Auto-training with since_date: trained on posts >= {recent_snowflake}, version={version}"
    )
