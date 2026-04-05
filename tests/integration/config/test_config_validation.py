"""Tests for configuration validation and embedding config check."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.config import (
    HintGridSettings,
    build_embedding_signature,
    validate_settings,
)
from hintgrid.exceptions import ConfigurationError
from hintgrid.pipeline.graph import (
    check_embedding_config,
    count_posts_in_neo4j,
    force_reindex,
    get_embedding_status,
    reembed_existing_posts,
)
from hintgrid.state import StateStore
from hintgrid.utils.coercion import coerce_int

if TYPE_CHECKING:
    from pathlib import Path

    from psycopg import Connection
    from psycopg.rows import TupleRow

    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.postgres import PostgresClient


def _seed_fasttext_training_corpus(postgres_conn: Connection[TupleRow], *, rows: int = 5) -> None:
    """Insert accounts and public statuses so FastText can train (min documents gate)."""
    texts = [
        "Neo4j stores the social graph for recommendation pipelines.",
        "PostgreSQL holds mastodon statuses streamed into embeddings.",
        "Redis caches personalized feeds with scored post identifiers.",
        "Vector indexes accelerate similarity search over post embeddings.",
        "Integration tests validate loaders against dockerized services.",
    ]
    if rows > len(texts):
        raise ValueError("rows exceeds seeded corpus templates")
    with postgres_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO accounts (id, username, domain) VALUES (100, 'a100', NULL), (101, 'a101', NULL)"
        )
        for i in range(rows):
            cur.execute(
                "INSERT INTO statuses (id, account_id, text, language, visibility) "
                "VALUES (%s, %s, %s, %s, %s)",
                (i + 1, 100 if i % 2 == 0 else 101, texts[i], "en", 0),
            )
    postgres_conn.commit()


class TestValidateSettings:
    """Tests for validate_settings() function."""

    def test_valid_default_settings(self) -> None:
        """Default settings should be valid."""
        settings = HintGridSettings()
        # Should not raise
        validate_settings(settings)

    def test_invalid_postgres_port(self) -> None:
        """Invalid postgres port should raise ConfigurationError."""
        settings = HintGridSettings(postgres_port=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "postgres_port" in str(exc_info.value)

    def test_invalid_postgres_port_too_high(self) -> None:
        """Port above 65535 should raise ConfigurationError."""
        settings = HintGridSettings(postgres_port=99999)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "postgres_port" in str(exc_info.value)

    def test_invalid_redis_db(self) -> None:
        """Invalid redis_db should raise ConfigurationError."""
        settings = HintGridSettings(redis_db=20)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "redis_db" in str(exc_info.value)

    def test_invalid_llm_dimensions_zero(self) -> None:
        """Zero llm_dimensions should raise ConfigurationError."""
        settings = HintGridSettings(llm_dimensions=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "llm_dimensions" in str(exc_info.value)

    def test_invalid_llm_dimensions_too_large(self) -> None:
        """Very large llm_dimensions should raise ConfigurationError."""
        settings = HintGridSettings(llm_dimensions=10000)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "llm_dimensions" in str(exc_info.value)

    def test_invalid_llm_provider(self) -> None:
        """Unknown llm_provider should raise ConfigurationError."""
        settings = HintGridSettings(llm_provider="unknown_provider")
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "llm_provider" in str(exc_info.value)

    def test_invalid_llm_base_url(self) -> None:
        """Invalid URL scheme should raise ConfigurationError."""
        settings = HintGridSettings(llm_base_url="ftp://invalid")
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "llm_base_url" in str(exc_info.value)

    def test_invalid_fasttext_vector_size(self) -> None:
        """Too small fasttext_vector_size should raise ConfigurationError."""
        settings = HintGridSettings(fasttext_vector_size=8)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "fasttext_vector_size" in str(exc_info.value)

    def test_invalid_batch_size_zero(self) -> None:
        """Zero batch_size should raise ConfigurationError."""
        settings = HintGridSettings(batch_size=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "batch_size" in str(exc_info.value)

    def test_invalid_batch_size_too_large(self) -> None:
        """Very large batch_size should raise ConfigurationError."""
        settings = HintGridSettings(batch_size=1_000_000)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "batch_size" in str(exc_info.value)

    def test_invalid_similarity_iterate_batch_size(self) -> None:
        """Zero similarity_iterate_batch_size should raise ConfigurationError."""
        settings = HintGridSettings(similarity_iterate_batch_size=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "similarity_iterate_batch_size" in str(exc_info.value)

    def test_invalid_leiden_resolution(self) -> None:
        """Zero or negative leiden_resolution should raise ConfigurationError."""
        settings = HintGridSettings(leiden_resolution=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "leiden_resolution" in str(exc_info.value)

    def test_invalid_similarity_threshold(self) -> None:
        """Similarity threshold outside 0-1 should raise ConfigurationError."""
        settings = HintGridSettings(similarity_threshold=1.5)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "similarity_threshold" in str(exc_info.value)

    def test_invalid_feed_ttl(self) -> None:
        """Invalid feed_ttl value should raise ConfigurationError."""
        settings = HintGridSettings(feed_ttl="invalid")
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "feed_ttl" in str(exc_info.value)

    def test_invalid_cold_start_fallback(self) -> None:
        """Invalid cold_start_fallback should raise ConfigurationError."""
        settings = HintGridSettings(cold_start_fallback="invalid")
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "cold_start_fallback" in str(exc_info.value)

    def test_invalid_personalized_weights_sum(self) -> None:
        """Personalized weights not summing to 1.0 should raise ConfigurationError."""
        settings = HintGridSettings(
            personalized_interest_weight=0.5,
            personalized_popularity_weight=0.5,
            personalized_recency_weight=0.5,
        )
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "Personalized weights" in str(exc_info.value)

    def test_invalid_cold_start_weights_sum(self) -> None:
        """Cold start weights not summing to 1.0 should raise ConfigurationError."""
        settings = HintGridSettings(
            cold_start_popularity_weight=0.5,
            cold_start_recency_weight=0.3,
        )
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "Cold start weights" in str(exc_info.value)

    def test_invalid_serendipity_probability(self) -> None:
        """Serendipity probability outside 0-1 should raise ConfigurationError."""
        settings = HintGridSettings(serendipity_probability=2.0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "serendipity_probability" in str(exc_info.value)

    def test_negative_weight(self) -> None:
        """Negative weights should raise ConfigurationError."""
        settings = HintGridSettings(likes_weight=-1.0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "likes_weight" in str(exc_info.value)

    def test_invalid_log_level(self) -> None:
        """Invalid log level should raise ConfigurationError."""
        settings = HintGridSettings(log_level="VERBOSE")
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "log_level" in str(exc_info.value)

    def test_invalid_llm_batch_size_zero(self) -> None:
        """Zero llm_batch_size should raise ConfigurationError."""
        settings = HintGridSettings(llm_batch_size=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "llm_batch_size" in str(exc_info.value)

    def test_invalid_llm_batch_size_too_large(self) -> None:
        """Very large llm_batch_size should raise ConfigurationError."""
        settings = HintGridSettings(llm_batch_size=20_000)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "llm_batch_size" in str(exc_info.value)

    def test_valid_llm_batch_size(self) -> None:
        """Valid llm_batch_size should not raise."""
        settings = HintGridSettings(llm_batch_size=256)
        validate_settings(settings)

    def test_multiple_errors_collected(self) -> None:
        """Multiple validation errors should be collected and reported together."""
        settings = HintGridSettings(
            postgres_port=0,
            redis_db=100,
            llm_dimensions=0,
        )
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        error_message = str(exc_info.value)
        assert "postgres_port" in error_message
        assert "redis_db" in error_message
        assert "llm_dimensions" in error_message


class TestBuildEmbeddingSignature:
    """Tests for build_embedding_signature() function."""

    def test_fasttext_signature(self) -> None:
        """FastText provider should use fasttext_vector_size."""
        settings = HintGridSettings(
            llm_provider="fasttext",
            llm_base_url=None,
            fasttext_vector_size=128,
        )
        signature = build_embedding_signature(settings)
        assert signature == "fasttext:nomic-embed-text:128"

    def test_no_base_url_uses_fasttext_size(self) -> None:
        """Without base_url, fasttext_vector_size should be used."""
        settings = HintGridSettings(
            llm_provider="ollama",
            llm_base_url=None,
            llm_model="test-model",
            fasttext_vector_size=64,
            llm_dimensions=768,
        )
        signature = build_embedding_signature(settings)
        assert signature == "ollama:test-model:64"

    def test_with_base_url_uses_llm_dimensions(self) -> None:
        """With base_url, llm_dimensions should be used."""
        settings = HintGridSettings(
            llm_provider="openai",
            llm_base_url="http://localhost:8080",
            llm_model="text-embedding-3-small",
            llm_dimensions=768,
            fasttext_vector_size=128,
        )
        signature = build_embedding_signature(settings)
        assert signature == "openai:text-embedding-3-small:768"

    def test_signature_format(self) -> None:
        """Signature should be in 'provider:model:dimension' format."""
        settings = HintGridSettings(
            llm_provider="ollama",
            llm_base_url="http://localhost:11434",
            llm_model="nomic-embed-text",
            llm_dimensions=768,
        )
        signature = build_embedding_signature(settings)
        parts = signature.split(":")
        assert len(parts) == 3
        assert parts[0] == "ollama"
        assert parts[1] == "nomic-embed-text"
        assert parts[2] == "768"


@pytest.mark.integration
class TestCheckEmbeddingConfig:
    """Tests for check_embedding_config() and related functions.

    Uses shared docker-compose Neo4j container via 'neo4j' fixture.
    """

    def test_first_run_records_signature(self, neo4j: Neo4jClient) -> None:
        """First run should record the signature without migration."""
        settings = HintGridSettings(
            llm_provider="fasttext",
            fasttext_vector_size=128,
        )
        state_store = StateStore(neo4j)

        # Clear any existing signature
        state = state_store.load()
        state.embedding_signature = ""
        state_store.save(state)

        result = check_embedding_config(neo4j, settings, state_store)

        assert result.migrated is False
        assert result.previous_signature is None
        assert result.current_signature == "fasttext:nomic-embed-text:128"

        # Verify signature was saved
        new_state = state_store.load()
        assert new_state.embedding_signature == "fasttext:nomic-embed-text:128"

    def test_same_signature_no_migration(self, neo4j: Neo4jClient) -> None:
        """Same signature should not trigger migration."""

        settings = HintGridSettings(
            llm_provider="fasttext",
            fasttext_vector_size=128,
        )
        state_store = StateStore(neo4j)

        # Set existing signature
        state = state_store.load()
        state.embedding_signature = "fasttext:nomic-embed-text:128"
        state_store.save(state)

        result = check_embedding_config(neo4j, settings, state_store)

        assert result.migrated is False
        assert result.previous_signature == "fasttext:nomic-embed-text:128"
        assert result.current_signature == "fasttext:nomic-embed-text:128"

    def test_changed_signature_triggers_migration(self, neo4j: Neo4jClient) -> None:
        """Changed signature should trigger migration."""

        settings = HintGridSettings(
            llm_provider="openai",
            llm_base_url="http://localhost:8080",
            llm_model="text-embedding-3-small",
            llm_dimensions=768,
        )
        state_store = StateStore(neo4j)

        # Set old signature (different from current)
        state = state_store.load()
        state.embedding_signature = "fasttext:local:128"
        state_store.save(state)

        result = check_embedding_config(neo4j, settings, state_store)

        assert result.migrated is True
        assert result.previous_signature == "fasttext:local:128"
        assert result.current_signature == "openai:text-embedding-3-small:768"

        # Verify cursor was reset
        new_state = state_store.load()
        assert new_state.last_status_id == 0
        assert new_state.embedding_signature == "openai:text-embedding-3-small:768"

    def test_first_run_clears_stale_embeddings(self, neo4j: Neo4jClient) -> None:
        """First run with existing embeddings must trigger migration to avoid dimension mismatch.

        Scenario: Posts already have embeddings from a prior configuration (e.g. 768-dim
        from an LLM provider), but no embedding signature is stored (state was reset).
        Without clearing, the new 128-dim vector index would conflict with old 768-dim
        embeddings causing db.index.vector.queryNodes to fail.
        """
        settings = HintGridSettings(
            llm_provider="fasttext",
            fasttext_vector_size=128,
        )
        state_store = StateStore(neo4j)

        # Clear any existing signature (simulate first run)
        state = state_store.load()
        state.embedding_signature = ""
        state_store.save(state)

        # Create a Post node with a stale embedding (simulating prior config)
        neo4j.execute_labeled(
            "CREATE (p:__post__ {id: 99999, text: 'stale embedding post', "
            "embedding: [0.1, 0.2, 0.3]})",
            {"post": "Post"},
        )

        result = check_embedding_config(neo4j, settings, state_store)

        # Migration must be triggered to clear stale embeddings
        assert result.migrated is True
        assert result.previous_signature is None
        assert result.current_signature == "fasttext:nomic-embed-text:128"
        assert result.posts_cleared > 0

        # Verify signature was saved
        new_state = state_store.load()
        assert new_state.embedding_signature == "fasttext:nomic-embed-text:128"

        # Verify stale embeddings were actually cleared
        remaining = list(neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) WHERE p.embedding IS NOT NULL "
            "RETURN count(p) AS count",
            {"post": "Post"},
        ))
        remaining_count = coerce_int(remaining[0]["count"]) if remaining else 0
        assert remaining_count == 0, "All stale embeddings must be cleared"

    def test_get_embedding_status_first_run(self, neo4j: Neo4jClient) -> None:
        """get_embedding_status on first run returns empty stored signature."""

        settings = HintGridSettings(
            llm_provider="fasttext",
            fasttext_vector_size=128,
        )
        state_store = StateStore(neo4j)

        # Clear signature
        state = state_store.load()
        state.embedding_signature = ""
        state_store.save(state)

        status = get_embedding_status(settings, state_store)

        assert status["stored_signature"] == "(not set)"
        assert status["current_signature"] == "fasttext:nomic-embed-text:128"
        assert status["match"] is True  # First run is considered matching

    def test_get_embedding_status_match(self, neo4j: Neo4jClient) -> None:
        """get_embedding_status should show match when signatures are equal."""

        settings = HintGridSettings(
            llm_provider="fasttext",
            fasttext_vector_size=128,
        )
        state_store = StateStore(neo4j)

        # Set matching signature
        state = state_store.load()
        state.embedding_signature = "fasttext:nomic-embed-text:128"
        state_store.save(state)

        status = get_embedding_status(settings, state_store)

        assert status["match"] is True

    def test_get_embedding_status_mismatch(self, neo4j: Neo4jClient) -> None:
        """get_embedding_status should show mismatch when signatures differ."""

        settings = HintGridSettings(
            llm_provider="openai",
            llm_base_url="http://localhost:8080",
            llm_dimensions=768,
        )
        state_store = StateStore(neo4j)

        # Set different signature
        state = state_store.load()
        state.embedding_signature = "fasttext:local:128"
        state_store.save(state)

        status = get_embedding_status(settings, state_store)

        assert status["match"] is False

    def test_force_reindex_dry_run(self, neo4j: Neo4jClient) -> None:
        """force_reindex with dry_run should not make changes."""

        settings = HintGridSettings(
            llm_provider="fasttext",
            fasttext_vector_size=128,
        )
        state_store = StateStore(neo4j)

        # Set some signature
        state = state_store.load()
        state.embedding_signature = "old:signature:100"
        original_signature = state.embedding_signature
        state_store.save(state)

        result = force_reindex(neo4j, settings, state_store, dry_run=True)

        assert result.migrated is False

        # Verify no changes were made
        new_state = state_store.load()
        assert new_state.embedding_signature == original_signature

    def test_force_reindex(self, neo4j: Neo4jClient) -> None:
        """force_reindex should clear embeddings and update signature."""

        settings = HintGridSettings(
            llm_provider="fasttext",
            fasttext_vector_size=128,
        )
        state_store = StateStore(neo4j)

        # Set some signature
        state = state_store.load()
        state.embedding_signature = "old:signature:100"
        state_store.save(state)

        result = force_reindex(neo4j, settings, state_store, dry_run=False)

        assert result.migrated is True
        assert result.current_signature == "fasttext:nomic-embed-text:128"

        # Verify changes were made
        new_state = state_store.load()
        assert new_state.embedding_signature == "fasttext:nomic-embed-text:128"
        # Note: last_status_id is NOT reset - we preserve incremental position


@pytest.mark.integration
class TestReembedExistingPosts:
    """Tests for reembed_existing_posts() function.

    Uses shared docker-compose Neo4j container via 'neo4j' fixture.
    """

    def test_reembed_empty_database(
        self, neo4j: Neo4jClient, postgres_client: PostgresClient, settings: HintGridSettings
    ) -> None:
        """Reembedding empty database should return 0."""
        from hintgrid.embeddings.provider import EmbeddingProvider

        test_settings = settings.model_copy(update={"fasttext_vector_size": 128})
        provider = EmbeddingProvider(test_settings, neo4j, postgres_client)

        count = reembed_existing_posts(neo4j, provider, test_settings)

        assert count == 0

    def test_reembed_with_posts(
        self,
        neo4j: Neo4jClient,
        postgres_client: PostgresClient,
        postgres_conn: Connection[TupleRow],
        mastodon_schema: None,
        settings: HintGridSettings,
        tmp_path: Path,
    ) -> None:
        """Reembedding should update embeddings for existing posts.

        ``EmbeddingProvider`` with Postgres triggers FastText auto-train on first
        embed when no model exists; training streams ``statuses`` in the worker
        schema, so we seed a corpus and align ``fasttext_min_documents``.
        """
        from hintgrid.embeddings.provider import EmbeddingProvider

        _seed_fasttext_training_corpus(postgres_conn, rows=5)
        test_settings = settings.model_copy(
            update={
                "fasttext_vector_size": 128,
                "fasttext_min_documents": 5,
                "fasttext_min_count": 1,
                "fasttext_model_path": str(tmp_path / "reembed_models"),
            }
        )
        neo4j.label("Post")

        neo4j.execute_labeled(
            "CREATE (p1:__post__ {id: 1, text: 'Hello world', authorId: 100}) "
            "CREATE (p2:__post__ {id: 2, text: 'Test post', authorId: 100}) "
            "CREATE (p3:__post__ {id: 3, text: 'Another test', authorId: 101})",
            {"post": "Post"},
        )

        provider = EmbeddingProvider(test_settings, neo4j, postgres_client)

        count = reembed_existing_posts(neo4j, provider, test_settings, batch_size=10)

        assert count == 3

        result = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (p:__post__) WHERE p.embedding IS NOT NULL RETURN count(p) AS count",
                {"post": "Post"},
            )
        )
        assert result[0]["count"] == 3

    def test_reembed_preserves_relationships(
        self,
        neo4j: Neo4jClient,
        postgres_client: PostgresClient,
        postgres_conn: Connection[TupleRow],
        mastodon_schema: None,
        settings: HintGridSettings,
        tmp_path: Path,
    ) -> None:
        """Reembedding should preserve all relationships."""
        from hintgrid.embeddings.provider import EmbeddingProvider

        _seed_fasttext_training_corpus(postgres_conn, rows=5)
        test_settings = settings.model_copy(
            update={
                "fasttext_vector_size": 128,
                "fasttext_min_documents": 5,
                "fasttext_min_count": 1,
                "fasttext_model_path": str(tmp_path / "reembed_models_rel"),
            }
        )
        neo4j.label("User")
        neo4j.label("Post")

        neo4j.execute_labeled(
            "CREATE (u:__user__ {id: 100}) "
            "CREATE (p1:__post__ {id: 1, text: 'Hello', authorId: 100}) "
            "CREATE (p2:__post__ {id: 2, text: 'World', authorId: 100}) "
            "CREATE (u)-[:WROTE]->(p1) "
            "CREATE (u)-[:WROTE]->(p2) "
            "CREATE (u)-[:FAVORITED]->(p2)",
            {"user": "User", "post": "Post"},
        )

        wrote_before = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (:__user__)-[r:WROTE]->(:__post__) RETURN count(r) AS count",
                {"user": "User", "post": "Post"},
            )
        )
        assert wrote_before[0]["count"] == 2

        fav_before = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (:__user__)-[r:FAVORITED]->(:__post__) RETURN count(r) AS count",
                {"user": "User", "post": "Post"},
            )
        )
        assert fav_before[0]["count"] == 1

        provider = EmbeddingProvider(test_settings, neo4j, postgres_client)

        count = reembed_existing_posts(neo4j, provider, test_settings, batch_size=10)
        assert count == 2

        wrote_after = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (:__user__)-[r:WROTE]->(:__post__) RETURN count(r) AS count",
                {"user": "User", "post": "Post"},
            )
        )
        assert wrote_after[0]["count"] == 2

        fav_after = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (:__user__)-[r:FAVORITED]->(:__post__) RETURN count(r) AS count",
                {"user": "User", "post": "Post"},
            )
        )
        assert fav_after[0]["count"] == 1

    def test_count_posts_in_neo4j(self, neo4j: Neo4jClient) -> None:
        """count_posts_in_neo4j should return correct count."""

        neo4j.label("Post")

        # Empty database
        count = count_posts_in_neo4j(neo4j)
        assert count == 0

        # Add posts (worker-isolated labels)
        neo4j.execute_labeled(
            "CREATE (:__post__ {id: 1, text: 'Test 1'}) "
            "CREATE (:__post__ {id: 2, text: 'Test 2'}) "
            "CREATE (:__post__ {id: 3, text: 'Test 3'})",
            {"post": "Post"},
        )

        count = count_posts_in_neo4j(neo4j)
        assert count == 3
