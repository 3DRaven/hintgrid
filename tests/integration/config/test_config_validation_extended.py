"""Extended configuration validation tests for uncovered branches."""

from __future__ import annotations

import pytest

from hintgrid.config import HintGridSettings, validate_settings
from hintgrid.exceptions import ConfigurationError


class TestPostgresValidation:
    """Tests for PostgreSQL configuration validation branches."""

    def test_empty_postgres_database(self) -> None:
        """Empty postgres_database should raise ConfigurationError."""
        settings = HintGridSettings(postgres_database="")
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "postgres_database" in str(exc_info.value)

    def test_empty_postgres_user(self) -> None:
        """Empty postgres_user should raise ConfigurationError."""
        settings = HintGridSettings(postgres_user="")
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "postgres_user" in str(exc_info.value)

    def test_invalid_pg_pool_min_size(self) -> None:
        """pg_pool_min_size < 1 should raise ConfigurationError."""
        settings = HintGridSettings(pg_pool_min_size=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "pg_pool_min_size" in str(exc_info.value)

    def test_pg_pool_max_less_than_min(self) -> None:
        """pg_pool_max_size < pg_pool_min_size should raise ConfigurationError."""
        settings = HintGridSettings(pg_pool_min_size=10, pg_pool_max_size=5)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "pg_pool_max_size" in str(exc_info.value)

    def test_invalid_pg_pool_timeout(self) -> None:
        """pg_pool_timeout_seconds < 1 should raise ConfigurationError."""
        settings = HintGridSettings(pg_pool_timeout_seconds=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "pg_pool_timeout" in str(exc_info.value)


class TestNeo4jValidation:
    """Tests for Neo4j configuration validation branches."""

    def test_invalid_neo4j_port_too_low(self) -> None:
        """neo4j_port < 1 should raise ConfigurationError."""
        settings = HintGridSettings(neo4j_port=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "neo4j_port" in str(exc_info.value)

    def test_invalid_neo4j_port_too_high(self) -> None:
        """neo4j_port > 65535 should raise ConfigurationError."""
        settings = HintGridSettings(neo4j_port=70000)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "neo4j_port" in str(exc_info.value)

    def test_empty_neo4j_password(self) -> None:
        """Empty neo4j_password should raise ConfigurationError."""
        settings = HintGridSettings(neo4j_password="")
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "neo4j_password" in str(exc_info.value)

    def test_invalid_neo4j_ready_retries(self) -> None:
        """neo4j_ready_retries < 1 should raise ConfigurationError."""
        settings = HintGridSettings(neo4j_ready_retries=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "neo4j_ready_retries" in str(exc_info.value)

    def test_invalid_neo4j_ready_sleep_seconds(self) -> None:
        """neo4j_ready_sleep_seconds < 0 should raise ConfigurationError."""
        settings = HintGridSettings(neo4j_ready_sleep_seconds=-1)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "neo4j_ready_sleep_seconds" in str(exc_info.value)

    def test_invalid_neo4j_worker_label_special_chars(self) -> None:
        """Worker label with special chars should raise ConfigurationError."""
        settings = HintGridSettings(neo4j_worker_label="test-label!")
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "neo4j_worker_label" in str(exc_info.value)


class TestRedisValidation:
    """Tests for Redis configuration validation branches."""

    def test_invalid_redis_port_too_low(self) -> None:
        """redis_port < 1 should raise ConfigurationError."""
        settings = HintGridSettings(redis_port=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "redis_port" in str(exc_info.value)

    def test_invalid_redis_port_too_high(self) -> None:
        """redis_port > 65535 should raise ConfigurationError."""
        settings = HintGridSettings(redis_port=70000)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "redis_port" in str(exc_info.value)

    def test_invalid_redis_db_negative(self) -> None:
        """Negative redis_db should raise ConfigurationError."""
        settings = HintGridSettings(redis_db=-1)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "redis_db" in str(exc_info.value)


class TestFastTextValidation:
    """Tests for FastText configuration validation branches."""

    def test_invalid_fasttext_window(self) -> None:
        """fasttext_window < 1 should raise ConfigurationError."""
        settings = HintGridSettings(fasttext_window=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "fasttext_window" in str(exc_info.value)

    def test_invalid_fasttext_min_count(self) -> None:
        """fasttext_min_count < 1 should raise ConfigurationError."""
        settings = HintGridSettings(fasttext_min_count=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "fasttext_min_count" in str(exc_info.value)

    def test_invalid_fasttext_epochs(self) -> None:
        """fasttext_epochs < 1 should raise ConfigurationError."""
        settings = HintGridSettings(fasttext_epochs=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "fasttext_epochs" in str(exc_info.value)

    def test_invalid_fasttext_bucket(self) -> None:
        """fasttext_bucket < 1000 should raise ConfigurationError."""
        settings = HintGridSettings(fasttext_bucket=500)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "fasttext_bucket" in str(exc_info.value)

    def test_invalid_fasttext_min_documents(self) -> None:
        """fasttext_min_documents < 1 should raise ConfigurationError."""
        settings = HintGridSettings(fasttext_min_documents=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "fasttext_min_documents" in str(exc_info.value)

    def test_invalid_fasttext_vector_size_too_large(self) -> None:
        """fasttext_vector_size > 1024 should raise ConfigurationError."""
        settings = HintGridSettings(fasttext_vector_size=2000)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "fasttext_vector_size" in str(exc_info.value)


class TestLLMValidation:
    """Tests for LLM configuration validation branches."""

    def test_invalid_llm_timeout(self) -> None:
        """llm_timeout < 1 should raise ConfigurationError."""
        settings = HintGridSettings(llm_timeout=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "llm_timeout" in str(exc_info.value)

    def test_invalid_llm_max_retries(self) -> None:
        """llm_max_retries < 0 should raise ConfigurationError."""
        settings = HintGridSettings(llm_max_retries=-1)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "llm_max_retries" in str(exc_info.value)


class TestPipelineValidation:
    """Tests for pipeline configuration validation branches."""

    def test_invalid_max_retries(self) -> None:
        """max_retries < 0 should raise ConfigurationError."""
        settings = HintGridSettings(max_retries=-1)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "max_retries" in str(exc_info.value)

    def test_invalid_checkpoint_interval(self) -> None:
        """checkpoint_interval < 1 should raise ConfigurationError."""
        settings = HintGridSettings(checkpoint_interval=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "checkpoint_interval" in str(exc_info.value)


class TestClusteringValidation:
    """Tests for clustering configuration validation branches."""

    def test_invalid_leiden_max_levels(self) -> None:
        """leiden_max_levels < 1 should raise ConfigurationError."""
        settings = HintGridSettings(leiden_max_levels=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "leiden_max_levels" in str(exc_info.value)

    def test_invalid_noise_community_id_zero(self) -> None:
        """noise_community_id 0 is reserved for empty-graph fallback; must raise."""
        settings = HintGridSettings(noise_community_id=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "noise_community_id" in str(exc_info.value)

    def test_invalid_singleton_collapse_in_transactions_of_negative(self) -> None:
        """singleton_collapse_in_transactions_of < 0 must raise."""
        settings = HintGridSettings(singleton_collapse_in_transactions_of=-1)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "singleton_collapse_in_transactions_of" in str(exc_info.value)

    def test_invalid_singleton_collapse_in_transactions_of_too_large(self) -> None:
        """singleton_collapse_in_transactions_of above max must raise."""
        settings = HintGridSettings(singleton_collapse_in_transactions_of=100_001)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "singleton_collapse_in_transactions_of" in str(exc_info.value)

    def test_invalid_knn_neighbors(self) -> None:
        """knn_neighbors < 1 should raise ConfigurationError."""
        settings = HintGridSettings(knn_neighbors=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "knn_neighbors" in str(exc_info.value)

    def test_invalid_knn_self_neighbor_offset(self) -> None:
        """knn_self_neighbor_offset < 0 should raise ConfigurationError."""
        settings = HintGridSettings(knn_self_neighbor_offset=-1)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "knn_self_neighbor_offset" in str(exc_info.value)

    def test_invalid_similarity_threshold_negative(self) -> None:
        """similarity_threshold < 0 should raise ConfigurationError."""
        settings = HintGridSettings(similarity_threshold=-0.5)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "similarity_threshold" in str(exc_info.value)

    def test_invalid_similarity_recency_days(self) -> None:
        """similarity_recency_days < 1 should raise ConfigurationError."""
        settings = HintGridSettings(similarity_recency_days=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "similarity_recency_days" in str(exc_info.value)


class TestFeedValidation:
    """Tests for feed configuration validation branches."""

    def test_invalid_feed_size(self) -> None:
        """feed_size < 1 should raise ConfigurationError."""
        settings = HintGridSettings(feed_size=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "feed_size" in str(exc_info.value)

    def test_invalid_feed_days(self) -> None:
        """feed_days < 1 should raise ConfigurationError."""
        settings = HintGridSettings(feed_days=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "feed_days" in str(exc_info.value)

    def test_invalid_feed_score_multiplier(self) -> None:
        """feed_score_multiplier < 1 should raise ConfigurationError."""
        # Test multiplier = 0 (must be >= 1)
        settings = HintGridSettings(feed_score_multiplier=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "feed_score_multiplier" in str(exc_info.value)

        # Test multiplier = 1 (should pass, >= 1 is valid)
        # Note: validation allows >= 1, so 1 is valid
        settings = HintGridSettings(feed_score_multiplier=1)
        # Should not raise
        validate_settings(settings)

    def test_invalid_feed_score_decimals(self) -> None:
        """feed_score_decimals < 0 should raise ConfigurationError."""
        settings = HintGridSettings(feed_score_decimals=-1)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "feed_score_decimals" in str(exc_info.value)


class TestInterestsValidation:
    """Tests for interests configuration validation branches."""

    def test_invalid_interests_min_favourites(self) -> None:
        """interests_min_favourites < 1 should raise ConfigurationError."""
        settings = HintGridSettings(interests_min_favourites=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "interests_min_favourites" in str(exc_info.value)


class TestCtrValidation:
    """Tests for CTR configuration validation branches."""

    def test_invalid_ctr_weight_too_low(self) -> None:
        """ctr_weight < 0 should raise ConfigurationError."""
        settings = HintGridSettings(ctr_weight=-0.1)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "ctr_weight" in str(exc_info.value)

    def test_invalid_ctr_weight_too_high(self) -> None:
        """ctr_weight > 1 should raise ConfigurationError."""
        settings = HintGridSettings(ctr_weight=1.1)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "ctr_weight" in str(exc_info.value)

    def test_invalid_min_ctr_negative(self) -> None:
        """min_ctr < 0 should raise ConfigurationError."""
        settings = HintGridSettings(min_ctr=-0.1)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "min_ctr" in str(exc_info.value)

    def test_invalid_ctr_smoothing_negative(self) -> None:
        """ctr_smoothing < 0 should raise ConfigurationError."""
        settings = HintGridSettings(ctr_smoothing=-0.1)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "ctr_smoothing" in str(exc_info.value)
