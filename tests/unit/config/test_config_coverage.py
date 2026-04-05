"""Unit tests for config.py uncovered validation branches.

Covers CliOverrides.apply empty path and missing validate_settings branches
for cold_start_limit, similarity_pruning, prune_similarity_threshold,
prune_days, serendipity_limit, serendipity_score, interests_ttl_days,
export_max_items, text_preview_limit.
"""

from __future__ import annotations

import pytest

from hintgrid.config import CliOverrides, HintGridSettings, validate_settings
from hintgrid.exceptions import ConfigurationError


class TestCliOverrides:
    """Tests for CliOverrides.apply method."""

    def test_empty_overrides_returns_original(self) -> None:
        """Test that empty overrides return the same settings object."""
        settings = HintGridSettings()
        overrides = CliOverrides(overrides={})
        result = overrides.apply(settings)
        assert result is settings

    def test_all_none_overrides_returns_original(self) -> None:
        """Test that all-None overrides return the same settings object."""
        settings = HintGridSettings()
        overrides = CliOverrides(overrides={"feed_size": None, "feed_days": None})
        result = overrides.apply(settings)
        assert result is settings


class TestColdStartValidation:
    """Tests for cold_start_limit validation."""

    def test_invalid_cold_start_limit(self) -> None:
        """cold_start_limit < 1 should raise ConfigurationError."""
        settings = HintGridSettings(cold_start_limit=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "cold_start_limit" in str(exc_info.value)


class TestSimilarityPruningValidation:
    """Tests for similarity pruning validation branches."""

    def test_invalid_similarity_pruning_value(self) -> None:
        """Invalid similarity_pruning value should raise ConfigurationError."""
        settings = HintGridSettings(similarity_pruning="invalid_value")
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "similarity_pruning" in str(exc_info.value)

    def test_invalid_prune_similarity_threshold(self) -> None:
        """prune_similarity_threshold outside 0-1 should raise ConfigurationError."""
        settings = HintGridSettings(prune_similarity_threshold=1.5)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "prune_similarity_threshold" in str(exc_info.value)

    def test_invalid_prune_days(self) -> None:
        """prune_days < 1 should raise ConfigurationError."""
        settings = HintGridSettings(prune_days=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "prune_days" in str(exc_info.value)


class TestSerendipityValidation:
    """Tests for serendipity settings validation."""

    def test_invalid_serendipity_limit(self) -> None:
        """serendipity_limit < 0 should raise ConfigurationError."""
        settings = HintGridSettings(serendipity_limit=-1)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "serendipity_limit" in str(exc_info.value)

    def test_invalid_serendipity_score(self) -> None:
        """serendipity_score outside 0-1 should raise ConfigurationError."""
        settings = HintGridSettings(serendipity_score=2.0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "serendipity_score" in str(exc_info.value)


class TestInterestsValidation:
    """Tests for interests settings validation."""

    def test_invalid_interests_ttl_days(self) -> None:
        """interests_ttl_days < 1 should raise ConfigurationError."""
        settings = HintGridSettings(interests_ttl_days=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "interests_ttl_days" in str(exc_info.value)


class TestExportValidation:
    """Tests for export settings validation."""

    def test_invalid_export_max_items(self) -> None:
        """export_max_items < 1 should raise ConfigurationError."""
        settings = HintGridSettings(export_max_items=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "export_max_items" in str(exc_info.value)

    def test_invalid_text_preview_limit(self) -> None:
        """text_preview_limit < 1 should raise ConfigurationError."""
        settings = HintGridSettings(text_preview_limit=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "text_preview_limit" in str(exc_info.value)
