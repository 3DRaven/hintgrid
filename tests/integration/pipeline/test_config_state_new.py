"""Integration tests for new config validation and state persistence.

Covers:
- Config validation for new settings (language_match_weight, bookmark_weight,
  public_feed_*, redis_namespace)
- last_bookmark_id in PipelineState and StateStore
"""

from __future__ import annotations

import pytest

from hintgrid.config import HintGridSettings, validate_settings
from hintgrid.exceptions import ConfigurationError
from hintgrid.state import INITIAL_CURSOR, PipelineState, StateStore
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.parallel import IsolatedNeo4jClient


# ==========================================================================
# Config validation tests
# ==========================================================================


class TestNewConfigValidation:
    """Validation tests for newly added configuration settings."""

    @pytest.mark.integration
    def test_valid_language_match_weight(self) -> None:
        """Valid language_match_weight should not raise."""
        settings = HintGridSettings(language_match_weight=0.3)
        validate_settings(settings)

    @pytest.mark.integration
    def test_invalid_language_match_weight_negative(self) -> None:
        """Negative language_match_weight should raise ConfigurationError."""
        settings = HintGridSettings(language_match_weight=-0.5)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "language_match_weight" in str(exc_info.value)

    @pytest.mark.integration
    def test_valid_ui_language_match_weight_pair(self) -> None:
        """ui_language_match_weight >= language_match_weight should not raise."""
        settings = HintGridSettings(
            language_match_weight=0.3,
            ui_language_match_weight=0.5,
        )
        validate_settings(settings)

    @pytest.mark.integration
    def test_invalid_ui_language_match_weight_below_chosen(self) -> None:
        """ui_language_match_weight < language_match_weight should raise."""
        settings = HintGridSettings(
            language_match_weight=0.6,
            ui_language_match_weight=0.3,
        )
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "ui_language_match_weight" in str(exc_info.value)

    @pytest.mark.integration
    def test_valid_bookmark_weight(self) -> None:
        """Valid bookmark_weight should not raise."""
        settings = HintGridSettings(bookmark_weight=2.0)
        validate_settings(settings)

    @pytest.mark.integration
    def test_invalid_bookmark_weight_negative(self) -> None:
        """Negative bookmark_weight should raise ConfigurationError."""
        settings = HintGridSettings(bookmark_weight=-1.0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "bookmark_weight" in str(exc_info.value)

    @pytest.mark.integration
    def test_valid_public_feed_strategy_all_communities(self) -> None:
        """'all_communities' is a valid public_feed_strategy."""
        settings = HintGridSettings(public_feed_strategy="all_communities")
        validate_settings(settings)

    @pytest.mark.integration
    def test_valid_public_feed_strategy_local_communities(self) -> None:
        """'local_communities' is a valid public_feed_strategy."""
        settings = HintGridSettings(public_feed_strategy="local_communities")
        validate_settings(settings)

    @pytest.mark.integration
    def test_invalid_public_feed_strategy(self) -> None:
        """Unknown public_feed_strategy should raise ConfigurationError."""
        settings = HintGridSettings(public_feed_strategy="invalid_strategy")
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "public_feed_strategy" in str(exc_info.value)

    @pytest.mark.integration
    def test_invalid_public_feed_size_zero(self) -> None:
        """Zero public_feed_size should raise ConfigurationError."""
        settings = HintGridSettings(public_feed_size=0)
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "public_feed_size" in str(exc_info.value)

    @pytest.mark.integration
    def test_valid_redis_namespace(self) -> None:
        """Valid redis_namespace should not raise."""
        settings = HintGridSettings(redis_namespace="cache")
        validate_settings(settings)

    @pytest.mark.integration
    def test_valid_redis_namespace_with_special_chars(self) -> None:
        """redis_namespace with allowed chars (:-_) should not raise."""
        settings = HintGridSettings(redis_namespace="my-app:cache_v2")
        validate_settings(settings)

    @pytest.mark.integration
    def test_invalid_redis_namespace_with_spaces(self) -> None:
        """redis_namespace with spaces should raise ConfigurationError."""
        settings = HintGridSettings(redis_namespace="invalid namespace")
        with pytest.raises(ConfigurationError) as exc_info:
            validate_settings(settings)
        assert "redis_namespace" in str(exc_info.value)


# ==========================================================================
# State persistence tests: last_bookmark_id
# ==========================================================================


def _state_id(worker_id: str) -> str:
    """Generate worker-specific state ID for isolation."""
    if worker_id == "master":
        return "bookmark_state"
    return f"bookmark_state_{worker_id}"


@pytest.mark.integration
def test_last_bookmark_id_default_is_initial_cursor(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Freshly created AppState has last_bookmark_id == INITIAL_CURSOR."""
    neo4j = isolated_neo4j.client
    state_store = StateStore(neo4j, state_id=_state_id(worker_id))
    state = state_store.load()

    assert state.last_bookmark_id == INITIAL_CURSOR


@pytest.mark.integration
def test_last_bookmark_id_save_and_load(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Saving last_bookmark_id persists the value across load cycles."""
    neo4j = isolated_neo4j.client
    state_store = StateStore(neo4j, state_id=_state_id(worker_id))

    state = PipelineState(last_bookmark_id=555)
    state_store.save(state)

    loaded = state_store.load()
    assert loaded.last_bookmark_id == 555


@pytest.mark.integration
def test_last_bookmark_id_does_not_affect_other_cursors(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Updating last_bookmark_id preserves other cursor values."""
    neo4j = isolated_neo4j.client
    state_store = StateStore(neo4j, state_id=_state_id(worker_id))

    initial = PipelineState(
        last_status_id=1000,
        last_favourite_id=2000,
        last_bookmark_id=INITIAL_CURSOR,
    )
    state_store.save(initial)

    # Update only bookmark cursor
    state = state_store.load()
    state.last_bookmark_id = 4567
    state_store.save(state)

    final = state_store.load()
    assert final.last_status_id == 1000
    assert final.last_favourite_id == 2000
    assert final.last_bookmark_id == 4567


@pytest.mark.integration
def test_last_bookmark_id_in_from_dict_and_to_dict() -> None:
    """PipelineState.from_dict correctly deserializes last_bookmark_id."""
    data = {
        "last_status_id": 100,
        "last_favourite_id": 200,
        "last_follow_id": 300,
        "last_block_id": 400,
        "last_mute_id": 500,
        "last_bookmark_id": 777,
    }
    state = PipelineState.from_dict(data)
    assert state.last_bookmark_id == 777

    # Round-trip through to_dict
    d = state.to_dict()
    assert d["last_bookmark_id"] == 777


@pytest.mark.integration
def test_last_bookmark_id_incremental_checkpointing(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Simulate checkpointing: bookmark cursor advances through IDs."""
    neo4j = isolated_neo4j.client
    state_store = StateStore(neo4j, state_id=_state_id(worker_id))

    for checkpoint_id in [100, 500, 1000]:
        state = state_store.load()
        state.last_bookmark_id = checkpoint_id
        state_store.save(state)

        reloaded = state_store.load()
        assert reloaded.last_bookmark_id == checkpoint_id
