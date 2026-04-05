"""Unit tests for PipelineState dataclass.

Tests verify PipelineState serialization/deserialization
without requiring Neo4j connection.
"""

from hintgrid.state import INITIAL_CURSOR, PipelineState
from hintgrid.utils.coercion import convert_dict_to_neo4j_value


class TestPipelineStateDefaults:
    """Tests for PipelineState default values."""

    def test_default_cursors_are_zero(self) -> None:
        """Test that all default cursor values are INITIAL_CURSOR (0)."""
        state = PipelineState()

        assert state.last_status_id == INITIAL_CURSOR
        assert state.last_favourite_id == INITIAL_CURSOR
        assert state.last_block_id == INITIAL_CURSOR
        assert state.last_mute_id == INITIAL_CURSOR
        assert state.last_reblog_id == INITIAL_CURSOR
        assert state.last_reply_id == INITIAL_CURSOR

    def test_default_embedding_signature_is_empty(self) -> None:
        """Test that default embedding_signature is empty string."""
        state = PipelineState()
        assert state.embedding_signature == ""


class TestPipelineStateFromDict:
    """Tests for PipelineState.from_dict method."""

    def test_from_dict_with_all_fields(self) -> None:
        """Test creating state from dict with all fields."""
        data = {
            "last_status_id": 1000,
            "last_favourite_id": 2000,
            "last_block_id": 4000,
            "last_mute_id": 5000,
            "last_reblog_id": 6000,
            "last_reply_id": 7000,
            "embedding_signature": "openai:gpt-4:768",
        }

        state = PipelineState.from_dict(convert_dict_to_neo4j_value(data))

        assert state.last_status_id == 1000
        assert state.last_favourite_id == 2000
        assert state.last_block_id == 4000
        assert state.last_mute_id == 5000
        assert state.last_reblog_id == 6000
        assert state.last_reply_id == 7000
        assert state.embedding_signature == "openai:gpt-4:768"

    def test_from_dict_with_missing_fields(self) -> None:
        """Test that missing fields get default values."""
        data: dict[str, object] = {"last_status_id": 100}

        state = PipelineState.from_dict(convert_dict_to_neo4j_value(data))

        assert state.last_status_id == 100
        assert state.last_favourite_id == 0
        assert state.last_block_id == 0
        assert state.last_mute_id == 0
        assert state.last_reblog_id == 0
        assert state.last_reply_id == 0
        assert state.embedding_signature == ""

    def test_from_dict_with_empty_dict(self) -> None:
        """Test that empty dict produces default state."""
        state = PipelineState.from_dict({})

        assert state.last_status_id == 0
        assert state.embedding_signature == ""

    def test_from_dict_coerces_string_values(self) -> None:
        """Test that string values are coerced to int."""
        data = {
            "last_status_id": "1000",
            "last_favourite_id": "2000",
        }

        state = PipelineState.from_dict(convert_dict_to_neo4j_value(data))

        assert state.last_status_id == 1000
        assert state.last_favourite_id == 2000

    def test_from_dict_handles_none_values(self) -> None:
        """Test that None values become defaults."""
        data: dict[str, object] = {
            "last_status_id": None,
            "embedding_signature": None,
        }

        state = PipelineState.from_dict(convert_dict_to_neo4j_value(data))

        assert state.last_status_id == 0
        assert state.embedding_signature == ""


class TestPipelineStateToDict:
    """Tests for PipelineState.to_dict method."""

    def test_to_dict_serializes_all_fields(self) -> None:
        """Test that to_dict includes all fields."""
        state = PipelineState(
            last_status_id=1000,
            last_favourite_id=2000,
            last_block_id=4000,
            last_mute_id=5000,
            last_reblog_id=6000,
            last_reply_id=7000,
            embedding_signature="fasttext:model:64",
        )

        result = state.to_dict()

        assert result["last_status_id"] == 1000
        assert result["last_favourite_id"] == 2000
        assert result["last_block_id"] == 4000
        assert result["last_mute_id"] == 5000
        assert result["last_reblog_id"] == 6000
        assert result["last_reply_id"] == 7000
        assert result["embedding_signature"] == "fasttext:model:64"

    def test_to_dict_default_state(self) -> None:
        """Test that default state serializes to zeros."""
        state = PipelineState()
        result = state.to_dict()

        for key in [
            "last_status_id",
            "last_favourite_id",
            "last_block_id",
            "last_mute_id",
            "last_reblog_id",
            "last_reply_id",
        ]:
            assert result[key] == 0

        assert result["embedding_signature"] == ""


class TestPipelineStateRoundtrip:
    """Tests for serialization/deserialization roundtrip."""

    def test_roundtrip_preserves_values(self) -> None:
        """Test that to_dict -> from_dict preserves all values."""
        original = PipelineState(
            last_status_id=12345,
            last_favourite_id=67890,
            last_block_id=22222,
            last_mute_id=33333,
            last_reblog_id=44444,
            last_reply_id=55555,
            embedding_signature="provider:model:128",
        )

        serialized = original.to_dict()
        restored = PipelineState.from_dict(serialized)

        assert restored.last_status_id == original.last_status_id
        assert restored.last_favourite_id == original.last_favourite_id
        assert restored.last_block_id == original.last_block_id
        assert restored.last_mute_id == original.last_mute_id
        assert restored.last_reblog_id == original.last_reblog_id
        assert restored.last_reply_id == original.last_reply_id
        assert restored.embedding_signature == original.embedding_signature

    def test_roundtrip_default_state(self) -> None:
        """Test that default state survives roundtrip."""
        original = PipelineState()
        serialized = original.to_dict()
        restored = PipelineState.from_dict(serialized)

        assert restored.last_status_id == original.last_status_id
        assert restored.embedding_signature == original.embedding_signature
