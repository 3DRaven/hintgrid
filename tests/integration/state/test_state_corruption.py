"""Integration tests for state corruption recovery and edge cases.

Covers:
- Recovery from corrupted AppState node
- Concurrent access to state (multiple processes simulation)
- Recovery after partial writes
- Edge cases for cursor values (negative, very large)
- State migration between versions
"""

from __future__ import annotations


import pytest

from hintgrid.state import INITIAL_CURSOR, PipelineState, StateStore
from hintgrid.utils.coercion import coerce_int, convert_dict_to_neo4j_value
from datetime import UTC
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.parallel import IsolatedNeo4jClient


def _state_id(worker_id: str) -> str:
    """Generate worker-specific state ID for isolation."""
    if worker_id == "master":
        return "corruption_test"
    return f"corruption_test_{worker_id}"


# ---------------------------------------------------------------------------
# Tests: Corrupted data recovery
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_state_load_handles_missing_properties(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test that load() handles AppState node with missing properties."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    neo4j.label("AppState")

    # Create AppState node with only id (missing all cursor properties)
    neo4j.execute_labeled(
        "CREATE (s:__app_state__ {id: $sid, updated_at: timestamp()}) RETURN s",
        {"app_state": "AppState"},
        {"sid": state_id},
    )

    # StateStore should handle missing properties gracefully
    state_store = StateStore(neo4j, state_id=state_id)
    state = state_store.load()

    # Should return default values (0) for missing properties
    assert state.last_status_id == INITIAL_CURSOR
    assert state.last_favourite_id == INITIAL_CURSOR


@pytest.mark.integration
def test_state_load_handles_null_properties(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test that load() handles AppState node with null properties."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    neo4j.label("AppState")

    # Create AppState node with null cursor properties
    neo4j.execute_labeled(
        "CREATE (s:__app_state__ {"
        "id: $sid, "
        "last_processed_status_id: null, "
        "last_processed_favourite_id: null, "
        "updated_at: timestamp()"
        "})",
        {"app_state": "AppState"},
        {"sid": state_id},
    )

    state_store = StateStore(neo4j, state_id=state_id)
    state = state_store.load()

    # Should return default values (0) for null properties
    assert state.last_status_id == INITIAL_CURSOR
    assert state.last_favourite_id == INITIAL_CURSOR


@pytest.mark.integration
def test_state_load_handles_wrong_type_properties(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test that load() handles AppState node with wrong type properties."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    neo4j.label("AppState")

    # Create AppState node with string instead of int
    neo4j.execute_labeled(
        "CREATE (s:__app_state__ {"
        "id: $sid, "
        "last_processed_status_id: 'invalid_string', "
        "updated_at: timestamp()"
        "})",
        {"app_state": "AppState"},
        {"sid": state_id},
    )

    state_store = StateStore(neo4j, state_id=state_id)
    state = state_store.load()

    # coerce_int should handle invalid types and return 0
    assert state.last_status_id == INITIAL_CURSOR


@pytest.mark.integration
def test_state_save_overwrites_corrupted_data(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test that save() can overwrite corrupted AppState node."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    neo4j.label("AppState")

    # Create corrupted AppState node
    neo4j.execute_labeled(
        "CREATE (s:__app_state__ {"
        "id: $sid, "
        "last_processed_status_id: 'corrupted', "
        "last_processed_favourite_id: -999, "
        "updated_at: timestamp()"
        "})",
        {"app_state": "AppState"},
        {"sid": state_id},
    )

    # Save should overwrite corrupted data
    state_store = StateStore(neo4j, state_id=state_id)
    clean_state = PipelineState(
        last_status_id=5000,
        last_favourite_id=6000,
    )
    state_store.save(clean_state)

    # Verify data is now clean
    loaded = state_store.load()
    assert loaded.last_status_id == 5000
    assert loaded.last_favourite_id == 6000


# ---------------------------------------------------------------------------
# Tests: Concurrent access simulation
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_state_concurrent_save_last_write_wins(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test that concurrent saves result in last write winning."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)

    # Create two StateStore instances (simulating concurrent processes)
    state_store1 = StateStore(neo4j, state_id=state_id)
    state_store2 = StateStore(neo4j, state_id=state_id)

    # Both save different values
    state1 = PipelineState(last_status_id=1000)
    state2 = PipelineState(last_status_id=2000)

    state_store1.save(state1)
    state_store2.save(state2)

    # Last write should win
    loaded = state_store1.load()
    assert loaded.last_status_id == 2000, "Last write should win in concurrent scenario"


@pytest.mark.integration
def test_state_concurrent_load_consistent(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test that concurrent loads see consistent state."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)

    # Set initial state
    state_store = StateStore(neo4j, state_id=state_id)
    state_store.save(PipelineState(last_status_id=3000))

    # Create multiple StateStore instances
    store1 = StateStore(neo4j, state_id=state_id)
    store2 = StateStore(neo4j, state_id=state_id)
    store3 = StateStore(neo4j, state_id=state_id)

    # All should see the same state
    state1 = store1.load()
    state2 = store2.load()
    state3 = store3.load()

    assert state1.last_status_id == state2.last_status_id == state3.last_status_id == 3000


# ---------------------------------------------------------------------------
# Tests: Edge cases for cursor values
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_state_negative_cursor_values(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test that negative cursor values are handled correctly.
    
    coerce_int preserves negative values as-is (doesn't convert to 0).
    In practice, cursors should never be negative, but the system
    should handle corrupted data gracefully.
    """
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    neo4j.label("AppState")

    # Create AppState with negative cursor (corrupted data)
    neo4j.execute_labeled(
        "CREATE (s:__app_state__ {"
        "id: $sid, "
        "last_processed_status_id: -100, "
        "updated_at: timestamp()"
        "})",
        {"app_state": "AppState"},
        {"sid": state_id},
    )

    state_store = StateStore(neo4j, state_id=state_id)
    state = state_store.load()

    # coerce_int preserves negative values as-is
    assert isinstance(state.last_status_id, int), "Should return integer"
    assert state.last_status_id == -100, (
        "coerce_int should preserve negative values as-is, "
        "even though they shouldn't occur in practice"
    )
    
    # Save should overwrite with valid value
    clean_state = PipelineState(last_status_id=5000)
    state_store.save(clean_state)
    loaded = state_store.load()
    assert loaded.last_status_id == 5000, "Save should overwrite corrupted value"


@pytest.mark.integration
def test_state_very_large_cursor_values(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test that very large cursor values are handled correctly."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)

    # Use very large value (simulating snowflake IDs)
    large_value = 9223372036854775807  # Max int64

    state_store = StateStore(neo4j, state_id=state_id)
    state = PipelineState(last_status_id=large_value)
    state_store.save(state)

    loaded = state_store.load()
    assert loaded.last_status_id == large_value, "Should handle very large cursor values"


@pytest.mark.integration
def test_state_zero_cursor_values(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test that zero cursor values are handled correctly."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)

    state_store = StateStore(neo4j, state_id=state_id)
    state = PipelineState(
        last_status_id=0,
        last_favourite_id=0,
    )
    state_store.save(state)

    loaded = state_store.load()
    assert loaded.last_status_id == 0
    assert loaded.last_favourite_id == 0


# ---------------------------------------------------------------------------
# Tests: State migration
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_state_migration_missing_new_fields(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test migration when new fields are added to PipelineState."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    neo4j.label("AppState")

    # Create old AppState node (missing new fields like last_bookmark_id)
    neo4j.execute_labeled(
        "CREATE (s:__app_state__ {"
        "id: $sid, "
        "last_processed_status_id: 1000, "
        "last_processed_favourite_id: 2000, "
        "updated_at: timestamp()"
        "})",
        {"app_state": "AppState"},
        {"sid": state_id},
    )

    state_store = StateStore(neo4j, state_id=state_id)
    state = state_store.load()

    # New fields should have default values
    assert state.last_status_id == 1000
    assert state.last_favourite_id == 2000
    assert state.last_bookmark_id == INITIAL_CURSOR, "New field should default to INITIAL_CURSOR"


@pytest.mark.integration
def test_state_migration_embedding_signature(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test migration of embedding_signature field."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    neo4j.label("AppState")

    # Create AppState with old embedding_signature
    neo4j.execute_labeled(
        "CREATE (s:__app_state__ {"
        "id: $sid, "
        "embedding_signature: 'old:model:128', "
        "updated_at: timestamp()"
        "})",
        {"app_state": "AppState"},
        {"sid": state_id},
    )

    state_store = StateStore(neo4j, state_id=state_id)
    state = state_store.load()

    assert state.embedding_signature == "old:model:128"

    # Update to new signature
    state.embedding_signature = "new:model:256"
    state_store.save(state)

    loaded = state_store.load()
    assert loaded.embedding_signature == "new:model:256"


@pytest.mark.integration
def test_state_migration_interests_rebuild_timestamp(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test migration of last_interests_rebuild_at field."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    neo4j.label("AppState")

    # Create AppState without last_interests_rebuild_at (old version)
    neo4j.execute_labeled(
        "CREATE (s:__app_state__ {"
        "id: $sid, "
        "last_processed_status_id: 1000, "
        "updated_at: timestamp()"
        "})",
        {"app_state": "AppState"},
        {"sid": state_id},
    )

    state_store = StateStore(neo4j, state_id=state_id)
    state = state_store.load()

    # Should default to empty string
    assert state.last_interests_rebuild_at == ""

    # Set new timestamp
    from datetime import datetime

    new_timestamp = datetime.now(UTC).isoformat()
    state.last_interests_rebuild_at = new_timestamp
    state_store.save(state)

    loaded = state_store.load()
    assert loaded.last_interests_rebuild_at == new_timestamp


# ---------------------------------------------------------------------------
# Tests: Partial write recovery
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_state_partial_write_recovery(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test that state can be recovered after a partial write."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    neo4j.label("AppState")

    # Simulate partial write (only some properties set)
    neo4j.execute_labeled(
        "CREATE (s:__app_state__ {"
        "id: $sid, "
        "last_processed_status_id: 5000, "
        "last_processed_favourite_id: 6000, "
        "updated_at: timestamp()"
        "})",
        {"app_state": "AppState"},
        {"sid": state_id},
    )

    # Load should work even with partial data
    state_store = StateStore(neo4j, state_id=state_id)
    state = state_store.load()

    assert state.last_status_id == 5000
    assert state.last_favourite_id == 6000
    # Other fields should have defaults

    # Save should complete the state
    state_store.save(state)

    state_store.load()


@pytest.mark.integration
def test_state_ensure_initialized_idempotent(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test that _ensure_initialized is idempotent (can be called multiple times)."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)

    # Create multiple StateStore instances (each calls _ensure_initialized)
    _ = StateStore(neo4j, state_id=state_id)
    _ = StateStore(neo4j, state_id=state_id)
    _ = StateStore(neo4j, state_id=state_id)

    # Should still have only one AppState node
    # apoc.merge.node is idempotent and should not create duplicates
    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (s:__app_state__ {id: $sid}) RETURN count(s) AS count",
            {"app_state": "AppState"},
            {"sid": state_id},
        )
    )
    count = coerce_int(rows[0].get("count"))

    assert count == 1, (
        f"Should have exactly one AppState node after multiple initializations, "
        f"got {count} nodes with id={state_id}"
    )


@pytest.mark.integration
def test_state_from_dict_handles_missing_keys(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test PipelineState.from_dict handles missing keys gracefully."""
    # Partial dict (missing some keys)
    partial_dict = {
        "last_status_id": 1000,
        "last_favourite_id": 2000,
        # Missing other fields
    }

    state = PipelineState.from_dict(convert_dict_to_neo4j_value(partial_dict))

    assert state.last_status_id == 1000
    assert state.last_favourite_id == 2000
    # Missing fields should default to INITIAL_CURSOR
    assert state.last_block_id == INITIAL_CURSOR


@pytest.mark.integration
def test_state_from_dict_handles_extra_keys(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test PipelineState.from_dict ignores extra keys."""
    # Dict with extra keys
    extra_dict = {
        "last_status_id": 1000,
        "last_favourite_id": 2000,
        "unknown_field": 9999,
        "another_unknown": "test",
    }

    state = PipelineState.from_dict(convert_dict_to_neo4j_value(extra_dict))

    assert state.last_status_id == 1000
    assert state.last_favourite_id == 2000
    # Extra keys should be ignored
    assert not hasattr(state, "unknown_field")
