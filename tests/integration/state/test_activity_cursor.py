"""Integration tests for last_activity_account_id cursor in PipelineState.

Tests verify:
- Activity cursor is persisted and loaded from Neo4j
- Activity cursor survives save/load roundtrip
- Activity cursor is independent of other cursors
- Activity cursor reset to 0 at the start of a full pipeline run
"""

from __future__ import annotations

import pytest

from hintgrid.state import INITIAL_CURSOR, PipelineState, StateStore
from hintgrid.utils.coercion import convert_dict_to_neo4j_value
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.parallel import IsolatedNeo4jClient


def _state_id(worker_id: str) -> str:
    """Generate worker-specific state ID for isolation."""
    if worker_id == "master":
        return "activity_cursor_test"
    return f"activity_cursor_test_{worker_id}"


@pytest.mark.integration
def test_activity_cursor_default_is_zero(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Activity cursor defaults to INITIAL_CURSOR (0) on fresh state."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    state_store = StateStore(neo4j, state_id=state_id)
    state = state_store.load()

    assert state.last_activity_account_id == INITIAL_CURSOR


@pytest.mark.integration
def test_activity_cursor_save_and_load(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Activity cursor is persisted to Neo4j and loaded correctly."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    state_store = StateStore(neo4j, state_id=state_id)

    state = PipelineState(last_activity_account_id=42000)
    state_store.save(state)

    loaded = state_store.load()
    assert loaded.last_activity_account_id == 42000


@pytest.mark.integration
def test_activity_cursor_incremental_updates(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Activity cursor updates incrementally as batches are processed."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    state_store = StateStore(neo4j, state_id=state_id)
    state = state_store.load()

    # Simulate processing batches with increasing account IDs
    for batch_max_id in [100, 500, 1200]:
        state.last_activity_account_id = batch_max_id
        state_store.save(state)

        loaded = state_store.load()
        assert loaded.last_activity_account_id == batch_max_id


@pytest.mark.integration
def test_activity_cursor_independent_of_other_cursors(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Updating activity cursor does not affect other cursors."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    state_store = StateStore(neo4j, state_id=state_id)

    # Set all cursors
    initial_state = PipelineState(
        last_status_id=1000,
        last_favourite_id=2000,
        last_block_id=4000,
        last_mute_id=5000,
        last_activity_account_id=0,
    )
    state_store.save(initial_state)

    # Update only activity cursor
    loaded = state_store.load()
    loaded.last_activity_account_id = 9999
    state_store.save(loaded)

    # Verify other cursors untouched
    final = state_store.load()
    assert final.last_status_id == 1000
    assert final.last_favourite_id == 2000
    assert final.last_block_id == 4000
    assert final.last_mute_id == 5000
    assert final.last_activity_account_id == 9999


@pytest.mark.integration
def test_activity_cursor_roundtrip_via_dict(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Activity cursor survives to_dict -> from_dict roundtrip."""
    original = PipelineState(
        last_status_id=100,
        last_activity_account_id=77777,
        embedding_signature="test:model:64",
    )

    serialized = original.to_dict()
    assert "last_activity_account_id" in serialized
    assert serialized["last_activity_account_id"] == 77777

    restored = PipelineState.from_dict(serialized)
    assert restored.last_activity_account_id == 77777
    assert restored.last_status_id == 100
    assert restored.embedding_signature == "test:model:64"


@pytest.mark.integration
def test_activity_cursor_from_dict_missing_field_defaults_to_zero() -> None:
    """from_dict with missing activity cursor defaults to 0 (backward compat)."""
    old_data: dict[str, object] = {
        "last_status_id": 500,
        "last_favourite_id": 600,
    }

    state = PipelineState.from_dict(convert_dict_to_neo4j_value(old_data))
    assert state.last_activity_account_id == INITIAL_CURSOR


@pytest.mark.integration
def test_activity_cursor_in_neo4j_state_node(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Activity cursor is stored as last_processed_activity_account_id in Neo4j."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    state_store = StateStore(neo4j, state_id=state_id)

    state = PipelineState(last_activity_account_id=12345)
    state_store.save(state)

    # Query Neo4j directly to verify the property name
    rows = list(neo4j.execute_and_fetch(
        "MATCH (s:AppState {id: $sid}) "
        "RETURN s.last_processed_activity_account_id AS activity_cursor",
        {"sid": state_id},
    ))

    assert len(rows) == 1
    from hintgrid.utils.coercion import coerce_int
    assert coerce_int(rows[0].get("activity_cursor")) == 12345
