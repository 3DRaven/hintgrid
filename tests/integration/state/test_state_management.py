"""Integration tests for Neo4j-based state management (Singleton Node pattern)."""

from __future__ import annotations


import pytest

from hintgrid.state import PipelineState, StateStore
from hintgrid.utils.coercion import coerce_int
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.parallel import IsolatedNeo4jClient
    from hintgrid.config import HintGridSettings


def _state_id(worker_id: str) -> str:
    """Generate worker-specific state ID for isolation."""
    if worker_id == "master":
        return "main"
    return f"main_{worker_id}"


@pytest.mark.smoke
@pytest.mark.integration
def test_state_initialization(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test that AppState node is created automatically."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    _ = StateStore(neo4j, state_id=state_id)

    # Verify AppState node exists
    neo4j.label("AppState")
    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (s:__appstate__ {id: $sid}) RETURN s",
            {"appstate": "AppState"},
            {"sid": state_id},
        )
    )

    assert len(rows) == 1
    state_node_obj = rows[0].get("s")
    assert isinstance(state_node_obj, dict)
    state_node = state_node_obj
    assert state_node.get("id") == state_id


@pytest.mark.integration
def test_state_load_default_values(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test loading state returns zero values for new AppState."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    state_store = StateStore(neo4j, state_id=state_id)
    state = state_store.load()

    assert state.last_status_id == 0
    assert state.last_favourite_id == 0
    assert state.last_block_id == 0
    assert state.last_mute_id == 0


@pytest.mark.smoke
@pytest.mark.integration
def test_state_save_and_load(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test atomic save and load operations."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    state_store = StateStore(neo4j, state_id=state_id)

    # Create state with specific values
    state = PipelineState(
        last_status_id=1000,
        last_favourite_id=2000,
        last_block_id=4000,
        last_mute_id=5000,
    )

    # Save state
    state_store.save(state)

    # Load state back
    loaded_state = state_store.load()

    assert loaded_state.last_status_id == 1000
    assert loaded_state.last_favourite_id == 2000
    assert loaded_state.last_block_id == 4000
    assert loaded_state.last_mute_id == 5000


@pytest.mark.integration
def test_state_idempotency(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test that repeated MERGE operations are idempotent."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    # Create multiple StateStore instances (simulating multiple runs)
    state_store1 = StateStore(neo4j, state_id=state_id)
    state_store2 = StateStore(neo4j, state_id=state_id)

    # Both should see the same node
    state1 = state_store1.load()
    state2 = state_store2.load()

    assert state1.last_status_id == state2.last_status_id

    # Count AppState nodes with this state_id (should be exactly 1)
    neo4j.label("AppState")
    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (s:__appstate__ {id: $sid}) RETURN count(s) AS count",
            {"appstate": "AppState"},
            {"sid": state_id},
        )
    )
    assert coerce_int(rows[0].get("count")) == 1


@pytest.mark.integration
def test_state_incremental_update(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test incremental updates to state (checkpoint pattern)."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    state_store = StateStore(neo4j, state_id=state_id)

    # Initial state
    state = state_store.load()
    assert state.last_status_id == 0

    # Simulate processing batches
    for batch_max_id in [100, 200, 300]:
        state.last_status_id = batch_max_id
        state_store.save(state)

        # Verify checkpoint
        loaded = state_store.load()
        assert loaded.last_status_id == batch_max_id


@pytest.mark.integration
def test_state_partial_update(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test that updating one cursor doesn't affect others."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    state_store = StateStore(neo4j, state_id=state_id)

    # Set initial values
    initial_state = PipelineState(
        last_status_id=1000,
        last_favourite_id=2000,
        last_block_id=4000,
        last_mute_id=5000,
    )
    state_store.save(initial_state)

    # Update only status_id
    updated_state = state_store.load()
    updated_state.last_status_id = 5000
    state_store.save(updated_state)

    # Load and verify all cursors
    final_state = state_store.load()
    assert final_state.last_status_id == 5000
    assert final_state.last_favourite_id == 2000
    assert final_state.last_block_id == 4000
    assert final_state.last_mute_id == 5000


@pytest.mark.integration
def test_state_constraint_enforced(
    isolated_neo4j: IsolatedNeo4jClient,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Test that AppState.id constraint prevents duplicates."""
    from hintgrid.pipeline.graph import ensure_graph_indexes

    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    # Ensure constraint is created
    ensure_graph_indexes(neo4j, settings)

    # Try to create duplicate AppState (should be MERGE'd, not fail)
    state_store1 = StateStore(neo4j, state_id=state_id)
    state_store2 = StateStore(neo4j, state_id=state_id)

    state_store1.save(PipelineState(last_status_id=100))
    state_store2.save(PipelineState(last_status_id=200))

    # Should have only one AppState with last update
    loaded = StateStore(neo4j, state_id=state_id).load()
    assert loaded.last_status_id == 200

    # Verify single node with this state_id
    query = "MATCH (s:AppState {id: $sid}) RETURN count(s) AS count"
    rows = list(neo4j.execute_and_fetch(query, {"sid": state_id}))
    assert coerce_int(rows[0].get("count")) == 1


@pytest.mark.integration
def test_state_timestamp_updated(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test that updated_at timestamp is set on save."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    state_store = StateStore(neo4j, state_id=state_id)

    # Initial save
    state_store.save(PipelineState(last_status_id=100))

    # Check timestamp exists
    query = "MATCH (s:AppState {id: $sid}) RETURN s.updated_at AS ts"
    rows = list(neo4j.execute_and_fetch(query, {"sid": state_id}))

    assert rows[0]["ts"] is not None
    first_timestamp_obj = rows[0].get("ts")
    assert isinstance(first_timestamp_obj, (int, float))
    first_timestamp = int(first_timestamp_obj)

    # Update again
    import time

    time.sleep(0.01)  # Ensure timestamp difference
    state_store.save(PipelineState(last_status_id=200))

    # Verify timestamp updated
    rows = list(neo4j.execute_and_fetch(query, {"sid": state_id}))
    second_timestamp_obj = rows[0].get("ts")
    assert isinstance(second_timestamp_obj, (int, float))
    second_timestamp = int(second_timestamp_obj)

    # Timestamps should be different (second >= first)
    assert second_timestamp >= first_timestamp


@pytest.mark.integration
def test_state_from_dict_compatibility(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test PipelineState.from_dict for backward compatibility."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    # Simulate old JSON format
    old_json_data = {
        "last_status_id": 1500,
        "last_favourite_id": 2500,
        "last_follow_id": 3500,
        "last_block_id": 4500,
        "last_mute_id": 5500,
    }

    state = PipelineState.from_dict(old_json_data)

    assert state.last_status_id == 1500
    assert state.last_favourite_id == 2500
    assert state.last_block_id == 4500
    assert state.last_mute_id == 5500

    # Should be able to save to Neo4j
    state_store = StateStore(neo4j, state_id=state_id)
    state_store.save(state)

    # Verify migration
    loaded = state_store.load()
    assert loaded.last_status_id == 1500
