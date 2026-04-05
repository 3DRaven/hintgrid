"""Integration tests for feed cursor persistence and feedGeneratedAt.

Covers:
- last_feed_user_id cursor persistence (resumable feed generation)
- set_feed_generated_at timestamp on User nodes
"""

from __future__ import annotations

from typing import cast, TYPE_CHECKING

import pytest

from hintgrid.pipeline.feed import set_feed_generated_at
from hintgrid.state import INITIAL_CURSOR, PipelineState, StateStore

if TYPE_CHECKING:
    from neo4j.time import DateTime
    from hintgrid.clients.neo4j import Neo4jClient
    from tests.parallel import IsolatedNeo4jClient


# ============================================================================
# Helpers
# ============================================================================


def _state_id(worker_id: str) -> str:
    """Generate worker-specific state ID for isolation."""
    if worker_id == "master":
        return "feed_opt"
    return f"feed_opt_{worker_id}"


# ============================================================================
# Tests: last_feed_user_id cursor persistence
# ============================================================================


@pytest.mark.integration
def test_last_feed_user_id_default_is_initial_cursor(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Freshly created AppState has last_feed_user_id == INITIAL_CURSOR."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    state_store = StateStore(neo4j, state_id=state_id)
    state = state_store.load()

    assert state.last_feed_user_id == INITIAL_CURSOR


@pytest.mark.integration
def test_last_feed_user_id_save_and_load(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Saving last_feed_user_id persists the value across load cycles."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    state_store = StateStore(neo4j, state_id=state_id)

    state = PipelineState(last_feed_user_id=42)
    state_store.save(state)

    loaded = state_store.load()
    assert loaded.last_feed_user_id == 42


@pytest.mark.integration
def test_last_feed_user_id_incremental_checkpointing(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Simulate checkpointing: cursor advances through user IDs."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    state_store = StateStore(neo4j, state_id=state_id)

    # Simulate processing batches of users
    for checkpoint_user_id in [100, 250, 500]:
        state = state_store.load()
        state.last_feed_user_id = checkpoint_user_id
        state_store.save(state)

        reloaded = state_store.load()
        assert reloaded.last_feed_user_id == checkpoint_user_id


@pytest.mark.integration
def test_last_feed_user_id_does_not_affect_other_cursors(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Updating last_feed_user_id preserves other cursor values."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    state_store = StateStore(neo4j, state_id=state_id)

    initial = PipelineState(
        last_status_id=1000,
        last_favourite_id=2000,
        last_feed_user_id=INITIAL_CURSOR,
    )
    state_store.save(initial)

    # Update only feed cursor
    state = state_store.load()
    state.last_feed_user_id = 999
    state_store.save(state)

    final = state_store.load()
    assert final.last_status_id == 1000
    assert final.last_favourite_id == 2000
    assert final.last_feed_user_id == 999


@pytest.mark.integration
def test_last_feed_user_id_reset_after_completion(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """After full feed generation completes, cursor resets to INITIAL_CURSOR."""
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    state_store = StateStore(neo4j, state_id=state_id)

    # Simulate a partial run
    state = PipelineState(last_feed_user_id=500)
    state_store.save(state)

    # Simulate completion: reset cursor
    state = state_store.load()
    state.last_feed_user_id = INITIAL_CURSOR
    state_store.save(state)

    loaded = state_store.load()
    assert loaded.last_feed_user_id == INITIAL_CURSOR


@pytest.mark.integration
def test_last_feed_user_id_in_from_dict(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """PipelineState.from_dict correctly deserializes last_feed_user_id."""
    data = {
        "last_status_id": 100,
        "last_favourite_id": 200,
        "last_block_id": 400,
        "last_mute_id": 500,
        "last_feed_user_id": 777,
    }
    state = PipelineState.from_dict(data)
    assert state.last_feed_user_id == 777

    # Round-trip through to_dict
    d = state.to_dict()
    assert d["last_feed_user_id"] == 777

    # Save to Neo4j and reload
    neo4j = isolated_neo4j.client
    state_id = _state_id(worker_id)
    store = StateStore(neo4j, state_id=state_id)
    store.save(state)
    loaded = store.load()
    assert loaded.last_feed_user_id == 777


# ============================================================================
# Tests: set_feed_generated_at
# ============================================================================


@pytest.mark.integration
def test_set_feed_generated_at_creates_timestamp(
    neo4j: Neo4jClient,
) -> None:
    """set_feed_generated_at sets feedGeneratedAt on User node."""
    # Create user without feedGeneratedAt
    neo4j.execute_labeled(
        "CREATE (:__user__ {id: $uid})",
        {"user": "User"},
        {"uid": 10001},
    )

    # Verify feedGeneratedAt is initially absent
    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__ {id: $uid}) RETURN u.feedGeneratedAt AS ts",
            {"user": "User"},
            {"uid": 10001},
        )
    )
    assert rows[0]["ts"] is None

    # Set feedGeneratedAt
    set_feed_generated_at(neo4j, 10001)

    # Verify timestamp is now set
    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__ {id: $uid}) RETURN u.feedGeneratedAt AS ts",
            {"user": "User"},
            {"uid": 10001},
        )
    )
    assert rows[0]["ts"] is not None


@pytest.mark.integration
def test_set_feed_generated_at_updates_on_subsequent_calls(
    neo4j: Neo4jClient,
) -> None:
    """Calling set_feed_generated_at again overwrites the old value."""
    import time


    neo4j.execute_labeled(
        "CREATE (:__user__ {id: $uid})",
        {"user": "User"},
        {"uid": 10002},
    )

    set_feed_generated_at(neo4j, 10002)

    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__ {id: $uid}) RETURN u.feedGeneratedAt AS ts",
            {"user": "User"},
            {"uid": 10002},
        )
    )
    first_ts = cast("DateTime", rows[0]["ts"])

    # Wait a small amount and update again
    time.sleep(0.01)
    set_feed_generated_at(neo4j, 10002)

    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__ {id: $uid}) RETURN u.feedGeneratedAt AS ts",
            {"user": "User"},
            {"uid": 10002},
        )
    )
    second_ts = cast("DateTime", rows[0]["ts"])

    assert second_ts >= first_ts
