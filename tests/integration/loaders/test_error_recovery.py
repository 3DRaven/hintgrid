"""Integration tests for loader error recovery.

Covers:
- Checkpoint recovery after interruption
- Corrupted data handling
- Shutdown during loading
"""

from __future__ import annotations

import pytest

from hintgrid.cli.shutdown import ShutdownManager
from hintgrid.state import PipelineState, StateStore
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tests.parallel import IsolatedNeo4jClient


@pytest.mark.integration
def test_checkpoint_recovery_after_interruption(
    isolated_neo4j: IsolatedNeo4jClient,
    worker_id: str,
) -> None:
    """Test that checkpoints allow recovery after interruption."""
    neo4j = isolated_neo4j.client
    state_id = f"checkpoint_test_{worker_id}"
    state_store = StateStore(neo4j, state_id=state_id)

    # Simulate processing up to checkpoint
    state = PipelineState(last_status_id=5000)
    state_store.save(state)

    # Load checkpoint
    recovered = state_store.load()
    assert recovered.last_status_id == 5000, "Should recover from checkpoint"


@pytest.mark.integration
def test_shutdown_during_loading(
    isolated_neo4j: IsolatedNeo4jClient,
) -> None:
    """Test shutdown manager during loading operations."""
    shutdown = ShutdownManager()
    shutdown.register_steps()

    shutdown.begin_step("statuses")
    shutdown.update_step_progress("statuses", 1000)
    shutdown.request_shutdown()

    assert shutdown.shutdown_requested is True
    steps = shutdown.steps
    status_step = next(s for s in steps if s.name == "statuses")
    assert status_step.status.value in ["in_progress", "interrupted"]
