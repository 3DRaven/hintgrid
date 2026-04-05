"""Integration tests for ShutdownManager signal handling and step tracking.

Tests verify:
- Context manager installs/restores signal handlers
- First signal sets shutdown_requested flag without raising
- Second signal raises KeyboardInterrupt
- Pipeline step lifecycle: register → begin → complete
- Thread-safe concurrent step mutations
- Interrupted step is correctly marked
"""

from __future__ import annotations

import os
import signal
import threading

import pytest

from hintgrid.cli.shutdown import (
    ResumeStrategy,
    ShutdownManager,
    StepStatus,
)


@pytest.mark.integration
def test_shutdown_manager_context_installs_and_restores_handlers() -> None:
    """ShutdownManager installs custom handlers on __enter__ and restores on __exit__."""
    original_sigint = signal.getsignal(signal.SIGINT)
    original_sigterm = signal.getsignal(signal.SIGTERM)

    with ShutdownManager() as _sm:
        # Inside context: handlers should be replaced
        current_sigint = signal.getsignal(signal.SIGINT)
        current_sigterm = signal.getsignal(signal.SIGTERM)
        assert current_sigint != original_sigint, "SIGINT handler should be replaced"
        assert current_sigterm != original_sigterm, "SIGTERM handler should be replaced"

    # After context: handlers should be restored
    restored_sigint = signal.getsignal(signal.SIGINT)
    restored_sigterm = signal.getsignal(signal.SIGTERM)
    assert restored_sigint == original_sigint, "SIGINT handler should be restored"
    assert restored_sigterm == original_sigterm, "SIGTERM handler should be restored"


@pytest.mark.integration
def test_shutdown_manager_not_requested_by_default() -> None:
    """ShutdownManager starts with shutdown_requested == False."""
    sm = ShutdownManager()
    assert sm.shutdown_requested is False


@pytest.mark.integration
def test_first_signal_sets_shutdown_requested() -> None:
    """First SIGINT sets shutdown_requested without raising."""
    sm = ShutdownManager()
    with sm:
        # Send SIGINT to ourselves — the handler should catch it
        os.kill(os.getpid(), signal.SIGINT)
        assert sm.shutdown_requested is True


@pytest.mark.integration
def test_request_shutdown_sets_flag() -> None:
    """request_shutdown() programmatically sets shutdown_requested."""
    sm = ShutdownManager()
    assert sm.shutdown_requested is False
    sm.request_shutdown()
    assert sm.shutdown_requested is True


@pytest.mark.integration
def test_second_signal_raises_keyboard_interrupt() -> None:
    """Second signal raises KeyboardInterrupt for force-quit."""
    sm = ShutdownManager()
    with sm:
        # First signal: sets flag via public API
        sm.request_shutdown()
        assert sm.shutdown_requested is True

        # Second signal: should raise KeyboardInterrupt
        with pytest.raises(KeyboardInterrupt):
            os.kill(os.getpid(), signal.SIGINT)


@pytest.mark.integration
def test_register_steps_creates_all_pipeline_steps() -> None:
    """register_steps populates the steps list with loading, analytics, and feed steps."""
    sm = ShutdownManager()
    sm.register_steps()
    steps = sm.steps

    # Verify we have steps from all categories
    step_names = [s.name for s in steps]
    assert "statuses" in step_names, "Should include loading step 'statuses'"
    assert "favourites" in step_names, "Should include loading step 'favourites'"
    assert "user_activity" in step_names, "Should include loading step 'user_activity'"
    assert "user_clustering" in step_names, "Should include analytics step 'user_clustering'"
    assert "pagerank" in step_names, "Should include analytics step 'pagerank'"
    assert "feed_generation" in step_names, "Should include feed step 'feed_generation'"

    # All steps start as PENDING
    for step in steps:
        assert step.status == StepStatus.PENDING, f"Step {step.name} should start as PENDING"


@pytest.mark.integration
def test_register_steps_sets_correct_resume_strategies() -> None:
    """Loading steps resume, analytics steps restart."""
    sm = ShutdownManager()
    sm.register_steps()
    steps = sm.steps

    # Loading steps should RESUME
    loading_names = {"statuses", "favourites", "blocks", "mutes", "user_activity"}
    for step in steps:
        if step.name in loading_names:
            assert step.resume_strategy == ResumeStrategy.RESUMES, (
                f"Loading step {step.name} should have RESUMES strategy"
            )

    # Analytics steps should RESTART
    analytics_names = {
        "user_clustering", "post_clustering", "pagerank",
        "interests", "community_similarity", "serendipity",
    }
    for step in steps:
        if step.name in analytics_names:
            assert step.resume_strategy == ResumeStrategy.RESTARTS, (
                f"Analytics step {step.name} should have RESTARTS strategy"
            )


@pytest.mark.integration
def test_begin_step_marks_in_progress() -> None:
    """begin_step sets step status to IN_PROGRESS."""
    sm = ShutdownManager()
    sm.register_steps()

    sm.begin_step("statuses")

    steps = sm.steps
    status_step = next(s for s in steps if s.name == "statuses")
    assert status_step.status == StepStatus.IN_PROGRESS


@pytest.mark.integration
def test_complete_step_marks_completed_with_items() -> None:
    """complete_step sets status to COMPLETED and records items_processed."""
    sm = ShutdownManager()
    sm.register_steps()

    sm.begin_step("statuses")
    sm.complete_step("statuses", items_processed=1500)

    steps = sm.steps
    status_step = next(s for s in steps if s.name == "statuses")
    assert status_step.status == StepStatus.COMPLETED
    assert status_step.items_processed == 1500


@pytest.mark.integration
def test_update_step_progress_sets_items_without_changing_status() -> None:
    """update_step_progress updates items count without touching status."""
    sm = ShutdownManager()
    sm.register_steps()

    sm.begin_step("favourites")
    sm.update_step_progress("favourites", 250)

    steps = sm.steps
    fav_step = next(s for s in steps if s.name == "favourites")
    assert fav_step.status == StepStatus.IN_PROGRESS, "Status should remain IN_PROGRESS"
    assert fav_step.items_processed == 250


@pytest.mark.integration
def test_signal_marks_current_step_as_interrupted() -> None:
    """When signal fires, the current in-progress step is marked INTERRUPTED."""
    sm = ShutdownManager()
    with sm:
        sm.register_steps()
        sm.begin_step("statuses")

        # Send SIGINT to trigger the signal handler
        os.kill(os.getpid(), signal.SIGINT)

        steps = sm.steps
        statuses_step = next(s for s in steps if s.name == "statuses")
        assert statuses_step.status == StepStatus.INTERRUPTED


@pytest.mark.integration
def test_step_lifecycle_multiple_steps() -> None:
    """Verify full step lifecycle across multiple sequential steps."""
    sm = ShutdownManager()
    sm.register_steps()

    # Complete statuses
    sm.begin_step("statuses")
    sm.complete_step("statuses", items_processed=1000)

    # Complete favourites
    sm.begin_step("favourites")
    sm.complete_step("favourites", items_processed=500)

    # Begin blocks but don't complete
    sm.begin_step("blocks")

    steps = sm.steps
    statuses = next(s for s in steps if s.name == "statuses")
    favourites = next(s for s in steps if s.name == "favourites")
    blocks = next(s for s in steps if s.name == "blocks")
    mutes = next(s for s in steps if s.name == "mutes")

    assert statuses.status == StepStatus.COMPLETED
    assert statuses.items_processed == 1000
    assert favourites.status == StepStatus.COMPLETED
    assert favourites.items_processed == 500
    assert blocks.status == StepStatus.IN_PROGRESS
    assert mutes.status == StepStatus.PENDING


@pytest.mark.integration
def test_steps_property_returns_new_list() -> None:
    """steps property returns a new list object each time (not the internal list)."""
    sm = ShutdownManager()
    sm.register_steps()

    snapshot1 = sm.steps
    snapshot2 = sm.steps

    # Each call returns a different list object
    assert snapshot1 is not snapshot2, "steps should return a new list each time"

    # But the list contents are equivalent
    assert len(snapshot1) == len(snapshot2)
    assert [s.name for s in snapshot1] == [s.name for s in snapshot2]

    # Mutating the returned list does not affect the manager
    snapshot1.clear()
    assert len(sm.steps) > 0, "Clearing snapshot should not affect internal steps"


@pytest.mark.integration
def test_concurrent_step_mutations_are_thread_safe() -> None:
    """Multiple threads can safely call begin_step/complete_step concurrently."""
    sm = ShutdownManager()
    sm.register_steps()

    errors: list[Exception] = []
    barrier = threading.Barrier(4)

    def worker(step_name: str) -> None:
        try:
            barrier.wait(timeout=5)
            sm.begin_step(step_name)
            sm.update_step_progress(step_name, 100)
            sm.complete_step(step_name, items_processed=100)
        except Exception as e:
            errors.append(e)

    threads = [
        threading.Thread(target=worker, args=(name,))
        for name in ("favourites", "blocks", "mutes", "user_activity")
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert errors == [], f"Thread errors: {errors}"

    # All four steps should be completed
    steps = sm.steps
    for name in ("favourites", "blocks", "mutes", "user_activity"):
        step = next(s for s in steps if s.name == name)
        assert step.status == StepStatus.COMPLETED, f"{name} should be COMPLETED"
        assert step.items_processed == 100, f"{name} should have 100 items"


@pytest.mark.integration
def test_shutdown_requested_is_thread_safe() -> None:
    """shutdown_requested can be checked from multiple threads safely."""
    sm = ShutdownManager()
    results: list[bool] = []
    barrier = threading.Barrier(4)

    def checker() -> None:
        barrier.wait(timeout=5)
        results.append(sm.shutdown_requested)

    with sm:
        threads = [threading.Thread(target=checker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

    assert all(r is False for r in results), "All threads should see False"


@pytest.mark.integration
def test_shutdown_event_visible_across_threads() -> None:
    """After request_shutdown, shutdown_requested is visible from background thread."""
    sm = ShutdownManager()
    seen_shutdown = threading.Event()

    def background_poller() -> None:
        while not sm.shutdown_requested:
            pass
        seen_shutdown.set()

    with sm:
        thread = threading.Thread(target=background_poller, daemon=True)
        thread.start()

        # Programmatically request shutdown
        sm.request_shutdown()

        # Background thread should see the shutdown flag
        assert seen_shutdown.wait(timeout=5), "Background thread should see shutdown_requested"
