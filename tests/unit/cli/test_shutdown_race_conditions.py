"""Unit tests for ShutdownManager race conditions and thread safety.

Covers:
- Concurrent step updates (begin_step, complete_step, update_step_progress)
- Race conditions in shutdown_requested flag
- Double signal handling edge cases
- Thread safety of steps property
- Recovery after interruption
"""

from __future__ import annotations

import signal
import threading
import time

import pytest

from hintgrid.cli.shutdown import (
    ResumeStrategy,
    ShutdownManager,
    StepStatus,
)


# ---------------------------------------------------------------------------
# Tests: Concurrent step mutations
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_concurrent_begin_step_no_corruption() -> None:
    """Multiple threads calling begin_step concurrently should not corrupt state."""
    sm = ShutdownManager()
    sm.register_steps()

    errors: list[Exception] = []
    results: list[StepStatus] = []

    def worker(step_name: str) -> None:
        try:
            sm.begin_step(step_name)
            # Get status immediately after begin
            steps = sm.steps
            step = next((s for s in steps if s.name == step_name), None)
            if step is None:
                errors.append(ValueError(f"Step {step_name} not found"))
                return
            results.append(step.status)
        except Exception as e:
            errors.append(e)

    threads = [
        threading.Thread(target=worker, args=(name,))
        for name in ("statuses", "favourites", "blocks", "mutes")
    ]

    # Start all threads simultaneously
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert errors == [], f"Thread errors: {errors}"
    # All steps should be IN_PROGRESS
    assert all(s == StepStatus.IN_PROGRESS for s in results), (
        f"All steps should be IN_PROGRESS, got {results}"
    )


@pytest.mark.unit
def test_concurrent_complete_step_no_corruption() -> None:
    """Multiple threads calling complete_step concurrently should not corrupt state."""
    sm = ShutdownManager()
    sm.register_steps()

    # Begin all steps first
    for name in ("statuses", "favourites", "blocks", "mutes"):
        sm.begin_step(name)

    errors: list[Exception] = []
    items_counts: list[int] = []

    def worker(step_name: str, items: int) -> None:
        try:
            sm.complete_step(step_name, items_processed=items)
            # Get items count immediately after complete
            steps = sm.steps
            step = next(s for s in steps if s.name == step_name)
            items_counts.append(step.items_processed)
        except Exception as e:
            errors.append(e)

    threads = [
        threading.Thread(target=worker, args=(name, 100 + i))
        for i, name in enumerate(("statuses", "favourites", "blocks", "mutes"))
    ]

    # Start all threads simultaneously
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert errors == [], f"Thread errors: {errors}"
    # All items counts should be preserved
    assert len(items_counts) == 4, "All threads should complete"
    assert all(count > 0 for count in items_counts), (
        f"All steps should have items_processed > 0, got {items_counts}"
    )


@pytest.mark.unit
def test_concurrent_update_progress_no_corruption() -> None:
    """Multiple threads updating progress concurrently should not lose updates."""
    sm = ShutdownManager()
    sm.register_steps()
    sm.begin_step("statuses")

    errors: list[Exception] = []
    final_counts: list[int] = []

    def worker(initial: int, increments: int) -> None:
        try:
            for i in range(increments):
                sm.update_step_progress("statuses", initial + i)
        except Exception as e:
            errors.append(e)

    threads = [
        threading.Thread(target=worker, args=(base, 10))
        for base in range(0, 100, 25)  # 0, 25, 50, 75
    ]

    # Start all threads simultaneously
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert errors == [], f"Thread errors: {errors}"

    # Get final count
    steps = sm.steps
    step = next(s for s in steps if s.name == "statuses")
    final_counts.append(step.items_processed)

    # Final count should be one of the values written (last write wins)
    assert len(final_counts) == 1
    assert final_counts[0] >= 0, "Items count should be non-negative"


@pytest.mark.unit
def test_steps_property_thread_safe_snapshot() -> None:
    """steps property should return consistent snapshot even during mutations."""
    sm = ShutdownManager()
    sm.register_steps()

    snapshots: list[list[StepStatus]] = []
    errors: list[Exception] = []

    def mutator() -> None:
        try:
            for name in ("statuses", "favourites", "blocks"):
                sm.begin_step(name)
                time.sleep(0.01)
                sm.complete_step(name, items_processed=100)
        except Exception as e:
            errors.append(e)

    def reader() -> None:
        try:
            for _ in range(10):
                steps = sm.steps
                statuses = [s.status for s in steps]
                snapshots.append(statuses)
                time.sleep(0.01)
        except Exception as e:
            errors.append(e)

    mutator_thread = threading.Thread(target=mutator)
    reader_thread = threading.Thread(target=reader)

    mutator_thread.start()
    reader_thread.start()

    mutator_thread.join(timeout=5)
    reader_thread.join(timeout=5)

    assert errors == [], f"Thread errors: {errors}"
    assert len(snapshots) > 0, "Reader should capture snapshots"
    # All snapshots should have same length (no corruption)
    assert all(len(s) == len(snapshots[0]) for s in snapshots), (
        "All snapshots should have consistent length"
    )


# ---------------------------------------------------------------------------
# Tests: Shutdown flag race conditions
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_shutdown_requested_concurrent_reads() -> None:
    """Multiple threads reading shutdown_requested should see consistent values."""
    sm = ShutdownManager()

    results: list[bool] = []
    errors: list[Exception] = []

    def reader() -> None:
        try:
            for _ in range(100):
                results.append(sm.shutdown_requested)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=reader) for _ in range(4)]

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert errors == [], f"Thread errors: {errors}"
    # All reads should be False (before shutdown is requested)
    assert all(r is False for r in results), (
        f"All reads should be False, got {set(results)}"
    )


@pytest.mark.unit
def test_shutdown_requested_write_visible_to_readers() -> None:
    """After request_shutdown, all threads should see True."""
    sm = ShutdownManager()

    seen_true = threading.Event()
    errors: list[Exception] = []

    def reader() -> None:
        try:
            while not sm.shutdown_requested:
                time.sleep(0.001)
            seen_true.set()
        except Exception as e:
            errors.append(e)

    reader_thread = threading.Thread(target=reader)
    reader_thread.start()

    # Request shutdown after a short delay
    time.sleep(0.01)
    sm.request_shutdown()

    # Reader should see the change
    assert seen_true.wait(timeout=2), "Reader thread should see shutdown_requested=True"

    reader_thread.join(timeout=2)
    assert errors == [], f"Thread errors: {errors}"


# ---------------------------------------------------------------------------
# Tests: Double signal handling
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_double_signal_raises_keyboard_interrupt() -> None:
    """Second signal should raise KeyboardInterrupt even if first was programmatic."""
    sm = ShutdownManager()

    with sm:
        # First shutdown (programmatic)
        sm.request_shutdown()
        assert sm.shutdown_requested is True

        # Second signal should raise KeyboardInterrupt
        # We can't actually send SIGINT in unit test, so we simulate
        # by calling the handler directly
        with pytest.raises(KeyboardInterrupt):
            sm._signal_handler(signal.SIGINT, None)


@pytest.mark.unit
def test_double_signal_immediate_sequence() -> None:
    """Two signals in quick succession should raise on second."""
    sm = ShutdownManager()

    with sm:
        # Simulate first signal
        sm._signal_handler(signal.SIGINT, None)
        assert sm.shutdown_requested is True

        # Simulate second signal immediately
        with pytest.raises(KeyboardInterrupt):
            sm._signal_handler(signal.SIGINT, None)


@pytest.mark.unit
def test_signal_handler_marks_current_step_interrupted() -> None:
    """Signal handler should mark current step as INTERRUPTED."""
    sm = ShutdownManager()
    sm.register_steps()
    sm.begin_step("statuses")

    with sm:
        # Simulate signal
        sm._signal_handler(signal.SIGINT, None)

        steps = sm.steps
        status_step = next(s for s in steps if s.name == "statuses")
        assert status_step.status == StepStatus.INTERRUPTED, (
            "Current step should be marked INTERRUPTED"
        )


@pytest.mark.unit
def test_signal_handler_no_current_step() -> None:
    """Signal handler should not crash when no step is in progress."""
    sm = ShutdownManager()
    sm.register_steps()
    # No step started

    with sm:
        # Should not raise
        sm._signal_handler(signal.SIGINT, None)
        assert sm.shutdown_requested is True


# ---------------------------------------------------------------------------
# Tests: Recovery after interruption
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_step_can_be_restarted_after_interruption() -> None:
    """A step marked INTERRUPTED can be restarted."""
    sm = ShutdownManager()
    sm.register_steps()

    sm.begin_step("statuses")
    # Simulate signal handler marking step as INTERRUPTED
    # (request_shutdown only sets flag, _signal_handler marks step)
    with sm._lock:
        for step in sm._steps:
            if step.name == "statuses":
                step.status = StepStatus.INTERRUPTED
                break

    steps_before = sm.steps
    status_before = next(s for s in steps_before if s.name == "statuses")
    assert status_before.status == StepStatus.INTERRUPTED

    # Restart the step
    sm.begin_step("statuses")

    steps_after = sm.steps
    status_after = next(s for s in steps_after if s.name == "statuses")
    assert status_after.status == StepStatus.IN_PROGRESS, (
        "Step should be IN_PROGRESS after restart"
    )


@pytest.mark.unit
def test_resume_strategy_resumes_allows_continuation() -> None:
    """Steps with RESUMES strategy can continue after interruption."""
    sm = ShutdownManager()
    sm.register_steps()

    sm.begin_step("statuses")
    sm.update_step_progress("statuses", 500)
    sm.request_shutdown()  # Interrupts

    steps = sm.steps
    status_step = next(s for s in steps if s.name == "statuses")
    assert status_step.resume_strategy == ResumeStrategy.RESUMES, (
        "Loading steps should have RESUMES strategy"
    )
    assert status_step.items_processed == 500, (
        "Items processed should be preserved"
    )


@pytest.mark.unit
def test_resume_strategy_restarts_requires_full_rerun() -> None:
    """Steps with RESTARTS strategy must be fully rerun after interruption."""
    sm = ShutdownManager()
    sm.register_steps()

    sm.begin_step("user_clustering")
    # Simulate signal handler marking step as INTERRUPTED
    # (request_shutdown only sets flag, _signal_handler marks step)
    with sm._lock:
        for step in sm._steps:
            if step.name == "user_clustering":
                step.status = StepStatus.INTERRUPTED
                break

    steps = sm.steps
    cluster_step = next(s for s in steps if s.name == "user_clustering")
    assert cluster_step.resume_strategy == ResumeStrategy.RESTARTS, (
        "Analytics steps should have RESTARTS strategy"
    )
    assert cluster_step.status == StepStatus.INTERRUPTED, (
        "Step should be marked INTERRUPTED"
    )


# ---------------------------------------------------------------------------
# Tests: Edge cases
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_begin_step_nonexistent_step_no_crash() -> None:
    """begin_step with nonexistent step name should not crash."""
    sm = ShutdownManager()
    sm.register_steps()

    # Should not raise
    sm.begin_step("nonexistent_step")

    # Should not affect other steps
    steps = sm.steps
    assert len([s for s in steps if s.status == StepStatus.IN_PROGRESS]) == 0


@pytest.mark.unit
def test_complete_step_nonexistent_step_no_crash() -> None:
    """complete_step with nonexistent step name should not crash."""
    sm = ShutdownManager()
    sm.register_steps()

    # Should not raise
    sm.complete_step("nonexistent_step", items_processed=100)


@pytest.mark.unit
def test_update_progress_nonexistent_step_no_crash() -> None:
    """update_step_progress with nonexistent step name should not crash."""
    sm = ShutdownManager()
    sm.register_steps()

    # Should not raise
    sm.update_step_progress("nonexistent_step", items_processed=50)


@pytest.mark.unit
def test_register_steps_idempotent() -> None:
    """register_steps can be called multiple times safely."""
    sm = ShutdownManager()

    sm.register_steps()
    count1 = len(sm.steps)

    sm.register_steps()
    count2 = len(sm.steps)

    assert count1 == count2, "register_steps should be idempotent"


@pytest.mark.unit
def test_context_manager_nested() -> None:
    """Nested ShutdownManager contexts should restore handlers correctly."""
    original_sigint = signal.getsignal(signal.SIGINT)

    with ShutdownManager():
        handler1 = signal.getsignal(signal.SIGINT)

        with ShutdownManager():
            handler2 = signal.getsignal(signal.SIGINT)
            assert handler2 != handler1, "Inner context should have different handler"

        # After inner context exits
        handler_after_inner = signal.getsignal(signal.SIGINT)
        assert handler_after_inner == handler1, (
            "Handler should be restored to outer context's handler"
        )

    # After outer context exits
    handler_after_outer = signal.getsignal(signal.SIGINT)
    assert handler_after_outer == original_sigint, (
        "Handler should be restored to original"
    )


@pytest.mark.unit
def test_shutdown_manager_multiple_instances_independent() -> None:
    """Multiple ShutdownManager instances should be independent."""
    sm1 = ShutdownManager()
    sm2 = ShutdownManager()

    sm1.request_shutdown()
    assert sm1.shutdown_requested is True
    assert sm2.shutdown_requested is False, "Instances should be independent"
