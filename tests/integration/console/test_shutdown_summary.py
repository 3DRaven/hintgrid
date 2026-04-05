"""Integration tests for print_shutdown_summary Rich output.

Tests verify:
- Summary renders without errors for various step combinations
- All step statuses are displayed correctly
- Cursor table is displayed when state has active cursors
- Empty cursor table is omitted when all cursors are at INITIAL_CURSOR
- Activity cursor is included in the cursor table
"""

from __future__ import annotations

from io import StringIO

import pytest
from rich.console import Console

from hintgrid.cli.console import print_shutdown_summary
from hintgrid.cli.shutdown import PipelineStep, ResumeStrategy, StepStatus
from hintgrid.cli.shutdown import ShutdownManager
from hintgrid.state import PipelineState


def _capture_shutdown_summary(
    steps: list[PipelineStep],
    state: PipelineState | None = None,
) -> str:
    """Render shutdown summary to a string for assertions."""
    # print_shutdown_summary uses the global console; we capture its output
    # by temporarily replacing it
    import hintgrid.cli.console as console_mod

    original_console = console_mod.console
    buffer = StringIO()
    console_mod.console = Console(file=buffer, width=120, force_terminal=True)
    try:
        print_shutdown_summary(steps, state)
    finally:
        console_mod.console = original_console
    return buffer.getvalue()


@pytest.mark.integration
def test_shutdown_summary_renders_without_errors() -> None:
    """print_shutdown_summary executes without raising for typical input."""
    steps = [
        PipelineStep("statuses", "Data loading: statuses", ResumeStrategy.RESUMES, StepStatus.COMPLETED, 1500),
        PipelineStep("favourites", "Data loading: favourites", ResumeStrategy.RESUMES, StepStatus.INTERRUPTED, 200),
        PipelineStep("follows", "Data loading: follows", ResumeStrategy.RESUMES, StepStatus.PENDING),
        PipelineStep("user_clustering", "User clustering", ResumeStrategy.RESTARTS, StepStatus.PENDING),
    ]
    state = PipelineState(last_status_id=5000, last_favourite_id=3000)

    # Should not raise
    print_shutdown_summary(steps, state)


@pytest.mark.integration
def test_shutdown_summary_shows_step_names() -> None:
    """Summary output contains step display names."""
    steps = [
        PipelineStep("statuses", "Data loading: statuses", ResumeStrategy.RESUMES, StepStatus.COMPLETED, 100),
        PipelineStep("user_clustering", "User clustering (Leiden)", ResumeStrategy.RESTARTS, StepStatus.PENDING),
    ]

    output = _capture_shutdown_summary(steps)

    assert "Data loading: statuses" in output
    assert "User clustering (Leiden)" in output


@pytest.mark.integration
def test_shutdown_summary_shows_status_indicators() -> None:
    """Summary shows different status indicators (completed, interrupted, pending)."""
    steps = [
        PipelineStep("statuses", "Statuses", ResumeStrategy.RESUMES, StepStatus.COMPLETED, 100),
        PipelineStep("favourites", "Favourites", ResumeStrategy.RESUMES, StepStatus.INTERRUPTED, 50),
        PipelineStep("follows", "Follows", ResumeStrategy.RESUMES, StepStatus.PENDING),
    ]

    output = _capture_shutdown_summary(steps)

    assert "Completed" in output
    assert "Interrupted" in output
    assert "Pending" in output


@pytest.mark.integration
def test_shutdown_summary_shows_resume_strategies() -> None:
    """Summary shows Resumes vs Restarts for different step types."""
    steps = [
        PipelineStep("statuses", "Statuses", ResumeStrategy.RESUMES, StepStatus.COMPLETED, 100),
        PipelineStep("clustering", "Clustering", ResumeStrategy.RESTARTS, StepStatus.PENDING),
    ]

    output = _capture_shutdown_summary(steps)

    assert "Resumes" in output
    assert "Restarts" in output


@pytest.mark.integration
def test_shutdown_summary_shows_items_processed() -> None:
    """Summary shows formatted item counts for steps with items."""
    steps = [
        PipelineStep("statuses", "Statuses", ResumeStrategy.RESUMES, StepStatus.COMPLETED, 1500),
        PipelineStep("follows", "Follows", ResumeStrategy.RESUMES, StepStatus.COMPLETED, 0),
    ]

    output = _capture_shutdown_summary(steps)

    # 1500 should be formatted with comma as 1,500
    assert "1,500" in output


@pytest.mark.integration
def test_shutdown_summary_shows_cursor_table_when_cursors_active() -> None:
    """Summary shows Saved Cursors table when state has non-zero cursors."""
    steps = [
        PipelineStep("statuses", "Statuses", ResumeStrategy.RESUMES, StepStatus.COMPLETED, 100),
    ]
    state = PipelineState(
        last_status_id=5000,
        last_favourite_id=3000,
        last_activity_account_id=1200,
    )

    output = _capture_shutdown_summary(steps, state)

    assert "Saved Cursors" in output
    assert "last_status_id" in output
    assert "last_favourite_id" in output
    assert "last_activity_account_id" in output
    assert "5,000" in output
    assert "3,000" in output
    assert "1,200" in output


@pytest.mark.integration
def test_shutdown_summary_hides_cursor_table_when_all_zero() -> None:
    """Summary omits Saved Cursors table when all cursors are at INITIAL_CURSOR."""
    steps = [
        PipelineStep("statuses", "Statuses", ResumeStrategy.RESUMES, StepStatus.PENDING),
    ]
    state = PipelineState()  # All cursors at 0

    output = _capture_shutdown_summary(steps, state)

    assert "Saved Cursors" not in output


@pytest.mark.integration
def test_shutdown_summary_without_state() -> None:
    """Summary renders without state (no cursor table)."""
    steps = [
        PipelineStep("statuses", "Statuses", ResumeStrategy.RESUMES, StepStatus.INTERRUPTED, 50),
    ]

    output = _capture_shutdown_summary(steps, state=None)

    assert "Saved Cursors" not in output
    assert "Pipeline Interrupted" in output


@pytest.mark.integration
def test_shutdown_summary_empty_steps() -> None:
    """Summary renders without error even with empty steps list."""
    output = _capture_shutdown_summary([], state=None)

    assert "Pipeline Interrupted" in output
    assert "Pipeline will resume" in output


@pytest.mark.integration
def test_shutdown_summary_all_steps_completed() -> None:
    """Summary renders correctly when all steps are completed."""
    steps = [
        PipelineStep("statuses", "Statuses", ResumeStrategy.RESUMES, StepStatus.COMPLETED, 1000),
        PipelineStep("favourites", "Favourites", ResumeStrategy.RESUMES, StepStatus.COMPLETED, 500),
        PipelineStep("clustering", "Clustering", ResumeStrategy.RESTARTS, StepStatus.COMPLETED, 0),
        PipelineStep("feed_generation", "Feed gen", ResumeStrategy.RESUMES, StepStatus.COMPLETED, 200),
    ]

    output = _capture_shutdown_summary(steps)

    # All should show as completed
    assert output.count("Completed") == 4


@pytest.mark.integration
def test_shutdown_summary_in_progress_shown_as_interrupted() -> None:
    """Steps with IN_PROGRESS status are shown as Interrupted in the summary."""
    steps = [
        PipelineStep("statuses", "Statuses", ResumeStrategy.RESUMES, StepStatus.IN_PROGRESS, 50),
    ]

    output = _capture_shutdown_summary(steps)

    # IN_PROGRESS maps to "Interrupted" in the display
    assert "Interrupted" in output


@pytest.mark.integration
def test_shutdown_summary_activity_cursor_included() -> None:
    """Activity cursor is specifically included in the cursor table."""
    steps = [
        PipelineStep("user_activity", "User activity", ResumeStrategy.RESUMES, StepStatus.INTERRUPTED, 300),
    ]
    state = PipelineState(last_activity_account_id=42000)

    output = _capture_shutdown_summary(steps, state)

    assert "last_activity_account_id" in output
    assert "42,000" in output


@pytest.mark.integration
def test_shutdown_summary_shows_resume_message() -> None:
    """Summary always shows the resume hint message."""
    steps = [
        PipelineStep("statuses", "Statuses", ResumeStrategy.RESUMES, StepStatus.INTERRUPTED, 100),
    ]

    output = _capture_shutdown_summary(steps)

    assert "Pipeline will resume from saved cursors on next run" in output


@pytest.mark.integration
def test_display_shutdown_summary_via_manager() -> None:
    """ShutdownManager.display_shutdown_summary delegates to print_shutdown_summary."""
    sm = ShutdownManager()
    sm.register_steps()
    sm.begin_step("statuses")
    sm.complete_step("statuses", items_processed=999)
    sm.begin_step("favourites")

    state = PipelineState(last_status_id=5000)

    # Should not raise
    sm.display_shutdown_summary(state)
