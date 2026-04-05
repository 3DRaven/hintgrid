"""Unit tests for progress display resolution and PlainProgress."""

from __future__ import annotations

from unittest.mock import MagicMock

from hintgrid.cli.progress_display import (
    PlainProgress,
    PlainTaskView,
    resolve_progress_output,
)
from hintgrid.config import HintGridSettings


def test_resolve_progress_output_forces_rich() -> None:
    """Explicit rich ignores non-TTY console."""
    console = MagicMock()
    console.is_terminal = False
    s = HintGridSettings.model_validate({"progress_output": "rich"})
    assert resolve_progress_output(s, console=console) == "rich"


def test_resolve_progress_output_forces_plain() -> None:
    """Explicit plain ignores TTY console."""
    console = MagicMock()
    console.is_terminal = True
    s = HintGridSettings.model_validate({"progress_output": "plain"})
    assert resolve_progress_output(s, console=console) == "plain"


def test_resolve_progress_output_auto_uses_tty() -> None:
    """Auto mode follows console.is_terminal."""
    console = MagicMock()
    console.is_terminal = True
    s = HintGridSettings.model_validate({"progress_output": "auto"})
    assert resolve_progress_output(s, console=console) == "rich"
    console.is_terminal = False
    assert resolve_progress_output(s, console=console) == "plain"


def test_resolve_progress_output_none_settings_auto() -> None:
    """Without settings, only TTY detection applies."""
    console = MagicMock()
    console.is_terminal = True
    assert resolve_progress_output(None, console=console) == "rich"
    console.is_terminal = False
    assert resolve_progress_output(None, console=console) == "plain"


def test_plain_progress_tasks_completed_matches_advance() -> None:
    """PlainProgress maintains completed for integration-style assertions."""
    with PlainProgress() as progress:
        tid = progress.add_task("[cyan]t[/cyan]", total=10)
        progress.advance(tid, 3)
        assert isinstance(progress.tasks[tid], PlainTaskView)
        assert progress.tasks[tid].completed == 3.0


def test_plain_progress_update_completed_and_description() -> None:
    """Single update can set completed, total, and description together."""
    p = PlainProgress()
    tid = p.add_task("x", total=100)
    p.update(
        tid,
        completed=50.0,
        total=100.0,
        description="halfway",
    )
    assert p.tasks[tid].completed == 50.0
    assert p.tasks[tid].total == 100.0
