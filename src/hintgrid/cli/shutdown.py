"""Graceful shutdown handling for HintGrid CLI."""

from __future__ import annotations

import signal
import threading
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Self

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import FrameType

    from hintgrid.state import PipelineState


class StepStatus(Enum):
    """Status of a pipeline step."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    INTERRUPTED = "interrupted"


class ResumeStrategy(Enum):
    """How a step resumes after interruption."""

    RESUMES = "resumes"
    RESTARTS = "restarts"


@dataclass
class PipelineStep:
    """A tracked pipeline step with its status and resumability."""

    name: str
    display_name: str
    resume_strategy: ResumeStrategy
    status: StepStatus = StepStatus.PENDING
    items_processed: int = 0


# Pipeline step definitions: (internal_name, display_name)
LOADING_STEPS: list[tuple[str, str]] = [
    ("statuses", "Data loading: statuses"),
    ("favourites", "Data loading: favourites"),
    ("blocks", "Data loading: blocks"),
    ("mutes", "Data loading: mutes"),
    ("user_activity", "Data loading: user activity"),
]

ANALYTICS_STEPS: list[tuple[str, str]] = [
    ("user_clustering", "User clustering (Leiden)"),
    ("post_clustering", "Post clustering (Leiden)"),
    ("pagerank", "PageRank"),
    ("similarity_pruning", "Similarity pruning"),
    ("interests", "Interest rebuild"),
    ("community_similarity", "Community similarity"),
    ("serendipity", "Serendipity"),
]

FEED_STEPS: list[tuple[str, str]] = [
    ("feed_generation", "Feed generation"),
]


class ShutdownManager:
    """Manages graceful shutdown on SIGINT/SIGTERM.

    Context manager that installs signal handlers for graceful shutdown.
    First Ctrl+C sets a flag; batch loops check it and exit cleanly.
    Second Ctrl+C raises KeyboardInterrupt for immediate exit.

    Tracks pipeline step progress for the shutdown summary panel.
    Thread-safe: step mutations are serialized via a lock so that
    concurrent entity loaders can call begin_step / complete_step.
    """

    def __init__(self) -> None:
        self._shutdown_requested = threading.Event()
        self._original_sigint: Callable[[int, FrameType | None], None] | int | None = (
            signal.getsignal(signal.SIGINT)
        )
        self._original_sigterm: Callable[[int, FrameType | None], None] | int | None = (
            signal.getsignal(signal.SIGTERM)
        )
        self._steps: list[PipelineStep] = []
        self._current_step_name: str | None = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def shutdown_requested(self) -> bool:
        """Check if graceful shutdown was requested (first Ctrl+C)."""
        return self._shutdown_requested.is_set()

    def request_shutdown(self) -> None:
        """Programmatically request graceful shutdown.

        Sets the shutdown flag without going through the signal handler.
        Useful for testing and for internal shutdown coordination.
        """
        self._shutdown_requested.set()

    def register_steps(self) -> None:
        """Register all pipeline steps for tracking.

        Must be called once at the beginning of run_full_pipeline.
        """
        with self._lock:
            self._steps.clear()
            for name, display in LOADING_STEPS:
                self._steps.append(PipelineStep(
                    name=name,
                    display_name=display,
                    resume_strategy=ResumeStrategy.RESUMES,
                ))
            for name, display in ANALYTICS_STEPS:
                self._steps.append(PipelineStep(
                    name=name,
                    display_name=display,
                    resume_strategy=ResumeStrategy.RESTARTS,
                ))
            for name, display in FEED_STEPS:
                self._steps.append(PipelineStep(
                    name=name,
                    display_name=display,
                    resume_strategy=ResumeStrategy.RESUMES,
                ))

    def begin_step(self, name: str) -> None:
        """Mark a step as in progress."""
        with self._lock:
            self._current_step_name = name
            for step in self._steps:
                if step.name == name:
                    step.status = StepStatus.IN_PROGRESS
                    break

    def complete_step(self, name: str, items_processed: int = 0) -> None:
        """Mark a step as completed with its item count."""
        with self._lock:
            for step in self._steps:
                if step.name == name:
                    step.status = StepStatus.COMPLETED
                    step.items_processed = items_processed
                    break
            if self._current_step_name == name:
                self._current_step_name = None

    def update_step_progress(self, name: str, items_processed: int) -> None:
        """Update items_processed count for a step (without changing status)."""
        with self._lock:
            for step in self._steps:
                if step.name == name:
                    step.items_processed = items_processed
                    break

    @property
    def steps(self) -> list[PipelineStep]:
        """Get a snapshot of all pipeline steps."""
        with self._lock:
            return list(self._steps)

    def display_shutdown_summary(
        self, state: PipelineState | None = None,
    ) -> None:
        """Display the shutdown summary panel."""
        from hintgrid.cli.console import print_shutdown_summary

        print_shutdown_summary(self.steps, state)

    # ------------------------------------------------------------------
    # Context manager: signal handler install / restore
    # ------------------------------------------------------------------

    def __enter__(self) -> Self:
        """Install signal handlers."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Restore original signal handlers."""
        signal.signal(signal.SIGINT, self._original_sigint)
        signal.signal(signal.SIGTERM, self._original_sigterm)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _signal_handler(self, signum: int, frame: FrameType | None) -> None:
        """Handle SIGINT/SIGTERM. First call sets flag, second raises."""
        if self._shutdown_requested.is_set():
            # Second signal: force-quit
            raise KeyboardInterrupt

        # First signal: request graceful shutdown
        self._shutdown_requested.set()

        from hintgrid.cli.console import console

        console.print()
        console.print(
            "[yellow]⚠ Shutdown requested, finishing current batch...[/yellow]",
        )
        console.print("[dim]Press Ctrl+C again to force quit[/dim]")

        # Mark current step as interrupted
        with self._lock:
            if self._current_step_name is not None:
                for step in self._steps:
                    if step.name == self._current_step_name:
                        step.status = StepStatus.INTERRUPTED
                        break
