"""Progress UI: Rich bars in TTY, plain line-based output for journald/pipes."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Self, TypeAlias

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.text import Text

from hintgrid.utils.coercion import coerce_int

if TYPE_CHECKING:
    from rich.console import Console

    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings

_PROGRESS_LOGGER = logging.getLogger("hintgrid.progress")

# Emit at most once per task per second unless percentage jumps (integer %).
_PLAIN_MIN_INTERVAL_S = 1.0
# Indeterminate: emit every N completed items if no time throttle yet
_PLAIN_INDETERMINATE_EVERY = 500


def strip_progress_markup(markup: str) -> str:
    """Convert Rich markup to plain text for log lines."""
    try:
        return Text.from_markup(markup, emoji=False).plain
    except Exception:
        return markup


ResolvedProgressOutput = Literal["rich", "plain"]


def resolve_progress_output(
    settings: HintGridSettings | None,
    *,
    console: Console,
) -> ResolvedProgressOutput:
    """Resolve effective progress output (Rich vs plain line logging).

    When ``settings`` is None, only TTY detection applies (equivalent to ``auto``).
    """
    if settings is not None:
        po = settings.progress_output
        if po == "rich":
            return "rich"
        if po == "plain":
            return "plain"
    if console.is_terminal:
        return "rich"
    return "plain"


@dataclass
class PlainTaskView:
    """Minimal task state exposed for compatibility (e.g. tests)."""

    completed: float = 0.0
    total: float | None = None
    description: str = ""


@dataclass
class PlainProgress:
    """Line-based progress for non-TTY (systemd, CI, pipes).

    Implements the subset of Rich ``Progress`` API used by HintGrid.
    """

    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _next_id: int = 0
    tasks: dict[TaskID, PlainTaskView] = field(default_factory=dict)
    _last_emit_percent: dict[TaskID, int] = field(default_factory=dict)
    _last_emit_time: dict[TaskID, float] = field(default_factory=dict)
    _last_indeterminate_emit: dict[TaskID, int] = field(default_factory=dict)

    def start(self) -> None:
        """No-op (Rich Progress compatibility)."""

    def stop(self) -> None:
        """No-op (Rich Progress compatibility)."""

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        self.stop()

    def add_task(self, description: str, *, total: float | None = None) -> TaskID:
        with self._lock:
            tid: TaskID = TaskID(self._next_id)
            self._next_id += 1
            self.tasks[tid] = PlainTaskView(
                completed=0.0,
                total=total,
                description=description,
            )
            self._emit(tid, force=True)
        return tid

    def advance(self, task_id: TaskID, advance: float = 1) -> None:
        with self._lock:
            t = self.tasks[task_id]
            t.completed += advance
            self._emit(task_id, force=False)

    def update(
        self,
        task_id: TaskID,
        *,
        total: float | None = None,
        completed: float | None = None,
        description: str | None = None,
        advance: float | None = None,
    ) -> None:
        with self._lock:
            t = self.tasks[task_id]
            if total is not None:
                t.total = total
            if completed is not None:
                t.completed = completed
            if advance is not None:
                t.completed += advance
            if description is not None:
                t.description = description
            self._emit(task_id, force=False)

    def _emit(self, task_id: TaskID, *, force: bool) -> None:
        t = self.tasks[task_id]
        plain_desc = strip_progress_markup(t.description)
        now = time.monotonic()

        if t.total is not None and t.total > 0:
            pct = int(100.0 * t.completed / t.total)
            last_pct = self._last_emit_percent.get(task_id, -1)
            last_t = self._last_emit_time.get(task_id, 0.0)
            should = force or pct != last_pct or (now - last_t) >= _PLAIN_MIN_INTERVAL_S
            if should:
                self._last_emit_percent[task_id] = pct
                self._last_emit_time[task_id] = now
                line = f"progress {pct}% ({int(t.completed):,}/{int(t.total):,}) {plain_desc}"
                _PROGRESS_LOGGER.info("%s", line)
            return

        # Indeterminate or unknown total
        completed_i = int(t.completed)
        last_t = self._last_emit_time.get(task_id, 0.0)
        last_c = self._last_indeterminate_emit.get(task_id, -1)
        should = (
            force
            or (completed_i != last_c and completed_i % _PLAIN_INDETERMINATE_EVERY == 0)
            or (now - last_t) >= _PLAIN_MIN_INTERVAL_S
        )
        if should:
            self._last_emit_time[task_id] = now
            self._last_indeterminate_emit[task_id] = completed_i
            line = f"progress {completed_i:,} items {plain_desc}"
            _PROGRESS_LOGGER.info("%s", line)


HintGridProgress: TypeAlias = Progress | PlainProgress


def _get_shared_console() -> Console:
    from hintgrid.cli.console import console as shared

    return shared


def create_pipeline_progress(
    settings: HintGridSettings | None = None,
) -> Progress | PlainProgress:
    """Create progress for pipeline stages (analytics main task)."""
    console = _get_shared_console()
    if resolve_progress_output(settings, console=console) == "plain":
        return PlainProgress()
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    )


def create_data_loading_progress(
    settings: HintGridSettings | None = None,
) -> Progress | PlainProgress:
    """Progress for data loading (streaming, unknown total common)."""
    console = _get_shared_console()
    if resolve_progress_output(settings, console=console) == "plain":
        return PlainProgress()
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TextColumn("[cyan]{task.completed:,}[/cyan] items"),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    )


def create_batch_progress(
    total: int | None = None,
    *,
    settings: HintGridSettings | None = None,
) -> Progress | PlainProgress:
    """Progress for batch operations."""
    console = _get_shared_console()
    if resolve_progress_output(settings, console=console) == "plain":
        return PlainProgress()
    if total is not None:
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            TimeElapsedColumn(),
            console=console,
        )
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TextColumn("[cyan]{task.completed:,}[/cyan] items"),
        TimeElapsedColumn(),
        console=console,
    )


def create_feed_generation_progress(
    settings: HintGridSettings | None = None,
) -> Progress | PlainProgress:
    """Progress for per-user feed generation (matches former app.py layout)."""
    console = _get_shared_console()
    if resolve_progress_output(settings, console=console) == "plain":
        return PlainProgress()
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    )


def track_periodic_iterate_progress(
    neo4j: Neo4jClient,
    operation_id: str,
    progress: HintGridProgress,
    task_id: TaskID,
    poll_interval: float = 0.5,
) -> threading.Thread:
    """Background polling thread for apoc.periodic.iterate ProgressTracker."""
    stop_event = threading.Event()
    logger = logging.getLogger(__name__)

    def poll_progress() -> None:
        last_processed = 0
        while not stop_event.is_set():
            try:
                progress_data = neo4j.get_progress(operation_id)
                if not progress_data:
                    time.sleep(poll_interval)
                    continue

                processed_raw = progress_data.get("processed", 0)
                total_raw = progress_data.get("total")
                batches_raw = progress_data.get("batches", 0)

                processed = coerce_int(processed_raw, 0)
                total = coerce_int(total_raw) if total_raw is not None else None
                batches = coerce_int(batches_raw, 0)

                if total is not None:
                    desc = f"Processed {processed:,} / {total:,} items ({batches} batches)"
                    progress.update(
                        task_id,
                        completed=float(processed),
                        total=float(total),
                        description=desc,
                    )
                else:
                    if processed > last_processed:
                        progress.update(
                            task_id,
                            advance=float(processed - last_processed),
                        )
                    progress.update(
                        task_id,
                        description=(f"Processed {processed:,} items ({batches} batches)"),
                    )

                last_processed = processed

            except Exception as exc:
                logger.debug("Error polling progress: %s", exc, exc_info=True)
            finally:
                time.sleep(poll_interval)

    thread = threading.Thread(target=poll_progress, daemon=True)
    thread.start()
    thread.stop_event = stop_event
    return thread
