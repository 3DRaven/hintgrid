"""Memory monitoring utilities for HintGrid CLI."""

from __future__ import annotations

import os
import threading

import psutil
from rich.live import Live
from rich.text import Text
from typing import TYPE_CHECKING, Self

from hintgrid.cli.console import console

if TYPE_CHECKING:
    from rich.console import RenderableType


def get_memory_usage_mb() -> float:
    """Get current process memory usage in megabytes (RSS)."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)


def print_memory_usage() -> None:
    """Print current memory usage with rich formatting."""
    mb = get_memory_usage_mb()
    if mb < 100:
        color = "green"
    elif mb < 500:
        color = "yellow"
    else:
        color = "red"
    console.print(f"[dim]💾 Memory:[/dim] [{color}]{mb:.1f} MB[/{color}]")


def print_memory_panel(title: str = "Memory Usage") -> None:
    """Print memory usage in a panel for more prominent display."""
    from rich.panel import Panel

    mb = get_memory_usage_mb()
    if mb < 100:
        color = "green"
        icon = "✓"
    elif mb < 500:
        color = "yellow"
        icon = "⚠"
    else:
        color = "red"
        icon = "!"
    console.print(
        Panel(
            f"[{color}]{icon} {mb:.1f} MB[/{color}]",
            title=f"[dim]{title}[/dim]",
            border_style="dim",
            expand=False,
        )
    )


class MemoryMonitor:
    """Context manager for periodic memory usage display.

    Uses Rich Live display to show memory as a single updating line,
    similar to spinners. Thread-safe and non-intrusive.
    """

    # Default interval in seconds
    DEFAULT_INTERVAL = 10

    def __init__(self, interval_seconds: int = DEFAULT_INTERVAL) -> None:
        """Initialize memory monitor.

        Args:
            interval_seconds: Interval between memory updates in seconds.
                              Use 0 to disable periodic updates.
        """
        self._interval = interval_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._start_memory: float = 0.0
        self._live: Live | None = None

    def _get_memory_renderable(self) -> RenderableType:
        """Create renderable for current memory state."""
        current_mb = get_memory_usage_mb()
        delta = current_mb - self._start_memory
        delta_sign = "+" if delta >= 0 else ""

        if current_mb < 100:
            color = "green"
        elif current_mb < 500:
            color = "yellow"
        else:
            color = "red"

        return Text.from_markup(
            f"[dim]💾 Memory:[/dim] [{color}]{current_mb:.1f} MB[/{color}] "
            f"[dim]({delta_sign}{delta:.1f} MB)[/dim]"
        )

    def _monitor_loop(self) -> None:
        """Background thread loop for periodic memory display updates."""
        while not self._stop_event.wait(timeout=self._interval):
            if self._stop_event.is_set():
                break
            if self._live is not None:
                self._live.update(self._get_memory_renderable())

    def __enter__(self) -> Self:
        """Start memory monitoring."""
        self._start_memory = get_memory_usage_mb()

        if self._interval > 0:
            self._live = Live(
                self._get_memory_renderable(),
                console=console,
                refresh_per_second=1,
                transient=False,
            )
            self._live.start()

            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._monitor_loop,
                daemon=True,
                name="MemoryMonitor",
            )
            self._thread.start()
        else:
            # Just show initial memory without live updates
            print_memory_usage()

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Stop memory monitoring and print final stats."""
        if self._thread is not None:
            self._stop_event.set()
            self._thread.join(timeout=1.0)
            self._thread = None

        if self._live is not None:
            self._live.stop()
            self._live = None

        # Print final memory usage
        final_mb = get_memory_usage_mb()
        delta = final_mb - self._start_memory
        delta_sign = "+" if delta >= 0 else ""

        if final_mb < 100:
            color = "green"
        elif final_mb < 500:
            color = "yellow"
        else:
            color = "red"

        console.print(
            f"[dim]💾 Final:[/dim] [{color}]{final_mb:.1f} MB[/{color}] "
            f"[dim](Δ {delta_sign}{delta:.1f} MB)[/dim]"
        )

    def get_current_usage(self) -> float:
        """Get current memory usage in MB."""
        return get_memory_usage_mb()

    def get_delta(self) -> float:
        """Get memory delta since start in MB."""
        return get_memory_usage_mb() - self._start_memory


# Global memory monitor instance for sharing interval across commands
_memory_interval: int = MemoryMonitor.DEFAULT_INTERVAL


def set_memory_interval(interval: int) -> None:
    """Set global memory monitoring interval."""
    global _memory_interval
    _memory_interval = interval


def get_memory_interval() -> int:
    """Get global memory monitoring interval."""
    return _memory_interval


def create_memory_monitor() -> MemoryMonitor:
    """Create a memory monitor with the global interval setting."""
    return MemoryMonitor(interval_seconds=_memory_interval)
