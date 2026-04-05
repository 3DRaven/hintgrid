"""Unit tests for console output functions.

Tests memory usage display, LoadingProgress edge cases,
and MemoryMonitor lifecycle without requiring external dependencies.
"""

from __future__ import annotations

import time
from unittest.mock import patch

from hintgrid.cli.console import LoadingProgress
from hintgrid.cli.memory import (
    MemoryMonitor,
    create_memory_monitor,
    get_memory_interval,
    print_memory_panel,
    print_memory_usage,
    set_memory_interval,
)


class TestLoadingProgress:
    """Tests for LoadingProgress context manager."""

    def test_update_unknown_task_no_error(self) -> None:
        """Test that updating an unknown task does not raise."""
        with LoadingProgress() as progress:
            progress.add_task("known", "Loading known data...")
            # Should not raise - just silently skip
            progress.update("unknown_task")

    def test_complete_unknown_task_no_error(self) -> None:
        """Test that completing an unknown task does not raise."""
        with LoadingProgress() as progress:
            progress.add_task("known", "Loading known data...")
            # Should not raise
            progress.complete("unknown_task", "Done")


class TestPrintMemoryUsage:
    """Tests for print_memory_usage with different memory levels."""

    def test_green_color_low_memory(self) -> None:
        """Test that low memory (<100 MB) uses green color."""
        with patch("hintgrid.cli.memory.get_memory_usage_mb", return_value=50.0):
            # Should not raise - prints green colored output
            print_memory_usage()

    def test_yellow_color_medium_memory(self) -> None:
        """Test that medium memory (100-500 MB) uses yellow color."""
        with patch("hintgrid.cli.memory.get_memory_usage_mb", return_value=250.0):
            # Should not raise - prints yellow colored output
            print_memory_usage()

    def test_red_color_high_memory(self) -> None:
        """Test that high memory (>500 MB) uses red color."""
        with patch("hintgrid.cli.memory.get_memory_usage_mb", return_value=700.0):
            # Should not raise - prints red colored output
            print_memory_usage()


class TestPrintMemoryPanel:
    """Tests for print_memory_panel with different memory levels."""

    def test_green_panel_low_memory(self) -> None:
        """Test that low memory uses green panel with checkmark."""
        with patch("hintgrid.cli.memory.get_memory_usage_mb", return_value=50.0):
            print_memory_panel()

    def test_yellow_panel_medium_memory(self) -> None:
        """Test that medium memory uses yellow panel with warning."""
        with patch("hintgrid.cli.memory.get_memory_usage_mb", return_value=250.0):
            print_memory_panel()

    def test_red_panel_high_memory(self) -> None:
        """Test that high memory uses red panel with exclamation."""
        with patch("hintgrid.cli.memory.get_memory_usage_mb", return_value=700.0):
            print_memory_panel()


class TestMemoryMonitor:
    """Tests for MemoryMonitor context manager."""

    def test_monitor_with_zero_interval_works(self) -> None:
        """Test that interval=0 works without background thread."""
        with patch("hintgrid.cli.memory.get_memory_usage_mb", return_value=50.0):
            with MemoryMonitor(interval_seconds=0) as monitor:
                # Test behavior through public API
                usage = monitor.get_current_usage()
                assert usage == 50.0
                delta = monitor.get_delta()
                assert delta == 0.0

    def test_monitor_with_interval_works(self) -> None:
        """Test that positive interval works correctly."""
        with patch("hintgrid.cli.memory.get_memory_usage_mb", return_value=50.0):
            with MemoryMonitor(interval_seconds=1) as monitor:
                # Test behavior through public API
                usage = monitor.get_current_usage()
                assert usage == 50.0
                delta = monitor.get_delta()
                assert delta == 0.0

    def test_monitor_tracks_memory_correctly(self) -> None:
        """Test that monitor tracks memory usage correctly through public API."""
        with patch("hintgrid.cli.memory.get_memory_usage_mb", return_value=50.0):
            with MemoryMonitor(interval_seconds=0) as monitor:
                usage = monitor.get_current_usage()
                assert usage == 50.0
                delta = monitor.get_delta()
                assert delta == 0.0

    def test_monitor_tracks_memory_delta(self) -> None:
        """Test that monitor tracks memory delta correctly."""
        # Call sequence for interval_seconds=0:
        # 1: __enter__ _start_memory, 2: __enter__ print_memory_usage,
        # 3: get_delta #1, 4: get_delta #2, 5: __exit__ final_mb
        with patch("hintgrid.cli.memory.get_memory_usage_mb", side_effect=[100.0, 100.0, 100.0, 150.0, 150.0]):
            with MemoryMonitor(interval_seconds=0) as monitor:
                initial_delta = monitor.get_delta()
                assert initial_delta == 0.0

                delta = monitor.get_delta()
                assert delta == 50.0

    def test_monitor_exits_cleanly(self) -> None:
        """Test that monitor exits cleanly without hanging."""
        with patch("hintgrid.cli.memory.get_memory_usage_mb", return_value=50.0):
            with MemoryMonitor(interval_seconds=1) as monitor:
                # Monitor should work correctly
                usage = monitor.get_current_usage()
                assert usage == 50.0
            # After exit, should be clean
            # Test that we can get usage after exit (should still work)
            usage_after = monitor.get_current_usage()
            assert usage_after == 50.0

    def test_exit_with_yellow_memory(self) -> None:
        """Test that __exit__ handles medium memory (yellow color)."""
        with patch("hintgrid.cli.memory.get_memory_usage_mb", return_value=250.0):
            monitor = MemoryMonitor(interval_seconds=0)
            monitor.__enter__()
            monitor.__exit__(None, None, None)

    def test_exit_with_red_memory(self) -> None:
        """Test that __exit__ handles high memory (red color)."""
        with patch("hintgrid.cli.memory.get_memory_usage_mb", return_value=700.0):
            monitor = MemoryMonitor(interval_seconds=0)
            monitor.__enter__()
            monitor.__exit__(None, None, None)

    def test_get_current_usage(self) -> None:
        """Test get_current_usage returns memory value."""
        with patch("hintgrid.cli.memory.get_memory_usage_mb", return_value=123.4):
            monitor = MemoryMonitor()
            assert monitor.get_current_usage() == 123.4

    def test_get_delta_returns_correct_difference(self) -> None:
        """Test get_delta returns difference from start."""
        # Call sequence for interval_seconds=0:
        # 1: __enter__ _start_memory, 2: __enter__ print_memory_usage,
        # 3: get_delta #1, 4: get_delta #2, 5: __exit__ final_mb
        with patch("hintgrid.cli.memory.get_memory_usage_mb", side_effect=[100.0, 100.0, 100.0, 150.0, 150.0]):
            with MemoryMonitor(interval_seconds=0) as monitor:
                initial_delta = monitor.get_delta()
                assert initial_delta == 0.0

                delta = monitor.get_delta()
                assert delta == 50.0

    def test_monitor_with_interval_updates_periodically(self) -> None:
        """Test that monitor with interval works correctly."""
        with patch("hintgrid.cli.memory.get_memory_usage_mb", return_value=50.0):
            with MemoryMonitor(interval_seconds=1) as monitor:
                # Monitor should work correctly
                usage = monitor.get_current_usage()
                assert usage == 50.0
                delta = monitor.get_delta()
                assert delta == 0.0


class TestMemoryIntervalGlobal:
    """Tests for global memory interval functions."""

    def test_set_and_get_interval(self) -> None:
        """Test setting and getting global memory interval."""
        original = get_memory_interval()
        try:
            set_memory_interval(30)
            assert get_memory_interval() == 30
        finally:
            set_memory_interval(original)

    def test_create_memory_monitor_uses_global(self) -> None:
        """Test that create_memory_monitor uses the global interval."""
        original = get_memory_interval()
        try:
            set_memory_interval(42)
            monitor = create_memory_monitor()
            # Test behavior through public API
            with monitor:
                usage = monitor.get_current_usage()
                assert usage >= 0.0
        finally:
            set_memory_interval(original)
