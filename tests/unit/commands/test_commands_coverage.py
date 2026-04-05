"""Unit tests for CLI commands uncovered paths.

Tests the version callback and train command validation.
"""

from __future__ import annotations

from typer.testing import CliRunner

from hintgrid.cli.commands import app

runner = CliRunner()


class TestVersionCallback:
    """Tests for --version flag."""

    def test_version_flag_shows_version(self) -> None:
        """Test that --version prints version string and exits."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "hintgrid" in result.output

    def test_short_version_flag(self) -> None:
        """Test that -V also prints version."""
        result = runner.invoke(app, ["-V"])
        assert result.exit_code == 0
        assert "hintgrid" in result.output


class TestTrainCommandValidation:
    """Tests for train command mutually exclusive options."""

    def test_train_without_full_or_incremental(self) -> None:
        """Test that train without --full or --incremental exits with error."""
        result = runner.invoke(app, ["train"])
        assert result.exit_code == 2
        assert "Must specify either --full or --incremental" in result.output

    def test_train_with_both_full_and_incremental(self) -> None:
        """Test that train with both flags exits with error."""
        result = runner.invoke(app, ["train", "--full", "--incremental"])
        assert result.exit_code == 2
        assert "Cannot specify both --full and --incremental" in result.output
