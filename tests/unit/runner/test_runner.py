"""Unit tests for CLI runner utilities.

Tests for date parsing and other pure functions
without requiring external dependencies.
"""

from datetime import datetime, timedelta

from hintgrid.cli.runner import _parse_since_date


class TestParseSinceDate:
    """Tests for _parse_since_date helper function."""

    def test_none_input(self) -> None:
        """Test that None returns None."""
        result = _parse_since_date(None)
        assert result is None

    def test_relative_days_format(self) -> None:
        """Test parsing relative days format like '30d'."""
        result = _parse_since_date("30d")

        assert result is not None
        expected = datetime.now() - timedelta(days=30)
        # Allow 1 second tolerance for test execution time
        assert abs((result - expected).total_seconds()) < 1

    def test_relative_days_zero(self) -> None:
        """Test parsing '0d' returns approximately now."""
        result = _parse_since_date("0d")

        assert result is not None
        expected = datetime.now()
        assert abs((result - expected).total_seconds()) < 1

    def test_relative_days_large_number(self) -> None:
        """Test parsing large relative days like '365d'."""
        result = _parse_since_date("365d")

        assert result is not None
        expected = datetime.now() - timedelta(days=365)
        assert abs((result - expected).total_seconds()) < 1

    def test_iso_date_format(self) -> None:
        """Test parsing ISO date format like '2024-01-15'."""
        result = _parse_since_date("2024-01-15")

        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_iso_datetime_format(self) -> None:
        """Test parsing full ISO datetime format."""
        result = _parse_since_date("2024-06-15T10:30:00")

        assert result is not None
        assert result.year == 2024
        assert result.month == 6
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30

    def test_invalid_relative_format(self) -> None:
        """Test that invalid relative format like 'abcd' returns None."""
        result = _parse_since_date("abcd")
        assert result is None

    def test_invalid_days_suffix(self) -> None:
        """Test that 'notanumber'd returns None."""
        result = _parse_since_date("notanumberd")
        assert result is None

    def test_invalid_date_format(self) -> None:
        """Test that completely invalid format returns None."""
        result = _parse_since_date("not-a-date")
        assert result is None

    def test_empty_string(self) -> None:
        """Test that empty string returns None via ISO parsing failure."""
        result = _parse_since_date("")
        assert result is None

    def test_single_d(self) -> None:
        """Test that single 'd' is handled gracefully."""
        result = _parse_since_date("d")
        assert result is None
