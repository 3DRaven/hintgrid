"""Unit tests for type coercion utilities.

Tests verify safe conversion of various types to int/float/str
without requiring any external dependencies.
"""

import pytest

from hintgrid.utils.coercion import (
    coerce_float,
    coerce_int,
    coerce_optional_str,
    coerce_str,
    parse_load_since,
)


class TestCoerceInt:
    """Tests for coerce_int function."""

    def test_int_passthrough(self) -> None:
        """Test that int values are returned unchanged."""
        assert coerce_int(42) == 42
        assert coerce_int(-10) == -10
        assert coerce_int(0) == 0

    def test_float_truncation(self) -> None:
        """Test that float values are truncated to int."""
        assert coerce_int(3.14) == 3
        assert coerce_int(3.99) == 3
        assert coerce_int(-2.5) == -2

    def test_string_conversion(self) -> None:
        """Test that numeric strings are converted."""
        assert coerce_int("42") == 42
        assert coerce_int("-10") == -10
        assert coerce_int("0") == 0

    def test_string_invalid_returns_default(self) -> None:
        """Test that invalid strings return default."""
        assert coerce_int("abc") == 0
        assert coerce_int("abc", default=99) == 99
        assert coerce_int("") == 0
        assert coerce_int("3.14") == 0  # Float string not valid

    def test_bytes_conversion(self) -> None:
        """Test that bytes are decoded and converted."""
        assert coerce_int(b"42") == 42
        assert coerce_int(b"-10") == -10

    def test_bytes_invalid_returns_default(self) -> None:
        """Test that invalid bytes return default."""
        assert coerce_int(b"abc") == 0
        assert coerce_int(b"\xff\xfe") == 0  # Invalid UTF-8

    def test_none_returns_default(self) -> None:
        """Test that None returns default."""
        assert coerce_int(None) == 0
        assert coerce_int(None, default=99) == 99

    def test_bool_conversion(self) -> None:
        """Test that bool is converted to int."""
        assert coerce_int(True) == 1
        assert coerce_int(False) == 0

    def test_unknown_type_returns_default(self) -> None:
        """Test that unknown types return default."""
        assert coerce_int([1, 2, 3]) == 0
        assert coerce_int({"a": 1}) == 0
        assert coerce_int(object()) == 0

    def test_strict_mode_raises_on_none(self) -> None:
        """Test that strict mode raises TypeError on None."""
        with pytest.raises(TypeError, match="expected int, got None"):
            coerce_int(None, strict=True)

    def test_strict_mode_raises_on_bool(self) -> None:
        """Test that strict mode raises TypeError on bool."""
        with pytest.raises(TypeError, match="bool is not allowed"):
            coerce_int(True, strict=True)

    def test_strict_mode_raises_on_invalid_string(self) -> None:
        """Test that strict mode raises ValueError on invalid string."""
        with pytest.raises(ValueError, match="expected numeric string"):
            coerce_int("abc", strict=True)

    def test_strict_mode_with_field_name(self) -> None:
        """Test that strict mode includes field name in error."""
        with pytest.raises(TypeError, match="Invalid user_id"):
            coerce_int(None, strict=True, field="user_id")

    def test_strict_mode_raises_on_invalid_bytes(self) -> None:
        """Test that strict mode raises ValueError on non-decodable bytes."""
        with pytest.raises(ValueError, match="cannot decode bytes to int"):
            coerce_int(b"\xff\xfe", strict=True)

    def test_strict_mode_raises_on_unknown_type(self) -> None:
        """Test that strict mode raises TypeError on unsupported type."""
        with pytest.raises(TypeError, match="expected int, got list"):
            coerce_int([1, 2], strict=True)


class TestCoerceFloat:
    """Tests for coerce_float function."""

    def test_float_passthrough(self) -> None:
        """Test that float values are returned unchanged."""
        assert coerce_float(3.14) == 3.14
        assert coerce_float(-2.5) == -2.5
        assert coerce_float(0.0) == 0.0

    def test_int_conversion(self) -> None:
        """Test that int values are converted to float."""
        assert coerce_float(42) == 42.0
        assert coerce_float(-10) == -10.0

    def test_string_conversion(self) -> None:
        """Test that numeric strings are converted."""
        assert coerce_float("3.14") == 3.14
        assert coerce_float("-2.5") == -2.5
        assert coerce_float("42") == 42.0

    def test_string_invalid_returns_default(self) -> None:
        """Test that invalid strings return default."""
        assert coerce_float("abc") == 0.0
        assert coerce_float("abc", default=1.5) == 1.5

    def test_bool_conversion(self) -> None:
        """Test that bool is converted to float."""
        assert coerce_float(True) == 1.0
        assert coerce_float(False) == 0.0

    def test_none_returns_default(self) -> None:
        """Test that None returns default."""
        assert coerce_float(None) == 0.0
        assert coerce_float(None, default=9.9) == 9.9


class TestCoerceStr:
    """Tests for coerce_str function."""

    def test_string_passthrough(self) -> None:
        """Test that string values are returned unchanged."""
        assert coerce_str("hello") == "hello"
        assert coerce_str("") == ""

    def test_int_conversion(self) -> None:
        """Test that int values are converted to string."""
        assert coerce_str(42) == "42"
        assert coerce_str(-10) == "-10"

    def test_float_conversion(self) -> None:
        """Test that float values are converted to string."""
        assert coerce_str(3.14) == "3.14"

    def test_none_returns_default(self) -> None:
        """Test that None returns default."""
        assert coerce_str(None) == ""
        assert coerce_str(None, default="N/A") == "N/A"

    def test_bytes_decoded_as_utf8(self) -> None:
        """Test that bytes are decoded to string without b'' prefix."""
        assert coerce_str(b"en") == "en"
        assert coerce_str(b"ru") == "ru"
        assert coerce_str(b"hello world") == "hello world"
        assert coerce_str(b"") == ""

    def test_bytes_invalid_utf8_returns_default(self) -> None:
        """Test that non-decodable bytes return default."""
        assert coerce_str(b"\xff\xfe") == ""
        assert coerce_str(b"\xff\xfe", default="unknown") == "unknown"


class TestCoerceOptionalStr:
    """Tests for coerce_optional_str function."""

    def test_string_passthrough(self) -> None:
        """Test that string values are returned unchanged."""
        assert coerce_optional_str("hello") == "hello"
        assert coerce_optional_str("") == ""

    def test_int_conversion(self) -> None:
        """Test that int values are converted to string."""
        assert coerce_optional_str(42) == "42"

    def test_none_returns_none(self) -> None:
        """Test that None returns None (not empty string)."""
        assert coerce_optional_str(None) is None

    def test_bytes_decoded_as_utf8(self) -> None:
        """Test that bytes are decoded to string without b'' prefix."""
        assert coerce_optional_str(b"en") == "en"
        assert coerce_optional_str(b"ru") == "ru"
        assert coerce_optional_str(b"ja") == "ja"

    def test_bytes_invalid_utf8_returns_str_repr(self) -> None:
        """Test that non-decodable bytes fall back to str() representation."""
        result = coerce_optional_str(b"\xff\xfe")
        assert result is not None
        assert isinstance(result, str)


class TestParseLoadSince:
    """Tests for parse_load_since function."""

    def test_valid_days_format(self) -> None:
        """Test parsing valid day formats."""
        assert parse_load_since("30d") == 30
        assert parse_load_since("1d") == 1
        assert parse_load_since("365d") == 365
        assert parse_load_since("  7d  ") == 7  # Whitespace trimmed
        assert parse_load_since("14D") == 14  # Case insensitive

    def test_none_returns_none(self) -> None:
        """Test that None input returns None."""
        assert parse_load_since(None) is None

    def test_empty_string_returns_none(self) -> None:
        """Test that empty string returns None."""
        assert parse_load_since("") is None
        assert parse_load_since("   ") is None

    def test_missing_suffix_raises(self) -> None:
        """Test that missing 'd' suffix raises ValueError."""
        with pytest.raises(ValueError, match="Expected format"):
            parse_load_since("30")

    def test_invalid_number_raises(self) -> None:
        """Test that non-numeric value raises ValueError."""
        with pytest.raises(ValueError, match="Expected format"):
            parse_load_since("abcd")

    def test_zero_days_raises(self) -> None:
        """Test that zero days raises ValueError."""
        with pytest.raises(ValueError, match="must be positive"):
            parse_load_since("0d")

    def test_negative_days_raises(self) -> None:
        """Test that negative days raises ValueError."""
        with pytest.raises(ValueError, match="must be positive"):
            parse_load_since("-5d")
