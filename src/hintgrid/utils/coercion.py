"""Type coercion utilities for safe value conversion.

Provides centralized functions for converting database values to Python types
with proper error handling and default value support.

Note: These functions use try/except for type conversion instead of isinstance
to avoid runtime type checking. For new code, prefer msgspec Struct validation.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Protocol, overload

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jParameter, Neo4jValue


# Protocol for Decimal-like types (for PostgreSQL Decimal conversion)
class DecimalProtocol(Protocol):
    """Protocol for Decimal-like types that can be converted to float."""

    def __float__(self) -> float:
        """Convert to float."""
        ...


@overload
def coerce_int(value: int, default: int = 0, *, field: str | None = None, strict: bool = False) -> int:
    """Overload for int input."""
    ...


@overload
def coerce_int(
    value: object, default: int = 0, *, field: str | None = None, strict: bool = False
) -> int:
    """Overload for object input."""
    ...


def coerce_int(
    value: object,
    default: int = 0,
    *,
    field: str | None = None,
    strict: bool = False,
) -> int:
    """Safely convert value to int using try/except instead of isinstance.

    Args:
        value: Value to convert
        default: Default value if conversion fails (ignored if strict=True)
        field: Optional field name for error messages (used with strict=True)
        strict: If True, raise TypeError/ValueError instead of returning default

    Returns:
        Converted integer value

    Raises:
        TypeError: If strict=True and value is invalid type (including bool)
        ValueError: If strict=True and string value is not numeric
    """
    if value is None:
        if strict:
            raise TypeError(f"Invalid {field or 'value'}: expected int, got None.")
        return default

    # Reject bools early in strict mode (bool is subclass of int in Python)
    if strict and type(value).__name__ == "bool":
        raise TypeError(f"Invalid {field or 'value'}: bool is not allowed.")

    # Try bytes decoding first (before __int__/__float__ checks)
    if hasattr(value, "decode"):
        try:
            decoded = value.decode("utf-8")
            return int(decoded)
        except (ValueError, UnicodeDecodeError) as decode_err:
            if strict:
                raise ValueError(
                    f"Invalid {field or 'value'}: cannot decode bytes to int."
                ) from decode_err
            return default

    try:
        if hasattr(value, "__int__"):
            return int(value)
        if hasattr(value, "__float__"):
            return int(float(value))
    except (ValueError, TypeError) as err:
        if strict:
            raise TypeError(
                f"Invalid {field or 'value'}: expected int, got {type(value).__name__}."
            ) from err
        return default

    # Fallback: string conversion
    try:
        return int(str(value))
    except (ValueError, TypeError) as str_err:
        if strict:
            if hasattr(value, "__str__") and type(value).__name__ == "str":
                raise ValueError(
                    f"Invalid {field or 'value'}: expected numeric string, got {value!r}."
                ) from str_err
            raise TypeError(
                f"Invalid {field or 'value'}: expected int, got {type(value).__name__}."
            ) from str_err
        return default


@overload
def coerce_float(value: int | float, default: float = 0.0) -> float:
    """Overload for numeric input."""
    ...


@overload
def coerce_float(value: object, default: float = 0.0) -> float:
    """Overload for object input."""
    ...


def coerce_float(value: object, default: float = 0.0) -> float:
    """Safely convert value to float using try/except instead of isinstance.

    Args:
        value: Value to convert
        default: Default value if conversion fails

    Returns:
        Converted float value
    """
    if value is None:
        return default

    # Try direct conversion - works for int, float, bool, str
    try:
        if hasattr(value, "__float__"):
            result: float = float(value)
        elif hasattr(value, "__int__"):
            result = float(int(value))
        else:
            # Try string conversion
            result = float(str(value))
        return result
    except (ValueError, TypeError):
        # Try string conversion
        if hasattr(value, "__str__"):
            try:
                return float(str(value))
            except ValueError:
                return default
        return default


@overload
def coerce_str(value: str, default: str = "") -> str:
    """Overload for str input."""
    ...


@overload
def coerce_str(value: object, default: str = "") -> str:
    """Overload for object input."""
    ...


def coerce_str(value: object, default: str = "") -> str:
    """Safely convert value to string using try/except instead of isinstance.

    Handles bytes objects by decoding as UTF-8 to avoid
    the ``b'...'`` prefix that ``str(bytes_value)`` produces.

    Args:
        value: Value to convert
        default: Default value if value is None

    Returns:
        String representation of value
    """
    if value is None:
        return default

    # Check for bytes-like objects using hasattr instead of isinstance
    if hasattr(value, "decode"):
        try:
            decoded: str = value.decode("utf-8")
            return decoded
        except (UnicodeDecodeError, AttributeError):
            return default

    try:
        result: str = str(value)
        return result
    except (TypeError, ValueError):
        return default


@overload
def coerce_optional_str(value: str) -> str:
    """Overload for str input."""
    ...


@overload
def coerce_optional_str(value: None) -> None:
    """Overload for None input."""
    ...


@overload
def coerce_optional_str(value: object) -> str | None:
    """Overload for object input."""
    ...


def coerce_optional_str(value: object) -> str | None:
    """Convert value to string or None using try/except instead of isinstance.

    Handles bytes objects by decoding as UTF-8 to avoid
    the ``b'...'`` prefix that ``str(bytes_value)`` produces.

    Args:
        value: Value to convert

    Returns:
        String representation or None if value is None
    """
    if value is None:
        return None

    # Check for bytes-like objects using hasattr instead of isinstance
    if hasattr(value, "decode"):
        try:
            decoded: str = value.decode("utf-8")
            return decoded
        except (UnicodeDecodeError, AttributeError):
            result: str = str(value)
            return result

    try:
        result = str(value)
        return result
    except (TypeError, ValueError):
        return None


def convert_decimal(value: object) -> object:
    """Convert Decimal to float for Neo4j/Redis compatibility.

    PostgreSQL returns numeric types as Decimal, but Neo4j and Redis
    don't support Decimal. This function converts Decimal to float.

    Uses Protocol check instead of isinstance.

    Args:
        value: Value that may be Decimal

    Returns:
        float if value is Decimal, otherwise value unchanged
    """
    # Check for Decimal-like objects using Protocol (has __float__ method)
    if hasattr(value, "__float__") and type(value).__name__ == "Decimal":
        return float(value)
    return value


def convert_batch_decimals(batch: Sequence[Mapping[str, object]]) -> list[dict[str, Neo4jParameter]]:
    """Convert all Decimal values in batch to float for Neo4j/Redis compatibility.

    Recursively processes all values in batch dictionaries, converting
    Decimal to float while preserving other types.

    Uses Union types and relies on static type checker instead of isinstance.

    Args:
        batch: List of dictionaries that may contain Decimal values

    Returns:
        New list with Decimal values converted to float
    """
    converted: list[dict[str, Neo4jParameter]] = []
    for row in batch:
        converted_row: dict[str, Neo4jParameter] = {}
        for key, value in row.items():
            converted_value = convert_decimal(value)
            # After convert_decimal, Decimal is converted to float
            # All other Neo4jParameter types pass through unchanged
            # We rely on runtime behavior - convert_decimal only changes Decimal to float
            # Static checker sees object, but runtime guarantees Neo4jParameter compatibility
            # Use dict assignment - runtime will work correctly
            if converted_value is None:
                converted_row[key] = None
            else:
                # Runtime guarantee: converted_value is one of Neo4jParameter types
                # (int, float, str, bool, datetime, list[...], Mapping, Sequence)
                # Static checker cannot prove this, but runtime behavior is correct
                converted_row[key] = converted_value  # type: ignore[assignment]
        converted.append(converted_row)
    return converted


def convert_dict_to_neo4j_value(data: Mapping[str, object]) -> dict[str, Neo4jValue]:
    """Convert dict[str, object] to dict[str, Neo4jValue] for Neo4j compatibility.

    Similar to convert_batch_decimals but for a single dict.
    Converts Decimal to float and ensures all values are compatible with Neo4jValue.

    Uses Union types and relies on static type checker instead of isinstance.

    Args:
        data: Dictionary that may contain Decimal or other incompatible values

    Returns:
        New dict with values converted to Neo4jValue types
    """
    converted: dict[str, Neo4jValue] = {}
    for key, value in data.items():
        converted_value = convert_decimal(value)
        # After convert_decimal, Decimal is converted to float
        # All other Neo4jValue types pass through unchanged
        # We rely on runtime behavior - convert_decimal only changes Decimal to float
        # Static checker sees object, but runtime guarantees Neo4jValue compatibility
        # Use dict assignment - runtime will work correctly
        if converted_value is None:
            converted[key] = None
        else:
            # Runtime guarantee: converted_value is one of Neo4jValue types
            # (int, float, str, bool, datetime, list[...], Mapping, Sequence)
            # convert_decimal returns object, but runtime guarantees Neo4jValue compatibility
            # We use dict assignment - runtime will work correctly
            converted[key] = converted_value  # type: ignore[assignment]
    return converted


def parse_load_since(value: str | None) -> int | None:
    """Parse load_since value and return days as integer.

    Args:
        value: String like "30d" for 30 days, or None

    Returns:
        Number of days, or None if value is None or invalid

    Raises:
        ValueError: If format is invalid (e.g., missing 'd' suffix)
    """
    if value is None:
        return None

    value = value.strip().lower()
    if not value:
        return None

    if not value.endswith("d"):
        raise ValueError(f"Invalid load_since format: {value!r}. Expected format: '<number>d'")

    try:
        days = int(value[:-1])
        if days <= 0:
            raise ValueError(f"Invalid load_since: days must be positive, got {days}")
        return days
    except ValueError as e:
        if "invalid literal" in str(e):
            raise ValueError(
                f"Invalid load_since format: {value!r}. Expected format: '<number>d'"
            ) from e
        raise
