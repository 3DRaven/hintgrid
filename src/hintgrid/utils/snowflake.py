"""Mastodon Snowflake ID utilities.

Mastodon uses Snowflake IDs for statuses, accounts, and other entities.
These IDs encode a timestamp, allowing efficient range queries by time
using only the primary key index.

Snowflake ID structure (64 bits):
- Bits 16-63: Unix timestamp in milliseconds (48 bits)
- Bits 0-15: Sequence/random data (16 bits)

Reference: mastodon/lib/mastodon/snowflake.rb
"""

from __future__ import annotations

from datetime import datetime, UTC


def snowflake_id_at(dt: datetime) -> int:
    """Convert datetime to minimum Mastodon Snowflake ID for that timestamp.

    This returns the MINIMUM possible Snowflake ID that could exist at
    the given timestamp (sequence bits = 0). Useful for range queries
    like "all records since date X".

    Args:
        dt: Datetime to convert. Should be timezone-aware (UTC recommended).
            If naive, treated as local time.

    Returns:
        Minimum Snowflake ID that could exist at that timestamp.

    Example:
        >>> from datetime import datetime, timezone
        >>> dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        >>> snowflake_id_at(dt)
        113834880798720000
    """
    timestamp_ms = int(dt.timestamp() * 1000)
    return timestamp_ms << 16


def snowflake_id_to_datetime(snowflake_id: int) -> datetime:
    """Convert Mastodon Snowflake ID to UTC datetime.

    Inverse of snowflake_id_at(). Extracts the timestamp portion
    of the Snowflake ID.

    Args:
        snowflake_id: Mastodon Snowflake ID (64-bit integer).

    Returns:
        UTC datetime when this ID was approximately created.
        Note: millisecond precision only.

    Example:
        >>> snowflake_id_to_datetime(113834880798720000)
        datetime.datetime(2024, 1, 15, 12, 0, tzinfo=datetime.timezone.utc)
    """
    timestamp_ms = snowflake_id >> 16
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
