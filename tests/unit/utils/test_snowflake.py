"""Unit tests for Mastodon Snowflake ID utilities.

Tests verify that Snowflake ID conversion matches Mastodon's implementation
as documented in mastodon/lib/mastodon/snowflake.rb.

Snowflake ID structure:
- Bits 16-63: Unix timestamp in milliseconds (48 bits)
- Bits 0-15: Sequence/random data (16 bits)
"""

from datetime import datetime, timezone, UTC

from hintgrid.utils.snowflake import snowflake_id_at, snowflake_id_to_datetime


class TestSnowflakeIdAt:
    """Tests for snowflake_id_at conversion."""

    def test_basic_conversion(self) -> None:
        """Test basic datetime to Snowflake ID conversion."""
        dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        snowflake_id = snowflake_id_at(dt)

        # ID should be positive
        assert snowflake_id > 0

        # ID structure: (timestamp_ms << 16)
        expected_ms = int(dt.timestamp() * 1000)
        expected_id = expected_ms << 16
        assert snowflake_id == expected_id

    def test_ordering_preserved(self) -> None:
        """Test that later datetimes produce larger Snowflake IDs."""
        dt1 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        dt2 = datetime(2024, 1, 2, 0, 0, 0, tzinfo=UTC)
        dt3 = datetime(2024, 6, 15, 12, 30, 45, tzinfo=UTC)

        id1 = snowflake_id_at(dt1)
        id2 = snowflake_id_at(dt2)
        id3 = snowflake_id_at(dt3)

        assert id1 < id2 < id3

    def test_minimum_id_property(self) -> None:
        """Test that snowflake_id_at returns minimum possible ID for timestamp.

        Since sequence bits are set to 0, the returned ID is the minimum
        possible ID that could exist at that timestamp.
        """
        dt = datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC)
        min_id = snowflake_id_at(dt)

        # The maximum possible ID at same timestamp has sequence = 0xFFFF
        max_id = min_id | 0xFFFF

        assert max_id > min_id
        assert max_id - min_id == 0xFFFF  # 65535

    def test_naive_datetime_treated_as_local(self) -> None:
        """Test that naive datetime is treated as local time."""
        # Create naive datetime (no timezone)
        naive_dt = datetime(2024, 1, 15, 12, 0, 0)
        naive_id = snowflake_id_at(naive_dt)

        # Should produce a valid ID
        assert naive_id > 0

    def test_different_timezones(self) -> None:
        """Test that same instant in different timezones produces same ID."""
        from datetime import timedelta

        # Create UTC datetime
        utc_dt = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)

        # Create equivalent datetime in UTC+3
        tz_plus3 = timezone(timedelta(hours=3))
        local_dt = datetime(2024, 6, 15, 15, 0, 0, tzinfo=tz_plus3)

        # Both represent the same instant, so IDs should be equal
        utc_id = snowflake_id_at(utc_dt)
        local_id = snowflake_id_at(local_dt)

        assert utc_id == local_id


class TestSnowflakeIdToDatetime:
    """Tests for snowflake_id_to_datetime conversion."""

    def test_basic_conversion(self) -> None:
        """Test basic Snowflake ID to datetime conversion."""
        # Known ID (constructed manually)
        timestamp_ms = 1705320000000  # 2024-01-15 12:00:00 UTC
        snowflake_id = timestamp_ms << 16

        dt = snowflake_id_to_datetime(snowflake_id)

        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 15
        assert dt.hour == 12
        assert dt.minute == 0
        assert dt.second == 0
        assert dt.tzinfo == UTC

    def test_roundtrip(self) -> None:
        """Test that conversion is reversible (within millisecond precision)."""
        original = datetime(2024, 6, 15, 14, 30, 45, tzinfo=UTC)
        snowflake_id = snowflake_id_at(original)
        recovered = snowflake_id_to_datetime(snowflake_id)

        # Should match within 1 millisecond (microseconds are lost)
        delta = abs((recovered - original).total_seconds())
        assert delta < 0.001

    def test_sequence_bits_ignored(self) -> None:
        """Test that sequence bits don't affect datetime extraction."""
        timestamp_ms = 1705320000000
        base_id = timestamp_ms << 16

        # IDs with different sequence bits
        id_min = base_id | 0x0000
        id_mid = base_id | 0x7FFF
        id_max = base_id | 0xFFFF

        dt_min = snowflake_id_to_datetime(id_min)
        dt_mid = snowflake_id_to_datetime(id_mid)
        dt_max = snowflake_id_to_datetime(id_max)

        # All should produce same datetime
        assert dt_min == dt_mid == dt_max


class TestSnowflakeIntegration:
    """Integration tests matching Mastodon's snowflake.rb behavior."""

    def test_matches_mastodon_id_at(self) -> None:
        """Test that our implementation matches Mastodon's id_at method.

        From snowflake.rb:
            def id_at(timestamp, with_random: true)
              id  = timestamp.to_i * 1000
              id += rand(1000) if with_random
              id <<= 16
              id += rand(2**16) if with_random
              id
            end
        """
        # Test with a known timestamp
        dt = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        our_id = snowflake_id_at(dt)

        # Mastodon's id_at without random:
        # id = timestamp.to_i * 1000
        # id <<= 16
        unix_ts = int(dt.timestamp())
        expected_id = (unix_ts * 1000) << 16

        assert our_id == expected_id

    def test_matches_mastodon_to_time(self) -> None:
        """Test that our implementation matches Mastodon's to_time method.

        From snowflake.rb:
            def to_time(id)
              Time.at((id >> 16) / 1000).utc
            end
        """
        # Create a known ID
        unix_ts = 1704067200  # 2024-01-01 00:00:00 UTC
        snowflake_id = (unix_ts * 1000) << 16

        dt = snowflake_id_to_datetime(snowflake_id)

        assert dt == datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)

    def test_real_world_id_range(self) -> None:
        """Test with realistic Mastodon ID values."""
        # A post from around 2024 would have an ID like this
        dt_2024 = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        id_2024 = snowflake_id_at(dt_2024)

        # ID should be in realistic range (64-bit positive integer)
        assert id_2024 > 0
        assert id_2024 < 2**63  # Within signed 64-bit range

        # Should be roughly 18-19 digits
        assert len(str(id_2024)) >= 17
        assert len(str(id_2024)) <= 20

    def test_since_date_use_case(self) -> None:
        """Test typical use case: finding posts since a date."""
        # User wants posts from last 30 days
        from datetime import timedelta

        now = datetime.now(UTC)
        since = now - timedelta(days=30)

        min_id = snowflake_id_at(since)

        # Query: WHERE id > min_id should return posts from last 30 days
        # Verify the ID is reasonable
        assert min_id > 0

        # Verify recovered datetime is close to since
        recovered = snowflake_id_to_datetime(min_id)
        delta = abs((recovered - since).total_seconds())
        assert delta < 0.001
