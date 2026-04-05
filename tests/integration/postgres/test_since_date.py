"""Integration tests for PostgreSQL client methods with since_date parameter.

Tests verify that since_date is correctly converted to Snowflake ID
for efficient index-based filtering.
"""

from __future__ import annotations

from datetime import datetime, timedelta, UTC
from typing import TYPE_CHECKING

import pytest

from hintgrid.utils.coercion import coerce_int
from hintgrid.utils.snowflake import snowflake_id_at

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow

    from hintgrid.clients.postgres import PostgresClient


@pytest.mark.integration
def test_fetch_statuses_with_since_date(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test fetch_statuses with since_date parameter converts to Snowflake ID."""
    # Create test data with different dates
    now = datetime.now(UTC)
    old_date = now - timedelta(days=60)  # 60 days ago
    recent_date = now - timedelta(days=10)  # 10 days ago

    old_snowflake = snowflake_id_at(old_date)
    recent_snowflake = snowflake_id_at(recent_date)

    with postgres_conn.cursor() as cur:
        # Create accounts
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (301, 'user_old', NULL), (302, 'user_recent', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )

        # Create old statuses (should be excluded)
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at)
            VALUES
                (%s, 301, 'Old post that should be excluded', 'en', 0, %s),
                (%s, 301, 'Another old post', 'en', 0, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (old_snowflake, old_date, old_snowflake + 1, old_date),
        )

        # Create recent statuses (should be included)
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at)
            VALUES
                (%s, 302, 'Recent post that should be included', 'en', 0, %s),
                (%s, 302, 'Another recent post', 'en', 0, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (recent_snowflake, recent_date, recent_snowflake + 1, recent_date),
        )
        postgres_conn.commit()

    # Fetch with since_date (30 days ago should include recent posts)
    since_date = now - timedelta(days=30)
    results = list(postgres_client.stream_statuses(last_id=0, since_date=since_date))

    # Should only get recent posts
    result_ids = [coerce_int(r.get("id")) for r in results]
    assert recent_snowflake in result_ids, "Should include recent posts"
    assert old_snowflake not in result_ids, "Should exclude old posts"
    assert len(results) >= 2, f"Expected at least 2 recent posts, got {len(results)}"


@pytest.mark.integration
def test_stream_statuses_with_since_date(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test stream_statuses with since_date parameter converts to Snowflake ID."""
    # Create test data with different dates
    now = datetime.now(UTC)
    old_date = now - timedelta(days=60)
    recent_date = now - timedelta(days=10)

    old_snowflake = snowflake_id_at(old_date)
    recent_snowflake = snowflake_id_at(recent_date)

    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (303, 'user_stream_old', NULL), (304, 'user_stream_recent', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at)
            VALUES
                (%s, 303, 'Old post for streaming test', 'en', 0, %s),
                (%s, 304, 'Recent post for streaming test', 'en', 0, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (old_snowflake, old_date, recent_snowflake, recent_date),
        )
        postgres_conn.commit()

    # Stream with since_date
    since_date = now - timedelta(days=30)
    results = list(
        postgres_client.stream_statuses(last_id=0, since_date=since_date)
    )

    # Should only get recent posts
    result_ids = [coerce_int(r.get("id")) for r in results]
    assert recent_snowflake in result_ids, "Should include recent posts"
    assert old_snowflake not in result_ids, "Should exclude old posts"


@pytest.mark.integration
def test_fetch_favourites_with_since_date(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test stream_favourites with since_date uses created_at filter, not Snowflake ID.

    Favourites use standard auto-increment IDs (not Snowflake),
    so since_date must filter by created_at column directly.
    """
    now = datetime.now(UTC)
    old_date = now - timedelta(days=60)
    recent_date = now - timedelta(days=10)

    # Use small sequential IDs (like real auto-increment favourites)
    old_fav_id = 1001
    recent_fav_id = 1002

    old_snowflake = snowflake_id_at(old_date)
    recent_snowflake = snowflake_id_at(recent_date)

    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (305, 'user_fav', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at)
            VALUES
                (%s, 305, 'Status for old favourite', 'en', 0, %s),
                (%s, 305, 'Status for recent favourite', 'en', 0, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (old_snowflake, old_date, recent_snowflake, recent_date),
        )

        # Create favourites with small auto-increment IDs
        cur.execute(
            """
            INSERT INTO favourites (id, account_id, status_id, created_at)
            VALUES
                (%s, 305, %s, %s),
                (%s, 305, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (
                old_fav_id,
                old_snowflake,
                old_date,
                recent_fav_id,
                recent_snowflake,
                recent_date,
            ),
        )
        postgres_conn.commit()

    # Fetch with since_date (30 days ago should include only recent favourites)
    since_date = now - timedelta(days=30)
    results = list(postgres_client.stream_favourites(
        last_id=0, since_date=since_date
    ))

    # Should only get recent favourites
    result_ids = [coerce_int(r.get("id")) for r in results]
    assert recent_fav_id in result_ids, "Should include recent favourites"
    assert old_fav_id not in result_ids, "Should exclude old favourites"


@pytest.mark.integration
def test_stream_favourites_with_since_date(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test stream_favourites with since_date uses created_at filter, not Snowflake ID.

    Verifies that even with small auto-increment IDs, since_date correctly
    filters by created_at column instead of trying Snowflake conversion.
    """
    now = datetime.now(UTC)
    old_date = now - timedelta(days=60)
    recent_date = now - timedelta(days=10)

    # Use small sequential IDs (like real auto-increment favourites)
    old_fav_id = 2001
    recent_fav_id = 2002

    old_snowflake = snowflake_id_at(old_date)
    recent_snowflake = snowflake_id_at(recent_date)

    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (306, 'user_stream_fav', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at)
            VALUES
                (%s, 306, 'Status for stream favourite test', 'en', 0, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (recent_snowflake, recent_date),
        )
        cur.execute(
            """
            INSERT INTO favourites (id, account_id, status_id, created_at)
            VALUES
                (%s, 306, %s, %s),
                (%s, 306, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (
                old_fav_id,
                old_snowflake,
                old_date,
                recent_fav_id,
                recent_snowflake,
                recent_date,
            ),
        )
        postgres_conn.commit()

    # Stream with since_date
    since_date = now - timedelta(days=30)
    results = list(
        postgres_client.stream_favourites(
            last_id=0, since_date=since_date
        )
    )

    # Should only get recent favourites
    result_ids = [coerce_int(r.get("id")) for r in results]
    assert recent_fav_id in result_ids, "Should include recent favourites"
    assert old_fav_id not in result_ids, "Should exclude old favourites"


@pytest.mark.integration
def test_fetch_replies_with_since_date(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test fetch_replies with since_date parameter converts to Snowflake ID."""
    # Create test data
    now = datetime.now(UTC)
    old_date = now - timedelta(days=60)
    recent_date = now - timedelta(days=10)

    old_snowflake = snowflake_id_at(old_date)
    recent_snowflake = snowflake_id_at(recent_date)

    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (307, 'user_reply_author', NULL), (308, 'user_reply_target', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        # Create original posts
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at, reply)
            VALUES
                (%s, 308, 'Original post for old reply', 'en', 0, %s, false),
                (%s, 308, 'Original post for recent reply', 'en', 0, %s, false)
            ON CONFLICT (id) DO NOTHING;
            """,
            (old_snowflake - 100, old_date, recent_snowflake - 100, recent_date),
        )
        # Create replies
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at, 
                                  in_reply_to_id, in_reply_to_account_id, reply)
            VALUES
                (%s, 307, 'Old reply that should be excluded', 'en', 0, %s, %s, 308, true),
                (%s, 307, 'Recent reply that should be included', 'en', 0, %s, %s, 308, true)
            ON CONFLICT (id) DO NOTHING;
            """,
            (
                old_snowflake,
                old_date,
                old_snowflake - 100,
                recent_snowflake,
                recent_date,
                recent_snowflake - 100,
            ),
        )
        postgres_conn.commit()

    # Fetch with since_date (using unified stream_statuses)
    since_date = now - timedelta(days=30)
    all_results = list(postgres_client.stream_statuses(
        last_id=0, since_date=since_date
    ))
    # Filter for replies only (in_reply_to_id is set)
    results = [r for r in all_results if r.get("in_reply_to_id") is not None]

    # Should only get recent replies
    result_ids = [coerce_int(r.get("id")) for r in results]
    assert recent_snowflake in result_ids, "Should include recent replies"
    assert old_snowflake not in result_ids, "Should exclude old replies"


@pytest.mark.integration
def test_stream_replies_with_since_date(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test stream_statuses with since_date returns replies with correct date filtering."""
    # Create test data
    now = datetime.now(UTC)
    old_date = now - timedelta(days=60)
    recent_date = now - timedelta(days=10)

    old_snowflake = snowflake_id_at(old_date)
    recent_snowflake = snowflake_id_at(recent_date)

    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (309, 'user_stream_reply_author', NULL), (310, 'user_stream_reply_target', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at, reply)
            VALUES
                (%s, 310, 'Original for stream reply test', 'en', 0, %s, false)
            ON CONFLICT (id) DO NOTHING;
            """,
            (recent_snowflake - 200, recent_date),
        )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at,
                                  in_reply_to_id, in_reply_to_account_id, reply)
            VALUES
                (%s, 309, 'Old stream reply', 'en', 0, %s, %s, 310, true),
                (%s, 309, 'Recent stream reply', 'en', 0, %s, %s, 310, true)
            ON CONFLICT (id) DO NOTHING;
            """,
            (
                old_snowflake,
                old_date,
                recent_snowflake - 200,
                recent_snowflake,
                recent_date,
                recent_snowflake - 200,
            ),
        )
        postgres_conn.commit()

    # Stream with since_date (using unified stream_statuses)
    since_date = now - timedelta(days=30)
    all_results = list(
        postgres_client.stream_statuses(last_id=0, since_date=since_date)
    )
    # Filter for replies only (in_reply_to_id is set)
    results = [r for r in all_results if r.get("in_reply_to_id") is not None]

    # Should only get recent replies
    result_ids = [coerce_int(r.get("id")) for r in results]
    assert recent_snowflake in result_ids, "Should include recent replies"
    assert old_snowflake not in result_ids, "Should exclude old replies"
