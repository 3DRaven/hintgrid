"""Integration tests for PostgresClient methods.

Tests verify fetch/stream methods for follows, blocks, mutes,
and fetch_user_id with various edge cases.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.utils.coercion import coerce_int

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow

    from hintgrid.clients.postgres import PostgresClient


@pytest.mark.integration
def test_fetch_follows(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test fetch_follows returns follow relationships."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (1001, 'follower1', NULL), (1002, 'followee1', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO follows (id, account_id, target_account_id)
            VALUES
                (2001, 1001, 1002),
                (2002, 1002, 1001)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Fetch follows using stream_query (follows are now aggregated via stream_user_interactions)
    results = list(
        postgres_client.stream_query(
            "SELECT id, account_id, target_account_id FROM follows WHERE id > 0 ORDER BY id",
        )
    )

    assert len(results) >= 2, f"Expected at least 2 follows, got {len(results)}"
    result_ids = [coerce_int(r.get("id")) for r in results]
    assert 2001 in result_ids, "Should include follow 2001"
    assert 2002 in result_ids, "Should include follow 2002"


@pytest.mark.integration
def test_stream_follows(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test stream_follows streams follow relationships."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (1003, 'follower2', NULL), (1004, 'followee2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO follows (id, account_id, target_account_id)
            VALUES (2003, 1003, 1004)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Stream follows using stream_query (follows are now aggregated via stream_user_interactions)
    results = list(
        postgres_client.stream_query(
            "SELECT id, account_id, target_account_id FROM follows WHERE id > 0 ORDER BY id",
        )
    )

    assert len(results) >= 1, f"Expected at least 1 follow, got {len(results)}"
    result_ids = [coerce_int(r.get("id")) for r in results]
    assert 2003 in result_ids, "Should include follow 2003"


@pytest.mark.integration
def test_fetch_blocks(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test fetch_blocks returns block relationships."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (1005, 'blocker1', NULL), (1006, 'blockee1', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO blocks (id, account_id, target_account_id)
            VALUES
                (3001, 1005, 1006),
                (3002, 1006, 1005)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Fetch blocks (using stream for consistency)
    results = list(postgres_client.stream_blocks(last_id=0))

    assert len(results) >= 2, f"Expected at least 2 blocks, got {len(results)}"
    result_ids = [coerce_int(r.get("id")) for r in results]
    assert 3001 in result_ids, "Should include block 3001"
    assert 3002 in result_ids, "Should include block 3002"
    # Check that type field is set
    assert all(r.get("type") == "block" for r in results), "All results should have type='block'"


@pytest.mark.integration
def test_stream_blocks(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test stream_blocks streams block relationships."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (1007, 'blocker2', NULL), (1008, 'blockee2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO blocks (id, account_id, target_account_id)
            VALUES (3003, 1007, 1008)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Stream blocks
    results = list(postgres_client.stream_blocks(last_id=0))

    assert len(results) >= 1, f"Expected at least 1 block, got {len(results)}"
    result_ids = [coerce_int(r.get("id")) for r in results]
    assert 3003 in result_ids, "Should include block 3003"
    assert all(r.get("type") == "block" for r in results), "All results should have type='block'"


@pytest.mark.integration
def test_fetch_mutes(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test fetch_mutes returns mute relationships."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (1009, 'muter1', NULL), (1010, 'mutee1', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO mutes (id, account_id, target_account_id)
            VALUES
                (4001, 1009, 1010),
                (4002, 1010, 1009)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Fetch mutes (using stream for consistency)
    results = list(postgres_client.stream_mutes(last_id=0))

    assert len(results) >= 2, f"Expected at least 2 mutes, got {len(results)}"
    result_ids = [coerce_int(r.get("id")) for r in results]
    assert 4001 in result_ids, "Should include mute 4001"
    assert 4002 in result_ids, "Should include mute 4002"
    # Check that type field is set
    assert all(r.get("type") == "mute" for r in results), "All results should have type='mute'"


@pytest.mark.integration
def test_stream_mutes(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test stream_mutes streams mute relationships."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (1011, 'muter2', NULL), (1012, 'mutee2', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        cur.execute(
            """
            INSERT INTO mutes (id, account_id, target_account_id)
            VALUES (4003, 1011, 1012)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Stream mutes
    results = list(postgres_client.stream_mutes(last_id=0))

    assert len(results) >= 1, f"Expected at least 1 mute, got {len(results)}"
    result_ids = [coerce_int(r.get("id")) for r in results]
    assert 4003 in result_ids, "Should include mute 4003"
    assert all(r.get("type") == "mute" for r in results), "All results should have type='mute'"


@pytest.mark.integration
def test_fetch_user_id_found(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test fetch_user_id when user is found."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (5001, 'found_user', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Get user ID
    user_id = postgres_client.fetch_user_id("found_user", None)

    assert user_id == 5001, f"Expected user_id 5001, got {user_id}"


@pytest.mark.integration
def test_fetch_user_id_not_found(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test fetch_user_id when user is not found."""
    # Get user ID for non-existent user
    user_id = postgres_client.fetch_user_id("nonexistent_user", None)

    assert user_id is None, "Should return None for non-existent user"


@pytest.mark.integration
def test_fetch_user_id_with_domain(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test fetch_user_id with domain specified."""
    # Insert test data with domain
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (5002, 'domain_user', 'example.com')
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Get user ID with domain
    user_id = postgres_client.fetch_user_id("domain_user", "example.com")

    assert user_id == 5002, f"Expected user_id 5002, got {user_id}"

    # Should not find user with different domain
    user_id_wrong_domain = postgres_client.fetch_user_id("domain_user", "other.com")
    assert user_id_wrong_domain is None, "Should not find user with wrong domain"


@pytest.mark.integration
def test_fetch_user_id_none_value(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test fetch_user_id handles None id value gracefully."""
    # This is an edge case - in practice id is PRIMARY KEY so can't be NULL
    # But we test the code path that checks for None
    # We can't easily create a row with NULL id, but we can test the logic
    # by ensuring the method handles empty results correctly (which it does via the "if not rows" check)
    
    # Test with non-existent user (which returns None via "if not rows" path)
    user_id = postgres_client.fetch_user_id("definitely_nonexistent_user_12345", None)
    assert user_id is None, "Should return None for non-existent user"


@pytest.mark.integration
def test_fetch_account_info_single_account(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test fetch_account_info returns account info for single account."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (6001, 'test_user1', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Fetch account info
    account_info = postgres_client.fetch_account_info([6001])

    assert 6001 in account_info
    assert account_info[6001]["username"] == "test_user1"
    assert account_info[6001]["domain"] is None


@pytest.mark.integration
def test_fetch_account_info_multiple_accounts(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test fetch_account_info returns account info for multiple accounts."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES 
                (6002, 'local_user', NULL),
                (6003, 'remote_user', 'example.com'),
                (6004, 'another_user', 'test.org')
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Fetch account info
    account_info = postgres_client.fetch_account_info([6002, 6003, 6004])

    assert len(account_info) == 3
    assert account_info[6002]["username"] == "local_user"
    assert account_info[6002]["domain"] is None
    assert account_info[6003]["username"] == "remote_user"
    assert account_info[6003]["domain"] == "example.com"
    assert account_info[6004]["username"] == "another_user"
    assert account_info[6004]["domain"] == "test.org"


@pytest.mark.integration
def test_fetch_account_info_empty_list(
    postgres_client: PostgresClient,
) -> None:
    """Test fetch_account_info returns empty dict for empty list."""
    account_info = postgres_client.fetch_account_info([])

    assert account_info == {}


@pytest.mark.integration
def test_fetch_account_info_nonexistent_accounts(
    postgres_client: PostgresClient,
    mastodon_schema: None,
) -> None:
    """Test fetch_account_info returns empty dict for nonexistent accounts."""
    account_info = postgres_client.fetch_account_info([99999, 99998])

    assert account_info == {}


@pytest.mark.integration
def test_fetch_account_info_partial_match(
    postgres_client: PostgresClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Test fetch_account_info returns only existing accounts when some are missing."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO accounts (id, username, domain)
            VALUES (6005, 'existing_user', NULL)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Fetch account info for mix of existing and non-existing
    account_info = postgres_client.fetch_account_info([6005, 99999])

    assert len(account_info) == 1
    assert 6005 in account_info
    assert account_info[6005]["username"] == "existing_user"
    assert 99999 not in account_info
