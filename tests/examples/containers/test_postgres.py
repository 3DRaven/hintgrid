"""PostgreSQL integration tests."""

from typing import cast

import pytest
from psycopg import Connection
from psycopg.rows import TupleRow
from psycopg_pool import ConnectionPool

from .conftest import (
    EXPECTED_STATUSES_COUNT,
    FAVOURITES_COUNT,
    PAGINATION_BATCH_SIZE,
    POOL_CONNECTIONS_COUNT,
    TOTAL_PAGINATION_BATCHES,
)


@pytest.mark.smoke
@pytest.mark.integration
def test_postgres_connectivity(postgres_conn: Connection[TupleRow]) -> None:
    """Test PostgreSQL connection."""
    with postgres_conn.cursor() as cur:
        cur.execute("SELECT version();")
        row = cur.fetchone()
        assert row is not None
        version = cast("tuple[str]", row)
        assert "PostgreSQL" in version[0]
        print(f"✅ PostgreSQL: {version[0][:50]}...")

    print("✅ PostgreSQL: connection works")


@pytest.mark.integration
def test_postgres_mastodon_schema(postgres_conn: Connection[TupleRow]) -> None:
    """Test creating simplified Mastodon schema."""
    with postgres_conn.cursor() as cur:
        # Create tables
        cur.execute("""
            CREATE TABLE IF NOT EXISTS statuses (
                id BIGSERIAL PRIMARY KEY,
                account_id BIGINT NOT NULL,
                text TEXT,
                language VARCHAR(10),
                created_at TIMESTAMP DEFAULT NOW(),
                deleted_at TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS favourites (
                id BIGSERIAL PRIMARY KEY,
                account_id BIGINT NOT NULL,
                status_id BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE TABLE IF NOT EXISTS follows (
                id BIGSERIAL PRIMARY KEY,
                account_id BIGINT NOT NULL,
                target_account_id BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_statuses_account ON statuses(account_id);
            CREATE INDEX IF NOT EXISTS idx_favourites_status ON favourites(status_id);
        """)

        # Insert test data
        cur.execute("""
            INSERT INTO statuses (account_id, text, language)
            VALUES (1, 'Test post #1', 'en'),
                   (2, 'Test post #2', 'ru'),
                   (1, 'Test post #3', 'en')
            RETURNING id;
        """)
        status_ids = [row[0] for row in cur.fetchall()]
        assert len(status_ids) == EXPECTED_STATUSES_COUNT

        # Verify data
        cur.execute("SELECT count(*) FROM statuses WHERE deleted_at IS NULL;")
        row = cur.fetchone()
        assert row is not None
        count = cast("tuple[int]", row)[0]
        assert count == EXPECTED_STATUSES_COUNT

        # Favourites
        cur.execute(
            """
            INSERT INTO favourites (account_id, status_id)
            VALUES (2, %s), (3, %s)
        """,
            (status_ids[0], status_ids[0]),
        )

        cur.execute("SELECT count(*) FROM favourites;")
        row = cur.fetchone()
        assert row is not None
        fav_count = cast("tuple[int]", row)[0]
        assert fav_count == FAVOURITES_COUNT

        postgres_conn.commit()

    print("✅ PostgreSQL: Mastodon schema created and works")


@pytest.mark.integration
def test_postgres_basic_pagination(postgres_conn: Connection[TupleRow]) -> None:
    """Test basic PostgreSQL pagination (LIMIT/OFFSET pattern)."""
    with postgres_conn.cursor() as cur:
        # Create table
        cur.execute("""
            CREATE TABLE test_statuses (
                id BIGSERIAL PRIMARY KEY,
                content TEXT
            );
        """)

        # Insert data
        for i in range(1, 21):
            cur.execute("INSERT INTO test_statuses (content) VALUES (%s)", (f"Post {i}",))

        postgres_conn.commit()

        # Incremental loading (as in HintGrid)
        last_id = 0
        batch_size = PAGINATION_BATCH_SIZE
        batches: list[list[tuple[int, str]]] = []

        while True:
            cur.execute(
                """
                SELECT id, content 
                FROM test_statuses 
                WHERE id > %s 
                ORDER BY id ASC 
                LIMIT %s
            """,
                (last_id, batch_size),
            )

            batch = cast("list[tuple[int, str]]", cur.fetchall())
            if not batch:
                break

            batches.append(batch)
            last_id = batch[-1][0]

        # Verify: should be 4 batches of 5 elements
        assert len(batches) == TOTAL_PAGINATION_BATCHES
        assert all(len(b) == PAGINATION_BATCH_SIZE for b in batches)
        assert batches[0][0][1] == "Post 1"
        assert batches[-1][-1][1] == "Post 20"

    print("✅ PostgreSQL: incremental loading works")


@pytest.mark.integration
def test_postgres_connection_pool_usage(postgres_pool: ConnectionPool[Connection]) -> None:
    """Test PostgreSQL connection pool functionality."""
    # Check pool status
    pool_info = postgres_pool.get_stats()
    print(f"Pool stats: {pool_info}")

    # Use multiple connections simultaneously
    connections: list[Connection[TupleRow]] = []
    try:
        # Get 3 connections from pool
        for _ in range(POOL_CONNECTIONS_COUNT):
            conn = postgres_pool.getconn()
            connections.append(conn)

            # Execute query
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS test_value;")
                result = cast("tuple[int]", cur.fetchone())
                assert result[0] == 1

            conn.commit()

        # Check that connections are active
        assert len(connections) == POOL_CONNECTIONS_COUNT
        print(f"PostgreSQL: got {POOL_CONNECTIONS_COUNT} connections from pool")

    finally:
        # Return all connections to pool
        for conn in connections:
            postgres_pool.putconn(conn)

        print("PostgreSQL: all connections returned to pool")

    # Check final pool status
    final_stats = postgres_pool.get_stats()
    print(f"Final pool stats: {final_stats}")
