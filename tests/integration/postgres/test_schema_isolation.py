"""Integration tests for PostgreSQL schema isolation in CorpusStreamer.

Tests verify that search_path is correctly set when schema is specified,
ensuring worker isolation in parallel test execution.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from psycopg import sql

from hintgrid.clients.postgres import PostgresCorpus

if TYPE_CHECKING:
    from psycopg.rows import TupleRow
    from psycopg import Connection

    from tests.conftest import DockerComposeInfo


@pytest.mark.integration
def test_corpus_streamer_with_schema(
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    worker_schema: str,
    mastodon_schema: None,
) -> None:
    """Test that CorpusStreamer sets search_path when schema is specified.

    Verifies that __iter__ method sets search_path to worker schema
    for proper isolation in parallel test execution.
    """
    # Insert test data in worker schema
    with postgres_conn.cursor() as cur:
        # Ensure we're in the worker schema
        if worker_schema != "public":
            cur.execute(
                sql.SQL("SET search_path TO {}, public").format(
                    sql.Identifier(worker_schema)
                )
            )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility)
            VALUES
                (1001, 201, 'Test post in worker schema for isolation testing', 'en', 0),
                (1002, 202, 'Another test post with sufficient length for corpus', 'en', 0)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Create DSN
    dsn = (
        f"postgresql://{docker_compose.postgres_user}:{docker_compose.postgres_password}"
        f"@{docker_compose.postgres_host}:{docker_compose.postgres_port}/{docker_compose.postgres_db}"
    )

    # Create corpus with schema specified
    corpus = PostgresCorpus(
        dsn=dsn,
        min_id=0,
        schema=worker_schema,
        batch_size=100,
    )

    # Iterate and collect documents
    documents = list(corpus)

    # Should find documents from worker schema
    assert len(documents) >= 2, f"Expected at least 2 documents, got {len(documents)}"
    assert any(
        "isolation testing" in " ".join(doc) for doc in documents
    ), "Should find document from worker schema"


@pytest.mark.integration
def test_corpus_total_count_with_schema(
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    worker_schema: str,
    mastodon_schema: None,
) -> None:
    """Test that CorpusStreamer.total_count sets search_path when schema is specified.

    Verifies that total_count method sets search_path to worker schema
    for proper isolation in parallel test execution.
    """
    # Insert test data in worker schema
    with postgres_conn.cursor() as cur:
        # Ensure we're in the worker schema
        if worker_schema != "public":
            cur.execute(
                sql.SQL("SET search_path TO {}, public").format(
                    sql.Identifier(worker_schema)
                )
            )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility)
            VALUES
                (2001, 301, 'First document for count test with enough text', 'en', 0),
                (2002, 302, 'Second document for count test with enough text', 'en', 0),
                (2003, 303, 'Third document for count test with enough text', 'en', 0)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    # Create DSN
    dsn = (
        f"postgresql://{docker_compose.postgres_user}:{docker_compose.postgres_password}"
        f"@{docker_compose.postgres_host}:{docker_compose.postgres_port}/{docker_compose.postgres_db}"
    )

    # Create corpus with schema specified
    corpus = PostgresCorpus(
        dsn=dsn,
        min_id=0,
        schema=worker_schema,
        batch_size=100,
    )

    # Get total count
    count = corpus.total_count()

    # Should count documents from worker schema
    assert count >= 3, f"Expected at least 3 documents, got {count}"
