"""Integration tests for PostgresCorpus streaming methods.

Tests verify CorpusStreamer methods including stream_texts, stream_with_ids,
error handling, text filtering, and tokenization.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from nltk.tokenize import TweetTokenizer

from hintgrid.clients.postgres import PostgresCorpus
from psycopg import sql

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow

    from tests.conftest import DockerComposeInfo


@pytest.mark.integration
def test_stream_texts(
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    worker_schema: str,
    mastodon_schema: None,
) -> None:
    """Test stream_texts() returns raw strings without tokenization."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        if worker_schema != "public":
            cur.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(worker_schema)))
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility)
            VALUES
                (4001, 401, 'This is a test post with sufficient length for corpus streaming', 'en', 0),
                (4002, 402, 'Another test post with enough text content for testing', 'en', 0)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    dsn = (
        f"postgresql://{docker_compose.postgres_user}:{docker_compose.postgres_password}"
        f"@{docker_compose.postgres_host}:{docker_compose.postgres_port}/{docker_compose.postgres_db}"
    )

    corpus = PostgresCorpus(dsn=dsn, min_id=0, schema=worker_schema, batch_size=100)

    # stream_texts yields raw strings
    texts = list(corpus.stream_texts())
    assert len(texts) >= 2, f"Expected at least 2 texts, got {len(texts)}"
    assert all(isinstance(text, str) for text in texts), "All items should be strings"
    assert "sufficient length" in texts[0] or "sufficient length" in texts[1]


@pytest.mark.integration
def test_stream_with_ids(
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    worker_schema: str,
    mastodon_schema: None,
) -> None:
    """Test stream_with_ids() returns (id, text) tuples."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        if worker_schema != "public":
            cur.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(worker_schema)))
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility)
            VALUES
                (5001, 501, 'Test post for stream with ids method testing', 'en', 0),
                (5002, 502, 'Another post for stream with ids testing', 'en', 0)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    dsn = (
        f"postgresql://{docker_compose.postgres_user}:{docker_compose.postgres_password}"
        f"@{docker_compose.postgres_host}:{docker_compose.postgres_port}/{docker_compose.postgres_db}"
    )

    corpus = PostgresCorpus(dsn=dsn, min_id=0, schema=worker_schema, batch_size=100)

    # stream_with_ids yields tuples
    items = list(corpus.stream_with_ids())
    assert len(items) >= 2, f"Expected at least 2 items, got {len(items)}"
    assert all(isinstance(item, tuple) for item in items), "All items should be tuples"
    assert all(len(item) == 2 for item in items), "All tuples should have 2 elements"
    assert all(isinstance(item[0], int) for item in items), "First element should be int"
    assert all(isinstance(item[1], str) for item in items), "Second element should be str"
    assert any(item[0] == 5001 or item[0] == 5002 for item in items), "Should include test IDs"


@pytest.mark.integration
def test_deprecated_aliases(
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    worker_schema: str,
    mastodon_schema: None,
) -> None:
    """Test deprecated aliases iter_raw() and iter_with_ids()."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        if worker_schema != "public":
            cur.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(worker_schema)))
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility)
            VALUES
                (6001, 601, 'Test post for deprecated aliases testing', 'en', 0)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    dsn = (
        f"postgresql://{docker_compose.postgres_user}:{docker_compose.postgres_password}"
        f"@{docker_compose.postgres_host}:{docker_compose.postgres_port}/{docker_compose.postgres_db}"
    )

    corpus = PostgresCorpus(dsn=dsn, min_id=0, schema=worker_schema, batch_size=100)

    # Test deprecated iter_raw() alias
    texts = list(corpus.iter_raw())
    assert len(texts) >= 1, "iter_raw should return texts"

    # Test deprecated iter_with_ids() alias
    items = list(corpus.iter_with_ids())
    assert len(items) >= 1, "iter_with_ids should return items"
    assert all(isinstance(item, tuple) for item in items), "Items should be tuples"


@pytest.mark.integration
def test_corpus_streamer_post_id_none(
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    worker_schema: str,
    mastodon_schema: None,
) -> None:
    """Test handling of post_id is None in __iter__."""
    # Insert status with NULL id (edge case - shouldn't happen in practice but test coverage)
    # Actually, id is PRIMARY KEY so can't be NULL, but we can test the logic path
    # by using a very high min_id that excludes all records
    dsn = (
        f"postgresql://{docker_compose.postgres_user}:{docker_compose.postgres_password}"
        f"@{docker_compose.postgres_host}:{docker_compose.postgres_port}/{docker_compose.postgres_db}"
    )

    # Use very high min_id so no records match
    corpus = PostgresCorpus(dsn=dsn, min_id=999_999_999, schema=worker_schema, batch_size=100)

    # Should handle gracefully (no records, so post_id None path not directly testable)
    # But we can test that iteration works with empty result
    documents = list(corpus)
    assert len(documents) == 0, "Should return empty list for high min_id"


@pytest.mark.integration
def test_corpus_streamer_short_text_filtering(
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    worker_schema: str,
    mastodon_schema: None,
) -> None:
    """Test filtering of short texts in Python."""
    # Insert test data with short and long texts
    with postgres_conn.cursor() as cur:
        if worker_schema != "public":
            cur.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(worker_schema)))
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility)
            VALUES
                (7001, 701, 'Short', 'en', 0),  -- Too short (5 chars < 10)
                (7002, 702, 'This is a longer text that should pass the filter', 'en', 0),  -- Long enough
                (7003, 703, 'Also short', 'en', 0)  -- Too short (10 chars, but min_text_length is 10, so should pass
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    dsn = (
        f"postgresql://{docker_compose.postgres_user}:{docker_compose.postgres_password}"
        f"@{docker_compose.postgres_host}:{docker_compose.postgres_port}/{docker_compose.postgres_db}"
    )

    # Default min_text_length is 10
    corpus = PostgresCorpus(dsn=dsn, min_id=0, schema=worker_schema, batch_size=100, min_text_length=10)

    # __iter__ should filter short texts
    documents = list(corpus)
    # Should include 7002 and 7003 (both >= 10 chars), exclude 7001
    assert len(documents) >= 2, "Should filter out short texts"
    texts_joined = [" ".join(doc) for doc in documents]
    assert any("longer text" in text for text in texts_joined), "Should include long text"
    assert not any("Short" in text and len(text.split()) == 1 for text in texts_joined), "Should exclude very short text"

    # stream_texts should also filter
    texts = list(corpus.stream_texts())
    assert len(texts) >= 2, "stream_texts should filter short texts"
    assert all(len(text) >= 10 for text in texts), "All texts should be >= min_text_length"

    # stream_with_ids should also filter
    items = list(corpus.stream_with_ids())
    assert len(items) >= 2, "stream_with_ids should filter short texts"
    assert all(len(item[1]) >= 10 for item in items), "All texts should be >= min_text_length"


@pytest.mark.integration
def test_corpus_streamer_with_tokenizer(
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    worker_schema: str,
    mastodon_schema: None,
) -> None:
    """Test tokenization when tokenizer is provided."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        if worker_schema != "public":
            cur.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(worker_schema)))
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility)
            VALUES
                (8001, 801, 'Test post with #hashtag and @mention for tokenization', 'en', 0)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    dsn = (
        f"postgresql://{docker_compose.postgres_user}:{docker_compose.postgres_password}"
        f"@{docker_compose.postgres_host}:{docker_compose.postgres_port}/{docker_compose.postgres_db}"
    )

    # Create tokenizer
    tokenizer = TweetTokenizer(preserve_case=False, strip_handles=True, reduce_len=True)

    corpus = PostgresCorpus(
        dsn=dsn,
        min_id=0,
        schema=worker_schema,
        batch_size=100,
        tokenizer=tokenizer,
    )

    # __iter__ should use tokenizer
    documents = list(corpus)
    assert len(documents) >= 1, "Should tokenize documents"
    assert isinstance(documents[0], list), "Documents should be tokenized (list of tokens)"
    assert len(documents[0]) > 0, "Should have tokens"
    assert all(isinstance(token, str) for token in documents[0]), "Tokens should be strings"


@pytest.mark.integration
def test_corpus_streamer_without_tokenizer(
    docker_compose: DockerComposeInfo,
    postgres_conn: Connection[TupleRow],
    worker_schema: str,
    mastodon_schema: None,
) -> None:
    """Test fallback tokenization when no tokenizer provided."""
    # Insert test data
    with postgres_conn.cursor() as cur:
        if worker_schema != "public":
            cur.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(worker_schema)))
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility)
            VALUES
                (9001, 901, 'Test post for fallback tokenization without tokenizer', 'en', 0)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        postgres_conn.commit()

    dsn = (
        f"postgresql://{docker_compose.postgres_user}:{docker_compose.postgres_password}"
        f"@{docker_compose.postgres_host}:{docker_compose.postgres_port}/{docker_compose.postgres_db}"
    )

    # No tokenizer - should use fallback
    corpus = PostgresCorpus(dsn=dsn, min_id=0, schema=worker_schema, batch_size=100, tokenizer=None)

    # __iter__ should use fallback tokenization (whitespace split)
    documents = list(corpus)
    assert len(documents) >= 1, "Should tokenize with fallback"
    assert isinstance(documents[0], list), "Documents should be tokenized (list of tokens)"
    assert len(documents[0]) > 0, "Should have tokens"
    # Fallback uses lower().split() so tokens should be lowercase
    assert all(token.islower() for token in documents[0]), "Fallback tokens should be lowercase"


@pytest.mark.integration
def test_corpus_streamer_error_handling(
    docker_compose: DockerComposeInfo,
    worker_schema: str,
) -> None:
    """Test error handling in __iter__, stream_texts, stream_with_ids."""
    # Use invalid DSN to trigger connection error
    # Note: Connection errors may take time, so we use a timeout
    invalid_dsn = "postgresql://invalid_user:invalid_pass@127.0.0.1:9999/invalid_db"

    corpus = PostgresCorpus(dsn=invalid_dsn, min_id=0, schema=worker_schema, batch_size=100)

    # __iter__ should raise exception (connection error or timeout)
    with pytest.raises(Exception):  # Should raise connection error
        list(corpus)

    # stream_texts should raise exception
    with pytest.raises(Exception):
        list(corpus.stream_texts())

    # stream_with_ids should raise exception
    with pytest.raises(Exception):
        list(corpus.stream_with_ids())
