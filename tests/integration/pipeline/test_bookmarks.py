"""Integration tests for bookmarks integration.

Covers:
- stream_bookmarks from PostgreSQL with cursor and since_date
- merge_bookmarks creates BOOKMARKED relationships in Neo4j
- Bookmarks contribute to interest calculation (rebuild/refresh)
- last_bookmark_id cursor is advanced during loading
"""
from __future__ import annotations
from datetime import datetime, UTC
from typing import TYPE_CHECKING
import pytest
from hintgrid.config import HintGridSettings
from hintgrid.pipeline.graph import merge_bookmarks
from hintgrid.utils.coercion import coerce_int, convert_batch_decimals
if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from psycopg import Connection
    from psycopg.rows import TupleRow
    from hintgrid.clients.postgres import PostgresClient
    from tests.conftest import DockerComposeInfo

@pytest.fixture
def bookmark_sample_data(postgres_conn: Connection[TupleRow], mastodon_schema: None) -> dict[str, list[int]]:
    """Insert bookmark data into PostgreSQL.

    Creates accounts, statuses, and bookmarks for testing.
    """
    with postgres_conn.cursor() as cur:
        cur.execute("\n            INSERT INTO accounts (id, username, domain) VALUES\n                (30001, 'bookmarker', NULL),\n                (30002, 'author1', NULL),\n                (30003, 'author2', 'remote.social')\n            ON CONFLICT (id) DO NOTHING;\n        ")
        cur.execute("\n            INSERT INTO statuses (id, account_id, text, language, visibility,\n                                  reblog_of_id, in_reply_to_id,\n                                  in_reply_to_account_id, reply)\n            VALUES\n                (30101, 30002, 'Post about Python', 'en', 0,\n                 NULL, NULL, NULL, false),\n                (30102, 30003, 'Post about Rust', 'en', 0,\n                 NULL, NULL, NULL, false),\n                (30103, 30002, 'Post about Go', 'en', 0,\n                 NULL, NULL, NULL, false)\n            ON CONFLICT (id) DO NOTHING;\n        ")
        cur.execute("\n            INSERT INTO bookmarks (id, account_id, status_id, created_at) VALUES\n                (1, 30001, 30101, '2026-02-01 10:00:00'),\n                (2, 30001, 30102, '2026-02-02 10:00:00'),\n                (3, 30001, 30103, '2026-02-03 10:00:00')\n            ON CONFLICT (id) DO NOTHING;\n        ")
        postgres_conn.commit()
    return {'bookmark_ids': [1, 2, 3], 'account_ids': [30001, 30002, 30003], 'status_ids': [30101, 30102, 30103]}

@pytest.mark.integration
def test_stream_bookmarks_returns_all(docker_compose: DockerComposeInfo, postgres_client: PostgresClient, mastodon_schema: None, worker_schema: str, bookmark_sample_data: dict[str, list[int]]) -> None:
    """stream_bookmarks returns all bookmarks when cursor is 0."""
    from hintgrid.clients.postgres import PostgresClient
    pg = PostgresClient.from_settings(HintGridSettings(postgres_host=docker_compose.postgres_host, postgres_port=docker_compose.postgres_port, postgres_database=docker_compose.postgres_db, postgres_user=docker_compose.postgres_user, postgres_password=docker_compose.postgres_password, postgres_schema=worker_schema))
    try:
        rows = list(pg.stream_bookmarks(last_id=0))
        assert len(rows) >= 3, f'Expected >= 3 bookmarks, got {len(rows)}'
        bookmark_ids = sorted((coerce_int(r['id']) for r in rows))
        assert 1 in bookmark_ids
        assert 2 in bookmark_ids
        assert 3 in bookmark_ids
    finally:
        pg.close()

@pytest.mark.integration
def test_stream_bookmarks_respects_cursor(docker_compose: DockerComposeInfo, postgres_client: PostgresClient, mastodon_schema: None, worker_schema: str, bookmark_sample_data: dict[str, list[int]]) -> None:
    """stream_bookmarks with last_id=2 skips bookmarks 1 and 2."""
    from hintgrid.clients.postgres import PostgresClient
    pg = PostgresClient.from_settings(HintGridSettings(postgres_host=docker_compose.postgres_host, postgres_port=docker_compose.postgres_port, postgres_database=docker_compose.postgres_db, postgres_user=docker_compose.postgres_user, postgres_password=docker_compose.postgres_password, postgres_schema=worker_schema))
    try:
        rows = list(pg.stream_bookmarks(last_id=2))
        bookmark_ids = [coerce_int(r['id']) for r in rows]
        assert 1 not in bookmark_ids
        assert 2 not in bookmark_ids
        assert 3 in bookmark_ids
    finally:
        pg.close()

@pytest.mark.integration
def test_stream_bookmarks_since_date(docker_compose: DockerComposeInfo, postgres_client: PostgresClient, mastodon_schema: None, worker_schema: str, bookmark_sample_data: dict[str, list[int]]) -> None:
    """stream_bookmarks with since_date filters by created_at."""
    from hintgrid.clients.postgres import PostgresClient
    pg = PostgresClient.from_settings(HintGridSettings(postgres_host=docker_compose.postgres_host, postgres_port=docker_compose.postgres_port, postgres_database=docker_compose.postgres_db, postgres_user=docker_compose.postgres_user, postgres_password=docker_compose.postgres_password, postgres_schema=worker_schema))
    try:
        since = datetime(2026, 2, 2, 0, 0, 0, tzinfo=UTC)
        rows = list(pg.stream_bookmarks(last_id=0, since_date=since))
        bookmark_ids = [coerce_int(r['id']) for r in rows]
        assert 1 not in bookmark_ids, 'Bookmark 1 (Feb 1) should be excluded by since_date'
        assert 2 in bookmark_ids
        assert 3 in bookmark_ids
    finally:
        pg.close()

@pytest.mark.integration
def test_stream_bookmarks_empty_table(docker_compose: DockerComposeInfo, postgres_conn: Connection[TupleRow], mastodon_schema: None, worker_schema: str) -> None:
    """stream_bookmarks returns empty iterator when no bookmarks exist."""
    from hintgrid.clients.postgres import PostgresClient
    pg = PostgresClient.from_settings(HintGridSettings(postgres_host=docker_compose.postgres_host, postgres_port=docker_compose.postgres_port, postgres_database=docker_compose.postgres_db, postgres_user=docker_compose.postgres_user, postgres_password=docker_compose.postgres_password, postgres_schema=worker_schema))
    try:
        rows = list(pg.stream_bookmarks(last_id=0))
        assert rows == []
    finally:
        pg.close()

@pytest.mark.integration
def test_merge_bookmarks_creates_relationships(neo4j: Neo4jClient) -> None:
    """merge_bookmarks creates BOOKMARKED relationships in Neo4j."""
    neo4j.label('User')
    neo4j.label('Post')
    neo4j.execute_labeled('CREATE (:__post__ {id: 30101})', {'post': 'Post'})
    neo4j.execute_labeled('CREATE (:__post__ {id: 30102})', {'post': 'Post'})
    batch: list[dict[str, object]] = [{'account_id': 30001, 'status_id': 30101, 'created_at': '2026-02-01T10:00:00Z'}, {'account_id': 30001, 'status_id': 30102, 'created_at': '2026-02-02T10:00:00Z'}]
    merge_bookmarks(neo4j, convert_batch_decimals(batch))
    rows = list(neo4j.execute_and_fetch_labeled('MATCH (:__user__)-[b:BOOKMARKED]->(:__post__) RETURN count(b) AS cnt', {'user': 'User', 'post': 'Post'}))
    assert coerce_int(rows[0]['cnt']) == 2

@pytest.mark.integration
def test_merge_bookmarks_idempotent(neo4j: Neo4jClient) -> None:
    """merge_bookmarks with same data twice does not duplicate relationships."""
    neo4j.label('Post')
    neo4j.label('User')
    neo4j.execute_labeled('CREATE (:__post__ {id: 30201})', {'post': 'Post'})
    batch: list[dict[str, object]] = [{'account_id': 30010, 'status_id': 30201, 'created_at': '2026-02-01T10:00:00Z'}]
    merge_bookmarks(neo4j, convert_batch_decimals(batch))
    merge_bookmarks(neo4j, convert_batch_decimals(batch))
    rows = list(neo4j.execute_and_fetch_labeled('MATCH (u:__user__ {id: 30010})-[b:BOOKMARKED]->(p:__post__ {id: 30201}) RETURN count(b) AS cnt', {'user': 'User', 'post': 'Post'}))
    assert coerce_int(rows[0]['cnt']) == 1, 'MERGE should create exactly one BOOKMARKED relationship'

@pytest.mark.integration
def test_merge_bookmarks_skips_missing_posts(neo4j: Neo4jClient) -> None:
    """merge_bookmarks skips bookmarks for posts not in Neo4j.

    Uses MATCH for Post, so bookmarks referencing non-existent posts
    are silently skipped (no stub nodes created).
    """
    neo4j.label('User')
    batch: list[dict[str, object]] = [{'account_id': 30020, 'status_id': 99999, 'created_at': '2026-02-01T10:00:00Z'}]
    merge_bookmarks(neo4j, convert_batch_decimals(batch))
    rows = list(neo4j.execute_and_fetch_labeled('MATCH (u:__user__ {id: 30020})-[b:BOOKMARKED]->() RETURN count(b) AS cnt', {'user': 'User'}))
    count = coerce_int(rows[0]['cnt']) if rows else 0
    assert count == 0, 'No BOOKMARKED relationship for non-existent post'

@pytest.mark.integration
def test_merge_bookmarks_empty_batch(neo4j: Neo4jClient) -> None:
    """merge_bookmarks with empty batch does nothing."""
    merge_bookmarks(neo4j, convert_batch_decimals([]))

@pytest.mark.integration
def test_merge_bookmarks_sets_timestamp(neo4j: Neo4jClient) -> None:
    """merge_bookmarks sets 'at' timestamp on BOOKMARKED relationship."""
    neo4j.label('Post')
    neo4j.label('User')
    neo4j.execute_labeled('CREATE (:__post__ {id: 30301})', {'post': 'Post'})
    batch: list[dict[str, object]] = [{'account_id': 30030, 'status_id': 30301, 'created_at': '2026-02-01T10:00:00Z'}]
    merge_bookmarks(neo4j, convert_batch_decimals(batch))
    rows = list(neo4j.execute_and_fetch_labeled('MATCH (:__user__ {id: 30030})-[b:BOOKMARKED]->(:__post__ {id: 30301}) RETURN b.at AS at', {'user': 'User', 'post': 'Post'}))
    assert rows[0]['at'] is not None, 'BOOKMARKED.at should be set'