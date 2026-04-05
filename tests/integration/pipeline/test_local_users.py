"""Integration tests for isLocal flag and local-user filtering.

Covers:
- stream_active_user_ids returns only isLocal=true users
- stream_dirty_user_ids returns only isLocal=true users
- stream_local_user_ids returns only isLocal=true users
- Remote users (isLocal=false) are excluded from all three streams
- Users without isLocal property are excluded (treated as remote)
- update_user_activity sets isLocal and languages on User nodes
"""
from __future__ import annotations
from typing import TYPE_CHECKING
import pytest
from hintgrid.config import HintGridSettings
from hintgrid.pipeline.graph import update_user_activity
from hintgrid.utils.coercion import coerce_int, convert_batch_decimals, convert_dict_to_neo4j_value
if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from psycopg import Connection
    from psycopg.rows import TupleRow
    from tests.conftest import DockerComposeInfo
from psycopg import sql

def _create_user(neo4j: Neo4jClient, user_id: int, *, is_local: bool | None=None, languages: list[str] | None=None, last_active: str | None=None, feed_generated_at: str | None=None) -> None:
    """Create a User node with optional isLocal, languages, and timestamps."""
    props: dict[str, object] = {'id': user_id}
    if is_local is not None:
        props['isLocal'] = is_local
    if languages is not None:
        props['languages'] = languages
    base_query = 'CREATE (u:__user__ {id: $id})'
    set_parts = []
    params: dict[str, object] = {'id': user_id}
    if len(props) > 1:
        additional_props = {k: v for k, v in props.items() if k != 'id'}
        set_parts.append('u += $props')
        params['props'] = additional_props
    if last_active is not None:
        set_parts.append('u.lastActive = datetime($lastActive)')
        params['lastActive'] = last_active
    if feed_generated_at is not None:
        set_parts.append('u.feedGeneratedAt = datetime($feedGeneratedAt)')
        params['feedGeneratedAt'] = feed_generated_at
    query = base_query + ' SET ' + ', '.join(set_parts) if set_parts else base_query
    neo4j.execute_labeled(query, label_map={'user': 'User'}, params=convert_dict_to_neo4j_value(params))

@pytest.mark.integration
def test_stream_active_user_ids_only_local(neo4j: Neo4jClient) -> None:
    """stream_active_user_ids returns only users with isLocal=true."""
    _create_user(neo4j, 50001, is_local=True)
    _create_user(neo4j, 50002, is_local=False)
    _create_user(neo4j, 50003, is_local=True)
    active_ids = list(neo4j.stream_active_user_ids(active_days=90))
    assert 50001 in active_ids
    assert 50003 in active_ids
    assert 50002 not in active_ids, 'Remote user 50002 (isLocal=false) must be excluded'

@pytest.mark.integration
def test_stream_active_user_ids_excludes_users_without_is_local(neo4j: Neo4jClient) -> None:
    """Users without isLocal property are excluded from stream_active_user_ids."""
    _create_user(neo4j, 50010, is_local=True)
    _create_user(neo4j, 50011)
    active_ids = list(neo4j.stream_active_user_ids(active_days=90))
    assert 50010 in active_ids
    assert 50011 not in active_ids, 'User 50011 without isLocal property should be excluded'

@pytest.mark.integration
def test_stream_active_user_ids_respects_last_active_threshold(neo4j: Neo4jClient) -> None:
    """stream_active_user_ids excludes users whose lastActive is too old."""
    _create_user(neo4j, 50020, is_local=True, last_active='2026-02-06T00:00:00Z')
    _create_user(neo4j, 50021, is_local=True, last_active='2025-07-01T00:00:00Z')
    _create_user(neo4j, 50022, is_local=True)
    active_ids = list(neo4j.stream_active_user_ids(active_days=90))
    assert 50020 in active_ids, 'Recently active local user should be included'
    assert 50022 in active_ids, 'User without lastActive should be included'
    assert 50021 not in active_ids, 'User active 200 days ago should be excluded (active_days=90)'

@pytest.mark.integration
def test_stream_dirty_user_ids_only_local(neo4j: Neo4jClient) -> None:
    """stream_dirty_user_ids returns only local users with stale feeds."""
    _create_user(neo4j, 50030, is_local=True)
    _create_user(neo4j, 50031, is_local=False)
    _create_user(neo4j, 50032, is_local=True, feed_generated_at='2026-02-06T12:00:00Z')
    dirty_ids = list(neo4j.stream_dirty_user_ids(active_days=90, feed_size=20))
    assert 50030 in dirty_ids, 'Local user without feedGeneratedAt should be dirty'
    assert 50031 not in dirty_ids, 'Remote user must be excluded even if dirty'

@pytest.mark.integration
def test_stream_local_user_ids_returns_only_local(neo4j: Neo4jClient) -> None:
    """stream_local_user_ids returns all local users regardless of activity."""
    _create_user(neo4j, 50040, is_local=True)
    _create_user(neo4j, 50041, is_local=False)
    _create_user(neo4j, 50042, is_local=True)
    _create_user(neo4j, 50043)
    local_ids = list(neo4j.stream_local_user_ids())
    assert 50040 in local_ids
    assert 50042 in local_ids
    assert 50041 not in local_ids
    assert 50043 not in local_ids

@pytest.mark.integration
def test_stream_local_user_ids_empty_graph(neo4j: Neo4jClient) -> None:
    """stream_local_user_ids returns empty iterator for empty graph."""
    local_ids = list(neo4j.stream_local_user_ids())
    assert local_ids == []

@pytest.mark.integration
def test_update_user_activity_sets_is_local(neo4j: Neo4jClient) -> None:
    """update_user_activity sets isLocal property based on PostgreSQL data."""
    _create_user(neo4j, 50050)
    _create_user(neo4j, 50051)
    batch: list[dict[str, object]] = [{'account_id': 50050, 'last_active': '2026-02-06T00:00:00Z', 'is_local': True, 'chosen_languages': None}, {'account_id': 50051, 'last_active': '2026-02-06T00:00:00Z', 'is_local': False, 'chosen_languages': None}]
    update_user_activity(neo4j, convert_batch_decimals(batch))
    neo4j.label('User')
    rows = list(neo4j.execute_and_fetch_labeled('MATCH (u:__user__) WHERE u.id IN [50050, 50051] RETURN u.id AS id, u.isLocal AS isLocal ORDER BY u.id', {'user': 'User'}))
    assert len(rows) == 2
    assert rows[0]['isLocal'] is True
    assert rows[1]['isLocal'] is False

@pytest.mark.integration
def test_update_user_activity_sets_languages(neo4j: Neo4jClient) -> None:
    """update_user_activity sets languages property from chosen_languages."""
    _create_user(neo4j, 50060)
    batch: list[dict[str, object]] = [{'account_id': 50060, 'last_active': '2026-02-06T00:00:00Z', 'is_local': True, 'chosen_languages': ['en', 'ru', 'de']}]
    update_user_activity(neo4j, convert_batch_decimals(batch))
    neo4j.label('User')
    rows = list(neo4j.execute_and_fetch_labeled('MATCH (u:__user__ {id: 50060}) RETURN u.languages AS languages', {'user': 'User'}))
    assert rows[0]['languages'] == ['en', 'ru', 'de']

@pytest.mark.integration
def test_stream_user_activity_returns_is_local_and_languages(docker_compose: DockerComposeInfo, postgres_conn: Connection[TupleRow], mastodon_schema: None, worker_schema: str, settings: HintGridSettings) -> None:
    """stream_user_activity SQL query returns is_local and chosen_languages."""
    from hintgrid.clients.postgres import PostgresClient
    with postgres_conn.cursor() as cur:
        if worker_schema != 'public':
            cur.execute(sql.SQL('SET search_path TO {}, public').format(sql.Identifier(worker_schema)))
        cur.execute("\n            INSERT INTO accounts (id, username, domain) VALUES\n                (50070, 'local_user', NULL),\n                (50071, 'remote_user', 'remote.social')\n            ON CONFLICT (id) DO NOTHING;\n        ")
        cur.execute("\n            INSERT INTO users (id, account_id, email, current_sign_in_at, chosen_languages)\n            VALUES\n                (70, 50070, 'local@test.com', NOW(), '{en,ru}'),\n                (71, 50071, 'remote@test.com', NOW(), NULL)\n            ON CONFLICT (id) DO NOTHING;\n        ")
        cur.execute('\n            INSERT INTO account_stats (account_id, last_status_at) VALUES\n                (50070, NOW()),\n                (50071, NOW())\n            ON CONFLICT (account_id) DO NOTHING;\n        ')
        postgres_conn.commit()
    pg = PostgresClient.from_settings(HintGridSettings(postgres_host=docker_compose.postgres_host, postgres_port=docker_compose.postgres_port, postgres_database=docker_compose.postgres_db, postgres_user=docker_compose.postgres_user, postgres_password=docker_compose.postgres_password, postgres_schema=worker_schema))
    try:
        rows = list(pg.stream_user_activity(active_days=90))
        row_map = {coerce_int(r['account_id']): r for r in rows}
        assert 50070 in row_map, 'Local user should be in activity stream'
        assert row_map[50070]['is_local'] is True
        assert row_map[50070]['chosen_languages'] == ['en', 'ru']
        assert 50071 in row_map, 'Remote user should be in activity stream'
        assert row_map[50071]['is_local'] is False
        assert row_map[50071]['chosen_languages'] is None
    finally:
        pg.close()