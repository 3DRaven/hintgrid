"""Integration tests for PostgreSQL status reference resolution."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.clients.postgres import PostgresClient
from hintgrid.config import HintGridSettings

if TYPE_CHECKING:
    from psycopg import Connection
    from psycopg.rows import TupleRow

    from tests.conftest import DockerComposeInfo


@pytest.mark.integration
def test_resolve_status_id_ambiguous(
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    worker_schema: str,
    docker_compose: DockerComposeInfo,
    sample_data_for_cli: dict[str, list[int]],
) -> None:
    """Two statuses sharing the same public id fragment yield an error."""
    assert sample_data_for_cli["user_ids"]
    from psycopg import sql

    with postgres_conn.cursor() as cur:
        if worker_schema != "public":
            cur.execute(
                sql.SQL("SET search_path TO {}, public").format(sql.Identifier(worker_schema))
            )
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, uri)
            VALUES
                (9001, 101, 'dup a', 'en', 0, 'https://a.test/users/u/statuses/777888'),
                (9002, 101, 'dup b', 'en', 0, 'https://b.test/users/u/statuses/777888');
            """
        )
        postgres_conn.commit()

    settings = HintGridSettings(
        postgres_host=docker_compose.postgres_host,
        postgres_port=docker_compose.postgres_port,
        postgres_database=docker_compose.postgres_db,
        postgres_user=docker_compose.postgres_user,
        postgres_password=docker_compose.postgres_password,
        postgres_schema=worker_schema,
    )
    client = PostgresClient.from_settings(settings)
    try:
        resolved, err = client.resolve_status_id("777888")
        assert resolved is None
        assert err is not None
        assert "Multiple posts" in err
    finally:
        client.close()


@pytest.mark.integration
def test_resolve_status_id_public_id(
    postgres_client: PostgresClient,
    sample_data_for_cli: dict[str, list[int]],
) -> None:
    """Public id from URI maps to internal statuses.id."""
    resolved, err = postgres_client.resolve_status_id("999999001")
    assert err is None
    assert resolved == 1
