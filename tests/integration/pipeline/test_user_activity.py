"""Integration tests for user activity loading and filtering.

Tests verify:
- PostgreSQL filtering by active_days and cursor (last_account_id)
- count_active_users() returns correct totals
- _load_user_activity skips when no User nodes exist in Neo4j
- _load_user_activity sets lastActive on User nodes
- Activity cursor (last_activity_account_id) persists in state
- LoadingProgress.add_task() with total creates a determinate progress bar
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.cli.console import LoadingProgress
from hintgrid.config import HintGridSettings
from hintgrid.pipeline.loaders import load_incremental_data
from hintgrid.state import INITIAL_CURSOR, StateStore
from hintgrid.utils.coercion import coerce_int

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from psycopg import Connection
    from psycopg.rows import TupleRow

    from hintgrid.clients.postgres import PostgresClient
    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def activity_sample_data(
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> dict[str, list[int]]:
    """Insert sample data with varied last-activity timestamps.

    account 201 — active now
    account 202 — active 30 days ago (within default 90-day window)
    account 203 — active 200 days ago (outside 90-day window)
    account 204 — no activity data at all (falls back to created_at = NOW)
    """
    with postgres_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO accounts (id, username, domain)
            VALUES
                (201, 'active_now', NULL),
                (202, 'active_30d', 'remote.social'),
                (203, 'inactive_200d', 'old.social'),
                (204, 'no_stats', NULL)
            ON CONFLICT (id) DO NOTHING;
        """)

        cur.execute("""
            INSERT INTO account_stats (id, account_id, last_status_at)
            VALUES
                (1, 201, NOW()),
                (2, 202, NOW() - INTERVAL '30 days'),
                (3, 203, NOW() - INTERVAL '200 days')
            ON CONFLICT (id) DO NOTHING;
        """)

        cur.execute("""
            INSERT INTO users (id, account_id, email, current_sign_in_at)
            VALUES
                (1, 201, 'a@test.com', NOW()),
                (2, 202, 'b@test.com', NOW() - INTERVAL '25 days'),
                (3, 203, 'c@test.com', NOW() - INTERVAL '180 days')
            ON CONFLICT (id) DO NOTHING;
        """)

        # Create statuses so that load_incremental_data produces User nodes
        cur.execute("""
            INSERT INTO statuses (id, account_id, text, language, visibility,
                                  reblog_of_id, in_reply_to_id,
                                  in_reply_to_account_id, reply)
            VALUES
                (1001, 201, 'Hello from active_now', 'en', 0,
                 NULL, NULL, NULL, false),
                (1002, 202, 'Hello from active_30d', 'en', 0,
                 NULL, NULL, NULL, false),
                (1003, 203, 'Hello from inactive_200d', 'en', 0,
                 NULL, NULL, NULL, false),
                (1004, 204, 'Hello from no_stats', 'en', 0,
                 NULL, NULL, NULL, false)
            ON CONFLICT (id) DO NOTHING;
        """)

        postgres_conn.commit()

    return {
        "account_ids": [201, 202, 203, 204],
        "active_within_90d": [201, 202, 204],
        "active_within_15d": [201, 204],
    }


# ---------------------------------------------------------------------------
# PostgreSQL-level tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_stream_user_activity_filters_by_active_days(
    docker_compose: DockerComposeInfo,
    postgres_client: PostgresClient,
    activity_sample_data: dict[str, list[int]],
    mastodon_schema: None,
    worker_schema: str,
) -> None:
    """stream_user_activity(active_days=90) excludes account 203 (200d ago)."""
    from hintgrid.clients.postgres import PostgresClient

    pg = PostgresClient.from_settings(
        HintGridSettings(
            postgres_host=docker_compose.postgres_host,
            postgres_port=docker_compose.postgres_port,
            postgres_database=docker_compose.postgres_db,
            postgres_user=docker_compose.postgres_user,
            postgres_password=docker_compose.postgres_password,
            postgres_schema=worker_schema,
        )
    )
    try:
        rows = list(pg.stream_user_activity(active_days=90))
        streamed_ids = sorted(coerce_int(r["account_id"]) for r in rows)

        # account 203 is 200 days inactive — must be excluded
        assert 203 not in streamed_ids, (
            f"Inactive account 203 should be filtered out, got {streamed_ids}"
        )
        # accounts 201, 202 are within 90 days; 204 falls back to created_at=NOW
        for aid in activity_sample_data["active_within_90d"]:
            assert aid in streamed_ids, (
                f"Active account {aid} should be included, got {streamed_ids}"
            )
    finally:
        pg.close()


@pytest.mark.integration
def test_stream_user_activity_respects_cursor(
    docker_compose: DockerComposeInfo,
    postgres_client: PostgresClient,
    activity_sample_data: dict[str, list[int]],
    mastodon_schema: None,
    worker_schema: str,
) -> None:
    """stream_user_activity with last_account_id skips already-processed rows."""
    from hintgrid.clients.postgres import PostgresClient

    pg = PostgresClient.from_settings(
        HintGridSettings(
            postgres_host=docker_compose.postgres_host,
            postgres_port=docker_compose.postgres_port,
            postgres_database=docker_compose.postgres_db,
            postgres_user=docker_compose.postgres_user,
            postgres_password=docker_compose.postgres_password,
            postgres_schema=worker_schema,
        )
    )
    try:
        # All active within 90 days
        all_rows = list(pg.stream_user_activity(active_days=90))
        all_ids = sorted(coerce_int(r["account_id"]) for r in all_rows)
        assert len(all_ids) >= 2, f"Expected at least 2 active accounts, got {all_ids}"

        # Use cursor = first id -> should skip it
        cursor_id = all_ids[0]
        resumed_rows = list(
            pg.stream_user_activity(active_days=90, last_account_id=cursor_id)
        )
        resumed_ids = sorted(coerce_int(r["account_id"]) for r in resumed_rows)

        assert cursor_id not in resumed_ids, (
            f"Cursor account {cursor_id} should be skipped, got {resumed_ids}"
        )
        assert len(resumed_ids) == len(all_ids) - 1, (
            f"Expected {len(all_ids) - 1} rows after cursor, got {len(resumed_ids)}"
        )
    finally:
        pg.close()


@pytest.mark.integration
def test_count_active_users_matches_stream(
    docker_compose: DockerComposeInfo,
    postgres_client: PostgresClient,
    activity_sample_data: dict[str, list[int]],
    mastodon_schema: None,
    worker_schema: str,
) -> None:
    """count_active_users() returns same cardinality as stream_user_activity()."""
    from hintgrid.clients.postgres import PostgresClient

    pg = PostgresClient.from_settings(
        HintGridSettings(
            postgres_host=docker_compose.postgres_host,
            postgres_port=docker_compose.postgres_port,
            postgres_database=docker_compose.postgres_db,
            postgres_user=docker_compose.postgres_user,
            postgres_password=docker_compose.postgres_password,
            postgres_schema=worker_schema,
        )
    )
    try:
        count_90 = pg.count_active_users(active_days=90)
        stream_90 = list(pg.stream_user_activity(active_days=90))

        assert count_90 == len(stream_90), (
            f"count_active_users(90)={count_90} != len(stream)={len(stream_90)}"
        )

        # With cursor
        count_cursor = pg.count_active_users(active_days=90, last_account_id=201)
        stream_cursor = list(
            pg.stream_user_activity(active_days=90, last_account_id=201)
        )
        assert count_cursor == len(stream_cursor), (
            f"count with cursor={count_cursor} != stream with cursor={len(stream_cursor)}"
        )
    finally:
        pg.close()


@pytest.mark.integration
def test_count_active_users_narrow_window_excludes_old(
    docker_compose: DockerComposeInfo,
    postgres_client: PostgresClient,
    activity_sample_data: dict[str, list[int]],
    mastodon_schema: None,
    worker_schema: str,
) -> None:
    """count_active_users(active_days=15) excludes accounts 202 and 203."""
    from hintgrid.clients.postgres import PostgresClient

    pg = PostgresClient.from_settings(
        HintGridSettings(
            postgres_host=docker_compose.postgres_host,
            postgres_port=docker_compose.postgres_port,
            postgres_database=docker_compose.postgres_db,
            postgres_user=docker_compose.postgres_user,
            postgres_password=docker_compose.postgres_password,
            postgres_schema=worker_schema,
        )
    )
    try:
        count_15 = pg.count_active_users(active_days=15)
        expected = len(activity_sample_data["active_within_15d"])
        assert count_15 == expected, (
            f"Expected {expected} active within 15 days, got {count_15}"
        )
    finally:
        pg.close()


# ---------------------------------------------------------------------------
# Neo4j-level tests (_load_user_activity)
# ---------------------------------------------------------------------------

@pytest.fixture
def empty_graph_data(
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> None:
    """Insert account_stats and users but NO statuses.

    This means load_incremental_data will NOT create User nodes in Neo4j,
    and _load_user_activity should skip entirely.
    """
    with postgres_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO accounts (id, username, domain)
            VALUES (301, 'ghost', NULL)
            ON CONFLICT (id) DO NOTHING;
        """)
        cur.execute("""
            INSERT INTO account_stats (id, account_id, last_status_at)
            VALUES (10, 301, NOW())
            ON CONFLICT (id) DO NOTHING;
        """)
        postgres_conn.commit()


@pytest.mark.integration
def test_load_user_activity_skips_empty_graph(
    docker_compose: DockerComposeInfo,
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    empty_graph_data: None,
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """Activity update is skipped when graph has no User nodes (e.g. after clean)."""
    from hintgrid.clients.postgres import PostgresClient

    test_settings = settings.model_copy(
        update={
            "llm_provider": "openai",
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_dimensions": settings.fasttext_vector_size,
            "batch_size": 100,
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
            "active_user_days": 90,
        }
    )
    state_store = StateStore(neo4j, f"activity_empty_{worker_id}")
    pg = PostgresClient.from_settings(test_settings)

    try:
        # No statuses => no User nodes created => activity update skipped
        load_incremental_data(pg, neo4j, test_settings, state_store)

        # Cursor should remain at initial value
        saved = state_store.load()
        assert saved.last_activity_account_id == INITIAL_CURSOR, (
            "Cursor should not advance when graph is empty"
        )
    finally:
        pg.close()


@pytest.mark.integration
def test_load_user_activity_sets_last_active(
    docker_compose: DockerComposeInfo,
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    activity_sample_data: dict[str, list[int]],
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """_load_user_activity sets lastActive property on User nodes."""
    from hintgrid.clients.postgres import PostgresClient

    test_settings = settings.model_copy(
        update={
            "llm_provider": "openai",
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_dimensions": settings.fasttext_vector_size,
            "batch_size": 100,
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
            "active_user_days": 90,
        }
    )
    state_store = StateStore(neo4j, f"activity_lastactive_{worker_id}")
    pg = PostgresClient.from_settings(test_settings)

    try:
        # Full pipeline: creates User nodes then updates lastActive
        load_incremental_data(pg, neo4j, test_settings, state_store)

        # Check that active users have lastActive set
        rows_with_active = list(
            neo4j.execute_and_fetch_labeled(
                "MATCH (u:__user__) WHERE u.lastActive IS NOT NULL "
                "RETURN u.id AS id",
                {"user": "User"},
            )
        )
        ids_with_active = {coerce_int(r["id"]) for r in rows_with_active}

        # At least the accounts within active_days=90 should have lastActive
        for aid in activity_sample_data["active_within_90d"]:
            assert aid in ids_with_active, (
                f"Active account {aid} should have lastActive, "
                f"but only these do: {ids_with_active}"
            )
    finally:
        pg.close()


@pytest.mark.integration
def test_load_user_activity_cursor_persists(
    docker_compose: DockerComposeInfo,
    postgres_client: PostgresClient,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    activity_sample_data: dict[str, list[int]],
    settings: HintGridSettings,
    worker_id: str,
) -> None:
    """After _load_user_activity the cursor is saved in PipelineState."""
    from hintgrid.clients.postgres import PostgresClient

    test_settings = settings.model_copy(
        update={
            "llm_provider": "openai",
            "llm_base_url": fasttext_embedding_service["api_base"],
            "llm_model": fasttext_embedding_service["model"],
            "llm_dimensions": settings.fasttext_vector_size,
            "batch_size": 100,
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
            "postgres_schema": settings.postgres_schema,
            "active_user_days": 90,
        }
    )
    state_store = StateStore(neo4j, f"activity_cursor_{worker_id}")
    pg = PostgresClient.from_settings(test_settings)

    try:
        load_incremental_data(pg, neo4j, test_settings, state_store)

        # Note: load_incremental_data resets cursor at start, so after a full
        # run the cursor should equal the max account_id that was processed.
        # But cursor is reset to INITIAL_CURSOR at start of each run, so
        # after completing, it reflects the last processed account.
        saved = state_store.load()

        # At least some activity was processed
        assert saved.last_activity_account_id > INITIAL_CURSOR, (
            f"Cursor should advance past {INITIAL_CURSOR}, "
            f"got {saved.last_activity_account_id}"
        )
    finally:
        pg.close()


# ---------------------------------------------------------------------------
# LoadingProgress with total
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_loading_progress_with_total_does_not_raise() -> None:
    """LoadingProgress.add_task(total=N) works end-to-end without errors."""
    with LoadingProgress() as progress:
        progress.add_task("det_task", "[cyan]Testing...[/cyan]", total=100)
        progress.update("det_task", 50)
        progress.update("det_task", 50)
        progress.complete("det_task", "Done 100 items")

    # If we reach here without exception, the determinate path works.


@pytest.mark.integration
def test_loading_progress_without_total_does_not_raise() -> None:
    """LoadingProgress.add_task() without total works end-to-end."""
    with LoadingProgress() as progress:
        progress.add_task("spinner", "[cyan]Spinning...[/cyan]")
        progress.update("spinner", 10)
        progress.complete("spinner", "Done")

    # If we reach here without exception, the indeterminate path works.


@pytest.mark.integration
def test_loading_progress_mixed_tasks() -> None:
    """LoadingProgress supports both determinate and indeterminate tasks."""
    with LoadingProgress() as progress:
        progress.add_task("with_total", "[cyan]Counted...[/cyan]", total=50)
        progress.add_task("no_total", "[cyan]Uncounted...[/cyan]")

        for _ in range(5):
            progress.update("with_total", 10)
            progress.update("no_total", 1)

        progress.complete("with_total", "Counted 50 items")
        progress.complete("no_total", "Uncounted done")
