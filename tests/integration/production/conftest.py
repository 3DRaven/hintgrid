"""Shared fixtures for production-mode integration tests.

Production tests run with ``worker_label=None`` under the
``exclusive_production_mode`` fixture (no other xdist workers are active).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import psycopg
import redis

if TYPE_CHECKING:
    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig

# Redis DB dedicated to exclusive production tests (high number to avoid
# collision with per-worker DBs 0–15 assigned by parse_worker_number).
PRODUCTION_REDIS_DB = 15


def build_production_env(
    docker_compose: DockerComposeInfo,
    fasttext_service: EmbeddingServiceConfig,
    log_file: Path,
    *,
    postgres_schema: str = "public",
    redis_db: int = PRODUCTION_REDIS_DB,
) -> dict[str, str]:
    """Build a minimal env-dict for running hintgrid CLI in production mode.

    Key difference from the worker-isolated ``_build_subprocess_env``:
    **``HINTGRID_NEO4J_WORKER_LABEL`` is intentionally absent**,
    which makes ``HintGridSettings.neo4j_worker_label`` default to ``None``
    and activates production-only logic (uniqueness constraints, single-label
    queries, etc.).
    """
    env: dict[str, str] = {}

    # Pass essential system vars
    for key in ["PATH", "PYTHONPATH", "HOME", "USER", "LANG", "LC_ALL", "VIRTUAL_ENV"]:
        val = os.environ.get(key)
        if val is not None:
            env[key] = val

    # PostgreSQL
    env["HINTGRID_POSTGRES_HOST"] = docker_compose.postgres_host
    env["HINTGRID_POSTGRES_PORT"] = str(docker_compose.postgres_port)
    env["HINTGRID_POSTGRES_DATABASE"] = docker_compose.postgres_db
    env["HINTGRID_POSTGRES_USER"] = docker_compose.postgres_user
    env["HINTGRID_POSTGRES_PASSWORD"] = docker_compose.postgres_password
    env["HINTGRID_POSTGRES_SCHEMA"] = postgres_schema

    # Neo4j — NO HINTGRID_NEO4J_WORKER_LABEL → production mode
    env["HINTGRID_NEO4J_HOST"] = docker_compose.neo4j_host
    env["HINTGRID_NEO4J_PORT"] = str(docker_compose.neo4j_port)
    env["HINTGRID_NEO4J_USERNAME"] = docker_compose.neo4j_user
    env["HINTGRID_NEO4J_PASSWORD"] = docker_compose.neo4j_password

    # Redis
    env["HINTGRID_REDIS_HOST"] = docker_compose.redis_host
    env["HINTGRID_REDIS_PORT"] = str(docker_compose.redis_port)
    env["HINTGRID_REDIS_DB"] = str(redis_db)
    env["HINTGRID_REDIS_PASSWORD"] = ""

    # Embedding service
    env["HINTGRID_LLM_PROVIDER"] = "openai"
    env["HINTGRID_LLM_BASE_URL"] = fasttext_service["api_base"]
    env["HINTGRID_LLM_MODEL"] = fasttext_service["model"]
    env["HINTGRID_LLM_API_KEY"] = "sk-fake-key-for-testing"
    env["OPENAI_API_KEY"] = "sk-fake-key-for-testing"
    env["HINTGRID_LLM_DIMENSIONS"] = "128"

    # Logging
    env["HINTGRID_LOG_FILE"] = str(log_file)
    env["HINTGRID_LOG_LEVEL"] = "INFO"

    # Test-friendly settings
    env["HINTGRID_INTERESTS_MIN_FAVOURITES"] = "1"
    env["HINTGRID_FEED_DAYS"] = "365"
    env["HINTGRID_FASTTEXT_MIN_COUNT"] = "1"

    return env


def run_hintgrid_subprocess(
    args: list[str],
    env: dict[str, str],
    *,
    timeout: int = 180,
) -> subprocess.CompletedProcess[str]:
    """Run ``python -m hintgrid.app <args>`` in a clean subprocess."""
    cmd = [sys.executable, "-m", "hintgrid.app", *args]
    return subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=str(Path(__file__).parent.parent.parent.parent),
    )


def seed_postgres_production(
    host: str, port: int, user: str, password: str, db: str, schema: str
) -> dict[str, list[int]]:
    """Insert sample Mastodon data into PostgreSQL for a production run.

    Uses a direct ``psycopg`` connection (no pool) because this helper
    is called inside the ``exclusive_production_mode`` fixture where no
    pool fixture is available.

    Returns:
        Mapping with ``user_ids`` and ``status_ids``.
    """
    dsn = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    conn = psycopg.connect(dsn, autocommit=False)
    try:
        from psycopg import sql
        
        with conn.cursor() as cur:
            if schema != "public":
                cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
                cur.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))

            # Ensure tables exist (idempotent)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id BIGINT PRIMARY KEY,
                    username VARCHAR NOT NULL,
                    domain VARCHAR,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS statuses (
                    id BIGINT PRIMARY KEY,
                    account_id BIGINT NOT NULL,
                    text TEXT NOT NULL DEFAULT '',
                    language VARCHAR(10),
                    visibility INTEGER NOT NULL DEFAULT 0,
                    reblog_of_id BIGINT,
                    in_reply_to_id BIGINT,
                    in_reply_to_account_id BIGINT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    deleted_at TIMESTAMP,
                    sensitive BOOLEAN NOT NULL DEFAULT false,
                    spoiler_text TEXT NOT NULL DEFAULT '',
                    reply BOOLEAN NOT NULL DEFAULT false,
                    local BOOLEAN,
                    uri VARCHAR,
                    url VARCHAR,
                    conversation_id BIGINT,
                    poll_id BIGINT,
                    edited_at TIMESTAMP,
                    trendable BOOLEAN
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS favourites (
                    id BIGINT PRIMARY KEY,
                    account_id BIGINT NOT NULL,
                    status_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS follows (
                    id BIGINT PRIMARY KEY,
                    account_id BIGINT NOT NULL,
                    target_account_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    show_reblogs BOOLEAN NOT NULL DEFAULT true,
                    notify BOOLEAN NOT NULL DEFAULT false,
                    languages VARCHAR[]
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS blocks (
                    id BIGINT PRIMARY KEY,
                    account_id BIGINT NOT NULL,
                    target_account_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    uri VARCHAR
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mutes (
                    id BIGINT PRIMARY KEY,
                    account_id BIGINT NOT NULL,
                    target_account_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    hide_notifications BOOLEAN NOT NULL DEFAULT true,
                    expires_at TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS account_stats (
                    id BIGSERIAL PRIMARY KEY,
                    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
                    statuses_count BIGINT NOT NULL DEFAULT 0,
                    following_count BIGINT NOT NULL DEFAULT 0,
                    followers_count BIGINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    last_status_at TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id BIGSERIAL PRIMARY KEY,
                    email VARCHAR NOT NULL DEFAULT '',
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
                    current_sign_in_at TIMESTAMP,
                    last_sign_in_at TIMESTAMP,
                    chosen_languages VARCHAR[]
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mentions (
                    id BIGSERIAL PRIMARY KEY,
                    status_id BIGINT NOT NULL,
                    account_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    silent BOOLEAN NOT NULL DEFAULT false
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS status_stats (
                    id BIGSERIAL PRIMARY KEY,
                    status_id BIGINT NOT NULL,
                    replies_count BIGINT NOT NULL DEFAULT 0,
                    reblogs_count BIGINT NOT NULL DEFAULT 0,
                    favourites_count BIGINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    untrusted_favourites_count BIGINT NOT NULL DEFAULT 0,
                    untrusted_reblogs_count BIGINT NOT NULL DEFAULT 0
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bookmarks (
                    id BIGSERIAL PRIMARY KEY,
                    account_id BIGINT NOT NULL,
                    status_id BIGINT NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)

            # Truncate to ensure clean state
            for tbl in (
                "account_stats", "users", "status_stats",
                "mutes", "blocks", "follows", "favourites", "statuses", "accounts",
                "mentions", "bookmarks",
            ):
                cur.execute(f"TRUNCATE {tbl} CASCADE")

            # Seed accounts
            cur.execute("""
                INSERT INTO accounts (id, username, domain) VALUES
                    (501, 'alice_prod', NULL),
                    (502, 'bob_prod', 'mastodon.social'),
                    (503, 'carol_prod', 'example.org')
            """)

            # Seed statuses
            cur.execute("""
                INSERT INTO statuses (id, account_id, text, language, visibility)
                VALUES
                    (901, 501, 'Hello production! #introduction', 'en', 0),
                    (902, 502, 'Production Python programming #python', 'en', 0),
                    (903, 501, 'Production post about #technology', 'en', 0),
                    (904, 503, 'More production content #coding', 'en', 0),
                    (905, 502, 'Production GraphDB is awesome #neo4j', 'en', 0)
            """)

            # Seed favourites (cross-user likes for community detection)
            cur.execute("""
                INSERT INTO favourites (id, account_id, status_id) VALUES
                    (801, 502, 901),
                    (802, 503, 901),
                    (803, 501, 902),
                    (804, 502, 905),
                    (805, 503, 904)
            """)

            # Seed follows
            cur.execute("""
                INSERT INTO follows (id, account_id, target_account_id) VALUES
                    (701, 501, 502),
                    (702, 502, 503),
                    (703, 503, 501)
            """)

            # Seed account_stats (for user activity tracking)
            cur.execute("""
                INSERT INTO account_stats (account_id, last_status_at) VALUES
                    (501, NOW()),
                    (502, NOW()),
                    (503, NOW())
            """)

            # Seed users (for sign-in tracking)
            cur.execute("""
                INSERT INTO users (email, account_id, current_sign_in_at) VALUES
                    ('alice@example.com', 501, NOW()),
                    ('bob@example.com', 502, NOW()),
                    ('carol@example.com', 503, NOW())
            """)

            # Add indexes
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_st_acct ON statuses(account_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_st_del ON statuses(deleted_at)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_fav_st ON favourites(status_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_fol_acct ON follows(account_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_mentions_status ON mentions(status_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_mentions_account ON mentions(account_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_bookmarks_account ON bookmarks(account_id)"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_bookmarks_status ON bookmarks(status_id)"
            )

        conn.commit()
    finally:
        conn.close()

    return {"user_ids": [501, 502, 503], "status_ids": [901, 902, 903, 904, 905]}


def cleanup_postgres_production(
    host: str, port: int, user: str, password: str, db: str, schema: str
) -> None:
    """Drop tables seeded by ``seed_postgres_production``."""
    dsn = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    from psycopg import sql
    
    conn = psycopg.connect(dsn, autocommit=True)
    try:
        with conn.cursor() as cur:
            if schema != "public":
                cur.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))
            for tbl in (
                "account_stats", "users", "status_stats",
                "mutes", "blocks", "follows", "favourites", "statuses", "accounts",
                "mentions", "bookmarks",
            ):
                cur.execute(f"DROP TABLE IF EXISTS {tbl} CASCADE")
    finally:
        conn.close()


def flush_redis_production(host: str, port: int, db: int = PRODUCTION_REDIS_DB) -> None:
    """Flush the dedicated production Redis DB."""
    client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
    try:
        client.flushdb()
    finally:
        client.close()
