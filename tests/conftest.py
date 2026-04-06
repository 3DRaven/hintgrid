"""Common fixtures for all tests.

Uses docker-compose.test.yml for infrastructure management via testcontainers.
Optimized for parallel execution with database-level isolation:
- Neo4j Community: label-based isolation per worker
- Redis: DB numbers (0-15) per worker for complete isolation
- PostgreSQL: Schemas per worker for complete isolation

Single container per service, multiple workers share infrastructure safely.
Uses filelock for xdist coordination and pytest_sessionfinish for cleanup.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict, cast

if TYPE_CHECKING:
    from collections.abc import Generator

    from hintgrid.clients.postgres import PostgresClient
    from hintgrid.embeddings.fasttext_service import FastTextEmbeddingService
    from psycopg.rows import TupleRow
    from tests.protocols import RedisClientOps

import psycopg
import pytest
import redis
from filelock import FileLock
from neo4j import GraphDatabase
from psycopg import Connection, sql
from psycopg_pool import ConnectionPool
from redis import ConnectionPool as RedisConnectionPool
from tenacity import retry, stop_after_attempt, wait_exponential
from testcontainers.compose import DockerCompose

from hintgrid.clients.neo4j import Neo4jClient
from hintgrid.config import HintGridSettings
from tests.fasttext_embedding_service import start_embedding_service
from tests.parallel import (
    IsolatedNeo4jClient,
    WorkerContext,
    ensure_worker_indexes,
    parse_worker_number,
)


class EmbeddingServiceConfig(TypedDict):
    api_base: str
    port: int
    model: str


# Path to docker-compose.test.yml directory and filename
COMPOSE_DIR = Path(__file__).parent.parent
COMPOSE_FILE_NAME = "docker-compose.test.yml"

# Service names from docker-compose.test.yml
NEO4J_SERVICE = "hintgrid-neo4j"
REDIS_SERVICE = "hintgrid-redis"
POSTGRES_SERVICE = "hintgrid-postgres"

# Fixed ports from docker-compose.test.yml
NEO4J_BOLT_PORT = 17687
NEO4J_HTTP_PORT = 17474
REDIS_PORT = 16379
POSTGRES_PORT = 15432

# Internal ports (inside containers)
NEO4J_INTERNAL_BOLT_PORT = 7687
REDIS_INTERNAL_PORT = 6379
POSTGRES_INTERNAL_PORT = 5432

# Credentials from docker-compose.test.yml
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "testpassword"
POSTGRES_USER = "test"
POSTGRES_PASSWORD = "test"
POSTGRES_DB = "test"

# Synchronization file names for xdist
DOCKER_LOCK_FILE = "docker_setup.lock"
DOCKER_READY_FLAG = "docker_ready.flag"

# Exclusive mode coordination (single_worker marker)
EXCLUSIVE_MODE_FLAG = "exclusive_mode.flag"
EXCLUSIVE_MODE_LOCK = "exclusive_mode.lock"
ACTIVE_WORKERS_DIR = "active_workers"
EXCLUSIVE_WAIT_TIMEOUT = 300  # seconds (matches per-test timeout)

# Global variable for compose instance (needed for pytest_sessionfinish)
_compose_instance: DockerCompose | None = None


# =============================================================================
# SQL Schema Definition (centralized, no duplication)
# =============================================================================

MASTODON_SCHEMA_SQL = """
    -- Drop existing tables
    DROP TABLE IF EXISTS bookmarks CASCADE;
    DROP TABLE IF EXISTS account_stats CASCADE;
    DROP TABLE IF EXISTS users CASCADE;
    DROP TABLE IF EXISTS accounts CASCADE;
    DROP TABLE IF EXISTS favourites CASCADE;
    DROP TABLE IF EXISTS follows CASCADE;
    DROP TABLE IF EXISTS blocks CASCADE;
    DROP TABLE IF EXISTS mutes CASCADE;
    DROP TABLE IF EXISTS mentions CASCADE;
    DROP TABLE IF EXISTS status_stats CASCADE;
    DROP TABLE IF EXISTS statuses CASCADE;

    -- Create tables (Mastodon-like schema)
    CREATE TABLE accounts (
        id BIGINT PRIMARY KEY,
        username VARCHAR NOT NULL,
        domain VARCHAR,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    );

    CREATE TABLE statuses (
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
    );

    CREATE TABLE favourites (
        id BIGINT PRIMARY KEY,
        account_id BIGINT NOT NULL,
        status_id BIGINT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    );

    CREATE TABLE follows (
        id BIGINT PRIMARY KEY,
        account_id BIGINT NOT NULL,
        target_account_id BIGINT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        show_reblogs BOOLEAN NOT NULL DEFAULT true,
        notify BOOLEAN NOT NULL DEFAULT false,
        languages VARCHAR[]
    );

    CREATE TABLE blocks (
        id BIGINT PRIMARY KEY,
        account_id BIGINT NOT NULL,
        target_account_id BIGINT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        uri VARCHAR
    );

    CREATE TABLE mutes (
        id BIGINT PRIMARY KEY,
        account_id BIGINT NOT NULL,
        target_account_id BIGINT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        hide_notifications BOOLEAN NOT NULL DEFAULT true,
        expires_at TIMESTAMP
    );

    CREATE TABLE account_stats (
        id BIGSERIAL PRIMARY KEY,
        account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
        statuses_count BIGINT NOT NULL DEFAULT 0,
        following_count BIGINT NOT NULL DEFAULT 0,
        followers_count BIGINT NOT NULL DEFAULT 0,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        last_status_at TIMESTAMP
    );

    CREATE TABLE users (
        id BIGSERIAL PRIMARY KEY,
        email VARCHAR NOT NULL DEFAULT '',
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
        current_sign_in_at TIMESTAMP,
        last_sign_in_at TIMESTAMP,
        locale VARCHAR,
        chosen_languages VARCHAR[]
    );

    CREATE TABLE mentions (
        id BIGSERIAL PRIMARY KEY,
        status_id BIGINT NOT NULL,
        account_id BIGINT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        silent BOOLEAN NOT NULL DEFAULT false
    );

    CREATE TABLE status_stats (
        id BIGSERIAL PRIMARY KEY,
        status_id BIGINT NOT NULL,
        replies_count BIGINT NOT NULL DEFAULT 0,
        reblogs_count BIGINT NOT NULL DEFAULT 0,
        favourites_count BIGINT NOT NULL DEFAULT 0,
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
        untrusted_favourites_count BIGINT NOT NULL DEFAULT 0,
        untrusted_reblogs_count BIGINT NOT NULL DEFAULT 0
    );

    CREATE TABLE bookmarks (
        id BIGSERIAL PRIMARY KEY,
        account_id BIGINT NOT NULL,
        status_id BIGINT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );

    -- Indexes for performance
    CREATE INDEX idx_statuses_account ON statuses(account_id);
    CREATE INDEX idx_statuses_deleted ON statuses(deleted_at);
    CREATE INDEX idx_favourites_status ON favourites(status_id);
    CREATE INDEX idx_follows_account ON follows(account_id);
    CREATE UNIQUE INDEX idx_account_stats_account ON account_stats(account_id);
    CREATE INDEX idx_mentions_status ON mentions(status_id);
    CREATE INDEX idx_mentions_account ON mentions(account_id);
    CREATE UNIQUE INDEX idx_status_stats_status ON status_stats(status_id);
    CREATE INDEX idx_users_account ON users(account_id);
    CREATE INDEX idx_bookmarks_account ON bookmarks(account_id);
    CREATE INDEX idx_bookmarks_status ON bookmarks(status_id);
"""


@dataclass
class DockerComposeInfo:
    """Information about docker-compose services with fixed port mapping."""

    @property
    def neo4j_host(self) -> str:
        return "localhost"

    @property
    def neo4j_port(self) -> int:
        return NEO4J_BOLT_PORT

    @property
    def neo4j_user(self) -> str:
        return NEO4J_USER

    @property
    def neo4j_password(self) -> str:
        return NEO4J_PASSWORD

    @property
    def redis_host(self) -> str:
        return "localhost"

    @property
    def redis_port(self) -> int:
        return REDIS_PORT

    @property
    def postgres_host(self) -> str:
        return "localhost"

    @property
    def postgres_port(self) -> int:
        return POSTGRES_PORT

    @property
    def postgres_user(self) -> str:
        return POSTGRES_USER

    @property
    def postgres_password(self) -> str:
        return POSTGRES_PASSWORD

    @property
    def postgres_db(self) -> str:
        return POSTGRES_DB


# =============================================================================
# Service Readiness Functions (with tenacity retries)
# =============================================================================


@retry(stop=stop_after_attempt(60), wait=wait_exponential(multiplier=0.5, min=0.5, max=3))
def _wait_for_neo4j(host: str, port: int, user: str, password: str) -> None:
    """Wait for Neo4j to be ready using exponential backoff.

    Runs multiple queries to confirm the database is fully operational,
    not just accepting Bolt connections.
    """
    uri = f"bolt://{host}:{port}"
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            session.run("RETURN 1").consume()
            session.run("SHOW INDEXES YIELD name RETURN count(name)").consume()
        print(f"✅ Neo4j ready at {uri}")
    finally:
        driver.close()


@retry(stop=stop_after_attempt(30), wait=wait_exponential(multiplier=0.2, min=0.2, max=2))
def _wait_for_redis(host: str, port: int) -> None:
    """Wait for Redis to be ready using exponential backoff."""
    client = redis.Redis(host=host, port=port)
    try:
        cast("RedisClientOps", client).ping()
        print(f"✅ Redis ready at {host}:{port}")
    finally:
        client.close()


@retry(stop=stop_after_attempt(30), wait=wait_exponential(multiplier=0.2, min=0.2, max=2))
def _wait_for_postgres(host: str, port: int, user: str, password: str, db: str) -> None:
    """Wait for PostgreSQL to be ready using exponential backoff."""
    dsn = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    conn = psycopg.connect(dsn)
    conn.close()
    print(f"✅ PostgreSQL ready at {host}:{port}")


def _setup_neo4j_indexes(info: DockerComposeInfo) -> None:
    """Create Neo4j indexes for efficient worker-based cleanup.

    Removes ALL uniqueness constraints (global and worker-specific) that
    might survive from a previous test run.  Constraints are skipped during
    worker-isolated execution (see ``ensure_graph_indexes``), so any
    leftover constraints from prior runs must be dropped to prevent
    ``IndexEntryConflictException`` in ``apoc.merge.node``.
    """
    client = Neo4jClient(
        host=info.neo4j_host,
        port=info.neo4j_port,
        username=info.neo4j_user,
        password=info.neo4j_password,
    )
    try:
        # Drop ALL constraints — global and worker-specific leftovers.
        # ensure_graph_indexes() skips constraint creation when worker_label
        # is set, so no new constraints will be created during this run.
        try:
            existing = client.execute_and_fetch("SHOW CONSTRAINTS YIELD name RETURN name")
            for row in existing:
                constraint_name = str(row.get("name", ""))
                if constraint_name:
                    # IF EXISTS should not raise, but catch any unexpected errors
                    try:
                        client.execute_labeled(
                            "DROP CONSTRAINT __name__ IF EXISTS",
                            ident_map={"name": constraint_name},
                        )
                    except Exception as e:
                        # Log but don't fail - constraint may not exist or may be in use
                        print(f"Warning: Could not drop constraint {constraint_name}: {e}")
        except Exception:
            # Fallback: drop known constraint names (global + common worker suffixes)
            known_bases = [
                "user_id_unique",
                "post_id_unique",
                "user_community_id_unique",
                "post_community_id_unique",
                "app_state_id_unique",
                "progress_tracker_id_unique",
            ]
            for base_name in known_bases:
                # IF EXISTS should not raise, but catch any unexpected errors
                try:
                    client.execute_labeled(
                        "DROP CONSTRAINT __name__ IF EXISTS",
                        ident_map={"name": base_name},
                    )
                except Exception as e:
                    # Log but don't fail - constraint may not exist
                    print(f"Warning: Could not drop constraint {base_name}: {e}")

        # Also remove global indexes that might conflict (base names without worker suffix).
        # SHOW INDEXES + DROP INDEX is not used here; list matches ensure_graph_indexes
        # naming for non-xdist / local runs.
        global_indexes = [
            "post_created_at",
            "post_author_id",
            "user_username",
            "post_embedding_index",
            "user_community_id",
            "post_community_id",
            "rel_interested_in_last_updated",
            "rel_was_recommended_at",
            "rel_similar_to_weight",
        ]
        for index_name in global_indexes:
            client.execute_labeled(
                "DROP INDEX __name__ IF EXISTS",
                ident_map={"name": index_name},
            )

        ensure_worker_indexes(client)
        print("✅ Neo4j worker isolation indexes created (all constraints removed)")
    except Exception as e:
        print(f"⚠️  Neo4j index creation warning: {e}")
    finally:
        client.close()


# =============================================================================
# Parallel Execution Support (pytest-xdist)
# =============================================================================


@pytest.fixture(scope="session")
def worker_id(request: pytest.FixtureRequest) -> str:
    """Get unique worker ID for parallel test execution.

    Returns:
        Worker ID (e.g., 'gw0', 'gw1') for parallel execution
        or 'master' for sequential execution.
    """
    # pytest-xdist adds workerinput attribute at runtime
    workerinput = getattr(request.config, "workerinput", None)
    if workerinput is not None:
        return str(workerinput.get("workerid", "master"))
    return "master"


@pytest.fixture(scope="session")
def worker_num(worker_id: str) -> int:
    """Get numeric worker ID (0-15) for database isolation.

    Used for:
    - Redis DB selection (0-15)
    - PostgreSQL schema naming
    """
    return parse_worker_number(worker_id)


@pytest.fixture(scope="session")
def worker_schema(worker_id: str) -> str:
    """Get PostgreSQL schema name for this worker.

    Returns:
        Schema name like 'test_gw0', 'test_gw1', or 'public' for master
    """
    if worker_id == "master":
        return "public"
    return f"test_{worker_id}"


@pytest.fixture(scope="session")
def neo4j_worker_label(worker_id: str) -> str:
    """Worker-specific Neo4j label for label-based isolation."""
    return f"worker_{worker_id}"


@pytest.fixture(scope="session")
def worker_context(worker_id: str, worker_num: int) -> WorkerContext:
    """Get complete worker context for all services.

    Provides consistent namespace generation for Neo4j, Redis, PostgreSQL.
    """
    return WorkerContext(worker_id=worker_id, worker_num=worker_num)


# =============================================================================
# Session-Scoped Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def settings(worker_schema: str, neo4j_worker_label: str) -> HintGridSettings:
    """HintGrid settings for tests.

    Uses fasttext_vector_size for llm_dimensions to ensure
    vector index dimensions match FastText embeddings.

    Includes postgres connection params matching docker-compose.test.yml
    so that any code building a DSN via build_postgres_dsn(settings)
    connects to the test container (port 15432) instead of the default (5432).
    """
    base_settings = HintGridSettings()
    return HintGridSettings(
        llm_dimensions=base_settings.fasttext_vector_size,
        postgres_schema=worker_schema,
        neo4j_worker_label=neo4j_worker_label,
        fasttext_min_count=1,
        postgres_host="localhost",
        postgres_port=POSTGRES_PORT,
        postgres_database=POSTGRES_DB,
        postgres_user=POSTGRES_USER,
        postgres_password=POSTGRES_PASSWORD,
    )


@pytest.fixture(scope="session")
def docker_compose(
    tmp_path_factory: pytest.TempPathFactory,
    worker_id: str,
) -> Generator[DockerComposeInfo, None, None]:
    """Start docker-compose infrastructure for all tests.

    Uses filelock for xdist coordination:
    - First worker to acquire lock starts containers
    - Other workers wait for flag file and reuse containers
    - pytest_sessionfinish stops containers when all tests done
    """
    global _compose_instance

    compose_file = COMPOSE_DIR / COMPOSE_FILE_NAME
    if not compose_file.exists():
        pytest.fail(f"docker-compose.test.yml not found at {compose_file}")

    info = DockerComposeInfo()

    # Get shared temp directory for all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    lock_file = root_tmp_dir / DOCKER_LOCK_FILE
    flag_file = root_tmp_dir / DOCKER_READY_FLAG

    # Single mode (no xdist)
    if worker_id == "master":
        print(f"\n🚀 [Single mode] Starting Docker Compose from {compose_file}")
        compose = DockerCompose(
            context=str(COMPOSE_DIR),
            compose_file_name=COMPOSE_FILE_NAME,
            pull=False,
            wait=True,
        )
        compose.start()
        _compose_instance = compose

        # Wait for services
        _wait_for_redis(info.redis_host, info.redis_port)
        _wait_for_postgres(
            info.postgres_host,
            info.postgres_port,
            info.postgres_user,
            info.postgres_password,
            info.postgres_db,
        )
        _wait_for_neo4j(info.neo4j_host, info.neo4j_port, info.neo4j_user, info.neo4j_password)
        _setup_neo4j_indexes(info)

        print("✅ [Single mode] All infrastructure ready!")
        yield info

        # Cleanup in single mode
        compose.stop()
        print("🧹 [Single mode] Docker Compose stopped.")
        return

    # Parallel mode (xdist) - use filelock
    with FileLock(str(lock_file)):
        if not flag_file.exists():
            print(f"\n🚀 [Worker {worker_id}] Initializing Docker environment...")

            compose = DockerCompose(
                context=str(COMPOSE_DIR),
                compose_file_name=COMPOSE_FILE_NAME,
                pull=False,
                wait=True,
            )

            try:
                compose.start()
                _compose_instance = compose

                # Wait for services
                _wait_for_redis(info.redis_host, info.redis_port)
                _wait_for_postgres(
                    info.postgres_host,
                    info.postgres_port,
                    info.postgres_user,
                    info.postgres_password,
                    info.postgres_db,
                )
                _wait_for_neo4j(
                    info.neo4j_host, info.neo4j_port, info.neo4j_user, info.neo4j_password
                )
                _setup_neo4j_indexes(info)

                # Mark as ready
                flag_file.touch()
                print(f"✅ [Worker {worker_id}] Docker environment ready!")

            except Exception as e:
                print(f"❌ [Worker {worker_id}] Setup failed: {e}")
                compose.stop()
                raise
        else:
            print(f"\n🔄 [Worker {worker_id}] Environment already up. Waiting for services...")
            # Wait for services to be accessible
            _wait_for_redis(info.redis_host, info.redis_port)
            _wait_for_postgres(
                info.postgres_host,
                info.postgres_port,
                info.postgres_user,
                info.postgres_password,
                info.postgres_db,
            )
            _wait_for_neo4j(info.neo4j_host, info.neo4j_port, info.neo4j_user, info.neo4j_password)
            print(f"✅ [Worker {worker_id}] Connected to existing environment.")

    yield info
    # Teardown handled by pytest_sessionfinish


def _cleanup_stale_pytest_tmp(keep_last: int = 3) -> None:
    """Remove old pytest temp directories to prevent /tmp (tmpfs) overflow.

    pytest-xdist may not reliably clean up old sessions when tests
    are interrupted or killed, leading to /tmp exhaustion.
    Keeps only the most recent ``keep_last`` session directories.
    """
    import getpass
    import shutil
    import tempfile

    try:
        username = getpass.getuser()
    except Exception:
        return

    tmp_base = Path(tempfile.gettempdir()) / f"pytest-of-{username}"
    if not tmp_base.exists():
        return

    # Sort by modification time, newest first
    session_dirs = sorted(
        [d for d in tmp_base.iterdir() if d.is_dir() and d.name.startswith("pytest-")],
        key=lambda d: d.stat().st_mtime,
        reverse=True,
    )

    for old_dir in session_dirs[keep_last:]:
        try:
            shutil.rmtree(old_dir)
            print(f"🧹 Removed stale pytest temp: {old_dir.name}")
        except Exception as e:
            print(f"⚠️ Failed to cleanup {old_dir.name}: {e}")


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Automatically route single_worker tests to a dedicated xdist group.

    All tests marked with ``@pytest.mark.single_worker`` are assigned
    ``xdist_group="exclusive"`` so they land on the same worker and
    never run in parallel with each other.
    """
    for item in items:
        if item.get_closest_marker("single_worker"):
            item.add_marker(pytest.mark.xdist_group(name="exclusive"))


def pytest_sessionstart(session: pytest.Session) -> None:
    """Clean stale pytest temp directories at session start.

    Prevents /tmp (tmpfs) overflow when previous sessions were
    interrupted without proper teardown (e.g. killed, timed out).
    Runs only in the master process, not in xdist workers.
    """
    if not hasattr(session.config, "workerinput"):
        _cleanup_stale_pytest_tmp(keep_last=3)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Stop Docker Compose after all tests complete.

    Called only in the main process (not in xdist workers).
    Also cleans up stale pytest temp directories.
    """
    global _compose_instance

    # Check if we're in the master process (not a worker)
    if hasattr(session.config, "workerinput"):
        return  # This is a worker, don't cleanup

    # Skip in CI (containers cleaned by agent)
    if os.getenv("CI") == "true":
        print("\ni Skipping Docker teardown (CI environment).")
        return

    print("\n🧹 [Teardown] Cleaning up resources...")

    # Clean up stale pytest temp directories
    _cleanup_stale_pytest_tmp(keep_last=1)

    if _compose_instance is not None:
        try:
            _compose_instance.stop()
            print("✅ Docker Compose stopped successfully.")
        except Exception as e:
            print(f"⚠️ Failed to stop Docker Compose: {e}")
    else:
        print("i No compose instance found (maybe tests didn't use docker_compose).")


# Legacy fixtures for compatibility


@pytest.fixture(scope="session")
def neo4j_container(docker_compose: DockerComposeInfo) -> Generator[DockerComposeInfo, None, None]:
    """Neo4j container info (legacy compatibility)."""
    yield docker_compose


@pytest.fixture(scope="session")
def redis_container(docker_compose: DockerComposeInfo) -> Generator[DockerComposeInfo, None, None]:
    """Redis container info (legacy compatibility)."""
    yield docker_compose


@pytest.fixture(scope="session")
def postgres_container(
    docker_compose: DockerComposeInfo,
) -> Generator[DockerComposeInfo, None, None]:
    """PostgreSQL container info (legacy compatibility)."""
    yield docker_compose


@pytest.fixture(scope="session")
def postgres_pool(
    docker_compose: DockerComposeInfo,
    worker_schema: str,
) -> Generator[ConnectionPool[Connection], None, None]:
    """PostgreSQL connection pool with worker schema isolation.

    Each worker uses its own PostgreSQL schema for complete isolation.
    """
    conninfo = (
        f"postgresql://{docker_compose.postgres_user}:{docker_compose.postgres_password}"
        f"@{docker_compose.postgres_host}:{docker_compose.postgres_port}"
        f"/{docker_compose.postgres_db}"
    )

    pool: ConnectionPool[Connection] = ConnectionPool(
        conninfo=conninfo,
        min_size=2,
        max_size=10,
        timeout=30.0,
        max_idle=300.0,
        max_lifetime=3600.0,
        open=True,
    )

    pool.wait()

    # Create worker schema if not exists (for parallel execution)
    if worker_schema != "public":
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(worker_schema))
                )
            conn.commit()

    yield pool
    pool.close()


@pytest.fixture(scope="session")
def redis_pool(
    docker_compose: DockerComposeInfo,
    worker_num: int,
) -> Generator[RedisConnectionPool, None, None]:
    """Redis connection pool with worker DB isolation.

    Each worker uses a different Redis DB (0-15) for complete isolation.
    """
    pool = RedisConnectionPool(
        host=docker_compose.redis_host,
        port=docker_compose.redis_port,
        db=worker_num,  # Worker-specific DB for isolation
        decode_responses=True,
        max_connections=10,
        socket_timeout=5.0,
        socket_connect_timeout=5.0,
    )

    yield pool
    pool.disconnect()


@pytest.fixture(scope="session")
def ollama_container() -> Generator[None, None, None]:
    """Ollama container (disabled by default, use FastText service instead)."""
    yield None


def _get_free_port() -> int:
    """Get a free port by binding to port 0."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        port = s.getsockname()[1]
    return int(port)


@pytest.fixture(scope="function")
def fasttext_embedding_service(
    settings: HintGridSettings,
    docker_compose: DockerComposeInfo,  # Ensure docker_compose runs first
) -> Generator[EmbeddingServiceConfig, None, None]:
    """FastText embedding service emulating LLM embeddings.

    Each test gets its own instance on a unique port for complete isolation.
    This prevents dimension mismatch issues between parallel tests.

    Service is pre-trained on test data at startup for immediate readiness.
    """
    import requests

    # Use docker_compose to ensure it runs before us
    _ = docker_compose

    # Get unique free port for this test
    port = _get_free_port()

    os.environ["OPENAI_API_KEY"] = "sk-fake-key-for-testing"

    @retry(stop=stop_after_attempt(30), wait=wait_exponential(multiplier=0.1, min=0.1, max=1))
    def _check_service_ready(service_port: int) -> None:
        response = requests.get(f"http://127.0.0.1:{service_port}/health", timeout=2)
        assert response.status_code == 200

    # Preload test data for training at startup
    # Common test texts that cover typical use cases
    preload_texts = [
        "Hello Fediverse! #introduction",
        "I love Python programming #python",
        "Second post about #technology",
        "More Python content #python #coding",
        "GraphDB is awesome #neo4j #graphs",
        "Machine learning with Python is powerful",
        "Data science requires statistical knowledge",
        "Natural language processing with transformers",
        "Deep learning neural networks are fascinating",
        "Web development with modern frameworks",
    ]

    service_thread = start_embedding_service(
        port=port,
        vector_size=settings.fasttext_vector_size,
        window=settings.fasttext_window,
        min_count=1,  # Override: test preload data is too small for default min_count=10
        epochs=settings.fasttext_epochs,
        bucket=settings.fasttext_bucket,
        min_documents=1,
        preload_texts=preload_texts,
    )
    service_thread.ready.wait(timeout=5.0)

    try:
        _check_service_ready(port)
    except Exception as e:
        pytest.fail(f"FastText embedding service failed to start on port {port}: {e}")

    info: EmbeddingServiceConfig = {
        "api_base": f"http://127.0.0.1:{port}/v1",
        "port": port,
        "model": f"openai/fasttext-{settings.fasttext_vector_size}",
    }

    yield info
    # Daemon thread stops automatically when test ends


# =============================================================================
# Function-Scoped Fixtures (with database-level isolation)
# =============================================================================


def _cleanup_neo4j_data(client: Neo4jClient, worker_id: str, neo4j_worker_label: str) -> None:
    """Clean up Neo4j data for a specific worker.

    Extracted to avoid code duplication in fixture setup/teardown.
    """
    if neo4j_worker_label:
        client.execute_labeled(
            "MATCH (n:__worker__) DETACH DELETE n",
            ident_map={"worker": neo4j_worker_label},
        )
        return
    if worker_id == "master":
        # Sequential execution: clean everything
        client.execute("MATCH (n) DETACH DELETE n")
        return
    # Parallel execution: clean only this worker's nodes
    client.execute(
        "MATCH (n {_worker: $worker}) DETACH DELETE n",
        {"worker": worker_id},
    )


def _cleanup_gds_graphs(client: Neo4jClient, worker_id: str, neo4j_worker_label: str) -> None:
    """Clean up GDS graphs for a specific worker."""
    result = client.execute_and_fetch("CALL gds.graph.list() YIELD graphName RETURN graphName")
    for record in result:
        graph_name = str(record["graphName"])
        if worker_id == "master":
            should_drop = True
        else:
            should_drop = graph_name.startswith(f"{worker_id}_")
            if neo4j_worker_label:
                should_drop = should_drop or graph_name.startswith(f"{neo4j_worker_label}-")
        if should_drop:
            client.execute(
                "CALL gds.graph.drop($graphName) YIELD graphName",
                {"graphName": graph_name},
            )


@pytest.fixture
def neo4j(
    docker_compose: DockerComposeInfo,
    worker_id: str,
    neo4j_worker_label: str,
) -> Generator[Neo4jClient, None, None]:
    """Neo4j client with worker-isolated cleanup.

    Uses label-based isolation per worker in Community Edition.
    Index-backed for efficient cleanup.

    Note: Vector indexes are managed at session level via _setup_neo4j_indexes().
    Tests requiring custom dimensions should drop/recreate indexes explicitly.
    """
    client = Neo4jClient(
        host=docker_compose.neo4j_host,
        port=docker_compose.neo4j_port,
        username=docker_compose.neo4j_user,
        password=docker_compose.neo4j_password,
        worker_label=neo4j_worker_label,
    )

    # Setup: clean data before test
    try:
        _cleanup_neo4j_data(client, worker_id, neo4j_worker_label)
    except Exception as e:
        print(f"⚠️  Neo4j pre-cleanup warning: {e}")

    yield client

    # Teardown: clean data and GDS graphs after test
    try:
        _cleanup_neo4j_data(client, worker_id, neo4j_worker_label)
        _cleanup_gds_graphs(client, worker_id, neo4j_worker_label)
    except Exception as e:
        print(f"⚠️  Neo4j cleanup warning: {e}")
    finally:
        client.close()


@pytest.fixture
def neo4j_id_offset(worker_num: int) -> int:
    """Base offset for numeric User/Post ids in parallel tests.

    Neo4j enforces uniqueness on ``User.id`` / ``Post.id`` across the whole
    database; worker-scoped labels do not split the constraint. Shifting ids
    by worker avoids collisions when xdist runs many workers against one DB.
    """
    return worker_num * 10_000_000


@pytest.fixture
def isolated_neo4j(
    neo4j: Neo4jClient, worker_id: str, neo4j_worker_label: str
) -> IsolatedNeo4jClient:
    """Neo4j client with automatic worker isolation.

    All nodes created through this client are automatically tagged
    with _worker property for parallel test safety.

    Example:
        def test_something(isolated_neo4j: IsolatedNeo4jClient):
            isolated_neo4j.create_user(1, username="alice")
            assert isolated_neo4j.count_users() == 1
    """
    return IsolatedNeo4jClient(neo4j, worker_id, worker_label=neo4j_worker_label)


@pytest.fixture
def redis_client(
    docker_compose: DockerComposeInfo,
    worker_num: int,
) -> Generator[redis.Redis, None, None]:
    """Redis client with worker DB isolation.

    Each worker uses a separate Redis DB (0-15), so FLUSHDB is safe.
    Flushes the worker DB before and after each test to prevent
    stale data from leaking between tests.
    """
    client = redis.Redis(
        host=docker_compose.redis_host,
        port=docker_compose.redis_port,
        db=worker_num,  # Worker-specific DB
        decode_responses=True,
    )

    # Pre-test cleanup: remove stale data from previous tests
    client.flushdb()

    yield client

    # Post-test cleanup: remove data written by this test
    client.flushdb()


@pytest.fixture
def postgres_conn(
    postgres_pool: ConnectionPool[Connection],
    worker_schema: str,
) -> Generator[Connection[TupleRow], None, None]:
    """PostgreSQL connection with worker schema isolation.

    Each worker uses its own schema, so DROP operations are safe.
    """
    with postgres_pool.connection() as conn:
        # Set search path to worker's schema
        with conn.cursor() as cur:
            if worker_schema != "public":
                cur.execute(
                    sql.SQL("SET search_path TO {}, public").format(sql.Identifier(worker_schema))
                )
            conn.commit()

        yield conn

        # Cleanup: drop all tables in worker's schema
        try:
            with conn.cursor() as cur:
                cur.execute(
                    sql.SQL("""
                        SELECT tablename FROM pg_tables 
                        WHERE schemaname = {}
                    """).format(sql.Literal(worker_schema))
                )
                tables = [cast("str", row[0]) for row in cur.fetchall()]

                for table in tables:
                    cur.execute(
                        sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE").format(
                            sql.Identifier(worker_schema),
                            sql.Identifier(table),
                        )
                    )

            conn.commit()
        except Exception as e:
            print(f"⚠️  PostgreSQL cleanup warning: {e}")
            conn.rollback()


@pytest.fixture
def redis_client_from_pool(
    redis_pool: RedisConnectionPool,
) -> Generator[redis.Redis, None, None]:
    """Redis client from connection pool with worker DB isolation."""
    client = redis.Redis(connection_pool=redis_pool)

    yield client

    # Safe FLUSHDB - pool is already configured with worker's DB
    try:
        cast("RedisClientOps", client).flushdb()
    except Exception as e:
        print(f"⚠️  Redis cleanup warning: {e}")


# =============================================================================
# Schema Fixtures (with worker schema support)
# =============================================================================


@pytest.fixture
def mastodon_schema(
    postgres_conn: Connection[TupleRow],
    worker_schema: str,
) -> None:
    """Create unified Mastodon schema for all tests.

    Creates tables in the worker's schema for parallel isolation.
    """
    with postgres_conn.cursor() as cur:
        if worker_schema != "public":
            cur.execute(
                sql.SQL("SET search_path TO {}, public").format(sql.Identifier(worker_schema))
            )
        cur.execute(MASTODON_SCHEMA_SQL)
        postgres_conn.commit()


@pytest.fixture
def setup_mastodon_schema_for_cli(mastodon_schema: None) -> None:
    """Create Mastodon-like schema for CLI tests (legacy alias)."""


@pytest.fixture
def sample_data_for_cli(
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
) -> dict[str, list[int]]:
    """Insert sample data into PostgreSQL for CLI tests."""
    with postgres_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO accounts (id, username, domain)
            VALUES
                (101, 'alice', NULL),
                (102, 'bob', 'mastodon.social'),
                (103, 'carol', 'example.org')
            RETURNING id;
        """)
        cur.execute("""
            INSERT INTO statuses (id, account_id, text, language, visibility, reblog_of_id)
            VALUES
                (1, 101, 'Hello Fediverse! #introduction', 'en', 0, NULL),
                (2, 102, 'I love Python programming #python', 'en', 0, NULL),
                (3, 101, 'Second post about #technology', 'en', 0, NULL),
                (4, 103, 'Deleted post', 'en', 0, NULL),
                (5, 102, 'More Python content #python #coding', 'en', 0, NULL),
                (6, 103, 'GraphDB is awesome #neo4j #graphs', 'en', 0, NULL)
            RETURNING id;
        """)
        cur.execute("UPDATE statuses SET deleted_at = NOW() WHERE id = 4;")

        cur.execute("""
            UPDATE statuses
            SET uri = 'https://mastodon.test/users/alice/statuses/999999001'
            WHERE id = 1;
        """)

        cur.execute("""
            INSERT INTO favourites (id, account_id, status_id)
            VALUES
                (1, 102, 1),
                (2, 103, 1),
                (3, 101, 2),
                (4, 102, 5),
                (5, 103, 6)
            RETURNING id;
        """)

        cur.execute("""
            INSERT INTO follows (id, account_id, target_account_id)
            VALUES
                (1, 101, 102),
                (2, 102, 103),
                (3, 103, 101)
            RETURNING id;
        """)

        # Seed account_stats for user activity tracking
        cur.execute("""
            INSERT INTO account_stats (id, account_id, last_status_at)
            VALUES
                (1, 101, NOW()),
                (2, 102, NOW() - INTERVAL '5 days'),
                (3, 103, NOW() - INTERVAL '10 days')
        """)

        # Seed users table for sign-in activity tracking
        cur.execute("""
            INSERT INTO users (id, account_id, email, current_sign_in_at)
            VALUES
                (1, 101, 'alice@example.com', NOW()),
                (2, 102, 'bob@example.com', NOW() - INTERVAL '3 days'),
                (3, 103, 'carol@example.com', NOW() - INTERVAL '7 days')
        """)

        postgres_conn.commit()

    return {"user_ids": [101, 102, 103]}


# =============================================================================
# Client Fixtures
# =============================================================================


@pytest.fixture
def postgres_client(
    docker_compose: DockerComposeInfo,
    worker_schema: str,
) -> Generator[PostgresClient, None, None]:
    """PostgreSQL client for integration tests."""
    from hintgrid.clients.postgres import PostgresClient
    from hintgrid.config import HintGridSettings

    test_settings = HintGridSettings(
        postgres_host=docker_compose.postgres_host,
        postgres_port=docker_compose.postgres_port,
        postgres_database=docker_compose.postgres_db,
        postgres_user=docker_compose.postgres_user,
        postgres_password=docker_compose.postgres_password,
        postgres_schema=worker_schema,
    )

    client = PostgresClient.from_settings(test_settings)
    yield client
    client.close()


@pytest.fixture
def fasttext_service(
    neo4j: Neo4jClient,
    postgres_client: PostgresClient,
    docker_compose: DockerComposeInfo,
    settings: HintGridSettings,
    tmp_path: Path,
) -> Generator[FastTextEmbeddingService, None, None]:
    """FastText embedding service for integration tests."""
    from hintgrid.embeddings.fasttext_service import FastTextEmbeddingService

    # Use docker_compose for correct PostgreSQL connection settings
    test_settings = settings.model_copy(
        update={
            "fasttext_model_path": str(tmp_path),
            "fasttext_min_documents": 2,
            "fasttext_min_count": 1,
            "fasttext_vector_size": 64,
            "fasttext_epochs": 3,
            # Use dynamic Docker container ports
            "postgres_host": docker_compose.postgres_host,
            "postgres_port": docker_compose.postgres_port,
            "postgres_database": docker_compose.postgres_db,
            "postgres_user": docker_compose.postgres_user,
            "postgres_password": docker_compose.postgres_password,
        }
    )

    service = FastTextEmbeddingService(neo4j, test_settings, postgres_client)
    yield service


# =============================================================================
# Exclusive Execution Mode (single_worker marker)
# =============================================================================


@pytest.fixture(autouse=True)
def _exclusive_mode_barrier(
    tmp_path_factory: pytest.TempPathFactory,
    worker_id: str,
    request: pytest.FixtureRequest,
) -> Generator[None, None, None]:
    """Barrier that pauses workers while exclusive production tests run.

    Tracks active workers via per-worker marker files.  When the
    ``exclusive_mode.flag`` file exists, regular tests sleep-wait
    until it is removed.  Exclusive tests (those requesting the
    ``exclusive_production_mode`` fixture) skip the flag check so
    they never block themselves.
    """
    base = tmp_path_factory.getbasetemp().parent
    flag_path = base / EXCLUSIVE_MODE_FLAG
    active_dir = base / ACTIVE_WORKERS_DIR
    active_dir.mkdir(exist_ok=True)
    active_file = active_dir / worker_id

    is_exclusive = "exclusive_production_mode" in request.fixturenames

    if is_exclusive:
        # Exclusive tests handle their own coordination via the
        # ``exclusive_production_mode`` fixture.  We must NOT create an
        # active-worker file here — otherwise the exclusive fixture
        # (which holds the FileLock and waits for other active files)
        # would deadlock with a second exclusive test on another worker
        # whose active file never clears.
        yield
        return

    # Regular test path: wait for exclusive mode, then mark as active
    deadline = time.monotonic() + EXCLUSIVE_WAIT_TIMEOUT
    while flag_path.exists():
        if time.monotonic() > deadline:
            raise TimeoutError(f"Worker {worker_id} timed out waiting for exclusive mode to finish")
        time.sleep(0.2)

    active_file.touch()
    try:
        yield
    finally:
        active_file.unlink(missing_ok=True)


@pytest.fixture
def exclusive_production_mode(
    tmp_path_factory: pytest.TempPathFactory,
    docker_compose: DockerComposeInfo,
    worker_id: str,
) -> Generator[Neo4jClient, None, None]:
    """Provide a production-mode Neo4j client with no worker_label.

    Acquires an exclusive file lock, raises a flag file that pauses
    every other xdist worker, waits for all currently-running tests
    to finish, then yields a clean Neo4j client without any worker
    label.  This allows the test to create real uniqueness constraints
    and verify production APOC-merge behaviour.

    Teardown drops **all** constraints, removes **all** data, recreates
    worker-isolation indexes, and removes the flag so other workers
    resume.
    """
    base = tmp_path_factory.getbasetemp().parent
    flag_path = base / EXCLUSIVE_MODE_FLAG
    lock_path = base / EXCLUSIVE_MODE_LOCK
    active_dir = base / ACTIVE_WORKERS_DIR
    active_dir.mkdir(exist_ok=True)

    with FileLock(str(lock_path)):
        # Signal other workers to pause
        flag_path.touch()

        # Wait until every *other* worker finishes its current test
        deadline = time.monotonic() + EXCLUSIVE_WAIT_TIMEOUT
        active_files: list[Path] = []
        while time.monotonic() < deadline:
            active_files = [f for f in active_dir.iterdir() if f.is_file() and f.name != worker_id]
            if not active_files:
                break
            time.sleep(0.5)
        else:
            flag_path.unlink(missing_ok=True)
            raise TimeoutError(
                f"Exclusive mode timed out waiting for workers: {[f.name for f in active_files]}"
            )

        # Create a production Neo4j client (no worker label)
        client = Neo4jClient(
            host=docker_compose.neo4j_host,
            port=docker_compose.neo4j_port,
            username=docker_compose.neo4j_user,
            password=docker_compose.neo4j_password,
            worker_label=None,
        )

        # Clean the entire database so the test starts from scratch
        client.execute("MATCH (n) DETACH DELETE n")

        try:
            yield client
        finally:
            # ----- Rigorous cleanup -----

            # 1. Drop ALL constraints
            rows = client.execute_and_fetch("SHOW CONSTRAINTS YIELD name RETURN name")
            for row in rows:
                cname = str(row.get("name", ""))
                if cname:
                    client.execute_labeled(
                        "DROP CONSTRAINT __name__ IF EXISTS",
                        ident_map={"name": cname},
                    )

            # 2. Drop ALL indexes (except built-in)
            idx_rows = client.execute_and_fetch(
                "SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name"
            )
            for row in idx_rows:
                iname = str(row.get("name", ""))
                if iname:
                    client.execute_labeled(
                        "DROP INDEX __name__ IF EXISTS",
                        ident_map={"name": iname},
                    )

            # 3. Remove all data
            client.execute("MATCH (n) DETACH DELETE n")

            # 4. Recreate worker-isolation indexes for parallel tests
            ensure_worker_indexes(client)

            # 5. Remove flag so other workers resume
            flag_path.unlink(missing_ok=True)

            client.close()


# =============================================================================
# Auto-Use Fixtures
# =============================================================================


@pytest.fixture(autouse=True)
def reset_fasttext_service(request: pytest.FixtureRequest) -> Generator[None, None, None]:
    """Auto-reset FastText service cache before each test."""
    if "fasttext_embedding_service" in request.fixturenames:
        import requests

        service_config = cast(
            "EmbeddingServiceConfig", request.getfixturevalue("fasttext_embedding_service")
        )

        try:
            response = requests.post(
                f"{service_config['api_base']}/reset",
                timeout=2,
            )
            if response.status_code == 200:
                print("🔄 FastText service cache reset before test")
        except Exception:
            pass

    yield
