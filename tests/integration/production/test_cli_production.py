"""Full end-to-end CLI tests running hintgrid in production mode (subprocess).

These tests launch the real CLI binary in a subprocess **without**
``HINTGRID_NEO4J_WORKER_LABEL``, which activates:
*   Real Neo4j uniqueness constraints (``User.id``, ``Post.id``, ``AppState.id``).
*   Single-label Cypher queries (no worker-label prefix).
*   Full ``ensure_graph_indexes`` constraint creation path.

Each test is marked ``@pytest.mark.single_worker`` so the
``exclusive_production_mode`` fixture pauses every other xdist worker
before execution.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import redis

from hintgrid.utils.coercion import coerce_int

from .conftest import (
    PRODUCTION_REDIS_DB,
    build_production_env,
    cleanup_postgres_production,
    flush_redis_production,
    run_hintgrid_subprocess,
    seed_postgres_production,
)

if TYPE_CHECKING:
    from pathlib import Path
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings
    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig


# ---------------------------------------------------------------------------
# Test 1: Full pipeline run in production mode
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.single_worker
def test_full_pipeline_production_subprocess(
    exclusive_production_mode: Neo4jClient,
    settings: HintGridSettings,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    tmp_path: Path,
) -> None:
    """Run complete ``hintgrid run`` in a subprocess without worker_label.

    Verifies:
    *   CLI exits with code 0.
    *   Uniqueness constraints are created in Neo4j.
    *   Users, Posts, and WROTE relationships are loaded.
    *   User/Post communities are formed (Leiden clustering).
    *   Redis feeds are generated for each user.
    """
    neo4j = exclusive_production_mode

    # Seed Postgres with test data
    seed_data = seed_postgres_production(
        host=docker_compose.postgres_host,
        port=docker_compose.postgres_port,
        user=docker_compose.postgres_user,
        password=docker_compose.postgres_password,
        db=docker_compose.postgres_db,
        schema="public",
    )

    log_file = tmp_path / "prod_pipeline.log"
    env = build_production_env(
        docker_compose,
        fasttext_embedding_service,
        log_file,
    )

    try:
        result = run_hintgrid_subprocess(["run"], env, timeout=240)

        assert result.returncode == 0, (
            f"Production CLI failed (exit {result.returncode})\n"
            f"stdout:\n{result.stdout[-3000:]}\n"
            f"stderr:\n{result.stderr[-3000:]}"
        )

        # --- Neo4j assertions ---

        # Constraints should exist (production mode creates them)
        constraints = list(neo4j.execute_and_fetch(
            "SHOW CONSTRAINTS YIELD name, type RETURN name, type"
        ))
        unique_names = [
            str(c["name"]) for c in constraints if str(c["type"]) == "UNIQUENESS"
        ]
        assert len(unique_names) >= 2, (
            f"Expected production uniqueness constraints, got: {unique_names}"
        )

        # Users loaded
        users = list(neo4j.execute_and_fetch(
            "MATCH (u:User) RETURN count(u) AS cnt"
        ))
        assert coerce_int(users[0]["cnt"]) == len(seed_data["user_ids"]), (
            f"Expected {len(seed_data['user_ids'])} users"
        )

        # Posts loaded
        posts = list(neo4j.execute_and_fetch(
            "MATCH (p:Post) RETURN count(p) AS cnt"
        ))
        assert coerce_int(posts[0]["cnt"]) == len(seed_data["status_ids"]), (
            f"Expected {len(seed_data['status_ids'])} posts"
        )

        # WROTE relationships
        wrote = list(neo4j.execute_and_fetch(
            "MATCH (:User)-[:WROTE]->(:Post) RETURN count(*) AS cnt"
        ))
        assert coerce_int(wrote[0]["cnt"]) == len(seed_data["status_ids"]), (
            "Every post should have a WROTE relationship"
        )

        # Embeddings stored
        emb_rows = list(neo4j.execute_and_fetch(
            "MATCH (p:Post) WHERE p.embedding IS NOT NULL RETURN count(p) AS cnt"
        ))
        assert coerce_int(emb_rows[0]["cnt"]) == len(seed_data["status_ids"]), (
            "All posts should have embeddings after pipeline run"
        )

        # Clustering happened (user communities)
        uc = list(neo4j.execute_and_fetch(
            "MATCH (:User)-[:BELONGS_TO]->(:UserCommunity) RETURN count(*) AS cnt"
        ))
        assert coerce_int(uc[0]["cnt"]) > 0, (
            "User communities should be created in production mode"
        )

        # Redis feeds
        client = redis.Redis(
            host=docker_compose.redis_host,
            port=docker_compose.redis_port,
            db=PRODUCTION_REDIS_DB,
            decode_responses=True,
        )
        try:
            total_feeds = 0
            for uid in seed_data["user_ids"]:
                total_feeds += client.zcard(f"feed:home:{uid}")
            assert total_feeds > 0, "Redis feeds should be populated in production mode"
        finally:
            client.close()

    finally:
        # Clean up Postgres test data
        cleanup_postgres_production(
            host=docker_compose.postgres_host,
            port=docker_compose.postgres_port,
            user=docker_compose.postgres_user,
            password=docker_compose.postgres_password,
            db=docker_compose.postgres_db,
            schema="public",
        )
        flush_redis_production(
            docker_compose.redis_host,
            docker_compose.redis_port,
        )


# ---------------------------------------------------------------------------
# Test 2: Dry-run mode in production
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.single_worker
def test_dry_run_production_subprocess(
    exclusive_production_mode: Neo4jClient,
    settings: HintGridSettings,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    tmp_path: Path,
) -> None:
    """Run ``hintgrid run --dry-run`` in production mode.

    Verifies:
    *   CLI exits with code 0.
    *   Neo4j data is loaded (users, posts, clustering).
    *   Redis feeds are **not** written.
    """
    neo4j = exclusive_production_mode

    seed_data = seed_postgres_production(
        host=docker_compose.postgres_host,
        port=docker_compose.postgres_port,
        user=docker_compose.postgres_user,
        password=docker_compose.postgres_password,
        db=docker_compose.postgres_db,
        schema="public",
    )

    log_file = tmp_path / "prod_dry_run.log"
    env = build_production_env(
        docker_compose,
        fasttext_embedding_service,
        log_file,
    )

    try:
        result = run_hintgrid_subprocess(["run", "--dry-run"], env, timeout=240)

        assert result.returncode == 0, (
            f"Production dry-run CLI failed (exit {result.returncode})\n"
            f"stdout:\n{result.stdout[-3000:]}\n"
            f"stderr:\n{result.stderr[-3000:]}"
        )

        # Posts should be loaded in Neo4j
        posts = list(neo4j.execute_and_fetch(
            "MATCH (p:Post) RETURN count(p) AS cnt"
        ))
        assert coerce_int(posts[0]["cnt"]) == len(seed_data["status_ids"])

        # Redis feeds must be empty (dry-run)
        client = redis.Redis(
            host=docker_compose.redis_host,
            port=docker_compose.redis_port,
            db=PRODUCTION_REDIS_DB,
            decode_responses=True,
        )
        try:
            for uid in seed_data["user_ids"]:
                count = client.zcard(f"feed:home:{uid}")
                assert count == 0, (
                    f"Feed for user {uid} should be empty in dry-run, got {count}"
                )
        finally:
            client.close()

    finally:
        cleanup_postgres_production(
            host=docker_compose.postgres_host,
            port=docker_compose.postgres_port,
            user=docker_compose.postgres_user,
            password=docker_compose.postgres_password,
            db=docker_compose.postgres_db,
            schema="public",
        )
        flush_redis_production(
            docker_compose.redis_host,
            docker_compose.redis_port,
        )


# ---------------------------------------------------------------------------
# Test 3: Idempotent re-run (merge, not duplicate)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.single_worker
def test_idempotent_rerun_production_subprocess(
    exclusive_production_mode: Neo4jClient,
    settings: HintGridSettings,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    tmp_path: Path,
) -> None:
    """Run the full pipeline twice and verify post/user counts stay stable.

    This validates ``apoc.merge.node`` idempotency at the CLI level:
    a second ``hintgrid run`` must NOT create duplicate nodes when
    production uniqueness constraints are active.
    """
    neo4j = exclusive_production_mode

    seed_data = seed_postgres_production(
        host=docker_compose.postgres_host,
        port=docker_compose.postgres_port,
        user=docker_compose.postgres_user,
        password=docker_compose.postgres_password,
        db=docker_compose.postgres_db,
        schema="public",
    )

    log_file = tmp_path / "prod_idempotent.log"
    env = build_production_env(
        docker_compose,
        fasttext_embedding_service,
        log_file,
    )

    try:
        # --- First run ---
        r1 = run_hintgrid_subprocess(["run"], env, timeout=240)
        assert r1.returncode == 0, (
            f"First run failed (exit {r1.returncode})\n"
            f"stderr:\n{r1.stderr[-3000:]}"
        )

        # Snapshot counts after first run
        users_r1 = coerce_int(next(iter(neo4j.execute_and_fetch(
            "MATCH (u:User) RETURN count(u) AS cnt"
        )))["cnt"])
        posts_r1 = coerce_int(next(iter(neo4j.execute_and_fetch(
            "MATCH (p:Post) RETURN count(p) AS cnt"
        )))["cnt"])

        assert users_r1 == len(seed_data["user_ids"])
        assert posts_r1 == len(seed_data["status_ids"])

        # --- Second run ---
        log_file2 = tmp_path / "prod_idempotent_2.log"
        env["HINTGRID_LOG_FILE"] = str(log_file2)
        r2 = run_hintgrid_subprocess(["run"], env, timeout=240)
        assert r2.returncode == 0, (
            f"Second run failed (exit {r2.returncode})\n"
            f"stderr:\n{r2.stderr[-3000:]}"
        )

        # Counts must not change (merge, not duplicate)
        users_r2 = coerce_int(next(iter(neo4j.execute_and_fetch(
            "MATCH (u:User) RETURN count(u) AS cnt"
        )))["cnt"])
        posts_r2 = coerce_int(next(iter(neo4j.execute_and_fetch(
            "MATCH (p:Post) RETURN count(p) AS cnt"
        )))["cnt"])

        assert users_r2 == users_r1, (
            f"Users duplicated: {users_r1} -> {users_r2}"
        )
        assert posts_r2 == posts_r1, (
            f"Posts duplicated: {posts_r1} -> {posts_r2}"
        )

    finally:
        cleanup_postgres_production(
            host=docker_compose.postgres_host,
            port=docker_compose.postgres_port,
            user=docker_compose.postgres_user,
            password=docker_compose.postgres_password,
            db=docker_compose.postgres_db,
            schema="public",
        )
        flush_redis_production(
            docker_compose.redis_host,
            docker_compose.redis_port,
        )


# ---------------------------------------------------------------------------
# Test 4: Clean command in production mode
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.single_worker
def test_clean_production_subprocess(
    exclusive_production_mode: Neo4jClient,
    settings: HintGridSettings,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    tmp_path: Path,
) -> None:
    """Run ``hintgrid run`` then ``hintgrid clean --graph --redis``.

    Verifies that the clean command removes all Neo4j data and Redis
    feeds when running in production mode (no worker label).
    """
    neo4j = exclusive_production_mode

    seed_postgres_production(
        host=docker_compose.postgres_host,
        port=docker_compose.postgres_port,
        user=docker_compose.postgres_user,
        password=docker_compose.postgres_password,
        db=docker_compose.postgres_db,
        schema="public",
    )

    log_file = tmp_path / "prod_clean.log"
    env = build_production_env(
        docker_compose,
        fasttext_embedding_service,
        log_file,
    )

    try:
        # First, populate Neo4j and Redis
        r1 = run_hintgrid_subprocess(["run"], env, timeout=240)
        assert r1.returncode == 0, (
            f"Pipeline run failed (exit {r1.returncode})\n"
            f"stderr:\n{r1.stderr[-3000:]}"
        )

        # Verify data exists
        posts_before = coerce_int(next(iter(neo4j.execute_and_fetch(
            "MATCH (p:Post) RETURN count(p) AS cnt"
        )))["cnt"])
        assert posts_before > 0, "Posts should exist before clean"

        # Run clean
        log_file2 = tmp_path / "prod_clean_cmd.log"
        env["HINTGRID_LOG_FILE"] = str(log_file2)
        r2 = run_hintgrid_subprocess(
            ["clean", "--graph", "--redis"], env, timeout=120
        )
        assert r2.returncode == 0, (
            f"Clean command failed (exit {r2.returncode})\n"
            f"stderr:\n{r2.stderr[-3000:]}"
        )

        # Neo4j should be empty
        posts_after = coerce_int(next(iter(neo4j.execute_and_fetch(
            "MATCH (p:Post) RETURN count(p) AS cnt"
        )))["cnt"])
        assert posts_after == 0, (
            f"Posts should be deleted after clean, got {posts_after}"
        )

        # Redis should be empty
        client = redis.Redis(
            host=docker_compose.redis_host,
            port=docker_compose.redis_port,
            db=PRODUCTION_REDIS_DB,
            decode_responses=True,
        )
        try:
            # Count keys using getattr to access scan_iter (not in type stubs)
            scan_iter = getattr(client, "scan_iter", None)
            if scan_iter:
                keys = sum(1 for _ in scan_iter("*"))
            else:
                # Fallback: check if any known feed keys exist
                keys = 0
                for uid in range(1, 1000):  # Check first 1000 user IDs
                    if client.zcard(f"feed:home:{uid}") > 0:
                        keys += 1
            assert keys == 0, f"Redis should be empty after clean, got {keys} keys"
        finally:
            client.close()

    finally:
        cleanup_postgres_production(
            host=docker_compose.postgres_host,
            port=docker_compose.postgres_port,
            user=docker_compose.postgres_user,
            password=docker_compose.postgres_password,
            db=docker_compose.postgres_db,
            schema="public",
        )
        flush_redis_production(
            docker_compose.redis_host,
            docker_compose.redis_port,
        )
