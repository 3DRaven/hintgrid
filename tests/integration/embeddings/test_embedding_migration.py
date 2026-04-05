"""CLI integration tests for embedding migration (dimension mismatch cleanup).

Verifies that switching embedding providers with different vector dimensions
is handled correctly: old embeddings are cleared, new ones are created,
and the pipeline completes without dimension mismatch errors.
"""

from __future__ import annotations

import socket
from typing import TYPE_CHECKING

import pytest

from hintgrid import app as app_module
from hintgrid.utils.coercion import coerce_int

from tests.integration.cli.conftest import drop_index_if_exists, run_cli, set_cli_env

if TYPE_CHECKING:
    from hintgrid.config import HintGridSettings
    from hintgrid.clients.neo4j import Neo4jClient
    from pathlib import Path
    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig


def _get_free_port() -> int:
    """Get a free port by binding to port 0."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        port = s.getsockname()[1]
    return int(port)


def _create_embedding_service(vector_size: int) -> EmbeddingServiceConfig:
    """Create embedding service with specific vector size on a free port."""
    from tests.fasttext_embedding_service import start_embedding_service

    port = _get_free_port()
    thread = start_embedding_service(port=port, vector_size=vector_size)
    thread.ready.wait(timeout=10)

    return {
        "api_base": f"http://127.0.0.1:{port}/v1",
        "port": port,
        "model": f"fasttext-{vector_size}",
    }


def _count_posts_with_embedding(neo4j: Neo4jClient) -> int:
    """Count Post nodes that have an embedding property."""
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) WHERE p.embedding IS NOT NULL "
            "RETURN count(p) AS cnt",
            {"post": "Post"},
        )
    )
    return coerce_int(result[0]["cnt"]) if result else 0



def _get_all_embedding_dims(neo4j: Neo4jClient) -> list[int]:
    """Get all distinct embedding dimensions present in the database."""
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) WHERE p.embedding IS NOT NULL "
            "RETURN DISTINCT size(p.embedding) AS dim",
            {"post": "Post"},
        )
    )
    return [coerce_int(row["dim"]) for row in result]


def _count_total_posts(neo4j: Neo4jClient) -> int:
    """Count total Post nodes in the database."""
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) RETURN count(p) AS cnt",
            {"post": "Post"},
        )
    )
    return coerce_int(result[0]["cnt"]) if result else 0


def _worker_index_name(neo4j: Neo4jClient) -> str:
    """Get worker-specific vector index name."""
    if neo4j.worker_label:
        return f"{neo4j.worker_label}_posts"
    return "post_embedding_index"


@pytest.mark.integration
def test_cli_run_clears_old_embeddings_on_dimension_change(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Full CLI pipeline: switching from 64-dim to 128-dim clears old embeddings.

    Scenario reproducing the production bug:
    1. Run pipeline with 64-dim embeddings → posts get 64-dim vectors
    2. Switch to 128-dim embedding service
    3. Run pipeline again → migration detects change, clears old embeddings
    4. Pipeline completes without dimension mismatch error
    5. All posts now have 128-dim embeddings
    """
    log_file = tmp_path / "migration_dim_change.log"
    idx_name = _worker_index_name(neo4j)

    # --- Phase 1: Run pipeline with 64-dim embeddings ---
    service_64 = _create_embedding_service(vector_size=64)
    settings_64 = set_cli_env(
        monkeypatch, docker_compose, service_64, log_file, worker_id, settings,
    )
    settings_64 = settings_64.model_copy(update={"llm_dimensions": 64})
    monkeypatch.setenv("HINTGRID_LLM_DIMENSIONS", "64")
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: settings_64)

    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0

    # Verify: posts have 64-dim embeddings
    embedded_count_phase1 = _count_posts_with_embedding(neo4j)
    assert embedded_count_phase1 > 0, "Phase 1 should create embeddings"
    dims_phase1 = _get_all_embedding_dims(neo4j)
    assert dims_phase1 == [64], f"Expected [64]-dim, got {dims_phase1}"

    # --- Phase 2: Switch to 128-dim and re-run ---
    drop_index_if_exists(neo4j, idx_name)
    service_128 = _create_embedding_service(vector_size=128)
    settings_128 = settings_64.model_copy(
        update={
            "llm_base_url": service_128["api_base"],
            "llm_model": service_128["model"],
            "llm_dimensions": 128,
        }
    )
    monkeypatch.setenv("HINTGRID_LLM_DIMENSIONS", "128")
    monkeypatch.setenv("HINTGRID_LLM_BASE_URL", service_128["api_base"])
    monkeypatch.setenv("HINTGRID_LLM_MODEL", service_128["model"])
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: settings_128)

    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0

    # Verify: no 64-dim embeddings remain, all are now 128-dim
    dims_phase2 = _get_all_embedding_dims(neo4j)
    assert 64 not in dims_phase2, "Old 64-dim embeddings must be cleared"
    assert dims_phase2 == [128], f"Expected [128]-dim only, got {dims_phase2}"

    # Post count should not change (no posts deleted, only re-embedded)
    total_posts = _count_total_posts(neo4j)
    assert total_posts > 0, "Posts must survive migration"


@pytest.mark.integration
def test_cli_run_no_crash_after_dimension_change(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Pipeline must not crash with vector index error after dimension change.

    This is the exact bug scenario from production:
    - Index has 128-dim, but old posts have 768-dim embeddings
    - db.index.vector.queryNodes fails with dimension mismatch

    After the fix, migration clears old embeddings preventing the crash.
    """
    log_file = tmp_path / "no_crash_dim.log"
    idx_name = _worker_index_name(neo4j)

    # --- Phase 1: Run pipeline with 64-dim ---
    service_64 = _create_embedding_service(vector_size=64)
    settings_64 = set_cli_env(
        monkeypatch, docker_compose, service_64, log_file, worker_id, settings,
    )
    settings_64 = settings_64.model_copy(update={"llm_dimensions": 64})
    monkeypatch.setenv("HINTGRID_LLM_DIMENSIONS", "64")
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: settings_64)

    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0

    posts_after_phase1 = _count_posts_with_embedding(neo4j)
    assert posts_after_phase1 > 0

    # --- Phase 2: Switch to 32-dim and run full pipeline including clustering ---
    drop_index_if_exists(neo4j, idx_name)
    service_32 = _create_embedding_service(vector_size=32)
    settings_32 = settings_64.model_copy(
        update={
            "llm_base_url": service_32["api_base"],
            "llm_model": service_32["model"],
            "llm_dimensions": 32,
        }
    )
    monkeypatch.setenv("HINTGRID_LLM_DIMENSIONS", "32")
    monkeypatch.setenv("HINTGRID_LLM_BASE_URL", service_32["api_base"])
    monkeypatch.setenv("HINTGRID_LLM_MODEL", service_32["model"])
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: settings_32)

    # This would crash before the fix with:
    # "Index query vector has 64 dimensions, but indexed vectors have 32"
    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0, "Pipeline must not crash after dimension change"

    # Verify all embeddings are now 32-dim
    dims = _get_all_embedding_dims(neo4j)
    assert dims == [32], f"Expected [32]-dim only, got {dims}"


@pytest.mark.integration
def test_cli_run_preserves_relationships_after_migration(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Migration must preserve graph relationships (WROTE, FOLLOWS, etc.).

    Embedding cleanup only removes the embedding property from Post nodes.
    All nodes and relationships must survive the migration intact.
    """
    log_file = tmp_path / "preserve_rels.log"
    idx_name = _worker_index_name(neo4j)
    neo4j.label("User")
    neo4j.label("Post")

    # --- Phase 1: Run pipeline with 64-dim ---
    service_64 = _create_embedding_service(vector_size=64)
    settings_64 = set_cli_env(
        monkeypatch, docker_compose, service_64, log_file, worker_id, settings,
    )
    settings_64 = settings_64.model_copy(update={"llm_dimensions": 64})
    monkeypatch.setenv("HINTGRID_LLM_DIMENSIONS", "64")
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: settings_64)

    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0

    # Snapshot relationship counts before migration
    wrote_before = coerce_int(
        next(iter(neo4j.execute_and_fetch_labeled(
                "MATCH (:__user__)-[r:WROTE]->(:__post__) "
                "RETURN count(r) AS cnt",
                {"user": "User", "post": "Post"},
            )))["cnt"]
    )
    interacts_before = coerce_int(
        next(iter(neo4j.execute_and_fetch_labeled(
                "MATCH (:__user__)-[r:INTERACTS_WITH]->(:__user__) "
                "RETURN count(r) AS cnt",
                {"user": "User"},
            )))["cnt"]
    )
    user_count_before = coerce_int(
        next(iter(neo4j.execute_and_fetch_labeled(
                "MATCH (u:__user__) RETURN count(u) AS cnt",
                {"user": "User"},
            )))["cnt"]
    )
    post_count_before = _count_total_posts(neo4j)

    assert wrote_before > 0, "Should have WROTE relationships"
    assert interacts_before > 0, "Should have INTERACTS_WITH relationships (includes FOLLOWS)"

    # --- Phase 2: Switch to 128-dim → triggers migration ---
    drop_index_if_exists(neo4j, idx_name)
    service_128 = _create_embedding_service(vector_size=128)
    settings_128 = settings_64.model_copy(
        update={
            "llm_base_url": service_128["api_base"],
            "llm_model": service_128["model"],
            "llm_dimensions": 128,
        }
    )
    monkeypatch.setenv("HINTGRID_LLM_DIMENSIONS", "128")
    monkeypatch.setenv("HINTGRID_LLM_BASE_URL", service_128["api_base"])
    monkeypatch.setenv("HINTGRID_LLM_MODEL", service_128["model"])
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: settings_128)

    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0

    # Verify: relationships preserved
    wrote_after = coerce_int(
        next(iter(neo4j.execute_and_fetch_labeled(
                "MATCH (:__user__)-[r:WROTE]->(:__post__) "
                "RETURN count(r) AS cnt",
                {"user": "User", "post": "Post"},
            )))["cnt"]
    )
    interacts_after = coerce_int(
        next(iter(neo4j.execute_and_fetch_labeled(
                "MATCH (:__user__)-[r:INTERACTS_WITH]->(:__user__) "
                "RETURN count(r) AS cnt",
                {"user": "User"},
            )))["cnt"]
    )
    user_count_after = coerce_int(
        next(iter(neo4j.execute_and_fetch_labeled(
                "MATCH (u:__user__) RETURN count(u) AS cnt",
                {"user": "User"},
            )))["cnt"]
    )
    post_count_after = _count_total_posts(neo4j)

    assert wrote_after == wrote_before, "WROTE relationships must survive migration"
    assert interacts_after == interacts_before, "INTERACTS_WITH relationships must survive migration"
    assert user_count_after == user_count_before, "User count must not change"
    assert post_count_after == post_count_before, "Post count must not change"


@pytest.mark.integration
def test_cli_run_same_dimension_no_migration(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Running pipeline twice with same config must not trigger migration.

    Embeddings should remain intact when signature hasn't changed.
    """
    log_file = tmp_path / "same_dim.log"

    service = _create_embedding_service(vector_size=64)
    test_settings = set_cli_env(
        monkeypatch, docker_compose, service, log_file, worker_id, settings,
    )
    test_settings = test_settings.model_copy(update={"llm_dimensions": 64})
    monkeypatch.setenv("HINTGRID_LLM_DIMENSIONS", "64")
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # First run
    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0

    embedded_after_first = _count_posts_with_embedding(neo4j)
    assert embedded_after_first > 0

    # Second run with same settings
    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0

    # Embeddings should still be there (not cleared)
    embedded_after_second = _count_posts_with_embedding(neo4j)
    assert embedded_after_second == embedded_after_first, (
        "Same-config re-run must not clear embeddings"
    )
    dims = _get_all_embedding_dims(neo4j)
    assert dims == [64], "Dimensions must remain 64"
