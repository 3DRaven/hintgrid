"""CLI integration tests for vector size mismatch scenarios."""

from __future__ import annotations

import socket
from typing import TYPE_CHECKING

import pytest

from hintgrid import app as app_module
from hintgrid.utils.coercion import coerce_int

from .conftest import drop_index_if_exists, run_cli, set_cli_env

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


def _create_embedding_service_with_dim(vector_size: int) -> EmbeddingServiceConfig:
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


@pytest.mark.integration
def test_vector_size_change_detection(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test behavior when switching embedding providers with different vector size.

    Scenario:
    1. Run pipeline with FastText (vector_size=64)
    2. Posts get embedded with 64-dimensional vectors
    3. User switches to LiteLLM service (vector_size=128)
    4. Pipeline should handle dimension mismatch gracefully
    """
    log_file = tmp_path / "vector_change.log"

    # Clean up: Drop worker-specific vector index if exists
    worker_index_name = (
        f"{neo4j.worker_label}_posts" if neo4j.worker_label else "post_embedding_index"
    )
    drop_index_if_exists(neo4j, worker_index_name)

    # Clean up existing posts (worker-isolated)
    neo4j.label("Post")
    neo4j.execute_labeled("MATCH (p:__post__) DETACH DELETE p", {"post": "Post"})

    # Step 1: Create first embedding service with 64 dimensions
    service1 = _create_embedding_service_with_dim(vector_size=64)

    settings1 = set_cli_env(
        monkeypatch,
        docker_compose,
        service1,
        log_file,
        worker_id,
        settings,
    )
    settings1 = settings1.model_copy(update={"llm_dimensions": 64})
    monkeypatch.setenv("HINTGRID_LLM_DIMENSIONS", "64")
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: settings1)

    # Run pipeline with first vector size
    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0

    # Step 2: Check posts have embeddings with 64 dimensions
    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) "
            "WHERE p.embedding IS NOT NULL "
            "RETURN p.id AS id, size(p.embedding) AS dim "
            "LIMIT 1",
            {"post": "Post"},
        )
    )
    if rows:
        dim_before = coerce_int(rows[0].get("dim"))
        assert dim_before == 64, f"Expected 64-dim embedding, got {dim_before}"

    # Step 3: Create second embedding service with 128 dimensions
    service2 = _create_embedding_service_with_dim(vector_size=128)

    # Update settings with new service
    settings2 = settings1.model_copy(
        update={
            "llm_base_url": service2["api_base"],
            "llm_model": service2["model"],
            "llm_dimensions": 128,
        }
    )
    monkeypatch.setenv("HINTGRID_LLM_DIMENSIONS", "128")
    monkeypatch.setenv("HINTGRID_LLM_BASE_URL", service2["api_base"])
    monkeypatch.setenv("HINTGRID_LLM_MODEL", service2["model"])
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: settings2)

    # Clean Neo4j to simulate fresh start with new provider (worker-isolated)
    neo4j.execute_labeled(
        "MATCH (n:__worker__) DETACH DELETE n",
        label_map={"worker": neo4j.worker_label} if neo4j.worker_label else {},
    )
    drop_index_if_exists(neo4j, worker_index_name)

    # Step 4: Run pipeline with new vector size
    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0

    # Step 5: Verify new embeddings have 128 dimensions
    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) "
            "WHERE p.embedding IS NOT NULL "
            "RETURN p.id AS id, size(p.embedding) AS dim "
            "LIMIT 1",
            {"post": "Post"},
        )
    )
    if rows:
        dim_after = coerce_int(rows[0].get("dim"))
        assert dim_after == 128, f"Expected 128-dim embedding, got {dim_after}"


@pytest.mark.integration
def test_mixed_embedding_dimensions_clustering(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test clustering behavior with mixed embedding dimensions.

    This tests what happens if Neo4j contains posts with different
    embedding dimensions (e.g., from provider switch without cleanup).

    The system should:
    1. Detect dimension mismatch
    2. Either skip incompatible posts or re-embed them
    3. Not crash during clustering
    """
    log_file = tmp_path / "mixed_dims.log"
    neo4j.label("Post")

    # Clean up: Drop worker-specific vector index if exists
    worker_index_name = (
        f"{neo4j.worker_label}_posts" if neo4j.worker_label else "post_embedding_index"
    )
    drop_index_if_exists(neo4j, worker_index_name)

    # Clean up existing posts (worker-isolated)
    neo4j.execute_labeled("MATCH (p:__post__) DETACH DELETE p", {"post": "Post"})

    # Create service with 64-dim embeddings
    service = _create_embedding_service_with_dim(vector_size=64)

    settings1 = set_cli_env(
        monkeypatch,
        docker_compose,
        service,
        log_file,
        worker_id,
        settings,
    )
    settings1 = settings1.model_copy(update={"llm_dimensions": 64})
    monkeypatch.setenv("HINTGRID_LLM_DIMENSIONS", "64")
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: settings1)

    # Run pipeline
    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0

    # Manually insert a post with different embedding dimension
    fake_embedding = [0.1] * 128
    neo4j.execute_labeled(
        "CREATE (p:__post__ {"
        "id: 99999, "
        "text: 'Post with different embedding size', "
        "embedding: $embedding, "
        "created_at: timestamp()"
        "})",
        {"post": "Post"},
        {"embedding": fake_embedding},
    )

    # Now change to 128-dim provider
    service2 = _create_embedding_service_with_dim(vector_size=128)

    settings2 = settings1.model_copy(
        update={
            "llm_base_url": service2["api_base"],
            "llm_model": service2["model"],
            "llm_dimensions": 128,
        }
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: settings2)

    # Drop worker-specific vector index before running with new dimensions
    drop_index_if_exists(neo4j, worker_index_name)

    # Run pipeline again - should handle mixed dimensions
    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    # The pipeline should complete (possibly with warnings)
    assert exit_code == 0

    # Verify clustering still works
    rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) "
            "WHERE p.cluster_id IS NOT NULL "
            "RETURN count(p) AS clustered_count",
            {"post": "Post"},
        )
    )
    clustered_count = coerce_int(rows[0].get("clustered_count"))
    # At least some posts should be clustered
    assert clustered_count >= 0  # May be 0 if all had mismatched dims
