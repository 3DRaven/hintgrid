"""CLI integration tests for reindex command."""

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
def test_cli_reindex_dry_run(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test 'hintgrid reindex --dry-run' command.

    First run pipeline to create posts with embeddings,
    then run reindex --dry-run to verify it analyzes without making changes.
    """
    log_file = tmp_path / "reindex_dry.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # First run pipeline to create embeddings
    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0

    # Get post count before reindex
    neo4j.label("Post")
    posts_before = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) WHERE p.embedding IS NOT NULL RETURN count(p) AS cnt",
            {"post": "Post"},
        )
    )
    count_before = coerce_int(posts_before[0].get("cnt"))

    # Run reindex in dry-run mode
    exit_code = run_cli(monkeypatch, ["reindex", "--dry-run"])
    assert exit_code == 0

    # Verify embeddings are NOT cleared (dry-run)
    posts_after = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) WHERE p.embedding IS NOT NULL RETURN count(p) AS cnt",
            {"post": "Post"},
        )
    )
    count_after = coerce_int(posts_after[0].get("cnt"))

    assert count_after == count_before, "Dry run should not modify embeddings"

    output = capsys.readouterr().out
    assert "DRY RUN" in output or "dry" in output.lower()


@pytest.mark.integration
def test_cli_reindex_actual(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test 'hintgrid reindex' command (actual execution).

    First run pipeline to create posts with embeddings,
    then run reindex to regenerate all embeddings.
    """
    log_file = tmp_path / "reindex_actual.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # First run pipeline to create embeddings
    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0

    # Run actual reindex
    exit_code = run_cli(monkeypatch, ["reindex"])
    assert exit_code == 0

    output = capsys.readouterr().out
    # Should show reindex results
    assert "Reindex" in output or "complete" in output.lower()


@pytest.mark.integration
def test_cli_reindex_after_signature_change(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test reindex detects embedding signature change.

    Scenario:
    1. Run pipeline with FastText (creates signature)
    2. Change to different embedding config
    3. Run reindex - should detect signature mismatch
    """
    log_file = tmp_path / "reindex_signature.log"

    # Clean up worker-specific vector index
    neo4j.label("Post")
    worker_index_name = (
        f"{neo4j.worker_label}_posts" if neo4j.worker_label else "post_embedding_index"
    )
    drop_index_if_exists(neo4j, worker_index_name)
    neo4j.execute_labeled("MATCH (p:__post__) DETACH DELETE p", {"post": "Post"})

    # Step 1: Run with first embedding service (64 dim)
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

    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0

    # Step 2: Change to different config (128 dim)
    drop_index_if_exists(neo4j, worker_index_name)
    service2 = _create_embedding_service_with_dim(vector_size=128)
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

    # Step 3: Run reindex dry-run - should detect signature mismatch
    exit_code = run_cli(monkeypatch, ["reindex", "--dry-run"])
    assert exit_code == 0

    output = capsys.readouterr().out
    # Should show signature information in output
    assert "Signature" in output or "signature" in output.lower() or "Reindex" in output
