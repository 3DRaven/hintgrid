"""Integration tests for selective 'clean' command flags (--graph, --redis, --models)."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest
import redis

from hintgrid import app as app_module
from hintgrid.utils.coercion import coerce_int

from .conftest import RedisTestClient, run_cli, set_cli_env

if TYPE_CHECKING:
    from hintgrid.config import HintGridSettings
    from hintgrid.clients.neo4j import Neo4jClient
    from pathlib import Path
    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig

# Type alias matching existing stub (Redis is non-generic in our stubs)
RedisClientType = redis.Redis


def _count_graph_nodes(neo4j: Neo4jClient) -> int:
    """Count all nodes visible to the current worker."""
    if neo4j.worker_label:
        rows = neo4j.execute_and_fetch_labeled(
            "MATCH (n:__worker__) RETURN count(n) AS count",
            ident_map={"worker": neo4j.worker_label},
        )
    else:
        rows = neo4j.execute_and_fetch("MATCH (n) RETURN count(n) AS count")
    return coerce_int(rows[0].get("count"))


def _setup_model_files(tmp_path: Path) -> tuple[Path, list[Path]]:
    """Create a model directory with dummy files and return (dir, files)."""
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    dummy_files = [
        model_dir / "phrases_v1.pkl",
        model_dir / "fasttext_v1.bin",
        model_dir / "fasttext_v1.bin.wv.vectors_ngrams.npy",
    ]
    for f in dummy_files:
        f.write_bytes(b"dummy")
    return model_dir, dummy_files


def _prepare_env(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
    model_dir: Path | None = None,
) -> HintGridSettings:
    """Set CLI env variables and run the pipeline to populate data."""
    log_file = tmp_path / "clean_selective.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    if model_dir is not None:
        test_settings = test_settings.model_copy(
            update={"fasttext_model_path": str(model_dir)}
        )
        monkeypatch.setenv("HINTGRID_FASTTEXT_MODEL_PATH", str(model_dir))

    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)
    return test_settings


@pytest.mark.integration
@pytest.mark.single_worker
def test_cli_clean_graph_only(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    redis_client: RedisClientType,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test 'clean --graph' removes Neo4j data but preserves Redis and model files.

    Runs in a single xdist group: full ``hintgrid run`` hammers shared Neo4j indexes
    and can transiently deadlock under concurrent workers (CE Forseti).
    """
    model_dir, dummy_files = _setup_model_files(tmp_path)
    _prepare_env(
        monkeypatch, docker_compose, fasttext_embedding_service,
        tmp_path, worker_id, settings, model_dir,
    )

    # Populate data
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    # Add user entry to Redis so we can verify selective cleaning
    redis_raw = cast("RedisTestClient", redis_client)
    redis_raw.zadd("feed:home:101", {"999": 999})
    assert redis_raw.zcard("feed:home:101") > 0
    assert _count_graph_nodes(neo4j) > 0

    # Clean only graph
    exit_code = run_cli(monkeypatch, ["clean", "--graph"])
    assert exit_code == 0

    # Neo4j should be empty
    assert _count_graph_nodes(neo4j) == 0

    # Redis feed should still have entries (both HintGrid and user)
    assert redis_raw.zcard("feed:home:101") > 0

    # Model files should still exist
    for f in dummy_files:
        assert f.exists(), f"Model file should not be deleted: {f.name}"


@pytest.mark.integration
def test_cli_clean_redis_only(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    redis_client: RedisClientType,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test 'clean --redis' removes Redis data but preserves Neo4j and model files."""
    model_dir, dummy_files = _setup_model_files(tmp_path)
    test_settings = _prepare_env(
        monkeypatch, docker_compose, fasttext_embedding_service,
        tmp_path, worker_id, settings, model_dir,
    )

    # Populate data
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    # Add user entry to Redis
    redis_raw = cast("RedisTestClient", redis_client)
    redis_raw.zadd("feed:home:101", {"999": 999})
    assert redis_raw.zcard("feed:home:101") > 0
    graph_count_before = _count_graph_nodes(neo4j)
    assert graph_count_before > 0

    # Clean only Redis
    exit_code = run_cli(monkeypatch, ["clean", "--redis"])
    assert exit_code == 0

    # Neo4j should still have data
    assert _count_graph_nodes(neo4j) == graph_count_before

    # Redis: HintGrid entries removed, user entry "999" preserved
    remaining = redis_raw.zrange("feed:home:101", 0, -1, withscores=True)
    remaining_ids = [
        member.decode("utf-8") if isinstance(member, bytes) else str(member)
        for member, _ in remaining
    ]
    assert "999" in remaining_ids
    # Verify HintGrid entries were removed
    for member, score in remaining:
        member_text = member.decode("utf-8") if isinstance(member, bytes) else str(member)
        member_id = coerce_int(member_text)
        assert score != float(member_id * test_settings.feed_score_multiplier)

    # Model files should still exist
    for f in dummy_files:
        assert f.exists(), f"Model file should not be deleted: {f.name}"


@pytest.mark.integration
def test_cli_clean_models_only(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    redis_client: RedisClientType,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test 'clean --models' removes model files but preserves Neo4j and Redis."""
    model_dir, dummy_files = _setup_model_files(tmp_path)
    _prepare_env(
        monkeypatch, docker_compose, fasttext_embedding_service,
        tmp_path, worker_id, settings, model_dir,
    )

    # Populate data
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    redis_raw = cast("RedisTestClient", redis_client)
    assert redis_raw.zcard("feed:home:101") > 0
    graph_count_before = _count_graph_nodes(neo4j)
    assert graph_count_before > 0

    # Clean only models
    exit_code = run_cli(monkeypatch, ["clean", "--models"])
    assert exit_code == 0

    # Neo4j should still have data
    assert _count_graph_nodes(neo4j) == graph_count_before

    # Redis should still have data
    assert redis_raw.zcard("feed:home:101") > 0

    # Model files should be deleted
    for f in dummy_files:
        assert not f.exists(), f"Model file was not deleted: {f.name}"
    # Directory itself should remain
    assert model_dir.exists()


@pytest.mark.integration
def test_cli_clean_graph_and_redis(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    redis_client: RedisClientType,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test 'clean --graph --redis' cleans Neo4j and Redis but preserves model files."""
    model_dir, dummy_files = _setup_model_files(tmp_path)
    _prepare_env(
        monkeypatch, docker_compose, fasttext_embedding_service,
        tmp_path, worker_id, settings, model_dir,
    )

    # Populate data
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    redis_raw = cast("RedisTestClient", redis_client)
    assert redis_raw.zcard("feed:home:101") > 0
    assert _count_graph_nodes(neo4j) > 0

    # Add Mastodon entry (score == post_id) to verify it's preserved
    redis_raw.zadd("feed:home:101", {"888": 888.0})
    
    # Clean graph and redis
    exit_code = run_cli(monkeypatch, ["clean", "--graph", "--redis"])
    assert exit_code == 0

    # Neo4j should be empty
    assert _count_graph_nodes(neo4j) == 0

    # Redis: HintGrid entries should be removed, but Mastodon entries preserved
    remaining = redis_raw.zrange("feed:home:101", 0, -1, withscores=True)
    remaining_ids = [
        member.decode("utf-8") if isinstance(member, bytes) else str(member)
        for member, _ in remaining
    ]
    # Mastodon entry (888 with score 888) should be preserved
    assert "888" in remaining_ids
    # Verify all remaining entries are Mastodon entries (score == post_id)
    for member, score in remaining:
        member_text = member.decode("utf-8") if isinstance(member, bytes) else str(member)
        member_id = coerce_int(member_text)
        # Mastodon entries have score == post_id
        assert float(score) == float(member_id), f"Entry {member_id} has score {score}, expected {member_id}"

    # Model files should still exist
    for f in dummy_files:
        assert f.exists(), f"Model file should not be deleted: {f.name}"


@pytest.mark.integration
def test_cli_clean_all_flags(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    redis_client: RedisClientType,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test 'clean --graph --redis --models' cleans everything (same as no flags)."""
    model_dir, dummy_files = _setup_model_files(tmp_path)
    _prepare_env(
        monkeypatch, docker_compose, fasttext_embedding_service,
        tmp_path, worker_id, settings, model_dir,
    )

    # Populate data
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    redis_raw = cast("RedisTestClient", redis_client)
    assert redis_raw.zcard("feed:home:101") > 0
    assert _count_graph_nodes(neo4j) > 0

    # Clean with all flags
    exit_code = run_cli(monkeypatch, ["clean", "--graph", "--redis", "--models"])
    assert exit_code == 0

    # Everything should be cleaned
    assert _count_graph_nodes(neo4j) == 0
    feed_entries = redis_raw.zrange("feed:home:101", 0, -1)
    assert len(feed_entries) == 0
    for f in dummy_files:
        assert not f.exists(), f"Model file was not deleted: {f.name}"
    assert model_dir.exists()
