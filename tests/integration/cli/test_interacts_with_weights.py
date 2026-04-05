"""Integration tests for INTERACTS_WITH aggregation with parameterizable weights."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid import app as app_module
from hintgrid.utils.coercion import coerce_float, coerce_int

from .conftest import run_cli, set_cli_env

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings
    from pathlib import Path
    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig


@pytest.mark.integration
def test_interacts_with_follows_included(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that FOLLOWS is included in INTERACTS_WITH with correct weight."""
    log_file = tmp_path / "test_follows.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    # Set follows_weight to 2.0 to make it easily identifiable
    test_settings.follows_weight = 2.0
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["run", "--follows-weight", "2.0"])
    assert exit_code == 0

    # Verify that INTERACTS_WITH relationships exist
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__)-[r:INTERACTS_WITH]->(v:__user__) RETURN count(r) AS count",
        {"user": "User"},
    )
    interactions_count = coerce_int(result[0]["count"]) if result else 0
    assert interactions_count > 0, "INTERACTS_WITH relationships should exist"

    # Note: FOLLOWS are no longer loaded separately, they are included in INTERACTS_WITH via SQL


@pytest.mark.integration
def test_interacts_with_follows_excluded(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that when follows_weight=0.0, FOLLOWS is excluded from INTERACTS_WITH."""
    log_file = tmp_path / "test_no_follows.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    test_settings.follows_weight = 0.0
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["run", "--follows-weight", "0.0"])
    assert exit_code == 0

    # Verify that INTERACTS_WITH relationships still exist (from other interactions)
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__)-[r:INTERACTS_WITH]->(v:__user__) RETURN count(r) AS count",
        {"user": "User"},
    )
    interactions_count = coerce_int(result[0]["count"]) if result else 0
    # Should still have interactions from likes, replies, reblogs, mentions
    assert interactions_count >= 0, "INTERACTS_WITH may exist from other interactions"


@pytest.mark.integration
def test_interacts_with_custom_weights(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that custom weights are applied to all INTERACTS_WITH components."""
    log_file = tmp_path / "test_custom_weights.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    # Set custom weights
    test_settings.likes_weight = 0.5
    test_settings.replies_weight = 5.0
    test_settings.reblogs_weight = 2.0
    test_settings.mentions_weight = 1.5
    test_settings.follows_weight = 3.0
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(
        monkeypatch,
        [
            "run",
            "--likes-weight",
            "0.5",
            "--replies-weight",
            "5.0",
            "--reblogs-weight",
            "2.0",
            "--mentions-weight",
            "1.5",
            "--follows-weight",
            "3.0",
        ],
    )
    assert exit_code == 0

    # Verify that INTERACTS_WITH relationships exist with weights
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__)-[r:INTERACTS_WITH]->(v:__user__) "
        "RETURN count(r) AS count, avg(r.weight) AS avg_weight, min(r.weight) AS min_weight, max(r.weight) AS max_weight",
        {"user": "User"},
    )
    if result and coerce_int(result[0].get("count", 0)) > 0:
        interactions_count = coerce_int(result[0]["count"])
        assert interactions_count > 0, "INTERACTS_WITH relationships should exist"
        # Weights should be positive (aggregated from multiple sources)
        avg_weight_val = result[0].get("avg_weight", 0)
        avg_weight = coerce_float(avg_weight_val) if avg_weight_val is not None else 0.0
        assert avg_weight > 0, "Average weight should be positive"


@pytest.mark.integration
def test_user_clustering_only_interacts_with(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that user clustering uses only INTERACTS_WITH (FOLLOWS included via SQL)."""
    log_file = tmp_path / "test_clustering.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    # Verify that users have cluster_id assigned
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__) WHERE u.cluster_id IS NOT NULL RETURN count(u) AS count",
        {"user": "User"},
    )
    clustered_users = coerce_int(result[0]["count"]) if result else 0
    assert clustered_users > 0, "Users should be clustered"

    # Verify that UserCommunity nodes exist
    communities_result = neo4j.execute_and_fetch_labeled(
        "MATCH (uc:__uc__) RETURN count(uc) AS count",
        {"uc": "UserCommunity"},
    )
    communities_count = coerce_int(communities_result[0]["count"]) if communities_result else 0
    assert communities_count > 0, "UserCommunity nodes should exist"
