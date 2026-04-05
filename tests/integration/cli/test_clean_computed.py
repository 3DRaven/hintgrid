"""Integration tests for selective computed data cleaning with cascading."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest
import redis

from hintgrid import app as app_module
from hintgrid.pipeline.graph import check_clusters_exist, check_embeddings_exist, check_interests_exist
from hintgrid.utils.coercion import coerce_int

from .conftest import RedisTestClient, run_cli, set_cli_env

if TYPE_CHECKING:
    from hintgrid.config import HintGridSettings
    from hintgrid.clients.neo4j import Neo4jClient
    from pathlib import Path
    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig

# Type alias matching existing stub (Redis is non-generic in our stubs)
RedisClientType = redis.Redis


def _count_similarity_links(neo4j: Neo4jClient) -> int:
    """Count SIMILAR_TO relationships."""
    rows = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__)-[r:SIMILAR_TO]->(:__post__) RETURN count(r) AS count",
        {"post": "Post"},
    )
    return coerce_int(rows[0].get("count")) if rows else 0


def _count_interests(neo4j: Neo4jClient) -> int:
    """Count INTERESTED_IN relationships."""
    rows = neo4j.execute_and_fetch_labeled(
        "MATCH (uc:__uc__)-[i:INTERESTED_IN]->(:__pc__) RETURN count(i) AS count",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )
    return coerce_int(rows[0].get("count")) if rows else 0


def _count_recommendations(neo4j: Neo4jClient) -> int:
    """Count WAS_RECOMMENDED relationships."""
    rows = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__)-[r:WAS_RECOMMENDED]->(:__post__) RETURN count(r) AS count",
        {"user": "User", "post": "Post"},
    )
    return coerce_int(rows[0].get("count")) if rows else 0


def _count_fasttext_state(neo4j: Neo4jClient) -> int:
    """Count FastTextState nodes."""
    from hintgrid.embeddings.fasttext_service import STATE_NODE_ID

    rows = neo4j.execute_and_fetch_labeled(
        "MATCH (s:__label__ {id: $id}) RETURN count(s) AS count",
        {"label": "FastTextState"},
        {"id": STATE_NODE_ID},
    )
    return coerce_int(rows[0].get("count")) if rows else 0


def _create_fasttext_state(neo4j: Neo4jClient) -> None:
    """Create FastTextState node manually for testing."""
    from hintgrid.embeddings.fasttext_service import STATE_NODE_ID

    # Create FastTextState node using APOC (same as FastTextEmbeddingService)
    neo4j.execute(
        "CALL apoc.merge.node($labels, {id: $id}, "
        "{version: $version, last_trained_post_id: 0, "
        " vocab_size: 0, corpus_size: 0, "
        " updated_at: timestamp()}, {}) "
        "YIELD node",
        {
            "labels": neo4j.labels_list("FastTextState"),
            "id": STATE_NODE_ID,
            "version": 0,
        },
    )


def _count_user_communities(neo4j: Neo4jClient) -> int:
    """Count UserCommunity nodes."""
    rows = neo4j.execute_and_fetch_labeled(
        "MATCH (uc:__uc__) RETURN count(uc) AS count",
        {"uc": "UserCommunity"},
    )
    return coerce_int(rows[0].get("count")) if rows else 0


def _count_post_communities(neo4j: Neo4jClient) -> int:
    """Count PostCommunity nodes."""
    rows = neo4j.execute_and_fetch_labeled(
        "MATCH (pc:__pc__) RETURN count(pc) AS count",
        {"pc": "PostCommunity"},
    )
    return coerce_int(rows[0].get("count")) if rows else 0


def _prepare_data(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> HintGridSettings:
    """Set CLI env and run pipeline to populate computed data."""
    log_file = tmp_path / "clean_computed.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    # Set low similarity threshold to ensure similarity links are created
    monkeypatch.setenv("HINTGRID_SIMILARITY_THRESHOLD", "0.1")
    test_settings = test_settings.model_copy(update={"similarity_threshold": 0.1})
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Run pipeline to create computed data
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    return test_settings


@pytest.mark.integration
def test_clean_embeddings_cascades_to_similarity(
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
    """Test that clean --embeddings cascades to similarity graph."""
    _prepare_data(
        monkeypatch, docker_compose, fasttext_embedding_service,
        tmp_path, worker_id, settings,
    )

    # Verify embeddings exist
    assert check_embeddings_exist(neo4j)
    # Similarity links may not exist if embeddings are not similar enough
    # But we still test cascading cleanup behavior

    # Clean embeddings
    exit_code = run_cli(monkeypatch, ["clean", "--embeddings"])
    assert exit_code == 0

    # Embeddings should be cleared
    assert not check_embeddings_exist(neo4j)

    # Similarity should be cleared (cascade)
    similarity_count_after = _count_similarity_links(neo4j)
    assert similarity_count_after == 0

    # Post clusters should be cleared (cascade from similarity)
    _, posts_exist = check_clusters_exist(neo4j)
    assert not posts_exist

    # Interests should be cleared (cascade from similarity)
    assert not check_interests_exist(neo4j)

    # User clusters should remain (not cascaded from embeddings)
    users_exist, _ = check_clusters_exist(neo4j)
    assert users_exist


@pytest.mark.integration
def test_clean_similarity_cascades_to_clusters(
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
    """Test that clean --similarity cascades to post clusters and interests."""
    _prepare_data(
        monkeypatch, docker_compose, fasttext_embedding_service,
        tmp_path, worker_id, settings,
    )

    # Verify post clusters exist (similarity may not exist if embeddings are not similar)
    _, posts_exist_before = check_clusters_exist(neo4j)
    assert posts_exist_before
    interests_count_before = _count_interests(neo4j)
    assert interests_count_before > 0

    # Clean similarity
    exit_code = run_cli(monkeypatch, ["clean", "--similarity"])
    assert exit_code == 0

    # Similarity should be cleared
    similarity_count_after = _count_similarity_links(neo4j)
    assert similarity_count_after == 0

    # Post clusters should be cleared (cascade)
    _, posts_exist_after = check_clusters_exist(neo4j)
    assert not posts_exist_after

    # Interests should be cleared (cascade)
    interests_count_after = _count_interests(neo4j)
    assert interests_count_after == 0

    # User clusters should remain (not cascaded from similarity)
    users_exist, _ = check_clusters_exist(neo4j)
    assert users_exist

    # Embeddings should remain (not cascaded from similarity)
    assert check_embeddings_exist(neo4j)


@pytest.mark.integration
def test_clean_clusters_cascades_to_interests(
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
    """Test that clean --clusters cascades to interests and recommendations."""
    _prepare_data(
        monkeypatch, docker_compose, fasttext_embedding_service,
        tmp_path, worker_id, settings,
    )

    # Verify clusters, interests, and recommendations exist
    users_exist_before, posts_exist_before = check_clusters_exist(neo4j)
    assert users_exist_before
    assert posts_exist_before
    interests_count_before = _count_interests(neo4j)
    assert interests_count_before > 0
    recommendations_count_before = _count_recommendations(neo4j)
    assert recommendations_count_before > 0

    # Clean clusters
    exit_code = run_cli(monkeypatch, ["clean", "--clusters"])
    assert exit_code == 0

    # Clusters should be cleared
    users_exist_after, posts_exist_after = check_clusters_exist(neo4j)
    assert not users_exist_after
    assert not posts_exist_after

    # Communities should be deleted
    assert _count_user_communities(neo4j) == 0
    assert _count_post_communities(neo4j) == 0

    # Interests should be cleared (cascade)
    interests_count_after = _count_interests(neo4j)
    assert interests_count_after == 0

    # Recommendations should be cleared (cascade)
    recommendations_count_after = _count_recommendations(neo4j)
    assert recommendations_count_after == 0

    # Embeddings should remain (similarity may not exist if embeddings are not similar)
    assert check_embeddings_exist(neo4j)


@pytest.mark.integration
def test_clean_all_computed_selectively(
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
    """Test cleaning all computed data types selectively."""
    _prepare_data(
        monkeypatch, docker_compose, fasttext_embedding_service,
        tmp_path, worker_id, settings,
    )

    # Verify all computed data exists
    assert check_embeddings_exist(neo4j)
    users_exist, posts_exist = check_clusters_exist(neo4j)
    assert users_exist
    assert posts_exist
    assert check_interests_exist(neo4j)
    assert _count_recommendations(neo4j) > 0
    # FastTextState is only created with built-in FastText, so create it manually for testing
    _create_fasttext_state(neo4j)
    assert _count_fasttext_state(neo4j) > 0

    # Clean fasttext state only
    exit_code = run_cli(monkeypatch, ["clean", "--fasttext-state"])
    assert exit_code == 0
    assert _count_fasttext_state(neo4j) == 0
    # Other data should remain
    assert check_embeddings_exist(neo4j)
    assert check_clusters_exist(neo4j) == (True, True)

    # Clean interests only
    exit_code = run_cli(monkeypatch, ["clean", "--interests"])
    assert exit_code == 0
    assert not check_interests_exist(neo4j)
    # Clusters should remain
    assert check_clusters_exist(neo4j) == (True, True)

    # Clean recommendations only
    exit_code = run_cli(monkeypatch, ["clean", "--recommendations"])
    assert exit_code == 0
    assert _count_recommendations(neo4j) == 0
    # Clusters should remain
    assert check_clusters_exist(neo4j) == (True, True)

    # Clean clusters (will cascade to interests and recommendations, but they're already clean)
    exit_code = run_cli(monkeypatch, ["clean", "--clusters"])
    assert exit_code == 0
    assert check_clusters_exist(neo4j) == (False, False)

    # Clean similarity (will cascade to post clusters and interests)
    exit_code = run_cli(monkeypatch, ["clean", "--similarity"])
    assert exit_code == 0
    assert _count_similarity_links(neo4j) == 0
    # Post clusters already cleaned, but verify
    _, posts_exist_after = check_clusters_exist(neo4j)
    assert not posts_exist_after

    # Clean embeddings (will cascade to similarity, but it's already clean)
    exit_code = run_cli(monkeypatch, ["clean", "--embeddings"])
    assert exit_code == 0
    assert not check_embeddings_exist(neo4j)


@pytest.mark.integration
def test_run_warns_missing_embeddings(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    redis_client: RedisClientType,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test that run warns when embeddings are missing."""
    _prepare_data(
        monkeypatch, docker_compose, fasttext_embedding_service,
        tmp_path, worker_id, settings,
    )

    # Clean embeddings
    exit_code = run_cli(monkeypatch, ["clean", "--embeddings"])
    assert exit_code == 0
    assert not check_embeddings_exist(neo4j)

    # Run pipeline - should warn about missing embeddings
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    # Check for specific warning about missing embeddings (from print_warning)
    output = capsys.readouterr().out
    assert "No embeddings found" in output


@pytest.mark.integration
def test_run_warns_missing_clusters(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    redis_client: RedisClientType,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test that run warns when clusters are missing."""
    _prepare_data(
        monkeypatch, docker_compose, fasttext_embedding_service,
        tmp_path, worker_id, settings,
    )

    # Clean clusters
    exit_code = run_cli(monkeypatch, ["clean", "--clusters"])
    assert exit_code == 0
    assert check_clusters_exist(neo4j) == (False, False)

    # Run pipeline - should warn about missing clusters
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    # Check for specific warnings about missing clusters (from print_warning)
    # Warning appears BEFORE analytics, not after (since clusters are recreated)
    output = capsys.readouterr().out
    assert "No user clusters found" in output or "No post clusters found" in output


@pytest.mark.integration
def test_train_full_after_clean_fasttext_state(
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
    """Test that train --full fails for LiteLLM backend (expected behavior).
    
    LiteLLM backends (including test FastText service) don't support training
    from the application side. Training is handled internally by the service.
    """
    # Prepare data with LiteLLM for initial run
    _prepare_data(
        monkeypatch, docker_compose, fasttext_embedding_service,
        tmp_path, worker_id, settings,
    )

    # FastTextState is only created with built-in FastText, so create it manually for testing
    _create_fasttext_state(neo4j)
    assert _count_fasttext_state(neo4j) > 0

    # Clean FastText state
    exit_code = run_cli(monkeypatch, ["clean", "--fasttext-state"])
    assert exit_code == 0
    assert _count_fasttext_state(neo4j) == 0

    # Keep using LiteLLM with test FastText service
    # Training is not supported for LiteLLM backends (expected behavior)
    log_file = tmp_path / "train_test.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,  # Keep using test service
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Train full - should fail for LiteLLM backend (expected)
    exit_code = run_cli(monkeypatch, ["train", "--full"])
    assert exit_code == 1  # Expected to fail - training not supported for LiteLLM

    # FastTextState should remain unchanged (not created)
    assert _count_fasttext_state(neo4j) == 0


@pytest.mark.integration
def test_experiment_workflow(
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
    """Test full experiment workflow: clean clusters, change params, rerun."""
    # Step 1: Load data and run pipeline
    log_file = tmp_path / "experiment.log"
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

    # Verify clusters exist
    users_exist_before, posts_exist_before = check_clusters_exist(neo4j)
    assert users_exist_before
    assert posts_exist_before
    assert _count_user_communities(neo4j) > 0
    assert _count_post_communities(neo4j) > 0

    # Step 2: Clean only clusters (preserving source data)
    exit_code = run_cli(monkeypatch, ["clean", "--clusters"])
    assert exit_code == 0

    # Verify clusters are cleared but source data remains
    users_exist_after_clean, posts_exist_after_clean = check_clusters_exist(neo4j)
    assert not users_exist_after_clean
    assert not posts_exist_after_clean
    # Source data should remain
    user_count = coerce_int(
        next(iter(neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) RETURN count(u) AS count",
            {"user": "User"},
        ))).get("count")
    )
    post_count = coerce_int(
        next(iter(neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) RETURN count(p) AS count",
            {"post": "Post"},
        ))).get("count")
    )
    assert user_count > 0
    assert post_count > 0

    # Step 3: Change clustering parameters via env
    # Use different leiden_resolution to get different clusters
    monkeypatch.setenv("HINTGRID_LEIDEN_RESOLUTION", "0.5")  # Different from default 0.1
    new_settings = test_settings.model_copy(update={"leiden_resolution": 0.5})
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: new_settings)

    # Step 4: Run pipeline again - should recalculate clusters
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    # Step 5: Verify clusters were recalculated
    users_exist_after_rerun, posts_exist_after_rerun = check_clusters_exist(neo4j)
    assert users_exist_after_rerun
    assert posts_exist_after_rerun

    # Clusters may have different counts due to different resolution
    user_communities_after = _count_user_communities(neo4j)
    post_communities_after = _count_post_communities(neo4j)
    # At least clusters should exist
    assert user_communities_after > 0
    assert post_communities_after > 0

    # Interests should be recalculated
    assert check_interests_exist(neo4j)

    # Feeds should be regenerated
    # (not all users may have recommendations)
    redis_raw = cast("RedisTestClient", redis_client)
    feed_counts = [
        redis_raw.zcard(f"feed:home:{user_id}")
        for user_id in sample_data_for_cli["user_ids"]
    ]
    total_feeds = sum(feed_counts)
    assert total_feeds > 0, (
        f"At least some feeds should be created after rerun, got {feed_counts} for users "
        f"{sample_data_for_cli['user_ids']}"
    )
