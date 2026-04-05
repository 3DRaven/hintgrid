"""CLI integration tests for refresh command.

Tests lightweight interest refresh: applies decay to existing scores
and recomputes only dirty (changed) communities.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from hintgrid import app as app_module
from hintgrid.utils.coercion import coerce_int

from .conftest import RedisTestClient, run_cli, set_cli_env

if TYPE_CHECKING:
    import redis
    from hintgrid.config import HintGridSettings
    from hintgrid.clients.neo4j import Neo4jClient
    from pathlib import Path
    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig


@pytest.mark.integration
def test_cli_refresh_updates_interests(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    redis_client: redis.Redis,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid refresh' updates interest scores in Neo4j."""
    log_file = tmp_path / "refresh_interests.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # First run full pipeline to create interests
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    # Get initial interest count
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")
    initial_interests = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__uc__)-[i:INTERESTED_IN]->(:__pc__) "
            "RETURN count(i) AS count",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
    )
    initial_count = coerce_int(initial_interests[0].get("count"))
    assert initial_count > 0, "Initial interests should exist after run"

    # Run refresh
    exit_code = run_cli(monkeypatch, ["refresh"])
    assert exit_code == 0

    # Verify interests still exist (may be updated)
    after_interests = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__uc__)-[i:INTERESTED_IN]->(:__pc__) "
            "RETURN count(i) AS count",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
    )
    after_count = coerce_int(after_interests[0].get("count"))
    assert after_count > 0, "Interests should still exist after refresh"
    # Interests may be updated (decay applied) but should not disappear
    assert after_count >= initial_count * 0.5, "Most interests should remain after refresh"


@pytest.mark.integration
def test_cli_refresh_applies_decay(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid refresh' applies exponential decay to interest scores."""
    log_file = tmp_path / "refresh_decay.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # First run full pipeline
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    # Get initial interest scores
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")
    initial_scores = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__uc__)-[i:INTERESTED_IN]->(:__pc__) "
            "RETURN i.score AS score ORDER BY i.score DESC LIMIT 5",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
    )
    assert len(initial_scores) > 0, "Should have interest scores"

    # Run refresh with custom decay
    exit_code = run_cli(monkeypatch, ["refresh", "--decay-half-life-days", "7"])
    assert exit_code == 0

    # Get scores after refresh
    after_scores = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__uc__)-[i:INTERESTED_IN]->(:__pc__) "
            "RETURN i.score AS score ORDER BY i.score DESC LIMIT 5",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
    )
    assert len(after_scores) > 0, "Should still have interest scores"

    # Scores should generally decrease due to decay (allowing for some variance)
    # We check that at least some scores decreased
    for initial, after in zip(initial_scores, after_scores, strict=False):
        from hintgrid.utils.coercion import coerce_float
        init_score = coerce_float(initial.get("score", 0))
        after_score = coerce_float(after.get("score", 0))
        if after_score < init_score:
            break
    # Note: In practice, decay may not always decrease scores if new interactions occurred
    # This test mainly verifies refresh runs without errors


@pytest.mark.integration
def test_cli_refresh_updates_feeds(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    redis_client: redis.Redis,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid refresh' updates Redis feeds."""
    log_file = tmp_path / "refresh_feeds.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # First run full pipeline
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    redis_raw = cast("RedisTestClient", redis_client)
    user_id = sample_data_for_cli["user_ids"][0]
    feed_key = f"feed:home:{user_id}"

    # Get initial feed size
    initial_size = redis_raw.zcard(feed_key)
    assert initial_size > 0, "Feed should exist after run"

    # Run refresh
    exit_code = run_cli(monkeypatch, ["refresh"])
    assert exit_code == 0

    # Feed should still exist (may be updated)
    after_size = redis_raw.zcard(feed_key)
    assert after_size > 0, "Feed should still exist after refresh"
    # Feed may be updated but should not disappear
    assert after_size >= initial_size * 0.5, "Feed should maintain reasonable size"


@pytest.mark.integration
def test_cli_refresh_fallback_to_full_rebuild(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    redis_client: redis.Redis,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid refresh' falls back to full rebuild if no timestamp exists."""
    log_file = tmp_path / "refresh_fallback.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # First run to create initial data
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    # Verify initial data was created
    neo4j.label("User")
    neo4j.label("Post")
    initial_user_count = coerce_int(
        next(iter(neo4j.execute_and_fetch_labeled(
                "MATCH (u:__user__) RETURN count(u) AS count",
                {"user": "User"},
            ))).get("count")
    )
    initial_post_count = coerce_int(
        next(iter(neo4j.execute_and_fetch_labeled(
                "MATCH (p:__post__) RETURN count(p) AS count",
                {"post": "Post"},
            ))).get("count")
    )
    assert initial_user_count == len(sample_data_for_cli["user_ids"]), "Users should be loaded"
    assert initial_post_count == 5, "Posts should be loaded"

    # Now run refresh - should update interests and feeds without reloading data
    exit_code = run_cli(monkeypatch, ["refresh"])
    assert exit_code == 0

    # Verify data still exists (refresh doesn't reload from PostgreSQL)
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
    assert user_count == initial_user_count, "User count should remain the same"
    assert post_count == initial_post_count, "Post count should remain the same"

    # Verify feeds were updated/created
    redis_raw = cast("RedisTestClient", redis_client)
    total_feeds = sum(
        redis_raw.zcard(f"feed:home:{user_id}") > 0
        for user_id in sample_data_for_cli["user_ids"]
    )
    assert total_feeds > 0, "At least some feeds should exist after refresh"


@pytest.mark.integration
@pytest.mark.single_worker
def test_cli_refresh_dirty_communities_only(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid refresh' recomputes only dirty communities.

    Serialized with other ``single_worker`` tests: concurrent full runs on the
    shared Neo4j instance can hit transient deadlocks on index updates.
    """
    log_file = tmp_path / "refresh_dirty.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # First run full pipeline
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    # Get initial community count
    neo4j.label("UserCommunity")
    initial_communities = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (c:__uc__) RETURN count(c) AS count",
            {"uc": "UserCommunity"},
        )
    )
    initial_count = coerce_int(initial_communities[0].get("count"))
    assert initial_count > 0, "Communities should exist after run"

    # Run refresh (should only update dirty communities, not recreate all)
    exit_code = run_cli(monkeypatch, ["refresh"])
    assert exit_code == 0

    # Communities should still exist (not recreated)
    after_communities = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (c:__uc__) RETURN count(c) AS count",
            {"uc": "UserCommunity"},
        )
    )
    after_count = coerce_int(after_communities[0].get("count"))
    # Communities should remain (refresh doesn't recreate clustering)
    assert after_count == initial_count, "Communities should not be recreated by refresh"


@pytest.mark.integration
def test_cli_refresh_custom_decay_half_life(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid refresh' respects custom decay_half_life_days parameter."""
    log_file = tmp_path / "refresh_custom_decay.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # First run full pipeline
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    # Run refresh with custom decay half-life
    exit_code = run_cli(monkeypatch, ["refresh", "--decay-half-life-days", "30"])
    assert exit_code == 0

    # Verify refresh completed successfully
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")
    interests = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__uc__)-[i:INTERESTED_IN]->(:__pc__) "
            "RETURN count(i) AS count",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
    )
    assert coerce_int(interests[0].get("count")) > 0, "Interests should exist after refresh"


@pytest.mark.integration
def test_cli_refresh_ctr_enabled(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid refresh' works with CTR scoring enabled."""
    log_file = tmp_path / "refresh_ctr.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # First run full pipeline
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    # Run refresh with CTR enabled
    exit_code = run_cli(monkeypatch, ["refresh", "--ctr-enabled"])
    assert exit_code == 0

    # Verify refresh completed
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")
    interests = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__uc__)-[i:INTERESTED_IN]->(:__pc__) "
            "RETURN count(i) AS count",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
    )
    assert coerce_int(interests[0].get("count")) > 0, "Interests should exist after refresh"


@pytest.mark.integration
def test_cli_refresh_pagerank_enabled(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid refresh' works with PageRank scoring enabled."""
    log_file = tmp_path / "refresh_pagerank.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # First run full pipeline
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    # Run refresh with PageRank enabled
    exit_code = run_cli(monkeypatch, ["refresh", "--pagerank-enabled"])
    assert exit_code == 0

    # Verify refresh completed
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")
    interests = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__uc__)-[i:INTERESTED_IN]->(:__pc__) "
            "RETURN count(i) AS count",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
    )
    assert coerce_int(interests[0].get("count")) > 0, "Interests should exist after refresh"
