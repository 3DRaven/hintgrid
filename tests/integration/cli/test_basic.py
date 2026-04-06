"""Basic CLI integration tests: run, export, clean, get-user-id commands."""

from __future__ import annotations

from typing import TYPE_CHECKING, LiteralString, cast

import pytest

from hintgrid import app as app_module
from hintgrid.utils.coercion import coerce_int

from .conftest import RedisTestClient, run_cli, set_cli_env

if TYPE_CHECKING:
    from pathlib import Path

    import redis

    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings
    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig
else:
    from pathlib import Path

    import redis
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings


@pytest.mark.integration
def test_cli_run_writes_redis(
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
    # Explicit runtime use of types
    assert isinstance(redis_client, redis.Redis)
    assert isinstance(neo4j, Neo4jClient)
    assert isinstance(tmp_path, Path)
    """Test that 'hintgrid run' writes feed data to Redis."""
    log_file = tmp_path / "run.log"
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

    redis_raw = cast("RedisTestClient", redis_client)
    # Check that at least some feeds were created
    # (not all users may have recommendations)
    feed_counts = [
        redis_raw.zcard(f"feed:home:{user_id}") for user_id in sample_data_for_cli["user_ids"]
    ]
    total_feeds = sum(feed_counts)
    assert total_feeds > 0, (
        f"At least some feeds should be created, got {feed_counts} for users "
        f"{sample_data_for_cli['user_ids']}"
    )


@pytest.mark.integration
def test_cli_run_dry_run_skips_redis(
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
    """Test that 'hintgrid run --dry-run' skips Redis writes."""
    log_file = tmp_path / "dry_run.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0

    redis_raw = cast("RedisTestClient", redis_client)
    for user_id in sample_data_for_cli["user_ids"]:
        feed_key = f"feed:home:{user_id}"
        assert redis_raw.exists(feed_key) == 0


@pytest.mark.integration
def test_cli_export_only(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test 'hintgrid export' command creates export file."""
    log_file = tmp_path / "export.log"
    export_path = tmp_path / "export.md"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["export", str(export_path), "--user-id", "101"])
    assert exit_code == 0
    assert export_path.exists()
    assert "HintGrid Export" in export_path.read_text(encoding="utf-8")


@pytest.mark.integration
def test_cli_dry_run_then_export_has_graph_no_redis(
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
    """Test dry-run creates graph but skips Redis."""
    log_file = tmp_path / "dry_export.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0

    user_result = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__) RETURN count(u) AS count",
        {"user": "User"},
    )
    user_count = coerce_int((user_result[0] if user_result else {}).get("count"))

    post_result = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) RETURN count(p) AS count",
        {"post": "Post"},
    )
    post_count = coerce_int((post_result[0] if post_result else {}).get("count"))

    user_communities_result = neo4j.execute_and_fetch_labeled(
        "MATCH (:__user__)-[:BELONGS_TO]->(:__uc__) RETURN count(*) AS count",
        {"user": "User", "uc": "UserCommunity"},
    )
    user_communities = (user_communities_result[0] if user_communities_result else {}).get("count")

    post_communities_result = neo4j.execute_and_fetch_labeled(
        "MATCH (:__post__)-[:BELONGS_TO]->(:__pc__) RETURN count(*) AS count",
        {"post": "Post", "pc": "PostCommunity"},
    )
    post_communities = (post_communities_result[0] if post_communities_result else {}).get("count")

    interests_result = neo4j.execute_and_fetch_labeled(
        "MATCH (:__uc__)-[i:INTERESTED_IN]->(:__pc__) RETURN count(i) AS count",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )
    interests = (interests_result[0] if interests_result else {}).get("count")

    clustered_posts_result = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) WHERE p.cluster_id IS NOT NULL RETURN count(p) AS count",
        {"post": "Post"},
    )
    clustered_posts = (clustered_posts_result[0] if clustered_posts_result else {}).get("count")

    assert user_count == len(sample_data_for_cli["user_ids"])
    assert post_count == 5
    assert coerce_int(user_communities) > 0
    assert coerce_int(post_communities) > 0
    assert coerce_int(interests) > 0
    assert coerce_int(clustered_posts) > 0

    redis_raw = cast("RedisTestClient", redis_client)
    for user_id in sample_data_for_cli["user_ids"]:
        feed_key = f"feed:home:{user_id}"
        assert redis_raw.exists(feed_key) == 0


@pytest.mark.integration
def test_cli_run_then_export_includes_feed_rows(
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
    """Test that export includes feed rows after run."""
    log_file = tmp_path / "run_export.log"
    export_path = tmp_path / "run_export.md"
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

    redis_raw = cast("RedisTestClient", redis_client)
    feed_key = "feed:home:101"
    feed_size = int(redis_raw.zcard(feed_key))
    assert feed_size > 0

    exit_code = run_cli(monkeypatch, ["export", str(export_path), "--user-id", "101"])
    assert exit_code == 0
    assert export_path.exists()
    export_text = export_path.read_text(encoding="utf-8")
    assert "HintGrid Export" in export_text
    assert f"### {feed_key}" in export_text
    assert f"- Total posts: {feed_size}" in export_text
    assert "| Post ID | Score | Source | Text Preview |" in export_text

    # Get all entries to verify rank-based scoring
    all_rows = redis_raw.zrevrange(feed_key, 0, -1, withscores=True)
    assert len(all_rows) > 0

    # Extract post IDs and scores
    post_ids = [coerce_int(post_id_raw) for post_id_raw, _ in all_rows]
    max_post_id = max(post_ids)
    total = len(all_rows)
    base = max_post_id * settings.feed_score_multiplier

    # Verify rank-based scoring: redis_score = base + (N - rank)
    # rank 0 = most interesting (first in zrevrange) → highest score
    for rank, (post_id_raw, score) in enumerate(all_rows):
        post_id = coerce_int(post_id_raw)
        expected_score = base + (total - rank)
        assert score == expected_score, (
            f"Post {post_id} at rank {rank}: expected score {expected_score}, got {score}. "
            f"Formula: base={base} + (N={total} - rank={rank}) = {base + (total - rank)}"
        )
        assert f"| {post_id} |" in export_text or f"| {post_id} |" in export_text.replace(".", "")


@pytest.mark.integration
def test_cli_clean_removes_hintgrid_entries(
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
    """Test 'hintgrid clean' removes HintGrid entries from Redis, Neo4j, and model files."""
    log_file = tmp_path / "clean.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )

    # Set up model directory with dummy model files
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    dummy_files = [
        model_dir / "phrases_v1.pkl",
        model_dir / "fasttext_v1.bin",
        model_dir / "fasttext_v1.bin.wv.vectors_ngrams.npy",
        model_dir / "phrases_v2.pkl",
        model_dir / "fasttext_v2.bin",
        model_dir / "fasttext_v2.bin.wv.vectors_ngrams.npy",
    ]
    for f in dummy_files:
        f.write_bytes(b"dummy")

    # Point settings to the temporary model directory
    # Explicit runtime use of HintGridSettings
    assert isinstance(test_settings, HintGridSettings)
    test_settings = test_settings.model_copy(update={"fasttext_model_path": str(model_dir)})
    monkeypatch.setenv("HINTGRID_FASTTEXT_MODEL_PATH", str(model_dir))
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    # Verify model files still exist before clean
    for f in dummy_files:
        assert f.exists()

    feed_key = "feed:home:101"
    redis_raw = cast("RedisTestClient", redis_client)
    redis_raw.zadd(feed_key, {"999": 999})
    assert redis_raw.zcard(feed_key) > 0

    exit_code = run_cli(monkeypatch, ["clean"])
    assert exit_code == 0

    # Verify Redis: HintGrid entries removed, user entry "999" preserved
    # After clean, only Mastodon entries should remain (score == post_id)
    remaining = redis_raw.zrange(feed_key, 0, -1, withscores=True)
    remaining_ids: list[str] = []
    for member, score in remaining:
        member_text = member.decode("utf-8") if isinstance(member, bytes) else str(member)
        remaining_ids.append(member_text)
        member_id = coerce_int(member_text)
        # Mastodon entries: score == post_id (within tolerance)
        # HintGrid entries would have rank-based scores (base + (N - rank)) >> post_id
        assert abs(float(score) - float(member_id)) < 1.0, (
            f"Remaining entry {member_id} should be Mastodon (score≈post_id), "
            f"got score={score}, post_id={member_id}"
        )
    assert "999" in remaining_ids

    # Verify Neo4j: all nodes removed
    match_all = neo4j.match_all_nodes("n")
    # Use string concatenation - match_all_nodes returns a validated string
    # Safe to use directly as it's validated by match_all_nodes
    # Type checker doesn't understand that match_all is safe, so we use cast
    query_template = cast("LiteralString", "MATCH " + match_all + " RETURN count(n) AS count")  # type: ignore[redundant-cast]
    result = neo4j.execute_and_fetch_labeled(
        query_template,
        {"user": "User", "post": "Post", "uc": "UserCommunity", "pc": "PostCommunity"},
    )
    node_count = coerce_int((result[0] if result else {}).get("count"))
    assert node_count == 0

    # Verify model files: all deleted from disk
    for f in dummy_files:
        assert not f.exists(), f"Model file was not deleted: {f.name}"
    # Directory itself should still exist
    assert model_dir.exists()


@pytest.mark.integration
def test_cli_get_user_info(
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
    """Test 'hintgrid get-user-info' command."""
    log_file = tmp_path / "get_user_info.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Full pipeline so Redis home feeds exist for feed preview in get-user-info
    exit_code = run_cli(monkeypatch, ["run"])
    assert exit_code == 0

    exit_code = run_cli(monkeypatch, ["get-user-info", "@alice"])
    assert exit_code == 0
    output = capsys.readouterr().out
    assert "User Information" in output
    assert "101" in output
    assert "@alice" in output or "alice" in output
    assert "Home feed (Redis)" in output
    assert "Redis score (feed)" in output
    assert "Post Information (home feed #1)" in output

    exit_code = run_cli(monkeypatch, ["get-user-info", "@bob@Mastodon.Social"])
    assert exit_code == 0
    output = capsys.readouterr().out
    assert "User Information" in output
    assert "102" in output
    assert "bob" in output.lower()

    exit_code = run_cli(monkeypatch, ["get-user-info", "@missing"])
    assert exit_code == 1
    error_output = capsys.readouterr().err
    assert "User not found" in error_output


@pytest.mark.integration
def test_cli_get_post_info(
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
    """Test 'hintgrid get-post-info' command."""
    log_file = tmp_path / "get_post_info.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0

    exit_code = run_cli(monkeypatch, ["get-post-info", "1"])
    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Post Information" in output
    assert "999999001" in output

    exit_code = run_cli(monkeypatch, ["get-post-info", "999999001"])
    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Post Information" in output

    exit_code = run_cli(
        monkeypatch,
        [
            "get-post-info",
            "https://mastodon.test/users/alice/statuses/999999001",
        ],
    )
    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Post Information" in output

    exit_code = run_cli(monkeypatch, ["get-post-info", "999999999999999999"])
    assert exit_code == 1
    err = capsys.readouterr().err
    assert "not found" in err.lower() or "Post not found" in err


@pytest.mark.integration
def test_cli_run_fasttext_quantize_disabled(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid run' works with FastText quantization disabled."""
    log_file = tmp_path / "fasttext_no_quantize.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["run", "--no-fasttext-quantize"])
    assert exit_code == 0

    # Verify pipeline completed successfully
    user_result = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__) RETURN count(u) AS count",
        {"user": "User"},
    )
    user_count = coerce_int((user_result[0] if user_result else {}).get("count"))

    post_result = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) RETURN count(p) AS count",
        {"post": "Post"},
    )
    post_count = coerce_int((post_result[0] if post_result else {}).get("count"))

    assert user_count == len(sample_data_for_cli["user_ids"])
    assert post_count == 5


@pytest.mark.integration
def test_cli_run_fasttext_custom_vector_size(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid run' works with custom FastText vector size."""
    log_file = tmp_path / "fasttext_vector_size.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # qdim must be <= vector_size and divide vector_size (PQ) when quantize is on
    exit_code = run_cli(
        monkeypatch,
        ["run", "--fasttext-vector-size", "64", "--fasttext-quantize-qdim", "64"],
    )
    assert exit_code == 0

    # Verify pipeline completed successfully
    user_result = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__) RETURN count(u) AS count",
        {"user": "User"},
    )
    user_count = coerce_int((user_result[0] if user_result else {}).get("count"))

    post_result = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) RETURN count(p) AS count",
        {"post": "Post"},
    )
    post_count = coerce_int((post_result[0] if post_result else {}).get("count"))

    assert user_count == len(sample_data_for_cli["user_ids"])
    assert post_count == 5


@pytest.mark.integration
def test_cli_run_fasttext_min_documents_threshold(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid run' respects FastText min_documents threshold."""
    log_file = tmp_path / "fasttext_min_docs.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Use low threshold to ensure training happens
    exit_code = run_cli(monkeypatch, ["run", "--fasttext-min-documents", "2"])
    assert exit_code == 0

    # Verify pipeline completed successfully
    user_result = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__) RETURN count(u) AS count",
        {"user": "User"},
    )
    user_count = coerce_int((user_result[0] if user_result else {}).get("count"))

    post_result = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) RETURN count(p) AS count",
        {"post": "Post"},
    )
    post_count = coerce_int((post_result[0] if post_result else {}).get("count"))

    assert user_count == len(sample_data_for_cli["user_ids"])
    assert post_count == 5


@pytest.mark.integration
def test_cli_run_ctr_scoring_enabled(
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
    """Test that 'hintgrid run' works with CTR scoring enabled."""
    log_file = tmp_path / "ctr_enabled.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Use --feed-force-refresh to ensure feeds are generated for all active users
    exit_code = run_cli(
        monkeypatch,
        ["run", "--ctr-enabled", "--ctr-weight", "0.5", "--feed-force-refresh"],
    )
    assert exit_code == 0

    # Verify feeds were created
    # Only local users (domain IS NULL) get feeds generated
    # From sample_data_for_cli: 101 is local, 102 and 103 are remote
    redis_raw = cast("RedisTestClient", redis_client)
    local_user_id = 101  # Only local user in sample_data_for_cli
    feed_key = f"feed:home:{local_user_id}"
    assert redis_raw.zcard(feed_key) > 0, (
        f"Feed {feed_key} should have entries. "
        f"User {local_user_id} is local and should have a feed."
    )


@pytest.mark.integration
def test_cli_run_pagerank_scoring_enabled(
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
    """Test that 'hintgrid run' works with PageRank scoring enabled."""
    log_file = tmp_path / "pagerank_enabled.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Use --feed-force-refresh to ensure feeds are generated for all active users
    exit_code = run_cli(
        monkeypatch,
        ["run", "--pagerank-enabled", "--pagerank-weight", "0.1", "--feed-force-refresh"],
    )
    assert exit_code == 0

    # Verify feeds were created
    # Only local users (domain IS NULL) get feeds generated
    # From sample_data_for_cli: 101 is local, 102 and 103 are remote
    redis_raw = cast("RedisTestClient", redis_client)
    local_user_id = 101  # Only local user in sample_data_for_cli
    feed_key = f"feed:home:{local_user_id}"
    assert redis_raw.zcard(feed_key) > 0, (
        f"Feed {feed_key} should have entries. "
        f"User {local_user_id} is local and should have a feed."
    )


@pytest.mark.integration
def test_cli_run_bookmark_weight_impact(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid run' respects bookmark_weight parameter."""
    log_file = tmp_path / "bookmark_weight.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["run", "--bookmark-weight", "3.0"])
    assert exit_code == 0

    # Verify interests were created
    interests = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__uc__)-[i:INTERESTED_IN]->(:__pc__) RETURN count(i) AS count",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
    )
    assert coerce_int(interests[0].get("count")) > 0


@pytest.mark.integration
def test_cli_run_personalized_weights_sum_to_one(
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
    """Test that 'hintgrid run' works with personalized weights that sum to 1.0."""
    log_file = tmp_path / "personalized_weights.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Set weights that sum to 1.0: 0.5 + 0.3 + 0.2 = 1.0
    exit_code = run_cli(
        monkeypatch,
        [
            "run",
            "--personalized-interest-weight",
            "0.5",
            "--personalized-popularity-weight",
            "0.3",
            "--personalized-recency-weight",
            "0.2",
            "--feed-force-refresh",
        ],
    )
    assert exit_code == 0

    # Verify feeds were created
    # Only local users (domain IS NULL) get feeds generated
    redis_raw = cast("RedisTestClient", redis_client)
    local_user_id = 101  # Only local user in sample_data_for_cli
    feed_key = f"feed:home:{local_user_id}"
    assert redis_raw.zcard(feed_key) > 0, (
        f"Feed {feed_key} should have entries. "
        f"User {local_user_id} is local and should have a feed."
    )


@pytest.mark.integration
def test_cli_run_feed_force_refresh(
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
    """Test that 'hintgrid run --feed-force-refresh' forces refresh of all feeds."""
    log_file = tmp_path / "feed_force_refresh.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # First run
    exit_code = run_cli(monkeypatch, ["run", "--feed-force-refresh"])
    assert exit_code == 0

    # Run again with force refresh
    exit_code = run_cli(monkeypatch, ["run", "--feed-force-refresh"])
    assert exit_code == 0

    # Verify feeds still exist
    # Only local users (domain IS NULL) get feeds generated
    redis_raw = cast("RedisTestClient", redis_client)
    local_user_id = 101  # Only local user in sample_data_for_cli
    feed_key = f"feed:home:{local_user_id}"
    assert redis_raw.zcard(feed_key) > 0, (
        f"Feed {feed_key} should have entries. "
        f"User {local_user_id} is local and should have a feed."
    )


@pytest.mark.integration
def test_cli_run_active_user_days_filter(
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
    """Test that 'hintgrid run' respects active_user_days parameter."""
    log_file = tmp_path / "active_user_days.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Use large window to include all users
    exit_code = run_cli(
        monkeypatch,
        ["run", "--active-user-days", "365", "--feed-force-refresh"],
    )
    assert exit_code == 0

    # Verify feeds were created for active users
    # Only local users (domain IS NULL) get feeds generated
    redis_raw = cast("RedisTestClient", redis_client)
    local_user_id = 101  # Only local user in sample_data_for_cli
    feed_key = f"feed:home:{local_user_id}"
    assert redis_raw.zcard(feed_key) > 0, (
        f"Feed {feed_key} should have entries. "
        f"User {local_user_id} is local and should have a feed."
    )


@pytest.mark.integration
def test_cli_run_language_match_weight(
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
    """Test that 'hintgrid run' respects language_match_weight parameter."""
    log_file = tmp_path / "language_match_weight.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(
        monkeypatch,
        ["run", "--language-match-weight", "0.5", "--feed-force-refresh"],
    )
    assert exit_code == 0

    # Verify feeds were created
    # Only local users (domain IS NULL) get feeds generated
    redis_raw = cast("RedisTestClient", redis_client)
    local_user_id = 101  # Only local user in sample_data_for_cli
    feed_key = f"feed:home:{local_user_id}"
    assert redis_raw.zcard(feed_key) > 0, (
        f"Feed {feed_key} should have entries. "
        f"User {local_user_id} is local and should have a feed."
    )


@pytest.mark.integration
def test_cli_run_feed_days_window(
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
    """Test that 'hintgrid run' respects feed_days window parameter."""
    log_file = tmp_path / "feed_days.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Use different feed_days values
    exit_code = run_cli(
        monkeypatch,
        ["run", "--feed-days", "14", "--feed-force-refresh"],
    )
    assert exit_code == 0

    # Verify feeds were created
    # Only local users (domain IS NULL) get feeds generated
    redis_raw = cast("RedisTestClient", redis_client)
    local_user_id = 101  # Only local user in sample_data_for_cli
    feed_key = f"feed:home:{local_user_id}"
    assert redis_raw.zcard(feed_key) > 0, (
        f"Feed {feed_key} should have entries. "
        f"User {local_user_id} is local and should have a feed."
    )


@pytest.mark.integration
def test_cli_run_parallel_feed_workers(
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
    """Test that 'hintgrid run' works with parallel feed workers."""
    log_file = tmp_path / "feed_workers.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(
        monkeypatch,
        ["run", "--feed-workers", "2", "--feed-force-refresh"],
    )
    assert exit_code == 0

    # Verify feeds were created
    # Only local users (domain IS NULL) get feeds generated
    redis_raw = cast("RedisTestClient", redis_client)
    local_user_id = 101  # Only local user in sample_data_for_cli
    feed_key = f"feed:home:{local_user_id}"
    assert redis_raw.zcard(feed_key) > 0, (
        f"Feed {feed_key} should have entries. "
        f"User {local_user_id} is local and should have a feed."
    )


@pytest.mark.integration
def test_cli_run_parallel_loader_workers(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid run' works with parallel loader workers."""
    log_file = tmp_path / "loader_workers.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["run", "--loader-workers", "2"])
    assert exit_code == 0

    # Verify data was loaded
    user_result = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__) RETURN count(u) AS count",
        {"user": "User"},
    )
    user_count = coerce_int((user_result[0] if user_result else {}).get("count"))

    post_result = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) RETURN count(p) AS count",
        {"post": "Post"},
    )
    post_count = coerce_int((post_result[0] if post_result else {}).get("count"))

    assert user_count == len(sample_data_for_cli["user_ids"])
    assert post_count == 5


@pytest.mark.integration
def test_cli_run_custom_batch_size(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid run' respects custom batch_size parameter."""
    log_file = tmp_path / "batch_size.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["run", "--batch-size", "5000"])
    assert exit_code == 0

    # Verify data was loaded
    user_result = neo4j.execute_and_fetch_labeled(
        "MATCH (u:__user__) RETURN count(u) AS count",
        {"user": "User"},
    )
    user_count = coerce_int((user_result[0] if user_result else {}).get("count"))

    post_result = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) RETURN count(p) AS count",
        {"post": "Post"},
    )
    post_count = coerce_int((post_result[0] if post_result else {}).get("count"))

    assert user_count == len(sample_data_for_cli["user_ids"])
    assert post_count == 5


@pytest.mark.integration
def test_cli_run_public_feed_enabled(
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
    """Test that 'hintgrid run' fills public timelines when enabled."""
    log_file = tmp_path / "public_feed_enabled.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["run", "--public-feed-enabled"])
    assert exit_code == 0

    # Verify public timeline was created
    redis_raw = cast("RedisTestClient", redis_client)
    public_key = "timeline:public"
    assert redis_raw.zcard(public_key) > 0


@pytest.mark.integration
def test_cli_run_public_feed_strategy_local(
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
    """Test that 'hintgrid run' works with local_communities strategy for public feeds."""
    log_file = tmp_path / "public_feed_local.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(
        monkeypatch,
        ["run", "--public-feed-enabled", "--public-feed-strategy", "local_communities"],
    )
    assert exit_code == 0

    # Verify public timeline was created
    redis_raw = cast("RedisTestClient", redis_client)
    public_key = "timeline:public"
    assert redis_raw.zcard(public_key) > 0


@pytest.mark.integration
def test_cli_run_public_feed_strategy_all(
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
    """Test that 'hintgrid run' works with all_communities strategy for public feeds."""
    log_file = tmp_path / "public_feed_all.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(
        monkeypatch,
        ["run", "--public-feed-enabled", "--public-feed-strategy", "all_communities"],
    )
    assert exit_code == 0

    # Verify public timeline was created
    redis_raw = cast("RedisTestClient", redis_client)
    public_key = "timeline:public"
    assert redis_raw.zcard(public_key) > 0


@pytest.mark.integration
def test_cli_run_prune_partial_threshold(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid run' works with partial pruning threshold."""
    log_file = tmp_path / "prune_partial.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(
        monkeypatch,
        [
            "run",
            "--similarity-pruning",
            "partial",
            "--prune-similarity-threshold",
            "0.85",
        ],
    )
    assert exit_code == 0

    # Verify clustering completed
    communities = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (c:__pc__) RETURN count(c) AS count",
            {"pc": "PostCommunity"},
        )
    )
    assert coerce_int(communities[0].get("count")) > 0


@pytest.mark.integration
def test_cli_run_prune_temporal_days(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid run' works with temporal pruning."""
    log_file = tmp_path / "prune_temporal.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(
        monkeypatch,
        [
            "run",
            "--similarity-pruning",
            "temporal",
            "--prune-days",
            "14",
        ],
    )
    assert exit_code == 0

    # Verify clustering completed
    communities = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (c:__pc__) RETURN count(c) AS count",
            {"pc": "PostCommunity"},
        )
    )
    assert coerce_int(communities[0].get("count")) > 0


@pytest.mark.integration
def test_cli_run_similarity_recency_window(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid run' respects similarity_recency_days window."""
    log_file = tmp_path / "similarity_recency.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["run", "--similarity-recency-days", "14"])
    assert exit_code == 0

    # Verify that SIMILAR_TO relationships respect recency window
    # Only posts within 14 days should have SIMILAR_TO relationships
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p1:__post__)-[r:SIMILAR_TO]->(p2:__post__) "
            "WHERE p1.createdAt <= datetime() - duration({days: 14}) "
            "OR p2.createdAt <= datetime() - duration({days: 14}) "
            "RETURN count(r) AS old_posts_count",
            {"post": "Post"},
        )
    )
    old_posts_count = coerce_int(result[0].get("old_posts_count")) if result else 0
    assert old_posts_count == 0, (
        f"SIMILAR_TO relationships should only exist for posts within 14 days, "
        f"but found {old_posts_count} relationships involving older posts"
    )
