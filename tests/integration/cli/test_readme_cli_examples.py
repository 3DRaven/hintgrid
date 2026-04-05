"""Verify README CLI examples run successfully."""

from __future__ import annotations

import shlex
from typing import TYPE_CHECKING

import pytest

from hintgrid import app as app_module
from hintgrid.utils.coercion import coerce_int

from tests.integration.cli.conftest import set_cli_env

# Fixtures setup_mastodon_schema_for_cli and sample_data_for_cli are defined in conftest.py

if TYPE_CHECKING:
    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig
    from hintgrid.config import HintGridSettings
    from hintgrid.clients.neo4j import Neo4jClient
    from pathlib import Path
    import redis


# Expected test data counts (must match sample_data_for_cli fixture in conftest.py)
EXPECTED_USER_COUNT = 3  # alice, bob, carol
EXPECTED_POST_COUNT = 5  # 6 total minus 1 deleted
EXPECTED_FAVOURITE_COUNT = 5
EXPECTED_FOLLOW_COUNT = 3


def _example_commands() -> dict[str, str]:
    return {
        "full_run": (
            "hintgrid run "
            "--postgres-host localhost "
            "--postgres-port 5432 "
            "--postgres-database mastodon_production "
            "--postgres-user mastodon "
            '--postgres-password "" '
            "--neo4j-host localhost "
            "--neo4j-port 7687 "
            "--neo4j-username neo4j "
            "--neo4j-password password "
            "--redis-host localhost "
            "--redis-port 6379 "
            "--redis-db 0 "
            '--redis-password "" '
            "--llm-provider ollama "
            "--llm-base-url http://localhost:11434 "
            "--llm-model nomic-embed-text "
            "--llm-dimensions 768 "
            "--llm-timeout 30 "
            "--llm-max-retries 3 "
            '--llm-api-key "" '
            "--batch-size 10000 "
            "--max-retries 3 "
            "--checkpoint-interval 1000 "
            "--user-communities dynamic "
            "--post-communities dynamic "
            "--leiden-resolution 1.0 "
            "--leiden-max-levels 10 "
            "--knn-neighbors 5 "
            "--knn-self-neighbor-offset 1 "
            "--similarity-threshold 0.85 "
            "--similarity-recency-days 7 "
            "--similarity-pruning aggressive "
            "--prune-after-clustering "
            "--prune-similarity-threshold 0.9 "
            "--prune-days 30 "
            "--interests-ttl-days 30 "
            "--interests-min-favourites 5 "
            "--likes-weight 1.0 "
            "--reblogs-weight 1.5 "
            "--replies-weight 3.0 "
            "--serendipity-probability 0.1 "
            "--serendipity-limit 100 "
            "--serendipity-score 0.1 "
            "--serendipity-based-on 0 "
            "--feed-size 500 "
            "--feed-days 7 "
            "--feed-ttl none "
            "--feed-score-multiplier 2 "
            "--feed-score-decimals 4 "
            "--personalized-interest-weight 0.5 "
            "--personalized-popularity-weight 0.3 "
            "--personalized-recency-weight 0.2 "
            "--cold-start-popularity-weight 0.7 "
            "--cold-start-recency-weight 0.3 "
            "--popularity-smoothing 1 "
            "--recency-smoothing 1 "
            "--recency-numerator 1.0 "
            "--cold-start-fallback global_top "
            "--cold-start-limit 500 "
            "--export-max-items 50 "
            "--text-preview-limit 60 "
            "--community-interest-limit 30 "
            "--community-member-sample 5 "
            "--community-sample-limit 5 "
            "--graph-sample-limit 10 "
            "--pg-pool-min-size 1 "
            "--pg-pool-max-size 5 "
            "--pg-pool-timeout-seconds 30 "
            "--neo4j-ready-retries 30 "
            "--neo4j-ready-sleep-seconds 1 "
            "--redis-score-tolerance 1e-06 "
            "--mastodon-public-visibility 0 "
            "--mastodon-account-lookup-limit 1 "
            "--log-level INFO "
            "--log-file hintgrid.log"
        ),
        "dry_run": "hintgrid run --dry-run",
        # --load-since for time-windowed loading
        "run_load_since": "hintgrid run --load-since 30d",
        "run_load_since_dry": "hintgrid run --dry-run --load-since 7d",
        # --user-id for single user processing
        "run_user": "hintgrid run --user-id 101",
        # --user-id with --dry-run
        "run_user_dry": "hintgrid run --dry-run --user-id 101",
        # --user-id is required for export command
        "export": "hintgrid export hintgrid_state.md --user-id 101",
        "export_user": "hintgrid export user_123_state.md --user-id 123",
        "clean": "hintgrid clean",
        "get_user_info": "hintgrid get-user-info @username@mastodon.social",
    }


def _parse_args(command: str) -> list[str]:
    parts = shlex.split(command)
    if not parts or parts[0] != "hintgrid":
        raise AssertionError(f"Unexpected command: {command}")
    return parts[1:]


def _replace_arg(args: list[str], option: str, value: str) -> None:
    if option not in args:
        raise AssertionError(f"Option {option} not found in args: {args}")
    index = args.index(option)
    if index + 1 >= len(args):
        raise AssertionError(f"Option {option} has no value in args: {args}")
    args[index + 1] = value


def _run_cli(monkeypatch: pytest.MonkeyPatch, args: list[str]) -> int:
    """Run CLI and return exit code.

    Typer raises SystemExit with the exit code, so we catch it.
    """
    monkeypatch.setattr("sys.argv", ["hintgrid", *args])
    try:
        app_module.main()
        return 0  # No SystemExit means success
    except SystemExit as exc:
        return int(exc.code) if exc.code is not None else 0


@pytest.mark.integration
def test_readme_full_cli_command_runs(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    settings: HintGridSettings,
) -> None:
    log_file = tmp_path / "readme_full.log"
    test_settings = set_cli_env(monkeypatch, docker_compose, fasttext_embedding_service, log_file, settings=settings)
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    full_cmd = _example_commands()["full_run"]
    args = _parse_args(full_cmd)

    _replace_arg(args, "--postgres-host", docker_compose.postgres_host)
    _replace_arg(args, "--postgres-port", str(docker_compose.postgres_port))
    _replace_arg(args, "--postgres-database", docker_compose.postgres_db)
    _replace_arg(args, "--postgres-user", docker_compose.postgres_user)
    _replace_arg(args, "--postgres-password", docker_compose.postgres_password)

    _replace_arg(args, "--neo4j-host", docker_compose.neo4j_host)
    _replace_arg(args, "--neo4j-port", str(docker_compose.neo4j_port))
    _replace_arg(args, "--neo4j-username", docker_compose.neo4j_user)
    _replace_arg(args, "--neo4j-password", docker_compose.neo4j_password)

    _replace_arg(args, "--redis-host", docker_compose.redis_host)
    _replace_arg(args, "--redis-port", str(docker_compose.redis_port))
    _replace_arg(args, "--redis-db", "0")
    _replace_arg(args, "--redis-password", "")

    _replace_arg(args, "--llm-provider", "openai")
    _replace_arg(args, "--llm-base-url", fasttext_embedding_service["api_base"])
    _replace_arg(args, "--llm-model", fasttext_embedding_service["model"])
    _replace_arg(args, "--llm-api-key", "sk-fake-key-for-testing")
    # Set dimensions to match FastText service (128)
    _replace_arg(args, "--llm-dimensions", "128")

    _replace_arg(args, "--log-file", str(log_file))

    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0


@pytest.mark.integration
def test_readme_dry_run_command_runs(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    settings: HintGridSettings,
) -> None:
    log_file = tmp_path / "readme_dry_run.log"
    test_settings = set_cli_env(monkeypatch, docker_compose, fasttext_embedding_service, log_file, settings=settings)
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    args = _parse_args(_example_commands()["dry_run"])

    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0


@pytest.mark.integration
def test_readme_run_load_since_command_runs(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    neo4j: Neo4jClient,
    tmp_path: Path,
    settings: HintGridSettings,
) -> None:
    """Test run --load-since command for time-windowed loading."""
    log_file = tmp_path / "readme_run_load_since.log"
    test_settings = set_cli_env(monkeypatch, docker_compose, fasttext_embedding_service, log_file, settings=settings)
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    args = _parse_args(_example_commands()["run_load_since"])

    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0


@pytest.mark.integration
def test_readme_run_load_since_dry_command_runs(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    neo4j: Neo4jClient,
    tmp_path: Path,
    settings: HintGridSettings,
) -> None:
    """Test run --dry-run --load-since command."""
    log_file = tmp_path / "readme_run_load_since_dry.log"
    test_settings = set_cli_env(monkeypatch, docker_compose, fasttext_embedding_service, log_file, settings=settings)
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    args = _parse_args(_example_commands()["run_load_since_dry"])

    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0


@pytest.mark.integration
def test_readme_run_user_command_runs(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    neo4j: Neo4jClient,
    tmp_path: Path,
    settings: HintGridSettings,
) -> None:
    """Test run --user-id command for single user processing."""
    log_file = tmp_path / "readme_run_user.log"
    test_settings = set_cli_env(monkeypatch, docker_compose, fasttext_embedding_service, log_file, settings=settings)
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    args = _parse_args(_example_commands()["run_user"])

    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0


@pytest.mark.integration
def test_readme_run_user_dry_command_runs(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    neo4j: Neo4jClient,
    tmp_path: Path,
    settings: HintGridSettings,
) -> None:
    """Test run --dry-run --user-id command for single user dry run."""
    log_file = tmp_path / "readme_run_user_dry.log"
    test_settings = set_cli_env(monkeypatch, docker_compose, fasttext_embedding_service, log_file, settings=settings)
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    args = _parse_args(_example_commands()["run_user_dry"])

    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0


@pytest.mark.integration
def test_readme_export_command_runs(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    neo4j: Neo4jClient,
    tmp_path: Path,
    settings: HintGridSettings,
) -> None:
    log_file = tmp_path / "readme_export.log"
    test_settings = set_cli_env(monkeypatch, docker_compose, fasttext_embedding_service, log_file, settings=settings)
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    args = _parse_args(_example_commands()["export"])
    args[1] = str(tmp_path / "export.md")

    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0


@pytest.mark.integration
def test_readme_export_user_command_runs(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    neo4j: Neo4jClient,
    tmp_path: Path,
    settings: HintGridSettings,
) -> None:
    log_file = tmp_path / "readme_export_user.log"
    test_settings = set_cli_env(monkeypatch, docker_compose, fasttext_embedding_service, log_file, settings=settings)
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    args = _parse_args(_example_commands()["export_user"])
    args[1] = str(tmp_path / "export_user.md")
    _replace_arg(args, "--user-id", "101")

    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0


@pytest.mark.integration
def test_readme_clean_command_runs(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    neo4j: Neo4jClient,
    tmp_path: Path,
    settings: HintGridSettings,
) -> None:
    log_file = tmp_path / "readme_clean.log"
    test_settings = set_cli_env(monkeypatch, docker_compose, fasttext_embedding_service, log_file, settings=settings)
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    args = _parse_args(_example_commands()["clean"])

    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0


@pytest.mark.integration
def test_readme_get_user_info_command_runs(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    neo4j: Neo4jClient,
    tmp_path: Path,
    settings: HintGridSettings,
    capsys: pytest.CaptureFixture[str],
) -> None:
    log_file = tmp_path / "readme_get_user_info.log"
    test_settings = set_cli_env(monkeypatch, docker_compose, fasttext_embedding_service, log_file, settings=settings)
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Load data from PostgreSQL into Neo4j via pipeline
    exit_code = _run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0
    capsys.readouterr()

    args = _parse_args(_example_commands()["get_user_info"])
    args[1] = "@alice"

    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0
    output = capsys.readouterr().out
    assert "User Information" in output
    assert "101" in output


# =============================================================================
# README Test Scenarios
# =============================================================================
# Full workflow scenarios as documented in README.md


@pytest.mark.integration
def test_scenario_single_user_testing(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    neo4j: Neo4jClient,
    tmp_path: Path,
    settings: HintGridSettings,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test scenario: Single user testing workflow from README.

    Workflow:
    1. get-user-info @alice -> shows user information table
    2. run --dry-run --user-id 101
    3. export user_101_state.md --user-id 101
    """
    log_file = tmp_path / "scenario_single_user.log"
    test_settings = set_cli_env(monkeypatch, docker_compose, fasttext_embedding_service, log_file, settings=settings)
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Pre-step: Load data from PostgreSQL into Neo4j via pipeline
    exit_code = _run_cli(monkeypatch, ["run", "--dry-run"])
    assert exit_code == 0
    capsys.readouterr()

    # Step 1: get-user-info
    args = ["get-user-info", "@alice"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0
    output = capsys.readouterr().out
    assert "User Information" in output
    assert "101" in output
    # Extract user_id from output for next step (we know it's 101 from test data)
    user_id = "101"

    # Step 2: dry-run for single user
    args = ["run", "--dry-run", "--user-id", user_id]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0

    # Step 3: export user state
    export_file = tmp_path / "user_101_state.md"
    args = ["export", str(export_file), "--user-id", user_id]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0

    # Verify export file exists and has content
    assert export_file.exists()
    content = export_file.read_text()
    assert len(content) > 0


@pytest.mark.integration
def test_scenario_all_users_testing(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    neo4j: Neo4jClient,
    tmp_path: Path,
    settings: HintGridSettings,
) -> None:
    """Test scenario: All users testing workflow from README.

    Workflow:
    1. run --dry-run (all users)
    2. export user_101_state.md --user-id 101
    3. export user_102_state.md --user-id 102
    """
    log_file = tmp_path / "scenario_all_users.log"
    test_settings = set_cli_env(monkeypatch, docker_compose, fasttext_embedding_service, log_file, settings=settings)
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Step 1: dry-run for all users
    args = ["run", "--dry-run"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0

    # Step 2: export first user
    export_file_101 = tmp_path / "user_101_state.md"
    args = ["export", str(export_file_101), "--user-id", "101"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0
    assert export_file_101.exists()

    # Step 3: export second user
    export_file_102 = tmp_path / "user_102_state.md"
    args = ["export", str(export_file_102), "--user-id", "102"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0
    assert export_file_102.exists()


@pytest.mark.integration
def test_scenario_incremental_runs(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    neo4j: Neo4jClient,
    tmp_path: Path,
    settings: HintGridSettings,
) -> None:
    """Test scenario: Incremental runs workflow from README.

    Workflow:
    1. run --dry-run (first run - full processing)
    2. run --dry-run (second run - incremental, only new data)
    3. export user_101_state.md --user-id 101

    Verifies that:
    - First run loads all expected data
    - Second run is truly incremental (no duplicate nodes created)
    - State is properly maintained between runs
    """
    log_file = tmp_path / "scenario_incremental.log"
    test_settings = set_cli_env(monkeypatch, docker_compose, fasttext_embedding_service, log_file, settings=settings)
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Step 1: first dry-run (full processing)
    args = ["run", "--dry-run"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0

    # Use worker-isolated labels for all queries
    neo4j.label("User")
    neo4j.label("Post")
    neo4j.label("AppState")

    # Verify first run loaded expected data (worker-isolated)
    user_count_after_first = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) RETURN count(u) AS cnt",
            {"user": "User"},
        )
    )
    post_count_after_first = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) RETURN count(p) AS cnt",
            {"post": "Post"},
        )
    )
    assert coerce_int(user_count_after_first[0].get("cnt")) == EXPECTED_USER_COUNT
    assert coerce_int(post_count_after_first[0].get("cnt")) == EXPECTED_POST_COUNT

    # Capture state after first run for incremental verification (worker-isolated)
    state_after_first = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (s:__app_state__ {id: 'main'}) RETURN s.last_processed_status_id AS last_status_id",
            {"app_state": "AppState"},
        )
    )
    last_status_id_after_first = coerce_int(state_after_first[0].get("last_status_id"))
    assert last_status_id_after_first > 0, "State should be updated after first run"

    # Step 2: second dry-run (incremental - should not create duplicates)
    args = ["run", "--dry-run"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0

    # Verify no duplicate nodes were created (incremental run, worker-isolated)
    user_count_after_second = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) RETURN count(u) AS cnt",
            {"user": "User"},
        )
    )
    post_count_after_second = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) RETURN count(p) AS cnt",
            {"post": "Post"},
        )
    )
    assert coerce_int(user_count_after_second[0].get("cnt")) == EXPECTED_USER_COUNT, (
        "Incremental run should not create duplicate users"
    )
    assert coerce_int(post_count_after_second[0].get("cnt")) == EXPECTED_POST_COUNT, (
        "Incremental run should not create duplicate posts"
    )

    # Verify state is unchanged (no new data to process, worker-isolated)
    state_after_second = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (s:__app_state__ {id: 'main'}) RETURN s.last_processed_status_id AS last_status_id",
            {"app_state": "AppState"},
        )
    )
    last_status_id_after_second = coerce_int(state_after_second[0].get("last_status_id"))
    assert last_status_id_after_second == last_status_id_after_first, (
        "State should be unchanged when no new data is available"
    )

    # Step 3: export result
    export_file = tmp_path / "user_101_state.md"
    args = ["export", str(export_file), "--user-id", "101"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0
    assert export_file.exists()
    content = export_file.read_text()
    assert len(content) > 0, "Export file should not be empty"
    assert "User ID: 101" in content or "101" in content, "Export should contain user information"


@pytest.mark.integration
def test_scenario_clean_and_retry(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    neo4j: Neo4jClient,
    redis_client: redis.Redis,
    tmp_path: Path,
    settings: HintGridSettings,
) -> None:
    """Test scenario: Clean and retry workflow from README.

    Workflow:
    1. clean (clear Neo4j and Redis)
    2. run --dry-run (fresh start)
    3. export user_101_state.md --user-id 101

    Verifies that:
    - Clean operation actually removes all graph data
    - Fresh run loads all expected data correctly
    """
    log_file = tmp_path / "scenario_clean_retry.log"
    test_settings = set_cli_env(monkeypatch, docker_compose, fasttext_embedding_service, log_file, settings=settings)
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # First run some data to have something to clean
    args = ["run", "--dry-run"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0

    # Use worker-isolated labels for all queries
    neo4j.label("User")
    neo4j.label("Post")
    # Verify data exists before clean (worker-isolated)
    nodes_before_clean = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (n:__worker__) RETURN count(n) AS cnt",
            label_map={"worker": neo4j.worker_label} if neo4j.worker_label else {},
        )
    )
    assert coerce_int(nodes_before_clean[0].get("cnt")) > 0, (
        "Should have data in Neo4j before clean"
    )

    # Step 1: clean
    args = ["clean"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0

    # Verify clean actually removed data (except AppState which is re-initialized, worker-isolated)
    user_nodes_after_clean = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) RETURN count(u) AS cnt",
            {"user": "User"},
        )
    )
    post_nodes_after_clean = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) RETURN count(p) AS cnt",
            {"post": "Post"},
        )
    )
    assert coerce_int(user_nodes_after_clean[0].get("cnt")) == 0, (
        "Clean should remove all User nodes"
    )
    assert coerce_int(post_nodes_after_clean[0].get("cnt")) == 0, (
        "Clean should remove all Post nodes"
    )

    # Step 2: fresh dry-run
    args = ["run", "--dry-run"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0

    # Verify fresh run loaded expected data (worker-isolated)
    user_count_after_run = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) RETURN count(u) AS cnt",
            {"user": "User"},
        )
    )
    post_count_after_run = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) RETURN count(p) AS cnt",
            {"post": "Post"},
        )
    )
    assert coerce_int(user_count_after_run[0].get("cnt")) == EXPECTED_USER_COUNT
    assert coerce_int(post_count_after_run[0].get("cnt")) == EXPECTED_POST_COUNT

    # Step 3: export result
    export_file = tmp_path / "user_101_state.md"
    args = ["export", str(export_file), "--user-id", "101"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0
    assert export_file.exists()
    content = export_file.read_text()
    assert len(content) > 0, "Export file should not be empty"


@pytest.mark.integration
def test_scenario_load_since_window(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    neo4j: Neo4jClient,
    tmp_path: Path,
    settings: HintGridSettings,
) -> None:
    """Test scenario: Load data with time window from README.

    Workflow:
    1. clean (fresh start)
    2. run --load-since 30d (load only last 30 days of data)
    3. run --load-since 7d (run again with smaller window - ignores previous state)
    4. export user_101_state.md --user-id 101
    """
    log_file = tmp_path / "scenario_load_since.log"
    test_settings = set_cli_env(monkeypatch, docker_compose, fasttext_embedding_service, log_file, settings=settings)
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Step 1: clean
    args = ["clean"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0

    # Step 2: run with 30d window
    args = ["run", "--load-since", "30d"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0

    # Step 3: run with 7d window (ignores incremental state)
    args = ["run", "--load-since", "7d"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0

    # Step 4: export result
    export_file = tmp_path / "user_101_state.md"
    args = ["export", str(export_file), "--user-id", "101"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0
    assert export_file.exists()


@pytest.mark.integration
def test_scenario_full_experiment_cycle(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    neo4j: Neo4jClient,
    tmp_path: Path,
    settings: HintGridSettings,
) -> None:
    """Test scenario: Full experiment cycle from README.

    Workflow:
    1. clean
    2. run --dry-run with experimental params
    3. export experiment_v1.md --user-id 101
    4. clean
    5. run --dry-run with different params
    6. export experiment_v2.md --user-id 101
    7. Both exports should exist (for comparison)

    Note: We use GDS-stable leiden-resolution values (0.5, 1.0) because
    Neo4j GDS Leiden has known issues with certain resolution values
    on small test graphs (NullPointerException in dendrogramManager).
    Production systems with larger graphs typically don't hit this issue.
    """
    log_file = tmp_path / "scenario_experiment.log"
    test_settings = set_cli_env(monkeypatch, docker_compose, fasttext_embedding_service, log_file, settings=settings)
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Experiment V1
    # Step 1: clean
    args = ["clean"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0

    # Step 2: dry-run with experimental params
    # Using GDS-stable resolution values for small test graphs
    args = [
        "run",
        "--dry-run",
        "--leiden-resolution",
        "1.0",
        "--similarity-threshold",
        "0.9",
        "--feed-size",
        "100",
    ]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0

    # Step 3: export v1
    export_v1 = tmp_path / "experiment_v1.md"
    args = ["export", str(export_v1), "--user-id", "101"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0
    assert export_v1.exists()
    content_v1 = export_v1.read_text()

    # Experiment V2
    # Step 4: clean
    args = ["clean"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0

    # Step 5: dry-run with different params
    args = [
        "run",
        "--dry-run",
        "--leiden-resolution",
        "0.5",
        "--similarity-threshold",
        "0.75",
        "--feed-size",
        "100",
    ]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0

    # Step 6: export v2
    export_v2 = tmp_path / "experiment_v2.md"
    args = ["export", str(export_v2), "--user-id", "101"]
    exit_code = _run_cli(monkeypatch, args)
    assert exit_code == 0
    assert export_v2.exists()
    content_v2 = export_v2.read_text()

    # Step 7: verify both exports exist and have content
    assert len(content_v1) > 0, "Experiment V1 export should have content"
    assert len(content_v2) > 0, "Experiment V2 export should have content"
