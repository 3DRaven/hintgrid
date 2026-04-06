"""Pairwise CLI integration tests for HintGrid refresh command.

Uses allpairspy for pairwise parameter combinations to ensure
comprehensive coverage of refresh CLI parameter interactions via subprocess.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, cast

import pytest
from allpairspy import AllPairs  # type: ignore[import-untyped]

from hintgrid.utils.coercion import coerce_int

# Fixtures setup_mastodon_schema_for_cli and sample_data_for_cli are defined in conftest.py

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    import redis
    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig


class _RedisTestClient(Protocol):
    def zcard(self, name: str) -> int: ...


# Pairwise parameter space for refresh command
# Each list contains possible values for a specific CLI parameter
REFRESH_PAIRWISE_PARAMS: list[list[str | bool | int | float]] = [
    # --decay-half-life-days: half-life in days for exponential decay
    [7, 14, 30],
    # --interests-ttl-days: TTL in days for INTERESTED_IN relationships
    [15, 30, 60],
    # --likes-weight: weight for FAVORITED edges
    [0.5, 1.0, 2.0],
    # --reblogs-weight: weight for REBLOGGED edges
    [1.0, 1.5, 2.0],
    # --bookmark-weight: weight for bookmarked posts
    [1.0, 2.0],
    # --ctr-enabled: enable CTR scoring
    [True, False],
    # --pagerank-enabled: enable PageRank scoring
    [True, False],
    # --community-similarity-enabled: enable community-based similarity scoring
    [True, False],
    # --language-match-weight: weight boost for language matching
    [0.0, 0.3, 0.5],
]

REFRESH_PARAM_NAMES = [
    "decay_half_life_days",
    "interests_ttl_days",
    "likes_weight",
    "reblogs_weight",
    "bookmark_weight",
    "ctr_enabled",
    "pagerank_enabled",
    "community_similarity_enabled",
    "language_match_weight",
]

# Type alias for pairwise combination tuple
RefreshPairwiseTuple = tuple[
    int, int, float, float, float, bool, bool, bool, float
]


def _build_refresh_cli_args(
    decay_half_life_days: int,
    interests_ttl_days: int,
    likes_weight: float,
    reblogs_weight: float,
    bookmark_weight: float,
    ctr_enabled: bool,
    pagerank_enabled: bool,
    community_similarity_enabled: bool,
    language_match_weight: float,
) -> list[str]:
    """Build CLI arguments from parameter values for refresh command."""
    args = ["refresh"]

    args.extend(["--decay-half-life-days", str(decay_half_life_days)])
    args.extend(["--interests-ttl-days", str(interests_ttl_days)])
    args.extend(["--likes-weight", str(likes_weight)])
    args.extend(["--reblogs-weight", str(reblogs_weight)])
    args.extend(["--bookmark-weight", str(bookmark_weight)])

    if ctr_enabled:
        args.append("--ctr-enabled")
    else:
        args.append("--no-ctr-enabled")

    if pagerank_enabled:
        args.append("--pagerank-enabled")
    else:
        args.append("--no-pagerank-enabled")

    if community_similarity_enabled:
        args.append("--community-similarity-enabled")
    else:
        args.append("--no-community-similarity-enabled")

    args.extend(["--language-match-weight", str(language_match_weight)])
    ui_w = max(0.5, float(language_match_weight))
    args.extend(["--ui-language-match-weight", str(ui_w)])

    return args


def _generate_refresh_pairwise_combinations() -> list[RefreshPairwiseTuple]:
    """Generate pairwise parameter combinations for refresh command."""
    combinations: list[RefreshPairwiseTuple] = []
    all_pairs_gen = AllPairs(REFRESH_PAIRWISE_PARAMS)
    for values in all_pairs_gen:
        vals = list(values)
        combinations.append(
            (
                int(vals[0]),
                int(vals[1]),
                float(vals[2]),
                float(vals[3]),
                float(vals[4]),
                bool(vals[5]),
                bool(vals[6]),
                bool(vals[7]),
                float(vals[8]),
            )
        )
    return combinations


REFRESH_PAIRWISE_COMBINATIONS = _generate_refresh_pairwise_combinations()


def _build_subprocess_env(
    docker_compose: DockerComposeInfo,
    fasttext_embedding_service: EmbeddingServiceConfig,
    log_file: Path,
    worker_id: str,
    worker_num: int,
) -> dict[str, str]:
    """Build complete environment for subprocess with Docker ports."""
    # Start with minimal system environment (PATH, PYTHONPATH, etc.)
    env: dict[str, str] = {}

    # Essential system variables
    for key in ["PATH", "PYTHONPATH", "HOME", "USER", "LANG", "LC_ALL", "VIRTUAL_ENV"]:
        if key in os.environ:
            env[key] = os.environ[key]

    # PostgreSQL - dynamic Docker ports
    env["HINTGRID_POSTGRES_HOST"] = docker_compose.postgres_host
    env["HINTGRID_POSTGRES_PORT"] = str(docker_compose.postgres_port)
    env["HINTGRID_POSTGRES_DATABASE"] = docker_compose.postgres_db
    env["HINTGRID_POSTGRES_USER"] = docker_compose.postgres_user
    env["HINTGRID_POSTGRES_PASSWORD"] = docker_compose.postgres_password
    env["HINTGRID_POSTGRES_SCHEMA"] = "public" if worker_id == "master" else f"test_{worker_id}"

    # Neo4j - dynamic Docker ports
    env["HINTGRID_NEO4J_HOST"] = docker_compose.neo4j_host
    env["HINTGRID_NEO4J_PORT"] = str(docker_compose.neo4j_port)
    env["HINTGRID_NEO4J_USERNAME"] = docker_compose.neo4j_user
    env["HINTGRID_NEO4J_PASSWORD"] = docker_compose.neo4j_password
    env["HINTGRID_NEO4J_WORKER_LABEL"] = (
        "worker_master" if worker_id == "master" else f"worker_{worker_id}"
    )

    # Redis - dynamic Docker ports
    env["HINTGRID_REDIS_HOST"] = docker_compose.redis_host
    env["HINTGRID_REDIS_PORT"] = str(docker_compose.redis_port)
    env["HINTGRID_REDIS_DB"] = str(worker_num)
    env["HINTGRID_REDIS_PASSWORD"] = ""

    # LLM (TF-IDF service)
    env["HINTGRID_LLM_PROVIDER"] = "openai"
    env["HINTGRID_LLM_BASE_URL"] = fasttext_embedding_service["api_base"]
    env["HINTGRID_LLM_MODEL"] = fasttext_embedding_service["model"]
    env["HINTGRID_LLM_API_KEY"] = "sk-fake-key-for-testing"
    env["OPENAI_API_KEY"] = "sk-fake-key-for-testing"
    # Set dimensions to match FastText service (128)
    env["HINTGRID_LLM_DIMENSIONS"] = "128"

    # Logging
    env["HINTGRID_LOG_FILE"] = str(log_file)
    env["HINTGRID_LOG_LEVEL"] = "INFO"

    # Test-friendly settings
    env["HINTGRID_INTERESTS_MIN_FAVOURITES"] = "1"
    env["HINTGRID_FEED_DAYS"] = "365"

    return env


def _run_subprocess(args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run hintgrid CLI via subprocess with full environment."""
    cmd = [sys.executable, "-m", "hintgrid.app", *args]
    return subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
        cwd=str(Path(__file__).parent.parent.parent),
    )


def _verify_neo4j_interests(neo4j: Neo4jClient) -> dict[str, int]:
    """Verify Neo4j interests were updated using existing client (worker-isolated)."""

    counts: dict[str, int] = {}

    # Use worker-isolated labels for all queries
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")

    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__uc__)-[i:INTERESTED_IN]->(:__pc__) "
            "RETURN count(i) AS count",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
        )
    )
    counts["interests"] = coerce_int(result[0]["count"]) if result else 0

    return counts


def _verify_redis_feeds(redis_client: redis.Redis, user_ids: list[int]) -> dict[str, int]:
    """Verify Redis feeds were updated using existing client."""
    redis_test = cast("_RedisTestClient", redis_client)

    feed_counts: dict[str, int] = {}
    for user_id in user_ids:
        key = f"feed:home:{user_id}"
        feed_counts[key] = redis_test.zcard(key)

    return feed_counts


@pytest.mark.integration
@pytest.mark.parametrize(
    REFRESH_PARAM_NAMES,
    REFRESH_PAIRWISE_COMBINATIONS,
    ids=[f"decay={c[0]}_ctr={c[5]}_pr={c[6]}" for c in REFRESH_PAIRWISE_COMBINATIONS],
)
def test_refresh_pairwise_cli_subprocess(
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    redis_client: redis.Redis,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    worker_num: int,
    decay_half_life_days: int,
    interests_ttl_days: int,
    likes_weight: float,
    reblogs_weight: float,
    bookmark_weight: float,
    ctr_enabled: bool,
    pagerank_enabled: bool,
    community_similarity_enabled: bool,
    language_match_weight: float,
) -> None:
    """Test HintGrid refresh CLI with pairwise parameter combinations via subprocess.

    Full end-to-end test: subprocess -> CLI -> Neo4j/Redis verification.
    Uses shared fixtures from conftest.py for infrastructure and cleanup.
    """
    # Build environment with dynamic Docker ports
    log_file = tmp_path / "refresh_pairwise.log"
    env = _build_subprocess_env(
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        worker_num,
    )

    # First run full pipeline to create initial data
    run_result = _run_subprocess(["run"], env)
    assert run_result.returncode == 0, (
        f"Initial run failed with exit code {run_result.returncode}\n"
        f"stdout: {run_result.stdout[-2000:] if len(run_result.stdout) > 2000 else run_result.stdout}\n"
        f"stderr: {run_result.stderr[-2000:] if len(run_result.stderr) > 2000 else run_result.stderr}"
    )

    # Get initial interest count
    initial_counts = _verify_neo4j_interests(neo4j)
    initial_interest_count = initial_counts["interests"]
    assert initial_interest_count > 0, "Initial interests should exist after run"

    # Build CLI arguments for refresh
    cli_args = _build_refresh_cli_args(
        decay_half_life_days=decay_half_life_days,
        interests_ttl_days=interests_ttl_days,
        likes_weight=likes_weight,
        reblogs_weight=reblogs_weight,
        bookmark_weight=bookmark_weight,
        ctr_enabled=ctr_enabled,
        pagerank_enabled=pagerank_enabled,
        community_similarity_enabled=community_similarity_enabled,
        language_match_weight=language_match_weight,
    )

    # Run refresh CLI via subprocess
    result = _run_subprocess(cli_args, env)

    # Verify exit code
    assert result.returncode == 0, (
        f"Refresh CLI failed with exit code {result.returncode}\n"
        f"Args: {cli_args}\n"
        f"Env ports: PG={env['HINTGRID_POSTGRES_PORT']}, "
        f"Neo4j={env['HINTGRID_NEO4J_PORT']}, Redis={env['HINTGRID_REDIS_PORT']}\n"
        f"stdout: {result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout}\n"
        f"stderr: {result.stderr[-2000:] if len(result.stderr) > 2000 else result.stderr}"
    )

    # Verify Neo4j interests were updated (not recreated, just updated)
    after_counts = _verify_neo4j_interests(neo4j)
    after_interest_count = after_counts["interests"]
    assert after_interest_count > 0, "Interests should still exist after refresh"
    # Interests should remain (refresh doesn't delete them, just updates scores)
    assert after_interest_count >= initial_interest_count * 0.5, (
        "Most interests should remain after refresh (decay may remove some expired ones)"
    )

    # Verify Redis feeds were updated using shared fixture
    feed_counts = _verify_redis_feeds(redis_client, sample_data_for_cli["user_ids"])

    # Feeds should still exist after refresh
    total_feeds = sum(feed_counts.values())
    assert total_feeds > 0, "At least some feeds should exist after refresh"

    print(
        f"✅ Refresh pairwise subprocess test passed: "
        f"decay={decay_half_life_days}, ctr={ctr_enabled}, pr={pagerank_enabled}"
    )
