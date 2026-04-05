"""Pairwise CLI integration tests for HintGrid.

Uses allpairspy for pairwise parameter combinations to ensure
comprehensive coverage of CLI parameter interactions via subprocess.
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


# Pairwise parameter space from env.example
# Each list contains possible values for a specific CLI parameter
PAIRWISE_PARAMS: list[list[str | bool | int | float]] = [
    # --dry-run: run pipeline without writing to Redis
    [True, False],
    # --similarity-pruning: strategy for SIMILAR_TO edges
    ["aggressive", "partial", "none"],
    # --prune-after-clustering: prune edges after clustering
    [True, False],
    # --cold-start-fallback: fallback strategy for new users
    ["global_top"],
    # --feed-ttl: TTL strategy for feeds
    ["none"],
    # --user-communities: user clustering strategy
    ["dynamic"],
    # --post-communities: post clustering strategy
    ["dynamic"],
    # --leiden-resolution: Leiden algorithm resolution
    # Note: 2.0 causes NullPointerException in GDS Leiden on small graphs
    [0.1, 0.5, 1.0],
    # --knn-neighbors: KNN neighbors count
    [3, 5, 10],
    # --similarity-threshold: threshold for SIMILAR_TO edges
    [0.7, 0.85, 0.95],
    # --feed-size: feed size per user
    [100, 500],
    # --likes-weight: weight for FAVORITED edges
    [0.5, 1.0, 2.0],
    # --reblogs-weight: weight for REBLOGGED edges
    [1.0, 1.5, 2.0],
    # --replies-weight: weight for REPLIED edges
    [2.0, 3.0],
    # --follows-weight: weight for FOLLOWS in INTERACTS_WITH aggregation
    [0.0, 1.0, 2.0],
    # --mentions-weight: weight for mentions in INTERACTS_WITH aggregation
    [0.5, 1.0, 1.5],
    # --serendipity-probability: probability of serendipity links
    [0.0, 0.1, 0.2],
    # Group A: FastText parameters
    # --fasttext-vector-size: embedding vector dimensions
    [64, 128, 256],
    # --fasttext-quantize: enable model quantization
    [True, False],
    # --fasttext-min-documents: minimum documents for training
    [10, 100],
    # Group B: Feed and scoring parameters
    # --feed-days: time window for candidate posts
    [3, 7, 30],
    # --feed-score-multiplier: score multiplier for ranking
    [1, 2, 5],
    # --bookmark-weight: weight for bookmarked posts
    [1.0, 2.0],
    # --personalized-interest-weight: interest component weight (sum with popularity+recency = 1.0)
    [0.3, 0.5, 0.7],
    # --ctr-enabled: enable CTR scoring
    [True, False],
    # --pagerank-enabled: enable PageRank scoring
    [True, False],
    # Group C: Batch and concurrency
    # --batch-size: batch size for data loading
    [1000, 10000],
    # --feed-workers: parallel feed generation workers
    [1, 2],
    # --loader-workers: parallel entity loading workers
    [1, 2],
    # Group D: Pruning and clustering
    # --prune-similarity-threshold: threshold for partial pruning
    [0.8, 0.9, 0.95],
    # --prune-days: window for temporal pruning
    [7, 30],
    # --similarity-recency-days: recency window for SIMILAR_TO edges
    [3, 7, 14],
    # --leiden-max-levels: maximum hierarchical Leiden levels
    [5, 10],
]

PARAM_NAMES = [
    "dry_run",
    "similarity_pruning",
    "prune_after_clustering",
    "cold_start_fallback",
    "feed_ttl",
    "user_communities",
    "post_communities",
    "leiden_resolution",
    "knn_neighbors",
    "similarity_threshold",
    "feed_size",
    "likes_weight",
    "reblogs_weight",
    "replies_weight",
    "follows_weight",
    "mentions_weight",
    "serendipity_probability",
    # Group A: FastText
    "fasttext_vector_size",
    "fasttext_quantize",
    "fasttext_min_documents",
    # Group B: Feed and scoring
    "feed_days",
    "feed_score_multiplier",
    "bookmark_weight",
    "personalized_interest_weight",
    "ctr_enabled",
    "pagerank_enabled",
    # Group C: Batch and concurrency
    "batch_size",
    "feed_workers",
    "loader_workers",
    # Group D: Pruning and clustering
    "prune_similarity_threshold",
    "prune_days",
    "similarity_recency_days",
    "leiden_max_levels",
]

# Type alias for pairwise combination tuple
# 36 parameters total: 15 original + 21 new (added follows_weight, mentions_weight)
PairwiseTuple = tuple[
    # Original 15
    bool, str, bool, str, str, str, str, float, int, float, int, float, float, float, float,
    # New weights (2)
    float, float,
    # Group A: FastText (3)
    int, bool, int,
    # Group B: Feed and scoring (6)
    int, int, float, float, bool, bool,
    # Group C: Batch and concurrency (3)
    int, int, int,
    # Group D: Pruning and clustering (4)
    float, int, int, int,
]


def _build_cli_args(
    dry_run: bool,
    similarity_pruning: str,
    prune_after_clustering: bool,
    cold_start_fallback: str,
    feed_ttl: str,
    user_communities: str,
    post_communities: str,
    leiden_resolution: float,
    knn_neighbors: int,
    similarity_threshold: float,
    feed_size: int,
    likes_weight: float,
    reblogs_weight: float,
    replies_weight: float,
    follows_weight: float,
    mentions_weight: float,
    serendipity_probability: float,
    # Group A: FastText
    fasttext_vector_size: int,
    fasttext_quantize: bool,
    fasttext_min_documents: int,
    # Group B: Feed and scoring
    feed_days: int,
    feed_score_multiplier: int,
    bookmark_weight: float,
    personalized_interest_weight: float,
    ctr_enabled: bool,
    pagerank_enabled: bool,
    # Group C: Batch and concurrency
    batch_size: int,
    feed_workers: int,
    loader_workers: int,
    # Group D: Pruning and clustering
    prune_similarity_threshold: float,
    prune_days: int,
    similarity_recency_days: int,
    leiden_max_levels: int,
) -> list[str]:
    """Build CLI arguments from parameter values."""
    args = ["run"]

    if dry_run:
        args.append("--dry-run")

    args.extend(["--similarity-pruning", similarity_pruning])

    if prune_after_clustering:
        args.append("--prune-after-clustering")
    else:
        args.append("--no-prune-after-clustering")

    args.extend(["--cold-start-fallback", cold_start_fallback])
    args.extend(["--feed-ttl", feed_ttl])
    args.extend(["--user-communities", user_communities])
    args.extend(["--post-communities", post_communities])
    args.extend(["--leiden-resolution", str(leiden_resolution)])
    args.extend(["--knn-neighbors", str(knn_neighbors)])
    args.extend(["--similarity-threshold", str(similarity_threshold)])
    args.extend(["--feed-size", str(feed_size)])
    args.extend(["--likes-weight", str(likes_weight)])
    args.extend(["--reblogs-weight", str(reblogs_weight)])
    args.extend(["--replies-weight", str(replies_weight)])
    args.extend(["--follows-weight", str(follows_weight)])
    args.extend(["--mentions-weight", str(mentions_weight)])
    args.extend(["--serendipity-probability", str(serendipity_probability)])

    # Group A: FastText
    args.extend(["--fasttext-vector-size", str(fasttext_vector_size)])
    if fasttext_quantize:
        args.append("--fasttext-quantize")
        # Ensure qdim <= vector_size when quantize is enabled
        qdim = min(100, fasttext_vector_size)  # Default is 100, but must be <= vector_size
        args.extend(["--fasttext-quantize-qdim", str(qdim)])
    else:
        args.append("--no-fasttext-quantize")
        # Still need to set qdim to avoid validation error (validation checks qdim even when quantize is off)
        # Set it to vector_size to ensure it's valid
        args.extend(["--fasttext-quantize-qdim", str(fasttext_vector_size)])
    args.extend(["--fasttext-min-documents", str(fasttext_min_documents)])

    # Group B: Feed and scoring
    args.extend(["--feed-days", str(feed_days)])
    args.extend(["--feed-score-multiplier", str(feed_score_multiplier)])
    args.extend(["--bookmark-weight", str(bookmark_weight)])
    args.extend(["--personalized-interest-weight", str(personalized_interest_weight)])
    # Calculate remaining weights to sum to 1.0
    remaining_weight = 1.0 - personalized_interest_weight
    args.extend(["--personalized-popularity-weight", str(round(remaining_weight * 0.6, 6))])
    args.extend(["--personalized-recency-weight", str(round(remaining_weight * 0.4, 6))])
    if ctr_enabled:
        args.append("--ctr-enabled")
    else:
        args.append("--no-ctr-enabled")
    if pagerank_enabled:
        args.append("--pagerank-enabled")
    else:
        args.append("--no-pagerank-enabled")

    # Group C: Batch and concurrency
    args.extend(["--batch-size", str(batch_size)])
    args.extend(["--feed-workers", str(feed_workers)])
    args.extend(["--loader-workers", str(loader_workers)])

    # Group D: Pruning and clustering
    args.extend(["--prune-similarity-threshold", str(prune_similarity_threshold)])
    args.extend(["--prune-days", str(prune_days)])
    args.extend(["--similarity-recency-days", str(similarity_recency_days)])
    args.extend(["--leiden-max-levels", str(leiden_max_levels)])

    return args


def _generate_pairwise_combinations() -> list[PairwiseTuple]:
    """Generate pairwise parameter combinations."""
    combinations: list[PairwiseTuple] = []
    all_pairs_gen = AllPairs(PAIRWISE_PARAMS)
    for values in all_pairs_gen:
        vals = list(values)
        combinations.append(
            (
                # Original 15
                bool(vals[0]),
                str(vals[1]),
                bool(vals[2]),
                str(vals[3]),
                str(vals[4]),
                str(vals[5]),
                str(vals[6]),
                float(vals[7]),
                int(vals[8]),
                float(vals[9]),
                int(vals[10]),
                float(vals[11]),
                float(vals[12]),
                float(vals[13]),
                float(vals[14]),
                # New weights (2)
                float(vals[15]),
                float(vals[16]),
                # Group A: FastText (3)
                int(vals[17]),
                bool(vals[18]),
                int(vals[19]),
                # Group B: Feed and scoring (6)
                int(vals[20]),
                int(vals[21]),
                float(vals[22]),
                float(vals[23]),
                bool(vals[24]),
                bool(vals[25]),
                # Group C: Batch and concurrency (3)
                int(vals[26]),
                int(vals[27]),
                int(vals[28]),
                # Group D: Pruning and clustering (4)
                float(vals[29]),
                int(vals[30]),
                int(vals[31]),
                int(vals[32]),
            )
        )
    return combinations


PAIRWISE_COMBINATIONS = _generate_pairwise_combinations()


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

    # LLM (TF-IDF service) - will be overridden by CLI args for FastText params
    env["HINTGRID_LLM_PROVIDER"] = "openai"
    env["HINTGRID_LLM_BASE_URL"] = fasttext_embedding_service["api_base"]
    env["HINTGRID_LLM_MODEL"] = fasttext_embedding_service["model"]
    env["HINTGRID_LLM_API_KEY"] = "sk-fake-key-for-testing"
    env["OPENAI_API_KEY"] = "sk-fake-key-for-testing"
    # Set dimensions to match FastText service (128) - may be overridden by CLI
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


def _verify_neo4j_data(neo4j: Neo4jClient) -> dict[str, int]:
    """Verify Neo4j data was created using existing client (worker-isolated)."""

    counts: dict[str, int] = {}

    # Use worker-isolated labels for all queries
    neo4j.label("User")
    neo4j.label("Post")
    neo4j.label("UserCommunity")
    neo4j.label("PostCommunity")

    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) RETURN count(u) AS count",
            {"user": "User"},
        )
    )
    counts["users"] = coerce_int(result[0]["count"]) if result else 0

    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) RETURN count(p) AS count",
            {"post": "Post"},
        )
    )
    counts["posts"] = coerce_int(result[0]["count"]) if result else 0

    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__user__)-[:BELONGS_TO]->(:__uc__) RETURN count(*) AS count",
            {"user": "User", "uc": "UserCommunity"},
        )
    )
    counts["user_communities"] = coerce_int(result[0]["count"]) if result else 0

    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (:__post__)-[:BELONGS_TO]->(:__pc__) RETURN count(*) AS count",
            {"post": "Post", "pc": "PostCommunity"},
        )
    )
    counts["post_communities"] = coerce_int(result[0]["count"]) if result else 0

    return counts


def _verify_redis_feeds(redis_client: redis.Redis, user_ids: list[int]) -> dict[str, int]:
    """Verify Redis feeds were created using existing client."""
    redis_test = cast("_RedisTestClient", redis_client)

    feed_counts: dict[str, int] = {}
    for user_id in user_ids:
        key = f"feed:home:{user_id}"
        feed_counts[key] = redis_test.zcard(key)

    return feed_counts


@pytest.mark.integration
@pytest.mark.parametrize(
    PARAM_NAMES,
    PAIRWISE_COMBINATIONS,
    ids=[f"dry={c[0]}_prune={c[1]}_res={c[7]}_ft={c[17]}" for c in PAIRWISE_COMBINATIONS],
)
def test_pairwise_cli_subprocess(
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    redis_client: redis.Redis,
    fasttext_embedding_service: EmbeddingServiceConfig,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    worker_id: str,
    worker_num: int,
    dry_run: bool,
    similarity_pruning: str,
    prune_after_clustering: bool,
    cold_start_fallback: str,
    feed_ttl: str,
    user_communities: str,
    post_communities: str,
    leiden_resolution: float,
    knn_neighbors: int,
    similarity_threshold: float,
    feed_size: int,
    likes_weight: float,
    reblogs_weight: float,
    replies_weight: float,
    follows_weight: float,
    mentions_weight: float,
    serendipity_probability: float,
    # Group A: FastText
    fasttext_vector_size: int,
    fasttext_quantize: bool,
    fasttext_min_documents: int,
    # Group B: Feed and scoring
    feed_days: int,
    feed_score_multiplier: int,
    bookmark_weight: float,
    personalized_interest_weight: float,
    ctr_enabled: bool,
    pagerank_enabled: bool,
    # Group C: Batch and concurrency
    batch_size: int,
    feed_workers: int,
    loader_workers: int,
    # Group D: Pruning and clustering
    prune_similarity_threshold: float,
    prune_days: int,
    similarity_recency_days: int,
    leiden_max_levels: int,
) -> None:
    """Test HintGrid CLI with pairwise parameter combinations via subprocess.

    Full end-to-end test: subprocess -> CLI -> Neo4j/Redis verification.
    Uses shared fixtures from conftest.py for infrastructure and cleanup.
    """
    # Build environment with dynamic Docker ports
    log_file = tmp_path / "pairwise.log"
    env = _build_subprocess_env(
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        worker_num,
    )

    # Build CLI arguments
    cli_args = _build_cli_args(
        dry_run=dry_run,
        similarity_pruning=similarity_pruning,
        prune_after_clustering=prune_after_clustering,
        cold_start_fallback=cold_start_fallback,
        feed_ttl=feed_ttl,
        user_communities=user_communities,
        post_communities=post_communities,
        leiden_resolution=leiden_resolution,
        knn_neighbors=knn_neighbors,
        similarity_threshold=similarity_threshold,
        feed_size=feed_size,
        likes_weight=likes_weight,
        reblogs_weight=reblogs_weight,
        replies_weight=replies_weight,
        follows_weight=follows_weight,
        mentions_weight=mentions_weight,
        serendipity_probability=serendipity_probability,
        fasttext_vector_size=fasttext_vector_size,
        fasttext_quantize=fasttext_quantize,
        fasttext_min_documents=fasttext_min_documents,
        feed_days=feed_days,
        feed_score_multiplier=feed_score_multiplier,
        bookmark_weight=bookmark_weight,
        personalized_interest_weight=personalized_interest_weight,
        ctr_enabled=ctr_enabled,
        pagerank_enabled=pagerank_enabled,
        batch_size=batch_size,
        feed_workers=feed_workers,
        loader_workers=loader_workers,
        prune_similarity_threshold=prune_similarity_threshold,
        prune_days=prune_days,
        similarity_recency_days=similarity_recency_days,
        leiden_max_levels=leiden_max_levels,
    )

    # Run CLI via subprocess
    result = _run_subprocess(cli_args, env)

    # Verify exit code
    assert result.returncode == 0, (
        f"CLI failed with exit code {result.returncode}\n"
        f"Args: {cli_args}\n"
        f"Env ports: PG={env['HINTGRID_POSTGRES_PORT']}, "
        f"Neo4j={env['HINTGRID_NEO4J_PORT']}, Redis={env['HINTGRID_REDIS_PORT']}\n"
        f"stdout: {result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout}\n"
        f"stderr: {result.stderr[-2000:] if len(result.stderr) > 2000 else result.stderr}"
    )

    # Verify Neo4j data using shared fixture (already connected to Docker)
    neo4j_counts = _verify_neo4j_data(neo4j)
    assert neo4j_counts["users"] == len(sample_data_for_cli["user_ids"]), (
        f"Expected {len(sample_data_for_cli['user_ids'])} users, got {neo4j_counts['users']}"
    )
    assert neo4j_counts["posts"] == 5, f"Expected 5 posts, got {neo4j_counts['posts']}"

    # Analytics should run (communities created)
    assert neo4j_counts["user_communities"] > 0, "User communities should be created"
    assert neo4j_counts["post_communities"] > 0, "Post communities should be created"

    # Verify Redis feeds based on dry_run flag using shared fixture
    feed_counts = _verify_redis_feeds(redis_client, sample_data_for_cli["user_ids"])

    if dry_run:
        # In dry-run mode, no feeds should be written
        for key, count in feed_counts.items():
            assert count == 0, f"Feed {key} should be empty in dry-run mode, got {count}"
    else:
        # In normal mode, feeds should be written
        total_feeds = sum(feed_counts.values())
        assert total_feeds > 0, "At least some feeds should be created in normal mode"

    print(f"✅ Pairwise subprocess test passed: dry_run={dry_run}, pruning={similarity_pruning}")
