"""CLI integration tests for train commands."""

from __future__ import annotations

from datetime import datetime, timedelta, UTC
from typing import TYPE_CHECKING, cast

import pytest

from hintgrid import app as app_module
from hintgrid.utils.coercion import coerce_int
from hintgrid.utils.snowflake import snowflake_id_at

from .conftest import RedisTestClient, run_cli, set_cli_env

if TYPE_CHECKING:
    from hintgrid.config import HintGridSettings
    from hintgrid.clients.neo4j import Neo4jClient
    import redis
    from pathlib import Path
    from psycopg import Connection
    from psycopg.rows import TupleRow

    from tests.conftest import DockerComposeInfo, EmbeddingServiceConfig


@pytest.mark.integration
def test_cli_train_full(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test 'hintgrid train --full' command.

    Uses built-in FastText provider (not external LLM) for training.
    """
    log_file = tmp_path / "train_full.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        None,  # No external embedding service - use built-in FastText
        log_file,
        worker_id,
        settings,
    )
    # Override to use FastText provider
    monkeypatch.setenv("HINTGRID_LLM_PROVIDER", "fasttext")
    monkeypatch.setenv("HINTGRID_FASTTEXT_MIN_DOCUMENTS", "2")
    monkeypatch.setenv("HINTGRID_FASTTEXT_MODEL_PATH", str(tmp_path))
    test_settings = test_settings.model_copy(
        update={
            "llm_provider": "fasttext",
            "llm_base_url": None,
            "fasttext_model_path": str(tmp_path),
            "fasttext_min_documents": 2,
        }
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["train", "--full"])
    assert exit_code == 0

    output = capsys.readouterr().out
    assert "Full training completed successfully" in output

    # Verify FastTextState node exists with version > 0
    # Use labeled query to scope to this worker's data in parallel mode
    rows = list(neo4j.execute_and_fetch_labeled(
        "MATCH (s:__label__) RETURN s.version AS version",
        {"label": "FastTextState"},
    ))
    assert len(rows) == 1
    version = coerce_int(rows[0].get("version"))
    assert version >= 1


@pytest.mark.integration
def test_cli_train_incremental_without_existing_model(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test 'hintgrid train --incremental' when no model exists.

    Uses built-in FastText provider (not external LLM) for training.
    """
    log_file = tmp_path / "train_incr.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        None,  # No external embedding service - use built-in FastText
        log_file,
        worker_id,
        settings,
    )
    # Override to use FastText provider
    monkeypatch.setenv("HINTGRID_LLM_PROVIDER", "fasttext")
    monkeypatch.setenv("HINTGRID_FASTTEXT_MIN_DOCUMENTS", "2")
    monkeypatch.setenv("HINTGRID_FASTTEXT_MODEL_PATH", str(tmp_path))
    test_settings = test_settings.model_copy(
        update={
            "llm_provider": "fasttext",
            "llm_base_url": None,
            "fasttext_model_path": str(tmp_path),
            "fasttext_min_documents": 2,
        }
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Incremental without existing model should perform full training
    exit_code = run_cli(monkeypatch, ["train", "--incremental"])
    assert exit_code == 0

    output = capsys.readouterr().out
    assert "Incremental training completed successfully" in output


@pytest.mark.integration
def test_cli_train_incremental_after_full(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    postgres_conn: Connection[TupleRow],
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test 'hintgrid train --incremental' after full training.

    Uses built-in FastText provider (not external LLM) for training.
    """
    log_file = tmp_path / "train_incr_after_full.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        None,  # No external embedding service - use built-in FastText
        log_file,
        worker_id,
        settings,
    )
    # Override to use FastText provider
    monkeypatch.setenv("HINTGRID_LLM_PROVIDER", "fasttext")
    monkeypatch.setenv("HINTGRID_FASTTEXT_MIN_DOCUMENTS", "2")
    monkeypatch.setenv("HINTGRID_FASTTEXT_MODEL_PATH", str(tmp_path))
    test_settings = test_settings.model_copy(
        update={
            "llm_provider": "fasttext",
            "llm_base_url": None,
            "fasttext_model_path": str(tmp_path),
            "fasttext_min_documents": 2,
        }
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # First do full training
    exit_code = run_cli(monkeypatch, ["train", "--full"])
    assert exit_code == 0
    capsys.readouterr()  # Clear output

    # Get version after full training (use labeled query for parallel isolation)
    rows = list(neo4j.execute_and_fetch_labeled(
        "MATCH (s:__label__) RETURN s.version AS version",
        {"label": "FastTextState"},
    ))
    version_after_full = coerce_int(rows[0].get("version"))

    # Add new posts for incremental training
    with postgres_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO statuses (id, account_id, text, language, visibility)
            VALUES
                (7, 101, 'New post for incremental training #test', 'en', 0),
                (8, 102, 'Another new post about programming #python', 'en', 0)
            RETURNING id;
        """)
        postgres_conn.commit()

    # Now do incremental training
    exit_code = run_cli(monkeypatch, ["train", "--incremental"])
    assert exit_code == 0

    output = capsys.readouterr().out
    assert "Incremental training completed successfully" in output

    # Verify version incremented (or stayed same if no new data)
    rows = list(neo4j.execute_and_fetch_labeled(
        "MATCH (s:__label__) RETURN s.version AS version",
        {"label": "FastTextState"},
    ))
    version_after_incr = coerce_int(rows[0].get("version"))
    # Version should be >= previous (no new data = same version)
    assert version_after_incr >= version_after_full


@pytest.mark.integration
def test_cli_run_with_train_flag(
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
    """Test 'hintgrid run --train' performs incremental training before pipeline."""
    log_file = tmp_path / "run_train.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        fasttext_embedding_service,
        log_file,
        worker_id,
        settings,
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Run with --train flag
    exit_code = run_cli(monkeypatch, ["run", "--train"])
    assert exit_code == 0

    # Verify feeds were created (not all users may have recommendations)
    redis_raw = cast("RedisTestClient", redis_client)
    feed_counts = [
        redis_raw.zcard(f"feed:home:{user_id}")
        for user_id in sample_data_for_cli["user_ids"]
    ]
    total_feeds = sum(feed_counts)
    assert total_feeds > 0, (
        f"At least some feeds should be created, got {feed_counts} for users "
        f"{sample_data_for_cli['user_ids']}"
    )


@pytest.mark.integration
def test_cli_train_full_with_since_date(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    postgres_conn: Connection[TupleRow],
    mastodon_schema: None,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test 'hintgrid train --full --since 30d' with date filter.

    Uses built-in FastText provider (not external LLM) for training.
    Creates test data with proper Snowflake IDs for date filtering.
    """
    log_file = tmp_path / "train_since.log"

    # Create sample data with Snowflake IDs for recent posts (within 30d)
    now = datetime.now(UTC)
    recent_date = now - timedelta(days=10)  # 10 days ago - within 30d filter

    with postgres_conn.cursor() as cur:
        # Create accounts
        cur.execute("""
            INSERT INTO accounts (id, username, domain)
            VALUES (201, 'snowflake_user1', NULL), (202, 'snowflake_user2', NULL)
            ON CONFLICT (id) DO NOTHING;
        """)

        # Create posts with Snowflake IDs
        snowflake_base = snowflake_id_at(recent_date)
        cur.execute(
            """
            INSERT INTO statuses (id, account_id, text, language, visibility, created_at)
            VALUES
                (%s, 201, 'Recent post 1 for training #test', 'en', 0, %s),
                (%s, 202, 'Recent post 2 about coding #python', 'en', 0, %s),
                (%s, 201, 'Recent post 3 about technology #tech', 'en', 0, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (
                snowflake_base, recent_date,
                snowflake_base + 1, recent_date,
                snowflake_base + 2, recent_date,
            ),
        )
        postgres_conn.commit()

    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        None,  # No external embedding service - use built-in FastText
        log_file,
        worker_id,
        settings,
    )
    # Override to use FastText provider
    monkeypatch.setenv("HINTGRID_LLM_PROVIDER", "fasttext")
    monkeypatch.setenv("HINTGRID_FASTTEXT_MIN_DOCUMENTS", "2")
    monkeypatch.setenv("HINTGRID_FASTTEXT_MODEL_PATH", str(tmp_path))
    test_settings = test_settings.model_copy(
        update={
            "llm_provider": "fasttext",
            "llm_base_url": None,
            "fasttext_model_path": str(tmp_path),
            "fasttext_min_documents": 2,
        }
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    # Train with date filter (30d should include our recent posts)
    exit_code = run_cli(monkeypatch, ["train", "--full", "--since", "30d"])
    assert exit_code == 0

    output = capsys.readouterr().out
    assert "Full training completed successfully" in output


@pytest.mark.integration
def test_cli_train_fasttext_epochs(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid train --full' respects fasttext_epochs parameter."""
    log_file = tmp_path / "train_epochs.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        None,  # No external embedding service - use built-in FastText
        log_file,
        worker_id,
        settings,
    )
    # Override to use FastText provider
    monkeypatch.setenv("HINTGRID_LLM_PROVIDER", "fasttext")
    monkeypatch.setenv("HINTGRID_FASTTEXT_MIN_DOCUMENTS", "2")
    monkeypatch.setenv("HINTGRID_FASTTEXT_MODEL_PATH", str(tmp_path))
    test_settings = test_settings.model_copy(
        update={
            "llm_provider": "fasttext",
            "llm_base_url": None,
            "fasttext_model_path": str(tmp_path),
            "fasttext_min_documents": 2,
        }
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["train", "--full", "--fasttext-epochs", "3"])
    assert exit_code == 0

    output = capsys.readouterr().out
    assert "Full training completed successfully" in output

    # Verify FastTextState node exists
    rows = list(neo4j.execute_and_fetch_labeled(
        "MATCH (s:__label__) RETURN s.version AS version",
        {"label": "FastTextState"},
    ))
    assert len(rows) == 1
    version = coerce_int(rows[0].get("version"))
    assert version >= 1


@pytest.mark.integration
def test_cli_train_fasttext_window(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid train --full' respects fasttext_window parameter."""
    log_file = tmp_path / "train_window.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        None,  # No external embedding service - use built-in FastText
        log_file,
        worker_id,
        settings,
    )
    # Override to use FastText provider
    monkeypatch.setenv("HINTGRID_LLM_PROVIDER", "fasttext")
    monkeypatch.setenv("HINTGRID_FASTTEXT_MIN_DOCUMENTS", "2")
    monkeypatch.setenv("HINTGRID_FASTTEXT_MODEL_PATH", str(tmp_path))
    test_settings = test_settings.model_copy(
        update={
            "llm_provider": "fasttext",
            "llm_base_url": None,
            "fasttext_model_path": str(tmp_path),
            "fasttext_min_documents": 2,
        }
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["train", "--full", "--fasttext-window", "5"])
    assert exit_code == 0

    output = capsys.readouterr().out
    assert "Full training completed successfully" in output

    # Verify FastTextState node exists
    rows = list(neo4j.execute_and_fetch_labeled(
        "MATCH (s:__label__) RETURN s.version AS version",
        {"label": "FastTextState"},
    ))
    assert len(rows) == 1
    version = coerce_int(rows[0].get("version"))
    assert version >= 1


@pytest.mark.integration
def test_cli_train_fasttext_training_workers(
    monkeypatch: pytest.MonkeyPatch,
    docker_compose: DockerComposeInfo,
    neo4j: Neo4jClient,
    sample_data_for_cli: dict[str, list[int]],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    worker_id: str,
    settings: HintGridSettings,
) -> None:
    """Test that 'hintgrid train --full' respects fasttext_training_workers parameter."""
    log_file = tmp_path / "train_workers.log"
    test_settings = set_cli_env(
        monkeypatch,
        docker_compose,
        None,  # No external embedding service - use built-in FastText
        log_file,
        worker_id,
        settings,
    )
    # Override to use FastText provider
    monkeypatch.setenv("HINTGRID_LLM_PROVIDER", "fasttext")
    monkeypatch.setenv("HINTGRID_FASTTEXT_MIN_DOCUMENTS", "2")
    monkeypatch.setenv("HINTGRID_FASTTEXT_MODEL_PATH", str(tmp_path))
    test_settings = test_settings.model_copy(
        update={
            "llm_provider": "fasttext",
            "llm_base_url": None,
            "fasttext_model_path": str(tmp_path),
            "fasttext_min_documents": 2,
        }
    )
    monkeypatch.setattr(app_module, "HintGridSettings", lambda: test_settings)

    exit_code = run_cli(monkeypatch, ["train", "--full", "--fasttext-training-workers", "2"])
    assert exit_code == 0

    output = capsys.readouterr().out
    assert "Full training completed successfully" in output

    # Verify FastTextState node exists
    rows = list(neo4j.execute_and_fetch_labeled(
        "MATCH (s:__label__) RETURN s.version AS version",
        {"label": "FastTextState"},
    ))
    assert len(rows) == 1
    version = coerce_int(rows[0].get("version"))
    assert version >= 1
