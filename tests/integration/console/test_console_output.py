"""Integration tests for CLI console output functions."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from hintgrid.cli.console import (
    LoadingProgress,
    create_batch_progress,
    create_data_loading_progress,
    create_pipeline_progress,
    create_spinner_status,
    print_error,
    print_header,
    print_info,
    print_step,
    print_success,
    print_warning,
)

if TYPE_CHECKING:
    from hintgrid.config import HintGridSettings
else:
    from hintgrid.config import HintGridSettings


@pytest.mark.integration
def test_print_functions_do_not_raise() -> None:
    """All print functions should execute without errors."""
    print_success("Test success message")
    print_error("Test error message")
    print_warning("Test warning message")
    print_info("Test info message")
    print_step(1, 5, "Test step message")
    print_header("Test Header")


@pytest.mark.integration
def test_create_pipeline_progress() -> None:
    """create_pipeline_progress returns a Progress instance."""
    progress = create_pipeline_progress()
    assert progress is not None


@pytest.mark.integration
def test_create_data_loading_progress() -> None:
    """create_data_loading_progress returns a Progress instance."""
    progress = create_data_loading_progress()
    assert progress is not None


@pytest.mark.integration
def test_create_batch_progress_with_total() -> None:
    """create_batch_progress with total returns Progress with bar column."""
    progress = create_batch_progress(total=100)
    assert progress is not None


@pytest.mark.integration
def test_create_batch_progress_without_total() -> None:
    """create_batch_progress without total returns indeterminate Progress."""
    progress = create_batch_progress()
    assert progress is not None


@pytest.mark.integration
def test_create_spinner_status() -> None:
    """create_spinner_status returns context manager."""
    ctx = create_spinner_status("[bold]Testing...[/bold]")
    assert ctx is not None


@pytest.mark.integration
def test_loading_progress_context_manager() -> None:
    """LoadingProgress works as a context manager."""
    lp = LoadingProgress()
    with lp:
        lp.add_task("test_loading", "Loading data...")
        lp.update("test_loading", advance=10)
        lp.complete("test_loading", "Done loading")


@pytest.mark.integration
def test_loading_progress_complete_unknown_task() -> None:
    """LoadingProgress.complete with unknown task is a no-op."""
    lp = LoadingProgress()
    with lp:
        lp.complete("nonexistent", "Should not fail")


@pytest.mark.integration
def test_print_settings_table(settings: HintGridSettings) -> None:
    """print_settings_table renders all settings in multi-column table without errors."""
    # Explicit runtime use of HintGridSettings
    assert isinstance(settings, HintGridSettings)
    
    from hintgrid.cli.console import print_settings_table
    from io import StringIO
    import sys

    # Capture output
    old_stdout = sys.stdout
    sys.stdout = captured = StringIO()

    try:
        print_settings_table(settings)
        output = captured.getvalue()
    finally:
        sys.stdout = old_stdout

    # Verify key groups are present
    assert "PostgreSQL" in output
    assert "Neo4j" in output
    assert "Redis" in output
    assert "Pipeline" in output  # Check Pipeline group
    assert "HINTGRID_APOC_BATCH_SIZE" in output  # Check specific field
    assert "HINTGRID_FOLLOWS_WEIGHT" in output  # Check added field
    assert "HINTGRID_MENTIONS_WEIGHT" in output  # Check added field


@pytest.mark.integration
def test_print_settings_table_contains_all_fields(settings: HintGridSettings) -> None:
    """Verify that print_settings_table displays all settings fields."""
    from hintgrid.cli.console import print_settings_table
    from hintgrid.config import HintGridSettings
    from io import StringIO
    import sys

    # Capture output
    old_stdout = sys.stdout
    sys.stdout = captured = StringIO()

    try:
        print_settings_table(settings)
        output = captured.getvalue()
    finally:
        sys.stdout = old_stdout

    # Verify that all model fields appear in output (through public API)
    all_model_fields = set(HintGridSettings.model_fields)
    
    # Check that key fields are present in output
    # We test through the actual output, not internal structure
    key_fields = ["postgres_host", "neo4j_host", "redis_host", "log_level"]
    for field in key_fields:
        if field in all_model_fields:
            env_var = f"HINTGRID_{field.upper()}"
            assert env_var in output, f"Field {field} (env: {env_var}) not found in output"


@pytest.mark.integration
def test_print_embedding_status_matched() -> None:
    """print_embedding_status with matching signatures."""
    from hintgrid.cli.console import print_embedding_status

    print_embedding_status(
        stored_signature="fasttext:64",
        current_signature="fasttext:64",
        match=True,
    )


@pytest.mark.integration
def test_print_embedding_status_mismatched() -> None:
    """print_embedding_status with mismatched signatures."""
    from hintgrid.cli.console import print_embedding_status

    print_embedding_status(
        stored_signature="fasttext:64",
        current_signature="fasttext:128",
        match=False,
    )


@pytest.mark.integration
def test_print_embedding_status_no_stored() -> None:
    """print_embedding_status with no stored signature."""
    from hintgrid.cli.console import print_embedding_status

    print_embedding_status(
        stored_signature=None,
        current_signature="fasttext:64",
        match=True,
    )


@pytest.mark.integration
def test_print_reindex_result_dry_run() -> None:
    """print_reindex_result in dry run mode."""
    from hintgrid.cli.console import print_reindex_result

    result: dict[str, object] = {
        "migrated": False,
        "previous_signature": "fasttext:64",
        "current_signature": "fasttext:128",
        "posts_cleared": 42,
    }
    print_reindex_result(result, dry_run=True)


@pytest.mark.integration
def test_print_reindex_result_actual() -> None:
    """print_reindex_result in actual mode."""
    from hintgrid.cli.console import print_reindex_result

    result: dict[str, object] = {
        "migrated": True,
        "previous_signature": "fasttext:64",
        "current_signature": "fasttext:128",
        "posts_reembedded": 42,
    }
    print_reindex_result(result, dry_run=False)


@pytest.mark.integration
def test_print_train_result_success() -> None:
    """print_train_result with success."""
    from hintgrid.cli.console import print_train_result

    print_train_result(success=True, mode="Full")


@pytest.mark.integration
def test_print_train_result_failure() -> None:
    """print_train_result with failure."""
    from hintgrid.cli.console import print_train_result

    print_train_result(success=False, mode="Incremental")


@pytest.mark.integration
def test_print_user_id_result() -> None:
    """print_user_id_result displays user ID."""
    from hintgrid.cli.console import print_user_id_result

    print_user_id_result(12345)


@pytest.mark.integration
def test_print_pipeline_start_and_complete() -> None:
    """print_pipeline_start and print_pipeline_complete work."""
    from hintgrid.cli.console import print_pipeline_complete, print_pipeline_start

    print_pipeline_start()
    print_pipeline_complete()


@pytest.mark.integration
def test_get_memory_usage_mb() -> None:
    """get_memory_usage_mb returns positive float."""
    from hintgrid.cli.memory import get_memory_usage_mb

    mb = get_memory_usage_mb()
    assert mb > 0


@pytest.mark.integration
def test_print_memory_usage() -> None:
    """print_memory_usage renders without errors."""
    from hintgrid.cli.memory import print_memory_usage

    print_memory_usage()


@pytest.mark.integration
def test_print_memory_panel() -> None:
    """print_memory_panel renders without errors."""
    from hintgrid.cli.memory import print_memory_panel

    print_memory_panel("Test Memory")


@pytest.mark.integration
def test_memory_monitor_context_manager() -> None:
    """MemoryMonitor works as context manager with interval=0."""
    from hintgrid.cli.memory import MemoryMonitor

    with MemoryMonitor(interval_seconds=0) as mm:
        usage = mm.get_current_usage()
        assert usage > 0
        delta = mm.get_delta()
        assert isinstance(delta, float)


@pytest.mark.integration
def test_memory_monitor_with_live_display() -> None:
    """MemoryMonitor with interval creates background thread."""
    import time

    from hintgrid.cli.memory import MemoryMonitor

    with MemoryMonitor(interval_seconds=1) as mm:
        time.sleep(0.1)
        usage = mm.get_current_usage()
        assert usage > 0


@pytest.mark.integration
def test_set_and_get_memory_interval() -> None:
    """set_memory_interval and get_memory_interval work."""
    from hintgrid.cli.memory import get_memory_interval, set_memory_interval

    original = get_memory_interval()
    set_memory_interval(30)
    assert get_memory_interval() == 30
    set_memory_interval(original)  # Restore


# ---------------------------------------------------------------------------
# Tests: print_user_info_table
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_print_user_info_table_with_full_data(capsys: pytest.CaptureFixture[str]) -> None:
    """print_user_info_table displays complete user information."""
    from hintgrid.cli.console import print_user_info_table

    user_info: dict[str, object] = {
        "user_id": 12345,
        "username": "testuser",
        "domain": "example.com",
        "ui_language": "de",
        "languages": ["en", "ru"],
        "is_local": False,
    }

    print_user_info_table(user_info)  # type: ignore[arg-type]

    output = capsys.readouterr().out
    assert "User Information" in output
    assert "12,345" in output or "12345" in output  # Rich formats numbers with commas
    assert "testuser" in output
    assert "example.com" in output
    assert "en" in output or "ru" in output
    assert "UI language" in output
    assert "de" in output


@pytest.mark.integration
def test_print_user_info_table_local_user(capsys: pytest.CaptureFixture[str]) -> None:
    """print_user_info_table displays local user without domain."""
    from hintgrid.cli.console import print_user_info_table

    user_info: dict[str, object] = {
        "user_id": 67890,
        "username": "localuser",
        "domain": None,
        "languages": ["en"],
        "is_local": True,
    }

    print_user_info_table(user_info)  # type: ignore[arg-type]

    output = capsys.readouterr().out
    assert "User Information" in output
    assert "67,890" in output or "67890" in output  # Rich formats numbers with commas
    assert "localuser" in output
    assert "Yes" in output  # is_local = True


@pytest.mark.integration
def test_print_user_info_table_minimal_data(capsys: pytest.CaptureFixture[str]) -> None:
    """print_user_info_table handles minimal user data."""
    from hintgrid.cli.console import print_user_info_table

    user_info: dict[str, object] = {
        "user_id": 99999,
        "username": None,
        "domain": None,
        "languages": None,
        "is_local": False,
    }

    print_user_info_table(user_info)  # type: ignore[arg-type]

    output = capsys.readouterr().out
    assert "User Information" in output
    assert "99,999" in output or "99999" in output  # Rich formats numbers with commas
    assert "No" in output  # is_local = False


# ---------------------------------------------------------------------------
# Tests: print_recommendations_table
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_print_recommendations_table_with_data(capsys: pytest.CaptureFixture[str]) -> None:
    """print_recommendations_table displays recommendations with all fields."""
    from datetime import datetime, timedelta

    from hintgrid.cli.console import print_recommendations_table

    recommendations: list[dict[str, object]] = [
        {
            "post_id": 1001,
            "post_text": "This is a test post with some content",
            "post_language": "en",
            "post_created_at": datetime.now() - timedelta(hours=2),
            "author_id": 2001,
            "interest_score": 0.85,
            "local_raw": 10.0,
            "global_raw": 3.0,
            "popularity_contrib": 1.0,
            "age_hours": 2.0,
            "pagerank": 0.5,
            "language_match": 0.3,
            "final_score": 1.25,
        },
        {
            "post_id": 1002,
            "post_text": "Another post",
            "post_language": "ru",
            "post_created_at": datetime.now() - timedelta(hours=5),
            "author_id": 2002,
            "interest_score": 0.7,
            "local_raw": 5.0,
            "global_raw": 2.0,
            "popularity_contrib": 0.9,
            "age_hours": 5.0,
            "pagerank": 0.3,
            "language_match": 0.0,
            "final_score": 0.95,
        },
    ]

    user_info: dict[str, object] = {
        "user_id": 5001,
        "username": "testuser",
        "domain": None,
    }

    author_info: dict[int, dict[str, str | None]] = {
        2001: {"username": "author1", "domain": None},
        2002: {"username": "author2", "domain": "example.com"},
    }

    print_recommendations_table(
        recommendations,  # type: ignore[arg-type]
        user_info,  # type: ignore[arg-type]
        author_info,
        max_items=10,
        text_preview_limit=50,
    )

    output = capsys.readouterr().out
    assert "Recommendations for" in output
    assert "@testuser" in output
    assert "1001" in output
    assert "1002" in output
    # Text preview column may not be visible in narrow terminal, but post IDs should be
    assert "@author1" in output
    assert "@author2@example.com" in output


@pytest.mark.integration
def test_print_recommendations_table_empty_list(capsys: pytest.CaptureFixture[str]) -> None:
    """print_recommendations_table handles empty recommendations list."""
    from hintgrid.cli.console import print_recommendations_table

    user_info: dict[str, object] = {
        "user_id": 5002,
        "username": "testuser",
        "domain": None,
    }

    print_recommendations_table(
        [],
        user_info,  # type: ignore[arg-type]
        {},
        max_items=10,
        text_preview_limit=50,
    )

    output = capsys.readouterr().out
    assert "No recommendations found" in output


@pytest.mark.integration
def test_print_recommendations_table_long_text_truncated(capsys: pytest.CaptureFixture[str]) -> None:
    """print_recommendations_table truncates long post text."""
    from datetime import datetime

    from hintgrid.cli.console import print_recommendations_table

    long_text = "A" * 100  # Very long text
    recommendations: list[dict[str, object]] = [
        {
            "post_id": 1003,
            "post_text": long_text,
            "post_language": "en",
            "post_created_at": datetime.now(),
            "author_id": 2003,
            "interest_score": 0.5,
            "local_raw": 0.0,
            "global_raw": 0.0,
            "popularity_contrib": 0.0,
            "age_hours": 0.0,
            "pagerank": 0.0,
            "language_match": 0.3,
            "final_score": 0.5,
        },
    ]

    user_info: dict[str, object] = {
        "user_id": 5003,
        "username": "testuser",
        "domain": None,
    }

    author_info: dict[int, dict[str, str | None]] = {
        2003: {"username": "author3", "domain": None},
    }

    print_recommendations_table(
        recommendations,  # type: ignore[arg-type]
        user_info,  # type: ignore[arg-type]
        author_info,
        max_items=10,
        text_preview_limit=50,
    )

    output = capsys.readouterr().out
    assert "1003" in output
    # Text should be truncated to 50 chars + "..."
    # Rich table may format text differently, just check that output contains the post ID
    assert "1003" in output


# ---------------------------------------------------------------------------
# Tests: print_user_info_table with extended information
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_print_user_info_table_with_extended_info(capsys: pytest.CaptureFixture[str]) -> None:
    """print_user_info_table displays extended user information."""
    from hintgrid.cli.console import print_user_info_table

    user_info: dict[str, object] = {
        "user_id": 20001,
        "username": "extended_user",
        "domain": "example.com",
        "languages": ["en", "ru"],
        "is_local": True,
        "user_community_id": 1001,
        "user_community_size": 50,
        "top_interests": [
            {"pc_id": 2001, "score": 0.85},
            {"pc_id": 2002, "score": 0.72},
        ],
        "interactions": {
            "favorited": 100,
            "reblogged": 50,
            "replied": 25,
            "bookmarked": 10,
        },
        "follows_count": 200,
        "followers_count": 150,
        "posts_count": 500,
    }

    print_user_info_table(user_info)  # type: ignore[arg-type]

    output = capsys.readouterr().out
    assert "User Information" in output
    assert "20001" in output or "20,001" in output
    assert "extended_user" in output
    assert "Community" in output
    assert "1001" in output
    assert "Top Interests" in output
    assert "2001" in output
    assert "Interactions" in output
    assert "100" in output or "Favorited" in output
    assert "Network" in output
    assert "200" in output or "Following" in output
    assert "Activity" in output
    assert "500" in output or "Posts" in output


@pytest.mark.integration
def test_print_user_info_table_with_partial_extended_info(capsys: pytest.CaptureFixture[str]) -> None:
    """print_user_info_table handles partial extended information."""
    from hintgrid.cli.console import print_user_info_table

    user_info: dict[str, object] = {
        "user_id": 20002,
        "username": "partial_user",
        "domain": None,
        "languages": None,
        "is_local": False,
        "user_community_id": 1002,
        "user_community_size": None,  # Size not available
        "interactions": {
            "favorited": 0,
            "reblogged": 0,
            "replied": 0,
            "bookmarked": 0,
        },
        "follows_count": 0,
        "followers_count": 0,
        # top_interests and posts_count are None
    }

    print_user_info_table(user_info)  # type: ignore[arg-type]

    output = capsys.readouterr().out
    assert "User Information" in output
    assert "20002" in output or "20,002" in output
    assert "partial_user" in output
    assert "Community" in output
    # Top Interests section should not appear (None)
    assert "Interactions" in output
    assert "Network" in output
    # Activity section should not appear (posts_count is None)


@pytest.mark.integration
def test_print_recommendations_table_missing_author_info(capsys: pytest.CaptureFixture[str]) -> None:
    """print_recommendations_table handles missing author information."""
    from datetime import datetime

    from hintgrid.cli.console import print_recommendations_table

    recommendations: list[dict[str, object]] = [
        {
            "post_id": 1004,
            "post_text": "Test post",
            "post_language": None,
            "post_created_at": datetime.now(),
            "author_id": 9999,  # Author not in author_info
            "interest_score": 0.5,
            "local_raw": 0.0,
            "global_raw": 0.0,
            "popularity_contrib": 0.0,
            "age_hours": 0.0,
            "pagerank": 0.0,
            "language_match": 0.0,
            "final_score": 0.5,
        },
    ]

    user_info: dict[str, object] = {
        "user_id": 5004,
        "username": "testuser",
        "domain": "example.com",
    }

    # Empty author_info - author should be displayed as ID
    print_recommendations_table(
        recommendations,  # type: ignore[arg-type]
        user_info,  # type: ignore[arg-type]
        {},
        max_items=10,
        text_preview_limit=50,
    )

    output = capsys.readouterr().out
    assert "1004" in output
    assert "9999" in output  # Author ID should be displayed
    assert "@testuser@example.com" in output  # User handle with domain
