"""CLI command execution logic for HintGrid."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from hintgrid.cli.console import (
    console,
    print_embedding_status,
    print_error,
    print_reindex_result,
    print_settings_table,
    print_success,
    print_train_result,
    print_warning,
)
from hintgrid.cli.memory import MemoryMonitor
from hintgrid.cli.progress_display import create_batch_progress
from hintgrid.cli.shutdown import ShutdownManager

if TYPE_CHECKING:
    from hintgrid.app import HintGridApp

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_INTERRUPTED = 130


def _parse_since_date(since_str: str | None) -> datetime | None:
    """Parse since_date string to datetime.

    Supports formats:
    - ISO date: "2024-01-15"
    - Relative days: "30d" (30 days ago)
    """
    if since_str is None:
        return None

    # Check for relative format (e.g., "30d")
    if since_str.endswith("d"):
        try:
            days = int(since_str[:-1])
            return datetime.now() - timedelta(days=days)
        except ValueError:
            pass

    # Try ISO date format
    try:
        return datetime.fromisoformat(since_str)
    except ValueError:
        return None


def _run_with_app(
    overrides: dict[str, object],
    verbose: bool,
    handler: type[CommandHandler],
    shutdown: ShutdownManager | None = None,
) -> int:
    """Run a command with app context and error handling.

    Args:
        overrides: CLI setting overrides
        verbose: Whether to enable verbose logging
        handler: Command handler class to execute
        shutdown: Optional ShutdownManager for graceful Ctrl+C handling.
                  If provided, installs signal handlers and displays
                  shutdown summary on interrupt.
    """
    from hintgrid.app import HintGridApp
    from hintgrid.clients import Neo4jClient, PostgresClient, RedisClient
    from hintgrid.config import CliOverrides, HintGridSettings
    from hintgrid.exceptions import HintGridError
    from hintgrid.logging import get_logger, setup_logging

    settings = HintGridSettings()
    settings = CliOverrides(overrides).apply(settings)

    if verbose:
        settings = settings.model_copy(update={"log_level": "DEBUG"})

    setup_logging(settings)
    logger = get_logger(__name__)

    if shutdown is None:
        shutdown = ShutdownManager()

    app: HintGridApp | None = None

    try:
        with (
            shutdown,
            PostgresClient.from_settings(settings) as pg,
            Neo4jClient.from_settings(settings) as neo4j,
            RedisClient.from_settings(settings) as redis_client,
        ):
            app = HintGridApp(
                neo4j=neo4j,
                postgres=pg,
                redis=redis_client,
                settings=settings,
            )
            return handler.execute(app)

    except HintGridError as exc:
        print_error(str(exc))
        if verbose:
            logger.debug("Full traceback:", exc_info=True)
        return exc.exit_code

    except KeyboardInterrupt:
        console.print()
        if len(shutdown.steps) > 0:
            # Graceful shutdown with step tracking (run command)
            state = None
            if app is not None:
                from contextlib import suppress
                with suppress(Exception):
                    state = app.state_store.load()
            shutdown.display_shutdown_summary(state)
        else:
            # Simple interrupt (other commands)
            print_warning("Pipeline interrupted by user")
        return EXIT_INTERRUPTED

    except Exception as exc:
        print_error(f"Unexpected error: {exc}")
        if verbose:
            console.print_exception()
        else:
            console.print("[dim]Run with --verbose for full traceback[/dim]")
        return EXIT_ERROR


class CommandHandler:
    """Base class for command handlers."""

    @staticmethod
    def execute(app: HintGridApp) -> int:
        """Execute the command. Override in subclass."""
        raise NotImplementedError


def execute_run(
    overrides: dict[str, object],
    dry_run: bool,
    user_id: int | None,
    do_train: bool,
    verbose: bool,
    memory_interval: int = 10,
) -> int:
    """Execute the 'run' command."""
    shutdown = ShutdownManager()

    class RunHandler(CommandHandler):
        @staticmethod
        def execute(app: HintGridApp) -> int:
            with MemoryMonitor(interval_seconds=memory_interval):
                if do_train:
                    with console.status("[bold blue]Running incremental training...[/bold blue]"):
                        success = app.train_incremental()
                    if not success:
                        print_warning("Incremental training failed, continuing with existing models")
                    else:
                        print_success("Incremental training completed")

                app.run_full_pipeline(
                    dry_run=dry_run, user_id=user_id, shutdown=shutdown,
                )

            # After pipeline returns (possibly after graceful shutdown)
            if shutdown.shutdown_requested:
                state = app.state_store.load()
                shutdown.display_shutdown_summary(state)
                return EXIT_INTERRUPTED

            return EXIT_OK

    return _run_with_app(overrides, verbose, RunHandler, shutdown=shutdown)


def execute_export(
    overrides: dict[str, object],
    filename: str,
    user_id: int,
    verbose: bool,
    memory_interval: int = 10,
) -> int:
    """Execute the 'export' command."""

    class ExportHandler(CommandHandler):
        @staticmethod
        def execute(app: HintGridApp) -> int:
            with MemoryMonitor(interval_seconds=memory_interval):
                with console.status(f"[bold blue]Exporting state to {filename}...[/bold blue]"):
                    app.export_state(filename, user_id)
                print_success(f"State exported to [bold]{filename}[/bold]")
            return EXIT_OK

    return _run_with_app(overrides, verbose, ExportHandler)


def execute_train(
    overrides: dict[str, object],
    full: bool,
    since: str | None,
    verbose: bool,
    memory_interval: int = 10,
) -> int:
    """Execute the 'train' command."""

    class TrainHandler(CommandHandler):
        @staticmethod
        def execute(app: HintGridApp) -> int:
            with MemoryMonitor(interval_seconds=memory_interval):
                if full:
                    mode = "Full"
                    since_date = _parse_since_date(since)
                    with console.status(f"[bold blue]Running {mode.lower()} training...[/bold blue]"):
                        success = app.train_full(since_date=since_date)
                else:
                    mode = "Incremental"
                    with console.status(f"[bold blue]Running {mode.lower()} training...[/bold blue]"):
                        success = app.train_incremental()

                print_train_result(success, mode)
            return EXIT_OK if success else EXIT_ERROR

    return _run_with_app(overrides, verbose, TrainHandler)


def execute_clean(
    overrides: dict[str, object],
    verbose: bool,
    memory_interval: int = 10,
    *,
    graph: bool = False,
    redis: bool = False,
    models: bool = False,
    embeddings: bool = False,
    clusters: bool = False,
    similarity: bool = False,
    interests: bool = False,
    interactions: bool = False,
    recommendations: bool = False,
    fasttext_state: bool = False,
) -> int:
    """Execute the 'clean' command.

    When no target flags are set, cleans everything (backward-compatible).
    When one or more flags are set, only the specified targets are cleaned.

    Computed data flags clean only computed data, preserving source data.
    """
    basic_flags = graph or redis or models
    computed_flags = (
        embeddings or clusters or similarity or interests
        or interactions or recommendations or fasttext_state
    )
    clean_all = not basic_flags and not computed_flags

    class CleanHandler(CommandHandler):
        @staticmethod
        def execute(app: HintGridApp) -> int:
            with MemoryMonitor(interval_seconds=memory_interval):
                cleaned: list[str] = []
                if clean_all:
                    batch_progress = create_batch_progress(settings=app.settings)
                    with batch_progress:
                        app.clean(progress=batch_progress)
                    cleaned = ["Neo4j", "Redis", "model files"]
                else:
                    # Rich Progress conflicts with console.status (both use Live); use a bar when
                    # clean_graph runs (graph delete is the long batched Neo4j step).
                    clean_kwargs = {
                        "graph": graph,
                        "redis": redis,
                        "models": models,
                        "embeddings": embeddings,
                        "clusters": clusters,
                        "similarity": similarity,
                        "interests": interests,
                        "interactions": interactions,
                        "recommendations": recommendations,
                        "fasttext_state": fasttext_state,
                    }
                    if graph:
                        batch_progress = create_batch_progress(settings=app.settings)
                        with batch_progress:
                            app.clean(**clean_kwargs, progress=batch_progress)
                    else:
                        with console.status("[bold red]Cleaning data...[/bold red]"):
                            app.clean(**clean_kwargs, progress=None)
                    if graph:
                        cleaned.append("Neo4j")
                    if redis:
                        cleaned.append("Redis")
                    if models:
                        cleaned.append("model files")
                    if fasttext_state:
                        cleaned.append("FastText state")
                    if embeddings:
                        cleaned.append("embeddings")
                    elif similarity:
                        cleaned.append("similarity graph")
                    if clusters:
                        with console.status("[bold red]Cleaning clusters...[/bold red]"):
                            # Check if post clusters already cleaned via embeddings/similarity
                            if (embeddings or similarity) and not graph:
                                app.clean_clusters(posts=False, users=True)
                            else:
                                app.clean_clusters()
                        cleaned.append("clusters")
                    if interests and not (embeddings or similarity or clusters):
                        with console.status("[bold red]Cleaning interests...[/bold red]"):
                            app.clean_interests()
                        cleaned.append("interests")
                    if interactions:
                        cleaned.append("interactions")
                    if recommendations and not clusters:
                        with console.status("[bold red]Cleaning recommendations...[/bold red]"):
                            app.clean_recommendations()
                        cleaned.append("recommendations")
                print_success(f"Data cleaned from {', '.join(cleaned)}")
            return EXIT_OK

    return _run_with_app(overrides, verbose, CleanHandler)


def execute_get_user_info(
    overrides: dict[str, object],
    handle: str,
    verbose: bool,
) -> int:
    """Execute the 'get-user-info' command."""

    class GetUserInfoHandler(CommandHandler):
        @staticmethod
        def execute(app: HintGridApp) -> int:
            from hintgrid.cli.console import print_user_info_table
            from hintgrid.pipeline.stats import get_extended_user_info

            # Get user ID first
            user_id_result = app.get_user_id(handle)
            if user_id_result is None:
                print_error("User not found")
                return EXIT_ERROR

            # Get extended user info
            user_info = get_extended_user_info(
                app.neo4j,
                app.postgres,
                user_id_result,
                redis=app.redis,
            )
            if user_info is None:
                print_error("User not found in database")
                return EXIT_ERROR

            # Print user info table
            print_user_info_table(user_info)
            return EXIT_OK

    return _run_with_app(overrides, verbose, GetUserInfoHandler)


def execute_get_post_info(
    overrides: dict[str, object],
    post_ref: str,
    verbose: bool,
) -> int:
    """Execute the 'get-post-info' command."""

    class GetPostInfoHandler(CommandHandler):
        @staticmethod
        def execute(app: HintGridApp) -> int:
            from hintgrid.cli.console import print_post_info_table
            from hintgrid.pipeline.post_info import get_extended_post_info

            resolved, resolve_err = app.postgres.resolve_status_id(post_ref)
            if resolve_err is not None:
                print_error(resolve_err)
                return EXIT_ERROR
            if resolved is None:
                print_error("Post not found")
                return EXIT_ERROR

            post_info = get_extended_post_info(app.neo4j, app.postgres, resolved)
            if post_info is None:
                print_error("Post not found in graph (load posts first)")
                return EXIT_ERROR

            print_post_info_table(post_info)
            return EXIT_OK

    return _run_with_app(overrides, verbose, GetPostInfoHandler)


def execute_validate(
    overrides: dict[str, object],
    verbose: bool,
    memory_interval: int = 10,
) -> int:
    """Execute the 'validate' command."""

    class ValidateHandler(CommandHandler):
        @staticmethod
        def execute(app: HintGridApp) -> int:
            with MemoryMonitor(interval_seconds=memory_interval):
                print_success("Configuration valid")
                console.print()

                # Print settings tree
                print_settings_table(app.settings)
                console.print()

                # Show embedding signature status
                status = app.check_embeddings()
                stored = status.get("stored_signature")
                current = status.get("current_signature", "")
                match = bool(status.get("match", False))

                print_embedding_status(
                    stored_signature=str(stored) if stored else None,
                    current_signature=str(current),
                    match=match,
                )

            return EXIT_OK

    return _run_with_app(overrides, verbose, ValidateHandler)


def execute_model_export(
    overrides: dict[str, object],
    output_path: str,
    mode: str,
    verbose: bool,
    memory_interval: int = 10,
) -> int:
    """Execute the 'model-export' command."""
    from pathlib import Path

    from hintgrid.embeddings.bundle import BundleMode, export_bundle

    class ModelExportHandler(CommandHandler):
        @staticmethod
        def execute(app: HintGridApp) -> int:
            with MemoryMonitor(interval_seconds=memory_interval):
                out = Path(output_path)
                bundle_mode: BundleMode = "full" if mode == "full" else "inference"
                with console.status(
                    f"[bold blue]Exporting {bundle_mode} model bundle...[/bold blue]"
                ):
                    result_path = export_bundle(
                        settings=app.settings,
                        neo4j=app.neo4j,
                        output_path=out,
                        mode=bundle_mode,
                    )
                size_mb = result_path.stat().st_size / (1024 * 1024)
                print_success(
                    f"Model bundle exported to [bold]{result_path}[/bold] "
                    f"({size_mb:.1f} MB, mode={bundle_mode})"
                )
            return EXIT_OK

    return _run_with_app(overrides, verbose, ModelExportHandler)


def execute_model_import(
    overrides: dict[str, object],
    archive_path: str,
    force: bool,
    verbose: bool,
    memory_interval: int = 10,
) -> int:
    """Execute the 'model-import' command."""
    from pathlib import Path

    from hintgrid.embeddings.bundle import import_bundle

    class ModelImportHandler(CommandHandler):
        @staticmethod
        def execute(app: HintGridApp) -> int:
            with MemoryMonitor(interval_seconds=memory_interval):
                archive = Path(archive_path)
                with console.status(
                    "[bold blue]Importing model bundle...[/bold blue]"
                ):
                    result = import_bundle(
                        settings=app.settings,
                        neo4j=app.neo4j,
                        archive_path=archive,
                        force=force,
                    )
                print_success(
                    f"Imported {result.mode} bundle v{result.version} "
                    f"({result.files_installed} files)"
                )
                if result.state_updated:
                    print_success("FastTextState updated in Neo4j")
            return EXIT_OK

    return _run_with_app(overrides, verbose, ModelImportHandler)


def execute_refresh(
    overrides: dict[str, object],
    verbose: bool,
    memory_interval: int = 10,
) -> int:
    """Execute the 'refresh' command.

    Lightweight interest refresh: applies decay to existing scores
    and recomputes only dirty (changed) communities.
    Falls back to full rebuild if no previous rebuild exists.
    """

    class RefreshHandler(CommandHandler):
        @staticmethod
        def execute(app: HintGridApp) -> int:
            with MemoryMonitor(interval_seconds=memory_interval):
                with console.status(
                    "[bold blue]Refreshing interests...[/bold blue]"
                ):
                    app.run_refresh()
                print_success("Interest refresh complete")
            return EXIT_OK

    return _run_with_app(overrides, verbose, RefreshHandler)


def execute_reindex(
    overrides: dict[str, object],
    dry_run: bool,
    verbose: bool,
    memory_interval: int = 10,
) -> int:
    """Execute the 'reindex' command."""

    class ReindexHandler(CommandHandler):
        @staticmethod
        def execute(app: HintGridApp) -> int:
            with MemoryMonitor(interval_seconds=memory_interval):
                status_msg = "[bold yellow]Analyzing...[/bold yellow]" if dry_run else "[bold blue]Reindexing embeddings...[/bold blue]"
                with console.status(status_msg):
                    result = app.reindex_embeddings(dry_run=dry_run)

                print_reindex_result(result, dry_run)
            return EXIT_OK

    return _run_with_app(overrides, verbose, ReindexHandler)
