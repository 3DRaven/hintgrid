"""Rich console utilities for HintGrid CLI."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Self, TypedDict

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from hintgrid.utils.coercion import coerce_float, coerce_int, coerce_str

import hintgrid.cli.progress_display as _progress_display

create_batch_progress = _progress_display.create_batch_progress
create_data_loading_progress = _progress_display.create_data_loading_progress
create_feed_generation_progress = _progress_display.create_feed_generation_progress
create_pipeline_progress = _progress_display.create_pipeline_progress
track_periodic_iterate_progress = _progress_display.track_periodic_iterate_progress

if TYPE_CHECKING:
    from collections.abc import Sequence
    from contextlib import AbstractContextManager

    from hintgrid.cli.shutdown import PipelineStep
    from hintgrid.clients.neo4j import Neo4jValue
    from hintgrid.config import HintGridSettings
    from hintgrid.pipeline.feed import RecommendationDetail
    from hintgrid.pipeline.stats import UserInfo
    from hintgrid.state import PipelineState

    from rich.progress import TaskID

logger = logging.getLogger(__name__)


class PipelineMetrics(TypedDict, total=False):
    """Metrics collected during a pipeline run for the summary panel."""

    total_duration_s: float
    load_duration_s: float
    analytics_duration_s: float
    feeds_duration_s: float
    user_count: int
    post_count: int
    interaction_count: int
    user_communities: int
    post_communities: int
    user_modularity: float
    post_modularity: float
    feeds_generated: int
    dry_run: bool
    warnings: list[str]


# Global console instance
console = Console()
error_console = Console(stderr=True)


class LoadingProgress:
    """Context manager for data loading progress display.

    Provides a simple interface for showing progress during data loading
    with support for multiple concurrent loading tasks.
    """

    def __init__(self, settings: HintGridSettings | None = None) -> None:
        self._progress = create_data_loading_progress(settings)
        self._tasks: dict[str, TaskID] = {}

    def __enter__(self) -> Self:
        self._progress.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        self._progress.stop()

    def add_task(
        self,
        name: str,
        description: str,
        total: int | None = None,
    ) -> None:
        """Add a new loading task.

        Args:
            name: Unique task identifier
            description: Rich-formatted description text
            total: Optional total item count for determinate progress bar.
                   When provided, rich shows percentage and ETA.
                   When None (default), shows indeterminate spinner.
        """
        task_id = self._progress.add_task(description, total=total)
        self._tasks[name] = task_id

    def update(self, name: str, advance: int = 1) -> None:
        """Update progress for a task."""
        if name in self._tasks:
            self._progress.advance(self._tasks[name], advance)

    def complete(self, name: str, message: str) -> None:
        """Mark a task as complete with a summary message."""
        if name in self._tasks:
            self._progress.update(
                self._tasks[name],
                description=f"[green]✓[/green] {message}",
            )


def create_spinner_status(message: str) -> AbstractContextManager[object]:
    """Create a spinner status context manager."""
    return console.status(message)


def print_success(message: str) -> None:
    """Print a success message with green checkmark."""
    console.print(f"[green]✓[/green] {message}")


def print_error(message: str) -> None:
    """Print an error message with red cross."""
    error_console.print(f"[red]✗[/red] {message}")


def print_warning(message: str) -> None:
    """Print a warning message with yellow warning sign."""
    console.print(f"[yellow]⚠[/yellow] {message}")


def print_info(message: str) -> None:
    """Print an info message with blue info sign."""
    console.print(f"[blue]i[/blue] {message}")


def print_step(step: int, total: int, message: str) -> None:
    """Print a pipeline step message."""
    console.print(f"[bold cyan][{step}/{total}][/bold cyan] {message}")


def print_header(title: str) -> None:
    """Print a header with decorative border."""
    console.print()
    console.print(Panel(f"[bold]{title}[/bold]", border_style="blue"))
    console.print()


def print_database_stats(stats: dict[str, dict[str, object]]) -> None:
    """Print database statistics table.

    Args:
        stats: Dictionary mapping table names to their statistics
              Each value should have: max_id, max_date, total_count
    """
    from hintgrid.state import INITIAL_CURSOR

    table = Table(
        title="[bold]📊 PostgreSQL Database Statistics[/bold]",
        border_style="cyan",
        show_header=True,
        header_style="bold",
        show_edge=True,
    )
    table.add_column("Table", style="cyan", no_wrap=True)
    table.add_column("Max ID", style="white", justify="right", no_wrap=True)
    table.add_column("Max Date", style="white", no_wrap=True)
    table.add_column("Total Count", style="green", justify="right", no_wrap=True)

    # Table display names
    table_names = {
        "statuses": "statuses",
        "favourites": "favourites",
        "blocks": "blocks",
        "mutes": "mutes",
        "bookmarks": "bookmarks",
        "status_stats": "status_stats",
        "accounts": "accounts",
    }

    for table_key in table_names:
        if table_key not in stats:
            continue

        table_stat = stats[table_key]
        max_id = table_stat.get("max_id")
        max_date = table_stat.get("max_date")
        total_count = table_stat.get("total_count", 0)

        # Format max_id - use coerce_int instead of isinstance
        if max_id is None or max_id == INITIAL_CURSOR:
            max_id_str = "[dim]—[/dim]"
        else:
            max_id_int = coerce_int(max_id, 0)
            max_id_str = f"{max_id_int:,}" if max_id_int >= 1000 else str(max_id_int)

        # Format max_date - check for datetime using hasattr instead of isinstance
        if max_date is None:
            max_date_str = "[dim]—[/dim]"
        elif hasattr(max_date, "strftime") and hasattr(max_date, "tzinfo"):
            # max_date is datetime-like
            if max_date.tzinfo is not None:
                max_date_str = max_date.strftime("%Y-%m-%d %H:%M:%S %Z")
            else:
                max_date_str = max_date.strftime("%Y-%m-%d %H:%M:%S")
        else:
            max_date_str = str(max_date)

        # Format total_count - use coerce_int instead of isinstance
        total_count_int = coerce_int(total_count, 0)
        total_count_str = (
            f"{total_count_int:,}" if total_count_int >= 1000 else str(total_count_int)
        )

        table.add_row(
            table_names[table_key],
            max_id_str,
            max_date_str,
            total_count_str,
        )

    console.print()
    console.print(table)
    console.print()


_SENSITIVE_FIELDS = frozenset(
    {
        "postgres_password",
        "neo4j_password",
        "redis_password",
        "llm_api_key",
    }
)

# Settings groups: (title, color, field_names)
_SETTINGS_GROUPS: list[tuple[str, str, list[str]]] = [
    (
        "🗄️  PostgreSQL",
        "green",
        [
            "postgres_host",
            "postgres_port",
            "postgres_database",
            "postgres_user",
            "postgres_password",
            "postgres_schema",
            "pg_pool_min_size",
            "pg_pool_max_size",
            "pg_pool_timeout_seconds",
        ],
    ),
    (
        "🔷 Neo4j",
        "blue",
        [
            "neo4j_host",
            "neo4j_port",
            "neo4j_username",
            "neo4j_password",
            "neo4j_worker_label",
            "neo4j_ready_retries",
            "neo4j_ready_sleep_seconds",
        ],
    ),
    (
        "🔴 Redis",
        "red",
        [
            "redis_host",
            "redis_port",
            "redis_db",
            "redis_password",
            "redis_score_tolerance",
            "redis_namespace",
        ],
    ),
    (
        "🧠 Embeddings",
        "magenta",
        [
            "llm_provider",
            "llm_base_url",
            "llm_model",
            "llm_dimensions",
            "llm_timeout",
            "llm_max_retries",
            "llm_batch_size",
            "llm_api_key",
            "min_embedding_tokens",
            "embedding_skip_percentile",
        ],
    ),
    (
        "📝 FastText",
        "cyan",
        [
            "fasttext_vector_size",
            "fasttext_window",
            "fasttext_min_count",
            "fasttext_max_vocab_size",
            "fasttext_epochs",
            "fasttext_bucket",
            "fasttext_min_documents",
            "fasttext_model_path",
            "fasttext_quantize",
            "fasttext_quantize_qdim",
            "fasttext_training_workers",
        ],
    ),
    (
        "🔧 Pipeline",
        "yellow",
        [
            "batch_size",
            "load_since",
            "max_retries",
            "apoc_batch_size",
            "checkpoint_interval",
            "feed_workers",
            "loader_workers",
        ],
    ),
    (
        "👥 Clustering",
        "green",
        [
            "user_communities",
            "post_communities",
            "leiden_resolution",
            "leiden_max_levels",
            "leiden_diagnostics_enabled",
            "knn_neighbors",
            "knn_self_neighbor_offset",
            "similarity_threshold",
            "similarity_recency_days",
            "similarity_iterate_batch_size",
        ],
    ),
    (
        "🎯 Scoring",
        "magenta",
        [
            "likes_weight",
            "reblogs_weight",
            "replies_weight",
            "bookmark_weight",
            "follows_weight",
            "mentions_weight",
            "serendipity_probability",
            "serendipity_limit",
            "serendipity_score",
            "serendipity_based_on",
            "interests_ttl_days",
            "interests_min_favourites",
            "decay_half_life_days",
            "ctr_enabled",
            "ctr_weight",
            "min_ctr",
            "ctr_smoothing",
            "pagerank_enabled",
            "pagerank_weight",
            "pagerank_damping_factor",
            "pagerank_max_iterations",
            "community_similarity_enabled",
            "community_similarity_top_k",
        ],
    ),
    (
        "📰 Feed",
        "cyan",
        [
            "feed_size",
            "feed_days",
            "feed_ttl",
            "feed_score_multiplier",
            "feed_score_decimals",
            "personalized_interest_weight",
            "personalized_popularity_weight",
            "personalized_recency_weight",
            "cold_start_popularity_weight",
            "cold_start_recency_weight",
            "cold_start_fallback",
            "cold_start_limit",
            "popularity_smoothing",
            "recency_smoothing",
            "recency_numerator",
            "active_user_days",
            "feed_force_refresh",
            "language_match_weight",
        ],
    ),
    (
        "✂️  Pruning",
        "yellow",
        [
            "similarity_pruning",
            "prune_after_clustering",
            "prune_similarity_threshold",
            "prune_days",
        ],
    ),
    (
        "📤 Export",
        "blue",
        [
            "export_max_items",
            "text_preview_limit",
            "community_interest_limit",
            "community_member_sample",
            "community_sample_limit",
            "graph_sample_limit",
        ],
    ),
    (
        "📡 Public Timelines",
        "green",
        [
            "public_feed_size",
            "public_feed_enabled",
            "public_feed_strategy",
            "public_timeline_key",
            "local_timeline_key",
        ],
    ),
    ("🐘 Mastodon", "green", ["mastodon_public_visibility", "mastodon_account_lookup_limit"]),
    (
        "📋 Logging",
        "dim",
        ["log_level", "log_file", "progress_output", "progress_poll_interval_seconds"],
    ),
]


def _format_setting_value(field: str, value: object) -> str:
    """Format a single setting value for table display."""
    if field in _SENSITIVE_FIELDS and value is not None:
        return "[dim]●●●●●[/dim]"
    if value is None:
        return "[dim]—[/dim]"
    # Check for bool using hasattr instead of isinstance
    if hasattr(value, "__bool__") and type(value).__name__ == "bool":
        return "[green]✓[/green]" if value else "[red]✗[/red]"
    # Use coerce_int for numeric values
    value_int = coerce_int(value, -1)
    if value_int >= 1000 and value_int != -1:
        return f"{value_int:,}"
    return str(value)


def _format_setting_name(field: str) -> str:
    """Format field name showing full env var and CLI flag."""
    env_var = f"HINTGRID_{field.upper()}"
    cli_flag = f"--{field.replace('_', '-')}"
    return f"{env_var}\n[dim]{cli_flag}[/dim]"


def print_settings_table(settings: HintGridSettings) -> None:
    """Print all settings with full env var and CLI flag names.

    Auto-detects any HintGridSettings field not listed in _SETTINGS_GROUPS
    and appends it to an 'Other' section so new parameters are never hidden.
    """
    from hintgrid.config import HintGridSettings

    table = Table(
        title="[bold]📊 HintGrid Configuration[/bold]",
        border_style="blue",
        show_header=True,
        header_style="bold",
        pad_edge=True,
        expand=True,
    )
    table.add_column("Parameter", style="cyan", no_wrap=True, ratio=4)
    table.add_column("Value", style="white", no_wrap=True, ratio=2)

    # Collect all fields already assigned to a group
    grouped_fields: set[str] = set()
    for _title, _color, fields in _SETTINGS_GROUPS:
        grouped_fields.update(fields)

    # Auto-detect uncategorized fields
    all_model_fields = set(HintGridSettings.model_fields)
    uncategorized = sorted(all_model_fields - grouped_fields)

    for title, color, fields in _SETTINGS_GROUPS:
        table.add_row(f"[bold {color}]{title}[/bold {color}]", "")
        for idx, f in enumerate(fields):
            table.add_row(
                _format_setting_name(f),
                _format_setting_value(f, getattr(settings, f)),
                end_section=(idx == len(fields) - 1),
            )

    if uncategorized:
        table.add_row("[bold dim]📦 Other[/bold dim]", "")
        for idx, f in enumerate(uncategorized):
            table.add_row(
                _format_setting_name(f),
                _format_setting_value(f, getattr(settings, f)),
                end_section=(idx == len(uncategorized) - 1),
            )

    console.print()
    console.print(table)
    console.print()


def print_embedding_status(
    stored_signature: str | None,
    current_signature: str,
    match: bool,
) -> None:
    """Print embedding signature status as a table."""
    table = Table(title="Embedding Signature Status", border_style="blue")
    table.add_column("Type", style="cyan")
    table.add_column("Signature", style="white")
    table.add_column("Status", justify="center")

    status_icon = "[green]✓[/green]" if match else "[red]⚠ MISMATCH[/red]"

    table.add_row("Stored", stored_signature or "(not set)", "")
    table.add_row("Current", current_signature, status_icon)

    console.print(table)

    if not match:
        print_warning("Embedding config changed - reindex will trigger on next run")


def print_reindex_result(result: dict[str, object], dry_run: bool) -> None:
    """Print reindex operation results."""
    table = Table(
        title="[bold]Reindex Results[/bold]" + (" (DRY RUN)" if dry_run else ""),
        border_style="green" if not dry_run else "yellow",
    )
    table.add_column("Property", style="cyan")
    table.add_column("Value", style="white")

    table.add_row("Previous Signature", str(result.get("previous_signature") or "(not set)"))
    table.add_row("Current Signature", str(result.get("current_signature")))

    if dry_run:
        table.add_row("Posts to Reembed", f"[yellow]{result.get('posts_cleared', 0):,}[/yellow]")
    else:
        table.add_row("Posts Reembedded", f"[green]{result.get('posts_reembedded', 0):,}[/green]")

    console.print(table)

    if not dry_run:
        print_success("Reindexing complete. Relationships and clusters preserved.")


def print_train_result(success: bool, mode: str) -> None:
    """Print training result."""
    if success:
        print_success(f"{mode} training completed successfully")
    else:
        print_error(f"{mode} training failed")


def print_user_id_result(user_id: int) -> None:
    """Print user ID lookup result."""
    console.print(f"[bold green]{user_id}[/bold green]")


def print_pipeline_start() -> None:
    """Print pipeline start header."""
    console.print()
    console.rule("[bold blue]🚀 Starting HintGrid Pipeline[/bold blue]")
    console.print()


def print_pipeline_complete() -> None:
    """Print pipeline completion message."""
    console.print()
    console.rule("[bold green]✨ Pipeline Completed Successfully[/bold green]")
    console.print()


def print_pipeline_summary(metrics: PipelineMetrics) -> None:
    """Print pipeline summary panel with timing, counts, and health."""
    lines: list[str] = []

    # Duration
    total_dur = metrics.get("total_duration_s", 0.0)
    mins, secs = divmod(total_dur, 60)
    lines.append(f"[bold cyan]Duration:[/bold cyan]         {int(mins)}m {secs:.0f}s")

    load_dur = metrics.get("load_duration_s")
    if load_dur is not None:
        lines.append(f"  ├── Load:            {load_dur:.1f}s")
    analytics_dur = metrics.get("analytics_duration_s")
    if analytics_dur is not None:
        lines.append(f"  ├── Analytics:       {analytics_dur:.1f}s")
    feeds_dur = metrics.get("feeds_duration_s")
    if feeds_dur is not None:
        lines.append(f"  └── Feeds:           {feeds_dur:.1f}s")
    lines.append("")

    # Data loaded
    lines.append("[bold cyan]Data loaded[/bold cyan]")
    user_count = metrics.get("user_count")
    if user_count is not None:
        lines.append(f"  ├── Users:           {user_count:,}")
    post_count = metrics.get("post_count")
    if post_count is not None:
        lines.append(f"  ├── Posts:           {post_count:,}")
    interaction_count = metrics.get("interaction_count")
    if interaction_count is not None:
        lines.append(f"  └── Interactions:    {interaction_count:,}")
    lines.append("")

    # Clustering
    lines.append("[bold cyan]Clustering[/bold cyan]")
    uc = metrics.get("user_communities")
    if uc is not None:
        mod_str = ""
        user_mod = metrics.get("user_modularity")
        if user_mod is not None:
            mod_str = f" (modularity: {user_mod:.3f})"
        lines.append(f"  ├── User clusters:   {uc:,}{mod_str}")
    pc = metrics.get("post_communities")
    if pc is not None:
        mod_str = ""
        post_mod = metrics.get("post_modularity")
        if post_mod is not None:
            mod_str = f" (modularity: {post_mod:.3f})"
        lines.append(f"  └── Post clusters:   {pc:,}{mod_str}")
    lines.append("")

    # Feeds
    feeds_generated = metrics.get("feeds_generated")
    dry_run = metrics.get("dry_run", False)
    if dry_run:
        lines.append("[bold cyan]Feeds:[/bold cyan]           [yellow]dry-run (skipped)[/yellow]")
    elif feeds_generated is not None:
        lines.append(f"[bold cyan]Feeds:[/bold cyan]           {feeds_generated:,} generated")
    lines.append("")

    # Warnings
    warnings = metrics.get("warnings", [])
    if warnings:
        lines.append(f"[bold cyan]Warnings:[/bold cyan]        {len(warnings)}")
        for w in warnings:
            lines.append(f"  [yellow]- {w}[/yellow]")
        lines.append("")

    # Health status
    status = _assess_health(metrics)
    status_styles: dict[str, str] = {
        "HEALTHY": "[bold green]HEALTHY[/bold green]",
        "DEGRADED": "[bold yellow]DEGRADED[/bold yellow]",
        "FAILED": "[bold red]FAILED[/bold red]",
    }
    lines.append(f"[bold cyan]Status:[/bold cyan]          {status_styles.get(status, status)}")

    panel = Panel(
        "\n".join(lines),
        title="[bold]📊 Pipeline Summary[/bold]",
        border_style="blue",
        expand=False,
    )
    console.print()
    console.print(panel)
    console.print()


def _assess_health(metrics: PipelineMetrics) -> str:
    """Determine pipeline health: HEALTHY, DEGRADED, or FAILED."""
    warnings = metrics.get("warnings", [])
    uc = metrics.get("user_communities", 0)
    post_count = metrics.get("post_count", 0)

    if uc is None or uc == 0:
        return "FAILED"
    if len(warnings) > 3:
        return "DEGRADED"
    if post_count is not None and post_count == 0:
        return "DEGRADED"
    if warnings:
        return "DEGRADED"
    return "HEALTHY"


def print_user_info_table(user_info: UserInfo) -> None:
    """Print user information table using Rich.

    Args:
        user_info: User information dictionary
    """
    table = Table(
        title="[bold]User Information[/bold]",
        border_style="cyan",
        show_header=True,
        header_style="bold",
        show_edge=True,
    )
    table.add_column("Property", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")

    # User ID
    user_id = user_info.get("user_id")
    if user_id is not None:
        table.add_row("User ID", f"{user_id:,}")
    else:
        table.add_row("User ID", "[dim]—[/dim]")

    # Username
    username = user_info.get("username") or "[dim]—[/dim]"
    table.add_row("Username", str(username))

    # Domain
    domain = user_info.get("domain")
    if domain:
        table.add_row("Domain", str(domain))
    else:
        table.add_row("Domain", "[dim]—[/dim]")

    # Handle
    username_val = user_info.get("username")
    if username_val:
        handle = f"@{username_val}"
        if domain:
            handle = f"@{username_val}@{domain}"
    else:
        handle = "[dim]—[/dim]"
    table.add_row("Handle", handle)

    # Languages
    languages = user_info.get("languages")
    if languages:
        languages_str = ", ".join(languages)
        table.add_row("Languages", languages_str)
    else:
        table.add_row("Languages", "[dim]—[/dim]")

    # Is Local
    is_local = user_info.get("is_local", False)
    is_local_str = "[green]Yes[/green]" if is_local else "[yellow]No[/yellow]"
    table.add_row("Is Local", is_local_str)

    console.print()
    console.print(table)
    console.print()

    # Extended information (if available)
    has_extended = any(
        key in user_info
        for key in [
            "user_community_id",
            "top_interests",
            "interactions",
            "follows_count",
            "followers_count",
            "posts_count",
        ]
    )

    if not has_extended:
        return

    # Community section
    if user_info.get("user_community_id") is not None:
        community_table = Table(
            title="[bold]Community[/bold]",
            border_style="blue",
            show_header=True,
            header_style="bold",
            show_edge=True,
        )
        community_table.add_column("Property", style="cyan", no_wrap=True)
        community_table.add_column("Value", style="white")

        uc_id = user_info.get("user_community_id")
        if uc_id is not None:
            community_table.add_row("Community ID", str(uc_id))

        uc_size = user_info.get("user_community_size")
        if uc_size is not None:
            community_table.add_row("Community Size", f"{uc_size:,}")
        else:
            community_table.add_row("Community Size", "[dim]—[/dim]")

        console.print(community_table)
        console.print()

    # Top Interests section
    top_interests = user_info.get("top_interests")
    if top_interests:
        interests_table = Table(
            title="[bold]Top Interests[/bold]",
            border_style="green",
            show_header=True,
            header_style="bold",
            show_edge=True,
        )
        interests_table.add_column("Rank", style="cyan", justify="right", no_wrap=True)
        interests_table.add_column(
            "Post Community ID", style="white", justify="right", no_wrap=True
        )
        interests_table.add_column("Score", style="yellow", justify="right", no_wrap=True)

        for rank, interest in enumerate(top_interests, start=1):
            pc_id = interest.get("pc_id")
            score = interest.get("score")
            if pc_id is not None and score is not None:
                score_float = coerce_float(score, 0.0)
                interests_table.add_row(
                    str(rank),
                    str(pc_id),
                    f"{score_float:.4f}",
                )

        console.print(interests_table)
        console.print()

    # Interactions section
    interactions = user_info.get("interactions")
    if interactions:
        interactions_table = Table(
            title="[bold]Interactions[/bold]",
            border_style="magenta",
            show_header=True,
            header_style="bold",
            show_edge=True,
        )
        interactions_table.add_column("Type", style="cyan", no_wrap=True)
        interactions_table.add_column("Count", style="white", justify="right", no_wrap=True)

        interactions_table.add_row(
            "Favorited",
            f"{interactions.get('favorited', 0):,}",
        )
        interactions_table.add_row(
            "Reblogged",
            f"{interactions.get('reblogged', 0):,}",
        )
        interactions_table.add_row(
            "Replied",
            f"{interactions.get('replied', 0):,}",
        )
        interactions_table.add_row(
            "Bookmarked",
            f"{interactions.get('bookmarked', 0):,}",
        )

        console.print(interactions_table)
        console.print()

    # Network section
    follows_count = user_info.get("follows_count")
    followers_count = user_info.get("followers_count")
    if follows_count is not None or followers_count is not None:
        network_table = Table(
            title="[bold]Network[/bold]",
            border_style="yellow",
            show_header=True,
            header_style="bold",
            show_edge=True,
        )
        network_table.add_column("Property", style="cyan", no_wrap=True)
        network_table.add_column("Count", style="white", justify="right", no_wrap=True)

        if follows_count is not None:
            network_table.add_row("Following", f"{follows_count:,}")
        else:
            network_table.add_row("Following", "[dim]—[/dim]")

        if followers_count is not None:
            network_table.add_row("Followers", f"{followers_count:,}")
        else:
            network_table.add_row("Followers", "[dim]—[/dim]")

        console.print(network_table)
        console.print()

    # Activity section
    posts_count = user_info.get("posts_count")
    if posts_count is not None:
        activity_table = Table(
            title="[bold]Activity[/bold]",
            border_style="red",
            show_header=True,
            header_style="bold",
            show_edge=True,
        )
        activity_table.add_column("Property", style="cyan", no_wrap=True)
        activity_table.add_column("Count", style="white", justify="right", no_wrap=True)

        activity_table.add_row("Posts", f"{posts_count:,}")

        console.print(activity_table)
        console.print()


def print_recommendations_table(
    recommendations: list[RecommendationDetail],
    user_info: UserInfo,
    author_info: dict[int, dict[str, str | None]],
    max_items: int = 10,
    text_preview_limit: int = 50,
) -> None:
    """Print recommendations table with score components using Rich.

    Args:
        recommendations: List of detailed recommendations
        user_info: User information dictionary
        author_info: Dictionary mapping author_id to account info (username, domain)
        max_items: Maximum number of recommendations to display
        text_preview_limit: Maximum length of text preview
    """
    if not recommendations:
        console.print("[yellow]⚠ No recommendations found[/yellow]")
        return

    # Format user handle for title
    username = user_info.get("username") or "unknown"
    domain = user_info.get("domain")
    handle = f"@{username}" if username else "@unknown"
    if domain:
        handle = f"@{username}@{domain}"

    table = Table(
        title=f"[bold]Recommendations for {handle}[/bold]",
        border_style="cyan",
        show_header=True,
        header_style="bold",
        show_edge=True,
    )

    table.add_column("Rank", style="cyan", justify="right", no_wrap=True)
    table.add_column("Post ID", style="white", justify="right", no_wrap=True)
    table.add_column("Text Preview", style="white", max_width=text_preview_limit)
    table.add_column("Author", style="green", no_wrap=True)
    table.add_column("Language", style="yellow", no_wrap=True)
    table.add_column("Created", style="dim", no_wrap=True)
    table.add_column("Score", style="bold green", justify="right", no_wrap=True)
    table.add_column("Components", style="dim")

    # Limit number of displayed items
    display_items = recommendations[:max_items]

    for rank, rec in enumerate(display_items, start=1):
        # Post ID
        post_id = str(rec["post_id"])

        # Text preview
        text = rec.get("post_text", "")
        if len(text) > text_preview_limit:
            text_preview = text[:text_preview_limit] + "..."
        else:
            text_preview = text or "[dim]—[/dim]"

        # Author
        author_id = rec.get("author_id", 0)
        author_data = author_info.get(author_id, {})
        author_username = author_data.get("username")
        author_domain = author_data.get("domain")
        if author_username:
            author_str = f"@{author_username}"
            if author_domain:
                author_str = f"@{author_username}@{author_domain}"
        else:
            author_str = f"[dim]ID:{author_id}[/dim]"

        # Language
        language = rec.get("post_language") or "[dim]—[/dim]"

        # Created date
        created_at = rec["post_created_at"]
        created_str = created_at.strftime("%Y-%m-%d %H:%M")

        # Final score
        final_score = rec.get("final_score", 0.0)
        score_str = f"{final_score:.3f}"

        # Score components
        interest_score = rec.get("interest_score", 0.0)
        popularity = rec.get("popularity", 0)
        age_hours = rec.get("age_hours", 0.0)
        pagerank = rec.get("pagerank", 0.0)
        language_match = rec.get("language_match", 0.0)

        components_str = (
            f"interest:{interest_score:.2f} "
            f"pop:{popularity} "
            f"age:{age_hours:.1f}h "
            f"pr:{pagerank:.3f} "
            f"lang:{language_match:.2f}"
        )

        table.add_row(
            str(rank),
            post_id,
            text_preview,
            author_str,
            str(language),
            created_str,
            score_str,
            components_str,
        )

    console.print()
    console.print(table)
    if len(recommendations) > max_items:
        console.print(
            f"[dim]Showing top {max_items} of {len(recommendations)} recommendations[/dim]"
        )
    console.print()


def print_shutdown_summary(
    steps: Sequence[PipelineStep],
    state: PipelineState | None = None,
) -> None:
    """Print shutdown summary panel with step statuses and cursor info.

    Args:
        steps: Snapshot of pipeline steps with their current status
        state: Optional pipeline state with saved cursor values
    """
    from hintgrid.cli.shutdown import ResumeStrategy, StepStatus
    from hintgrid.state import INITIAL_CURSOR

    console.print()
    console.rule("[bold yellow]⚠ Pipeline Interrupted (Ctrl+C)[/bold yellow]")
    console.print()

    if steps:
        table = Table(
            title="[bold]Pipeline Steps[/bold]",
            border_style="yellow",
            show_header=True,
            header_style="bold",
        )
        table.add_column("Step", style="white", no_wrap=True)
        table.add_column("Status", justify="center")
        table.add_column("Items", justify="right", style="cyan")
        table.add_column("On Resume", justify="center")

        status_display: dict[StepStatus, str] = {
            StepStatus.COMPLETED: "[green]✓ Completed[/green]",
            StepStatus.INTERRUPTED: "[yellow]⚠ Interrupted[/yellow]",
            StepStatus.IN_PROGRESS: "[yellow]⚠ Interrupted[/yellow]",
            StepStatus.PENDING: "[dim]○ Pending[/dim]",
        }
        resume_display: dict[ResumeStrategy, str] = {
            ResumeStrategy.RESUMES: "[green]Resumes[/green]",
            ResumeStrategy.RESTARTS: "[dim]Restarts[/dim]",
        }

        for step in steps:
            status_text = status_display.get(step.status, "[dim]?[/dim]")
            resume_text = resume_display.get(step.resume_strategy, "[dim]?[/dim]")
            items = f"{step.items_processed:,}" if step.items_processed > 0 else "[dim]—[/dim]"
            table.add_row(step.display_name, status_text, items, resume_text)

        console.print(table)

    if state is not None:
        cursors = [
            ("last_status_id", state.last_status_id),
            ("last_favourite_id", state.last_favourite_id),
            ("last_block_id", state.last_block_id),
            ("last_mute_id", state.last_mute_id),
            ("last_reblog_id", state.last_reblog_id),
            ("last_reply_id", state.last_reply_id),
            ("last_activity_account_id", state.last_activity_account_id),
        ]
        active_cursors = [(n, v) for n, v in cursors if v > INITIAL_CURSOR]

        if active_cursors:
            console.print()
            cursor_table = Table(
                title="[bold]Saved Cursors[/bold]",
                border_style="blue",
                show_header=True,
                header_style="bold",
            )
            cursor_table.add_column("Cursor", style="cyan")
            cursor_table.add_column("Value", justify="right", style="white")

            for name, value in active_cursors:
                cursor_table.add_row(name, f"{value:,}")

            console.print(cursor_table)

    console.print()
    console.print(
        "[dim]Pipeline will resume from saved cursors on next run.[/dim]",
    )
    console.print()


def print_similarity_diagnostics(
    diagnostics: dict[str, Neo4jValue],
    settings: HintGridSettings,
) -> None:
    """Print similarity graph building diagnostics using Rich tables.

    Shows:
    - Vector index status
    - Post eligibility statistics
    - Sample query results
    - Configuration parameters
    """
    from rich.panel import Panel

    lines: list[str] = []

    # Vector Index Status
    lines.append("[bold cyan]Vector Index[/bold cyan]")
    index_name = str(diagnostics.get("vector_index_name", "unknown"))
    index_exists = bool(diagnostics.get("vector_index_exists", False))
    index_state = diagnostics.get("vector_index_state")
    index_dimensions = diagnostics.get("vector_index_dimensions")

    if index_exists:
        status_icon = "[green]✓[/green]"
        if index_state == "ONLINE":
            status_text = f"{status_icon} ONLINE"
        elif index_state == "POPULATING":
            status_text = "[yellow]⚠ POPULATING[/yellow]"
        elif index_state == "FAILED":
            status_text = "[red]✗ FAILED[/red]"
        else:
            status_text = f"[yellow]⚠ {index_state or 'UNKNOWN'}[/yellow]"
    else:
        status_text = "[red]✗ NOT FOUND[/red]"

    lines.append(f"├── Name:        {index_name}")
    lines.append(f"├── Status:      {status_text}")
    if index_dimensions is not None:
        lines.append(f"└── Dimensions:  {index_dimensions}")
    else:
        lines.append("└── Dimensions:  [dim]—[/dim]")
    lines.append("")

    # Post Eligibility
    lines.append("[bold cyan]Post Eligibility[/bold cyan]")
    posts_with_embeddings = coerce_int(diagnostics.get("posts_with_embeddings", 0))
    posts_within_recency = coerce_int(diagnostics.get("posts_within_recency", 0))
    posts_eligible = coerce_int(diagnostics.get("posts_eligible", 0))

    # Get total posts count (we'll need to query it or estimate)
    total_posts = posts_with_embeddings  # Approximation
    if posts_with_embeddings > 0:
        pct_embeddings = (posts_with_embeddings / total_posts * 100.0) if total_posts > 0 else 0.0
        pct_recency = (
            (posts_within_recency / posts_with_embeddings * 100.0)
            if posts_with_embeddings > 0
            else 0.0
        )
        lines.append(
            f"├── With embeddings:       {posts_with_embeddings:,} ({pct_embeddings:.1f}%)"
        )
        lines.append(
            f"├── Within recency ({settings.similarity_recency_days}d): {posts_within_recency:,} ({pct_recency:.1f}%)"
        )
    else:
        lines.append(f"├── With embeddings:       {posts_with_embeddings:,}")
        lines.append(
            f"├── Within recency ({settings.similarity_recency_days}d): {posts_within_recency:,}"
        )
    lines.append(f"└── Eligible for processing: {posts_eligible:,}")
    lines.append("")

    # Sample Query Results
    sample_query_works = bool(diagnostics.get("sample_query_works", False))
    if sample_query_works:
        lines.append("[bold cyan]Sample Query Results[/bold cyan]")
        sample_post_id = diagnostics.get("sample_post_id")
        sample_neighbors = coerce_int(diagnostics.get("sample_neighbors_found", 0))
        sample_scores_raw = diagnostics.get("sample_scores", [])
        sample_above_threshold = coerce_int(diagnostics.get("sample_above_threshold", 0))

        if sample_post_id is not None:
            lines.append(f"├── Test post:             {sample_post_id}")
        lines.append(f"├── Neighbors found:       {sample_neighbors}")

        # Process sample_scores - use Sequence check instead of isinstance
        sample_scores: list[float] = []
        # Check if it's a Sequence-like object (has __iter__ and __len__)
        if (
            sample_scores_raw is not None
            and hasattr(sample_scores_raw, "__iter__")
            and hasattr(sample_scores_raw, "__len__")
        ):
            for s in sample_scores_raw:
                # Use coerce_float for all numeric types (works for int, float, str)
                sample_scores.append(coerce_float(s, 0.0))
            scores_str = ", ".join(f"{s:.2f}" for s in sample_scores[:10])
            if len(sample_scores) > 10:
                scores_str += f", ... ({len(sample_scores)} total)"
            lines.append(f"├── Scores:                {scores_str}")
        else:
            lines.append("├── Scores:                [dim]—[/dim]")

        lines.append(
            f"└── Above threshold ({settings.similarity_threshold:.2f}): {sample_above_threshold}"
        )
    else:
        lines.append("[bold cyan]Sample Query Results[/bold cyan]")
        lines.append("└── [yellow]⚠ Test query not executed or failed[/yellow]")
    lines.append("")

    # Configuration
    lines.append("[bold cyan]Configuration[/bold cyan]")
    top_k = max(1, settings.knn_neighbors + settings.knn_self_neighbor_offset)
    lines.append(f"├── Similarity threshold:  {settings.similarity_threshold:.2f}")
    lines.append(
        f"├── Top K neighbors:      {top_k} ({settings.knn_neighbors} + {settings.knn_self_neighbor_offset} offset)"
    )
    lines.append(f"└── Recency days:         {settings.similarity_recency_days}")

    panel = Panel(
        "\n".join(lines),
        title="[bold]Similarity Graph Diagnostics[/bold]",
        border_style="cyan",
    )
    console.print()
    console.print(panel)
    console.print()


def print_similarity_results(
    apoc_result: dict[str, Neo4jValue],
    similarity_stats: dict[str, Neo4jValue],
    settings: HintGridSettings,
) -> None:
    """Print detailed results of similarity graph building.

    Shows:
    - APOC execution statistics
    - Created relationships statistics
    - Score distribution
    - Posts without edges (if any)
    - Warnings if no relationships created
    """
    from rich.panel import Panel

    lines: list[str] = []

    # APOC Execution
    lines.append("[bold cyan]APOC Execution[/bold cyan]")
    batches = coerce_int(apoc_result.get("batches", 0))
    total = coerce_int(apoc_result.get("total", 0))
    committed = coerce_int(apoc_result.get("committedOperations", 0))
    failed = coerce_int(apoc_result.get("failedOperations", 0))

    lines.append(f"├── Batches:              {batches:,}")
    lines.append(f"├── Total processed:      {total:,}")
    lines.append(f"├── Committed:            {committed:,}")
    if failed > 0:
        lines.append(f"└── Failed:               [red]{failed:,}[/red]")
        error_messages_raw = apoc_result.get("errorMessages", [])
        # errorMessages from Neo4j is list[str], but apoc_result is dict[str, Neo4jValue]
        # Check type to ensure it's a list
        # Use Sequence check instead of isinstance
        error_messages: list[str] = []
        # Check if it's a Sequence-like object (has __iter__ and __len__)
        if (
            error_messages_raw is not None
            and hasattr(error_messages_raw, "__iter__")
            and hasattr(error_messages_raw, "__len__")
        ):
            error_messages = [coerce_str(msg, "") for msg in error_messages_raw]
            if error_messages:
                lines.append("")
                lines.append("[red]Error messages:[/red]")
                for msg in error_messages[:5]:  # Show first 5 errors
                    lines.append(f"  • {msg}")
                if len(error_messages) > 5:
                    lines.append(f"  ... and {len(error_messages) - 5} more")
    else:
        lines.append(f"└── Failed:               {failed}")
    lines.append("")

    # Created Relationships
    lines.append("[bold cyan]Created Relationships[/bold cyan]")
    total_rels = coerce_int(similarity_stats.get("total_relationships", 0))
    posts_with_edges = coerce_int(similarity_stats.get("posts_with_edges", 0))
    posts_without_edges = coerce_int(similarity_stats.get("posts_without_edges", 0))
    avg_edges_per_post = coerce_float(similarity_stats.get("avg_edges_per_post", 0.0))

    rel_icon = "[yellow]⚠[/yellow]" if total_rels == 0 else "[green]✓[/green]"

    lines.append(f"├── Total SIMILAR_TO:     {total_rels:,} {rel_icon}")
    lines.append(f"├── Posts with edges:     {posts_with_edges:,}")
    lines.append(f"└── Posts without edges:  {posts_without_edges:,}")

    if total_rels > 0:
        lines.append("")
        lines.append(f"[dim]Average edges per post: {avg_edges_per_post:.2f}[/dim]")
    lines.append("")

    # Score Distribution - use try/except instead of isinstance
    score_dist = similarity_stats.get("score_distribution", {})
    # Check for dict-like access (has get and items methods)
    if score_dist is not None and hasattr(score_dist, "get") and hasattr(score_dist, "items"):
        min_score = score_dist.get("min")
        max_score = score_dist.get("max")
        avg_score = score_dist.get("avg")
        median_score = score_dist.get("median")

        if any(s is not None for s in [min_score, max_score, avg_score, median_score]):
            lines.append("[bold cyan]Score Distribution[/bold cyan]")
            if min_score is not None:
                lines.append(f"├── Min:    {coerce_float(min_score, 0.0):.4f}")
            else:
                lines.append("├── Min:    [dim]—[/dim]")
            if max_score is not None:
                lines.append(f"├── Max:    {coerce_float(max_score, 0.0):.4f}")
            else:
                lines.append("├── Max:    [dim]—[/dim]")
            if avg_score is not None:
                lines.append(f"├── Avg:    {coerce_float(avg_score, 0.0):.4f}")
            else:
                lines.append("├── Avg:    [dim]—[/dim]")
            if median_score is not None:
                lines.append(f"└── Median: {coerce_float(median_score, 0.0):.4f}")
            else:
                lines.append("└── Median: [dim]—[/dim]")
            lines.append("")

    # Warnings
    if total_rels == 0 and total > 0:
        lines.append("[yellow]⚠ Warning: No SIMILAR_TO relationships created![/yellow]")
        lines.append("")
        lines.append("   Possible reasons:")
        lines.append(f"   - Similarity threshold ({settings.similarity_threshold:.2f}) too high")
        lines.append("   - Vector embeddings not similar enough")
        lines.append("   - Check sample query results in diagnostics above")

    panel = Panel(
        "\n".join(lines),
        title="[bold]Similarity Graph Results[/bold]",
        border_style="cyan" if total_rels > 0 else "yellow",
    )
    console.print()
    console.print(panel)
    console.print()
