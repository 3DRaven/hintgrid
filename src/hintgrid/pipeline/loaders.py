"""Incremental data loaders from PostgreSQL to Neo4j."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, UTC
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from hintgrid.cli.shutdown import ShutdownManager
    from hintgrid.clients.neo4j import Neo4jClient, Neo4jParameter
    from hintgrid.clients.postgres import PostgresClient

from hintgrid.cli.console import LoadingProgress, print_database_stats, print_info
from hintgrid.config import HintGridSettings
from hintgrid.embeddings.provider import EmbeddingProvider
from hintgrid.pipeline.graph import (
    merge_blocks,
    merge_bookmarks,
    merge_favourites,
    merge_interactions,
    merge_mutes,
    merge_posts,
    merge_reblogs,
    merge_replies,
    merge_status_stats,
    update_user_activity,
)
from hintgrid.state import INITIAL_CURSOR, PipelineState, StateStore
from hintgrid.utils.coercion import (
    coerce_int,
    coerce_optional_str,
    coerce_str,
    convert_batch_decimals,
    parse_load_since,
)
from hintgrid.utils.languages import user_activity_row_to_neo4j_fields

logger = logging.getLogger(__name__)


def _neo4j_user_activity_row(row: dict[str, object]) -> dict[str, object]:
    """Map PostgreSQL activity row to Neo4j ``update_user_activity`` batch shape."""
    raw_locale = row.get("locale")
    locale_str: str | None = (
        coerce_optional_str(raw_locale) if raw_locale is not None else None
    )
    chosen_raw = row.get("chosen_languages")
    chosen_list: list[str] | None
    if chosen_raw is None:
        chosen_list = None
    elif isinstance(chosen_raw, list):
        chosen_list = [str(x) for x in cast("list[object]", chosen_raw)]
    else:
        chosen_list = None
    ui, langs = user_activity_row_to_neo4j_fields(
        locale=locale_str,
        chosen_languages=chosen_list,
    )
    return {
        "account_id": row["account_id"],
        "last_active": row["last_active"],
        "is_local": row["is_local"],
        "ui_language": ui,
        "languages": langs,
    }


class _ThreadSafeStateStore(StateStore):
    """Thread-safe wrapper that serializes save() calls.

    Used when multiple entity loaders run concurrently in threads.
    Each loader mutates a different field of the shared PipelineState,
    but save() writes ALL fields to Neo4j and must be serialized.

    Re-uses the underlying StateStore's Neo4j client and state_id.
    The __init__ call to _ensure_initialized() is idempotent (MERGE).
    """

    _lock: threading.Lock

    def __init__(self, base: StateStore) -> None:
        super().__init__(base._neo4j, base._state_id)
        self._lock = threading.Lock()

    def save(self, state: PipelineState) -> None:
        with self._lock:
            super().save(state)


# ---------------------------------------------------------------------------
# Batch processors for different status types
# ---------------------------------------------------------------------------


def _process_status_batch(
    neo4j: Neo4jClient,
    embedding_client: EmbeddingProvider,
    batch: Sequence[Mapping[str, Neo4jParameter]],
    settings: HintGridSettings,
    state_id: str | None = None,
) -> int:
    """Process a batch of statuses: Post nodes + embeddings + type-specific relationships.

    Only rows that receive an embedding are loaded into Neo4j as Post nodes.
    Reblogs/replies create relationships only to existing Post nodes.

    Args:
        neo4j: Neo4j client
        embedding_client: Embedding provider
        batch: List of status dictionaries from PostgreSQL
        settings: Application settings
        state_id: Optional state ID for atomic state update

    Returns:
        Maximum ID from the entire batch (all statuses, not just those with embeddings).
    """
    from hintgrid.state import INITIAL_CURSOR

    if not batch:
        return INITIAL_CURSOR

    # Calculate max ID from entire batch first (needed for state update)
    # Use last element (O(1)) since data is guaranteed sorted by ORDER BY id ASC
    last_row = batch[-1]
    batch_max_id = coerce_int(last_row.get("id"), field="status.id", strict=True)

    # Check sorting for safety (only if batch > 1 element)
    if len(batch) > 1:
        first_id = coerce_int(batch[0].get("id"), field="status.id", strict=True)
        if first_id > batch_max_id:
            logger.warning(
                "Data not sorted! First ID (%d) > Last ID (%d). "
                "This should never happen with ORDER BY id ASC.",
                first_id,
                batch_max_id,
            )
            # Fallback: compute max() if data is not sorted
            batch_max_id = max(
                coerce_int(r.get("id"), field="status.id", strict=True) for r in batch
            )

    min_tokens = settings.min_embedding_tokens
    skip_pct = settings.embedding_skip_percentile

    # Step 1: Collect embeddable texts (non-empty, meeting min_tokens length)
    candidates: list[tuple[int, str]] = []
    for row in batch:
        row_id = coerce_int(row.get("id"), field="status.id", strict=True)
        text = coerce_str(row.get("text"))
        if text and len(text) >= min_tokens:
            candidates.append((row_id, text))

    # Step 2: Apply percentage-based filter (skip shortest texts)
    if skip_pct > 0 and candidates:
        candidates.sort(key=lambda x: len(x[1]))
        skip_count = int(len(candidates) * skip_pct)
        candidates = candidates[skip_count:]

    # Step 3: Compute embeddings
    embeddings_map: dict[int, list[float]] = {}
    if candidates:
        embeddings = embedding_client.embed_texts(candidates)
        for (row_id, _), emb in zip(candidates, embeddings):
            embeddings_map[row_id] = emb

    # Step 4: Create Post nodes ONLY for rows that got embeddings
    row_lookup = {coerce_int(r.get("id"), field="status.id", strict=True): r for r in batch}
    neo4j_batch: list[dict[str, Neo4jParameter]] = []
    for row_id, emb in embeddings_map.items():
        row = row_lookup[row_id]
        neo4j_batch.append(
            {
                "id": row_id,
                "authorId": coerce_int(
                    row.get("account_id"), field="status.account_id", strict=True
                ),
                "text": coerce_str(row.get("text")),
                "language": coerce_optional_str(row.get("language")),
                "embedding": emb,
                "createdAt": row.get("created_at"),
            }
        )

    # Atomic update: merge posts + update state in one transaction
    if neo4j_batch and state_id:
        merge_posts(neo4j, neo4j_batch, state_id=state_id, batch_max_id=batch_max_id)
    elif neo4j_batch:
        merge_posts(neo4j, neo4j_batch)

    # If no posts were created but we still need to update state
    # (e.g., all statuses were filtered out by embedding criteria)
    if not neo4j_batch and state_id:
        # Update state separately for empty batches
        neo4j.execute_labeled(
            "MATCH (s:__label__ {id: $state_id}) "
            "SET s.last_processed_status_id = CASE "
            "  WHEN $batch_max_id > s.last_processed_status_id "
            "  THEN $batch_max_id "
            "  ELSE s.last_processed_status_id "
            "END",
            {"label": "AppState"},
            {"state_id": state_id, "batch_max_id": batch_max_id},
        )

    # Dispatch to type-specific relationship processors
    # These now use MATCH for Post, so they silently skip non-existent posts
    reblog_rows = [r for r in batch if r.get("reblog_of_id") is not None]
    reply_rows = [r for r in batch if r.get("in_reply_to_id") is not None]
    _process_reblogs(neo4j, reblog_rows)
    _process_replies(neo4j, reply_rows)

    return batch_max_id


def _process_reblogs(neo4j: Neo4jClient, batch: Sequence[Mapping[str, Neo4jParameter]]) -> None:
    """Create REBLOGGED relationships for reblog rows."""
    merge_reblogs(neo4j, batch)


def _process_replies(neo4j: Neo4jClient, batch: Sequence[Mapping[str, Neo4jParameter]]) -> None:
    """Create REPLIED relationships for reply rows."""
    merge_replies(neo4j, batch)


def _compute_since_date(settings: HintGridSettings) -> datetime | None:
    """Compute since_date from load_since setting.

    If load_since is set (e.g., "30d"), returns UTC datetime of now - 30 days.
    Otherwise returns None for standard incremental behavior.

    Note: Uses UTC timezone for consistent Snowflake ID conversion.
    """
    days = parse_load_since(settings.load_since)
    if days is None:
        return None
    return datetime.now(UTC) - timedelta(days=days)


def load_incremental_data(
    postgres: PostgresClient,
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    state_store: StateStore,
    shutdown: ShutdownManager | None = None,
) -> None:
    """Load incremental data from Postgres to Neo4j.

    If load_since is set, the since_date is computed from current time.
    When load_since is set, incremental state (last_*_id) is ignored for
    time-based entities (statuses, favourites, reblogs, replies), but
    blocks and mutes are always loaded incrementally.

    When loader_workers > 1, independent entity loaders (favourites,
    blocks, mutes) run concurrently in threads after statuses
    are fully loaded. Each loader updates its own field of the shared
    PipelineState; concurrent save() calls are serialized via Lock.

    Args:
        postgres: PostgreSQL client
        neo4j: Neo4j client
        settings: Application settings
        state_store: State store for checkpointing
        shutdown: Optional shutdown manager for graceful Ctrl+C handling
    """
    state = state_store.load()

    # Display database statistics before loading
    try:
        db_stats = postgres.get_database_stats()
        if db_stats:
            print_database_stats(db_stats)
    except Exception as e:
        logger.warning("Failed to get database statistics: %s", e)

    # Reset activity cursor for full rescan on each pipeline run.
    # User activity is mutable data; cursor only helps with resume-on-interrupt
    # within a single run, not across runs.
    state.last_activity_account_id = INITIAL_CURSOR
    state_store.save(state)

    # Compute since_date for time-window loading (before creating EmbeddingProvider)
    since_date = _compute_since_date(settings)
    if since_date is not None:
        print_info(f"Using load_since window: loading data since {since_date}")

    # Pass postgres and since_date for FastText auto-training capability
    embedding_client = EmbeddingProvider(settings, neo4j, postgres, since_date=since_date)

    loader_workers = settings.loader_workers

    with LoadingProgress(settings) as progress:
        # Phase 1: Statuses always run first (needs embedding, creates Post nodes)
        state = _load_statuses(
            postgres,
            neo4j,
            settings,
            state,
            state_store,
            embedding_client,
            since_date,
            progress,
            shutdown,
        )

        if shutdown and shutdown.shutdown_requested:
            return

        if loader_workers <= 1:
            # Sequential mode (backward compatible)
            state = _load_favourites(
                postgres,
                neo4j,
                settings,
                state,
                state_store,
                since_date,
                progress,
                shutdown,
            )
            if shutdown and shutdown.shutdown_requested:
                return
            state = _load_blocks(
                postgres,
                neo4j,
                settings,
                state,
                state_store,
                progress,
                shutdown,
            )
            if shutdown and shutdown.shutdown_requested:
                return
            state = _load_mutes(
                postgres,
                neo4j,
                settings,
                state,
                state_store,
                progress,
                shutdown,
            )
            if shutdown and shutdown.shutdown_requested:
                return
            state = _load_bookmarks(
                postgres,
                neo4j,
                settings,
                state,
                state_store,
                since_date,
                progress,
                shutdown,
            )
        else:
            # Phase 2: Independent entity loaders run in parallel
            # Thread-safe store serializes concurrent save() calls
            safe_store = _ThreadSafeStateStore(state_store)
            logger.info(
                "Running entity loaders in parallel (%d workers)",
                loader_workers,
            )
            with ThreadPoolExecutor(max_workers=loader_workers) as pool:
                futures = [
                    pool.submit(
                        _load_favourites,
                        postgres,
                        neo4j,
                        settings,
                        state,
                        safe_store,
                        since_date,
                        progress,
                        shutdown,
                    ),
                    pool.submit(
                        _load_blocks,
                        postgres,
                        neo4j,
                        settings,
                        state,
                        safe_store,
                        progress,
                        shutdown,
                    ),
                    pool.submit(
                        _load_mutes,
                        postgres,
                        neo4j,
                        settings,
                        state,
                        safe_store,
                        progress,
                        shutdown,
                    ),
                    pool.submit(
                        _load_bookmarks,
                        postgres,
                        neo4j,
                        settings,
                        state,
                        safe_store,
                        since_date,
                        progress,
                        shutdown,
                    ),
                ]
                for future in as_completed(futures):
                    # Re-raise any exceptions from worker threads
                    future.result()

            # Final save to ensure all parallel results are persisted
            state_store.save(state)

        if shutdown and shutdown.shutdown_requested:
            return

        # Phase 3: Load interactions and status_stats
        state = _load_interactions(
            postgres,
            neo4j,
            settings,
            state,
            state_store,
            progress,
            shutdown,
        )
        if shutdown and shutdown.shutdown_requested:
            return

        state = _load_status_stats(
            postgres,
            neo4j,
            settings,
            state,
            state_store,
            progress,
            shutdown,
        )
        if shutdown and shutdown.shutdown_requested:
            return

        # Phase 4: Update user activity (lastActive) from PostgreSQL
        # Must run AFTER all loading phases so User nodes already exist in Neo4j
        _load_user_activity(
            postgres,
            neo4j,
            settings,
            state,
            state_store,
            progress,
            shutdown,
        )


def _load_statuses(
    postgres: PostgresClient,
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    state: PipelineState,
    state_store: StateStore,
    embedding_client: EmbeddingProvider,
    since_date: datetime | None = None,
    progress: LoadingProgress | None = None,
    shutdown: ShutdownManager | None = None,
) -> PipelineState:
    """Load all statuses (regular, reblogs, replies) from a single stream.

    Uses a unified stream from PostgreSQL and dispatches each row to
    type-specific processors based on reblog_of_id / in_reply_to_id.
    """
    # FIXED: When since_date is set, use max(saved_state, min_id_from_date)
    # This prevents skipping data when process is killed and restarted
    if since_date is not None:
        from hintgrid.utils.snowflake import snowflake_id_at

        min_id_from_date = snowflake_id_at(since_date)
        last_id = max(state.last_status_id, min_id_from_date)
        logger.info(
            "Using load_since window: min_id_from_date=%d, saved_last_id=%d, effective_last_id=%d",
            min_id_from_date,
            state.last_status_id,
            last_id,
        )
    else:
        last_id = state.last_status_id

    total_processed = 0
    batch_num = 0
    task_name = "statuses"
    state_id = state_store.state_id  # Get state_id for atomic updates

    if shutdown:
        shutdown.begin_step(task_name)
    if progress:
        progress.add_task(task_name, "[cyan]Loading statuses...[/cyan]")

    logger.info("  Loading statuses (unified stream: posts + reblogs + replies)...")
    batch: list[dict[str, object]] = []
    for row in postgres.stream_statuses(last_id, since_date):
        batch.append(row)
        if len(batch) >= settings.batch_size:
            batch_num += 1
            # Process batch and get max ID (atomic state update inside)
            from hintgrid.utils.coercion import convert_batch_decimals

            converted_batch = convert_batch_decimals(batch)
            batch_max_id = _process_status_batch(
                neo4j, embedding_client, converted_batch, settings, state_id=state_id
            )
            total_processed += len(batch)
            state.last_status_id = batch_max_id

            if progress:
                progress.update(task_name, len(batch))

            logger.debug(
                "  [statuses] Batch %d: %d items (total: %d, last_id=%d)",
                batch_num,
                len(batch),
                total_processed,
                batch_max_id,
            )
            # State is already updated atomically in merge_posts,
            # but we still save for other fields (total_processed tracking, etc.)
            if total_processed % settings.checkpoint_interval == 0:
                state_store.save(state)
            batch = []

            if shutdown and shutdown.shutdown_requested:
                state_store.save(state)
                break

    # Process remaining items in batch
    if batch:
        batch_num += 1
        from hintgrid.utils.coercion import convert_batch_decimals

        converted_batch = convert_batch_decimals(batch)
        batch_max_id = _process_status_batch(
            neo4j, embedding_client, converted_batch, settings, state_id=state_id
        )
        total_processed += len(batch)
        state.last_status_id = batch_max_id

        if progress:
            progress.update(task_name, len(batch))

        logger.debug(
            "  [statuses] Batch %d: %d items (total: %d, last_id=%d)",
            batch_num,
            len(batch),
            total_processed,
            batch_max_id,
        )
        if total_processed % settings.checkpoint_interval == 0:
            state_store.save(state)
    # Final save (state.last_status_id already updated atomically, but save other fields)
    state_store.save(state)

    if shutdown:
        if shutdown.shutdown_requested:
            shutdown.update_step_progress(task_name, total_processed)
        else:
            shutdown.complete_step(task_name, total_processed)

    if progress and not (shutdown and shutdown.shutdown_requested):
        progress.complete(task_name, f"Loaded {total_processed:,} statuses")

    logger.info("  Loaded %d statuses (last_id=%s)", total_processed, state.last_status_id)
    return state


def _run_entity_loader(
    name: str,
    stream: Iterator[dict[str, object]],
    merge_fn: Callable[
        [Neo4jClient, Sequence[Mapping[str, Neo4jParameter]], str | None, int | None], int | None
    ],
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    state_store: StateStore,
    id_field: str,
    state_field: str | None = None,
    progress: LoadingProgress | None = None,
    shutdown: ShutdownManager | None = None,
) -> tuple[int, int]:
    """Generic batch-streaming entity loader with optional atomic state update.

    Streams rows from PostgreSQL, batches them, and writes to Neo4j.
    Checks shutdown flag between batches for graceful Ctrl+C handling.

    Args:
        name: Entity name for logging and progress display
        stream: Iterator yielding rows from PostgreSQL
        merge_fn: Function to merge a batch into Neo4j. Signature:
                  (neo4j, batch, state_id, batch_max_id) -> int | None
        neo4j: Neo4j client
        settings: Application settings
        state_store: State store for checkpointing
        id_field: Field name for row ID extraction (e.g. "favourite.id")
        state_field: Optional field name in AppState for atomic update (e.g. "last_processed_favourite_id")
        progress: Optional progress tracker
        shutdown: Optional shutdown manager for graceful Ctrl+C handling

    Returns:
        Tuple of (last_id, total_processed)
    """
    from hintgrid.state import INITIAL_CURSOR

    total_processed = 0
    batch_num = 0
    last_id = INITIAL_CURSOR
    state_id = state_store.state_id if state_field else None

    if shutdown:
        shutdown.begin_step(name)
    if progress:
        progress.add_task(name, f"[cyan]Loading {name}...[/cyan]")

    logger.info("  Loading %s...", name)
    batch: list[dict[str, object]] = []
    for row in stream:
        batch.append(row)
        if len(batch) >= settings.batch_size:
            batch_num += 1
            # Calculate max ID from entire batch (needed for state update)
            # Use last element (O(1)) since data is guaranteed sorted by ORDER BY id ASC
            last_row = batch[-1]
            batch_max_id = coerce_int(last_row.get("id"), field=id_field, strict=True)

            # Check sorting for safety (only if batch > 1 element)
            if len(batch) > 1:
                first_id = coerce_int(batch[0].get("id"), field=id_field, strict=True)
                if first_id > batch_max_id:
                    logger.warning(
                        "Data not sorted! First ID (%d) > Last ID (%d). "
                        "This should never happen with ORDER BY id ASC.",
                        first_id,
                        batch_max_id,
                    )
                    # Fallback: compute max() if data is not sorted
                    batch_max_id = max(
                        coerce_int(r.get("id"), field=id_field, strict=True) for r in batch
                    )

            # Atomic update: merge entities + update state in one transaction
            from hintgrid.utils.coercion import convert_batch_decimals

            converted_batch = convert_batch_decimals(batch)
            if state_id:
                result = merge_fn(neo4j, converted_batch, state_id, batch_max_id)
                if result is not None:
                    last_id = result
            else:
                merge_fn(neo4j, converted_batch, None, None)
                last_id = batch_max_id

            total_processed += len(batch)

            if progress:
                progress.update(name, len(batch))

            logger.debug(
                "  [%s] Batch %d: %d items (total: %d, last_id=%d)",
                name,
                batch_num,
                len(batch),
                total_processed,
                last_id,
            )
            batch = []

            if shutdown and shutdown.shutdown_requested:
                break

    # Process remaining items in batch
    if batch:
        batch_num += 1
        # Calculate max ID from entire batch
        last_row = batch[-1]
        batch_max_id = coerce_int(last_row.get("id"), field=id_field, strict=True)

        # Check sorting for safety
        if len(batch) > 1:
            first_id = coerce_int(batch[0].get("id"), field=id_field, strict=True)
            if first_id > batch_max_id:
                logger.warning(
                    "Data not sorted! First ID (%d) > Last ID (%d). "
                    "This should never happen with ORDER BY id ASC.",
                    first_id,
                    batch_max_id,
                )
                batch_max_id = max(
                    coerce_int(r.get("id"), field=id_field, strict=True) for r in batch
                )

        # Atomic update: merge entities + update state in one transaction
        from hintgrid.utils.coercion import convert_batch_decimals

        converted_batch = convert_batch_decimals(batch)
        if state_id:
            result = merge_fn(neo4j, converted_batch, state_id, batch_max_id)
            if result is not None:
                last_id = result
        else:
            merge_fn(neo4j, converted_batch, None, None)
            last_id = batch_max_id

        total_processed += len(batch)

        if progress:
            progress.update(name, len(batch))

        logger.debug(
            "  [%s] Batch %d: %d items (total: %d, last_id=%d)",
            name,
            batch_num,
            len(batch),
            total_processed,
            last_id,
        )

    if shutdown:
        if shutdown.shutdown_requested:
            shutdown.update_step_progress(name, total_processed)
        else:
            shutdown.complete_step(name, total_processed)

    if progress and not (shutdown and shutdown.shutdown_requested):
        progress.complete(name, f"Loaded {total_processed:,} {name}")

    logger.info("  Loaded %d %s (last_id=%d)", total_processed, name, last_id)
    return last_id, total_processed


def _load_favourites(
    postgres: PostgresClient,
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    state: PipelineState,
    state_store: StateStore,
    since_date: datetime | None = None,
    progress: LoadingProgress | None = None,
    shutdown: ShutdownManager | None = None,
) -> PipelineState:
    """Load favourites from PostgreSQL to Neo4j."""
    from hintgrid.state import INITIAL_CURSOR

    initial_id = INITIAL_CURSOR if since_date is not None else state.last_favourite_id
    stream = postgres.stream_favourites(initial_id, since_date)
    last_id, _ = _run_entity_loader(
        "favourites",
        stream,
        merge_favourites,
        neo4j,
        settings,
        state_store,
        "favourite.id",
        state_field="last_processed_favourite_id",
        progress=progress,
        shutdown=shutdown,
    )
    # State is already updated atomically in merge_favourites
    if last_id > INITIAL_CURSOR:
        state.last_favourite_id = last_id
    # Still save for other fields (total_processed tracking, etc.)
    state_store.save(state)
    return state


def _load_blocks(
    postgres: PostgresClient,
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    state: PipelineState,
    state_store: StateStore,
    progress: LoadingProgress | None = None,
    shutdown: ShutdownManager | None = None,
) -> PipelineState:
    """Load blocks from PostgreSQL to Neo4j."""
    from hintgrid.state import INITIAL_CURSOR

    stream = postgres.stream_blocks(state.last_block_id)
    last_id, _ = _run_entity_loader(
        "blocks",
        stream,
        merge_blocks,
        neo4j,
        settings,
        state_store,
        "block.id",
        state_field="last_processed_block_id",
        progress=progress,
        shutdown=shutdown,
    )
    # State is already updated atomically in merge_blocks
    if last_id > INITIAL_CURSOR:
        state.last_block_id = last_id
    # Still save for other fields (total_processed tracking, etc.)
    state_store.save(state)
    return state


def _load_mutes(
    postgres: PostgresClient,
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    state: PipelineState,
    state_store: StateStore,
    progress: LoadingProgress | None = None,
    shutdown: ShutdownManager | None = None,
) -> PipelineState:
    """Load mutes from PostgreSQL to Neo4j."""
    from hintgrid.state import INITIAL_CURSOR

    stream = postgres.stream_mutes(state.last_mute_id)
    last_id, _ = _run_entity_loader(
        "mutes",
        stream,
        merge_mutes,
        neo4j,
        settings,
        state_store,
        "mute.id",
        state_field="last_processed_mute_id",
        progress=progress,
        shutdown=shutdown,
    )
    # State is already updated atomically in merge_mutes
    if last_id > INITIAL_CURSOR:
        state.last_mute_id = last_id
    # Still save for other fields (total_processed tracking, etc.)
    state_store.save(state)
    return state


def _load_bookmarks(
    postgres: PostgresClient,
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    state: PipelineState,
    state_store: StateStore,
    since_date: datetime | None = None,
    progress: LoadingProgress | None = None,
    shutdown: ShutdownManager | None = None,
) -> PipelineState:
    """Load bookmarks from PostgreSQL to Neo4j.

    Bookmarks are a strong implicit interest signal (stronger than favourites).
    Creates BOOKMARKED relationships between User and Post nodes.
    """
    from hintgrid.state import INITIAL_CURSOR

    initial_id = INITIAL_CURSOR if since_date is not None else state.last_bookmark_id
    stream = postgres.stream_bookmarks(initial_id, since_date)
    last_id, _ = _run_entity_loader(
        "bookmarks",
        stream,
        merge_bookmarks,
        neo4j,
        settings,
        state_store,
        "bookmark.id",
        state_field="last_processed_bookmark_id",
        progress=progress,
        shutdown=shutdown,
    )
    # State is already updated atomically in merge_bookmarks
    if last_id > INITIAL_CURSOR:
        state.last_bookmark_id = last_id
    # Still save for other fields (total_processed tracking, etc.)
    state_store.save(state)
    return state


def _load_user_activity(
    postgres: PostgresClient,
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    state: PipelineState,
    state_store: StateStore,
    progress: LoadingProgress | None = None,
    shutdown: ShutdownManager | None = None,
) -> None:
    """Stream user activity from PostgreSQL and update User.lastActive in Neo4j.

    Runs after entity loading to ensure User nodes already exist.
    Skips entirely when the graph has no User nodes (e.g. after clean).
    Only streams accounts active within active_user_days to avoid
    unnecessary network traffic for long-inactive accounts.

    Uses a cursor (last_activity_account_id) for resume-on-interrupt
    within a single pipeline run. The cursor is reset at the start
    of each full run in load_incremental_data().

    Args:
        postgres: PostgreSQL client
        neo4j: Neo4j client
        settings: Application settings (batch_size, active_user_days)
        state: Pipeline state with activity cursor
        state_store: State store for checkpointing
        progress: Optional progress tracker
        shutdown: Optional shutdown manager for graceful Ctrl+C handling
    """
    task_name = "user_activity"

    if shutdown:
        shutdown.begin_step(task_name)

    # Skip if no User nodes exist (e.g. after clean with no statuses loaded)
    user_count_rows = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (u:__user__) RETURN count(u) AS cnt",
            {"user": "User"},
        )
    )
    graph_user_count = (
        coerce_int(
            user_count_rows[0].get("cnt"),
        )
        if user_count_rows
        else 0
    )

    if graph_user_count == 0:
        logger.info("  No User nodes in graph, skipping activity update")
        if shutdown:
            shutdown.complete_step(task_name)
        if progress:
            progress.add_task(task_name, "[cyan]Updating user activity...[/cyan]")
            progress.complete(task_name, "Skipped (no users in graph)")
        return

    active_days = settings.active_user_days
    last_account_id = state.last_activity_account_id

    # Pre-count active accounts for progress bar (respects cursor)
    total_expected = postgres.count_active_users(active_days, last_account_id)
    logger.info(
        "  Updating user lastActive from PostgreSQL (%d active accounts within %d days)...",
        total_expected,
        active_days,
    )

    total_processed = 0
    batch_num = 0

    if progress:
        progress.add_task(
            task_name,
            "[cyan]Updating user activity...[/cyan]",
            total=total_expected if total_expected > 0 else None,
        )

    batch: list[dict[str, object]] = []
    state_id = state_store.state_id
    for row in postgres.stream_user_activity(active_days, last_account_id):
        batch.append(_neo4j_user_activity_row(row))
        if len(batch) >= settings.batch_size:
            batch_num += 1
            # Calculate max ID from entire batch (needed for state update)
            # Use last element (O(1)) since data is guaranteed sorted by ORDER BY id ASC
            last_row = batch[-1]
            batch_max_id = coerce_int(last_row.get("account_id"), field="account.id", strict=True)

            # Check sorting for safety (only if batch > 1 element)
            if len(batch) > 1:
                first_id = coerce_int(batch[0].get("account_id"), field="account.id", strict=True)
                if first_id > batch_max_id:
                    logger.warning(
                        "Data not sorted! First ID (%d) > Last ID (%d). "
                        "This should never happen with ORDER BY id ASC.",
                        first_id,
                        batch_max_id,
                    )
                    # Fallback: compute max() if data is not sorted
                    batch_max_id = max(
                        coerce_int(r.get("account_id"), field="account.id", strict=True)
                        for r in batch
                    )

            # Atomic update: update users + update state in one transaction
            from hintgrid.utils.coercion import convert_batch_decimals

            converted_batch = convert_batch_decimals(batch)
            result = update_user_activity(
                neo4j, converted_batch, state_id=state_id, batch_max_id=batch_max_id
            )
            if result is not None:
                state.last_activity_account_id = result
            total_processed += len(batch)

            if progress:
                progress.update(task_name, len(batch))

            logger.debug(
                "  [user_activity] Batch %d: %d items (total: %d, last_id=%d)",
                batch_num,
                len(batch),
                total_processed,
                state.last_activity_account_id,
            )

            if total_processed % settings.checkpoint_interval == 0:
                # State is already updated atomically, but save for other fields
                state_store.save(state)

            batch = []

            if shutdown and shutdown.shutdown_requested:
                state_store.save(state)
                break

    # Process remaining batch
    if batch:
        batch_num += 1
        # Calculate max ID from entire batch
        last_row = batch[-1]
        batch_max_id = coerce_int(last_row.get("account_id"), field="account.id", strict=True)

        # Check sorting for safety
        if len(batch) > 1:
            first_id = coerce_int(batch[0].get("account_id"), field="account.id", strict=True)
            if first_id > batch_max_id:
                logger.warning(
                    "Data not sorted! First ID (%d) > Last ID (%d). "
                    "This should never happen with ORDER BY id ASC.",
                    first_id,
                    batch_max_id,
                )
                batch_max_id = max(
                    coerce_int(r.get("account_id"), field="account.id", strict=True) for r in batch
                )

        # Atomic update: update users + update state in one transaction
        from hintgrid.utils.coercion import convert_batch_decimals

        converted_batch = convert_batch_decimals(batch)
        result = update_user_activity(
            neo4j, converted_batch, state_id=state_id, batch_max_id=batch_max_id
        )
        if result is not None:
            state.last_activity_account_id = result
        total_processed += len(batch)

        if progress:
            progress.update(task_name, len(batch))

        logger.debug(
            "  [user_activity] Batch %d: %d items (total: %d, last_id=%d)",
            batch_num,
            len(batch),
            total_processed,
            state.last_activity_account_id,
        )

    # Final save (state.last_activity_account_id already updated atomically, but save other fields)
    state_store.save(state)

    if shutdown:
        if shutdown.shutdown_requested:
            shutdown.update_step_progress(task_name, total_processed)
        else:
            shutdown.complete_step(task_name, total_processed)

    if progress and not (shutdown and shutdown.shutdown_requested):
        progress.complete(task_name, f"Updated {total_processed:,} user activities")

    logger.info("  Updated %d user activities", total_processed)


def _load_interactions(
    postgres: PostgresClient,
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    state: PipelineState,
    state_store: StateStore,
    progress: LoadingProgress | None = None,
    shutdown: ShutdownManager | None = None,
) -> PipelineState:
    """Load aggregated user-user INTERACTS_WITH edges incrementally.

    Streams new interactions from PostgreSQL using per-source cursors
    and merges them into Neo4j with incremental weight accumulation.
    Cursors are updated atomically in AppState within the same Neo4j
    transaction as the relationship merge.

    Args:
        postgres: PostgreSQL client
        neo4j: Neo4j client
        settings: Application settings (batch_size)
        state: Current pipeline state with interaction cursors
        state_store: State store for atomic cursor updates
        progress: Optional progress tracker
        shutdown: Optional shutdown manager for graceful Ctrl+C handling

    Returns:
        Updated PipelineState with new cursor positions
    """
    task_name = "interactions"

    if shutdown:
        shutdown.begin_step(task_name)
    if progress:
        progress.add_task(task_name, "[cyan]Loading user interactions...[/cyan]")

    total_processed = 0
    batch_num = 0
    batch: list[dict[str, object]] = []

    for row in postgres.stream_user_interactions(
        last_interaction_favourite_id=state.last_interaction_favourite_id,
        last_interaction_status_id=state.last_interaction_status_id,
        last_interaction_mention_id=state.last_interaction_mention_id,
        last_interaction_follow_id=state.last_interaction_follow_id,
        follows_weight=settings.follows_weight,
        likes_weight=settings.likes_weight,
        replies_weight=settings.replies_weight,
        reblogs_weight=settings.reblogs_weight,
        mentions_weight=settings.mentions_weight,
    ):
        batch.append(row)
        if len(batch) >= settings.batch_size:
            batch_num += 1
            converted_batch = convert_batch_decimals(batch)
            merge_interactions(neo4j, converted_batch, state_id=state_store.state_id)
            total_processed += len(batch)

            if progress:
                progress.update(task_name, len(batch))

            logger.debug(
                "  [interactions] Batch %d: %d items (total: %d)",
                batch_num,
                len(batch),
                total_processed,
            )
            batch = []

            if shutdown and shutdown.shutdown_requested:
                break

    if batch:
        batch_num += 1
        converted_batch = convert_batch_decimals(batch)
        merge_interactions(neo4j, converted_batch, state_id=state_store.state_id)
        total_processed += len(batch)

        if progress:
            progress.update(task_name, len(batch))

    if shutdown:
        if shutdown.shutdown_requested:
            shutdown.update_step_progress(task_name, total_processed)
        else:
            shutdown.complete_step(task_name, total_processed)

    if progress and not (shutdown and shutdown.shutdown_requested):
        progress.complete(task_name, f"Loaded {total_processed:,} interaction edges")

    logger.info("  Loaded %d interaction edges", total_processed)

    return state_store.load()


def _load_status_stats(
    postgres: PostgresClient,
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    state: PipelineState,
    state_store: StateStore,
    progress: LoadingProgress | None = None,
    shutdown: ShutdownManager | None = None,
) -> PipelineState:
    """Load status_stats from PostgreSQL and update Post.popularity in Neo4j.

    Incrementally streams rows after ``state.last_status_stats_id``.

    Args:
        postgres: PostgreSQL client
        neo4j: Neo4j client
        settings: Application settings (batch_size)
        state: Pipeline state with status_stats cursor
        state_store: State store for checkpointing
        progress: Optional progress tracker
        shutdown: Optional shutdown manager for graceful Ctrl+C handling

    Returns:
        Updated pipeline state
    """
    from hintgrid.state import INITIAL_CURSOR

    stream = postgres.stream_status_stats(state.last_status_stats_id)
    last_id, _ = _run_entity_loader(
        "status_stats",
        stream,
        merge_status_stats,
        neo4j,
        settings,
        state_store,
        "status_stats.id",
        state_field="last_processed_status_stats_id",
        progress=progress,
        shutdown=shutdown,
    )
    # State is already updated atomically in merge_status_stats
    if last_id > INITIAL_CURSOR:
        state.last_status_stats_id = last_id
    # Still save for other fields (total_processed tracking, etc.)
    state_store.save(state)
    return state
