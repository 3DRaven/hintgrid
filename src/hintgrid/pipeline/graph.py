"""Neo4j graph operations."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, LiteralString

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from hintgrid.clients.neo4j import Neo4jClient, Neo4jParameter, Neo4jValue
    from hintgrid.cli.progress_display import HintGridProgress
    from hintgrid.embeddings.provider import EmbeddingProvider
    from hintgrid.state import PipelineState, StateStore
    from rich.progress import TaskID

from hintgrid.cli.console import create_batch_progress
from hintgrid.config import HintGridSettings, build_embedding_signature
from hintgrid.utils.coercion import coerce_int, coerce_str, convert_batch_decimals

logger = logging.getLogger(__name__)

LOG_STATEMENT_PREVIEW = 50


def _make_params(**kwargs: Neo4jParameter) -> dict[str, Neo4jParameter]:
    """Create typed parameter dictionary for Neo4j queries."""
    return kwargs


# Removed _is_str_dict - replaced with direct isinstance checks


def _extract_vector_dimension(row: dict[str, Neo4jValue]) -> int | None:
    """Extract vector.dimensions from SHOW INDEXES row options.

    Neo4j SHOW INDEXES returns nested dicts
    (options → indexConfig → vector.dimensions).
    We drill down with hasattr-based checks at each level instead of isinstance.
    """
    raw_options: Neo4jValue = row.get("options")
    # Use hasattr instead of isinstance for dict-like check
    if raw_options is None or not (hasattr(raw_options, "get") and hasattr(raw_options, "items")):
        return None
    options: dict[str, Neo4jValue] = raw_options
    raw_config: Neo4jValue = options.get("indexConfig")
    if raw_config is None or not (hasattr(raw_config, "get") and hasattr(raw_config, "items")):
        return None
    config: dict[str, Neo4jValue] = raw_config
    dim: Neo4jValue = config.get("vector.dimensions")
    if dim is None:
        return None
    return int(str(dim))


def _get_worker_suffix(neo4j: Neo4jClient) -> str:
    """Get worker suffix for index/constraint names.

    Args:
        neo4j: Neo4j client with optional worker_label

    Returns:
        Worker suffix (e.g., "_worker_gw0" or "")
    """
    if neo4j.worker_label:
        return f"_{neo4j.worker_label}"
    return ""


def _get_embedding_index_name(neo4j: Neo4jClient) -> str:
    """Get embedding index name based on worker label.

    Args:
        neo4j: Neo4j client with optional worker_label

    Returns:
        Index name (e.g., "worker_gw0_posts" or "post_embedding_index")
    """
    if neo4j.worker_label:
        return f"{neo4j.worker_label}_posts"
    return "post_embedding_index"


def ensure_graph_indexes(neo4j: Neo4jClient, settings: HintGridSettings) -> None:
    """Create Neo4j constraints and indexes.

    When worker_label is set (parallel tests), uniqueness constraints are
    skipped entirely.  Neo4j Community Edition does not support multi-label
    constraints (``FOR (n:A:B) REQUIRE …``).  ``label()`` returns
    ``"BaseLabel:WorkerLabel"`` — if Neo4j applies the constraint to only
    one of the two labels, every node with that label shares the same
    unique index.  This causes ``IndexEntryConflictException`` when
    Leiden-assigned cluster IDs (small integers) collide with entity IDs
    (e.g.  Post id=4 blocks UserCommunity cluster_id=4).

    ``apoc.merge.node`` provides idempotent upserts without constraints,
    so data integrity is preserved.
    """
    # Generate constraint name suffix for worker isolation
    worker_suffix = _get_worker_suffix(neo4j)

    # Uniqueness constraints — only in production (no worker label).
    # UserCommunity / PostCommunity are excluded: their IDs come from
    # Leiden and are not globally meaningful.
    if not neo4j.worker_label:
        constraint_defs = [
            ("user_id_unique", "User"),
            ("post_id_unique", "Post"),
            ("app_state_id_unique", "AppState"),
        ]
        for name_base, base_label in constraint_defs:
            try:
                neo4j.execute_labeled(
                    "CREATE CONSTRAINT __name__ IF NOT EXISTS "
                    "FOR (n:__label__) REQUIRE n.id IS UNIQUE",
                    {"label": base_label},
                    ident_map={"name": name_base},
                )
            except Exception as exc:  # pragma: no cover - index may already exist
                logger.debug("Constraint creation skipped: %s", exc)

    # Performance indexes (non-unique — safe with any label scheme).
    # Indexes use the BASE label (e.g. "User", not "User:worker_gw7")
    # because Neo4j Community Edition does not support composite-label
    # index syntax (FOR (n:A:B) ON ...).  A global index on :User
    # covers all nodes with that label, including those with additional
    # worker labels used for test isolation.
    index_defs = [
        ("post_created_at", "Post", "createdAt"),
        ("post_author_id", "Post", "authorId"),
        ("user_username", "User", "username"),
        ("user_last_active", "User", "lastActive"),
        ("user_feed_generated_at", "User", "feedGeneratedAt"),
        ("user_is_local", "User", "isLocal"),
    ]
    for name_base, base_label, prop in index_defs:
        try:
            neo4j.execute_labeled(
                "CREATE INDEX __name__ IF NOT EXISTS FOR (n:__label__) ON (n.__prop__)",
                label_map=None,
                ident_map={
                    "name": f"{name_base}{worker_suffix}",
                    "label": base_label,
                    "prop": prop,
                },
            )
        except Exception as exc:  # pragma: no cover - index may already exist
            logger.debug("Index creation skipped: %s", exc)

    # Vector index needs special handling
    # Neo4j CREATE INDEX does not support parameters for OPTIONS,
    # so we validate dimensions is int and use safe string formatting
    # Use same logic as build_embedding_signature for dimensions:
    # For FastText (no base_url), use fasttext_vector_size; for others use llm_dimensions
    if settings.llm_provider == "fasttext" or not settings.llm_base_url:
        dimensions = settings.fasttext_vector_size
    else:
        dimensions = settings.llm_dimensions
    _create_vector_index(neo4j, dimensions)


def _create_vector_index(neo4j: Neo4jClient, dimensions: int) -> None:
    """Create vector index with validated dimensions.

    Uses Neo4jClient.create_vector_index() which validates all parameters
    and safely constructs the DDL (Neo4j doesn't support bind parameters
    for CREATE INDEX OPTIONS).

    For label-based isolation (Dynamic Indexing):
    - If worker_label is set: create index on worker label (e.g., worker_gw0)
      with name like "worker_gw0_posts"
    - If no worker_label: create global index on "Post" label

    This ensures perfect isolation - each worker's index only contains its data.

    If index exists with different dimensions, it is dropped and recreated.
    """
    # Get index name and label based on worker isolation
    index_name = _get_embedding_index_name(neo4j)
    # Worker-specific index: index on worker label, not Post (if worker_label is set)
    # Global index on Post label (if no worker_label)
    label = neo4j.worker_label or "Post"

    # Check if index exists with different dimensions
    try:
        result = list(
            neo4j.execute_and_fetch(
                "SHOW INDEXES YIELD name, type, options "
                "WHERE name = $name AND type = 'VECTOR' "
                "RETURN options",
                {"name": index_name},
            )
        )
        if result:
            existing_dim = _extract_vector_dimension(result[0])
            if existing_dim is not None and existing_dim != dimensions:
                logger.info(
                    "Vector index %s has dimension %s, but need %s. Dropping...",
                    index_name,
                    existing_dim,
                    dimensions,
                )
                neo4j.execute_labeled(
                    "DROP INDEX __idx__ IF EXISTS",
                    ident_map={"idx": index_name},
                )
    except Exception as exc:
        logger.debug("Index dimension check skipped: %s", exc)

    try:
        neo4j.create_vector_index(
            index_name=index_name,
            label=label,
            property_name="embedding",
            dimensions=dimensions,
            similarity_function="cosine",
        )
        logger.info(
            "Created vector index %s for %s.embedding (dimension=%s)", index_name, label, dimensions
        )
    except Exception as exc:  # pragma: no cover
        logger.debug("Vector index creation skipped: %s", exc)


def merge_posts(
    neo4j: Neo4jClient,
    batch: Sequence[Mapping[str, Neo4jParameter]],
    state_id: str | None = None,
    batch_max_id: int | None = None,
) -> int | None:
    """Merge posts into Neo4j with embeddings using APOC dynamic labels.

    New posts are created with ``pagerank`` set to ``0.0``; GDS PageRank overwrites
    this after clustering. On match, only ``embedding`` is updated (not ``pagerank``).

    If state_id is provided, atomically updates AppState.last_processed_status_id
    with the maximum ID from the entire batch (not just posts with embeddings) in the same transaction.

    Args:
        neo4j: Neo4j client
        batch: List of post dictionaries with id, authorId, text, language, embedding, createdAt
        state_id: Optional state ID for atomic state update. If None, only inserts posts.
        batch_max_id: Maximum ID from the entire batch (all statuses, not just those with embeddings).
                     Required if state_id is provided.

    Returns:
        Maximum ID from batch if state_id is provided, None otherwise.
    """
    if not batch:
        return None

    converted_batch = convert_batch_decimals(batch)

    if state_id is not None:
        if batch_max_id is None:
            raise ValueError("batch_max_id is required when state_id is provided")
        # Atomic: insert posts + update state in one transaction
        query = (
            "UNWIND $batch AS row "
            "CALL apoc.merge.node($post_labels, {id: row.id}, "
            "  {authorId: row.authorId, text: row.text, language: row.language, "
            "   embedding: row.embedding, createdAt: datetime(row.createdAt), "
            "   pagerank: 0.0}, "
            "  {embedding: row.embedding}) YIELD node AS p "
            "CALL apoc.merge.node($user_labels, {id: row.authorId}, {}, {}) "
            "YIELD node AS u "
            "MERGE (u)-[:WROTE]->(p) "
            "WITH $batch_max_id AS batch_max_id "
            "MATCH (s:__state_label__ {id: $state_id}) "
            "SET s.last_processed_status_id = CASE "
            "  WHEN batch_max_id > s.last_processed_status_id "
            "  THEN batch_max_id "
            "  ELSE s.last_processed_status_id "
            "END "
            "RETURN s.last_processed_status_id AS new_cursor"
        )
        query_params: dict[str, Neo4jParameter] = {
            "post_labels": neo4j.labels_list("Post"),
            "user_labels": neo4j.labels_list("User"),
            "batch": converted_batch,
            "state_id": state_id,
            "batch_max_id": batch_max_id,
        }
        result = neo4j.execute_and_fetch_labeled(
            query,
            {"post": "Post", "user": "User", "state_label": "AppState"},
            query_params,
        )
        rows = list(result)
        if rows and rows[0].get("new_cursor") is not None:
            return coerce_int(rows[0]["new_cursor"])
        return batch_max_id
    else:
        # Original behavior: just insert posts
        execute_params: dict[str, Neo4jParameter] = {
            "post_labels": neo4j.labels_list("Post"),
            "user_labels": neo4j.labels_list("User"),
            "batch": converted_batch,
        }
        neo4j.execute(
            "UNWIND $batch AS row "
            "CALL apoc.merge.node($post_labels, {id: row.id}, "
            "  {authorId: row.authorId, text: row.text, language: row.language, "
            "   embedding: row.embedding, createdAt: datetime(row.createdAt), "
            "   pagerank: 0.0}, "
            "  {embedding: row.embedding}) YIELD node AS p "
            "CALL apoc.merge.node($user_labels, {id: row.authorId}, {}, {}) "
            "YIELD node AS u "
            "MERGE (u)-[:WROTE]->(p)",
            execute_params,
        )
        return None


def merge_favourites(
    neo4j: Neo4jClient,
    batch: Sequence[Mapping[str, Neo4jParameter]],
    state_id: str | None = None,
    batch_max_id: int | None = None,
) -> int | None:
    """Merge FAVORITED relationships — only for existing Post nodes.

    Uses MATCH instead of apoc.merge.node for the Post side so that
    interactions with posts without embeddings (not in Neo4j) are silently
    skipped.  The User side still uses apoc.merge.node to create the user
    if needed.

    If state_id is provided, atomically updates AppState.last_processed_favourite_id
    with the maximum ID from the batch in the same transaction.

    Args:
        neo4j: Neo4j client
        batch: List of favourite dictionaries with account_id, status_id, created_at
        state_id: Optional state ID for atomic state update. If None, only inserts relationships.
        batch_max_id: Maximum ID from the entire batch. Required if state_id is provided.

    Returns:
        Maximum ID from batch if state_id is provided, None otherwise.
    """
    if not batch:
        return None

    converted_batch = convert_batch_decimals(batch)

    if state_id is not None:
        if batch_max_id is None:
            raise ValueError("batch_max_id is required when state_id is provided")
        # Atomic: insert relationships + update state in one transaction
        query = (
            "UNWIND $batch AS row "
            "CALL apoc.merge.node($user_labels, {id: row.account_id}, {}, {}) "
            "YIELD node AS u "
            "WITH u, row "
            "MATCH (p:__post__ {id: row.status_id}) "
            "MERGE (u)-[f:FAVORITED]->(p) "
            "ON CREATE SET f.at = datetime(row.created_at) "
            "WITH $batch_max_id AS batch_max_id "
            "MATCH (s:__state_label__ {id: $state_id}) "
            "SET s.last_processed_favourite_id = CASE "
            "  WHEN batch_max_id > s.last_processed_favourite_id "
            "  THEN batch_max_id "
            "  ELSE s.last_processed_favourite_id "
            "END "
            "RETURN s.last_processed_favourite_id AS new_cursor"
        )
        result = neo4j.execute_and_fetch_labeled(
            query,
            {"user": "User", "post": "Post", "state_label": "AppState"},
            {
                "user_labels": neo4j.labels_list("User"),
                "batch": converted_batch,
                "state_id": state_id,
                "batch_max_id": batch_max_id,
            },
        )
        rows = list(result)
        if rows and rows[0].get("new_cursor") is not None:
            return coerce_int(rows[0]["new_cursor"])
        return batch_max_id
    else:
        # Original behavior: just insert relationships
        params = _make_params(
            user_labels=neo4j.labels_list("User"),
            batch=converted_batch,
        )
        neo4j.execute_labeled(
            "UNWIND $batch AS row "
            "CALL apoc.merge.node($user_labels, {id: row.account_id}, {}, {}) "
            "YIELD node AS u "
            "WITH u, row "
            "MATCH (p:__post__ {id: row.status_id}) "
            "MERGE (u)-[f:FAVORITED]->(p) "
            "ON CREATE SET f.at = datetime(row.created_at)",
            {"user": "User", "post": "Post"},
            params,
        )
        return None


def merge_blocks(
    neo4j: Neo4jClient,
    batch: Sequence[Mapping[str, Neo4jParameter]],
    state_id: str | None = None,
    batch_max_id: int | None = None,
) -> int | None:
    """Merge HATES_USER relationships from blocks using APOC dynamic labels.

    If state_id is provided, atomically updates AppState.last_processed_block_id
    with the maximum ID from the batch in the same transaction.

    Args:
        neo4j: Neo4j client
        batch: List of block dictionaries with account_id, target_account_id
        state_id: Optional state ID for atomic state update. If None, only inserts relationships.
        batch_max_id: Maximum ID from the entire batch. Required if state_id is provided.

    Returns:
        Maximum ID from batch if state_id is provided, None otherwise.
    """
    if not batch:
        return None

    converted_batch = convert_batch_decimals(batch)

    if state_id is not None:
        if batch_max_id is None:
            raise ValueError("batch_max_id is required when state_id is provided")
        # Atomic: insert relationships + update state in one transaction
        query = (
            "UNWIND $batch AS row "
            "CALL apoc.merge.node($user_labels, {id: row.account_id}, {}, {}) "
            "YIELD node AS u1 "
            "CALL apoc.merge.node($user_labels, {id: row.target_account_id}, {}, {}) "
            "YIELD node AS u2 "
            "MERGE (u1)-[:HATES_USER]->(u2) "
            "WITH $batch_max_id AS batch_max_id "
            "MATCH (s:__state_label__ {id: $state_id}) "
            "SET s.last_processed_block_id = CASE "
            "  WHEN batch_max_id > s.last_processed_block_id "
            "  THEN batch_max_id "
            "  ELSE s.last_processed_block_id "
            "END "
            "RETURN s.last_processed_block_id AS new_cursor"
        )
        params = _make_params(
            user_labels=neo4j.labels_list("User"),
            batch=converted_batch,
            state_id=state_id,
            batch_max_id=batch_max_id,
        )
        result = neo4j.execute_and_fetch_labeled(
            query,
            {"user": "User", "state_label": "AppState"},
            params,
        )
        rows = list(result)
        if rows and rows[0].get("new_cursor") is not None:
            return coerce_int(rows[0]["new_cursor"])
        return batch_max_id
    else:
        # Original behavior: just insert relationships
        params = _make_params(
            user_labels=neo4j.labels_list("User"),
            batch=converted_batch,
        )
        neo4j.execute(
            "UNWIND $batch AS row "
            "CALL apoc.merge.node($user_labels, {id: row.account_id}, {}, {}) "
            "YIELD node AS u1 "
            "CALL apoc.merge.node($user_labels, {id: row.target_account_id}, {}, {}) "
            "YIELD node AS u2 "
            "MERGE (u1)-[:HATES_USER]->(u2)",
            params,
        )
        return None


def merge_mutes(
    neo4j: Neo4jClient,
    batch: Sequence[Mapping[str, Neo4jParameter]],
    state_id: str | None = None,
    batch_max_id: int | None = None,
) -> int | None:
    """Merge HATES_USER relationships from mutes using APOC dynamic labels.

    If state_id is provided, atomically updates AppState.last_processed_mute_id
    with the maximum ID from the batch in the same transaction.

    Args:
        neo4j: Neo4j client
        batch: List of mute dictionaries with account_id, target_account_id
        state_id: Optional state ID for atomic state update. If None, only inserts relationships.
        batch_max_id: Maximum ID from the entire batch. Required if state_id is provided.

    Returns:
        Maximum ID from batch if state_id is provided, None otherwise.
    """
    if not batch:
        return None

    converted_batch = convert_batch_decimals(batch)

    if state_id is not None:
        if batch_max_id is None:
            raise ValueError("batch_max_id is required when state_id is provided")
        # Atomic: insert relationships + update state in one transaction
        query = (
            "UNWIND $batch AS row "
            "CALL apoc.merge.node($user_labels, {id: row.account_id}, {}, {}) "
            "YIELD node AS u1 "
            "CALL apoc.merge.node($user_labels, {id: row.target_account_id}, {}, {}) "
            "YIELD node AS u2 "
            "MERGE (u1)-[:HATES_USER]->(u2) "
            "WITH $batch_max_id AS batch_max_id "
            "MATCH (s:__state_label__ {id: $state_id}) "
            "SET s.last_processed_mute_id = CASE "
            "  WHEN batch_max_id > s.last_processed_mute_id "
            "  THEN batch_max_id "
            "  ELSE s.last_processed_mute_id "
            "END "
            "RETURN s.last_processed_mute_id AS new_cursor"
        )
        params = _make_params(
            user_labels=neo4j.labels_list("User"),
            batch=converted_batch,
            state_id=state_id,
            batch_max_id=batch_max_id,
        )
        result = neo4j.execute_and_fetch_labeled(
            query,
            {"user": "User", "state_label": "AppState"},
            params,
        )
        rows = list(result)
        if rows and rows[0].get("new_cursor") is not None:
            return coerce_int(rows[0]["new_cursor"])
        return batch_max_id
    else:
        # Original behavior: just insert relationships
        params = _make_params(
            user_labels=neo4j.labels_list("User"),
            batch=converted_batch,
        )
        neo4j.execute(
            "UNWIND $batch AS row "
            "CALL apoc.merge.node($user_labels, {id: row.account_id}, {}, {}) "
            "YIELD node AS u1 "
            "CALL apoc.merge.node($user_labels, {id: row.target_account_id}, {}, {}) "
            "YIELD node AS u2 "
            "MERGE (u1)-[:HATES_USER]->(u2)",
            params,
        )
        return None


def merge_bookmarks(
    neo4j: Neo4jClient,
    batch: Sequence[Mapping[str, Neo4jParameter]],
    state_id: str | None = None,
    batch_max_id: int | None = None,
) -> int | None:
    """Merge BOOKMARKED relationships — only for existing Post nodes.

    Bookmarks are a strong implicit interest signal (stronger than favourites).
    Uses MATCH for Post to avoid creating stub nodes.

    If state_id is provided, atomically updates AppState.last_processed_bookmark_id
    with the maximum ID from the batch in the same transaction.

    Args:
        neo4j: Neo4j client
        batch: List of bookmark dictionaries with account_id, status_id, created_at
        state_id: Optional state ID for atomic state update. If None, only inserts relationships.
        batch_max_id: Maximum ID from the entire batch. Required if state_id is provided.

    Returns:
        Maximum ID from batch if state_id is provided, None otherwise.
    """
    if not batch:
        return None

    converted_batch = convert_batch_decimals(batch)

    if state_id is not None:
        if batch_max_id is None:
            raise ValueError("batch_max_id is required when state_id is provided")
        # Atomic: insert relationships + update state in one transaction
        query = (
            "UNWIND $batch AS row "
            "CALL apoc.merge.node($user_labels, {id: row.account_id}, {}, {}) "
            "YIELD node AS u "
            "WITH u, row "
            "MATCH (p:__post__ {id: row.status_id}) "
            "MERGE (u)-[b:BOOKMARKED]->(p) "
            "ON CREATE SET b.at = datetime(row.created_at) "
            "WITH $batch_max_id AS batch_max_id "
            "MATCH (s:__state_label__ {id: $state_id}) "
            "SET s.last_processed_bookmark_id = CASE "
            "  WHEN batch_max_id > s.last_processed_bookmark_id "
            "  THEN batch_max_id "
            "  ELSE s.last_processed_bookmark_id "
            "END "
            "RETURN s.last_processed_bookmark_id AS new_cursor"
        )
        params = _make_params(
            user_labels=neo4j.labels_list("User"),
            batch=converted_batch,
            state_id=state_id,
            batch_max_id=batch_max_id,
        )
        result = neo4j.execute_and_fetch_labeled(
            query,
            {"user": "User", "post": "Post", "state_label": "AppState"},
            params,
        )
        rows = list(result)
        if rows and rows[0].get("new_cursor") is not None:
            return coerce_int(rows[0]["new_cursor"])
        return batch_max_id
    else:
        # Original behavior: just insert relationships
        params = _make_params(
            user_labels=neo4j.labels_list("User"),
            batch=converted_batch,
        )
        neo4j.execute_labeled(
            "UNWIND $batch AS row "
            "CALL apoc.merge.node($user_labels, {id: row.account_id}, {}, {}) "
            "YIELD node AS u "
            "WITH u, row "
            "MATCH (p:__post__ {id: row.status_id}) "
            "MERGE (u)-[b:BOOKMARKED]->(p) "
            "ON CREATE SET b.at = datetime(row.created_at)",
            {"user": "User", "post": "Post"},
            params,
        )
        return None


def merge_reblogs(neo4j: Neo4jClient, batch: Sequence[Mapping[str, Neo4jParameter]]) -> None:
    """Merge REBLOGGED relationships — only for existing Post nodes.

    Uses MATCH for Post to avoid creating stub nodes.  The user-user
    interaction signal is captured separately via INTERACTS_WITH.
    """
    if not batch:
        return
    converted_batch = convert_batch_decimals(batch)
    params = _make_params(
        user_labels=neo4j.labels_list("User"),
        batch=converted_batch,
    )
    neo4j.execute_labeled(
        "UNWIND $batch AS row "
        "CALL apoc.merge.node($user_labels, {id: row.account_id}, {}, {}) "
        "YIELD node AS u "
        "WITH u, row "
        "MATCH (p:__post__ {id: row.reblog_of_id}) "
        "MERGE (u)-[r:REBLOGGED]->(p) "
        "ON CREATE SET r.at = datetime(row.created_at)",
        {"user": "User", "post": "Post"},
        params,
    )


def merge_replies(neo4j: Neo4jClient, batch: Sequence[Mapping[str, Neo4jParameter]]) -> None:
    """Merge REPLIED relationships — only for existing Post nodes.

    Uses MATCH for Post to avoid creating stub nodes.  The user-user
    interaction signal is captured separately via INTERACTS_WITH.
    """
    if not batch:
        return
    converted_batch = convert_batch_decimals(batch)
    params = _make_params(
        user_labels=neo4j.labels_list("User"),
        batch=converted_batch,
    )
    neo4j.execute_labeled(
        "UNWIND $batch AS row "
        "CALL apoc.merge.node($user_labels, {id: row.account_id}, {}, {}) "
        "YIELD node AS u "
        "WITH u, row "
        "MATCH (p:__post__ {id: row.in_reply_to_id}) "
        "MERGE (u)-[r:REPLIED]->(p) "
        "ON CREATE SET r.at = datetime(row.created_at)",
        {"user": "User", "post": "Post"},
        params,
    )


def merge_interactions(
    neo4j: Neo4jClient,
    batch: Sequence[Mapping[str, Neo4jParameter]],
    state_id: str | None = None,
) -> None:
    """Merge INTERACTS_WITH relationships between users.

    Each row must contain ``source_id``, ``target_id``, and
    ``total_weight``.  Uses APOC to ensure both User nodes exist
    and creates a weighted directed edge.

    Incremental: ON CREATE sets weight, ON MATCH adds to existing weight.

    If ``state_id`` is provided, atomically updates 4 interaction cursors
    in AppState using ``reduce()`` to compute global max IDs from the batch.

    Args:
        neo4j: Neo4j client
        batch: Row dicts with source_id, target_id, total_weight,
            and optionally max_favourite_id, max_status_id,
            max_mention_id, max_follow_id
        state_id: Optional state ID for atomic cursor update
    """
    if not batch:
        return
    converted_batch = convert_batch_decimals(batch)

    if state_id is not None:
        query: LiteralString = (
            "WITH $batch AS data, "
            "  reduce(m = 0, x IN $batch | CASE WHEN coalesce(x.max_favourite_id, 0) > m "
            "    THEN coalesce(x.max_favourite_id, 0) ELSE m END) AS batch_max_fav, "
            "  reduce(m = 0, x IN $batch | CASE WHEN coalesce(x.max_status_id, 0) > m "
            "    THEN coalesce(x.max_status_id, 0) ELSE m END) AS batch_max_stat, "
            "  reduce(m = 0, x IN $batch | CASE WHEN coalesce(x.max_mention_id, 0) > m "
            "    THEN coalesce(x.max_mention_id, 0) ELSE m END) AS batch_max_ment, "
            "  reduce(m = 0, x IN $batch | CASE WHEN coalesce(x.max_follow_id, 0) > m "
            "    THEN coalesce(x.max_follow_id, 0) ELSE m END) AS batch_max_foll "
            "MATCH (state:__state_label__ {id: $state_id}) "
            "SET state.last_processed_interaction_favourite_id = CASE "
            "  WHEN batch_max_fav > state.last_processed_interaction_favourite_id "
            "  THEN batch_max_fav ELSE state.last_processed_interaction_favourite_id END, "
            "  state.last_processed_interaction_status_id = CASE "
            "  WHEN batch_max_stat > state.last_processed_interaction_status_id "
            "  THEN batch_max_stat ELSE state.last_processed_interaction_status_id END, "
            "  state.last_processed_interaction_mention_id = CASE "
            "  WHEN batch_max_ment > state.last_processed_interaction_mention_id "
            "  THEN batch_max_ment ELSE state.last_processed_interaction_mention_id END, "
            "  state.last_processed_interaction_follow_id = CASE "
            "  WHEN batch_max_foll > state.last_processed_interaction_follow_id "
            "  THEN batch_max_foll ELSE state.last_processed_interaction_follow_id END "
            "WITH data "
            "UNWIND data AS row "
            "CALL apoc.merge.node($user_labels, {id: row.source_id}, {}, {}) "
            "YIELD node AS u1 "
            "CALL apoc.merge.node($user_labels, {id: row.target_id}, {}, {}) "
            "YIELD node AS u2 "
            "MERGE (u1)-[r:INTERACTS_WITH]->(u2) "
            "ON CREATE SET r.weight = row.total_weight "
            "ON MATCH SET r.weight = r.weight + row.total_weight"
        )
        neo4j.execute_labeled(
            query,
            {"state_label": "AppState"},
            {
                "user_labels": neo4j.labels_list("User"),
                "batch": converted_batch,
                "state_id": state_id,
            },
        )
    else:
        params = _make_params(
            user_labels=neo4j.labels_list("User"),
            batch=converted_batch,
        )
        neo4j.execute(
            "UNWIND $batch AS row "
            "CALL apoc.merge.node($user_labels, {id: row.source_id}, {}, {}) "
            "YIELD node AS u1 "
            "CALL apoc.merge.node($user_labels, {id: row.target_id}, {}, {}) "
            "YIELD node AS u2 "
            "MERGE (u1)-[r:INTERACTS_WITH]->(u2) "
            "ON CREATE SET r.weight = row.total_weight "
            "ON MATCH SET r.weight = r.weight + row.total_weight",
            params,
        )


def merge_status_stats(
    neo4j: Neo4jClient,
    batch: Sequence[Mapping[str, Neo4jParameter]],
    state_id: str | None = None,
    batch_max_id: int | None = None,
) -> int | None:
    """Set popularity properties on existing Post nodes from status_stats.

    Only updates posts that already exist in Neo4j (posts with
    embeddings).  Rows referencing non-existent posts are silently
    skipped.

    If state_id is provided, atomically updates AppState.last_processed_status_stats_id
    with the maximum ID from the batch in the same transaction.

    Args:
        neo4j: Neo4j client
        batch: List of status_stats dictionaries with id, total_favourites, total_reblogs, total_replies
        state_id: Optional state ID for atomic state update. If None, only updates posts.
        batch_max_id: Maximum ID from the entire batch. Required if state_id is provided.

    Returns:
        Maximum ID from batch if state_id is provided, None otherwise.
    """
    if not batch:
        return None

    converted_batch = convert_batch_decimals(batch)

    if state_id is not None:
        if batch_max_id is None:
            raise ValueError("batch_max_id is required when state_id is provided")
        # Atomic: update posts + update state in one transaction
        query = (
            "UNWIND $batch AS row "
            "MATCH (p:__post__ {id: row.id}) "
            "SET p.totalFavourites = row.total_favourites, "
            "    p.totalReblogs = row.total_reblogs, "
            "    p.totalReplies = row.total_replies "
            "WITH $batch_max_id AS batch_max_id "
            "MATCH (s:__state_label__ {id: $state_id}) "
            "SET s.last_processed_status_stats_id = CASE "
            "  WHEN batch_max_id > s.last_processed_status_stats_id "
            "  THEN batch_max_id "
            "  ELSE s.last_processed_status_stats_id "
            "END "
            "RETURN s.last_processed_status_stats_id AS new_cursor"
        )
        params = _make_params(
            batch=converted_batch,
            state_id=state_id,
            batch_max_id=batch_max_id,
        )
        result = neo4j.execute_and_fetch_labeled(
            query,
            {"post": "Post", "state_label": "AppState"},
            params,
        )
        rows = list(result)
        if rows and rows[0].get("new_cursor") is not None:
            return coerce_int(rows[0]["new_cursor"])
        return batch_max_id
    else:
        # Original behavior: just update posts
        params = _make_params(batch=converted_batch)
        neo4j.execute_labeled(
            "UNWIND $batch AS row "
            "MATCH (p:__post__ {id: row.id}) "
            "SET p.totalFavourites = row.total_favourites, "
            "    p.totalReblogs = row.total_reblogs, "
            "    p.totalReplies = row.total_replies",
            {"post": "Post"},
            params,
        )
        return None


@dataclass
class EmbeddingMigrationResult:
    """Result of embedding configuration check."""

    migrated: bool
    previous_signature: str | None
    current_signature: str
    posts_cleared: int = 0


def check_embedding_config(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    state_store: StateStore,
) -> EmbeddingMigrationResult:
    """Check if embedding configuration changed and migrate if needed.

    Compares stored signature with current configuration.
    If different, drops vector index and clears embeddings from posts.

    Args:
        neo4j: Neo4j client
        settings: Current settings
        state_store: State store for reading/writing signature

    Returns:
        EmbeddingMigrationResult with migration status
    """
    state = state_store.load()
    current_sig = build_embedding_signature(settings)
    stored_sig = state.embedding_signature

    # First run - record signature.
    # If posts already have embeddings (e.g. from a previous run with
    # a different provider/dimensions), they must be cleared to prevent
    # a dimension mismatch when the new vector index is queried.
    if not stored_sig:
        logger.info("Recording initial embedding signature: %s", current_sig)

        # Check for stale embeddings left by a prior configuration
        has_embeddings = _count_posts_with_embeddings(neo4j)
        if has_embeddings > 0:
            logger.warning(
                "First run but %d posts already have embeddings. "
                "Clearing to prevent dimension mismatch.",
                has_embeddings,
            )
            posts_cleared = _perform_embedding_migration(neo4j, state, current_sig, state_store)
            return EmbeddingMigrationResult(
                migrated=True,
                previous_signature=None,
                current_signature=current_sig,
                posts_cleared=posts_cleared,
            )

        state.embedding_signature = current_sig
        state_store.save(state)
        return EmbeddingMigrationResult(
            migrated=False,
            previous_signature=None,
            current_signature=current_sig,
        )

    # No change - continue normally
    if stored_sig == current_sig:
        logger.debug("Embedding signature unchanged: %s", current_sig)
        return EmbeddingMigrationResult(
            migrated=False,
            previous_signature=stored_sig,
            current_signature=current_sig,
        )

    # Configuration changed - trigger migration
    logger.warning(
        "Embedding configuration changed:\n"
        "  Previous: %s\n"
        "  Current:  %s\n"
        "Triggering full re-indexing...",
        stored_sig,
        current_sig,
    )

    posts_cleared = _perform_embedding_migration(neo4j, state, current_sig, state_store)
    return EmbeddingMigrationResult(
        migrated=True,
        previous_signature=stored_sig,
        current_signature=current_sig,
        posts_cleared=posts_cleared,
    )


def _count_posts_with_embeddings(neo4j: Neo4jClient) -> int:
    """Count posts that already have embeddings.

    Used to detect stale embeddings from a prior configuration
    during first-run signature recording.

    Args:
        neo4j: Neo4j client

    Returns:
        Number of Post nodes with non-null embedding property
    """
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) WHERE p.embedding IS NOT NULL RETURN count(p) AS count",
            {"post": "Post"},
        )
    )
    return coerce_int(result[0]["count"]) if result else 0


def _perform_embedding_migration(
    neo4j: Neo4jClient,
    state: PipelineState,
    new_signature: str,
    state_store: StateStore,
) -> int:
    """Execute embedding migration: drop index, clear embeddings, reset cursor.

    Args:
        neo4j: Neo4j client
        state: Current pipeline state
        new_signature: New embedding signature to save
        state_store: State store for persistence

    Returns:
        Number of posts whose embeddings were cleared
    """
    # 1. Drop old vector index (will be recreated with correct dimensions)
    # Use worker-specific index name if worker_label is set (Dynamic Indexing)
    index_name = _get_embedding_index_name(neo4j)
    try:
        neo4j.execute_labeled(
            "DROP INDEX __idx__ IF EXISTS",
            ident_map={"idx": index_name},
        )
        logger.info("Dropped vector index %s", index_name)
    except Exception as e:
        logger.debug("Index drop skipped (may not exist): %s", e)

    # 2. Count posts that will be reembedded
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) RETURN count(p) AS count",
            {"post": "Post"},
        )
    )
    count = coerce_int(result[0]["count"]) if result else 0

    # 3. Clear old embeddings to prevent dimension mismatch errors.
    # Without this, posts with old-dimension embeddings would cause
    # db.index.vector.queryNodes to fail when querying new-dimension index.
    cleared_result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) WHERE p.embedding IS NOT NULL "
            "REMOVE p.embedding RETURN count(p) AS cleared",
            {"post": "Post"},
        )
    )
    cleared = coerce_int(cleared_result[0]["cleared"]) if cleared_result else 0
    if cleared > 0:
        logger.info("Cleared %d old embeddings from Post nodes", cleared)

    # 4. Save new signature (DO NOT reset last_status_id!)
    # We keep cursor position to continue incremental loading after reembedding.
    # The reembed_existing_posts() function will update embeddings for existing posts.
    state.embedding_signature = new_signature
    state_store.save(state)

    logger.info(
        "Migration prepared. New signature: %s. %d posts will be reembedded.",
        new_signature,
        count,
    )

    return count


def force_reindex(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    state_store: StateStore,
    *,
    dry_run: bool = False,
) -> EmbeddingMigrationResult:
    """Force re-indexing of all embeddings.

    Drops vector index and prepares for reembedding existing posts.
    Does NOT reset cursor - incremental loading continues from last position.
    Use this when you want to rebuild embeddings manually.

    After calling this, run reembed_existing_posts() to compute new embeddings.

    Args:
        neo4j: Neo4j client
        settings: Current settings
        state_store: State store for persistence
        dry_run: If True, only report what would happen

    Returns:
        EmbeddingMigrationResult with migration status
    """
    state = state_store.load()
    current_sig = build_embedding_signature(settings)
    previous_sig = state.embedding_signature

    # Count posts that will be reembedded
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) RETURN count(p) AS count",
            {"post": "Post"},
        )
    )
    count = coerce_int(result[0]["count"]) if result else 0

    if dry_run:
        logger.info("DRY RUN: Would reembed %d posts with new configuration", count)
        return EmbeddingMigrationResult(
            migrated=False,
            previous_signature=previous_sig or None,
            current_signature=current_sig,
            posts_cleared=count,
        )

    posts_cleared = _perform_embedding_migration(neo4j, state, current_sig, state_store)
    return EmbeddingMigrationResult(
        migrated=True,
        previous_signature=previous_sig or None,
        current_signature=current_sig,
        posts_cleared=posts_cleared,
    )


def get_embedding_status(
    settings: HintGridSettings,
    state_store: StateStore,
) -> dict[str, str | bool]:
    """Get current embedding configuration status.

    Args:
        settings: Current settings
        state_store: State store for reading signature

    Returns:
        Dictionary with stored/current signatures and match status
    """
    state = state_store.load()
    current_sig = build_embedding_signature(settings)
    stored_sig = state.embedding_signature

    return {
        "stored_signature": stored_sig or "(not set)",
        "current_signature": current_sig,
        "match": stored_sig == current_sig if stored_sig else True,
    }


def reembed_existing_posts(
    neo4j: Neo4jClient,
    embedding_provider: EmbeddingProvider,
    settings: HintGridSettings,
    batch_size: int = 1000,
) -> int:
    """Recompute embeddings for all posts already in Neo4j.

    Uses streaming cursor for O(N) complexity instead of SKIP+LIMIT O(N²).

    Key benefits:
    - Does NOT reload from PostgreSQL
    - Does NOT change relationships (FOLLOWS, FAVORITED, WROTE, etc.)
    - Does NOT change cluster assignments (UserCommunity, PostCommunity)
    - Only updates the embedding field on Post nodes
    - Memory-efficient streaming with configurable batch size

    Args:
        neo4j: Neo4j client
        embedding_provider: Provider for computing embeddings
        settings: HintGrid settings
        batch_size: Number of posts to process per batch

    Returns:
        Total number of posts reembedded
    """
    # Get total count for progress bar
    total_posts = count_posts_in_neo4j(neo4j)

    total = 0
    batch: list[tuple[int, str]] = []

    with create_batch_progress(total_posts, settings=settings) as progress:
        task = progress.add_task("[cyan]Reembedding posts...[/cyan]", total=total_posts)

        # Stream all posts using server-side cursor - O(N) complexity
        for row in neo4j.stream_query_labeled(
            "MATCH (p:__post__) WHERE p.text IS NOT NULL RETURN p.id AS id, p.text AS text",
            {"post": "Post"},
            fetch_size=batch_size,
        ):
            batch.append((coerce_int(row["id"]), coerce_str(row["text"])))

            # Process batch when full
            if len(batch) >= batch_size:
                processed = _process_embedding_batch(neo4j, embedding_provider, batch)
                total += processed
                progress.advance(task, processed)
                logger.debug("Reembedded %d posts...", total)
                batch = []

        # Process remaining batch
        if batch:
            processed = _process_embedding_batch(neo4j, embedding_provider, batch)
            total += processed
            progress.advance(task, processed)

    logger.info("Reembedding complete: %d posts updated", total)
    return total


def _process_embedding_batch(
    neo4j: Neo4jClient,
    embedding_provider: EmbeddingProvider,
    batch: list[tuple[int, str]],
) -> int:
    """Compute embeddings and update posts in Neo4j.

    Args:
        neo4j: Neo4j client
        embedding_provider: Provider for computing embeddings
        batch: List of (post_id, text) tuples

    Returns:
        Number of posts processed
    """
    if not batch:
        return 0

    # Compute new embeddings
    embeddings = embedding_provider.embed_texts(batch)

    # Update embeddings in Neo4j using UNWIND for efficiency
    # embeddings[i] is list[float], which is now part of Neo4jParameter union type
    update_data: list[dict[str, Neo4jParameter]] = [
        {"id": batch[i][0], "embedding": embeddings[i]} for i in range(len(batch))
    ]
    # update_data is Sequence[Mapping[str, Neo4jParameter]], which is valid for Neo4jParameter
    params: dict[str, Neo4jParameter] = {"batch": update_data}
    neo4j.execute_labeled(
        "UNWIND $batch AS row MATCH (p:__post__ {id: row.id}) SET p.embedding = row.embedding",
        {"post": "Post"},
        params,
    )

    return len(batch)


def check_embeddings_exist(neo4j: Neo4jClient) -> bool:
    """Check if any posts have embeddings.

    Args:
        neo4j: Neo4j client

    Returns:
        True if at least one post has an embedding, False otherwise
    """
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (p:__post__) WHERE p.embedding IS NOT NULL RETURN count(p) AS count LIMIT 1",
        {"post": "Post"},
    )
    count = coerce_int(result[0].get("count")) if result else 0
    return count > 0


def check_clusters_exist(neo4j: Neo4jClient) -> tuple[bool, bool]:
    """Check whether user/post community structure exists in the graph.

    Uses counts of ``UserCommunity`` and ``PostCommunity`` nodes (materialized
    after Leiden in clustering), not ``User.cluster_id`` / ``Post.cluster_id``.
    That avoids referencing a property key before it has ever been written,
    which would trigger Memgraph GQL warning ``01N52`` on a cold graph.

    Args:
        neo4j: Neo4j client

    Returns:
        Tuple of (users_exist, posts_exist): each True if at least one matching
        community node exists.
    """
    user_result = neo4j.execute_and_fetch_labeled(
        "MATCH (uc:__uc__) RETURN count(uc) AS count LIMIT 1",
        {"uc": "UserCommunity"},
    )
    user_count = coerce_int(user_result[0].get("count")) if user_result else 0
    users_exist = user_count > 0

    post_result = neo4j.execute_and_fetch_labeled(
        "MATCH (pc:__pc__) RETURN count(pc) AS count LIMIT 1",
        {"pc": "PostCommunity"},
    )
    post_count = coerce_int(post_result[0].get("count")) if post_result else 0
    posts_exist = post_count > 0

    return (users_exist, posts_exist)


def check_interests_exist(neo4j: Neo4jClient) -> bool:
    """Check if INTERESTED_IN relationships exist.

    Args:
        neo4j: Neo4j client

    Returns:
        True if at least one INTERESTED_IN relationship exists, False otherwise
    """
    result = neo4j.execute_and_fetch_labeled(
        "MATCH (uc:__uc__)-[i:INTERESTED_IN]->(:__pc__) RETURN count(i) AS count LIMIT 1",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )
    count = coerce_int(result[0].get("count")) if result else 0
    return count > 0


def update_user_activity(
    neo4j: Neo4jClient,
    batch: Sequence[Mapping[str, Neo4jParameter]],
    state_id: str | None = None,
    batch_max_id: int | None = None,
) -> int | None:
    """Batch-update User.lastActive, isLocal, languages from PostgreSQL activity data.

    Each row must have 'account_id' (int), 'last_active' (datetime),
    'is_local' (bool), and optionally 'chosen_languages' (list[str] | None).
    Uses UNWIND for efficient batch processing.

    If state_id is provided, atomically updates AppState.last_processed_activity_account_id
    with the maximum account_id from the batch in the same transaction.

    Args:
        neo4j: Neo4j client
        batch: List of dicts with account_id, last_active, is_local, chosen_languages
        state_id: Optional state ID for atomic state update. If None, only updates users.
        batch_max_id: Maximum account_id from the entire batch. Required if state_id is provided.

    Returns:
        Maximum account_id from batch if state_id is provided, None otherwise.
    """
    if not batch:
        return None

    converted_batch = convert_batch_decimals(batch)

    if state_id is not None:
        if batch_max_id is None:
            raise ValueError("batch_max_id is required when state_id is provided")
        # Atomic: update users + update state in one transaction
        query = (
            "UNWIND $batch AS row "
            "MATCH (u:__user__ {id: row.account_id}) "
            "SET u.lastActive = datetime(row.last_active), "
            "    u.isLocal = row.is_local, "
            "    u.languages = row.chosen_languages "
            "WITH $batch_max_id AS batch_max_id "
            "MATCH (s:__state_label__ {id: $state_id}) "
            "SET s.last_processed_activity_account_id = CASE "
            "  WHEN batch_max_id > s.last_processed_activity_account_id "
            "  THEN batch_max_id "
            "  ELSE s.last_processed_activity_account_id "
            "END "
            "RETURN s.last_processed_activity_account_id AS new_cursor"
        )
        params = _make_params(
            batch=converted_batch,
            state_id=state_id,
            batch_max_id=batch_max_id,
        )
        result = neo4j.execute_and_fetch_labeled(
            query,
            {"user": "User", "state_label": "AppState"},
            params,
        )
        rows = list(result)
        if rows and rows[0].get("new_cursor") is not None:
            return coerce_int(rows[0]["new_cursor"])
        return batch_max_id
    else:
        # Original behavior: just update users
        params = _make_params(batch=converted_batch)
        neo4j.execute_labeled(
            "UNWIND $batch AS row "
            "MATCH (u:__user__ {id: row.account_id}) "
            "SET u.lastActive = datetime(row.last_active), "
            "    u.isLocal = row.is_local, "
            "    u.languages = row.chosen_languages",
            {"user": "User"},
            params,
        )
        return None


def cleanup_inactive_users(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    progress: HintGridProgress | None = None,
) -> int:
    """Cascade-delete inactive users and their posts from the graph.

    Users with lastActive older than active_user_days are removed along with:
    1. SIMILAR_TO relationships on their posts
    2. BELONGS_TO relationships on their posts (Post -> PostCommunity)
    3. FAVORITED relationships from other users to their posts
    4. All user relationships (FOLLOWS, FAVORITED, REPLIED, BELONGS_TO, WAS_RECOMMENDED, HATES_USER)
    5. Post nodes authored by inactive users
    6. User nodes
    7. Orphaned PostCommunity and UserCommunity nodes (size = 0)

    Uses apoc.periodic.iterate for batch processing to prevent OOM.

    Args:
        neo4j: Neo4j client
        settings: Settings with active_user_days threshold

    Returns:
        Total number of deleted user nodes
    """
    active_days = settings.active_user_days
    batch_size = settings.apoc_batch_size

    def _execute_step_with_progress(
        step_num: int,
        step_name: str,
        iterate_query: LiteralString,
        action_query: LiteralString,
        count_query: LiteralString,
        label_map: dict[str, str],
        params: Mapping[str, Neo4jParameter],
    ) -> dict[str, Neo4jValue]:
        """Execute a cleanup step with progress tracking."""
        # Get total count for progress tracking using separate count query
        count_result = neo4j.execute_and_fetch_labeled(
            count_query,
            label_map,
            params,
        )
        total = coerce_int(count_result[0].get("total", 0)) if count_result else None

        # Create ProgressTracker and start polling if progress is provided
        operation_id = f"cleanup_step_{step_num}_{uuid.uuid4().hex[:8]}"
        polling_thread = None
        task_id: TaskID | None = None

        if progress is not None:
            neo4j.create_progress_tracker(operation_id, total)
            task_id = progress.add_task(
                f"[cyan]Step {step_num}: {step_name}...",
                total=total,
            )
            from hintgrid.cli.console import track_periodic_iterate_progress

            polling_thread = track_periodic_iterate_progress(
                neo4j,
                operation_id,
                progress,
                task_id,
                poll_interval=settings.progress_poll_interval_seconds,
            )

        try:
            # Execute the step with progress tracking
            # Note: For DELETE operations, apoc.periodic.iterate returns total (number of items
            # found by iterate_query), not the number of deleted items. Since DELETE operations
            # don't return rows, we rely on total from iterate_query as the count of processed items.
            result = neo4j.execute_periodic_iterate(
                iterate_query,
                action_query,
                label_map=label_map,
                batch_size=batch_size,
                batch_mode="BATCH",
                progress_tracker_id=operation_id if progress is not None else None,
                params=params,
            )
            return result
        finally:
            # Stop polling thread
            if polling_thread is not None:
                polling_thread.stop_event.set()
                polling_thread.join(timeout=2.0)

            # Clean up ProgressTracker
            if progress is not None:
                neo4j.cleanup_progress_tracker(operation_id)
                if task_id is not None:
                    progress.update(
                        task_id, description=f"[green]✓ Step {step_num}: {step_name} complete"
                    )

    # Step 1: Delete SIMILAR_TO on posts of inactive users
    # Note: In batchMode="BATCH", $_batch contains result rows (dicts), not direct objects.
    # We must extract the relationship using row.r because iterate_query returns "RETURN r"
    _execute_step_with_progress(
        1,
        "Delete SIMILAR_TO relationships",
        "MATCH (u:__user__) "
        "WHERE u.lastActive < datetime() - duration({days: $days}) "
        "MATCH (u)-[:WROTE]->(p:__post__)-[r:SIMILAR_TO]-() "
        "RETURN r",
        "UNWIND $_batch AS row WITH row.r AS rel DELETE rel",
        "MATCH (u:__user__) "
        "WHERE u.lastActive < datetime() - duration({days: $days}) "
        "MATCH (u)-[:WROTE]->(p:__post__)-[r:SIMILAR_TO]-() "
        "RETURN count(*) AS total",
        {"user": "User", "post": "Post"},
        {"days": active_days},
    )

    # Step 2: Delete BELONGS_TO on posts of inactive users
    # Note: Extract relationship from batch row using row.r
    _execute_step_with_progress(
        2,
        "Delete BELONGS_TO relationships",
        "MATCH (u:__user__) "
        "WHERE u.lastActive < datetime() - duration({days: $days}) "
        "MATCH (u)-[:WROTE]->(p:__post__)-[r:BELONGS_TO]->(:__pc__) "
        "RETURN r",
        "UNWIND $_batch AS row WITH row.r AS rel DELETE rel",
        "MATCH (u:__user__) "
        "WHERE u.lastActive < datetime() - duration({days: $days}) "
        "MATCH (u)-[:WROTE]->(p:__post__)-[r:BELONGS_TO]->(:__pc__) "
        "RETURN count(*) AS total",
        {"user": "User", "post": "Post", "pc": "PostCommunity"},
        {"days": active_days},
    )

    # Step 3: Delete FAVORITED from other users to posts of inactive users
    # Note: Extract relationship from batch row using row.r
    _execute_step_with_progress(
        3,
        "Delete FAVORITED relationships",
        "MATCH (u:__user__) "
        "WHERE u.lastActive < datetime() - duration({days: $days}) "
        "MATCH (u)-[:WROTE]->(p:__post__)<-[r:FAVORITED]-() "
        "RETURN r",
        "UNWIND $_batch AS row WITH row.r AS rel DELETE rel",
        "MATCH (u:__user__) "
        "WHERE u.lastActive < datetime() - duration({days: $days}) "
        "MATCH (u)-[:WROTE]->(p:__post__)<-[r:FAVORITED]-() "
        "RETURN count(*) AS total",
        {"user": "User", "post": "Post"},
        {"days": active_days},
    )

    # Step 4: Delete Post nodes of inactive users (before deleting WROTE relationships)
    # Note: Extract node from batch row using row.p because iterate_query returns "RETURN p"
    _execute_step_with_progress(
        4,
        "Delete Post nodes",
        "MATCH (u:__user__) "
        "WHERE u.lastActive < datetime() - duration({days: $days}) "
        "MATCH (u)-[:WROTE]->(p:__post__) "
        "RETURN p",
        "UNWIND $_batch AS row WITH row.p AS node DETACH DELETE node",
        "MATCH (u:__user__) "
        "WHERE u.lastActive < datetime() - duration({days: $days}) "
        "MATCH (u)-[:WROTE]->(p:__post__) "
        "RETURN count(*) AS total",
        {"user": "User", "post": "Post"},
        {"days": active_days},
    )

    # Step 5: Delete all relationships of inactive users (after posts are deleted)
    # Note: Extract relationship from batch row using row.r
    _execute_step_with_progress(
        5,
        "Delete user relationships",
        "MATCH (u:__user__) "
        "WHERE u.lastActive < datetime() - duration({days: $days}) "
        "MATCH (u)-[r]-() "
        "RETURN r",
        "UNWIND $_batch AS row WITH row.r AS rel DELETE rel",
        "MATCH (u:__user__) "
        "WHERE u.lastActive < datetime() - duration({days: $days}) "
        "MATCH (u)-[r]-() "
        "RETURN count(*) AS total",
        {"user": "User"},
        {"days": active_days},
    )

    # Step 6: Delete inactive User nodes, count them
    # Note: Extract node from batch row using row.u because iterate_query returns "RETURN u"
    result = _execute_step_with_progress(
        6,
        "Delete User nodes",
        "MATCH (u:__user__) WHERE u.lastActive < datetime() - duration({days: $days}) RETURN u",
        "UNWIND $_batch AS row WITH row.u AS node DETACH DELETE node",
        "MATCH (u:__user__) "
        "WHERE u.lastActive < datetime() - duration({days: $days}) "
        "RETURN count(*) AS total",
        {"user": "User"},
        {"days": active_days},
    )
    deleted_users = coerce_int(result.get("total", 0))

    # Step 7: Delete orphaned community nodes (no members)
    neo4j.execute_labeled(
        "MATCH (uc:__uc__) WHERE NOT (uc)<-[:BELONGS_TO]-() DETACH DELETE uc",
        {"uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (pc:__pc__) WHERE NOT (pc)<-[:BELONGS_TO]-() DETACH DELETE pc",
        {"pc": "PostCommunity"},
    )

    # Step 8: Recalculate community sizes
    neo4j.execute_labeled(
        "MATCH (u:__user__)-[:BELONGS_TO]->(uc:__uc__) "
        "WITH uc, count(u) AS size SET uc.size = size",
        {"user": "User", "uc": "UserCommunity"},
    )
    neo4j.execute_labeled(
        "MATCH (p:__post__)-[:BELONGS_TO]->(pc:__pc__) "
        "WITH pc, count(p) AS size SET pc.size = size",
        {"post": "Post", "pc": "PostCommunity"},
    )

    logger.info("Cleanup complete: %d inactive users deleted", deleted_users)
    return deleted_users


def count_posts_in_neo4j(neo4j: Neo4jClient) -> int:
    """Count total posts in Neo4j.

    Args:
        neo4j: Neo4j client

    Returns:
        Number of Post nodes in database
    """
    result = list(
        neo4j.execute_and_fetch_labeled(
            "MATCH (p:__post__) RETURN count(p) AS count",
            {"post": "Post"},
        )
    )
    return coerce_int(result[0]["count"]) if result else 0
