"""Batched creation of UserCommunity/PostCommunity nodes and BELONGS_TO edges."""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Literal, LiteralString

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient, Neo4jParameter
    from hintgrid.cli.progress_display import HintGridProgress
    from hintgrid.config import HintGridSettings

from hintgrid.cli.console import console
from hintgrid.utils.coercion import coerce_int

logger = logging.getLogger(__name__)

_DELETE_USER_BELONGS: LiteralString = (
    "MATCH (u:__user__)-[old:BELONGS_TO]->(uc:__uc__) DELETE old"
)
_DELETE_POST_BELONGS: LiteralString = (
    "MATCH (p:__post__)-[old:BELONGS_TO]->(pc:__pc__) DELETE old"
)

_COUNT_USERS_WITH_CLUSTER: LiteralString = (
    "MATCH (u:__user__) WHERE u.cluster_id IS NOT NULL RETURN count(*) AS total"
)
_COUNT_POSTS_WITH_CLUSTER: LiteralString = (
    "MATCH (p:__post__) WHERE p.cluster_id IS NOT NULL RETURN count(*) AS total"
)

_ITERATE_USER_ROWS: LiteralString = (
    "MATCH (u:__user__) WHERE u.cluster_id IS NOT NULL RETURN id(u) AS node_id"
)
_ITERATE_POST_ROWS: LiteralString = (
    "MATCH (p:__post__) WHERE p.cluster_id IS NOT NULL RETURN id(p) AS node_id"
)

_ACTION_USER: LiteralString = (
    "UNWIND $_batch AS row "
    "MATCH (u:__user__) WHERE id(u) = row.node_id "
    "WITH u, u.cluster_id AS cluster_id "
    "CALL apoc.merge.node($uc_labels, {id: cluster_id}, {}, {}) YIELD node AS uc "
    "MERGE (u)-[:BELONGS_TO]->(uc)"
)
_ACTION_POST: LiteralString = (
    "UNWIND $_batch AS row "
    "MATCH (p:__post__) WHERE id(p) = row.node_id "
    "WITH p, p.cluster_id AS cluster_id "
    "CALL apoc.merge.node($pc_labels, {id: cluster_id}, {}, {}) YIELD node AS pc "
    "MERGE (p)-[:BELONGS_TO]->(pc)"
)

USER_LABEL_MAP: dict[str, str] = {"user": "User", "uc": "UserCommunity"}
POST_LABEL_MAP: dict[str, str] = {"post": "Post", "pc": "PostCommunity"}


def _run_periodic_community_structure(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    *,
    delete_query: LiteralString,
    count_query: LiteralString,
    iterate_query: LiteralString,
    action_query: LiteralString,
    label_map: dict[str, str],
    merge_labels_param: Literal["uc_labels", "pc_labels"],
    community_base_label: Literal["UserCommunity", "PostCommunity"],
    progress: HintGridProgress | None,
    console_status: str,
    progress_task_title: str,
    done_task_description: str,
    log_label: str,
) -> None:
    """Delete old BELONGS_TO, then merge community nodes and edges in batches."""
    neo4j.execute_labeled(delete_query, label_map)

    count_result = neo4j.execute_and_fetch_labeled(
        count_query,
        label_map,
    )
    total = (
        coerce_int(count_result[0].get("total", 0), 0) if count_result else 0
    )

    if total == 0:
        logger.info("No nodes with cluster_id for %s community structure, skipping", log_label)
        return

    labels_value = neo4j.labels_list(community_base_label)
    merge_key: Literal["uc_labels", "pc_labels"] = merge_labels_param
    iterate_params: dict[str, Neo4jParameter] = {merge_key: labels_value}

    operation_id = f"community_structure_{log_label}_{uuid.uuid4().hex[:8]}"

    def _run_iterate() -> None:
        result = neo4j.execute_periodic_iterate(
            iterate_query,
            action_query,
            label_map=label_map,
            batch_size=settings.apoc_batch_size,
            parallel=False,
            batch_mode="BATCH",
            progress_tracker_id=operation_id if progress is not None else None,
            params=iterate_params,
        )
        logger.info(
            "%s community structure: batches=%s total=%s committed=%s failed=%s",
            log_label,
            coerce_int(result.get("batches", 0)),
            coerce_int(result.get("total", 0)),
            coerce_int(result.get("committedOperations", 0)),
            coerce_int(result.get("failedOperations", 0)),
        )
        if coerce_int(result.get("failedOperations", 0)) > 0:
            logger.warning(
                "Some %s community batch operations failed: %s",
                log_label,
                result.get("errorMessages", []),
            )

    if progress is not None:
        neo4j.create_progress_tracker(operation_id, total)
        task_id = progress.add_task(progress_task_title, total=total)
        from hintgrid.cli.console import track_periodic_iterate_progress

        polling_thread = track_periodic_iterate_progress(
            neo4j,
            operation_id,
            progress,
            task_id,
            poll_interval=settings.progress_poll_interval_seconds,
        )
        try:
            _run_iterate()
        finally:
            polling_thread.stop_event.set()
            polling_thread.join(timeout=2.0)
            neo4j.cleanup_progress_tracker(operation_id)
            progress.update(task_id, description=done_task_description)
    else:
        with console.status(console_status):
            _run_iterate()


def create_user_community_structure(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    progress: HintGridProgress | None = None,
) -> None:
    """Create UserCommunity nodes and BELONGS_TO from User.cluster_id (batched)."""
    _run_periodic_community_structure(
        neo4j,
        settings,
        delete_query=_DELETE_USER_BELONGS,
        count_query=_COUNT_USERS_WITH_CLUSTER,
        iterate_query=_ITERATE_USER_ROWS,
        action_query=_ACTION_USER,
        label_map=USER_LABEL_MAP,
        merge_labels_param="uc_labels",
        community_base_label="UserCommunity",
        progress=progress,
        console_status="[bold blue]Building user communities...[/bold blue]",
        progress_task_title="[cyan]Building user communities...",
        done_task_description="[green]✓ User communities linked",
        log_label="user",
    )


def create_post_community_structure(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    progress: HintGridProgress | None = None,
) -> None:
    """Create PostCommunity nodes and BELONGS_TO from Post.cluster_id (batched)."""
    _run_periodic_community_structure(
        neo4j,
        settings,
        delete_query=_DELETE_POST_BELONGS,
        count_query=_COUNT_POSTS_WITH_CLUSTER,
        iterate_query=_ITERATE_POST_ROWS,
        action_query=_ACTION_POST,
        label_map=POST_LABEL_MAP,
        merge_labels_param="pc_labels",
        community_base_label="PostCommunity",
        progress=progress,
        console_status="[bold blue]Building post communities...[/bold blue]",
        progress_task_title="[cyan]Building post communities...",
        done_task_description="[green]✓ Post communities linked",
        log_label="post",
    )
