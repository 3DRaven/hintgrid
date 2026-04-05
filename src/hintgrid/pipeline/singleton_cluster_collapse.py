"""Collapse Leiden singleton communities (cluster size 1) into one noise cluster_id."""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, LiteralString

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient, Neo4jParameter
    from hintgrid.cli.progress_display import HintGridProgress
    from hintgrid.config import HintGridSettings

from hintgrid.cli.console import console
from hintgrid.utils.coercion import coerce_int

logger = logging.getLogger(__name__)

_COUNT_POST_SINGLETON_CLUSTERS: LiteralString = (
    "MATCH (p:__entity__) WHERE p.cluster_id IS NOT NULL "
    "WITH p.cluster_id AS cid, count(*) AS n "
    "WHERE n = 1 "
    "RETURN count(*) AS total"
)
_ITERATE_POST_SINGLETON_NODES: LiteralString = (
    "MATCH (p:__entity__) WHERE p.cluster_id IS NOT NULL "
    "WITH p.cluster_id AS cid, count(*) AS n "
    "WHERE n = 1 "
    "MATCH (p2:__entity__) WHERE p2.cluster_id = cid "
    "RETURN id(p2) AS node_id"
)
_ACTION_SET_CLUSTER: LiteralString = (
    "UNWIND $_batch AS row "
    "MATCH (p:__entity__) WHERE id(p) = row.node_id "
    "SET p.cluster_id = $noise_community_id"
)


def _collapse_singleton_entity_clusters(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    *,
    label_map: dict[str, str],
    progress: HintGridProgress | None,
    console_status: str,
    progress_task_title: str,
    done_task_description: str,
    log_label: str,
) -> None:
    """Set cluster_id to noise_community_id for every node in a size-1 Leiden cluster."""
    if not settings.singleton_collapse_enabled:
        logger.info("Singleton collapse disabled, skipping %s", log_label)
        return

    count_result = neo4j.execute_and_fetch_labeled(
        _COUNT_POST_SINGLETON_CLUSTERS,
        label_map,
    )
    total = (
        coerce_int(count_result[0].get("total", 0), 0) if count_result else 0
    )

    if total == 0:
        logger.info("No singleton %s clusters to collapse", log_label)
        return

    logger.info(
        "Collapsing %d singleton %s clusters to noise_community_id=%s",
        total,
        log_label,
        settings.noise_community_id,
    )

    params: dict[str, Neo4jParameter] = {
        "noise_community_id": settings.noise_community_id,
    }

    operation_id = f"singleton_collapse_{log_label}_{uuid.uuid4().hex[:8]}"

    def _run_iterate() -> None:
        result = neo4j.execute_periodic_iterate(
            _ITERATE_POST_SINGLETON_NODES,
            _ACTION_SET_CLUSTER,
            label_map=label_map,
            batch_size=settings.apoc_batch_size,
            parallel=False,
            batch_mode="BATCH",
            progress_tracker_id=operation_id if progress is not None else None,
            params=params,
        )
        logger.info(
            "%s singleton collapse: batches=%s total=%s committed=%s failed=%s",
            log_label,
            coerce_int(result.get("batches", 0)),
            coerce_int(result.get("total", 0)),
            coerce_int(result.get("committedOperations", 0)),
            coerce_int(result.get("failedOperations", 0)),
        )
        if coerce_int(result.get("failedOperations", 0)) > 0:
            logger.warning(
                "Some %s singleton collapse batch operations failed: %s",
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


def collapse_singleton_post_clusters(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    progress: HintGridProgress | None = None,
) -> None:
    """Assign noise_community_id to posts that are alone in their Leiden cluster."""
    project_label = neo4j.worker_label or "Post"
    label_map = {"entity": project_label}
    _collapse_singleton_entity_clusters(
        neo4j,
        settings,
        label_map=label_map,
        progress=progress,
        console_status="[bold blue]Collapsing singleton post clusters...[/bold blue]",
        progress_task_title="[cyan]Collapsing singleton post clusters...",
        done_task_description="[green]✓ Singleton post clusters collapsed",
        log_label="post",
    )


def collapse_singleton_user_clusters(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    progress: HintGridProgress | None = None,
) -> None:
    """Assign noise_community_id to users that are alone in their Leiden cluster."""
    project_label = neo4j.worker_label or "User"
    label_map = {"entity": project_label}
    _collapse_singleton_entity_clusters(
        neo4j,
        settings,
        label_map=label_map,
        progress=progress,
        console_status="[bold blue]Collapsing singleton user clusters...[/bold blue]",
        progress_task_title="[cyan]Collapsing singleton user clusters...",
        done_task_description="[green]✓ Singleton user clusters collapsed",
        log_label="user",
    )
