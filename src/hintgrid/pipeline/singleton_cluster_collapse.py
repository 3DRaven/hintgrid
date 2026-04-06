"""Collapse Leiden singleton communities (cluster size 1) into one noise cluster_id."""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, LiteralString, cast

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient, Neo4jParameter
    from hintgrid.cli.progress_display import HintGridProgress
    from hintgrid.config import HintGridSettings

from hintgrid.cli.console import console
from hintgrid.utils.coercion import coerce_int

logger = logging.getLogger(__name__)

_BULK_SET_SINGLETON_NO_BATCH: LiteralString = (
    "MATCH (n:__entity__) WHERE n.cluster_id IS NOT NULL "
    "WITH n.cluster_id AS cid, collect(n) AS nodes, count(*) AS cnt "
    "WHERE cnt = 1 "
    "UNWIND nodes AS singleton "
    "SET singleton.cluster_id = $noise_community_id "
    "RETURN count(singleton) AS updated"
)


def _safe_label_fragment(label: str) -> str:
    """Ensure interpolated label string is safe for Cypher (no injection)."""
    if not _SAFE_LABEL_RE.fullmatch(label):
        raise ValueError(f"Unsafe Neo4j label for collapse query: {label!r}")
    return label


_SAFE_LABEL_RE = re.compile(r"[A-Za-z0-9_:]+")


def _run_bulk_collapse_batched(
    neo4j: Neo4jClient,
    *,
    entity_base_label: str,
    batch: int,
    noise_community_id: int,
) -> None:
    """SET cluster_id in sub-batches (CALL/IN TRANSACTIONS) for large graphs."""
    entity_label = _safe_label_fragment(neo4j.label(entity_base_label))
    params: dict[str, Neo4jParameter] = {"noise_community_id": noise_community_id}
    cypher = (
        f"MATCH (n:{entity_label}) WHERE n.cluster_id IS NOT NULL "
        "WITH n.cluster_id AS cid, collect(n) AS nodes, count(*) AS cnt "
        "WHERE cnt = 1 "
        "UNWIND nodes AS singleton "
        "CALL (*) { "
        "SET singleton.cluster_id = $noise_community_id "
        "RETURN 1 AS batch_done "
        f"}} IN TRANSACTIONS OF {batch} ROWS "
        "RETURN 1 AS collapse_done"
    )
    neo4j.execute(cast("LiteralString", cypher), params)
    logger.info(
        "Singleton collapse (IN TRANSACTIONS OF %s rows): finished for label %s",
        batch,
        entity_label,
    )


def _collapse_singleton_entity_clusters(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    *,
    label_map: dict[str, str],
    progress: HintGridProgress | None,
    console_status: str,
    log_label: str,
) -> None:
    """Set cluster_id to noise_community_id for every node in a size-1 Leiden cluster."""
    if not settings.singleton_collapse_enabled:
        logger.info("Singleton collapse disabled, skipping %s", log_label)
        return

    base_label = label_map["entity"]
    params: dict[str, Neo4jParameter] = {
        "noise_community_id": settings.noise_community_id,
    }
    in_tx = settings.singleton_collapse_in_transactions_of

    def _run() -> None:
        if in_tx <= 0:
            # Single Cypher transaction: one aggregation + SET (no separate COUNT scan).
            rows = neo4j.execute_and_fetch_labeled(
                _BULK_SET_SINGLETON_NO_BATCH,
                label_map,
                params,
            )
            updated = (
                coerce_int(rows[0].get("updated", 0), 0) if rows else 0
            )
            if updated == 0:
                logger.info("No singleton %s clusters to collapse", log_label)
                return
            logger.info(
                "Singleton collapse (single transaction): collapsed %d singleton %s "
                "cluster(s) to noise_community_id=%s",
                updated,
                log_label,
                settings.noise_community_id,
            )
            return

        # Batched path: no separate COUNT — same aggregation runs inside the query below.
        logger.info(
            "Singleton collapse (batched): %s, noise_community_id=%s, "
            "IN TRANSACTIONS OF %s ROWS",
            log_label,
            settings.noise_community_id,
            in_tx,
        )
        _run_bulk_collapse_batched(
            neo4j,
            entity_base_label=base_label,
            batch=in_tx,
            noise_community_id=settings.noise_community_id,
        )

    if progress is not None:
        task_id = progress.add_task(
            f"[cyan]Collapsing singleton {log_label} clusters...",
            total=1,
        )
        try:
            _run()
        finally:
            progress.update(
                task_id,
                completed=1,
                description=f"[green]✓ Singleton {log_label} clusters collapsed",
            )
    else:
        with console.status(console_status):
            _run()


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
        log_label="user",
    )
