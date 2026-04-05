"""Community interest rebuilding and cleanup."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC
from typing import TYPE_CHECKING, LiteralString, cast

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient, Neo4jParameter
    from hintgrid.cli.progress_display import HintGridProgress
    from hintgrid.config import HintGridSettings
    from rich.progress import TaskID

from hintgrid.pipeline import community_similarity as _community_similarity_module
from hintgrid.pipeline.interests_queries import (
    INTEREST_LABELS,
    CommunityId,
    build_dirty_uc_query,
    build_interest_params,
    build_interests_count_query,
    build_interests_iterate_query,
    build_max_weights_query,
    bulk_set_max_weight_temp,
)
from hintgrid.utils.coercion import coerce_float, coerce_int

logger = logging.getLogger(__name__)

compute_community_similarity = _community_similarity_module.compute_community_similarity


def rebuild_interests(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    progress: HintGridProgress | None = None,
) -> None:
    """Rebuild INTERESTED_IN relationships between communities based on interactions."""
    logger.info("Rebuilding INTERESTED_IN relationships")

    neo4j.execute_labeled(
        "MATCH (uc:__uc__)-[i:INTERESTED_IN]->(pc:__pc__) DELETE i",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )

    rel_types = neo4j.get_existing_rel_types()
    params = build_interest_params(settings)

    # --- max_weight per UserCommunity (needed for normalization) ---------
    max_weights_query = build_max_weights_query(
        ctr_enabled=settings.ctr_enabled,
        rel_types=rel_types,
    )
    max_weights_result = neo4j.execute_and_fetch_labeled(
        max_weights_query,
        INTEREST_LABELS,
        params,
    )

    max_weight_map: dict[CommunityId, float] = {}
    for row in max_weights_result:
        raw_uc_id = row.get("uc_id")
        if raw_uc_id is None:
            continue
        uc_id: CommunityId = (
            raw_uc_id if isinstance(raw_uc_id, (str, int)) else coerce_int(raw_uc_id)
        )
        max_weight = coerce_float(row.get("max_weight"))
        if max_weight > 0.0:
            max_weight_map[uc_id] = max_weight

    # --- Create INTERESTED_IN via apoc.periodic.iterate -----------------
    iterate_query = build_interests_iterate_query(
        ctr_enabled=settings.ctr_enabled,
        rel_types=rel_types,
    )

    action_query: LiteralString = (
        "UNWIND $_batch AS row "
        "MATCH (uc:__uc__) WHERE uc.id = row.uc_id "
        "MATCH (pc:__pc__) WHERE pc.id = row.pc_id "
        "WITH uc, pc, row.weight AS weight, row.interactions AS interactions, "
        "     uc.max_weight_temp AS max_weight "
        "MERGE (uc)-[i:INTERESTED_IN]->(pc) "
        "SET i.score = toFloat(weight) / max_weight, "
        "    i.based_on = interactions, "
        "    i.last_updated = datetime(), "
        "    i.expires_at = datetime() + duration({days: $ttl_days})"
    )

    bulk_set_max_weight_temp(neo4j, max_weight_map)

    count_query = build_interests_count_query(
        ctr_enabled=settings.ctr_enabled,
        rel_types=rel_types,
    )
    count_result = neo4j.execute_and_fetch_labeled(
        count_query,
        INTEREST_LABELS,
        params,
    )
    total = coerce_int(count_result[0].get("total", 0), 0) if count_result else None

    operation_id = f"rebuild_interests_{uuid.uuid4().hex[:8]}"
    polling_thread = None
    task_id: TaskID | None = None

    if progress is not None:
        neo4j.create_progress_tracker(operation_id, total)
        task_id = progress.add_task(
            "[cyan]Rebuilding interests...",
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
        iterate_params: dict[str, Neo4jParameter] = {
            **params,
            "ttl_days": settings.interests_ttl_days,
        }
        result = neo4j.execute_periodic_iterate(
            iterate_query,
            action_query,
            label_map=INTEREST_LABELS,
            batch_size=settings.apoc_batch_size,
            parallel=False,
            batch_mode="BATCH",
            progress_tracker_id=operation_id if progress is not None else None,
            params=iterate_params,
        )
        logger.info(
            "INTERESTED_IN rebuild: %d batches, %d total, %d committed, %d failed",
            coerce_int(result.get("batches", 0)),
            coerce_int(result.get("total", 0)),
            coerce_int(result.get("committedOperations", 0)),
            coerce_int(result.get("failedOperations", 0)),
        )
        if coerce_int(result.get("failedOperations", 0)) > 0:
            logger.warning(
                "Some operations failed: %s",
                result.get("errorMessages", []),
            )
    finally:
        if polling_thread is not None:
            polling_thread.stop_event.set()
            polling_thread.join(timeout=2.0)

        if progress is not None:
            neo4j.cleanup_progress_tracker(operation_id)
            if task_id is not None:
                progress.update(task_id, description="[green]✓ Interests rebuilt")

        neo4j.execute_labeled(
            "MATCH (uc:__uc__) REMOVE uc.max_weight_temp",
            {"uc": "UserCommunity"},
        )

    logger.info("INTERESTED_IN relationships rebuilt")


def seed_serendipity(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Seed serendipity INTERESTED_IN relationships for discovery.

    If community similarity is enabled, uses SIMILAR_COMMUNITY relationships
    to recommend content from similar communities. Otherwise, uses random
    selection.
    """
    logger.info("Seeding serendipity relationships")

    if settings.community_similarity_enabled:
        neo4j.execute_labeled(
            "MATCH (uc1:__uc__)-[sim:SIMILAR_COMMUNITY]->(uc2:__uc__) "
            "MATCH (uc2)-[i:INTERESTED_IN]->(pc:__pc__) "
            "WHERE NOT (uc1)-[:INTERESTED_IN]->(pc) "
            "WITH uc1, pc, sim.score * i.score AS combined_score "
            "ORDER BY combined_score DESC "
            "LIMIT $serendipity_limit "
            "MERGE (uc1)-[s:INTERESTED_IN]->(pc) "
            "SET s.score = $serendipity_score * combined_score, "
            "    s.based_on = $serendipity_based_on, "
            "    s.serendipity = true, "
            "    s.last_updated = datetime(), "
            "    s.expires_at = datetime() + duration({days: $ttl_days})",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
            {
                "ttl_days": settings.interests_ttl_days,
                "serendipity_limit": settings.serendipity_limit,
                "serendipity_score": settings.serendipity_score,
                "serendipity_based_on": settings.serendipity_based_on,
            },
        )
    else:
        neo4j.execute_labeled(
            "MATCH (uc:__uc__), (pc:__pc__) "
            "WHERE NOT (uc)-[:INTERESTED_IN]->(pc) AND rand() < $probability "
            "MATCH (uc)-[:INTERESTED_IN]->(pc2:__pc__) "
            "WITH uc, pc LIMIT $serendipity_limit "
            "MERGE (uc)-[i:INTERESTED_IN]->(pc) "
            "SET i.score = $serendipity_score, "
            "    i.based_on = $serendipity_based_on, "
            "    i.serendipity = true, "
            "    i.last_updated = datetime(), "
            "    i.expires_at = datetime() + duration({days: $ttl_days})",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
            {
                "probability": settings.serendipity_probability,
                "ttl_days": settings.interests_ttl_days,
                "serendipity_limit": settings.serendipity_limit,
                "serendipity_score": settings.serendipity_score,
                "serendipity_based_on": settings.serendipity_based_on,
            },
        )

    logger.info("Serendipity relationships seeded")


def refresh_interests(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
    last_rebuild_at: str,
    progress: HintGridProgress | None = None,
) -> None:
    """Incremental refresh: apply global decay and recompute dirty communities.

    Instead of full DELETE + rebuild, this:
    1. Applies global exponential decay to all existing INTERESTED_IN scores
    2. Finds UserCommunities with new interactions since last_rebuild_at
    3. Deletes and recomputes INTERESTED_IN only for dirty UCs
    4. Removes INTERESTED_IN edges where score dropped near zero
    """
    from datetime import datetime

    now = datetime.now(UTC)
    last_dt = datetime.fromisoformat(last_rebuild_at)
    hours_since_last = (now - last_dt).total_seconds() / 3600.0

    logger.info(
        "Refreshing interests (%.1f hours since last rebuild, half_life=%d days)",
        hours_since_last,
        settings.decay_half_life_days,
    )

    rel_types = neo4j.get_existing_rel_types()

    neo4j.execute_labeled(
        "MATCH (uc:__uc__)-[i:INTERESTED_IN]->(pc:__pc__) "
        "SET i.score = i.score * exp(-0.693147 * $hours_since_last "
        "/ (toFloat($half_life_days) * 24.0))",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
        {
            "hours_since_last": hours_since_last,
            "half_life_days": settings.decay_half_life_days,
        },
    )
    logger.info("Applied global decay to existing interests")

    dirty_query = build_dirty_uc_query(rel_types)
    dirty_result = list(
        neo4j.execute_and_fetch_labeled(
            dirty_query,
            {"user": "User", "uc": "UserCommunity"},
            {"last_rebuild_at": last_rebuild_at},
        )
    )

    dirty_uc_ids: list[CommunityId] = []
    for row in dirty_result:
        raw = row.get("uc_id")
        if raw is None:
            continue
        if isinstance(raw, (str, int)):
            dirty_uc_ids.append(raw)
        else:
            dirty_uc_ids.append(coerce_int(raw))

    if not dirty_uc_ids:
        logger.info("No dirty UserCommunities found, skipping recompute")
    else:
        logger.info(
            "Found %d dirty UserCommunities, recomputing interests",
            len(dirty_uc_ids),
        )

        neo4j.execute_labeled(
            "MATCH (uc:__uc__)-[i:INTERESTED_IN]->(pc:__pc__) "
            "WHERE uc.id IN $dirty_uc_ids "
            "DELETE i",
            {"uc": "UserCommunity", "pc": "PostCommunity"},
            {"dirty_uc_ids": cast("Neo4jParameter", dirty_uc_ids)},
        )

        query_params: dict[str, Neo4jParameter] = {
            **build_interest_params(settings),
            "dirty_uc_ids": cast("Neo4jParameter", dirty_uc_ids),
        }

        max_weights_query = build_max_weights_query(
            ctr_enabled=settings.ctr_enabled,
            has_dirty_filter=True,
            rel_types=rel_types,
        )
        max_weights_result = neo4j.execute_and_fetch_labeled(
            max_weights_query,
            INTEREST_LABELS,
            query_params,
        )

        max_weight_map: dict[CommunityId, float] = {}
        for row in max_weights_result:
            raw_uc_id = row.get("uc_id")
            if raw_uc_id is None:
                continue
            uc_id_val: CommunityId = (
                raw_uc_id if isinstance(raw_uc_id, (str, int)) else coerce_int(raw_uc_id)
            )
            max_weight = coerce_float(row.get("max_weight"))
            if max_weight > 0.0:
                max_weight_map[uc_id_val] = max_weight

        bulk_set_max_weight_temp(neo4j, max_weight_map)

        iterate_query = build_interests_iterate_query(
            ctr_enabled=settings.ctr_enabled,
            has_dirty_filter=True,
            rel_types=rel_types,
        )

        action_query: LiteralString = (
            "UNWIND $_batch AS row "
            "MATCH (uc:__uc__) WHERE uc.id = row.uc_id "
            "MATCH (pc:__pc__) WHERE pc.id = row.pc_id "
            "WITH uc, pc, row.weight AS weight, "
            "     row.interactions AS interactions, "
            "     uc.max_weight_temp AS max_weight "
            "MERGE (uc)-[i:INTERESTED_IN]->(pc) "
            "SET i.score = toFloat(weight) / max_weight, "
            "    i.based_on = interactions, "
            "    i.last_updated = datetime(), "
            "    i.expires_at = datetime() + duration({days: $ttl_days})"
        )

        count_query = build_interests_count_query(
            ctr_enabled=settings.ctr_enabled,
            has_dirty_filter=True,
            rel_types=rel_types,
        )
        count_result = neo4j.execute_and_fetch_labeled(
            count_query,
            INTEREST_LABELS,
            query_params,
        )
        total = coerce_int(count_result[0].get("total", 0), 0) if count_result else None

        operation_id = f"refresh_interests_{uuid.uuid4().hex[:8]}"
        polling_thread = None
        task_id: TaskID | None = None

        if progress is not None:
            neo4j.create_progress_tracker(operation_id, total)
            task_id = progress.add_task(
                "[cyan]Refreshing interests...",
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
            iterate_params: dict[str, Neo4jParameter] = {
                **query_params,
                "ttl_days": settings.interests_ttl_days,
            }
            result = neo4j.execute_periodic_iterate(
                iterate_query,
                action_query,
                label_map=INTEREST_LABELS,
                batch_size=settings.apoc_batch_size,
                parallel=False,
                batch_mode="BATCH",
                progress_tracker_id=(operation_id if progress is not None else None),
                params=iterate_params,
            )
            logger.info(
                "INTERESTED_IN refresh: %d batches, %d total, %d committed, %d failed",
                coerce_int(result.get("batches", 0)),
                coerce_int(result.get("total", 0)),
                coerce_int(result.get("committedOperations", 0)),
                coerce_int(result.get("failedOperations", 0)),
            )
            if coerce_int(result.get("failedOperations", 0)) > 0:
                logger.warning(
                    "Some operations failed: %s",
                    result.get("errorMessages", []),
                )
        finally:
            if polling_thread is not None:
                polling_thread.stop_event.set()
                polling_thread.join(timeout=2.0)

            if progress is not None:
                neo4j.cleanup_progress_tracker(operation_id)
                if task_id is not None:
                    progress.update(
                        task_id,
                        description="[green]✓ Interests refreshed",
                    )

            neo4j.execute_labeled(
                "MATCH (uc:__uc__) WHERE uc.id IN $dirty_uc_ids REMOVE uc.max_weight_temp",
                {"uc": "UserCommunity"},
                {"dirty_uc_ids": cast("Neo4jParameter", dirty_uc_ids)},
            )

    neo4j.execute_labeled(
        "MATCH (uc:__uc__)-[i:INTERESTED_IN]->(pc:__pc__) WHERE i.score < 0.01 DELETE i",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )

    logger.info("Interest refresh complete")


def cleanup_expired_interests(neo4j: Neo4jClient) -> None:
    """Delete expired INTERESTED_IN relationships (TTL cleanup)."""
    logger.info("Cleaning up expired INTERESTED_IN relationships")

    neo4j.execute_labeled(
        "MATCH (uc:__uc__)-[i:INTERESTED_IN]->(pc:__pc__) WHERE i.expires_at < datetime() DELETE i",
        {"uc": "UserCommunity", "pc": "PostCommunity"},
    )

    logger.info("Expired interests cleaned up")
