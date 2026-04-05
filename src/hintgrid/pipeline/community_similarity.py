"""GDS nodeSimilarity between UserCommunity nodes (SIMILAR_COMMUNITY)."""

from __future__ import annotations

import contextlib
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings

from hintgrid.pipeline.clustering import validate_gds_name
from hintgrid.utils.coercion import coerce_int

logger = logging.getLogger(__name__)


def compute_community_similarity(
    neo4j: Neo4jClient,
    settings: HintGridSettings,
) -> None:
    """Compute similarity between UserCommunities using gds.nodeSimilarity.

    Creates SIMILAR_COMMUNITY relationships between UserCommunity nodes
    based on shared members (Jaccard similarity).
    """
    if not settings.community_similarity_enabled:
        logger.info("Community similarity disabled, skipping")
        return

    logger.info("Computing community similarity...")

    base_name = "uc-similarity"
    similarity_graph_name = f"{neo4j.worker_label}-{base_name}" if neo4j.worker_label else base_name

    with contextlib.suppress(Exception):
        neo4j.execute(
            "CALL gds.graph.drop($graph_name) YIELD graphName",
            {"graph_name": similarity_graph_name},
        )

    project_labels = [neo4j.worker_label] if neo4j.worker_label else ["User", "UserCommunity"]

    validate_gds_name(similarity_graph_name)
    neo4j.execute_labeled(
        "CALL gds.graph.project("
        "  '__graph_name__', $node_labels, "
        "  {BELONGS_TO: {orientation: 'UNDIRECTED'}}"
        ")",
        ident_map={"graph_name": similarity_graph_name},
        params={"node_labels": project_labels},
    )

    result = neo4j.execute_and_fetch_labeled(
        "CALL gds.nodeSimilarity.write('__graph_name__', {"
        "  writeRelationshipType: 'SIMILAR_COMMUNITY',"
        "  writeProperty: 'score',"
        "  topK: $top_k,"
        "  similarityCutoff: 0.0"
        "}) "
        "YIELD nodesCompared, relationshipsWritten "
        "RETURN nodesCompared, relationshipsWritten",
        ident_map={"graph_name": similarity_graph_name},
        params={"top_k": settings.community_similarity_top_k},
    )
    if result:
        compared = coerce_int(result[0].get("nodesCompared", 0))
        written = coerce_int(result[0].get("relationshipsWritten", 0))
        logger.info(
            "Community similarity: %d communities compared, %d relationships created",
            compared,
            written,
        )

    neo4j.execute(
        "CALL gds.graph.drop($graph_name) YIELD graphName",
        {"graph_name": similarity_graph_name},
    )

    logger.info("Community similarity computed")
