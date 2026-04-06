"""GDS nodeSimilarity between UserCommunity nodes (SIMILAR_COMMUNITY)."""

from __future__ import annotations

import contextlib
import logging
from typing import TYPE_CHECKING, LiteralString

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.config import HintGridSettings

from hintgrid.pipeline.clustering import validate_gds_name
from hintgrid.utils.coercion import coerce_int

logger = logging.getLogger(__name__)

_PROJECTION_NODE_COUNT: LiteralString = (
    "RETURN count { MATCH (u:__user__) } + "
    "count { MATCH (uc:__uc__) WHERE uc.id <> $noise } AS total"
)


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

    validate_gds_name(similarity_graph_name)
    noise = settings.noise_community_id
    projection_rows = neo4j.execute_and_fetch_labeled(
        _PROJECTION_NODE_COUNT,
        {"user": "User", "uc": "UserCommunity"},
        {"noise": noise},
    )
    projection_total = coerce_int(projection_rows[0].get("total", 0)) if projection_rows else 0
    if projection_total == 0:
        logger.info(
            "No users or non-noise UserCommunity nodes; skipping community similarity "
            "(GDS projection would be empty)"
        )
        return

    with contextlib.suppress(Exception):
        neo4j.execute(
            "CALL gds.graph.drop($graph_name) YIELD graphName",
            {"graph_name": similarity_graph_name},
        )

    user_lab = neo4j.label("User")
    uc_lab = neo4j.label("UserCommunity")
    node_query = (
        f"MATCH (u:{user_lab}) RETURN id(u) AS id "
        f"UNION MATCH (uc:{uc_lab}) WHERE uc.id <> {noise} RETURN id(uc) AS id"
    )
    # Project UC -> User so the bipartite "first node set" is UserCommunity.
    # GDS Node Similarity compares nodes with outgoing edges and writes pairs from
    # that set; User->UC would compare Users instead of communities.
    rel_query = (
        f"MATCH (u:{user_lab})-[:BELONGS_TO]->(uc:{uc_lab}) WHERE uc.id <> {noise} "
        f"RETURN id(uc) AS source, id(u) AS target"
    )
    neo4j.execute(
        "CALL gds.graph.project.cypher($graph_name, $node_query, $rel_query)",
        {
            "graph_name": similarity_graph_name,
            "node_query": node_query,
            "rel_query": rel_query,
        },
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
