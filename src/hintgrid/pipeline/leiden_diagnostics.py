"""Pre/post metrics for Neo4j GDS Leiden tuning (optional, gated by settings)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Literal, LiteralString, TypedDict

from hintgrid.utils.coercion import coerce_float, coerce_int

if TYPE_CHECKING:
    from logging import Logger

    from hintgrid.clients.neo4j import Neo4jClient, Neo4jValue
    from hintgrid.config import HintGridSettings


class WeightedGraphStats(TypedDict, total=False):
    """Aggregates for a weighted directed graph used for Leiden projection."""

    rel_count: int
    weight_sum: float
    weight_min: float
    weight_max: float
    weight_avg: float
    weight_p50: float
    weight_p90: float
    weight_p99: float
    node_count: int
    out_deg_avg: float
    out_deg_p50: float
    out_deg_p90: float
    out_deg_p99: float
    out_deg_max: int
    isolated_nodes: int


_GRAPH_KIND = Literal["user_interaction", "post_similarity"]

# --- Cypher: fixed relationship types (no dynamic injection) ---
_EDGE_STATS_INTERACTS: LiteralString = (
    "MATCH (u:__user__)-[r:INTERACTS_WITH]->(v:__user__) "
    "RETURN count(r) AS rel_count, "
    "sum(r.weight) AS weight_sum, "
    "min(r.weight) AS weight_min, "
    "max(r.weight) AS weight_max, "
    "avg(r.weight) AS weight_avg, "
    "percentileCont(r.weight, 0.5) AS weight_p50, "
    "percentileCont(r.weight, 0.9) AS weight_p90, "
    "percentileCont(r.weight, 0.99) AS weight_p99"
)

_EDGE_STATS_SIMILAR: LiteralString = (
    "MATCH (p:__post__)-[r:SIMILAR_TO]->(q:__post__) "
    "RETURN count(r) AS rel_count, "
    "sum(r.weight) AS weight_sum, "
    "min(r.weight) AS weight_min, "
    "max(r.weight) AS weight_max, "
    "avg(r.weight) AS weight_avg, "
    "percentileCont(r.weight, 0.5) AS weight_p50, "
    "percentileCont(r.weight, 0.9) AS weight_p90, "
    "percentileCont(r.weight, 0.99) AS weight_p99"
)

_DEG_STATS_USER: LiteralString = (
    "MATCH (u:__user__) "
    "WITH u, COUNT { (u)-[:INTERACTS_WITH]->(v:__user__) } AS out_deg "
    "RETURN count(u) AS node_count, "
    "avg(out_deg) AS out_deg_avg, "
    "percentileCont(out_deg, 0.5) AS out_deg_p50, "
    "percentileCont(out_deg, 0.9) AS out_deg_p90, "
    "percentileCont(out_deg, 0.99) AS out_deg_p99, "
    "max(out_deg) AS out_deg_max, "
    "sum(CASE WHEN out_deg = 0 THEN 1 ELSE 0 END) AS isolated_nodes"
)

_DEG_STATS_POST: LiteralString = (
    "MATCH (p:__post__) "
    "WITH p, COUNT { (p)-[:SIMILAR_TO]->(q:__post__) } AS out_deg "
    "RETURN count(p) AS node_count, "
    "avg(out_deg) AS out_deg_avg, "
    "percentileCont(out_deg, 0.5) AS out_deg_p50, "
    "percentileCont(out_deg, 0.9) AS out_deg_p90, "
    "percentileCont(out_deg, 0.99) AS out_deg_p99, "
    "max(out_deg) AS out_deg_max, "
    "sum(CASE WHEN out_deg = 0 THEN 1 ELSE 0 END) AS isolated_nodes"
)


def format_effective_gamma_hint(leiden_resolution: float, weight_sum: float) -> str:
    """Explain GDS normalization: gamma is divided by sum of relationship weights (weighted graphs).

    This is a documentation hint for operators; it does not replicate internal GDS scaling exactly.
    """
    if weight_sum <= 0.0:
        return "effective_gamma_ratio undefined (weight_sum is zero)"
    ratio = leiden_resolution / weight_sum
    return (
        f"leiden_resolution/weight_sum={ratio:.6e} "
        f"(GDS divides gamma by total relationship weight; use this to compare runs when weights change)"
    )


def _coerce_optional_float(value: Neo4jValue | None, default: float = 0.0) -> float:
    if value is None:
        return default
    return coerce_float(value, default)


def _coerce_optional_int(value: Neo4jValue | None, default: int = 0) -> int:
    if value is None:
        return default
    return coerce_int(value)


def _merge_edge_row(target: WeightedGraphStats, row: dict[str, Neo4jValue]) -> None:
    target["rel_count"] = _coerce_optional_int(row.get("rel_count"), 0)
    target["weight_sum"] = _coerce_optional_float(row.get("weight_sum"), 0.0)
    target["weight_min"] = _coerce_optional_float(row.get("weight_min"), 0.0)
    target["weight_max"] = _coerce_optional_float(row.get("weight_max"), 0.0)
    target["weight_avg"] = _coerce_optional_float(row.get("weight_avg"), 0.0)
    target["weight_p50"] = _coerce_optional_float(row.get("weight_p50"), 0.0)
    target["weight_p90"] = _coerce_optional_float(row.get("weight_p90"), 0.0)
    target["weight_p99"] = _coerce_optional_float(row.get("weight_p99"), 0.0)


def _merge_degree_row(target: WeightedGraphStats, row: dict[str, Neo4jValue]) -> None:
    target["node_count"] = _coerce_optional_int(row.get("node_count"), 0)
    target["out_deg_avg"] = _coerce_optional_float(row.get("out_deg_avg"), 0.0)
    target["out_deg_p50"] = _coerce_optional_float(row.get("out_deg_p50"), 0.0)
    target["out_deg_p90"] = _coerce_optional_float(row.get("out_deg_p90"), 0.0)
    target["out_deg_p99"] = _coerce_optional_float(row.get("out_deg_p99"), 0.0)
    target["out_deg_max"] = _coerce_optional_int(row.get("out_deg_max"), 0)
    target["isolated_nodes"] = _coerce_optional_int(row.get("isolated_nodes"), 0)


def collect_user_interaction_graph_stats(neo4j: Neo4jClient) -> WeightedGraphStats:
    """Aggregate INTERACTS_WITH weights and User out-degree percentiles."""
    stats: WeightedGraphStats = {}
    edge_rows = neo4j.execute_and_fetch_labeled(
        _EDGE_STATS_INTERACTS,
        {"user": "User"},
    )
    if edge_rows:
        _merge_edge_row(stats, edge_rows[0])
    deg_rows = neo4j.execute_and_fetch_labeled(_DEG_STATS_USER, {"user": "User"})
    if deg_rows:
        _merge_degree_row(stats, deg_rows[0])
    return stats


def collect_post_similarity_graph_stats(neo4j: Neo4jClient) -> WeightedGraphStats:
    """Aggregate SIMILAR_TO weights and Post out-degree percentiles."""
    stats: WeightedGraphStats = {}
    edge_rows = neo4j.execute_and_fetch_labeled(
        _EDGE_STATS_SIMILAR,
        {"post": "Post"},
    )
    if edge_rows:
        _merge_edge_row(stats, edge_rows[0])
    deg_rows = neo4j.execute_and_fetch_labeled(_DEG_STATS_POST, {"post": "Post"})
    if deg_rows:
        _merge_degree_row(stats, deg_rows[0])
    return stats


def _json_safe(value: Neo4jValue) -> object:
    """Convert Neo4j driver values to JSON-serializable Python objects."""
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return str(value)


def serialize_leiden_write_row(leiden_row: dict[str, Neo4jValue]) -> dict[str, object]:
    """Normalize gds.leiden.write result row for logging (compact + JSON-safe)."""
    out: dict[str, object] = {}
    for key in (
        "nodePropertiesWritten",
        "communityCount",
        "modularity",
        "ranLevels",
        "didConverge",
        "nodeCount",
    ):
        if key not in leiden_row:
            continue
        raw = leiden_row[key]
        if key in ("nodePropertiesWritten", "communityCount", "ranLevels", "nodeCount"):
            out[key] = _coerce_optional_int(raw, 0)
        elif key == "didConverge":
            out[key] = bool(raw) if raw is not None else False
        else:
            out[key] = _coerce_optional_float(raw, 0.0)

    if "modularities" in leiden_row and leiden_row["modularities"] is not None:
        mod_raw = leiden_row["modularities"]
        if isinstance(mod_raw, (list, tuple)):
            out["modularities"] = [_coerce_optional_float(m, 0.0) for m in mod_raw]
        else:
            out["modularities"] = _json_safe(mod_raw)

    if "communityDistribution" in leiden_row and leiden_row["communityDistribution"] is not None:
        cd_raw = leiden_row["communityDistribution"]
        if isinstance(cd_raw, dict):
            out["communityDistribution"] = {
                str(k): _coerce_optional_float(v, 0.0) for k, v in cd_raw.items()
            }
        else:
            out["communityDistribution"] = _json_safe(cd_raw)

    return out


def log_leiden_clustering_diagnostics(
    logger: Logger,
    *,
    graph_kind: _GRAPH_KIND,
    settings: HintGridSettings,
    pre_stats: WeightedGraphStats | None,
    leiden_row: dict[str, Neo4jValue] | None,
) -> None:
    """Emit INFO diagnostics; full communityDistribution at DEBUG."""
    gamma_hint = ""
    if pre_stats and "weight_sum" in pre_stats:
        gamma_hint = format_effective_gamma_hint(
            settings.leiden_resolution,
            float(pre_stats["weight_sum"]),
        )

    payload_pre: dict[str, object] = dict(pre_stats) if pre_stats else {}
    serialized = serialize_leiden_write_row(leiden_row) if leiden_row else {}

    summary = {
        "graph_kind": graph_kind,
        "leiden_resolution": settings.leiden_resolution,
        "leiden_max_levels": settings.leiden_max_levels,
        "gamma_hint": gamma_hint,
        "graph_pre_leiden": payload_pre,
        "leiden_write": serialized,
    }
    logger.info("Leiden diagnostics: %s", json.dumps(summary, default=str))

    if leiden_row and "communityDistribution" in leiden_row:
        cd_full = _json_safe(leiden_row["communityDistribution"])
        logger.debug("Leiden communityDistribution (full): %s", json.dumps(cd_full, default=str))


def leiden_write_yield_clause(*, extended: bool) -> LiteralString:
    """Cypher fragment: YIELD/RETURN for gds.leiden.write."""
    if not extended:
        return (
            "YIELD nodePropertiesWritten, communityCount, modularity "
            "RETURN nodePropertiesWritten, communityCount, modularity"
        )
    return (
        "YIELD nodePropertiesWritten, communityCount, modularity, ranLevels, didConverge, "
        "nodeCount, modularities, communityDistribution "
        "RETURN nodePropertiesWritten, communityCount, modularity, ranLevels, didConverge, "
        "nodeCount, modularities, communityDistribution"
    )
