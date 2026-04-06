"""Cypher fragments for feed scoring (keeps feed.py small; avoids Neo4j property warnings)."""

from __future__ import annotations

from typing import LiteralString

from hintgrid.config import HintGridSettings


def pagerank_binding(settings: HintGridSettings) -> LiteralString:
    """WITH-clause binding for pagerank; omit p.pagerank when PageRank is disabled (avoids 01N52)."""
    if settings.pagerank_enabled:
        return "     COALESCE(p.pagerank, 0.0) AS pagerank"
    return "     0.0 AS pagerank"


def pagerank_score_weight_line(settings: HintGridSettings) -> LiteralString:
    """Adds pagerank term to score; empty when PageRank is off."""
    if settings.pagerank_enabled:
        return "     pagerank * $pagerank_weight + "
    return ""


def build_feed_filters(rel_types: frozenset[str] | None) -> LiteralString:
    """Build NOT EXISTS filter clauses for feed queries."""
    parts: LiteralString = ""
    if rel_types is None or "WAS_RECOMMENDED" in rel_types:
        parts = parts + "  AND NOT EXISTS { (u)-[:WAS_RECOMMENDED]->(p) } "
    parts = parts + "  AND NOT EXISTS { (u)-[:WROTE]->(p) } "
    if rel_types is None or "FAVORITED" in rel_types:
        parts = parts + "  AND NOT EXISTS { (u)-[:FAVORITED]->(p) } "
    if rel_types is None or "HATES_USER" in rel_types:
        parts = parts + "  AND NOT EXISTS { (p)<-[:WROTE]-(:__user__)<-[:HATES_USER]-(u) } "
    return parts


def language_match_score_case() -> LiteralString:
    """Cypher fragment for feed language boost (UI locale vs chosen_languages).

    Parameters: ``$language_match_weight``, ``$ui_language_match_weight``.
    Expects ``User`` as ``u`` and ``Post`` as ``p`` in scope.
    """
    return (
        "     CASE "
        "WHEN p.language IS NULL THEN $language_match_weight "
        "WHEN u.uiLanguage IS NULL AND (u.languages IS NULL OR size(coalesce(u.languages, [])) = 0) "
        "THEN $language_match_weight "
        "WHEN u.uiLanguage IS NOT NULL AND p.language = u.uiLanguage THEN $ui_language_match_weight "
        "WHEN u.languages IS NOT NULL AND p.language IN u.languages THEN $language_match_weight "
        "ELSE 0.0 END "
    )


def build_popularity_expr(rel_types: frozenset[str] | None) -> LiteralString:
    """Build popularity expression from FAVORITED count or zero fallback."""
    if rel_types is None or "FAVORITED" in rel_types:
        return "     COUNT { (p)<-[:FAVORITED]-() } AS popularity"
    return "     0 AS popularity"
