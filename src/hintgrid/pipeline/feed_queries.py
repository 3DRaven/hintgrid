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


def _rel_type_enabled(rel_types: frozenset[str] | None, name: str) -> bool:
    return rel_types is None or name in rel_types


def build_local_raw_expression(rel_types: frozenset[str] | None) -> LiteralString:
    """Weighted sum expression (no ``AS`` alias) for graph-local popularity."""
    terms: list[LiteralString] = []
    if _rel_type_enabled(rel_types, "FAVORITED"):
        terms.append(
            "toFloat($likes_weight) * toFloat(COUNT { (p)<-[:FAVORITED]-() })"
        )
    if _rel_type_enabled(rel_types, "REBLOGGED"):
        terms.append(
            "toFloat($reblogs_weight) * toFloat(COUNT { (p)<-[:REBLOGGED]-() })"
        )
    if _rel_type_enabled(rel_types, "REPLIED"):
        terms.append(
            "toFloat($replies_weight) * toFloat(COUNT { (p)<-[:REPLIED]-() })"
        )
    if _rel_type_enabled(rel_types, "BOOKMARKED"):
        terms.append(
            "toFloat($bookmark_weight) * toFloat(COUNT { (p)<-[:BOOKMARKED]-() })"
        )
    if not terms:
        return "0.0"
    joined: LiteralString = terms[0]
    for t in terms[1:]:
        joined = joined + " + " + t
    return "(" + joined + ")"


def build_local_raw_binding(rel_types: frozenset[str] | None) -> LiteralString:
    """``WITH`` fragment: ``... AS local_raw`` for single-row-per-post queries."""
    expr = build_local_raw_expression(rel_types)
    return "     " + expr + " AS local_raw"


def build_global_raw_expression() -> LiteralString:
    """Expression only (no alias) for global composite from Post properties."""
    return (
        "toFloat($gf) * toFloat(COALESCE(p.totalFavourites, 0)) + "
        "toFloat($gr) * toFloat(COALESCE(p.totalReblogs, 0)) + "
        "toFloat($grp) * toFloat(COALESCE(p.totalReplies, 0))"
    )


def build_global_raw_binding() -> LiteralString:
    """Linear composite of Post status counters (fediverse-scale, from merge_status_stats)."""
    inner = build_global_raw_expression()
    return "     (" + inner + ") AS global_raw"


def build_popularity_contrib_expr(settings: HintGridSettings) -> LiteralString:
    """Expression referencing ``local_raw`` and ``global_raw`` aliases (no ``AS`` alias)."""
    mode = settings.feed_popularity_mode
    if mode == "local":
        return "log10(local_raw + $popularity_smoothing)"
    if mode == "global":
        return "log10(global_raw + $global_popularity_smoothing)"
    # blended
    return (
        "toFloat($blend_local) * log10(local_raw + $popularity_smoothing) + "
        "toFloat($blend_global) * log10(global_raw + $global_popularity_smoothing)"
    )


def build_stats_popular_post_query(
    rel_types: frozenset[str] | None,
    settings: HintGridSettings,
) -> LiteralString:
    """Cypher for most popular post in a PostCommunity (same popularity model as feed)."""
    lr = build_local_raw_expression(rel_types)
    gr = build_global_raw_expression()
    pc = build_popularity_contrib_expr(settings)
    return (
        "MATCH (pc:__pc__ {id: $comm_id})<-[:BELONGS_TO]-(p:__post__) "
        "WITH p, "
        + lr
        + " AS local_raw, ("
        + gr
        + ") AS global_raw "
        "WITH p, local_raw, global_raw, ("
        + pc
        + ") AS popularity_contrib "
        "ORDER BY popularity_contrib DESC LIMIT 1 "
        "RETURN p.id AS post_id, p.text AS post_text, popularity_contrib, local_raw, global_raw"
    )


def build_stats_avg_popularity_query(
    rel_types: frozenset[str] | None,
    settings: HintGridSettings,
) -> LiteralString:
    """Average ``popularity_contrib`` over posts in a PostCommunity."""
    lr = build_local_raw_expression(rel_types)
    gr = build_global_raw_expression()
    pc = build_popularity_contrib_expr(settings)
    return (
        "MATCH (pc:__pc__ {id: $comm_id})<-[:BELONGS_TO]-(p:__post__) "
        "WITH p, "
        + lr
        + " AS local_raw, ("
        + gr
        + ") AS global_raw "
        "WITH p, local_raw, global_raw, ("
        + pc
        + ") AS popularity_contrib "
        "RETURN avg(popularity_contrib) AS avg_popularity"
    )


def popularity_score_params(settings: HintGridSettings) -> dict[str, float]:
    """Parameters for local/global/blended popularity fragments and composite weights."""
    return {
        "likes_weight": settings.likes_weight,
        "reblogs_weight": settings.reblogs_weight,
        "replies_weight": settings.replies_weight,
        "bookmark_weight": settings.bookmark_weight,
        "gf": settings.global_popularity_favourites_weight,
        "gr": settings.global_popularity_reblogs_weight,
        "grp": settings.global_popularity_replies_weight,
        "blend_local": settings.feed_popularity_blend_local,
        "blend_global": settings.feed_popularity_blend_global,
        "popularity_smoothing": settings.popularity_smoothing,
        "global_popularity_smoothing": settings.global_popularity_smoothing,
    }


