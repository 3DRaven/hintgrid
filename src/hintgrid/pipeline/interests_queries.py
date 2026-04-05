"""Cypher query builders for UserCommunity-PostCommunity interest scoring."""

from __future__ import annotations

from typing import TYPE_CHECKING, LiteralString

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient, Neo4jParameter
    from hintgrid.config import HintGridSettings

CommunityId = str | int

# Decay per relationship variable (must match branch MATCH alias)
_DECAY_F: LiteralString = (
    "exp(-0.693147 * duration.between(f.at, datetime()).days / toFloat($half_life_days))"
)
_DECAY_R: LiteralString = (
    "exp(-0.693147 * duration.between(r.at, datetime()).days / toFloat($half_life_days))"
)
_DECAY_RP: LiteralString = (
    "exp(-0.693147 * duration.between(rp.at, datetime()).days / toFloat($half_life_days))"
)
_DECAY_BK: LiteralString = (
    "exp(-0.693147 * duration.between(bk.at, datetime()).days / toFloat($half_life_days))"
)
_DECAY_REC: LiteralString = (
    "exp(-0.693147 * duration.between(rec.at, datetime()).days / toFloat($half_life_days))"
)

_RETURN_MAX_WEIGHTS: LiteralString = (
    "WITH uc, max(weight) AS max_weight RETURN uc.id AS uc_id, max_weight"
)
_RETURN_ITERATE: LiteralString = "RETURN uc.id AS uc_id, pc.id AS pc_id, weight, interactions"
_RETURN_COUNT: LiteralString = "RETURN count(*) AS total"

_INTEREST_MATCH_CORE: LiteralString = (
    "MATCH (u)-[:BELONGS_TO]->(uc:__uc__), (p)-[:BELONGS_TO]->(pc:__pc__) "
    "WHERE uc.id <> $noise_community_id AND pc.id <> $noise_community_id "
)


def _dirty_suffix(has_dirty_filter: bool) -> LiteralString:
    if has_dirty_filter:
        return " AND uc.id IN $dirty_uc_ids "
    return ""


def _empty_interests_query(return_clause: LiteralString, *, use_ctr: bool) -> LiteralString:
    """No interaction types in graph: yield no rows (same effect as impossible match)."""
    if use_ctr:
        return (
            "MATCH (uc:__uc__), (pc:__pc__) WHERE false "
            "WITH uc, pc, 0 AS likes, 0 AS reblogs, 0 AS replies, 0 AS bookmarks, "
            "0 AS recommendations "
        ) + _weight_suffix_from_aggregates(return_clause, use_ctr=True)
    return (
        "MATCH (uc:__uc__), (pc:__pc__) WHERE false "
        "WITH uc, pc, 0 AS likes, 0 AS reblogs, 0 AS replies, 0 AS bookmarks "
    ) + _weight_suffix_from_aggregates(return_clause, use_ctr=False)


def _weight_suffix_from_aggregates(
    return_clause: LiteralString,
    *,
    use_ctr: bool,
) -> LiteralString:
    """Apply weight formula from likes/reblogs/replies/bookmarks [, recommendations]."""
    if use_ctr:
        return (
            "WITH uc, pc, "
            + "     likes * $likes_weight + reblogs * $reblogs_weight "
            + "     + replies * $replies_weight + bookmarks * $bookmark_weight "
            + "     AS base_weight, "
            + "     (likes + reblogs + replies + bookmarks) AS interactions, "
            + "     recommendations "
            + "WHERE interactions >= $min_interactions "
            + "WITH uc, pc, base_weight, interactions, recommendations, "
            + "     CASE WHEN recommendations > 0 OR $ctr_smoothing > 0 "
            + "          THEN toFloat(interactions + $ctr_smoothing) "
            + "               / toFloat(recommendations + $ctr_smoothing) "
            + "          ELSE 0.0 END AS ctr "
            + "WHERE ctr >= $min_ctr "
            + "WITH uc, pc, base_weight, interactions, ctr, "
            + "     base_weight * ($ctr_weight * ctr + (1.0 - $ctr_weight)) "
            + "     AS weight "
            + return_clause
        )
    return (
        "WITH uc, pc, "
        + "     likes * $likes_weight + reblogs * $reblogs_weight "
        + "     + replies * $replies_weight + bookmarks * $bookmark_weight "
        + "     AS weight, "
        + "     (likes + reblogs + replies + bookmarks) AS interactions "
        + "WHERE interactions >= $min_interactions "
        + return_clause
    )


def _build_interests_query(
    return_clause: LiteralString,
    *,
    ctr_enabled: bool,
    has_dirty_filter: bool = False,
    rel_types: frozenset[str] | None = None,
) -> LiteralString:
    """Build a Cypher interests query: anchor on interaction edges, UNION ALL per type.

    Rows are produced only for (uc, pc) pairs that have at least one interaction edge
    contributing to scores; aggregates match the former OPTIONAL MATCH + Cartesian design.
    """

    def _has(t: str) -> bool:
        return rel_types is None or t in rel_types

    has_f = _has("FAVORITED")
    has_r = _has("REBLOGGED")
    has_rp = _has("REPLIED")
    has_bk = _has("BOOKMARKED")
    has_rec = _has("WAS_RECOMMENDED")
    use_ctr = ctr_enabled and has_rec

    dirty = _dirty_suffix(has_dirty_filter)

    branches: list[LiteralString] = []

    if has_f:
        branches.append(
            "MATCH (u:__user__)-[f:FAVORITED]->(p:__post__) "
            + _INTEREST_MATCH_CORE
            + dirty
            + "WITH uc, pc, sum("
            + _DECAY_F
            + ") AS likes "
            + (
                "RETURN uc, pc, likes, 0 AS reblogs, 0 AS replies, 0 AS bookmarks, "
                "0 AS recommendations"
                if use_ctr
                else "RETURN uc, pc, likes, 0 AS reblogs, 0 AS replies, 0 AS bookmarks"
            )
        )
    if has_r:
        branches.append(
            "MATCH (u:__user__)-[r:REBLOGGED]->(p:__post__) "
            + _INTEREST_MATCH_CORE
            + dirty
            + "WITH uc, pc, sum("
            + _DECAY_R
            + ") AS reblogs "
            + (
                "RETURN uc, pc, 0 AS likes, reblogs, 0 AS replies, 0 AS bookmarks, "
                "0 AS recommendations"
                if use_ctr
                else "RETURN uc, pc, 0 AS likes, reblogs, 0 AS replies, 0 AS bookmarks"
            )
        )
    if has_rp:
        branches.append(
            "MATCH (u:__user__)-[rp:REPLIED]->(p:__post__) "
            + _INTEREST_MATCH_CORE
            + dirty
            + "WITH uc, pc, sum("
            + _DECAY_RP
            + ") AS replies "
            + (
                "RETURN uc, pc, 0 AS likes, 0 AS reblogs, replies, 0 AS bookmarks, "
                "0 AS recommendations"
                if use_ctr
                else "RETURN uc, pc, 0 AS likes, 0 AS reblogs, replies, 0 AS bookmarks"
            )
        )
    if has_bk:
        branches.append(
            "MATCH (u:__user__)-[bk:BOOKMARKED]->(p:__post__) "
            + _INTEREST_MATCH_CORE
            + dirty
            + "WITH uc, pc, sum("
            + _DECAY_BK
            + ") AS bookmarks "
            + (
                "RETURN uc, pc, 0 AS likes, 0 AS reblogs, 0 AS replies, bookmarks, "
                "0 AS recommendations"
                if use_ctr
                else "RETURN uc, pc, 0 AS likes, 0 AS reblogs, 0 AS replies, bookmarks"
            )
        )
    if use_ctr and has_rec:
        branches.append(
            "MATCH (u:__user__)-[rec:WAS_RECOMMENDED]->(p:__post__) "
            + _INTEREST_MATCH_CORE
            + dirty
            + "WITH uc, pc, sum("
            + _DECAY_REC
            + ") AS recommendations "
            + "RETURN uc, pc, 0 AS likes, 0 AS reblogs, 0 AS replies, 0 AS bookmarks, "
            "recommendations"
        )

    if not branches:
        return _empty_interests_query(return_clause, use_ctr=use_ctr)

    union_body: LiteralString = branches[0]
    for b in branches[1:]:
        union_body = union_body + " UNION ALL " + b

    if use_ctr:
        call_wrapped: LiteralString = (
            "CALL () { "
            + union_body
            + " } "
            + "WITH uc, pc, "
            + "     sum(likes) AS likes, sum(reblogs) AS reblogs, "
            + "     sum(replies) AS replies, sum(bookmarks) AS bookmarks, "
            + "     sum(recommendations) AS recommendations "
        )
    else:
        call_wrapped = (
            "CALL () { "
            + union_body
            + " } "
            + "WITH uc, pc, "
            + "     sum(likes) AS likes, sum(reblogs) AS reblogs, "
            + "     sum(replies) AS replies, sum(bookmarks) AS bookmarks "
        )

    return call_wrapped + _weight_suffix_from_aggregates(return_clause, use_ctr=use_ctr)


def build_max_weights_query(
    ctr_enabled: bool,
    has_dirty_filter: bool = False,
    rel_types: frozenset[str] | None = None,
) -> LiteralString:
    """Build query returning ``(uc_id, max_weight)`` per UserCommunity."""
    return _build_interests_query(
        _RETURN_MAX_WEIGHTS,
        ctr_enabled=ctr_enabled,
        has_dirty_filter=has_dirty_filter,
        rel_types=rel_types,
    )


def build_interests_iterate_query(
    ctr_enabled: bool,
    has_dirty_filter: bool = False,
    rel_types: frozenset[str] | None = None,
) -> LiteralString:
    """Build query returning ``(uc_id, pc_id, weight, interactions)``."""
    return _build_interests_query(
        _RETURN_ITERATE,
        ctr_enabled=ctr_enabled,
        has_dirty_filter=has_dirty_filter,
        rel_types=rel_types,
    )


def build_interests_count_query(
    ctr_enabled: bool,
    has_dirty_filter: bool = False,
    rel_types: frozenset[str] | None = None,
) -> LiteralString:
    """Build query returning ``count(*) AS total``."""
    return _build_interests_query(
        _RETURN_COUNT,
        ctr_enabled=ctr_enabled,
        has_dirty_filter=has_dirty_filter,
        rel_types=rel_types,
    )


def build_interest_params(
    settings: HintGridSettings,
) -> dict[str, int | float]:
    """Build common query parameters for interest queries."""
    params: dict[str, int | float] = {
        "half_life_days": settings.decay_half_life_days,
        "min_interactions": settings.interests_min_favourites,
        "likes_weight": settings.likes_weight,
        "reblogs_weight": settings.reblogs_weight,
        "replies_weight": settings.replies_weight,
        "bookmark_weight": settings.bookmark_weight,
        "noise_community_id": settings.noise_community_id,
    }
    if settings.ctr_enabled:
        params.update(
            {
                "ctr_weight": settings.ctr_weight,
                "min_ctr": settings.min_ctr,
                "ctr_smoothing": settings.ctr_smoothing,
            }
        )
    return params


INTEREST_LABELS: dict[str, str] = {
    "user": "User",
    "uc": "UserCommunity",
    "post": "Post",
    "pc": "PostCommunity",
}


def bulk_set_max_weight_temp(
    neo4j: Neo4jClient,
    max_weight_map: dict[CommunityId, float],
) -> None:
    """Set ``max_weight_temp`` on UserCommunity nodes in one UNWIND (replaces per-uc round-trips)."""
    if not max_weight_map:
        return
    rows: list[dict[str, Neo4jParameter]] = [
        {"uc_id": uc_id, "max_weight": weight} for uc_id, weight in max_weight_map.items()
    ]
    neo4j.execute_labeled(
        "UNWIND $rows AS row "
        "MATCH (uc:__uc__) WHERE uc.id = row.uc_id "
        "SET uc.max_weight_temp = row.max_weight",
        {"uc": "UserCommunity"},
        {"rows": rows},
    )


def build_dirty_uc_query(
    rel_types: frozenset[str] | None = None,
) -> LiteralString:
    """Build query to find dirty UserCommunities with new interactions.

    Only includes EXISTS clauses for relationship types that actually
    exist in the graph.
    """
    header: LiteralString = (
        "MATCH (u:__user__)-[:BELONGS_TO]->(uc:__uc__) "
        "WHERE uc.id <> $noise_community_id AND ("
    )

    clauses: list[LiteralString] = []
    _rel_exists: list[tuple[str, LiteralString]] = [
        (
            "FAVORITED",
            "EXISTS { MATCH (u)-[f:FAVORITED]->() WHERE f.at > datetime($last_rebuild_at) }",
        ),
        (
            "REBLOGGED",
            "EXISTS { MATCH (u)-[r:REBLOGGED]->() WHERE r.at > datetime($last_rebuild_at) }",
        ),
        (
            "REPLIED",
            "EXISTS { MATCH (u)-[rp:REPLIED]->() WHERE rp.at > datetime($last_rebuild_at) }",
        ),
        (
            "BOOKMARKED",
            "EXISTS { MATCH (u)-[bk:BOOKMARKED]->() WHERE bk.at > datetime($last_rebuild_at) }",
        ),
    ]
    for rel_name, clause in _rel_exists:
        if rel_types is None or rel_name in rel_types:
            clauses.append(clause)

    if not clauses:
        return "MATCH (uc:__uc__) WHERE false RETURN uc.id AS uc_id"

    joined: LiteralString = clauses[0]
    for clause in clauses[1:]:
        joined = joined + " OR " + clause

    return header + joined + ") RETURN DISTINCT uc.id AS uc_id"
