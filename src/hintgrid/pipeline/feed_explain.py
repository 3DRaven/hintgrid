"""Explain why a post would be scored for a viewer's home feed (diagnostics)."""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Literal, LiteralString, TypedDict, cast

if TYPE_CHECKING:
    from hintgrid.clients.neo4j import Neo4jClient
    from hintgrid.clients.redis import RedisClient

from hintgrid.config import HintGridSettings, feed_debug_settings_snapshot
from hintgrid.pipeline.feed_queries import (
    build_feed_filters,
    build_popularity_expr,
    language_match_score_case,
    pagerank_binding,
    pagerank_score_weight_line,
)
from hintgrid.utils.coercion import coerce_float, coerce_int

logger = logging.getLogger(__name__)

FeedInclusionPath = Literal["personalized", "cold_start", "not_scored"]


class FeedFilterStatus(TypedDict):
    """Graph filter predicates for feed eligibility (same semantics as build_feed_filters)."""

    within_feed_days: bool
    has_embedding: bool | None
    was_recommended_block: bool
    user_wrote_post: bool
    user_favorited_post: bool
    hates_user_block: bool
    hates_user_filter_active: bool
    was_recommended_filter_active: bool
    favorited_filter_active: bool


class InterestEdgeDetail(TypedDict, total=False):
    """INTERESTED_IN edge and community ids for personalized path."""

    user_community_id: int
    post_community_id: int
    interest_rel_score: float
    based_on: int | None
    serendipity: bool | None
    expires_at: object | None


class ScoreComponents(TypedDict):
    """Raw and weighted parts matching generate_user_feed Cypher formula."""

    interest_score: float
    popularity: int
    age_hours: float
    pagerank: float
    language_match: float
    weighted_interest: float
    weighted_popularity: float
    weighted_recency: float
    weighted_pagerank: float
    final_cypher_score: float


class RedisPlacement(TypedDict, total=False):
    """Redis sorted-set placement for feed:home:{user_id} (same key as write_feed_to_redis)."""

    redis_key: str
    member: str
    redis_score: float | None
    zrevrank_0_is_top: int | None
    zcard: int | None
    rank_formula: str


class FeedInclusionExplanation(TypedDict):
    """Full inclusion explanation for (viewer, post); settings are current values only."""

    viewer_user_id: int
    post_id: int
    path: FeedInclusionPath
    filter_status: FeedFilterStatus
    interest_edge: InterestEdgeDetail | None
    score_components: ScoreComponents | None
    redis: RedisPlacement
    settings_snapshot: dict[str, str | int | float | bool | None]
    notes: list[str]


def _home_feed_redis_key(user_id: int) -> str:
    """Must match write_feed_to_redis (no REDIS_NAMESPACE prefix for home feeds)."""
    return f"feed:home:{user_id}"


def feed_explain_rel_types(
    existing: frozenset[str], *, respect_was_recommended: bool
) -> frozenset[str]:
    """Derive ``rel_types`` for :func:`explain_feed_inclusion` from graph relationship types.

    When ``respect_was_recommended`` is False, ``WAS_RECOMMENDED`` is omitted so
    :func:`hintgrid.pipeline.feed_queries.build_feed_filters` does not add
    ``NOT EXISTS (u)-[:WAS_RECOMMENDED]->(p)`` — diagnostics can show personalized or
    cold_start scoring for posts already marked recommended. When True, behavior
    matches feed generation (strict duplicate filter when that type exists).
    """
    if respect_was_recommended:
        return existing
    return frozenset(t for t in existing if t != "WAS_RECOMMENDED")


def _compute_weighted_components(
    *,
    path: FeedInclusionPath,
    interest_score: float,
    popularity: int,
    age_hours: float,
    pagerank: float,
    language_match: float,
    settings: HintGridSettings,
) -> ScoreComponents:
    """Mirror Cypher scoring from generate_user_feed / get_detailed_recommendations."""
    pop = float(popularity)
    recency_term = settings.recency_numerator / (age_hours / 24.0 + settings.recency_smoothing)
    if path == "personalized":
        wi = interest_score * settings.personalized_interest_weight
        wp = (
            math.log10(pop + settings.popularity_smoothing)
            * settings.personalized_popularity_weight
        )
        wr = recency_term * settings.personalized_recency_weight
    elif path == "cold_start":
        wi = 0.0
        wp = math.log10(pop + settings.popularity_smoothing) * settings.cold_start_popularity_weight
        wr = recency_term * settings.cold_start_recency_weight
    else:
        wi = 0.0
        wp = 0.0
        wr = 0.0
    wpr = pagerank * settings.pagerank_weight if settings.pagerank_enabled else 0.0
    final = wi + wp + wr + wpr + language_match
    return ScoreComponents(
        interest_score=interest_score,
        popularity=popularity,
        age_hours=age_hours,
        pagerank=pagerank,
        language_match=language_match,
        weighted_interest=wi,
        weighted_popularity=wp,
        weighted_recency=wr,
        weighted_pagerank=wpr,
        final_cypher_score=final,
    )


def _filter_flags_query(rel_types: frozenset[str] | None) -> LiteralString:
    """Build RETURN expressions for active feed filters."""
    wr = "true" if rel_types is None or "WAS_RECOMMENDED" in rel_types else "false"
    fav = "true" if rel_types is None or "FAVORITED" in rel_types else "false"
    hates = "true" if rel_types is None or "HATES_USER" in rel_types else "false"
    return (
        f"  EXISTS {{ (u)-[:WAS_RECOMMENDED]->(p) }} AS was_recommended_block, "
        f"  EXISTS {{ (u)-[:WROTE]->(p) }} AS user_wrote_post, "
        f"  EXISTS {{ (u)-[:FAVORITED]->(p) }} AS user_favorited_post, "
        f"  CASE WHEN {hates} THEN EXISTS {{ (p)<-[:WROTE]-(:__user__)<-[:HATES_USER]-(u) }} "
        f"       ELSE false END AS hates_user_block, "
        f"  {hates} AS hates_user_filter_active, "
        f"  {wr} AS was_recommended_filter_active, "
        f"  {fav} AS favorited_filter_active, "
        f"  p.createdAt > datetime() - duration({{days: $feed_days}}) AS within_feed_days, "
        f"  p.embedding IS NOT NULL AS has_embedding"
    )


def _fetch_filter_status(
    neo4j: Neo4jClient,
    viewer_user_id: int,
    post_id: int,
    settings: HintGridSettings,
    rel_types: frozenset[str] | None,
) -> FeedFilterStatus | None:
    q: LiteralString = (
        "MATCH (u:__user__ {id: $user_id}), (p:__post__ {id: $post_id}) "
        "RETURN " + _filter_flags_query(rel_types)
    )
    rows = list(
        neo4j.execute_and_fetch_labeled(
            q,
            {"user": "User", "post": "Post"},
            {"user_id": viewer_user_id, "post_id": post_id, "feed_days": settings.feed_days},
        )
    )
    if not rows:
        return None
    row = rows[0]
    return FeedFilterStatus(
        within_feed_days=bool(row.get("within_feed_days")),
        has_embedding=bool(row.get("has_embedding"))
        if row.get("has_embedding") is not None
        else None,
        was_recommended_block=bool(row.get("was_recommended_block")),
        user_wrote_post=bool(row.get("user_wrote_post")),
        user_favorited_post=bool(row.get("user_favorited_post")),
        hates_user_block=bool(row.get("hates_user_block")),
        hates_user_filter_active=bool(row.get("hates_user_filter_active")),
        was_recommended_filter_active=bool(row.get("was_recommended_filter_active")),
        favorited_filter_active=bool(row.get("favorited_filter_active")),
    )


def _run_personalized_single(
    neo4j: Neo4jClient,
    viewer_user_id: int,
    post_id: int,
    settings: HintGridSettings,
    rel_types: frozenset[str] | None,
) -> dict[str, object] | None:
    filters = build_feed_filters(rel_types)
    popularity_expr = build_popularity_expr(rel_types)
    pr_bind = pagerank_binding(settings)
    pr_w = pagerank_score_weight_line(settings)
    personalized_query: LiteralString = (
        "MATCH (u:__user__ {id: $user_id})-[:BELONGS_TO]->(uc:__uc__) "
        "      -[i:INTERESTED_IN]->(pc:__pc__)<-[:BELONGS_TO]-(p:__post__ {id: $post_id}) "
        "WHERE p.createdAt > datetime() - duration({days: $feed_days}) "
        "AND uc.id <> $noise_community_id AND pc.id <> $noise_community_id "
        + filters
        + "WITH u, p, i, uc, pc, i.score AS interest_score, "
        + popularity_expr
        + ", "
        "     duration.between(datetime(p.createdAt), datetime()).hours AS age_hours, "
        + pr_bind
        + " "
        "WITH u, p, i, uc, pc, interest_score, popularity, age_hours, pagerank, "
        "     i.based_on AS based_on, i.serendipity AS serendipity, i.expires_at AS expires_at, "
        "     uc.id AS user_community_id, pc.id AS post_community_id, "
        "     interest_score * $interest_weight + "
        "     log10(popularity + $popularity_smoothing) * $popularity_weight + "
        "     ($recency_numerator / (age_hours / 24.0 + $recency_smoothing)) * $recency_weight + "
        + pr_w
        + language_match_score_case()
        + "AS score, "
        + language_match_score_case()
        + "AS language_match "
        "RETURN score AS final_score, interest_score, popularity, age_hours, pagerank, "
        "       language_match, based_on, serendipity, expires_at, "
        "       user_community_id, post_community_id "
        "ORDER BY final_score DESC LIMIT 1"
    )
    rows = list(
        neo4j.execute_and_fetch_labeled(
            personalized_query,
            {"user": "User", "uc": "UserCommunity", "pc": "PostCommunity", "post": "Post"},
            {
                "user_id": viewer_user_id,
                "post_id": post_id,
                "feed_days": settings.feed_days,
                "noise_community_id": settings.noise_community_id,
                "interest_weight": settings.personalized_interest_weight,
                "popularity_weight": settings.personalized_popularity_weight,
                "recency_weight": settings.personalized_recency_weight,
                "pagerank_weight": settings.pagerank_weight if settings.pagerank_enabled else 0.0,
                "popularity_smoothing": settings.popularity_smoothing,
                "recency_smoothing": settings.recency_smoothing,
                "recency_numerator": settings.recency_numerator,
                "language_match_weight": settings.language_match_weight,
                "ui_language_match_weight": settings.ui_language_match_weight,
            },
        )
    )
    if not rows:
        return None
    return cast(dict[str, object], rows[0])


def _run_cold_start_single(
    neo4j: Neo4jClient,
    viewer_user_id: int,
    post_id: int,
    settings: HintGridSettings,
    rel_types: frozenset[str] | None,
) -> dict[str, object] | None:
    filters = build_feed_filters(rel_types)
    popularity_expr = build_popularity_expr(rel_types)
    pr_bind = pagerank_binding(settings)
    pr_w = pagerank_score_weight_line(settings)
    cold_query: LiteralString = (
        "MATCH (u:__user__ {id: $user_id}), (p:__post__ {id: $post_id}) "
        "WHERE p.createdAt > datetime() - duration({days: $feed_days}) "
        "  AND p.embedding IS NOT NULL " + filters + "WITH u, p, " + popularity_expr + ", "
        "     duration.between(datetime(p.createdAt), datetime()).hours AS age_hours, "
        + pr_bind
        + " "
        "WITH u, p, popularity, age_hours, pagerank, "
        "     log10(popularity + $popularity_smoothing) * $popularity_weight + "
        "     ($recency_numerator / (age_hours / 24.0 + $recency_smoothing)) * $recency_weight + "
        + pr_w
        + language_match_score_case()
        + "AS score, "
        + language_match_score_case()
        + "AS language_match "
        "RETURN score AS final_score, popularity, age_hours, pagerank, language_match"
    )
    rows = list(
        neo4j.execute_and_fetch_labeled(
            cold_query,
            {"user": "User", "post": "Post"},
            {
                "user_id": viewer_user_id,
                "post_id": post_id,
                "feed_days": settings.feed_days,
                "popularity_weight": settings.cold_start_popularity_weight,
                "recency_weight": settings.cold_start_recency_weight,
                "pagerank_weight": settings.pagerank_weight if settings.pagerank_enabled else 0.0,
                "popularity_smoothing": settings.popularity_smoothing,
                "recency_smoothing": settings.recency_smoothing,
                "recency_numerator": settings.recency_numerator,
                "language_match_weight": settings.language_match_weight,
                "ui_language_match_weight": settings.ui_language_match_weight,
            },
        )
    )
    if not rows:
        return None
    return cast(dict[str, object], rows[0])


def _redis_placement(
    redis: RedisClient,
    viewer_user_id: int,
    post_id: int,
) -> RedisPlacement:
    key = _home_feed_redis_key(viewer_user_id)
    member = str(post_id)
    score = redis.zscore(key, member)
    rank = redis.zrevrank(key, member)
    card = redis.zcard(key)
    formula = (
        "HintGrid writes redis_score = max(post_id_in_batch) * feed_score_multiplier + "
        "(batch_size - zero_based_rank_in_batch); Mastodon may add native scores (score == post_id)."
    )
    return RedisPlacement(
        redis_key=key,
        member=member,
        redis_score=score,
        zrevrank_0_is_top=rank,
        zcard=card,
        rank_formula=formula,
    )


def explain_feed_inclusion(
    neo4j: Neo4jClient,
    redis: RedisClient,
    viewer_user_id: int,
    post_id: int,
    settings: HintGridSettings,
    *,
    rel_types: frozenset[str] | None = None,
) -> FeedInclusionExplanation | None:
    """Reconstruct scoring path for (viewer, post) using current graph and settings.

    Does not prove the post was in the last generated top-``feed_size`` batch; it only
    shows how the post would be scored under the same rules as ``generate_user_feed``.
    """
    filt = _fetch_filter_status(neo4j, viewer_user_id, post_id, settings, rel_types)
    if filt is None:
        return None

    notes: list[str] = [
        "Diagnostics use current HintGridSettings and graph state only (not historical runs).",
    ]

    snap = feed_debug_settings_snapshot(settings)

    personalized_row = _run_personalized_single(neo4j, viewer_user_id, post_id, settings, rel_types)
    if personalized_row:
        interest_score = coerce_float(personalized_row.get("interest_score") or 0.0)
        popularity = coerce_int(personalized_row.get("popularity") or 0)
        age_hours = coerce_float(personalized_row.get("age_hours") or 0.0)
        pagerank = coerce_float(personalized_row.get("pagerank") or 0.0)
        language_match = coerce_float(personalized_row.get("language_match") or 0.0)
        final_raw = coerce_float(personalized_row.get("final_score") or 0.0)
        components = _compute_weighted_components(
            path="personalized",
            interest_score=interest_score,
            popularity=popularity,
            age_hours=age_hours,
            pagerank=pagerank,
            language_match=language_match,
            settings=settings,
        )
        if abs(components["final_cypher_score"] - final_raw) > 1e-3:
            logger.warning(
                "Score component recompute mismatch: %s vs %s",
                components["final_cypher_score"],
                final_raw,
            )
            notes.append("Weighted recompute differs slightly from Cypher final_score (tolerance).")

        edge: InterestEdgeDetail = {}
        uc_raw = personalized_row.get("user_community_id")
        pc_raw = personalized_row.get("post_community_id")
        if uc_raw is not None:
            edge["user_community_id"] = coerce_int(uc_raw)
        if pc_raw is not None:
            edge["post_community_id"] = coerce_int(pc_raw)
        edge["interest_rel_score"] = interest_score
        bo = personalized_row.get("based_on")
        if bo is not None:
            edge["based_on"] = coerce_int(bo)
        ser = personalized_row.get("serendipity")
        if ser is not None:
            edge["serendipity"] = bool(ser)
        ex = personalized_row.get("expires_at")
        if ex is not None:
            edge["expires_at"] = ex

        return FeedInclusionExplanation(
            viewer_user_id=viewer_user_id,
            post_id=post_id,
            path="personalized",
            filter_status=filt,
            interest_edge=edge,
            score_components=components,
            redis=_redis_placement(redis, viewer_user_id, post_id),
            settings_snapshot=snap,
            notes=notes,
        )

    cold_row = _run_cold_start_single(neo4j, viewer_user_id, post_id, settings, rel_types)
    if cold_row:
        popularity = coerce_int(cold_row.get("popularity") or 0)
        age_hours = coerce_float(cold_row.get("age_hours") or 0.0)
        pagerank = coerce_float(cold_row.get("pagerank") or 0.0)
        language_match = coerce_float(cold_row.get("language_match") or 0.0)
        final_raw = coerce_float(cold_row.get("final_score") or 0.0)
        components = _compute_weighted_components(
            path="cold_start",
            interest_score=0.0,
            popularity=popularity,
            age_hours=age_hours,
            pagerank=pagerank,
            language_match=language_match,
            settings=settings,
        )
        if abs(components["final_cypher_score"] - final_raw) > 1e-3:
            notes.append("Weighted recompute differs slightly from Cypher final_score (tolerance).")

        return FeedInclusionExplanation(
            viewer_user_id=viewer_user_id,
            post_id=post_id,
            path="cold_start",
            filter_status=filt,
            interest_edge=None,
            score_components=components,
            redis=_redis_placement(redis, viewer_user_id, post_id),
            settings_snapshot=snap,
            notes=notes,
        )

    if not filt["within_feed_days"]:
        notes.append("Post is outside feed_days window.")
    if filt["has_embedding"] is False:
        notes.append("Cold start requires post.embedding IS NOT NULL.")
    if filt["was_recommended_block"] and filt["was_recommended_filter_active"]:
        notes.append("Blocked by WAS_RECOMMENDED (already recommended).")
    if filt["user_wrote_post"]:
        notes.append("Excluded: user wrote the post.")
    if filt["user_favorited_post"] and filt["favorited_filter_active"]:
        notes.append("Blocked by FAVORITED filter.")
    if filt["hates_user_block"] and filt["hates_user_filter_active"]:
        notes.append("Blocked by HATES_USER (author).")

    return FeedInclusionExplanation(
        viewer_user_id=viewer_user_id,
        post_id=post_id,
        path="not_scored",
        filter_status=filt,
        interest_edge=None,
        score_components=None,
        redis=_redis_placement(redis, viewer_user_id, post_id),
        settings_snapshot=snap,
        notes=notes,
    )


__all__ = [
    "FeedFilterStatus",
    "FeedInclusionExplanation",
    "FeedInclusionPath",
    "InterestEdgeDetail",
    "RedisPlacement",
    "ScoreComponents",
    "explain_feed_inclusion",
    "feed_explain_rel_types",
]
