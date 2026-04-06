"""Shared Cypher for personalized home feed (single-query soft PC diversification)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, LiteralString

if TYPE_CHECKING:
    from hintgrid.config import HintGridSettings

from hintgrid.pipeline.feed_queries import (
    language_match_score_case,
    popularity_score_params,
)


def personalized_feed_score_params(settings: HintGridSettings) -> dict[str, float | int]:
    """Parameters for personalized feed scoring (weights + popularity + language)."""
    base: dict[str, float | int] = {
        "feed_pc_share_weight": settings.feed_pc_share_weight,
        "feed_pc_size_weight": settings.feed_pc_size_weight,
        "popularity_weight": settings.personalized_popularity_weight,
        "recency_weight": settings.personalized_recency_weight,
        "pagerank_weight": settings.pagerank_weight if settings.pagerank_enabled else 0.0,
        "recency_smoothing": settings.recency_smoothing,
        "recency_numerator": settings.recency_numerator,
        "language_match_weight": settings.language_match_weight,
        "ui_language_match_weight": settings.ui_language_match_weight,
    }
    base.update(popularity_score_params(settings))
    return base


def public_feed_interest_weight(settings: HintGridSettings) -> float:
    """Scalar weight on aggregated community interest in public feed."""
    return settings.feed_pc_share_weight + settings.feed_pc_size_weight


PersonalizedReturnMode = Literal["feed", "detail", "explain"]


def _post_pattern(post_id_bound: bool) -> LiteralString:
    if post_id_bound:
        return (
            "MATCH (uc)-[i:INTERESTED_IN]->(pc:__pc__)<-[:BELONGS_TO]-(p:__post__ {id: $post_id}) "
        )
    return "MATCH (uc)-[i:INTERESTED_IN]->(pc:__pc__)<-[:BELONGS_TO]-(p:__post__) "


def build_personalized_feed_query(
    *,
    filters: LiteralString,
    local_raw_bind: LiteralString,
    global_raw_bind: LiteralString,
    pop_contrib: LiteralString,
    pr_bind: LiteralString,
    pr_w: LiteralString,
    return_mode: PersonalizedReturnMode,
) -> LiteralString:
    """Full personalized Cypher: UC aggregates (sum_i, max_pc_size), per-post score, RETURN."""
    post_pat = _post_pattern(return_mode == "explain")
    lang_case = language_match_score_case()

    head: LiteralString = (
        "MATCH (u:__user__ {id: $user_id})-[:BELONGS_TO]->(uc:__uc__) "
        "MATCH (uc)-[i_agg:INTERESTED_IN]->(pc_agg:__pc__) "
        "WITH u, uc, sum(i_agg.score) AS sum_i, "
        "max(toFloat(coalesce(pc_agg.size, 0))) AS max_pc_size "
    ) + post_pat

    where_clause: LiteralString = (
        "WHERE p.createdAt > datetime() - duration({days: $feed_days}) "
        "AND uc.id <> $noise_community_id AND pc.id <> $noise_community_id "
    ) + filters

    first_with: LiteralString = (
        head
        + where_clause
        + "WITH u, p, i, uc, pc, sum_i, max_pc_size, i.score AS interest_score, "
        + local_raw_bind
        + ", "
        + global_raw_bind
        + ", "
        "     duration.between(datetime(p.createdAt), datetime()).hours AS age_hours, "
        + pr_bind
        + " "
    )

    pop_with_plain: LiteralString = (
        "WITH u, p, i, uc, pc, sum_i, max_pc_size, interest_score, local_raw, global_raw, age_hours, pagerank, "
        "     (" + pop_contrib + ") AS popularity_contrib "
    )
    pop_with_explain: LiteralString = (
        "WITH u, p, i, uc, pc, sum_i, max_pc_size, interest_score, local_raw, global_raw, age_hours, pagerank, "
        "     (" + pop_contrib + ") AS popularity_contrib, "
        "     i.based_on AS based_on, i.serendipity AS serendipity, i.expires_at AS expires_at, "
        "     uc.id AS user_community_id, pc.id AS post_community_id "
    )
    pop_with = pop_with_explain if return_mode == "explain" else pop_with_plain

    share_with_plain: LiteralString = (
        "WITH u, p, i, uc, pc, sum_i, max_pc_size, interest_score, local_raw, global_raw, age_hours, pagerank, "
        "     popularity_contrib, "
        "     CASE WHEN sum_i > 0.0 THEN toFloat(i.score) / toFloat(sum_i) ELSE 0.0 END AS share_i, "
        "     CASE WHEN max_pc_size > 0.0 THEN toFloat(coalesce(pc.size, 0)) / max_pc_size "
        "          ELSE 0.0 END AS norm_pc_size "
    )
    share_with_explain: LiteralString = (
        "WITH u, p, i, uc, pc, sum_i, max_pc_size, interest_score, local_raw, global_raw, age_hours, pagerank, "
        "     popularity_contrib, based_on, serendipity, expires_at, user_community_id, post_community_id, "
        "     CASE WHEN sum_i > 0.0 THEN toFloat(i.score) / toFloat(sum_i) ELSE 0.0 END AS share_i, "
        "     CASE WHEN max_pc_size > 0.0 THEN toFloat(coalesce(pc.size, 0)) / max_pc_size "
        "          ELSE 0.0 END AS norm_pc_size "
    )
    share_with = share_with_explain if return_mode == "explain" else share_with_plain

    lang_with_plain: LiteralString = (
        "WITH u, p, i, uc, pc, sum_i, max_pc_size, interest_score, local_raw, global_raw, age_hours, pagerank, "
        "     popularity_contrib, share_i, norm_pc_size, " + lang_case + "AS language_match "
    )
    lang_with_explain: LiteralString = (
        "WITH u, p, i, uc, pc, sum_i, max_pc_size, interest_score, local_raw, global_raw, age_hours, pagerank, "
        "     popularity_contrib, based_on, serendipity, expires_at, user_community_id, post_community_id, "
        "     share_i, norm_pc_size, " + lang_case + "AS language_match "
    )
    lang_with = lang_with_explain if return_mode == "explain" else lang_with_plain

    score_with_plain: LiteralString = (
        "WITH u, p, i, uc, pc, sum_i, max_pc_size, interest_score, local_raw, global_raw, age_hours, pagerank, "
        "     popularity_contrib, share_i, norm_pc_size, language_match, "
        "     share_i * $feed_pc_share_weight + norm_pc_size * $feed_pc_size_weight + "
        "     popularity_contrib * $popularity_weight + "
        "     ($recency_numerator / (age_hours / 24.0 + $recency_smoothing)) * $recency_weight + "
        + pr_w
        + "language_match AS score "
    )
    score_with_explain: LiteralString = (
        "WITH u, p, i, uc, pc, sum_i, max_pc_size, interest_score, local_raw, global_raw, age_hours, pagerank, "
        "     popularity_contrib, based_on, serendipity, expires_at, user_community_id, post_community_id, "
        "     share_i, norm_pc_size, language_match, "
        "     share_i * $feed_pc_share_weight + norm_pc_size * $feed_pc_size_weight + "
        "     popularity_contrib * $popularity_weight + "
        "     ($recency_numerator / (age_hours / 24.0 + $recency_smoothing)) * $recency_weight + "
        + pr_w
        + "language_match AS score "
    )
    score_with = score_with_explain if return_mode == "explain" else score_with_plain

    core: LiteralString = first_with + pop_with + share_with + lang_with + score_with

    if return_mode == "feed":
        return core + "RETURN p.id AS post_id, score ORDER BY score DESC LIMIT $feed_size"

    if return_mode == "detail":
        return (
            core + "RETURN p.id AS post_id, "
            "       COALESCE(p.text, '') AS post_text, "
            "       p.language AS post_language, "
            "       p.createdAt AS post_created_at, "
            "       COALESCE(p.authorId, 0) AS author_id, "
            "       interest_score, share_i, norm_pc_size, "
            "       local_raw, "
            "       global_raw, "
            "       popularity_contrib, "
            "       age_hours, "
            "       pagerank, "
            "       language_match, "
            "       score AS final_score "
            "ORDER BY score DESC LIMIT $feed_size"
        )

    return (
        core
        + "RETURN score AS final_score, interest_score, share_i, norm_pc_size, local_raw, global_raw, "
        "       popularity_contrib, age_hours, pagerank, language_match, based_on, serendipity, expires_at, "
        "       user_community_id, post_community_id "
        "ORDER BY final_score DESC LIMIT 1"
    )
