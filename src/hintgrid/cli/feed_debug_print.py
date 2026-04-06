"""Rich output for feed inclusion diagnostics (keeps console.py under size limits)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

if TYPE_CHECKING:
    from hintgrid.pipeline.feed_explain import FeedInclusionExplanation

console = Console()


def print_feed_settings_snapshot(
    settings_snapshot: dict[str, str | int | float | bool | None],
) -> None:
    """Print feed-related settings snapshot (current values)."""
    table = Table(
        title="[bold]Feed-related settings (current)[/bold]",
        border_style="yellow",
        show_header=True,
        header_style="bold",
        show_edge=True,
    )
    table.add_column("Setting", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")
    for key in sorted(settings_snapshot.keys()):
        table.add_row(key, repr(settings_snapshot[key]))
    console.print()
    console.print(table)
    console.print()


def print_feed_inclusion_explanation(explanation: FeedInclusionExplanation) -> None:
    """Print feed inclusion path, filters, score breakdown, and Redis placement."""
    path = explanation["path"]
    filt = explanation["filter_status"]
    redis = explanation["redis"]
    notes = explanation["notes"]

    path_label = {
        "personalized": "[green]personalized[/green]",
        "cold_start": "[yellow]cold_start[/yellow]",
        "not_scored": "[red]not_scored[/red]",
    }.get(path, path)

    console.print(
        Panel.fit(
            f"[bold]Inclusion path:[/bold] {path_label}\n"
            f"[bold]Viewer[/bold] {explanation['viewer_user_id']}  "
            f"[bold]Post[/bold] {explanation['post_id']}",
            title="[bold]Feed inclusion[/bold]",
            border_style="magenta",
        )
    )
    console.print()

    ft = Table(
        title="[bold]Graph filters[/bold]",
        border_style="blue",
        show_header=True,
        header_style="bold",
    )
    ft.add_column("Check", style="cyan")
    ft.add_column("Value", style="white")
    ft.add_row("within_feed_days", str(filt["within_feed_days"]))
    ft.add_row("has_embedding", str(filt["has_embedding"]))
    ft.add_row("was_recommended_block", str(filt["was_recommended_block"]))
    ft.add_row("was_recommended_filter_active", str(filt["was_recommended_filter_active"]))
    ft.add_row("user_wrote_post", str(filt["user_wrote_post"]))
    ft.add_row("user_favorited_post", str(filt["user_favorited_post"]))
    ft.add_row("favorited_filter_active", str(filt["favorited_filter_active"]))
    ft.add_row("hates_user_block", str(filt["hates_user_block"]))
    ft.add_row("hates_user_filter_active", str(filt["hates_user_filter_active"]))
    console.print(ft)
    console.print()

    edge = explanation.get("interest_edge")
    if edge:
        et = Table(
            title="[bold]INTERESTED_IN edge[/bold]",
            border_style="green",
            show_header=True,
            header_style="bold",
        )
        et.add_column("Field", style="cyan")
        et.add_column("Value", style="white")
        if "user_community_id" in edge:
            et.add_row("user_community_id", str(edge["user_community_id"]))
        if "post_community_id" in edge:
            et.add_row("post_community_id", str(edge["post_community_id"]))
        et.add_row("interest_rel_score", f"{edge.get('interest_rel_score', 0.0):.6f}")
        if "based_on" in edge and edge["based_on"] is not None:
            et.add_row("based_on", str(edge["based_on"]))
        if "serendipity" in edge and edge["serendipity"] is not None:
            et.add_row("serendipity", str(edge["serendipity"]))
        if "expires_at" in edge and edge["expires_at"] is not None:
            et.add_row("expires_at", str(edge["expires_at"]))
        console.print(et)
        console.print()

    sc = explanation.get("score_components")
    if sc:
        st = Table(
            title="[bold]Cypher score components[/bold]",
            border_style="cyan",
            show_header=True,
            header_style="bold",
        )
        st.add_column("Component", style="cyan")
        st.add_column("Value", style="white", justify="right")
        st.add_row("interest_score", f"{sc['interest_score']:.6f}")
        st.add_row("popularity", str(sc["popularity"]))
        st.add_row("age_hours", f"{sc['age_hours']:.4f}")
        st.add_row("pagerank", f"{sc['pagerank']:.6f}")
        st.add_row("language_match", f"{sc['language_match']:.6f}")
        st.add_row("weighted_interest", f"{sc['weighted_interest']:.6f}")
        st.add_row("weighted_popularity", f"{sc['weighted_popularity']:.6f}")
        st.add_row("weighted_recency", f"{sc['weighted_recency']:.6f}")
        st.add_row("weighted_pagerank", f"{sc['weighted_pagerank']:.6f}")
        st.add_row("final_cypher_score (recomputed)", f"{sc['final_cypher_score']:.6f}")
        console.print(st)
        console.print()

    rt = Table(
        title="[bold]Redis home feed key[/bold]",
        border_style="red",
        show_header=True,
        header_style="bold",
    )
    rt.add_column("Field", style="cyan")
    rt.add_column("Value", style="white")
    rt.add_row("redis_key", str(redis.get("redis_key", "")))
    rt.add_row("member", str(redis.get("member", "")))
    rs = redis.get("redis_score")
    rt.add_row("redis_score", "—" if rs is None else f"{rs:,.6f}")
    zr = redis.get("zrevrank_0_is_top")
    rt.add_row("zrevrank (0 = highest score)", "—" if zr is None else str(zr))
    zc = redis.get("zcard")
    rt.add_row("zcard", "—" if zc is None else str(zc))
    rf = redis.get("rank_formula")
    if rf:
        rt.add_row("note", rf)
    console.print(rt)
    console.print()

    if notes:
        np = Panel(
            "\n".join(f"• {n}" for n in notes),
            title="[bold]Notes[/bold]",
            border_style="dim",
        )
        console.print(np)
        console.print()
