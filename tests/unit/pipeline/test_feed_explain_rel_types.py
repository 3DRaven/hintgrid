"""Unit tests for feed_explain_rel_types (CLI diagnostics rel_types selection)."""

from __future__ import annotations

from hintgrid.pipeline.feed_explain import feed_explain_rel_types


def test_feed_explain_rel_types_preserves_when_respect_true() -> None:
    existing = frozenset({"WAS_RECOMMENDED", "FAVORITED", "HATES_USER"})
    assert feed_explain_rel_types(existing, respect_was_recommended=True) == existing


def test_feed_explain_rel_types_drops_was_recommended_when_respect_false() -> None:
    existing = frozenset({"WAS_RECOMMENDED", "FAVORITED", "HATES_USER"})
    out = feed_explain_rel_types(existing, respect_was_recommended=False)
    assert out == frozenset({"FAVORITED", "HATES_USER"})
    assert "WAS_RECOMMENDED" not in out


def test_feed_explain_rel_types_empty_when_only_was_recommended() -> None:
    existing = frozenset({"WAS_RECOMMENDED"})
    assert feed_explain_rel_types(existing, respect_was_recommended=False) == frozenset()
