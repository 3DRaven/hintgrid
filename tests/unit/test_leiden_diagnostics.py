"""Unit tests for Leiden diagnostics helpers (no database)."""

from __future__ import annotations

from hintgrid.pipeline.leiden_diagnostics import (
    format_effective_gamma_hint,
    serialize_leiden_write_row,
)


def test_format_effective_gamma_hint_normal() -> None:
    """Ratio should be resolution divided by weight sum."""
    hint = format_effective_gamma_hint(0.1, 1000.0)
    assert "1.000000e-04" in hint or "0.0001" in hint
    assert "leiden_resolution/weight_sum=" in hint


def test_format_effective_gamma_hint_zero_weight() -> None:
    """Zero total weight must not divide."""
    hint = format_effective_gamma_hint(0.1, 0.0)
    assert "undefined" in hint


def test_serialize_leiden_write_row_maps_numbers() -> None:
    """GDS row keys are normalized to JSON-friendly dict."""
    row = {
        "nodePropertiesWritten": 10,
        "communityCount": 3,
        "modularity": 0.42,
        "ranLevels": 2,
        "didConverge": True,
        "nodeCount": 10,
        "modularities": [0.1, 0.42],
        "communityDistribution": {"min": 1.0, "p50": 3.0, "max": 5.0},
    }
    out = serialize_leiden_write_row(row)
    assert out["communityCount"] == 3
    assert out["ranLevels"] == 2
    assert out["didConverge"] is True
    assert out["modularities"] == [0.1, 0.42]
    assert out["communityDistribution"] == {"min": 1.0, "p50": 3.0, "max": 5.0}
