"""Unit tests for Mastodon post reference parsing."""

from __future__ import annotations

import pytest

from hintgrid.utils.status_reference import parse_post_reference


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            "https://mastodon.ml/users/vint/statuses/115695503537021231",
            (False, None, "%115695503537021231%"),
        ),
        (
            "mastodon.ml/users/vint/statuses/115695503537021231",
            (False, None, "%115695503537021231%"),
        ),
        (
            "https://mastodon.social/@alice/12345678901234567",
            (False, None, "%12345678901234567%"),
        ),
        ("115695503537021231", (True, 115695503537021231, "%115695503537021231%")),
        ("  42 \n", (True, 42, "%42%")),
    ],
)
def test_parse_post_reference_cases(
    raw: str,
    expected: tuple[bool, int | None, str],
) -> None:
    """parse_post_reference extracts URL fragments or digit-only strategy."""
    assert parse_post_reference(raw) == expected


def test_parse_post_reference_empty() -> None:
    """Empty or whitespace-only input returns None."""
    assert parse_post_reference("") is None
    assert parse_post_reference("   ") is None


def test_parse_post_reference_arbitrary_substring() -> None:
    """Non-URL free text becomes a LIKE substring."""
    r = parse_post_reference("some opaque blob")
    assert r is not None
    is_digits, pk, pat = r
    assert is_digits is False
    assert pk is None
    assert pat == "%some opaque blob%"
