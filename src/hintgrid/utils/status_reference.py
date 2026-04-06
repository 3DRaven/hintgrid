"""Parse Mastodon post references (URL, public id, internal id) for PostgreSQL lookup."""

from __future__ import annotations

import re
from urllib.parse import unquote, urlparse

_STATUS_PATH_RE = re.compile(r"/statuses/(\d+)")


def parse_post_reference(raw: str) -> tuple[bool, int | None, str] | None:
    """Parse user input into a resolution strategy for ``statuses``.

    Returns:
        ``(is_digits_only, pk_candidate, uri_like_pattern)`` where
        ``uri_like_pattern`` uses ``%`` wildcards for ``LIKE`` queries,
        or ``None`` if *raw* is empty after strip.

        When ``is_digits_only`` is True, callers should try primary key
        ``pk_candidate`` first, then fall back to ``uri_like_pattern``.
    """
    s = raw.strip()
    if not s:
        return None
    if s.isdigit():
        n = int(s)
        return (True, n, f"%{s}%")
    url_for_parse = s
    if "://" not in s:
        if s.startswith("//"):
            url_for_parse = "https:" + s
        elif "/" in s or s.startswith("www."):
            url_for_parse = "https://" + s
    parsed = urlparse(url_for_parse)
    path = unquote(parsed.path or "")
    m = _STATUS_PATH_RE.search(path)
    if m:
        frag = m.group(1)
        return (False, None, f"%{frag}%")
    parts = [p for p in path.split("/") if p]
    if len(parts) >= 2 and parts[-1].isdigit() and parts[-2].startswith("@"):
        frag = parts[-1]
        return (False, None, f"%{frag}%")
    return (False, None, f"%{s}%")
