"""Normalize Mastodon locale and language codes for Neo4j User properties."""

from __future__ import annotations


def normalize_language_code(raw: str | None) -> str | None:
    """Normalize a BCP 47 tag or ISO code for comparison with Post.language.

    Takes the primary subtag (segment before the first ``-`` or ``_``) and lowercases it,
    matching typical Mastodon ``statuses.language`` values (ISO 639-1).

    Args:
        raw: Locale string from PostgreSQL (e.g. ``en``, ``pt-BR``, ``zh_Hans``) or None.

    Returns:
        Normalized code or None if empty or whitespace-only input.
    """
    if raw is None:
        return None
    stripped = raw.strip()
    if not stripped:
        return None
    primary = stripped
    for sep in ("-", "_"):
        if sep in primary:
            primary = primary.split(sep, 1)[0]
            break
    return primary.lower()


def normalize_chosen_languages(chosen: list[str] | None) -> list[str] | None:
    """Normalize chosen_languages; dedupe by normalized code, preserve first-seen order.

    Args:
        chosen: Raw list from PostgreSQL or None.

    Returns:
        Non-empty list of normalized codes, or None if input is empty or None.
    """
    if not chosen:
        return None
    seen: set[str] = set()
    out: list[str] = []
    for item in chosen:
        code = normalize_language_code(item)
        if code is not None and code not in seen:
            seen.add(code)
            out.append(code)
    return out if out else None


def user_activity_row_to_neo4j_fields(
    *,
    locale: str | None,
    chosen_languages: list[str] | None,
) -> tuple[str | None, list[str] | None]:
    """Build ``ui_language`` and ``languages`` (chosen-only) for ``update_user_activity`` batch rows.

    Returns:
        Tuple ``(ui_language, languages)`` for Neo4j ``User.uiLanguage`` and ``User.languages``.
    """
    ui = normalize_language_code(locale)
    langs = normalize_chosen_languages(chosen_languages)
    return (ui, langs)
