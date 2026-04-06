"""Unit tests for language code normalization."""

from __future__ import annotations

import pytest

from hintgrid.utils.languages import (
    normalize_chosen_languages,
    normalize_language_code,
    user_activity_row_to_neo4j_fields,
)


@pytest.mark.unit
def test_normalize_language_code_basic() -> None:
    assert normalize_language_code("en") == "en"
    assert normalize_language_code("EN") == "en"


@pytest.mark.unit
def test_normalize_language_code_bcp47() -> None:
    assert normalize_language_code("pt-BR") == "pt"
    assert normalize_language_code("zh_Hans") == "zh"


@pytest.mark.unit
def test_normalize_language_code_empty() -> None:
    assert normalize_language_code(None) is None
    assert normalize_language_code("") is None
    assert normalize_language_code("   ") is None


@pytest.mark.unit
def test_normalize_chosen_languages_dedupes() -> None:
    assert normalize_chosen_languages(["en", "EN", "de"]) == ["en", "de"]


@pytest.mark.unit
def test_user_activity_row_to_neo4j_fields() -> None:
    ui, langs = user_activity_row_to_neo4j_fields(locale="ru_RU", chosen_languages=["en", "ru"])
    assert ui == "ru"
    assert langs == ["en", "ru"]
