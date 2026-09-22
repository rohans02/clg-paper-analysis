from __future__ import annotations

from publication_manager.taxonomy import (
    ALLOWED_CATEGORIES,
    OFFICIAL_SHEET_MAP,
    category_from_label,
    category_label,
    category_type_options,
    is_official_pair,
    normalize_category,
    official_sheet_for,
    types_for_category,
)


def test_book_chapter_label_maps_to_stored_book_category():
    assert normalize_category("Book Chapter") == "Book"
    assert normalize_category("Book") == "Book"
    assert normalize_category("  Scopus  ") == "Scopus"
    assert normalize_category(None) is None
    assert category_label("Book") == "Book Chapter"
    assert category_from_label("Book Chapter") == "Book"


def test_ui_options_cover_every_official_sheet_plus_other():
    options = category_type_options()
    assert "Book Chapter" in options and "Book" not in options
    assert options["Book Chapter"] == ["Book Chapter"]
    assert options["Other"] == ["Journal", "Conference", "Book Chapter"]
    for (cat, ptype) in OFFICIAL_SHEET_MAP:
        assert ptype in options[category_label(cat)]


def test_official_pair_checks_accept_labels():
    assert is_official_pair("Book Chapter", "Book Chapter")
    assert is_official_pair("Book", "Book Chapter")
    assert not is_official_pair("Scopus", "Book Chapter")
    assert not is_official_pair("Other", "Journal")
    assert official_sheet_for("Book Chapter", "Book Chapter") == "Book ChapterBook"
    assert official_sheet_for("Other", "Journal") is None


def test_allowed_categories_include_other_but_not_labels():
    assert "Other" in ALLOWED_CATEGORIES
    assert "Book" in ALLOWED_CATEGORIES
    assert "Book Chapter" not in ALLOWED_CATEGORIES
    assert types_for_category("Peer Reviewed") == ["Journal"]
    assert types_for_category("Other") == ["Journal", "Conference", "Book Chapter"]
