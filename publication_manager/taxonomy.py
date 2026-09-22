"""Single source of truth for publication categories and types.

The official MMCOE workbook has one sheet per (category, publication_type) pair.
Records must be stored with the workbook's own category values so that the
official-format export and the system checks agree with the UI.
"""

from __future__ import annotations

PUBLICATION_TYPES = ("Journal", "Conference", "Book Chapter")

# (category, publication_type) -> sheet name in the official workbook.
OFFICIAL_SHEET_MAP: dict[tuple[str, str], str] = {
    ("Scopus", "Journal"): "Scopus Journal",
    ("Scopus", "Conference"): "Scopus Conference",
    ("International Conference", "Conference"): "International Conference",
    ("National Conference", "Conference"): "National Conference",
    ("WoS", "Journal"): "WoS Journal",
    ("WoS", "Conference"): "WoS Conference",
    ("Peer Reviewed", "Journal"): "Peer Reviewed Journal",
    ("UGC Care", "Journal"): "UGC Care Journal",
    ("Book", "Book Chapter"): "Book ChapterBook",
}

# Category value stored in the DB -> label shown in the UI.
CATEGORY_LABELS: dict[str, str] = {"Book": "Book Chapter"}
_LABEL_TO_CATEGORY = {label: value for value, label in CATEGORY_LABELS.items()}

OTHER_CATEGORY = "Other"

# Categories the workbook knows, plus "Other" for records with no official sheet.
OFFICIAL_CATEGORIES: tuple[str, ...] = tuple(dict.fromkeys(cat for cat, _ in OFFICIAL_SHEET_MAP))
ALLOWED_CATEGORIES: frozenset[str] = frozenset(OFFICIAL_CATEGORIES) | {OTHER_CATEGORY}


def category_label(category: str | None) -> str:
    """Display label for a stored category value."""
    if not category:
        return ""
    return CATEGORY_LABELS.get(category, category)


def category_from_label(label: str | None) -> str:
    """Stored category value for a UI label (inverse of ``category_label``)."""
    if not label:
        return ""
    return _LABEL_TO_CATEGORY.get(label, label)


def normalize_category(category: str | None) -> str | None:
    """Map UI labels and legacy spellings onto stored category values."""
    if not category:
        return category
    cleaned = " ".join(str(category).strip().split())
    return _LABEL_TO_CATEGORY.get(cleaned, cleaned)


def types_for_category(category: str) -> list[str]:
    """Publication types that have an official sheet for this category."""
    stored = normalize_category(category)
    types = [ptype for (cat, ptype) in OFFICIAL_SHEET_MAP if cat == stored]
    return types or list(PUBLICATION_TYPES)


def category_type_options() -> dict[str, list[str]]:
    """UI label -> allowed publication types, including the "Other" bucket."""
    options: dict[str, list[str]] = {}
    for cat, ptype in OFFICIAL_SHEET_MAP:
        options.setdefault(category_label(cat), []).append(ptype)
    options[OTHER_CATEGORY] = list(PUBLICATION_TYPES)
    return options


def is_official_pair(category: str | None, publication_type: str | None) -> bool:
    return (normalize_category(category), publication_type) in OFFICIAL_SHEET_MAP


def official_sheet_for(category: str | None, publication_type: str | None) -> str | None:
    return OFFICIAL_SHEET_MAP.get((normalize_category(category), publication_type))
