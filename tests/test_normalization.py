from __future__ import annotations

from publication_manager.normalization import normalize_doi, parse_date


def test_normalize_doi():
    assert normalize_doi("https://doi.org/10.1000/xyz123") == "10.1000/xyz123"
    assert normalize_doi("DOI:10.1016/J.INS.2024.09.001") == "10.1016/j.ins.2024.09.001"
    assert normalize_doi("no doi") is None


def test_parse_date():
    assert str(parse_date("2025-01-14")) == "2025-01-14"
    assert str(parse_date("14-01-2025")) == "2025-01-14"
    assert str(parse_date(2026)) == "2026-01-01"

