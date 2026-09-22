from __future__ import annotations

import socket

import pytest

import publication_manager.ingestion as ingestion
from publication_manager.ingestion import UnsafeUrlError, ingest_source, validate_public_url


def test_doi_input_ingestion():
    result = ingest_source(
        source_input="10.1000/xyz123",
        input_method="DOI",
        faculty_name="Dr. X",
    )
    assert result.payload["doi"] == "10.1000/xyz123"
    assert result.payload["faculty_name"] == "Dr. X"
    assert "doi_direct" in result.method_trace


def test_manual_overrides_are_applied():
    result = ingest_source(
        source_input="https://example.com/test",
        input_method="URL",
        faculty_name="Dr. Y",
        manual_overrides={"title": "Custom", "category": "WoS", "publication_type": "Journal"},
    )
    assert result.payload["title"] == "Custom"
    assert result.payload["category"] == "WoS"
    assert result.payload["publication_type"] == "Journal"


def _fake_resolver(ip: str):
    def getaddrinfo(host, port, *args, **kwargs):
        return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (ip, port))]

    return getaddrinfo


@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "ftp://example.org/x",
        "javascript:alert(1)",
        "not a url",
        "https://user:pw@example.org/paper",
    ],
)
def test_rejects_non_http_or_malformed_links(url):
    with pytest.raises(UnsafeUrlError):
        validate_public_url(url)


@pytest.mark.parametrize("ip", ["127.0.0.1", "10.0.0.5", "192.168.1.10", "172.16.0.1", "169.254.169.254", "0.0.0.0"])
def test_rejects_links_that_resolve_to_internal_addresses(monkeypatch, ip):
    monkeypatch.setattr(ingestion.socket, "getaddrinfo", _fake_resolver(ip))
    with pytest.raises(UnsafeUrlError):
        validate_public_url("http://internal.example/paper")


def test_accepts_links_that_resolve_to_public_addresses(monkeypatch):
    monkeypatch.setattr(ingestion.socket, "getaddrinfo", _fake_resolver("93.184.216.34"))
    assert validate_public_url("  https://example.org/paper ") == "https://example.org/paper"


def test_unresolvable_host_is_rejected(monkeypatch):
    def boom(*args, **kwargs):
        raise socket.gaierror("no such host")

    monkeypatch.setattr(ingestion.socket, "getaddrinfo", boom)
    with pytest.raises(UnsafeUrlError):
        validate_public_url("https://does-not-exist.invalid/")


def test_url_ingestion_reports_friendly_warning_for_blocked_link(monkeypatch):
    monkeypatch.setattr(ingestion.socket, "getaddrinfo", _fake_resolver("127.0.0.1"))
    result = ingest_source("http://localhost/admin", "URL", faculty_name="Dr. Z")
    assert result.warnings
    assert "private or internal" in result.warnings[0]
    assert "Traceback" not in result.warnings[0]


def _fake_crossref(monkeypatch, message: dict):
    import json as _json
    from publication_manager.ingestion import FetchedResource

    def fake_safe_get(url, timeout=10):
        assert url.startswith("https://api.crossref.org/works/10.1000/abc")
        return FetchedResource(url=url, content=_json.dumps({"message": message}).encode(), encoding="utf-8")

    monkeypatch.setattr(ingestion, "safe_get", fake_safe_get)


def test_doi_lookup_fills_title_authors_venue_date_and_type(monkeypatch):
    _fake_crossref(
        monkeypatch,
        {
            "title": ["A  Study of   Things"],
            "author": [{"given": "Asha", "family": "Rao"}, {"name": "Consortium X"}],
            "container-title": ["Journal of Things"],
            "published-print": {"date-parts": [[2025, 3]]},
            "type": "journal-article",
            "ISSN": ["1234-5678"],
            "volume": "12",
            "issue": "4",
            "publisher": "Springer",
            "URL": "https://doi.org/10.1000/abc",
        },
    )
    result = ingest_source("https://doi.org/10.1000/abc", "DOI", faculty_name="Dr. Q")
    p = result.payload
    assert p["doi"] == "10.1000/abc"
    assert p["title"] == "A Study of Things"
    assert p["authors"] == "Asha Rao, Consortium X"
    assert p["publication_name"] == "Journal of Things"
    assert str(p["pub_date"]) == "2025-03-01"
    assert p["publication_type"] == "Journal"
    assert p["issn_isbn"] == "1234-5678"
    assert p["volume_issue"] == "Vol. 12, Issue 4"
    assert "crossref" in result.method_trace
    assert not result.warnings


def test_doi_lookup_failure_is_a_friendly_warning(monkeypatch):
    def boom(url, timeout=10):
        raise ingestion.UnsafeUrlError("Could not resolve the link's host name.")

    monkeypatch.setattr(ingestion, "safe_get", boom)
    result = ingest_source("10.1000/abc", "DOI", faculty_name="Dr. Q")
    assert result.payload["doi"] == "10.1000/abc"
    assert result.warnings and "Could not look the DOI up" in result.warnings[0]


def test_conference_and_book_types_map_from_crossref(monkeypatch):
    _fake_crossref(monkeypatch, {"title": ["Conf paper"], "type": "proceedings-article", "issued": {"date-parts": [[2024]]}})
    assert ingest_source("10.1000/abc", "DOI", faculty_name="x").payload["publication_type"] == "Conference"
    _fake_crossref(monkeypatch, {"title": ["Chapter"], "type": "book-chapter", "ISBN": ["978-1"]})
    p = ingest_source("10.1000/abc", "DOI", faculty_name="x").payload
    assert p["publication_type"] == "Book Chapter" and p["issn_isbn"] == "978-1"
