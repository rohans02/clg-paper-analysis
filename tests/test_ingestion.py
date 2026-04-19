from __future__ import annotations

from publication_manager.ingestion import ingest_source


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

