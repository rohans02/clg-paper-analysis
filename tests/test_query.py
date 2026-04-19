from __future__ import annotations

from datetime import date

from publication_manager.models import (
    Publication,
    PublicationBookDetails,
    PublicationCore,
    PublicationJournalDetails,
)
from publication_manager.query import PublicationFilters, get_publications_df


def test_get_publications_df_reads_core_when_available(session):
    core = PublicationCore(
        faculty_name="Dr. Core",
        title="Core Title",
        authors="A1",
        publication_name="Journal Core",
        category="Scopus",
        publication_type="Journal",
        venue="Venue Core",
        national_international="International",
        pub_date=date(2025, 1, 1),
        doi="10.1000/core",
        doi_normalized="10.1000/core",
        paper_url="https://example.org/core",
        indexing_source="Scopus",
        approved_submission_id=None,
    )
    session.add(core)
    session.flush()
    session.add(
        PublicationJournalDetails(
            publication_id=core.id,
            quartile="Q1",
            issn_isbn="ISSN-CORE",
        )
    )
    session.flush()

    df = get_publications_df(session, PublicationFilters(faculty_name="Dr. Core"))

    assert len(df) == 1
    assert df.iloc[0]["title"] == "Core Title"
    assert df.iloc[0]["quartile"] == "Q1"
    assert df.iloc[0]["issn_isbn"] == "ISSN-CORE"


def test_get_publications_df_falls_back_to_legacy_when_core_empty(session):
    session.add(
        Publication(
            faculty_name="Dr. Legacy",
            title="Legacy Title",
            authors="A2",
            publication_name="Legacy Journal",
            category="WoS",
            publication_type="Journal",
            venue="Legacy Venue",
            national_international="International",
            pub_date=date(2025, 2, 2),
            doi="10.1000/legacy",
            doi_normalized="10.1000/legacy",
            paper_url="https://example.org/legacy",
            indexing_source="WoS",
            quartile="Q2",
            issn_isbn="ISSN-LEGACY",
            approved_submission_id=None,
        )
    )
    session.flush()

    df = get_publications_df(session, PublicationFilters(faculty_name="Dr. Legacy"))

    assert len(df) == 1
    assert df.iloc[0]["title"] == "Legacy Title"
    assert df.iloc[0]["quartile"] == "Q2"
    assert df.iloc[0]["issn_isbn"] == "ISSN-LEGACY"


def test_get_publications_df_supports_book_keyword_in_core_details(session):
    core = PublicationCore(
        faculty_name="Dr. Book",
        title="Book Title",
        authors="A3",
        publication_name="Book Name",
        category="Book",
        publication_type="Book Chapter",
        venue="Book Venue",
        national_international=None,
        pub_date=date(2026, 1, 1),
        doi=None,
        doi_normalized=None,
        paper_url="https://example.org/book",
        indexing_source="Book",
        approved_submission_id=None,
    )
    session.add(core)
    session.flush()
    session.add(
        PublicationBookDetails(
            publication_id=core.id,
            publisher="Elsevier",
            isbn="978-0-443-36434-1",
            book_indexed_scopus="Yes",
        )
    )
    session.flush()

    df = get_publications_df(session, PublicationFilters(keyword="Elsevier"))

    assert len(df) == 1
    assert df.iloc[0]["publisher"] == "Elsevier"
    assert df.iloc[0]["issn_isbn"] == "978-0-443-36434-1"
