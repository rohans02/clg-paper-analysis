from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from statistics import median
from typing import Any

import pandas as pd
from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from publication_manager.enums import SubmissionStatus
from publication_manager.models import (
    PendingSubmission,
    Publication,
    PublicationBookDetails,
    PublicationConferenceDetails,
    PublicationCore,
    PublicationJournalDetails,
)


@dataclass
class PublicationFilters:
    faculty_name: str | None = None
    category: str | None = None
    publication_type: str | None = None
    indexing_source: str | None = None
    national_international: str | None = None
    quartile: str | None = None
    keyword: str | None = None
    date_from: date | None = None
    date_to: date | None = None


def get_publications_df(session: Session, filters: PublicationFilters | None = None) -> pd.DataFrame:
    if _has_core_data(session):
        return _get_publications_df_core(session, filters)
    return _get_publications_df_legacy(session, filters)


def _has_core_data(session: Session) -> bool:
    count = session.scalar(select(func.count(PublicationCore.id))) or 0
    return count > 0


def _get_publications_df_core(session: Session, filters: PublicationFilters | None = None) -> pd.DataFrame:
    filters = filters or PublicationFilters()
    stmt = (
        select(PublicationCore, PublicationJournalDetails, PublicationConferenceDetails, PublicationBookDetails)
        .outerjoin(PublicationJournalDetails, PublicationJournalDetails.publication_id == PublicationCore.id)
        .outerjoin(PublicationConferenceDetails, PublicationConferenceDetails.publication_id == PublicationCore.id)
        .outerjoin(PublicationBookDetails, PublicationBookDetails.publication_id == PublicationCore.id)
    )
    conditions = []

    if filters.faculty_name:
        conditions.append(PublicationCore.faculty_name == filters.faculty_name)
    if filters.category:
        conditions.append(PublicationCore.category == filters.category)
    if filters.publication_type:
        conditions.append(PublicationCore.publication_type == filters.publication_type)
    if filters.indexing_source:
        conditions.append(PublicationCore.indexing_source == filters.indexing_source)
    if filters.national_international:
        conditions.append(PublicationCore.national_international == filters.national_international)
    if filters.quartile:
        conditions.append(PublicationJournalDetails.quartile == filters.quartile)
    if filters.date_from:
        conditions.append(PublicationCore.pub_date >= filters.date_from)
    if filters.date_to:
        conditions.append(PublicationCore.pub_date <= filters.date_to)
    if filters.keyword:
        like = f"%{filters.keyword}%"
        conditions.append(
            or_(
                PublicationCore.title.ilike(like),
                PublicationCore.publication_name.ilike(like),
                PublicationCore.authors.ilike(like),
                PublicationCore.venue.ilike(like),
                PublicationCore.doi.ilike(like),
                PublicationBookDetails.publisher.ilike(like),
                PublicationBookDetails.isbn.ilike(like),
            )
        )
    if conditions:
        stmt = stmt.where(and_(*conditions))
    stmt = stmt.order_by(PublicationCore.created_at.desc())

    rows = session.execute(stmt).all()
    data = []
    for core, journal, conference, book in rows:
        issn_or_isbn = None
        if journal and journal.issn_isbn:
            issn_or_isbn = journal.issn_isbn
        elif conference and conference.issn_isbn:
            issn_or_isbn = conference.issn_isbn
        elif book and book.isbn:
            issn_or_isbn = book.isbn

        data.append(
            {
                "id": core.id,
                "faculty_name": core.faculty_name,
                "title": core.title,
                "publication_name": core.publication_name,
                "authors": core.authors,
                "category": core.category,
                "publication_type": core.publication_type,
                "venue": core.venue,
                "conference_date": core.conference_date,
                "pub_date": core.pub_date,
                "doi": core.doi,
                "paper_url": core.paper_url,
                "indexing_source": core.indexing_source,
                "quartile": journal.quartile if journal else None,
                "national_international": core.national_international,
                "approved_submission_id": core.approved_submission_id,
                "issn_isbn": issn_or_isbn,
                "publisher": book.publisher if book else None,
                "book_indexed_ugc": book.book_indexed_ugc if book else None,
                "book_indexed_scopus": book.book_indexed_scopus if book else None,
                "book_indexed_wos": book.book_indexed_wos if book else None,
                "presented_accepted_flag": conference.presented_accepted_flag if conference else None,
                "volume_issue": (journal.volume_issue if journal else None) or (conference.volume_issue if conference else None),
                "official_venue_url": (journal.official_venue_url if journal else None)
                or (conference.official_venue_url if conference else None)
                or (book.official_venue_url if book else None),
                "research_published_flag": (journal.research_published_flag if journal else None)
                or (conference.research_published_flag if conference else None),
                "indexing_flag": (journal.indexing_flag if journal else None) or (conference.indexing_flag if conference else None),
                "indexing_proof": (journal.indexing_proof if journal else None) or (conference.indexing_proof if conference else None),
                "certificate_ref": conference.certificate_ref if conference else None,
                "attachment_ref": (journal.attachment_ref if journal else None)
                or (conference.attachment_ref if conference else None)
                or (book.attachment_ref if book else None),
            }
        )
    return pd.DataFrame(data)


def _get_publications_df_legacy(session: Session, filters: PublicationFilters | None = None) -> pd.DataFrame:
    filters = filters or PublicationFilters()
    stmt = select(Publication)
    conditions = []

    if filters.faculty_name:
        conditions.append(Publication.faculty_name == filters.faculty_name)
    if filters.category:
        conditions.append(Publication.category == filters.category)
    if filters.publication_type:
        conditions.append(Publication.publication_type == filters.publication_type)
    if filters.indexing_source:
        conditions.append(Publication.indexing_source == filters.indexing_source)
    if filters.national_international:
        conditions.append(Publication.national_international == filters.national_international)
    if filters.quartile:
        conditions.append(Publication.quartile == filters.quartile)
    if filters.date_from:
        conditions.append(Publication.pub_date >= filters.date_from)
    if filters.date_to:
        conditions.append(Publication.pub_date <= filters.date_to)
    if filters.keyword:
        like = f"%{filters.keyword}%"
        conditions.append(
            or_(
                Publication.title.ilike(like),
                Publication.publication_name.ilike(like),
                Publication.authors.ilike(like),
                Publication.venue.ilike(like),
                Publication.doi.ilike(like),
            )
        )
    if conditions:
        stmt = stmt.where(and_(*conditions))
    stmt = stmt.order_by(Publication.created_at.desc())
    rows = session.execute(stmt).scalars().all()
    data = []
    for row in rows:
        data.append(
            {
                "id": row.id,
                "faculty_name": row.faculty_name,
                "title": row.title,
                "publication_name": row.publication_name,
                "authors": row.authors,
                "category": row.category,
                "publication_type": row.publication_type,
                "venue": row.venue,
                "conference_date": row.conference_date,
                "pub_date": row.pub_date,
                "doi": row.doi,
                "paper_url": row.paper_url,
                "indexing_source": row.indexing_source,
                "quartile": row.quartile,
                "national_international": row.national_international,
                "approved_submission_id": row.approved_submission_id,
                "issn_isbn": row.issn_isbn,
                "publisher": None,
                "book_indexed_ugc": None,
                "book_indexed_scopus": None,
                "book_indexed_wos": None,
                "presented_accepted_flag": None,
                "volume_issue": None,
                "official_venue_url": None,
                "research_published_flag": None,
                "indexing_flag": None,
                "indexing_proof": None,
                "certificate_ref": None,
                "attachment_ref": None,
            }
        )
    return pd.DataFrame(data)


def _collect_submission_stats(session: Session) -> pd.DataFrame:
    submissions = session.execute(select(PendingSubmission)).scalars()
    rows: list[dict[str, Any]] = []
    for sub in submissions:
        payload = sub.parsed_payload_json or {}
        faculty = payload.get("faculty_name") or sub.submitted_by
        review_hours = None
        if sub.reviewed_at and sub.created_at:
            review_hours = (sub.reviewed_at - sub.created_at).total_seconds() / 3600.0
        rows.append(
            {
                "faculty_name": faculty,
                "status": sub.status,
                "review_hours": review_hours,
            }
        )
    return pd.DataFrame(rows)


def get_dashboard_metrics(session: Session) -> dict[str, Any]:
    pub_df = get_publications_df(session)
    total_publications = len(pub_df)

    submissions_df = _collect_submission_stats(session)
    pending_reviews = 0
    if not submissions_df.empty:
        pending_reviews = int(
            submissions_df["status"].isin([SubmissionStatus.SUBMITTED.value, SubmissionStatus.UNDER_REVIEW.value]).sum()
        )

    by_faculty = (
        pub_df.groupby("faculty_name", dropna=True)["id"].count().reset_index(name="count").sort_values("count", ascending=False)
        if not pub_df.empty
        else pd.DataFrame(columns=["faculty_name", "count"])
    )
    by_category = (
        pub_df.groupby("category", dropna=True)["id"].count().reset_index(name="count").sort_values("count", ascending=False)
        if not pub_df.empty
        else pd.DataFrame(columns=["category", "count"])
    )
    by_year = (
        pub_df.assign(year=pd.to_datetime(pub_df["pub_date"], errors="coerce").dt.year)
        .groupby("year", dropna=True)["id"]
        .count()
        .reset_index(name="count")
        .sort_values("year", ascending=True)
        if not pub_df.empty
        else pd.DataFrame(columns=["year", "count"])
    )

    data_health = {
        "missing_doi_count": int(pub_df["doi"].isna().sum()) if "doi" in pub_df else 0,
        "missing_pub_date_count": int(pub_df["pub_date"].isna().sum()) if "pub_date" in pub_df else 0,
        "manual_cleanup_count": int(
            (
                (pub_df["publication_type"] != "Journal") & pub_df["quartile"].notna()
            ).sum()
        )
        if not pub_df.empty
        else 0,
    }

    return {
        "total_publications": total_publications,
        "pending_reviews": pending_reviews,
        "by_faculty": by_faculty,
        "by_category": by_category,
        "by_year": by_year,
        "data_health": data_health,
    }


def get_faculty_analysis_df(session: Session) -> pd.DataFrame:
    pub_df = get_publications_df(session)
    sub_df = _collect_submission_stats(session)

    if pub_df.empty and sub_df.empty:
        return pd.DataFrame(
            columns=[
                "faculty_name",
                "total_publications",
                "journal_count",
                "conference_count",
                "book_chapter_count",
                "pending_count",
                "approved_count",
                "rejected_count",
                "approval_rate_percent",
                "median_review_hours",
            ]
        )

    if pub_df.empty:
        faculty_names = sorted(sub_df["faculty_name"].dropna().unique().tolist())
    elif sub_df.empty:
        faculty_names = sorted(pub_df["faculty_name"].dropna().unique().tolist())
    else:
        faculty_names = sorted(set(pub_df["faculty_name"].dropna()) | set(sub_df["faculty_name"].dropna()))

    rows: list[dict[str, Any]] = []
    for faculty in faculty_names:
        faculty_pub = pub_df[pub_df["faculty_name"] == faculty] if not pub_df.empty else pd.DataFrame()
        faculty_sub = sub_df[sub_df["faculty_name"] == faculty] if not sub_df.empty else pd.DataFrame()
        approved = int((faculty_sub["status"] == SubmissionStatus.APPROVED.value).sum()) if not faculty_sub.empty else 0
        rejected = int((faculty_sub["status"] == SubmissionStatus.REJECTED.value).sum()) if not faculty_sub.empty else 0
        pending = int(
            faculty_sub["status"].isin([SubmissionStatus.SUBMITTED.value, SubmissionStatus.UNDER_REVIEW.value]).sum()
        ) if not faculty_sub.empty else 0
        decision_total = approved + rejected
        approval_rate = round((approved / decision_total) * 100, 2) if decision_total else None

        review_values = (
            faculty_sub.loc[
                faculty_sub["status"].isin([SubmissionStatus.APPROVED.value, SubmissionStatus.REJECTED.value]),
                "review_hours",
            ]
            .dropna()
            .tolist()
            if not faculty_sub.empty
            else []
        )
        med_review = round(float(median(review_values)), 2) if review_values else None

        category_counts = (
            faculty_pub.groupby("category")["id"].count().to_dict() if not faculty_pub.empty else {}
        )
        index_counts = (
            faculty_pub.groupby("indexing_source")["id"].count().to_dict() if not faculty_pub.empty else {}
        )

        row = {
            "faculty_name": faculty,
            "total_publications": int(len(faculty_pub)),
            "journal_count": int((faculty_pub["publication_type"] == "Journal").sum()) if not faculty_pub.empty else 0,
            "conference_count": int((faculty_pub["publication_type"] == "Conference").sum()) if not faculty_pub.empty else 0,
            "book_chapter_count": int((faculty_pub["publication_type"] == "Book Chapter").sum()) if not faculty_pub.empty else 0,
            "pending_count": pending,
            "approved_count": approved,
            "rejected_count": rejected,
            "approval_rate_percent": approval_rate,
            "median_review_hours": med_review,
        }
        for key, value in category_counts.items():
            row[f"cat_{key}"] = int(value)
        for key, value in index_counts.items():
            row[f"idx_{key}"] = int(value)
        rows.append(row)

    df = pd.DataFrame(rows).sort_values(["total_publications", "faculty_name"], ascending=[False, True])
    return df


def get_faculty_drilldown(session: Session, faculty_name: str) -> dict[str, Any]:
    pub_df = get_publications_df(session, PublicationFilters(faculty_name=faculty_name))
    sub_df = _collect_submission_stats(session)
    sub_df = sub_df[sub_df["faculty_name"] == faculty_name] if not sub_df.empty else sub_df

    trend = (
        pub_df.assign(year=pd.to_datetime(pub_df["pub_date"], errors="coerce").dt.year)
        .groupby("year", dropna=True)["id"]
        .count()
        .reset_index(name="count")
        .sort_values("year")
        if not pub_df.empty
        else pd.DataFrame(columns=["year", "count"])
    )
    latest = pub_df.sort_values("pub_date", ascending=False).head(10) if not pub_df.empty else pub_df

    kpis = {
        "publications": int(len(pub_df)),
        "journals": int((pub_df["publication_type"] == "Journal").sum()) if not pub_df.empty else 0,
        "conferences": int((pub_df["publication_type"] == "Conference").sum()) if not pub_df.empty else 0,
        "book_chapters": int((pub_df["publication_type"] == "Book Chapter").sum()) if not pub_df.empty else 0,
        "pending_reviews": int(
            sub_df["status"].isin([SubmissionStatus.SUBMITTED.value, SubmissionStatus.UNDER_REVIEW.value]).sum()
        )
        if not sub_df.empty
        else 0,
        "rejected": int((sub_df["status"] == SubmissionStatus.REJECTED.value).sum()) if not sub_df.empty else 0,
    }
    return {"kpis": kpis, "trend": trend, "latest": latest}
