from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    JSON,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class PendingSubmission(Base):
    __tablename__ = "pending_submissions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    submitted_by: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    source_input: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_input_method: Mapped[str] = mapped_column(String(32), nullable=False)
    parsed_payload_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    confidence_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    review_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    reviewed_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    actions: Mapped[list["ReviewAction"]] = relationship(
        back_populates="submission",
        cascade="all, delete-orphan",
    )


class Publication(Base):
    __tablename__ = "publications"
    __table_args__ = (UniqueConstraint("doi_normalized", name="uq_publication_doi_normalized"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    faculty_name: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    title: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    authors: Mapped[str | None] = mapped_column(Text, nullable=True)
    publication_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    category: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    publication_type: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    venue: Mapped[str | None] = mapped_column(Text, nullable=True)
    conference_date: Mapped[str | None] = mapped_column(Text, nullable=True)
    national_international: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    pub_date: Mapped[datetime | None] = mapped_column(Date, nullable=True, index=True)
    doi: Mapped[str | None] = mapped_column(String(256), nullable=True)
    doi_normalized: Mapped[str | None] = mapped_column(String(256), nullable=True, index=True)
    paper_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    indexing_source: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    quartile: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    issn_isbn: Mapped[str | None] = mapped_column(String(256), nullable=True)
    approved_submission_id: Mapped[int | None] = mapped_column(ForeignKey("pending_submissions.id"), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class ReviewAction(Base):
    __tablename__ = "review_actions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    submission_id: Mapped[int] = mapped_column(ForeignKey("pending_submissions.id"), nullable=False, index=True)
    actor: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    action: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    field_changes: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))

    submission: Mapped[PendingSubmission] = relationship(back_populates="actions")


class PublicationCore(Base):
    __tablename__ = "publications_core"
    __table_args__ = (UniqueConstraint("doi_normalized", name="uq_publications_core_doi_normalized"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    faculty_name: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    title: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    authors: Mapped[str | None] = mapped_column(Text, nullable=True)
    publication_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    category: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    publication_type: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    venue: Mapped[str | None] = mapped_column(Text, nullable=True)
    conference_date: Mapped[str | None] = mapped_column(Text, nullable=True)
    national_international: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    pub_date: Mapped[datetime | None] = mapped_column(Date, nullable=True, index=True)
    doi: Mapped[str | None] = mapped_column(String(256), nullable=True)
    doi_normalized: Mapped[str | None] = mapped_column(String(256), nullable=True, index=True)
    paper_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    indexing_source: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    approved_submission_id: Mapped[int | None] = mapped_column(ForeignKey("pending_submissions.id"), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class PublicationJournalDetails(Base):
    __tablename__ = "publication_journal_details"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    publication_id: Mapped[int] = mapped_column(ForeignKey("publications_core.id"), nullable=False, unique=True, index=True)
    quartile: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    volume_issue: Mapped[str | None] = mapped_column(Text, nullable=True)
    official_venue_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    research_published_flag: Mapped[str | None] = mapped_column(String(16), nullable=True)
    indexing_flag: Mapped[str | None] = mapped_column(String(32), nullable=True)
    indexing_proof: Mapped[str | None] = mapped_column(Text, nullable=True)
    attachment_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    issn_isbn: Mapped[str | None] = mapped_column(String(256), nullable=True)


class PublicationConferenceDetails(Base):
    __tablename__ = "publication_conference_details"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    publication_id: Mapped[int] = mapped_column(ForeignKey("publications_core.id"), nullable=False, unique=True, index=True)
    presented_accepted_flag: Mapped[str | None] = mapped_column(String(16), nullable=True)
    volume_issue: Mapped[str | None] = mapped_column(Text, nullable=True)
    official_venue_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    research_published_flag: Mapped[str | None] = mapped_column(String(16), nullable=True)
    indexing_flag: Mapped[str | None] = mapped_column(String(32), nullable=True)
    indexing_proof: Mapped[str | None] = mapped_column(Text, nullable=True)
    certificate_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    attachment_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    issn_isbn: Mapped[str | None] = mapped_column(String(256), nullable=True)


class PublicationBookDetails(Base):
    __tablename__ = "publication_book_details"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    publication_id: Mapped[int] = mapped_column(ForeignKey("publications_core.id"), nullable=False, unique=True, index=True)
    publisher: Mapped[str | None] = mapped_column(Text, nullable=True)
    isbn: Mapped[str | None] = mapped_column(Text, nullable=True)
    official_venue_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    book_indexed_ugc: Mapped[str | None] = mapped_column(String(16), nullable=True)
    book_indexed_scopus: Mapped[str | None] = mapped_column(String(16), nullable=True)
    book_indexed_wos: Mapped[str | None] = mapped_column(String(16), nullable=True)
    attachment_ref: Mapped[str | None] = mapped_column(Text, nullable=True)


class PublicationSourceRow(Base):
    __tablename__ = "publication_source_rows"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    publication_id: Mapped[int] = mapped_column(ForeignKey("publications_core.id"), nullable=False, unique=True, index=True)
    source_sheet: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    source_row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    template_version: Mapped[str] = mapped_column(String(64), nullable=False, default="v1")
    import_batch_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    row_checksum: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=lambda: datetime.now(timezone.utc))


class PublicationSourceCell(Base):
    __tablename__ = "publication_source_cells"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_row_id: Mapped[int] = mapped_column(ForeignKey("publication_source_rows.id"), nullable=False, index=True)
    publication_id: Mapped[int] = mapped_column(ForeignKey("publications_core.id"), nullable=False, index=True)
    source_sheet: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    source_row_number: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    column_index: Mapped[int] = mapped_column(Integer, nullable=False)
    column_label: Mapped[str] = mapped_column(Text, nullable=False)
    raw_value: Mapped[str] = mapped_column(Text, nullable=False)


class TemplateSchemaRegistry(Base):
    __tablename__ = "template_schema_registry"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sheet_name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    column_index: Mapped[int] = mapped_column(Integer, nullable=False)
    column_label: Mapped[str] = mapped_column(Text, nullable=False)
    mapping_target: Mapped[str] = mapped_column(String(128), nullable=False)
    mapping_version: Mapped[str] = mapped_column(String(64), nullable=False, default="v1")
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
