from __future__ import annotations

from hashlib import sha256
import json
from typing import Any, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from publication_manager.models import (
    PublicationBookDetails,
    PublicationConferenceDetails,
    PublicationCore,
    PublicationJournalDetails,
    PublicationSourceCell,
    PublicationSourceRow,
    TemplateSchemaRegistry,
)
from publication_manager.normalization import normalize_doi, parse_date


def create_publication_core_from_payload(
    session: Session,
    payload: dict[str, Any],
    approved_submission_id: int | None,
) -> PublicationCore:
    doi_normalized = normalize_doi(payload.get("doi"))
    core = PublicationCore(
        faculty_name=payload["faculty_name"],
        title=payload["title"],
        authors=payload.get("authors"),
        publication_name=payload.get("publication_name") or payload.get("venue"),
        category=payload.get("category", "Unknown"),
        publication_type=payload.get("publication_type", "Unknown"),
        venue=payload.get("venue"),
        conference_date=payload.get("conference_date"),
        national_international=payload.get("national_international"),
        pub_date=parse_date(payload.get("pub_date")),
        doi=payload.get("doi"),
        doi_normalized=doi_normalized,
        paper_url=payload.get("paper_url"),
        indexing_source=payload.get("indexing_source"),
        approved_submission_id=approved_submission_id,
    )
    session.add(core)
    session.flush()
    _insert_publication_details(session, core.id, payload)
    return core


def _insert_publication_details(session: Session, publication_id: int, payload: dict[str, Any]) -> None:
    publication_type = (payload.get("publication_type") or "").strip()

    if publication_type == "Journal":
        session.add(
            PublicationJournalDetails(
                publication_id=publication_id,
                quartile=payload.get("quartile"),
                volume_issue=payload.get("volume_issue"),
                official_venue_url=payload.get("official_venue_url"),
                research_published_flag=payload.get("research_published_flag"),
                indexing_flag=payload.get("indexing_flag"),
                indexing_proof=payload.get("indexing_proof"),
                attachment_ref=payload.get("attachment_ref"),
                issn_isbn=payload.get("issn_isbn"),
            )
        )
        return

    if publication_type == "Conference":
        session.add(
            PublicationConferenceDetails(
                publication_id=publication_id,
                presented_accepted_flag=payload.get("presented_accepted_flag"),
                volume_issue=payload.get("volume_issue"),
                official_venue_url=payload.get("official_venue_url"),
                research_published_flag=payload.get("research_published_flag"),
                indexing_flag=payload.get("indexing_flag"),
                indexing_proof=payload.get("indexing_proof"),
                certificate_ref=payload.get("certificate_ref"),
                attachment_ref=payload.get("attachment_ref"),
                issn_isbn=payload.get("issn_isbn"),
            )
        )
        return

    if publication_type == "Book Chapter":
        session.add(
            PublicationBookDetails(
                publication_id=publication_id,
                publisher=payload.get("publisher"),
                isbn=payload.get("issn_isbn") or payload.get("isbn"),
                official_venue_url=payload.get("official_venue_url"),
                book_indexed_ugc=payload.get("book_indexed_ugc"),
                book_indexed_scopus=payload.get("book_indexed_scopus"),
                book_indexed_wos=payload.get("book_indexed_wos"),
                attachment_ref=payload.get("attachment_ref"),
            )
        )


def ensure_template_registry(
    session: Session,
    sheet_name: str,
    headers: Sequence[str],
    template_version: str = "v1",
    mapping_targets: dict[int, str] | None = None,
) -> None:
    for index, header in enumerate(headers, start=1):
        label = str(header).strip() if header is not None else ""
        if not label:
            label = f"column_{index}"
        existing = session.execute(
            select(TemplateSchemaRegistry)
            .where(TemplateSchemaRegistry.sheet_name == sheet_name)
            .where(TemplateSchemaRegistry.column_index == index)
            .where(TemplateSchemaRegistry.mapping_version == template_version)
            .limit(1)
        ).scalar_one_or_none()
        mapping_target = "unmapped"
        if mapping_targets and index in mapping_targets:
            mapping_target = mapping_targets[index]
        if existing is None:
            session.add(
                TemplateSchemaRegistry(
                    sheet_name=sheet_name,
                    column_index=index,
                    column_label=label,
                    mapping_target=mapping_target,
                    mapping_version=template_version,
                    active=True,
                )
            )
        else:
            if mapping_target != "unmapped" and existing.mapping_target != mapping_target:
                existing.mapping_target = mapping_target
            if not existing.column_label and label:
                existing.column_label = label


def record_source_row_and_cells(
    session: Session,
    publication_id: int,
    source_sheet: str,
    source_row_number: int,
    row_values: Sequence[Any],
    headers: Sequence[str] | None,
    import_batch_id: str,
    template_version: str = "v1",
) -> None:
    normalized = ["" if value is None else str(value) for value in row_values]
    row_checksum = sha256(json.dumps(normalized, ensure_ascii=True).encode("utf-8")).hexdigest()

    source_row = PublicationSourceRow(
        publication_id=publication_id,
        source_sheet=source_sheet,
        source_row_number=source_row_number,
        template_version=template_version,
        import_batch_id=import_batch_id,
        row_checksum=row_checksum,
    )
    session.add(source_row)
    session.flush()

    for index, value in enumerate(row_values, start=1):
        if headers and len(headers) >= index:
            label = str(headers[index - 1]).strip() or f"column_{index}"
        else:
            label = f"column_{index}"
        session.add(
            PublicationSourceCell(
                source_row_id=source_row.id,
                publication_id=publication_id,
                source_sheet=source_sheet,
                source_row_number=source_row_number,
                column_index=index,
                column_label=label,
                raw_value="" if value is None else str(value),
            )
        )


def record_payload_snapshot(
    session: Session,
    publication_id: int,
    payload: dict[str, Any],
    source_row_number: int,
    import_batch_id: str,
) -> None:
    keys = list(payload.keys())
    values = [payload[key] for key in keys]
    record_source_row_and_cells(
        session=session,
        publication_id=publication_id,
        source_sheet="approved_submission_payload",
        source_row_number=source_row_number,
        row_values=values,
        headers=keys,
        import_batch_id=import_batch_id,
        template_version="v1",
    )
