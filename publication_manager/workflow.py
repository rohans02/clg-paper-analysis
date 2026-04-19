from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
import json
from typing import Any

from sqlalchemy import extract, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from publication_manager.enums import InputMethod, ReviewActionType, SubmissionStatus
from publication_manager.lossless import create_publication_core_from_payload, record_payload_snapshot
from publication_manager.models import PendingSubmission, Publication, PublicationCore, ReviewAction
from publication_manager.normalization import normalize_doi, parse_date


ALLOWED_TRANSITIONS = {
    SubmissionStatus.DRAFT: {SubmissionStatus.SUBMITTED},
    SubmissionStatus.SUBMITTED: {SubmissionStatus.UNDER_REVIEW, SubmissionStatus.REJECTED},
    SubmissionStatus.UNDER_REVIEW: {SubmissionStatus.APPROVED, SubmissionStatus.REJECTED},
    SubmissionStatus.APPROVED: set(),
    SubmissionStatus.REJECTED: set(),
}

REQUIRED_INSERT_FIELDS = ("faculty_name", "title", "category", "publication_type")


@dataclass
class ApprovalResult:
    publication_id: int | None
    hard_duplicate: bool
    soft_duplicate: bool
    warnings: list[str]


def _normalize_payload_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        text = " ".join(value.strip().split())
        return text or None
    return value


def _normalize_insert_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = {key: _normalize_payload_value(value) for key, value in payload.items()}
    normalized_doi = normalize_doi(normalized.get("doi"))
    if normalized_doi:
        normalized["doi"] = normalized_doi
    parsed_date = parse_date(normalized.get("pub_date"))
    if parsed_date:
        normalized["pub_date"] = parsed_date.isoformat()
    return normalized


def _missing_required_fields(payload: dict[str, Any]) -> list[str]:
    return [field for field in REQUIRED_INSERT_FIELDS if not payload.get(field)]


def _build_insertion_fingerprint(payload: dict[str, Any]) -> str:
    parsed_date = parse_date(payload.get("pub_date"))
    canonical = {
        "faculty_name": str(payload.get("faculty_name") or "").strip().lower(),
        "title": str(payload.get("title") or "").strip().lower(),
        "category": str(payload.get("category") or "").strip().lower(),
        "publication_type": str(payload.get("publication_type") or "").strip().lower(),
        "doi_normalized": normalize_doi(payload.get("doi")),
        "pub_year": parsed_date.year if parsed_date else None,
    }
    encoded = json.dumps(canonical, sort_keys=True, ensure_ascii=True)
    return sha256(encoded.encode("utf-8")).hexdigest()


def _find_existing_submission_by_fingerprint(
    session: Session,
    submitted_by: str,
    fingerprint: str,
    include_drafts: bool,
) -> PendingSubmission | None:
    active_statuses = [SubmissionStatus.SUBMITTED.value, SubmissionStatus.UNDER_REVIEW.value]
    if include_drafts:
        active_statuses.append(SubmissionStatus.DRAFT.value)

    rows = session.execute(
        select(PendingSubmission)
        .where(PendingSubmission.submitted_by == submitted_by)
        .where(PendingSubmission.status.in_(active_statuses))
        .order_by(PendingSubmission.created_at.desc())
    ).scalars()
    for row in rows:
        payload = row.parsed_payload_json or {}
        if payload.get("_insertion_fingerprint") == fingerprint:
            return row
    return None


def _ensure_transition(current: SubmissionStatus, target: SubmissionStatus) -> None:
    if target not in ALLOWED_TRANSITIONS.get(current, set()):
        raise ValueError(f"Invalid status transition: {current.value} -> {target.value}")


def _log_action(
    session: Session,
    submission_id: int,
    actor: str,
    action: ReviewActionType,
    note: str | None = None,
    field_changes: dict[str, Any] | None = None,
) -> None:
    session.add(
        ReviewAction(
            submission_id=submission_id,
            actor=actor,
            action=action.value,
            note=note,
            field_changes=field_changes,
        )
    )


def create_submission(
    session: Session,
    submitted_by: str,
    source_input: str | None,
    source_input_method: InputMethod,
    payload: dict[str, Any],
    confidence_score: float,
    as_draft: bool = False,
) -> PendingSubmission:
    normalized_payload = _normalize_insert_payload(payload)
    missing = _missing_required_fields(normalized_payload)
    if missing and not as_draft:
        raise ValueError(f"Cannot submit. Missing required fields: {', '.join(missing)}")

    fingerprint = _build_insertion_fingerprint(normalized_payload)
    normalized_payload["_insertion_fingerprint"] = fingerprint
    existing = _find_existing_submission_by_fingerprint(
        session=session,
        submitted_by=submitted_by,
        fingerprint=fingerprint,
        include_drafts=as_draft,
    )
    if existing is not None:
        return existing

    status = SubmissionStatus.DRAFT if as_draft else SubmissionStatus.SUBMITTED
    submission = PendingSubmission(
        submitted_by=submitted_by,
        status=status.value,
        source_input=source_input,
        source_input_method=source_input_method.value,
        parsed_payload_json=normalized_payload,
        confidence_score=confidence_score,
    )
    session.add(submission)
    session.flush()
    _log_action(session, submission.id, submitted_by, ReviewActionType.CREATED)
    if status == SubmissionStatus.SUBMITTED:
        _log_action(session, submission.id, submitted_by, ReviewActionType.SUBMITTED)
    return submission


def start_review(session: Session, submission_id: int, admin_user: str) -> PendingSubmission:
    submission = session.get(PendingSubmission, submission_id)
    if not submission:
        raise ValueError("Submission not found")
    current = SubmissionStatus(submission.status)
    _ensure_transition(current, SubmissionStatus.UNDER_REVIEW)
    submission.status = SubmissionStatus.UNDER_REVIEW.value
    submission.reviewed_by = admin_user
    submission.reviewed_at = datetime.now(timezone.utc)
    submission.updated_at = datetime.now(timezone.utc)
    _log_action(session, submission.id, admin_user, ReviewActionType.START_REVIEW)
    return submission


def _find_hard_duplicate(session: Session, doi_normalized: str | None) -> bool:
    if not doi_normalized:
        return False
    stmt = select(Publication.id).where(Publication.doi_normalized == doi_normalized).limit(1)
    if session.execute(stmt).scalar_one_or_none() is not None:
        return True
    core_stmt = select(PublicationCore.id).where(PublicationCore.doi_normalized == doi_normalized).limit(1)
    return session.execute(core_stmt).scalar_one_or_none() is not None


def _find_soft_duplicate(session: Session, payload: dict[str, Any]) -> bool:
    title = payload.get("title")
    faculty = payload.get("faculty_name")
    parsed_date = parse_date(payload.get("pub_date"))
    if not title or not faculty or not parsed_date:
        return False
    stmt = (
        select(Publication.id)
        .where(Publication.title == title)
        .where(Publication.faculty_name == faculty)
        .where(extract("year", Publication.pub_date) == parsed_date.year)
        .limit(1)
    )
    if session.execute(stmt).scalar_one_or_none() is not None:
        return True
    core_stmt = (
        select(PublicationCore.id)
        .where(PublicationCore.title == title)
        .where(PublicationCore.faculty_name == faculty)
        .where(extract("year", PublicationCore.pub_date) == parsed_date.year)
        .limit(1)
    )
    return session.execute(core_stmt).scalar_one_or_none() is not None


def approve_submission(
    session: Session,
    submission_id: int,
    admin_user: str,
    review_note: str | None = None,
    edited_payload: dict[str, Any] | None = None,
    override_soft_duplicate: bool = False,
) -> ApprovalResult:
    submission = session.get(PendingSubmission, submission_id)
    if not submission:
        raise ValueError("Submission not found")

    current = SubmissionStatus(submission.status)
    if current == SubmissionStatus.APPROVED:
        existing_publication_id = session.execute(
            select(Publication.id).where(Publication.approved_submission_id == submission.id).limit(1)
        ).scalar_one_or_none()
        return ApprovalResult(
            publication_id=existing_publication_id,
            hard_duplicate=False,
            soft_duplicate=False,
            warnings=["Submission already approved; existing publication returned."],
        )

    if current == SubmissionStatus.SUBMITTED:
        start_review(session, submission_id, admin_user)
        submission = session.get(PendingSubmission, submission_id)

    current = SubmissionStatus(submission.status)
    _ensure_transition(current, SubmissionStatus.APPROVED)

    payload = dict(submission.parsed_payload_json or {})
    payload.pop("_insertion_fingerprint", None)
    if edited_payload:
        payload.update({k: v for k, v in edited_payload.items() if v is not None})
    payload = _normalize_insert_payload(payload)
    doi_normalized = normalize_doi(payload.get("doi"))
    hard_duplicate = _find_hard_duplicate(session, doi_normalized)
    if hard_duplicate:
        return ApprovalResult(
            publication_id=None,
            hard_duplicate=True,
            soft_duplicate=False,
            warnings=["Hard duplicate found by DOI; approval blocked."],
        )

    soft_duplicate = _find_soft_duplicate(session, payload)
    if soft_duplicate and not override_soft_duplicate:
        return ApprovalResult(
            publication_id=None,
            hard_duplicate=False,
            soft_duplicate=True,
            warnings=["Potential duplicate found (title + faculty + year). Override required."],
        )

    missing = _missing_required_fields(payload)
    if missing:
        raise ValueError(f"Cannot approve. Missing required fields: {', '.join(missing)}")

    try:
        with session.begin_nested():
            publication = Publication(
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
                quartile=payload.get("quartile"),
                issn_isbn=payload.get("issn_isbn"),
                approved_submission_id=submission.id,
            )
            session.add(publication)
            session.flush()

            core_publication = create_publication_core_from_payload(
                session=session,
                payload=payload,
                approved_submission_id=submission.id,
            )
            record_payload_snapshot(
                session=session,
                publication_id=core_publication.id,
                payload=payload,
                source_row_number=submission.id,
                import_batch_id=f"approval:{submission.id}",
            )
    except IntegrityError:
        return ApprovalResult(
            publication_id=None,
            hard_duplicate=True,
            soft_duplicate=False,
            warnings=["Insert blocked by uniqueness conflict (likely duplicate DOI)."],
        )

    submission.status = SubmissionStatus.APPROVED.value
    submission.review_note = review_note
    submission.reviewed_by = admin_user
    submission.reviewed_at = datetime.now(timezone.utc)
    submission.updated_at = datetime.now(timezone.utc)
    submission.parsed_payload_json = payload

    _log_action(
        session,
        submission.id,
        admin_user,
        ReviewActionType.APPROVED,
        note=review_note,
        field_changes=edited_payload or None,
    )

    warnings: list[str] = []
    if soft_duplicate:
        warnings.append("Soft duplicate was overridden by admin.")
    return ApprovalResult(
        publication_id=publication.id,
        hard_duplicate=False,
        soft_duplicate=soft_duplicate,
        warnings=warnings,
    )


def reject_submission(session: Session, submission_id: int, admin_user: str, review_note: str) -> PendingSubmission:
    submission = session.get(PendingSubmission, submission_id)
    if not submission:
        raise ValueError("Submission not found")

    current = SubmissionStatus(submission.status)
    if current == SubmissionStatus.SUBMITTED:
        start_review(session, submission_id, admin_user)
        submission = session.get(PendingSubmission, submission_id)
        current = SubmissionStatus(submission.status)

    _ensure_transition(current, SubmissionStatus.REJECTED)
    submission.status = SubmissionStatus.REJECTED.value
    submission.review_note = review_note
    submission.reviewed_by = admin_user
    submission.reviewed_at = datetime.now(timezone.utc)
    submission.updated_at = datetime.now(timezone.utc)
    _log_action(session, submission.id, admin_user, ReviewActionType.REJECTED, note=review_note)
    return submission
