from __future__ import annotations

from enum import Enum


class SubmissionStatus(str, Enum):
    DRAFT = "DRAFT"
    SUBMITTED = "SUBMITTED"
    UNDER_REVIEW = "UNDER_REVIEW"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class ReviewActionType(str, Enum):
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    START_REVIEW = "START_REVIEW"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    UPDATED = "UPDATED"


class InputMethod(str, Enum):
    URL = "URL"
    DOI = "DOI"
    MANUAL = "MANUAL"
    MIGRATION = "MIGRATION"
