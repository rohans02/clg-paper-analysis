from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

DOI_PATTERN = re.compile(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", re.IGNORECASE)


def normalize_doi(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.strip().lower()
    cleaned = cleaned.replace("https://doi.org/", "").replace("http://doi.org/", "").replace("doi:", "")
    match = DOI_PATTERN.search(cleaned)
    if not match:
        return None
    return match.group(1).rstrip(".")


def parse_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, (int, float)) and 1900 <= value <= 2100:
        return date(int(value), 1, 1)
    as_str = str(value).strip()
    patterns = [
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y/%m/%d",
        "%Y",
    ]
    for pattern in patterns:
        try:
            parsed = datetime.strptime(as_str, pattern)
            return parsed.date()
        except ValueError:
            continue
    return None


def score_confidence(payload: dict) -> float:
    score = 0.0
    checks = [
        payload.get("title"),
        payload.get("faculty_name"),
        payload.get("category"),
        payload.get("publication_type"),
        payload.get("paper_url") or payload.get("source_input"),
    ]
    score += sum(0.15 for item in checks if item)
    if payload.get("doi"):
        score += 0.1
    if payload.get("authors"):
        score += 0.1
    if payload.get("venue"):
        score += 0.1
    return round(min(score, 1.0), 2)

