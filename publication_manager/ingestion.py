from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any

import requests
from bs4 import BeautifulSoup

from publication_manager.normalization import normalize_doi, parse_date, score_confidence

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover - optional in runtime
    PdfReader = None


@dataclass
class IngestionResult:
    payload: dict[str, Any]
    confidence_score: float
    warnings: list[str]
    method_trace: list[str]


def extract_from_doi_text(text: str | None) -> str | None:
    if not text:
        return None
    return normalize_doi(text)


def extract_html_metadata(url: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    response = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    title = soup.find("meta", attrs={"name": "citation_title"}) or soup.find("title")
    if title:
        result["title"] = title.get("content") if title.has_attr("content") else title.get_text(strip=True)
    authors = soup.find_all("meta", attrs={"name": "citation_author"})
    if authors:
        result["authors"] = ", ".join(a.get("content", "").strip() for a in authors if a.get("content"))
    doi = soup.find("meta", attrs={"name": "citation_doi"})
    if doi and doi.get("content"):
        result["doi"] = normalize_doi(doi.get("content"))
    if not result.get("doi"):
        result["doi"] = extract_from_doi_text(response.text)
    publication_date = soup.find("meta", attrs={"name": "citation_publication_date"})
    if publication_date and publication_date.get("content"):
        result["pub_date"] = parse_date(publication_date.get("content"))
    venue = soup.find("meta", attrs={"name": "citation_journal_title"}) or soup.find(
        "meta",
        attrs={"name": "citation_conference_title"},
    )
    if venue and venue.get("content"):
        result["venue"] = venue.get("content").strip()
    return result


def extract_pdf_metadata(url: str) -> dict[str, Any]:
    if PdfReader is None:
        return {}
    response = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    content = response.content
    from io import BytesIO

    reader = PdfReader(BytesIO(content))
    result: dict[str, Any] = {}
    metadata = reader.metadata or {}
    if metadata.get("/Title"):
        result["title"] = str(metadata.get("/Title")).strip()
    if metadata.get("/Author"):
        result["authors"] = str(metadata.get("/Author")).strip()
    text = ""
    if reader.pages:
        text = (reader.pages[0].extract_text() or "")[:5000]
    if text:
        doi = extract_from_doi_text(text)
        if doi:
            result["doi"] = doi
        first_line = next((line.strip() for line in text.splitlines() if line.strip()), "")
        if first_line and not result.get("title"):
            result["title"] = first_line
    return result


def ai_fallback_enrich(payload: dict[str, Any]) -> dict[str, Any]:
    # Placeholder for optional AI enrichment integration.
    return payload


def infer_publication_type(payload: dict[str, Any]) -> str:
    venue = (payload.get("venue") or "").lower()
    if "conference" in venue:
        return "Conference"
    if "book" in venue or "chapter" in venue:
        return "Book Chapter"
    return "Journal"


def ingest_source(
    source_input: str,
    input_method: str,
    faculty_name: str | None = None,
    manual_overrides: dict[str, Any] | None = None,
) -> IngestionResult:
    payload: dict[str, Any] = {
        "faculty_name": faculty_name,
        "source_input": source_input,
        "paper_url": source_input if input_method == "URL" else None,
        "category": "Scopus",
    }
    warnings: list[str] = []
    method_trace: list[str] = []

    if input_method == "DOI":
        payload["doi"] = normalize_doi(source_input)
        method_trace.append("doi_direct")
    elif input_method == "URL":
        try:
            html_meta = extract_html_metadata(source_input)
            payload.update({k: v for k, v in html_meta.items() if v})
            method_trace.append("html_metadata")
        except Exception as exc:
            warnings.append(f"HTML metadata extraction failed: {exc}")
        if source_input.lower().endswith(".pdf") or re.search(r"pdf($|[?&])", source_input.lower()):
            try:
                pdf_meta = extract_pdf_metadata(source_input)
                for key, value in pdf_meta.items():
                    payload.setdefault(key, value)
                method_trace.append("pdf_metadata")
            except Exception as exc:
                warnings.append(f"PDF extraction failed: {exc}")

    if manual_overrides:
        payload.update({k: v for k, v in manual_overrides.items() if v not in (None, "")})
        method_trace.append("manual_override")

    payload["doi"] = normalize_doi(payload.get("doi"))
    payload["pub_date"] = parse_date(payload.get("pub_date"))
    payload["publication_type"] = payload.get("publication_type") or infer_publication_type(payload)
    payload["category"] = payload.get("category") or "Scopus"

    required_missing = [k for k in ("title", "faculty_name", "category", "publication_type") if not payload.get(k)]
    if required_missing:
        payload = ai_fallback_enrich(payload)
        method_trace.append("ai_fallback")
        required_missing = [k for k in ("title", "faculty_name", "category", "publication_type") if not payload.get(k)]
        if required_missing:
            warnings.append(f"Required fields still missing after fallback: {', '.join(required_missing)}")

    confidence = score_confidence(payload)
    return IngestionResult(payload=payload, confidence_score=confidence, warnings=warnings, method_trace=method_trace)

