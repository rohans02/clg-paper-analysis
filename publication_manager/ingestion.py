from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
import ipaddress
import json
import re
import socket
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from publication_manager.normalization import normalize_doi, parse_date, score_confidence

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover - optional in runtime
    PdfReader = None


MAX_FETCH_BYTES = 5 * 1024 * 1024
MAX_REDIRECTS = 5
_USER_AGENT = {"User-Agent": "Mozilla/5.0 (compatible; FacultyPublicationManager/1.0)"}
_REDIRECT_CODES = {301, 302, 303, 307, 308}


class UnsafeUrlError(ValueError):
    """The link points somewhere the server must not fetch."""


@dataclass
class IngestionResult:
    payload: dict[str, Any]
    confidence_score: float
    warnings: list[str]
    method_trace: list[str]


@dataclass
class FetchedResource:
    url: str
    content: bytes
    encoding: str | None

    @property
    def text(self) -> str:
        return self.content.decode(self.encoding or "utf-8", errors="replace")


def validate_public_url(url: str) -> str:
    """Allow only http(s) links that resolve to public addresses.

    The paper link is user input fetched by the server, so without this check a
    visitor could make the app read internal services or cloud metadata endpoints.
    """
    cleaned = (url or "").strip()
    parsed = urlparse(cleaned)
    if parsed.scheme not in ("http", "https"):
        raise UnsafeUrlError("Only http and https links are supported.")
    host = parsed.hostname
    if not host:
        raise UnsafeUrlError("The link has no host name.")
    if parsed.username or parsed.password:
        raise UnsafeUrlError("Links with embedded credentials are not supported.")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise UnsafeUrlError("Could not resolve the link's host name.") from exc
    if not infos:
        raise UnsafeUrlError("Could not resolve the link's host name.")
    for info in infos:
        address = ipaddress.ip_address(info[4][0])
        if not address.is_global:
            raise UnsafeUrlError("Links to private or internal addresses are not allowed.")
    return cleaned


def safe_get(url: str, timeout: float = 10) -> FetchedResource:
    """GET a public URL, re-validating every redirect hop and capping the body size."""
    current = url
    for _ in range(MAX_REDIRECTS + 1):
        current = validate_public_url(current)
        response = requests.get(current, timeout=timeout, headers=_USER_AGENT, allow_redirects=False, stream=True)
        try:
            if response.status_code in _REDIRECT_CODES:
                location = response.headers.get("Location")
                if not location:
                    raise UnsafeUrlError("The link redirected without a destination.")
                current = urljoin(current, location)
                continue
            response.raise_for_status()
            chunks: list[bytes] = []
            size = 0
            for chunk in response.iter_content(64 * 1024):
                size += len(chunk)
                if size > MAX_FETCH_BYTES:
                    raise UnsafeUrlError("The linked page is too large to read.")
                chunks.append(chunk)
            return FetchedResource(url=current, content=b"".join(chunks), encoding=response.encoding)
        finally:
            response.close()
    raise UnsafeUrlError("The link redirected too many times.")


def extract_from_doi_text(text: str | None) -> str | None:
    if not text:
        return None
    return normalize_doi(text)


def extract_html_metadata(url: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    fetched = safe_get(url, timeout=10)
    soup = BeautifulSoup(fetched.text, "html.parser")
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
        result["doi"] = extract_from_doi_text(fetched.text)
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
    fetched = safe_get(url, timeout=15)
    reader = PdfReader(BytesIO(fetched.content))
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


CROSSREF_API = "https://api.crossref.org/works/"
_CROSSREF_TYPES = {
    "journal-article": "Journal",
    "proceedings-article": "Conference",
    "book-chapter": "Book Chapter",
    "book-section": "Book Chapter",
    "book-part": "Book Chapter",
}


def resolve_doi_metadata(doi: str) -> dict[str, Any]:
    """Look a DOI up on Crossref and return title, authors, venue, date, type, ids."""
    normalized = normalize_doi(doi)
    if not normalized:
        return {}
    fetched = safe_get(CROSSREF_API + normalized, timeout=10)
    message = json.loads(fetched.text).get("message") or {}
    result: dict[str, Any] = {"doi": normalized}

    titles = message.get("title") or []
    if titles:
        result["title"] = " ".join(str(titles[0]).split())

    authors = []
    for author in message.get("author") or []:
        name = " ".join(part for part in (author.get("given"), author.get("family")) if part)
        if not name and author.get("name"):
            name = author["name"]
        if name:
            authors.append(name)
    if authors:
        result["authors"] = ", ".join(authors)

    containers = message.get("container-title") or []
    if containers:
        result["venue"] = str(containers[0]).strip()
        result["publication_name"] = result["venue"]

    for key in ("published-print", "published-online", "issued", "created"):
        parts = ((message.get(key) or {}).get("date-parts") or [[]])[0]
        if parts and parts[0]:
            year = int(parts[0])
            month = int(parts[1]) if len(parts) > 1 and parts[1] else 1
            day = int(parts[2]) if len(parts) > 2 and parts[2] else 1
            result["pub_date"] = f"{year:04d}-{month:02d}-{day:02d}"
            break

    ptype = _CROSSREF_TYPES.get(str(message.get("type") or ""))
    if ptype:
        result["publication_type"] = ptype

    if message.get("publisher"):
        result["publisher"] = str(message["publisher"]).strip()
    issn = message.get("ISSN") or []
    isbn = message.get("ISBN") or []
    if issn:
        result["issn_isbn"] = str(issn[0])
    elif isbn:
        result["issn_isbn"] = str(isbn[0])
    volume = message.get("volume")
    issue = message.get("issue")
    if volume or issue:
        result["volume_issue"] = ", ".join(part for part in (f"Vol. {volume}" if volume else "", f"Issue {issue}" if issue else "") if part)
    if message.get("URL"):
        result["paper_url"] = str(message["URL"])
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


def _friendly_fetch_error(exc: Exception) -> str:
    if isinstance(exc, UnsafeUrlError):
        return str(exc)
    if isinstance(exc, requests.exceptions.Timeout):
        return "The site took too long to respond."
    if isinstance(exc, requests.exceptions.HTTPError):
        return "The site refused the request or the page does not exist."
    if isinstance(exc, requests.exceptions.RequestException):
        return "The link could not be opened."
    return "The page could not be read."


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
        try:
            crossref = resolve_doi_metadata(source_input)
            payload.update({k: v for k, v in crossref.items() if v})
            method_trace.append("crossref")
        except Exception as exc:
            warnings.append(f"Could not look the DOI up: {_friendly_fetch_error(exc)}")
    elif input_method == "URL":
        try:
            html_meta = extract_html_metadata(source_input)
            payload.update({k: v for k, v in html_meta.items() if v})
            method_trace.append("html_metadata")
        except Exception as exc:
            warnings.append(f"Could not read details from the link: {_friendly_fetch_error(exc)}")
        if payload.get("doi") and not payload.get("title"):
            # The page gave a DOI but no usable metadata; Crossref usually has it.
            try:
                crossref = resolve_doi_metadata(payload["doi"])
                for key, value in crossref.items():
                    if value and not payload.get(key):
                        payload[key] = value
                method_trace.append("crossref")
            except Exception:
                pass
        if source_input.lower().endswith(".pdf") or re.search(r"pdf($|[?&])", source_input.lower()):
            try:
                pdf_meta = extract_pdf_metadata(source_input)
                for key, value in pdf_meta.items():
                    payload.setdefault(key, value)
                method_trace.append("pdf_metadata")
            except Exception as exc:
                warnings.append(f"Could not read the PDF: {_friendly_fetch_error(exc)}")

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
            warnings.append("Some details could not be filled in automatically. Please complete them below.")

    confidence = score_confidence(payload)
    return IngestionResult(payload=payload, confidence_score=confidence, warnings=warnings, method_trace=method_trace)
