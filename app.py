from __future__ import annotations

from datetime import date, datetime, timezone
import hmac
import logging
import os
from pathlib import Path
import re
from tempfile import NamedTemporaryFile
from typing import Any
import uuid

import altair as alt
import pandas as pd
import streamlit as st
from streamlit import column_config
from sqlalchemy import delete, distinct, select

from publication_manager.auth import (
    ADMIN_THROTTLE,
    process_secret,
    revoke_session,
    sign_session,
    verify_session,
)
from publication_manager.db import init_db, session_scope
from publication_manager.enums import InputMethod, SubmissionStatus
from publication_manager.exporter import export_official_format_xlsx
from publication_manager.ingestion import ingest_source
from publication_manager.lossless import insert_publication_details
from publication_manager.migration import MIGRATION_STATUS_PATH, rebuild_publications_from_excel
from publication_manager.models import (
    PendingSubmission,
    Publication,
    PublicationBookDetails,
    PublicationConferenceDetails,
    PublicationCore,
    PublicationJournalDetails,
    PublicationSourceCell,
    PublicationSourceRow,
    ReviewAction,
)
from publication_manager.normalization import normalize_doi, parse_date
from publication_manager.query import (
    PublicationFilters,
    get_dashboard_metrics,
    get_faculty_analysis_df,
    get_faculty_drilldown,
    get_publications_df,
)
from publication_manager.system_checks import export_system_checks_xlsx, run_system_checks
from publication_manager.taxonomy import (
    ALLOWED_CATEGORIES,
    PUBLICATION_TYPES,
    category_from_label,
    category_label,
    category_type_options,
    is_official_pair,
)
from publication_manager.workflow import approve_submission, create_submission, reject_submission, start_review


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = "publication_manager.db"
DEFAULT_EXCEL = "Faculty Publications,A.Y. 2025-26,SEM-I & II.xlsx"
APP_LOG_PATH = "app.log"
ADMIN_PASSWORD_ENV = "APP_ADMIN_PASSWORD"
DB_PATH_ENV = "APP_DB_PATH"
TEMPLATE_PATH_ENV = "APP_TEMPLATE_PATH"
LOG_PATH_ENV = "APP_LOG_PATH"
FACULTY_PASSCODE_ENV = "APP_FACULTY_PASSCODE"
AUTH_SECRET_ENV = "APP_AUTH_SECRET"

APP_NAME = "Faculty Publication Manager"
NEW_FACULTY_OPTION = "Add my name"
FACULTY_PREFIX_OPTIONS = ["Dr.", "Dr. Ms.", "Dr. Mrs.", "Dr. Mr.", "Ms.", "Mr.", "Mrs.", "Prof.", "Other"]

_NAT_INT_OPTIONS = ["", "National", "International"]
_YESNO_OPTIONS = ["", "Yes", "No"]
_QUARTILE_OPTIONS = ["", "Q1", "Q2", "Q3", "Q4"]
_PRESENTED_OPTIONS = ["", "Presented", "Accepted"]
_ACCEPTED_OPTIONS = ["", "Yes", "No"]
_INDEXING_OPTIONS = ["", "Scopus", "WoS", "UGC Care", "Peer Reviewed", "International Conference", "National Conference", "Book Chapter", "Other"]
_PROOF_FILE_TYPES = ["pdf", "png", "jpg", "jpeg", "webp"]
UPLOADS_DIR = Path(__file__).resolve().parent / "uploads"

STATUS_LABELS = {
    SubmissionStatus.DRAFT.value: "Draft",
    SubmissionStatus.SUBMITTED.value: "Awaiting review",
    SubmissionStatus.UNDER_REVIEW.value: "Under review",
    SubmissionStatus.APPROVED.value: "Approved",
    SubmissionStatus.REJECTED.value: "Rejected",
}

ACTION_LABELS = {
    "CREATED": "Created",
    "SUBMITTED": "Submitted for review",
    "START_REVIEW": "Review started",
    "APPROVED": "Approved",
    "REJECTED": "Rejected",
    "UPDATED": "Updated",
}

# Column order and labels for the publications table. Columns not listed are hidden.
PUBLICATION_COLUMNS: dict[str, str] = {
    "id": "ID",
    "faculty_name": "Faculty",
    "title": "Title",
    "publication_type": "Type",
    "category": "Indexed in",
    "publication_name": "Journal / Conference / Book",
    "pub_date": "Published on",
    "authors": "Authors",
    "doi": "DOI",
    "paper_url": "Link",
}

DETAIL_LABELS: dict[str, str] = {
    **PUBLICATION_COLUMNS,
    "venue": "Venue",
    "conference_date": "Conference date",
    "national_international": "National / International",
    "quartile": "Quartile",
    "issn_isbn": "ISSN / ISBN",
    "publisher": "Publisher",
    "volume_issue": "Volume / Issue",
    "official_venue_url": "Official page",
    "presented_accepted_flag": "Accepted / Presented",
    "research_published_flag": "Published",
    "indexing_flag": "Indexed",
    "indexing_proof": "Indexing proof",
    "certificate_ref": "Certificate",
    "attachment_ref": "Attachment",
    "book_indexed_ugc": "Indexed in UGC",
    "book_indexed_scopus": "Indexed in Scopus",
    "book_indexed_wos": "Indexed in WoS",
}
DETAIL_HIDDEN = {"id", "approved_submission_id", "indexing_source", "isbn", "doi_normalized", "created_at", "updated_at"}

FACULTY_TABLE_COLUMNS = {
    "faculty_name": "Faculty",
    "total_publications": "Total",
    "journal_count": "Journals",
    "conference_count": "Conferences",
    "book_chapter_count": "Book chapters",
    "pending_count": "Awaiting review",
}

SKIP_REASON_LABELS = {
    "empty_row": "Empty rows",
    "missing_required": "Rows with no faculty name or title (usually blank template rows)",
    "unsupported_sheet": "Sheets that are not part of the official layout",
    "invalid_serial": "Rows with an invalid serial number",
    "title_looks_like_date": "Rows whose title looked like a date range",
}


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _save_uploaded_file(uploaded_file) -> str | None:
    """Save an uploaded file under a server-generated name and return its path."""
    if uploaded_file is None:
        return None
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    ext = Path(uploaded_file.name).suffix.lower()
    file_path = UPLOADS_DIR / f"{uuid.uuid4().hex}{ext}"
    file_path.write_bytes(uploaded_file.getvalue())
    return str(file_path)


def _dropdown(container, label: str, options: list[str], current: str, key: str, help: str | None = None) -> str:
    current = current or ""
    idx = options.index(current) if current in options else 0
    return container.selectbox(label, options, index=idx, key=key, help=help)


def _secret_or_env(secret_key: str, env_key: str) -> str | None:
    try:
        value = st.secrets.get(secret_key)
        if value:
            return str(value)
    except Exception:
        pass
    env_value = os.getenv(env_key)
    return env_value if env_value else None


def _resolve_runtime_paths() -> tuple[str, str, str]:
    base_dir = Path(__file__).resolve().parent
    db_path = _secret_or_env("DB_PATH", DB_PATH_ENV) or str((base_dir / "publication_manager.db").resolve())
    template_path = _secret_or_env("TEMPLATE_PATH", TEMPLATE_PATH_ENV) or str((base_dir / DEFAULT_EXCEL).resolve())
    log_path = _secret_or_env("LOG_PATH", LOG_PATH_ENV) or str((base_dir / "app.log").resolve())
    return db_path, template_path, log_path


def _setup_logging() -> None:
    logger = logging.getLogger("publication_manager")
    if logger.handlers:
        return
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    try:
        log_file = Path(APP_LOG_PATH)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception:
        logger.warning("File logging unavailable; continuing with stdout logging only.")


def _log_info(message: str) -> None:
    logging.getLogger("publication_manager").info(message)


def _log_error(message: str) -> None:
    logging.getLogger("publication_manager").error(message)


def _status_label(status: str | None) -> str:
    return STATUS_LABELS.get(status or "", status or "")


def _as_date(value: Any) -> date | None:
    parsed = parse_date(value)
    return parsed


def _display_publications(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder, relabel and prettify a publications DataFrame for on-screen tables."""
    if df.empty:
        return df
    columns = [c for c in PUBLICATION_COLUMNS if c in df.columns]
    shown = df[columns].copy()
    if "category" in shown.columns:
        shown["category"] = shown["category"].map(category_label)
    return shown.rename(columns=PUBLICATION_COLUMNS)


def _publication_table_config() -> dict[str, Any]:
    return {
        "Link": column_config.LinkColumn("Link", display_text="Open"),
        "Published on": column_config.DateColumn("Published on"),
        "ID": column_config.NumberColumn("ID", width="small"),
        "Title": column_config.TextColumn("Title", width="large"),
    }


def _table_key(name: str) -> str:
    """Widget key for a selectable table; bump ``table_version`` to clear selections."""
    return f"{name}_{st.session_state.get('table_version', 0)}"


def _bump_tables() -> None:
    """Call after rows are added or removed so stale row selections do not reopen the wrong record."""
    st.session_state["table_version"] = int(st.session_state.get("table_version", 0)) + 1


def _select_row_id(df: pd.DataFrame, event: Any) -> int | None:
    """Return the ``id`` of the single selected row in a dataframe selection event."""
    try:
        rows = list(event.selection.rows)
    except Exception:
        return None
    if not rows:
        return None
    return int(df.iloc[rows[0]]["id"])


# ---------------------------------------------------------------------------
# Session and authentication
# ---------------------------------------------------------------------------


def _init_state() -> None:
    defaults: dict[str, Any] = {
        "auth_is_authenticated": False,
        "auth_role": None,
        "auth_username": None,
        "auth_token": None,
        "ingestion_payload": {},
        "ingestion_warnings": [],
        "ingestion_confidence": 0.0,
        "ingestion_input_method": None,
        "ingestion_source_input": None,
        "ingestion_mode_last": None,
        "admin_submission_id": None,
        "migration_report": None,
        "migration_upload_bytes": None,
        "migration_upload_name": None,
        "system_checks_df": None,
        "system_checks_summary": None,
        "selected_publication_id": None,
        "publications_filter_defaulted": False,
        "table_version": 0,
        "flt_faculty": "",
        "flt_category": "",
        "flt_pub_type": "",
        "flt_indexing": "",
        "flt_national": "",
        "flt_quartile": "",
        "flt_keyword": "",
        "flt_date_from": None,
        "flt_date_to": None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

    # Restore auth from signed query params (survives page refresh). Unsigned,
    # tampered, expired or revoked parameters are ignored and removed.
    if not st.session_state.get("auth_is_authenticated"):
        qp = st.query_params
        verified = verify_session(
            _auth_secret(),
            {key: qp.get(key) for key in ("role", "user", "ts", "sig")},
        )
        if verified:
            role, user = verified
            st.session_state["auth_is_authenticated"] = True
            st.session_state["auth_role"] = role
            st.session_state["auth_username"] = user
            st.session_state["auth_token"] = {key: str(qp.get(key)) for key in ("role", "user", "ts", "sig")}
        elif any(qp.get(key) for key in ("role", "user", "ts", "sig")):
            st.query_params.clear()


def _normalize_faculty_name(name: str) -> str:
    return " ".join(name.strip().split())


_NAME_PART_PATTERN = re.compile(r"^[A-Za-z][A-Za-z.'-]*$")


def _is_valid_name_part(part: str, required: bool) -> bool:
    part = (part or "").strip()
    if not part:
        return not required
    return len(part) >= 2 and bool(_NAME_PART_PATTERN.match(part))


def _is_valid_faculty_name(prefix: str, first: str, middle: str, last: str) -> bool:
    if not prefix or not prefix.strip():
        return False
    return (
        _is_valid_name_part(first, required=True)
        and _is_valid_name_part(middle, required=False)
        and _is_valid_name_part(last, required=True)
    )


def _compose_faculty_name(prefix: str, first: str, middle: str, last: str) -> str:
    parts = [prefix, first, middle, last]
    return _normalize_faculty_name(" ".join(part.strip() for part in parts if part and part.strip()))


def _resolve_admin_password() -> str | None:
    return _secret_or_env("ADMIN_PASSWORD", ADMIN_PASSWORD_ENV)


def _resolve_faculty_passcode() -> str | None:
    """Optional shared passcode for faculty login. Unset means faculty login is open."""
    return _secret_or_env("FACULTY_PASSCODE", FACULTY_PASSCODE_ENV)


def _auth_secret() -> str:
    """Key used to sign login state stored in the URL.

    Prefers an explicit AUTH_SECRET, then the admin password (so existing deployments
    keep working), then a random per-process value (sessions then end on restart).
    """
    return _secret_or_env("AUTH_SECRET", AUTH_SECRET_ENV) or _resolve_admin_password() or process_secret()


def _client_key() -> str:
    """Identify the visitor for login throttling. Falls back to a shared bucket."""
    try:
        ip = st.context.ip_address
        if isinstance(ip, str) and ip.strip():
            return ip.strip()
    except Exception:
        pass
    try:
        forwarded = st.context.headers.get("X-Forwarded-For")
        if isinstance(forwarded, str) and forwarded.strip():
            return forwarded.split(",")[0].strip()
    except Exception:
        pass
    return "shared"


def _persist_login(role: str, username: str) -> None:
    token = sign_session(_auth_secret(), role, username)
    st.session_state["auth_is_authenticated"] = True
    st.session_state["auth_role"] = role
    st.session_state["auth_username"] = username
    st.session_state["auth_token"] = token
    st.query_params.clear()
    st.query_params.update(token)


def _keep_token_in_url() -> None:
    """Page navigation drops query parameters, and the login token lives there.

    Re-attach the stored token whenever it is missing so a browser refresh on any
    page still finds a valid sign-in instead of landing on "page not found".
    """
    token = st.session_state.get("auth_token")
    if not token:
        return
    if any(st.query_params.get(key) != token.get(key) for key in ("role", "user", "ts", "sig")):
        st.query_params.update(token)


def _logout() -> None:
    revoke_session({key: st.query_params.get(key) for key in ("role", "user", "ts", "sig")})
    if st.session_state.get("auth_token"):
        revoke_session(st.session_state["auth_token"])
    st.session_state["auth_token"] = None
    st.session_state["auth_is_authenticated"] = False
    st.session_state["auth_role"] = None
    st.session_state["auth_username"] = None
    st.session_state["publications_filter_defaulted"] = False
    st.session_state["selected_publication_id"] = None
    _reset_filters()
    st.query_params.clear()
    _log_info("User logged out.")


def _existing_faculty_names() -> list[str]:
    try:
        with session_scope(DB_PATH) as session:
            names_from_pub = session.execute(
                select(distinct(Publication.faculty_name)).where(Publication.faculty_name.is_not(None))
            ).scalars().all()
            names_from_core = session.execute(
                select(distinct(PublicationCore.faculty_name)).where(PublicationCore.faculty_name.is_not(None))
            ).scalars().all()
            return sorted(set(names_from_pub) | set(names_from_core))
    except Exception:
        return []


# Only on the sign-in page: stretch the content area to the viewport and centre it.
_SIGN_IN_CSS = """
<style>
.block-container,
[data-testid="stMainBlockContainer"] {
    min-height: 100vh !important;
    max-width: 1400px !important;
    margin: 0 auto !important;
    display: grid !important;
    align-content: center !important;
    align-items: center !important;
    justify-items: stretch !important;
    padding-top: 0 !important;
    padding-bottom: 0 !important;
}
</style>
"""


def _name_picker(key_prefix: str) -> tuple[str | None, str | None]:
    """Name selector with an "Add my name" form. Returns (name, error)."""
    faculty_options = _existing_faculty_names() + [NEW_FACULTY_OPTION]
    selected = st.selectbox("Your name", faculty_options, index=None, placeholder="Choose your name", key=f"{key_prefix}_select")
    if selected is None:
        return None, "Choose your name."
    if selected != NEW_FACULTY_OPTION:
        return selected, None

    n0, n1, n2, n3 = st.columns([1.1, 1.3, 1.3, 1.3])
    prefix_choice = n0.selectbox("Prefix", FACULTY_PREFIX_OPTIONS, key=f"{key_prefix}_prefix")
    first_name = n1.text_input("First name", key=f"{key_prefix}_first")
    middle_name = n2.text_input("Middle name", key=f"{key_prefix}_middle", placeholder="Optional")
    last_name = n3.text_input("Last name", key=f"{key_prefix}_last")
    custom_prefix = ""
    if prefix_choice == "Other":
        custom_prefix = st.text_input("Custom prefix", placeholder="e.g. Assoc. Prof.", key=f"{key_prefix}_custom_prefix")
    st.caption("Use the same spelling as in the department records, e.g. Dr. Mrs. Sankirti Sandeep Shirawale.")

    final_prefix = custom_prefix.strip() if prefix_choice == "Other" else prefix_choice
    if not _is_valid_faculty_name(final_prefix, first_name, middle_name, last_name):
        return None, "Choose a prefix and enter your first and last name (letters only, at least 2 each)."
    return _compose_faculty_name(final_prefix, first_name, middle_name, last_name), None


def _sign_in_page() -> None:
    """Landing page with the faculty and admin sign-in forms, sized to fit one screen."""
    st.markdown(_SIGN_IN_CSS, unsafe_allow_html=True)
    info, form = st.columns([1, 1.15], gap="large", vertical_alignment="center")
    with info:
        st.title(APP_NAME)
        st.write("Publication records for the department, in one place.")
        st.markdown("**Faculty**  \nSubmit publications, upload proofs, and follow their review status.")
        st.markdown("**Admin**  \nReview submissions, correct records, and download the official Excel workbook.")
        st.caption("After signing in, do not share the page address. It contains your sign-in.")

    with form:
        faculty_tab, admin_tab = st.tabs(["Faculty", "Admin"])

    with faculty_tab:
        faculty_name, name_error = _name_picker("faculty_login")

        faculty_passcode_expected = _resolve_faculty_passcode()
        faculty_passcode_input = ""
        if faculty_passcode_expected:
            faculty_passcode_input = st.text_input(
                "Department passcode",
                type="password",
                key="faculty_login_passcode",
                help="Ask the admin if you do not have it.",
            )

        if st.button("Sign in as faculty", key="faculty_login_btn", type="primary"):
            if faculty_passcode_expected and not hmac.compare_digest(faculty_passcode_input, faculty_passcode_expected):
                st.error("Incorrect passcode.")
                _log_error("Faculty login failed: wrong passcode.")
            elif name_error or not faculty_name:
                st.error(name_error or "Choose your name.")
            else:
                _persist_login("faculty", faculty_name)
                _log_info(f"Faculty login success: {faculty_name}")
                st.rerun()

    with admin_tab:
        client_key = _client_key()
        locked_seconds = ADMIN_THROTTLE.locked_for(client_key)
        if locked_seconds:
            st.warning(f"Too many failed attempts. Try again in {locked_seconds} seconds.")
        admin_password_input = st.text_input("Admin password", type="password", key="admin_login_password")
        if st.button("Sign in as admin", key="admin_login_btn", type="primary"):
            expected_password = _resolve_admin_password()
            if ADMIN_THROTTLE.locked_for(client_key):
                st.error("Admin sign-in is temporarily locked.")
            elif not expected_password:
                st.error("Admin password is not configured on this server.")
                _log_error("Admin login attempted but ADMIN_PASSWORD is not configured.")
            elif not hmac.compare_digest(admin_password_input, expected_password):
                attempts_left = ADMIN_THROTTLE.record_failure(client_key)
                _log_error(f"Admin login failed from {client_key} (attempts left {attempts_left}).")
                if attempts_left == 0:
                    st.error("Too many failed attempts. Admin sign-in is locked for 5 minutes.")
                else:
                    st.error(f"Incorrect password. {attempts_left} attempt(s) left.")
            else:
                ADMIN_THROTTLE.reset(client_key)
                _persist_login("admin", "admin")
                _log_info("Admin login success.")
                st.rerun()


# ---------------------------------------------------------------------------
# Shared widgets
# ---------------------------------------------------------------------------


def _safe_dataframe(df: pd.DataFrame, msg: str = "Nothing to show yet.", **kwargs) -> None:
    if df.empty:
        st.info(msg)
    else:
        st.dataframe(df, width="stretch", hide_index=True, **kwargs)


def _select_with_state(container, label: str, options: list[str], state_key: str, **kwargs) -> str:
    current = st.session_state.get(state_key, "")
    index = options.index(current) if current in options else 0
    value = container.selectbox(label, options, index=index, **kwargs)
    st.session_state[state_key] = value
    return value


def _build_filters(df: pd.DataFrame) -> PublicationFilters:
    def values(column: str) -> list[str]:
        if column not in df.columns:
            return []
        return sorted(df[column].dropna().astype(str).unique().tolist())

    faculty_options = [""] + values("faculty_name")
    stored_categories = values("category")
    category_labels = [""] + [category_label(c) for c in stored_categories]
    type_options = [""] + values("publication_type")

    c1, c2, c3 = st.columns(3)
    _select_with_state(c1, "Faculty", faculty_options, "flt_faculty")
    chosen_label = _select_with_state(c2, "Indexed in", category_labels, "flt_category_label")
    st.session_state["flt_category"] = category_from_label(chosen_label) if chosen_label else ""
    _select_with_state(c3, "Type", type_options, "flt_pub_type")

    st.session_state["flt_keyword"] = st.text_input(
        "Search",
        value=st.session_state["flt_keyword"],
        placeholder="Title, authors, journal, DOI",
    )

    with st.popover("More filters", width="stretch"):
        _select_with_state(st, "National / International", [""] + values("national_international"), "flt_national", key="adv_national")
        _select_with_state(st, "Quartile", [""] + values("quartile"), "flt_quartile", key="adv_quartile")
        st.session_state["flt_date_from"] = st.date_input("Published from", value=st.session_state["flt_date_from"], key="adv_date_from")
        st.session_state["flt_date_to"] = st.date_input("Published to", value=st.session_state["flt_date_to"], key="adv_date_to")
        if st.button("Clear filters", key="clear_filters_btn"):
            _reset_filters()
            st.rerun()

    return PublicationFilters(
        faculty_name=st.session_state["flt_faculty"] or None,
        category=st.session_state["flt_category"] or None,
        publication_type=st.session_state["flt_pub_type"] or None,
        national_international=st.session_state["flt_national"] or None,
        quartile=st.session_state["flt_quartile"] or None,
        keyword=st.session_state["flt_keyword"] or None,
        date_from=st.session_state["flt_date_from"] if isinstance(st.session_state["flt_date_from"], date) else None,
        date_to=st.session_state["flt_date_to"] if isinstance(st.session_state["flt_date_to"], date) else None,
    )


def _reset_filters() -> None:
    for key in ["flt_faculty", "flt_category", "flt_category_label", "flt_pub_type", "flt_national", "flt_quartile", "flt_keyword"]:
        st.session_state[key] = ""
    for key in ["flt_date_from", "flt_date_to"]:
        st.session_state[key] = None
    for widget_key in ["adv_national", "adv_quartile", "adv_date_from", "adv_date_to"]:
        st.session_state.pop(widget_key, None)


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------


def _donut_chart(df: pd.DataFrame, column: str, label: str, color_scheme: str | None = None):
    counts = df.groupby(column, dropna=True)["id"].count().reset_index(name="count")
    if column == "category":
        counts["category"] = counts["category"].map(category_label)
    color = alt.Color(f"{column}:N", legend=alt.Legend(title=None, orient="bottom", columns=3))
    if color_scheme:
        color = color.scale(scheme=color_scheme)
    return (
        alt.Chart(counts)
        .mark_arc(innerRadius=60, padAngle=0.01)
        .encode(
            theta=alt.Theta("count:Q"),
            color=color,
            tooltip=[alt.Tooltip(f"{column}:N", title=label), alt.Tooltip("count:Q", title="Publications")],
        )
        .properties(height=320, padding={"top": 10, "bottom": 10, "left": 10, "right": 10})
    )


def _by_indexing_chart(df: pd.DataFrame):
    return _donut_chart(df, "category", "Indexed in")


def _by_type_chart(df: pd.DataFrame):
    return _donut_chart(df, "publication_type", "Type", color_scheme="set2")


def _by_year_chart(by_year: pd.DataFrame, height: int = 320):
    return (
        alt.Chart(by_year)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("year:O", title=None),
            y=alt.Y("count:Q", title=None),
            color=alt.value("#1565C0"),
            tooltip=[alt.Tooltip("year:O", title="Year"), alt.Tooltip("count:Q", title="Publications")],
        )
        .properties(height=height)
    )


def _recent_table(df: pd.DataFrame, height: int | None = None) -> None:
    shown = _display_publications(df).drop(columns=["Faculty", "ID"], errors="ignore")
    if shown.empty:
        st.caption("No publications.")
        return
    kwargs = {"height": height} if height else {}
    st.dataframe(shown, width="stretch", hide_index=True, column_config=_publication_table_config(), **kwargs)


FACULTY_CHART_MIN_PUBLICATIONS = 5


def _pending_submissions_table(username: str) -> None:
    """The person's submissions still in review, if any."""
    with session_scope(DB_PATH) as session:
        rows = session.execute(
            select(PendingSubmission)
            .where(PendingSubmission.submitted_by == username)
            .where(PendingSubmission.status.in_([SubmissionStatus.SUBMITTED.value, SubmissionStatus.UNDER_REVIEW.value]))
            .order_by(PendingSubmission.created_at.desc())
        ).scalars().all()
        data = [
            {
                "Title": (row.parsed_payload_json or {}).get("title"),
                "Type": (row.parsed_payload_json or {}).get("publication_type"),
                "Status": _status_label(row.status),
                "Submitted on": row.created_at.date() if row.created_at else None,
            }
            for row in rows
        ]
    if not data:
        return
    st.markdown(f"**Awaiting review** ({len(data)})")
    st.dataframe(pd.DataFrame(data), width="stretch", hide_index=True, column_config={"Submitted on": column_config.DateColumn("Submitted on")})


def _faculty_overview(username: str) -> None:
    st.title("Overview")
    st.subheader(f"Welcome, {username}")
    with session_scope(DB_PATH) as session:
        my_df = get_publications_df(session, PublicationFilters(faculty_name=username))
        drilldown = get_faculty_drilldown(session, username)

    if my_df.empty:
        st.write("You have no publications in the list yet. Use **Submit a Publication** to add one.")
        _pending_submissions_table(username)
        return

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("My publications", drilldown["kpis"]["publications"])
    m2.metric("Journals", drilldown["kpis"]["journals"])
    m3.metric("Conferences", drilldown["kpis"]["conferences"])
    m4.metric("Book chapters", drilldown["kpis"]["book_chapters"])

    _pending_submissions_table(username)

    # Charts only become readable with a few records; below that they are noise.
    if len(my_df) >= FACULTY_CHART_MIN_PUBLICATIONS:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**My publications by type**")
            st.altair_chart(_by_type_chart(my_df), width="stretch")
        with c2:
            st.markdown("**By year**")
            if not drilldown["trend"].empty:
                st.altair_chart(_by_year_chart(drilldown["trend"]), width="stretch")
            else:
                st.caption("No dated publications yet.")

    st.markdown("**Most recent**")
    _recent_table(drilldown["latest"])
    st.caption("The full list, with filters, is under Publications.")


def _admin_overview() -> None:
    st.title("Overview")
    with session_scope(DB_PATH) as session:
        metrics = get_dashboard_metrics(session)
        faculty_df = get_faculty_analysis_df(session)

    by_year = metrics["by_year"]
    current_year = date.today().year
    this_year = int(by_year.loc[by_year["year"] == current_year, "count"].sum()) if not by_year.empty else 0
    faculty_count = int(faculty_df.loc[faculty_df["total_publications"] > 0].shape[0]) if not faculty_df.empty else 0
    c1, c2, c3 = st.columns(3)
    c1.metric("Total publications", metrics["total_publications"])
    c2.metric("Faculty with publications", faculty_count)
    c3.metric(f"Published in {current_year}", this_year)
    if metrics["pending_reviews"]:
        st.info(f"{metrics['pending_reviews']} submission(s) are waiting in the Review Queue.")

    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        st.markdown("**By indexing**")
        by_category = metrics["by_category"]
        if not by_category.empty:
            with session_scope(DB_PATH) as session:
                all_df = get_publications_df(session, PublicationFilters())
            st.altair_chart(_by_indexing_chart(all_df), width="stretch")
        else:
            st.caption("No publications yet.")
    with chart_col2:
        st.markdown("**By year**")
        if not by_year.empty:
            st.altair_chart(_by_year_chart(by_year), width="stretch")
        else:
            st.caption("No dated publications yet.")

    st.subheader("By faculty")
    if faculty_df.empty:
        st.info("No publications yet.")
        return

    columns = [c for c in FACULTY_TABLE_COLUMNS if c in faculty_df.columns]
    _safe_dataframe(faculty_df[columns].rename(columns=FACULTY_TABLE_COLUMNS))

    faculty_options = faculty_df["faculty_name"].tolist()
    selected_faculty = st.selectbox("Show details for", faculty_options, key="faculty_drilldown")
    with session_scope(DB_PATH) as session:
        drilldown = get_faculty_drilldown(session, selected_faculty)
    trend_col, latest_col = st.columns([1, 2])
    with trend_col:
        st.markdown("**Publications per year**")
        if not drilldown["trend"].empty:
            st.altair_chart(_by_year_chart(drilldown["trend"], height=220), width="stretch")
        else:
            st.caption("No dated publications.")
    with latest_col:
        st.markdown("**Most recent**")
        _recent_table(drilldown["latest"], height=220)


def _dashboard_page() -> None:
    if st.session_state["auth_role"] == "faculty":
        _faculty_overview(str(st.session_state["auth_username"]))
    else:
        _admin_overview()


# ---------------------------------------------------------------------------
# Publications
# ---------------------------------------------------------------------------


@st.dialog("Edit publication", width="large")
def _edit_publication_dialog(pub_id: int) -> None:
    with session_scope(DB_PATH) as read_session:
        core = read_session.get(PublicationCore, pub_id)
        legacy = read_session.get(Publication, pub_id)
        record = core or legacy
        if not record:
            st.error("This publication no longer exists.")
            return
        d: dict[str, Any] = {
            "title": record.title or "",
            "faculty_name": record.faculty_name or "",
            "authors": record.authors or "",
            "publication_name": record.publication_name or "",
            "doi": record.doi or "",
            "pub_date": record.pub_date,
            "paper_url": record.paper_url or "",
            "category": record.category or "",
            "publication_type": record.publication_type or "",
            "venue": record.venue or "",
            "conference_date": record.conference_date or "",
            "national_international": record.national_international or "",
            "quartile": getattr(legacy, "quartile", None) or "",
            "issn_isbn": getattr(legacy, "issn_isbn", None) or "",
        }
        journal = read_session.execute(select(PublicationJournalDetails).where(PublicationJournalDetails.publication_id == pub_id)).scalar_one_or_none()
        conference = read_session.execute(select(PublicationConferenceDetails).where(PublicationConferenceDetails.publication_id == pub_id)).scalar_one_or_none()
        book = read_session.execute(select(PublicationBookDetails).where(PublicationBookDetails.publication_id == pub_id)).scalar_one_or_none()
        for details in (journal, conference, book):
            if details is None:
                continue
            for field in (
                "quartile", "volume_issue", "official_venue_url", "research_published_flag", "indexing_flag",
                "indexing_proof", "attachment_ref", "issn_isbn", "presented_accepted_flag", "certificate_ref",
                "publisher", "isbn", "book_indexed_ugc", "book_indexed_scopus", "book_indexed_wos",
            ):
                value = getattr(details, field, None)
                if value:
                    d[field] = value
        if d.get("isbn") and not d.get("issn_isbn"):
            d["issn_isbn"] = d["isbn"]

    def text(label: str, field: str, container=st, **kwargs) -> str:
        return container.text_input(label, value=d.get(field) or "", key=f"e_{field}_{pub_id}", **kwargs)

    c1, c2 = st.columns(2)
    new: dict[str, Any] = {}
    new["title"] = text("Title", "title", c1)
    new["faculty_name"] = text("Faculty", "faculty_name", c2)
    c1, c2 = st.columns(2)
    new["authors"] = c1.text_area("Authors", value=d.get("authors") or "", key=f"e_authors_{pub_id}", height=80)
    new["publication_name"] = text("Journal / Conference / Book", "publication_name", c2)
    c1, c2 = st.columns(2)
    new["doi"] = text("DOI", "doi", c1)
    new["pub_date"] = c2.date_input("Published on", value=d.get("pub_date"), key=f"e_pub_date_{pub_id}", format="YYYY-MM-DD")
    c1, c2 = st.columns(2)
    category_options = [category_label(c) for c in sorted(ALLOWED_CATEGORIES)]
    current_category_label = category_label(d["category"])
    if current_category_label and current_category_label not in category_options:
        category_options.append(current_category_label)
    new["category"] = category_from_label(_dropdown(c1, "Indexed in", category_options, current_category_label, f"e_category_{pub_id}"))
    type_options = list(PUBLICATION_TYPES)
    if d["publication_type"] and d["publication_type"] not in type_options:
        type_options.append(d["publication_type"])
    ptype = _dropdown(c2, "Type", type_options, d["publication_type"], f"e_type_{pub_id}")
    new["publication_type"] = ptype
    if not is_official_pair(new["category"], ptype):
        st.caption("This combination has no sheet in the official workbook, so the record will be left out of the Excel download.")

    if ptype in ("Journal", "Conference"):
        new["national_international"] = _dropdown(st, "National / International", _NAT_INT_OPTIONS, d.get("national_international", ""), f"e_nat_{pub_id}")

    if ptype == "Journal":
        c1, c2 = st.columns(2)
        new["quartile"] = _dropdown(c1, "Quartile", _QUARTILE_OPTIONS, d.get("quartile", ""), f"e_quartile_{pub_id}")
        new["issn_isbn"] = text("ISSN", "issn_isbn", c2)
        c1, c2 = st.columns(2)
        new["volume_issue"] = text("Volume / Issue", "volume_issue", c1)
        new["official_venue_url"] = text("Journal website", "official_venue_url", c2)
        new["venue"] = new["publication_name"]
    elif ptype == "Conference":
        c1, c2 = st.columns(2)
        new["venue"] = text("Venue (organising institute)", "venue", c1)
        new["conference_date"] = text("Conference dates", "conference_date", c2)
        c1, c2 = st.columns(2)
        new["issn_isbn"] = text("ISSN / ISBN", "issn_isbn", c1)
        new["volume_issue"] = text("Volume / Issue", "volume_issue", c2)
        new["official_venue_url"] = text("Conference website", "official_venue_url")
    elif ptype == "Book Chapter":
        c1, c2 = st.columns(2)
        new["publisher"] = text("Publisher", "publisher", c1)
        new["issn_isbn"] = text("ISBN", "issn_isbn", c2)
        new["official_venue_url"] = text("Book website", "official_venue_url")
        new["venue"] = new["publisher"] or d.get("venue") or ""
        c1, c2, c3 = st.columns(3)
        new["book_indexed_ugc"] = _dropdown(c1, "In UGC list", _YESNO_OPTIONS, d.get("book_indexed_ugc", ""), f"e_bugc_{pub_id}")
        new["book_indexed_scopus"] = _dropdown(c2, "In Scopus", _YESNO_OPTIONS, d.get("book_indexed_scopus", ""), f"e_bsc_{pub_id}")
        new["book_indexed_wos"] = _dropdown(c3, "In WoS", _YESNO_OPTIONS, d.get("book_indexed_wos", ""), f"e_bwos_{pub_id}")

    st.markdown("**Status and proofs**")
    step1_label = "Presented or accepted?" if ptype == "Conference" else "Accepted?"
    step1_options = _PRESENTED_OPTIONS if ptype == "Conference" else _ACCEPTED_OPTIONS
    s1, s2, s3 = st.columns(3)
    new["presented_accepted_flag"] = _dropdown(s1, step1_label, step1_options, d.get("presented_accepted_flag", ""), f"e_acc_{pub_id}")
    new["research_published_flag"] = _dropdown(s2, "Published?", _YESNO_OPTIONS, d.get("research_published_flag", ""), f"e_pub_{pub_id}")
    new["indexing_flag"] = _dropdown(s3, "Indexed?", _YESNO_OPTIONS, d.get("indexing_flag", ""), f"e_idx_{pub_id}")
    new["paper_url"] = text("Link to the published paper", "paper_url")
    c1, c2, c3 = st.columns(3)
    new["certificate_ref"] = text("Acceptance proof (link or file path)", "certificate_ref", c1)
    new["attachment_ref"] = text("Publication proof (link or file path)", "attachment_ref", c2)
    new["indexing_proof"] = text("Indexing proof (link or file path)", "indexing_proof", c3)

    if st.button("Save", type="primary", key=f"e_save_{pub_id}"):
        payload = {k: (v.strip() if isinstance(v, str) else v) for k, v in new.items()}
        payload = {k: (v or None) if isinstance(v, str) or v is None else v for k, v in payload.items()}
        payload["isbn"] = payload.get("issn_isbn")
        with session_scope(DB_PATH) as session:
            core = session.get(PublicationCore, pub_id)
            legacy = session.get(Publication, pub_id)
            if not core and not legacy:
                st.error("This publication no longer exists.")
                return
            for record in (core, legacy):
                if record is None:
                    continue
                record.title = payload["title"] or record.title
                record.faculty_name = payload["faculty_name"] or record.faculty_name
                record.authors = payload.get("authors")
                record.publication_name = payload.get("publication_name")
                record.doi = payload.get("doi")
                record.doi_normalized = normalize_doi(payload.get("doi"))
                record.pub_date = payload.get("pub_date")
                record.paper_url = payload.get("paper_url")
                record.venue = payload.get("venue")
                record.conference_date = payload.get("conference_date")
                record.category = payload["category"]
                record.publication_type = payload["publication_type"]
                record.indexing_source = payload["category"]
                record.national_international = payload.get("national_international")
                record.updated_at = datetime.now(timezone.utc)
            if legacy is not None:
                legacy.quartile = payload.get("quartile") if payload["publication_type"] == "Journal" else None
                legacy.issn_isbn = payload.get("issn_isbn")
            if core is not None:
                # Replace the type-specific details row so a type change moves the record to the right table.
                session.execute(delete(PublicationJournalDetails).where(PublicationJournalDetails.publication_id == pub_id))
                session.execute(delete(PublicationConferenceDetails).where(PublicationConferenceDetails.publication_id == pub_id))
                session.execute(delete(PublicationBookDetails).where(PublicationBookDetails.publication_id == pub_id))
                session.flush()
                insert_publication_details(session, pub_id, payload)
        _log_info(f"Publication {pub_id} updated by admin.")
        _bump_tables()
        st.rerun()


@st.dialog("Delete publication")
def _delete_publication_dialog(pub_id: int, title: str) -> None:
    st.write(f"Delete **{title}**? This cannot be undone.")
    if st.button("Delete", type="primary", key=f"confirm_del_{pub_id}"):
        with session_scope(DB_PATH) as session:
            core = session.get(PublicationCore, pub_id)
            if core:
                session.execute(delete(PublicationJournalDetails).where(PublicationJournalDetails.publication_id == pub_id))
                session.execute(delete(PublicationConferenceDetails).where(PublicationConferenceDetails.publication_id == pub_id))
                session.execute(delete(PublicationBookDetails).where(PublicationBookDetails.publication_id == pub_id))
                session.execute(delete(PublicationSourceCell).where(PublicationSourceCell.publication_id == pub_id))
                session.execute(delete(PublicationSourceRow).where(PublicationSourceRow.publication_id == pub_id))
                session.delete(core)
            legacy = session.get(Publication, pub_id)
            if legacy:
                session.delete(legacy)
            if not core and not legacy:
                st.error("This publication no longer exists.")
                return
        _log_info(f"Publication {pub_id} deleted by admin.")
        st.session_state["selected_publication_id"] = None
        _bump_tables()
        st.rerun()


def _render_publication_details(row: pd.Series) -> None:
    items = []
    for col in row.index:
        if col in DETAIL_HIDDEN:
            continue
        val = row[col]
        if pd.isna(val) if not isinstance(val, (list, dict)) else False:
            continue
        if isinstance(val, str) and not val.strip():
            continue
        if col == "category":
            val = category_label(str(val))
        label = DETAIL_LABELS.get(col, col.replace("_", " ").capitalize())
        items.append((label, val))
    cols = st.columns(2)
    for i, (label, val) in enumerate(items):
        if isinstance(val, str) and val.startswith(("http://", "https://")):
            cols[i % 2].markdown(f"**{label}**: [{val}]({val})")
        else:
            cols[i % 2].markdown(f"**{label}**: {val}")


def _filters_key(filters: PublicationFilters | None) -> tuple:
    if filters is None:
        return ()
    return tuple((k, str(v)) for k, v in vars(filters).items() if v not in (None, ""))


def _data_fingerprint(df: pd.DataFrame) -> str:
    """Cheap change marker for the publications table: row count plus newest update."""
    if df.empty:
        return "0"
    ids = ",".join(str(i) for i in sorted(df["id"].tolist()))
    return f"{len(df)}:{hash(ids)}:{st.session_state.get('table_version', 0)}"


@st.cache_data(show_spinner=False, ttl=300)
def _cached_official_export(db_path: str, template_path: str, filters_key: tuple, fingerprint: str, actor: str) -> tuple[bytes, dict[str, Any]]:
    filters = PublicationFilters(**{k: (date.fromisoformat(v) if k in ("date_from", "date_to") else v) for k, v in filters_key}) if filters_key else None
    with session_scope(db_path) as session:
        return export_official_format_xlsx(session, actor, template_path=template_path, filters=filters)


def _download_buttons(username: str, filters: PublicationFilters, all_df: pd.DataFrame, filtered_df: pd.DataFrame) -> None:
    st.subheader("Download as Excel")
    st.caption("Files use the official department workbook layout.")
    if not Path(DEFAULT_EXCEL).exists():
        st.error("The official Excel template is missing on the server. Ask the person who deployed the app.")
        return
    d1, d2 = st.columns(2)
    try:
        all_bytes, all_meta = _cached_official_export(DB_PATH, DEFAULT_EXCEL, (), _data_fingerprint(all_df), username)
        flt_bytes, flt_meta = _cached_official_export(DB_PATH, DEFAULT_EXCEL, _filters_key(filters), _data_fingerprint(filtered_df), username)
    except Exception as exc:
        _log_error(f"Official export failed: {exc}")
        st.error("The Excel file could not be built. Check the log for details.")
        return
    d1.download_button(
        f"Download all records ({all_meta['exported_row_count']} rows)",
        all_bytes,
        file_name="publications_all.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="export_all_btn",
        width="stretch",
        on_click=lambda: _log_info(f"Official export (all) downloaded by {username}."),
    )
    d2.download_button(
        f"Download current filter ({flt_meta['exported_row_count']} rows)",
        flt_bytes,
        file_name="publications_filtered.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="export_filtered_btn",
        width="stretch",
        disabled=filtered_df.empty,
        on_click=lambda: _log_info(f"Official export (filtered) downloaded by {username}."),
    )
    unexported = all_meta["unexported"]
    if unexported:
        st.warning(
            f"{len(unexported)} record(s) have an 'Indexed in' and type combination with no sheet in the official "
            "workbook, so they are not in the file. Edit them to include them."
        )
        missing = pd.DataFrame(unexported)
        missing["category"] = missing["category"].map(category_label)
        st.dataframe(
            missing.rename(columns={"id": "ID", "faculty_name": "Faculty", "title": "Title", "category": "Indexed in", "publication_type": "Type"}),
            width="stretch",
            hide_index=True,
        )


def _publications_page() -> None:
    st.title("Publications")
    username = st.session_state["auth_username"]
    is_admin = st.session_state["auth_role"] == "admin"

    with session_scope(DB_PATH) as session:
        all_df = get_publications_df(session, PublicationFilters())
        if not is_admin and not st.session_state.get("publications_filter_defaulted"):
            # Faculty land on their own records first; "Clear filters" shows everyone.
            st.session_state["publications_filter_defaulted"] = True
            if not all_df.empty and username in all_df["faculty_name"].values:
                st.session_state["flt_faculty"] = username
        filters = _build_filters(all_df)
        filtered_df = get_publications_df(session, filters)

        if filtered_df.empty:
            st.info("No publications match these filters." if not all_df.empty else "No publications yet.")
        else:
            st.caption(f"{len(filtered_df)} of {len(all_df)} publications. Click a row to see its details.")
            event = st.dataframe(
                _display_publications(filtered_df),
                width="stretch",
                hide_index=True,
                column_config=_publication_table_config(),
                on_select="rerun",
                selection_mode="single-row",
                key=_table_key("publications_table"),
            )
            selected_id = _select_row_id(filtered_df, event)
            if selected_id is not None:
                st.session_state["selected_publication_id"] = selected_id

    _download_buttons(username, filters, all_df, filtered_df)

    view_id = st.session_state.get("selected_publication_id")
    if view_id and not filtered_df.empty:
        pub_row = filtered_df[filtered_df["id"] == view_id]
        if pub_row.empty:
            st.session_state["selected_publication_id"] = None
        else:
            row = pub_row.iloc[0]
            st.divider()
            st.subheader(str(row["title"]))
            _render_publication_details(row)
            if is_admin:
                a1, a2, _ = st.columns([1, 1, 4])
                if a1.button("Edit", key="edit_pub_btn", width="stretch"):
                    _edit_publication_dialog(int(view_id))
                if a2.button("Delete", key="delete_pub_btn", width="stretch"):
                    _delete_publication_dialog(int(view_id), str(row["title"]))


# ---------------------------------------------------------------------------
# Faculty: submit a publication
# ---------------------------------------------------------------------------


def _clear_ingestion() -> None:
    st.session_state["ingestion_payload"] = {}
    st.session_state["ingestion_warnings"] = []
    st.session_state["ingestion_confidence"] = 0.0
    st.session_state["ingestion_input_method"] = None
    st.session_state["ingestion_source_input"] = None


def _status_sentence(is_accepted: bool, is_published: bool, is_indexed: bool, ptype: str) -> str:
    if not is_accepted:
        return "Not yet accepted."
    parts = ["presented" if ptype == "Conference" else "accepted"]
    parts.append("published" if is_published else "not yet published")
    if is_published:
        parts.append("indexed" if is_indexed else "not yet indexed")
    return "Status: " + ", ".join(parts) + "."


def _faculty_new_submission() -> None:
    st.title("Submit a Publication")
    username = st.session_state["auth_username"]

    success_message = st.session_state.pop("submission_success", None)
    if success_message:
        st.success(success_message)

    st.caption(f"Submitting as **{username}**. The admin reviews every submission before it appears in the list.")

    workbook_categories = category_type_options()
    c1, c2 = st.columns(2)
    category_label_choice = c1.selectbox("Indexed in", list(workbook_categories.keys()), key="faculty_workbook_category")
    category = category_from_label(category_label_choice)
    sub_category = c2.selectbox("Publication type", workbook_categories.get(category_label_choice, ["Journal"]), key="faculty_workbook_subcategory")

    mode = st.radio("How do you want to add it?", ["From a link or DOI", "Enter details manually"], horizontal=True, key="faculty_input_mode")
    if st.session_state.get("ingestion_mode_last") not in (None, mode):
        _clear_ingestion()
    st.session_state["ingestion_mode_last"] = mode

    source_input = ""
    if mode == "From a link or DOI":
        f1, f2 = st.columns([4, 1], vertical_alignment="bottom")
        source_input = f1.text_input("Paper link or DOI", key="faculty_source_input", placeholder="https://... or 10.1234/...")
        if f2.button("Fetch details", key="fetch_details_btn", width="stretch"):
            if not source_input.strip():
                st.error("Enter a link or DOI first.")
            else:
                method = InputMethod.DOI if normalize_doi(source_input) else InputMethod.URL
                try:
                    result = ingest_source(source_input=source_input, input_method=method.value, faculty_name=username)
                    payload = dict(result.payload)
                    payload["faculty_name"] = username
                    st.session_state["ingestion_payload"] = payload
                    st.session_state["ingestion_confidence"] = result.confidence_score
                    st.session_state["ingestion_warnings"] = result.warnings
                    st.session_state["ingestion_input_method"] = method.value
                    st.session_state["ingestion_source_input"] = source_input
                except Exception as exc:
                    _log_error(f"Ingestion failed for {username}: {exc}")
                    st.error("The link could not be read. You can still enter the details manually.")
    else:
        if not st.session_state.get("ingestion_payload"):
            st.session_state["ingestion_payload"] = {"faculty_name": username}
            st.session_state["ingestion_confidence"] = 0.3
            st.session_state["ingestion_warnings"] = []
            st.session_state["ingestion_input_method"] = InputMethod.MANUAL.value
            st.session_state["ingestion_source_input"] = None

    payload = dict(st.session_state.get("ingestion_payload", {}))
    if not payload:
        return

    if st.session_state.get("ingestion_warnings"):
        st.info("Some details could not be read from the link. Please check and complete the fields below.")

    payload["faculty_name"] = username
    payload["category"] = category
    payload["publication_type"] = sub_category
    payload["indexing_source"] = payload.get("indexing_source") or category

    st.subheader("Details")
    c1, c2 = st.columns(2)
    payload["title"] = c1.text_input("Title", value=payload.get("title") or "", key="faculty_review_title")
    payload["publication_name"] = c2.text_input(
        "Journal / Conference / Book name",
        value=payload.get("publication_name") or payload.get("venue") or "",
        key="faculty_review_publication_name",
    )
    payload["authors"] = st.text_area("Authors", value=payload.get("authors") or "", key="faculty_review_authors", height=80, help="As they appear on the paper.")

    c1, c2 = st.columns(2)
    selected_pub_date = c1.date_input("Published on", value=_as_date(payload.get("pub_date")), key="faculty_review_pub_date", format="YYYY-MM-DD")
    payload["pub_date"] = selected_pub_date.isoformat() if selected_pub_date else None
    payload["doi"] = c2.text_input("DOI", value=payload.get("doi") or "", key="faculty_review_doi", placeholder="Optional")

    if sub_category in ("Journal", "Conference"):
        payload["national_international"] = _dropdown(st, "National / International", _NAT_INT_OPTIONS, payload.get("national_international", ""), "faculty_review_nat_int")

    if sub_category == "Journal":
        c1, c2 = st.columns(2)
        payload["quartile"] = _dropdown(c1, "Quartile", _QUARTILE_OPTIONS, payload.get("quartile", ""), "faculty_review_quartile")
        payload["issn_isbn"] = c2.text_input("ISSN", value=payload.get("issn_isbn") or "", key="faculty_review_issn")
        c1, c2 = st.columns(2)
        payload["volume_issue"] = c1.text_input("Volume / Issue", value=payload.get("volume_issue") or "", key="faculty_review_volume_issue_journal")
        payload["official_venue_url"] = c2.text_input("Journal website", value=payload.get("official_venue_url") or "", key="faculty_review_official_url_journal")
        payload["venue"] = payload.get("venue") or payload.get("publication_name")
    elif sub_category == "Conference":
        c1, c2 = st.columns(2)
        payload["venue"] = c1.text_input("Venue (organising institute)", value=payload.get("venue") or "", key="faculty_review_venue_conference")
        payload["conference_date"] = c2.text_input("Conference dates", value=payload.get("conference_date") or "", key="faculty_review_conference_date", placeholder="e.g. 12-06-2025 and 13-06-2025")
        c1, c2 = st.columns(2)
        payload["issn_isbn"] = c1.text_input("ISSN / ISBN", value=payload.get("issn_isbn") or "", key="faculty_review_issn_isbn_conf")
        payload["volume_issue"] = c2.text_input("Volume / Issue", value=payload.get("volume_issue") or "", key="faculty_review_volume_issue_conference")
        payload["official_venue_url"] = st.text_input("Conference website", value=payload.get("official_venue_url") or "", key="faculty_review_official_url_conference")
    elif sub_category == "Book Chapter":
        c1, c2 = st.columns(2)
        payload["publisher"] = c1.text_input("Publisher", value=payload.get("publisher") or "", key="faculty_review_publisher")
        payload["issn_isbn"] = c2.text_input("ISBN", value=payload.get("issn_isbn") or "", key="faculty_review_isbn")
        payload["official_venue_url"] = st.text_input("Book website", value=payload.get("official_venue_url") or "", key="faculty_review_book_url")
        payload["venue"] = payload.get("venue") or payload.get("publisher")

    st.subheader("Status")
    step1_label = "Presented or accepted?" if sub_category == "Conference" else "Accepted?"
    step1_options = _PRESENTED_OPTIONS if sub_category == "Conference" else _ACCEPTED_OPTIONS
    payload["presented_accepted_flag"] = _dropdown(st, step1_label, step1_options, payload.get("presented_accepted_flag", ""), "faculty_pipeline_accepted")
    is_accepted = payload["presented_accepted_flag"] in ("Yes", "Presented", "Accepted")
    is_published = False
    is_indexed = False

    if is_accepted:
        acceptance_proof = st.file_uploader(
            "Acceptance letter or certificate",
            type=_PROOF_FILE_TYPES,
            key="faculty_pipeline_acceptance_proof",
            help="PDF or image. Optional but speeds up review.",
        )
        if acceptance_proof:
            st.session_state["_acceptance_proof_file"] = acceptance_proof

        payload["research_published_flag"] = _dropdown(st, "Published?", _YESNO_OPTIONS, payload.get("research_published_flag", ""), "faculty_pipeline_published")
        is_published = payload["research_published_flag"] == "Yes"

        if is_published:
            payload["paper_url"] = st.text_input("Link to the published paper", value=payload.get("paper_url") or "", key="faculty_pipeline_pub_link")
            publication_proof = st.file_uploader(
                "Publication proof",
                type=_PROOF_FILE_TYPES,
                key="faculty_pipeline_pub_proof",
                help="First page, DOI screenshot or publisher confirmation. Optional.",
            )
            if publication_proof:
                st.session_state["_publication_proof_file"] = publication_proof

            payload["indexing_flag"] = _dropdown(st, "Indexed?", _YESNO_OPTIONS, payload.get("indexing_flag", ""), "faculty_pipeline_indexed")
            is_indexed = payload["indexing_flag"] == "Yes"

            if is_indexed:
                payload["indexing_source"] = _dropdown(st, "Indexed by", _INDEXING_OPTIONS, payload.get("indexing_source", ""), "faculty_pipeline_indexing_type")
                if payload["indexing_source"] and payload["indexing_source"] != "Other":
                    if is_official_pair(payload["indexing_source"], sub_category):
                        payload["category"] = category_from_label(payload["indexing_source"])
                    else:
                        st.caption(f"Kept 'Indexed in' as {category_label_choice}: the workbook has no {sub_category} sheet for {payload['indexing_source']}.")
                indexing_proof = st.file_uploader(
                    "Indexing proof",
                    type=_PROOF_FILE_TYPES,
                    key="faculty_pipeline_indexing_proof",
                    help="Scopus or WoS listing screenshot. Optional.",
                )
                if indexing_proof:
                    st.session_state["_indexing_proof_file"] = indexing_proof
                if sub_category == "Book Chapter":
                    c1, c2, c3 = st.columns(3)
                    payload["book_indexed_ugc"] = _dropdown(c1, "In UGC list", _YESNO_OPTIONS, payload.get("book_indexed_ugc", ""), "faculty_review_book_ugc")
                    payload["book_indexed_scopus"] = _dropdown(c2, "In Scopus", _YESNO_OPTIONS, payload.get("book_indexed_scopus", ""), "faculty_review_book_scopus")
                    payload["book_indexed_wos"] = _dropdown(c3, "In WoS", _YESNO_OPTIONS, payload.get("book_indexed_wos", ""), "faculty_review_book_wos")

    st.caption(_status_sentence(is_accepted, is_published, is_indexed, sub_category))

    if st.button("Submit for review", type="primary", key="submit_for_review_btn"):
        missing_fields = []
        if not (payload.get("title") or "").strip():
            missing_fields.append("Title")
        if not (payload.get("publication_name") or "").strip() and not (payload.get("venue") or "").strip():
            missing_fields.append("Journal / Conference / Book name")
        if not (payload.get("authors") or "").strip():
            missing_fields.append("Authors")
        if missing_fields:
            st.error("Please fill in: " + ", ".join(missing_fields))
            return

        for state_key, payload_key in (
            ("_acceptance_proof_file", "certificate_ref"),
            ("_publication_proof_file", "attachment_ref"),
            ("_indexing_proof_file", "indexing_proof"),
        ):
            uploaded = st.session_state.pop(state_key, None)
            if uploaded is not None:
                saved = _save_uploaded_file(uploaded)
                if saved:
                    payload[payload_key] = saved

        try:
            selected_method = st.session_state.get("ingestion_input_method") or (
                InputMethod.MANUAL.value if mode == "Enter details manually" else InputMethod.URL.value
            )
            with session_scope(DB_PATH) as session:
                submission = create_submission(
                    session=session,
                    submitted_by=username,
                    source_input=st.session_state.get("ingestion_source_input") or source_input or None,
                    source_input_method=InputMethod(selected_method),
                    payload=payload,
                    confidence_score=float(st.session_state.get("ingestion_confidence", 0.0)),
                    as_draft=False,
                )
                st.session_state["submission_success"] = (
                    f"Submitted. The admin will review it and it will then appear under Publications. "
                    f"You can follow its status in My Submissions (reference #{submission.id})."
                )
                _log_info(f"Submission {submission.id} created by {username}.")
            _clear_ingestion()
            st.session_state["ingestion_mode_last"] = None
            st.rerun()
        except ValueError as exc:
            st.error(str(exc))
            _log_error(f"Submission validation failed for {username}: {exc}")


def _faculty_my_submissions() -> None:
    st.title("My Submissions")
    username = st.session_state["auth_username"]
    with session_scope(DB_PATH) as session:
        rows = session.execute(
            select(PendingSubmission).where(PendingSubmission.submitted_by == username).order_by(PendingSubmission.created_at.desc())
        ).scalars()
        data = []
        for row in rows:
            payload = row.parsed_payload_json or {}
            data.append(
                {
                    "Ref": row.id,
                    "Title": payload.get("title"),
                    "Journal / Conference / Book": payload.get("publication_name") or payload.get("venue"),
                    "Type": payload.get("publication_type"),
                    "Status": _status_label(row.status),
                    "Submitted on": row.created_at.date() if row.created_at else None,
                    "Admin note": row.review_note,
                }
            )
    df = pd.DataFrame(data)
    if df.empty:
        st.info("You have not submitted anything yet. Use **Submit a Publication** to add your first paper.")
        return
    st.caption("Approved submissions appear under Publications. If something was rejected, the admin's note explains why.")
    _safe_dataframe(df, column_config={"Submitted on": column_config.DateColumn("Submitted on")})


# ---------------------------------------------------------------------------
# Admin: review queue
# ---------------------------------------------------------------------------


@st.dialog("Approve submission")
def _approve_dialog(submission_id: int, username: str, payload: dict[str, Any]) -> None:
    note = st.text_area("Note to the faculty member (optional)", key=f"approve_note_{submission_id}")
    override_soft_duplicate = st.checkbox("Approve even if it looks like a duplicate", key=f"override_{submission_id}")
    if st.button("Approve", type="primary", key=f"confirm_approve_{submission_id}"):
        try:
            with session_scope(DB_PATH) as session:
                result = approve_submission(
                    session=session,
                    submission_id=submission_id,
                    admin_user=username,
                    review_note=note,
                    edited_payload=payload,
                    override_soft_duplicate=override_soft_duplicate,
                )
            if result.hard_duplicate:
                st.error("A publication with this DOI already exists, so this cannot be approved.")
            elif result.soft_duplicate and result.publication_id is None:
                st.warning("A publication with the same title, faculty and year already exists. Tick the box above to approve anyway.")
            else:
                _log_info(f"Submission {submission_id} approved by {username}.")
                st.session_state["admin_submission_id"] = None
                _bump_tables()
                st.rerun()
        except ValueError as exc:
            st.error(str(exc))
            _log_error(f"Approval validation failed for submission {submission_id}: {exc}")


@st.dialog("Reject submission")
def _reject_dialog(submission_id: int, username: str) -> None:
    note = st.text_area("Reason (shown to the faculty member)", key=f"reject_note_{submission_id}")
    if st.button("Reject", type="primary", key=f"confirm_reject_{submission_id}"):
        with session_scope(DB_PATH) as session:
            reject_submission(session, submission_id, username, note or "Rejected by admin.")
        _log_info(f"Submission {submission_id} rejected by {username}.")
        st.session_state["admin_submission_id"] = None
        _bump_tables()
        st.rerun()


def _admin_review_queue() -> None:
    st.title("Review Queue")
    username = st.session_state["auth_username"]
    with session_scope(DB_PATH) as session:
        rows = session.execute(
            select(PendingSubmission)
            .where(PendingSubmission.status.in_([SubmissionStatus.SUBMITTED.value, SubmissionStatus.UNDER_REVIEW.value]))
            .order_by(PendingSubmission.created_at.asc())
        ).scalars()
        queue_rows = []
        for row in rows:
            payload = row.parsed_payload_json or {}
            queue_rows.append(
                {
                    "id": row.id,
                    "Submitted by": row.submitted_by,
                    "Title": payload.get("title"),
                    "Type": payload.get("publication_type"),
                    "Indexed in": category_label(payload.get("category")),
                    "Status": _status_label(row.status),
                    "Submitted on": row.created_at.date() if row.created_at else None,
                }
            )
    queue_df = pd.DataFrame(queue_rows)
    if queue_df.empty:
        st.success("Nothing to review.")
        st.session_state["admin_submission_id"] = None
        return

    st.caption(f"{len(queue_df)} waiting. Click a row to review it.")
    event = st.dataframe(
        queue_df.rename(columns={"id": "Ref"}),
        width="stretch",
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key=_table_key("review_queue_table"),
        column_config={"Submitted on": column_config.DateColumn("Submitted on"), "Ref": column_config.NumberColumn("Ref", width="small")},
    )
    selected_id = _select_row_id(queue_df, event)
    if selected_id is not None:
        st.session_state["admin_submission_id"] = selected_id

    sub_id = st.session_state.get("admin_submission_id")
    if sub_id and sub_id in queue_df["id"].tolist():
        st.divider()
        _admin_submission_detail(sub_id, username)


def _proof_download(label: str, ref: str | None, key: str) -> None:
    if not ref:
        return
    path = Path(ref)
    if path.exists() and path.is_file():
        st.download_button(label, data=path.read_bytes(), file_name=path.name, key=key)
    elif ref.startswith(("http://", "https://")):
        st.markdown(f"{label}: [{ref}]({ref})")
    else:
        st.caption(f"{label}: file not available on this server.")


def _admin_submission_detail(submission_id: int, username: str) -> None:
    with session_scope(DB_PATH) as session:
        submission = session.get(PendingSubmission, submission_id)
        if not submission:
            st.error("This submission no longer exists.")
            return
        payload = dict(submission.parsed_payload_json or {})
        submitted_by = submission.submitted_by
        status = submission.status
        actions = session.execute(
            select(ReviewAction).where(ReviewAction.submission_id == submission_id).order_by(ReviewAction.created_at.asc())
        ).scalars().all()
        audit_rows = [
            {"When": a.created_at, "Who": a.actor, "Action": ACTION_LABELS.get(a.action, a.action), "Note": a.note}
            for a in actions
        ]

    head_col, action_col = st.columns([3, 1], vertical_alignment="center")
    head_col.subheader(f"Submission #{submission_id} from {submitted_by}")
    head_col.caption(f"Status: {_status_label(status)}")
    if status == SubmissionStatus.SUBMITTED.value:
        if action_col.button("Start review", key=f"start_review_{submission_id}", width="stretch"):
            with session_scope(DB_PATH) as session:
                start_review(session, submission_id, username)
            _log_info(f"Submission {submission_id} moved to UNDER_REVIEW by {username}.")
            st.rerun()

    workbook_categories = category_type_options()
    category_options = list(workbook_categories.keys())
    current_category = category_label(payload.get("category"))
    if current_category not in category_options:
        current_category = category_options[0]

    c1, c2 = st.columns(2)
    category_label_choice = c1.selectbox("Indexed in", category_options, index=category_options.index(current_category), key=f"admin_category_{submission_id}")
    category = category_from_label(category_label_choice)
    sub_options = workbook_categories.get(category_label_choice, ["Journal"])
    current_sub_category = payload.get("publication_type")
    if current_sub_category not in sub_options:
        current_sub_category = sub_options[0]
    sub_category = c2.selectbox("Publication type", sub_options, index=sub_options.index(current_sub_category), key=f"admin_sub_category_{submission_id}")
    payload["category"] = category
    payload["publication_type"] = sub_category

    c1, c2 = st.columns(2)
    payload["faculty_name"] = c1.text_input("Faculty", value=payload.get("faculty_name") or "", key=f"f_{submission_id}")
    payload["title"] = c2.text_input("Title", value=payload.get("title") or "", key=f"t_{submission_id}")
    payload["publication_name"] = st.text_input(
        "Journal / Conference / Book name",
        value=payload.get("publication_name") or payload.get("venue") or "",
        key=f"pn_{submission_id}",
    )
    payload["authors"] = st.text_area("Authors", value=payload.get("authors") or "", key=f"a_{submission_id}", height=80)
    c1, c2 = st.columns(2)
    pub_date_value = c1.date_input("Published on", value=_as_date(payload.get("pub_date")), key=f"d_{submission_id}", format="YYYY-MM-DD")
    payload["pub_date"] = pub_date_value.isoformat() if pub_date_value else None
    payload["doi"] = c2.text_input("DOI", value=payload.get("doi") or "", key=f"doi_{submission_id}")

    if sub_category in ("Journal", "Conference"):
        payload["national_international"] = _dropdown(st, "National / International", _NAT_INT_OPTIONS, payload.get("national_international", ""), f"nat_{submission_id}")

    if sub_category == "Journal":
        c1, c2 = st.columns(2)
        payload["quartile"] = _dropdown(c1, "Quartile", _QUARTILE_OPTIONS, payload.get("quartile", ""), f"q_{submission_id}")
        payload["issn_isbn"] = c2.text_input("ISSN", value=payload.get("issn_isbn") or "", key=f"is_{submission_id}")
        c1, c2 = st.columns(2)
        payload["volume_issue"] = c1.text_input("Volume / Issue", value=payload.get("volume_issue") or "", key=f"vj_{submission_id}")
        payload["official_venue_url"] = c2.text_input("Journal website", value=payload.get("official_venue_url") or "", key=f"jou_{submission_id}")
        payload["venue"] = payload.get("venue") or payload.get("publication_name")
    elif sub_category == "Conference":
        c1, c2 = st.columns(2)
        payload["venue"] = c1.text_input("Venue (organising institute)", value=payload.get("venue") or "", key=f"v_{submission_id}")
        payload["conference_date"] = c2.text_input("Conference dates", value=payload.get("conference_date") or "", key=f"cd_{submission_id}")
        c1, c2 = st.columns(2)
        payload["issn_isbn"] = c1.text_input("ISSN / ISBN", value=payload.get("issn_isbn") or "", key=f"is_{submission_id}")
        payload["volume_issue"] = c2.text_input("Volume / Issue", value=payload.get("volume_issue") or "", key=f"cv_{submission_id}")
        payload["official_venue_url"] = st.text_input("Conference website", value=payload.get("official_venue_url") or "", key=f"cou_{submission_id}")
    elif sub_category == "Book Chapter":
        c1, c2 = st.columns(2)
        payload["publisher"] = c1.text_input("Publisher", value=payload.get("publisher") or "", key=f"bp_{submission_id}")
        payload["issn_isbn"] = c2.text_input("ISBN", value=payload.get("issn_isbn") or "", key=f"is_{submission_id}")
        payload["official_venue_url"] = st.text_input("Book website", value=payload.get("official_venue_url") or "", key=f"bou_{submission_id}")
        payload["venue"] = payload.get("venue") or payload.get("publisher")

    st.markdown("**Status and proofs**")
    step1_label = "Presented or accepted?" if sub_category == "Conference" else "Accepted?"
    step1_options = _PRESENTED_OPTIONS if sub_category == "Conference" else _ACCEPTED_OPTIONS
    s1, s2, s3 = st.columns(3)
    payload["presented_accepted_flag"] = _dropdown(s1, step1_label, step1_options, payload.get("presented_accepted_flag", ""), f"ap_{submission_id}")
    payload["research_published_flag"] = _dropdown(s2, "Published?", _YESNO_OPTIONS, payload.get("research_published_flag", ""), f"rp_{submission_id}")
    payload["indexing_flag"] = _dropdown(s3, "Indexed?", _YESNO_OPTIONS, payload.get("indexing_flag", ""), f"if_{submission_id}")
    c1, c2 = st.columns(2)
    payload["paper_url"] = c1.text_input("Link to the published paper", value=payload.get("paper_url") or "", key=f"url_{submission_id}")
    payload["indexing_source"] = _dropdown(c2, "Indexed by", _INDEXING_OPTIONS, payload.get("indexing_source", ""), f"idx_{submission_id}")
    if payload.get("indexing_source") and payload["indexing_source"] not in ("", "Other"):
        if is_official_pair(payload["indexing_source"], sub_category):
            payload["category"] = category_from_label(payload["indexing_source"])
        else:
            st.caption(f"Kept 'Indexed in' as {category_label_choice}: the workbook has no {sub_category} sheet for {payload['indexing_source']}.")
    if sub_category == "Book Chapter":
        c1, c2, c3 = st.columns(3)
        payload["book_indexed_ugc"] = _dropdown(c1, "In UGC list", _YESNO_OPTIONS, payload.get("book_indexed_ugc", ""), f"bugc_{submission_id}")
        payload["book_indexed_scopus"] = _dropdown(c2, "In Scopus", _YESNO_OPTIONS, payload.get("book_indexed_scopus", ""), f"bsc_{submission_id}")
        payload["book_indexed_wos"] = _dropdown(c3, "In WoS", _YESNO_OPTIONS, payload.get("book_indexed_wos", ""), f"bwos_{submission_id}")

    p1, p2, p3 = st.columns(3)
    with p1:
        _proof_download("Acceptance proof", payload.get("certificate_ref"), f"dl_cert_{submission_id}")
    with p2:
        _proof_download("Publication proof", payload.get("attachment_ref"), f"dl_att_{submission_id}")
    with p3:
        _proof_download("Indexing proof", payload.get("indexing_proof"), f"dl_idx_{submission_id}")

    b1, b2, _ = st.columns([1, 1, 4])
    if b1.button("Approve", type="primary", key=f"approve_btn_{submission_id}", width="stretch"):
        _approve_dialog(submission_id, username, payload)
    if b2.button("Reject", key=f"reject_btn_{submission_id}", width="stretch"):
        _reject_dialog(submission_id, username)

    with st.expander("History"):
        _safe_dataframe(pd.DataFrame(audit_rows), "No history yet.")


# ---------------------------------------------------------------------------
# Admin: import from Excel
# ---------------------------------------------------------------------------


@st.dialog("Replace all publications?")
def _migration_confirm_dialog(username: str) -> None:
    upload_bytes = st.session_state.get("migration_upload_bytes")
    upload_name = st.session_state.get("migration_upload_name") or "workbook.xlsx"
    st.write(
        f"Every publication currently in the app will be replaced by the rows in **{upload_name}**. "
        "A backup of the current data is saved first. Pending submissions are not affected."
    )
    if st.button("Replace publications", type="primary", key="confirm_rebuild_btn"):
        if not upload_bytes:
            st.error("Upload a workbook first.")
            return
        temp_excel_path: str | None = None
        try:
            with NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
                tmp.write(bytes(upload_bytes))
                temp_excel_path = tmp.name
            with session_scope(DB_PATH) as session:
                report = rebuild_publications_from_excel(
                    session=session,
                    db_path=DB_PATH,
                    excel_path=temp_excel_path,
                    status_path=MIGRATION_STATUS_PATH,
                )
        except Exception as exc:
            _log_error(f"Migration rebuild failed for {username}: {exc}")
            st.error("The workbook could not be imported. Check that it uses the official layout and try again.")
            return
        finally:
            if temp_excel_path and Path(temp_excel_path).exists():
                Path(temp_excel_path).unlink(missing_ok=True)

        st.session_state["migration_report"] = report
        st.session_state["migration_upload_bytes"] = None
        st.session_state["migration_upload_name"] = None
        _log_info(f"Migration rebuild run by {username}: imported={report.rows_imported}, skipped={report.rows_skipped}")
        st.session_state["selected_publication_id"] = None
        _bump_tables()
        st.rerun()


def _admin_import_page() -> None:
    st.title("Import from Excel")
    username = st.session_state["auth_username"]
    st.write(
        "Upload the official department workbook to load its publications into the app. "
        "This replaces the current publication list, so use it when the workbook is the most up-to-date copy."
    )

    uploaded_workbook = st.file_uploader("Workbook (.xlsx)", type=["xlsx"], key="migration_uploaded_workbook")
    if uploaded_workbook is not None:
        st.session_state["migration_upload_bytes"] = uploaded_workbook.getvalue()
        st.session_state["migration_upload_name"] = uploaded_workbook.name

    has_upload = bool(st.session_state.get("migration_upload_bytes"))
    if st.button("Import workbook", type="primary", key="rebuild_btn", disabled=not has_upload):
        _migration_confirm_dialog(username)
    if not has_upload:
        st.caption("Choose a workbook to enable the import.")

    report = st.session_state.get("migration_report")
    if report:
        st.subheader("Last import")
        m1, m2, m3 = st.columns(3)
        m1.metric("Imported", report.rows_imported)
        m2.metric("Skipped", report.rows_skipped)
        m3.metric("Rows read", report.rows_read)
        st.caption(f"Backup saved to {report.db_backup_file}")

        with st.expander("Why rows were skipped"):
            reasons = [(SKIP_REASON_LABELS.get(k, k), v) for k, v in report.skip_reasons.items() if v]
            if reasons:
                for label, count in sorted(reasons, key=lambda item: -item[1]):
                    st.write(f"- {label}: {count}")
            else:
                st.write("Nothing was skipped.")
            sheet_df = pd.DataFrame(
                [{"Sheet": name, "Rows read": v["read"], "Imported": v["imported"], "Skipped": v["skipped"]} for name, v in report.sheet_summary.items()]
            )
            _safe_dataframe(sheet_df)

        with st.expander("Quality checks"):
            checks_df = pd.DataFrame(report.quality_checks)
            if not checks_df.empty:
                checks_df = checks_df.rename(columns={"name": "Check", "pass": "Passed", "value": "Value", "details": "Details"})
                checks_df["Value"] = checks_df["Value"].astype(str)
            _safe_dataframe(checks_df, "No checks were run.")

    st.divider()
    with st.expander("Advanced diagnostics"):
        st.caption("Technical health checks of the database and log. Useful when reporting a problem.")
        if st.button("Run checks", key="run_system_checks_btn"):
            with session_scope(DB_PATH) as session:
                checks_df, summary = run_system_checks(session, migration_status_path=MIGRATION_STATUS_PATH, log_path=APP_LOG_PATH)
            st.session_state["system_checks_df"] = checks_df
            st.session_state["system_checks_summary"] = summary
            _log_info(f"System checks executed: failed={summary['failed_checks']}")
        checks_df = st.session_state.get("system_checks_df")
        summary = st.session_state.get("system_checks_summary")
        if checks_df is not None and summary is not None:
            st.write(f"{summary['passed_checks']} of {summary['total_checks']} checks passed.")
            shown = checks_df.copy()
            shown["value"] = shown["value"].astype(str)
            _safe_dataframe(shown.rename(columns={"name": "Check", "passed": "Passed", "details": "Details", "value": "Value"}))
            st.download_button(
                "Download report (Excel)",
                export_system_checks_xlsx(checks_df, summary),
                file_name="system_checks_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_checks_btn",
            )


# ---------------------------------------------------------------------------
# App shell
# ---------------------------------------------------------------------------

# Select boxes are searchable text inputs under the hood, so browsers show a text
# cursor over them. Show a pointer instead so they read as clickable.
_GLOBAL_CSS = """
<style>
div[data-baseweb="select"] > div,
div[data-baseweb="select"] input,
div[data-baseweb="select"] svg {
    cursor: pointer !important;
}
/* No sidebar is used; hide its collapsed-state toggle. */
[data-testid="stSidebarCollapsedControl"],
[data-testid="collapsedControl"] {
    display: none !important;
}
/* Streamlit's top bar is empty for viewers (navigation is hidden); hide it. */
header[data-testid="stHeader"] {
    display: none !important;
}
.block-container {
    padding-top: 1.5rem;
    padding-bottom: 1.5rem;
}
/* App bar: page links stay on one line and read as small nav items. */
.st-key-app_bar p {
    margin: 0;
}
/* Current page: highlighted link. */
.st-key-app_bar a[data-testid="stPageLink-NavLink"]:has(strong) {
    background-color: rgba(21, 101, 192, 0.10);
    border-radius: 0.5rem;
}
/* Phones get a single dropdown instead of a row of links. */
@media (max-width: 768px) {
    .st-key-app_bar_links { display: none !important; }
}
@media (min-width: 769px) {
    .st-key-app_bar_menu { display: none !important; }
}
.st-key-app_bar a[data-testid="stPageLink-NavLink"] {
    white-space: nowrap;
    justify-content: flex-start;
    overflow: hidden;
    container-type: inline-size;
}
.st-key-app_bar a[data-testid="stPageLink-NavLink"] [data-testid="stMarkdownContainer"] {
    display: inline-block;
    max-width: none;
    overflow: visible;
    white-space: nowrap;
}
/* A label wider than its item slides left on hover to reveal its end, then back. */
.st-key-app_bar a[data-testid="stPageLink-NavLink"]:hover [data-testid="stMarkdownContainer"] {
    animation: app-bar-reveal 2.4s ease-in-out infinite alternate;
}
@keyframes app-bar-reveal {
    0%, 15% { transform: translateX(0); }
    85%, 100% { transform: translateX(min(0px, calc(100cqw - 100% - 2rem))); }
}
</style>
"""


def _sign_out_page() -> None:
    """Reached via the Sign out link in the top bar."""
    _logout()
    st.rerun()


def _pages_for_role(role: str) -> list[Any]:
    pages = [
        st.Page(_dashboard_page, title="Overview", icon=":material/dashboard:", url_path="overview", default=True),
        st.Page(_publications_page, title="Publications", icon=":material/table_view:", url_path="publications"),
    ]
    if role == "faculty":
        pages += [
            st.Page(_faculty_new_submission, title="Submit a Publication", icon=":material/add:", url_path="submit"),
            st.Page(_faculty_my_submissions, title="My Submissions", icon=":material/history:", url_path="my-submissions"),
        ]
    else:
        pages += [
            st.Page(_admin_review_queue, title="Review Queue", icon=":material/rule:", url_path="review"),
            st.Page(_admin_import_page, title="Import from Excel", icon=":material/upload_file:", url_path="import"),
        ]
    pages.append(st.Page(_sign_out_page, title="Sign out", icon=":material/logout:", url_path="sign-out"))
    return pages


def _render_app_bar(username: str, role: str, pages: list[Any], current: Any) -> None:
    """One row: who is signed in on the left, the page links on the right.

    Two versions are rendered and CSS shows one: a row of links on wide screens and a
    single dropdown on phones, where five stacked links would eat the screen.
    """
    with st.container(key="app_bar"):
        left, right = st.columns([2, 3], vertical_alignment="center")
        left.markdown(f"Signed in as **{username}**")
        with right:
            with st.container(key="app_bar_links"):
                link_cols = st.columns(len(pages), vertical_alignment="center")
                for col, page in zip(link_cols, pages):
                    is_current = page.title == current.title
                    col.page_link(page, label=f"**{page.title}**" if is_current else page.title, icon=page.icon or None, width="stretch")
            with st.container(key="app_bar_menu"):
                choice = st.menu_button(
                    current.title,
                    [page.title for page in pages if page.title != current.title],
                    icon=":material/menu:",
                    key="app_bar_menu_button",
                    width="stretch",
                )
                if choice:
                    target = next((page for page in pages if page.title == choice), None)
                    if target is not None:
                        st.switch_page(target)
    st.divider()


def main() -> None:
    global DB_PATH, DEFAULT_EXCEL, APP_LOG_PATH
    DB_PATH, DEFAULT_EXCEL, APP_LOG_PATH = _resolve_runtime_paths()

    st.set_page_config(page_title=APP_NAME, page_icon=":material/school:", layout="wide", initial_sidebar_state="collapsed")
    st.markdown(_GLOBAL_CSS, unsafe_allow_html=True)
    _setup_logging()
    init_db(DB_PATH)
    _init_state()

    # Navigation is always registered (even signed out) so the browser never keeps
    # a stale menu, and it is hidden because the user menu in the header replaces it.
    if not st.session_state.get("auth_is_authenticated"):
        st.navigation([st.Page(_sign_in_page, title=APP_NAME, url_path="welcome", default=True)], position="hidden").run()
        return

    role = str(st.session_state["auth_role"])
    username = str(st.session_state["auth_username"])
    _keep_token_in_url()
    pages = _pages_for_role(role)
    current = st.navigation(pages, position="hidden")
    _render_app_bar(username, role, pages, current)
    current.run()


if __name__ == "__main__":
    main()
