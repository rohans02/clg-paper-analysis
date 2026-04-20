from __future__ import annotations

from datetime import date, datetime, timezone
import hmac
import logging
import os
from pathlib import Path
import re
import time
from tempfile import NamedTemporaryFile
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st
from streamlit import column_config
from sqlalchemy import delete, select

from publication_manager.db import init_db, session_scope
from publication_manager.enums import InputMethod, SubmissionStatus
from publication_manager.exporter import export_official_format_xlsx
from publication_manager.ingestion import ingest_source
from publication_manager.migration import MIGRATION_STATUS_PATH, SHEET_CONFIGS, rebuild_publications_from_excel
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
from publication_manager.workflow import approve_submission, create_submission, reject_submission, start_review

DB_PATH = "publication_manager.db"
DEFAULT_EXCEL = "Faculty Publications,A.Y. 2025-26,SEM-I & II.xlsx"
APP_LOG_PATH = "app.log"
ADMIN_PASSWORD_ENV = "APP_ADMIN_PASSWORD"
DB_PATH_ENV = "APP_DB_PATH"
TEMPLATE_PATH_ENV = "APP_TEMPLATE_PATH"
LOG_PATH_ENV = "APP_LOG_PATH"
ADMIN_MAX_FAILED_ATTEMPTS = 5
ADMIN_LOCKOUT_SECONDS = 300
FACULTY_PREFIX_OPTIONS = ["Dr.", "Dr. Ms.", "Dr. Mrs.", "Dr. Mr.", "Ms.", "Mr.", "Mrs.", "Prof.", "Other"]

_NAT_INT_OPTIONS = ["", "National", "International"]
_YESNO_OPTIONS = ["", "Yes", "No"]
_QUARTILE_OPTIONS = ["", "Q1", "Q2", "Q3", "Q4"]
_PRESENTED_OPTIONS = ["", "Presented", "Accepted"]
_ACCEPTED_OPTIONS = ["", "Yes", "No"]
_INDEXING_OPTIONS = ["", "Scopus", "WoS", "UGC Care", "Peer Reviewed", "International Conference", "National Conference", "Book Chapter", "Other"]
_PROOF_FILE_TYPES = ["pdf", "png", "jpg", "jpeg", "webp"]
UPLOADS_DIR = Path(__file__).resolve().parent / "uploads"


def _save_uploaded_file(uploaded_file) -> str | None:
    """Save an uploaded file to the uploads directory and return the file path."""
    if uploaded_file is None:
        return None
    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    import uuid
    ext = Path(uploaded_file.name).suffix.lower()
    filename = f"{uuid.uuid4().hex}{ext}"
    file_path = UPLOADS_DIR / filename
    file_path.write_bytes(uploaded_file.getvalue())
    return str(file_path)


def _dropdown(container, label: str, options: list[str], current: str, key: str) -> str:
    current = current or ""
    idx = options.index(current) if current in options else 0
    return container.selectbox(label, options, index=idx, key=key)


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


def _init_state() -> None:
    defaults: dict[str, Any] = {
        "auth_is_authenticated": False,
        "auth_role": None,
        "auth_username": None,
        "admin_failed_attempts": 0,
        "admin_lockout_until": 0.0,
        "ingestion_payload": {},
        "ingestion_confidence": 0.0,
        "ingestion_warnings": [],
        "ingestion_input_method": None,
        "ingestion_source_input": None,
        "admin_submission_id": None,
        "migration_report": None,
        "migration_upload_bytes": None,
        "migration_upload_name": None,
        "system_checks_df": None,
        "system_checks_summary": None,
        "last_export_payload": None,
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

    # Restore auth from query params (survives page refresh)
    if not st.session_state.get("auth_is_authenticated"):
        qp = st.query_params
        qp_role = qp.get("role")
        qp_user = qp.get("user")
        if qp_role and qp_user:
            st.session_state["auth_is_authenticated"] = True
            st.session_state["auth_role"] = qp_role
            st.session_state["auth_username"] = qp_user


def _normalize_faculty_name(name: str) -> str:
    return " ".join(name.strip().split())


def _is_valid_faculty_name(prefix: str, name: str) -> bool:
    if not prefix or not prefix.strip():
        return False
    if not name or len(name.strip()) < 2:
        return False
    return bool(re.match(r"^[A-Za-z][A-Za-z .'-]{1,}$", name.strip()))


def _resolve_admin_password() -> str | None:
    try:
        pwd = st.secrets.get("ADMIN_PASSWORD")
        if pwd:
            return str(pwd)
    except Exception:
        pass
    env_pwd = os.getenv(ADMIN_PASSWORD_ENV)
    return env_pwd if env_pwd else None


def _logout() -> None:
    st.session_state["auth_is_authenticated"] = False
    st.session_state["auth_role"] = None
    st.session_state["auth_username"] = None
    st.query_params.clear()
    _log_info("User logged out.")


def _auth_sidebar() -> tuple[str, str] | None:
    st.sidebar.header("Authentication")

    if st.session_state.get("auth_is_authenticated"):
        role = str(st.session_state.get("auth_role"))
        username = str(st.session_state.get("auth_username"))
        st.sidebar.success(f"Logged in as {username} ({role})")
        if st.sidebar.button("Logout", use_container_width=True):
            _logout()
            st.rerun()
        return role, username

    faculty_tab, admin_tab = st.sidebar.tabs(["Faculty", "Admin"])

    with faculty_tab:
        prefix_choice = st.selectbox(
            "Prefix",
            FACULTY_PREFIX_OPTIONS,
            key="faculty_login_prefix",
        )
        custom_prefix = ""
        if prefix_choice == "Other":
            custom_prefix = st.text_input(
                "Enter Custom Prefix",
                placeholder="e.g. Assoc. Prof.",
                key="faculty_login_custom_prefix",
            )
        faculty_name_input = st.text_input(
            "Full Name (without prefix)",
            value="",
            placeholder="Firstname Lastname",
            help="Enter your full name without any prefix.",
            key="faculty_login_name",
        )
        if st.button("Login as Faculty", key="faculty_login_btn", use_container_width=True):
            final_prefix = custom_prefix.strip() if prefix_choice == "Other" else prefix_choice
            if not _is_valid_faculty_name(final_prefix, faculty_name_input):
                st.sidebar.error("Please select a prefix and enter a valid name (at least 2 characters).")
            else:
                full_name = _normalize_faculty_name(f"{final_prefix} {faculty_name_input}")
                st.session_state["auth_is_authenticated"] = True
                st.session_state["auth_role"] = "faculty"
                st.session_state["auth_username"] = full_name
                st.query_params["role"] = "faculty"
                st.query_params["user"] = full_name
                _log_info(f"Faculty login success: {full_name}")
                st.rerun()

    with admin_tab:
        now = time.time()
        lockout_until = float(st.session_state.get("admin_lockout_until", 0.0) or 0.0)
        if lockout_until > now:
            remaining = int(lockout_until - now)
            st.warning(f"Too many failed attempts. Try again in {remaining}s.")
        admin_password_input = st.text_input("Admin Password", type="password", key="admin_login_password")
        if st.button("Login as Admin", key="admin_login_btn", use_container_width=True):
            if lockout_until > now:
                st.sidebar.error("Admin login is temporarily locked.")
            else:
                expected_password = _resolve_admin_password()
                if not expected_password:
                    st.sidebar.error(
                        f"Admin password is not configured. Set ADMIN_PASSWORD in Streamlit secrets or {ADMIN_PASSWORD_ENV}."
                    )
                elif not hmac.compare_digest(admin_password_input, expected_password):
                    failed = int(st.session_state.get("admin_failed_attempts", 0)) + 1
                    st.session_state["admin_failed_attempts"] = failed
                    _log_error(f"Admin login failed (attempt {failed}).")
                    if failed >= ADMIN_MAX_FAILED_ATTEMPTS:
                        st.session_state["admin_lockout_until"] = now + ADMIN_LOCKOUT_SECONDS
                        st.session_state["admin_failed_attempts"] = 0
                        st.sidebar.error("Too many failed attempts. Admin login locked for 5 minutes.")
                    else:
                        remaining = ADMIN_MAX_FAILED_ATTEMPTS - failed
                        st.sidebar.error(f"Invalid admin password. Attempts left: {remaining}")
                else:
                    st.session_state["auth_is_authenticated"] = True
                    st.session_state["auth_role"] = "admin"
                    st.session_state["auth_username"] = "admin"
                    st.session_state["admin_failed_attempts"] = 0
                    st.session_state["admin_lockout_until"] = 0.0
                    st.query_params["role"] = "admin"
                    st.query_params["user"] = "admin"
                    _log_info("Admin login success.")
                    st.rerun()

    return None


def _safe_dataframe(df: pd.DataFrame, msg: str = "No data available.") -> None:
    if df.empty:
        st.info(msg)
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)


def _build_filters(df: pd.DataFrame) -> PublicationFilters:
    faculty_options = [""] + sorted(df.get("faculty_name", pd.Series(dtype=str)).dropna().unique().tolist())
    category_options = [""] + sorted(df.get("category", pd.Series(dtype=str)).dropna().unique().tolist())
    type_options = [""] + sorted(df.get("publication_type", pd.Series(dtype=str)).dropna().unique().tolist())

    c1, c2, c3 = st.columns(3)
    st.session_state["flt_faculty"] = c1.selectbox("Faculty", faculty_options, index=max(0, faculty_options.index(st.session_state["flt_faculty"])) if st.session_state["flt_faculty"] in faculty_options else 0)
    st.session_state["flt_category"] = c2.selectbox("Category", category_options, index=max(0, category_options.index(st.session_state["flt_category"])) if st.session_state["flt_category"] in category_options else 0)
    st.session_state["flt_pub_type"] = c3.selectbox("Type", type_options, index=max(0, type_options.index(st.session_state["flt_pub_type"])) if st.session_state["flt_pub_type"] in type_options else 0)

    st.session_state["flt_keyword"] = st.text_input("Keyword", value=st.session_state["flt_keyword"])

    with st.popover("Advanced Filters", use_container_width=True):
        indexing_options = [""] + sorted(df.get("indexing_source", pd.Series(dtype=str)).dropna().unique().tolist())
        national_options = [""] + sorted(df.get("national_international", pd.Series(dtype=str)).dropna().unique().tolist())
        quartile_options = [""] + sorted(df.get("quartile", pd.Series(dtype=str)).dropna().unique().tolist())

        st.session_state["flt_indexing"] = st.selectbox(
            "Indexing Source",
            indexing_options,
            index=max(0, indexing_options.index(st.session_state["flt_indexing"])) if st.session_state["flt_indexing"] in indexing_options else 0,
            key="adv_indexing",
        )
        st.session_state["flt_national"] = st.selectbox(
            "National/International",
            national_options,
            index=max(0, national_options.index(st.session_state["flt_national"])) if st.session_state["flt_national"] in national_options else 0,
            key="adv_national",
        )
        st.session_state["flt_quartile"] = st.selectbox(
            "Quartile",
            quartile_options,
            index=max(0, quartile_options.index(st.session_state["flt_quartile"])) if st.session_state["flt_quartile"] in quartile_options else 0,
            key="adv_quartile",
        )
        st.session_state["flt_date_from"] = st.date_input("Date From", value=st.session_state["flt_date_from"], key="adv_date_from")
        st.session_state["flt_date_to"] = st.date_input("Date To", value=st.session_state["flt_date_to"], key="adv_date_to")

    return PublicationFilters(
        faculty_name=st.session_state["flt_faculty"] or None,
        category=st.session_state["flt_category"] or None,
        publication_type=st.session_state["flt_pub_type"] or None,
        indexing_source=st.session_state["flt_indexing"] or None,
        national_international=st.session_state["flt_national"] or None,
        quartile=st.session_state["flt_quartile"] or None,
        keyword=st.session_state["flt_keyword"] or None,
        date_from=st.session_state["flt_date_from"] if isinstance(st.session_state["flt_date_from"], date) else None,
        date_to=st.session_state["flt_date_to"] if isinstance(st.session_state["flt_date_to"], date) else None,
    )


def _reset_filters() -> None:
    for key in [
        "flt_faculty",
        "flt_category",
        "flt_pub_type",
        "flt_indexing",
        "flt_national",
        "flt_quartile",
        "flt_keyword",
        "flt_date_from",
        "flt_date_to",
    ]:
        st.session_state[key] = "" if isinstance(st.session_state[key], str) else None


def _dashboard_page() -> None:
    st.title("Faculty Publication Manager")
    username = st.session_state["auth_username"]
    role = st.session_state["auth_role"]
    with session_scope(DB_PATH) as session:
        metrics = get_dashboard_metrics(session)
        faculty_df = get_faculty_analysis_df(session)

    # Personalized faculty greeting
    if role == "faculty" and not faculty_df.empty:
        my_row = faculty_df[faculty_df["faculty_name"] == username]
        if not my_row.empty:
            r = my_row.iloc[0]
            st.subheader(f"Welcome, {username}!")
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("My Publications", int(r.get("total_publications", 0)))
            m2.metric("Journals", int(r.get("journal_count", 0)))
            m3.metric("Conferences", int(r.get("conference_count", 0)))
            m4.metric("Book Chapters", int(r.get("book_chapter_count", 0)))
            m5.metric("Pending", int(r.get("pending_count", 0)))
            st.divider()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Publications", metrics["total_publications"])
    c2.metric("Pending Reviews", metrics["pending_reviews"])
    c3.metric("Missing DOI", metrics["data_health"]["missing_doi_count"])
    c4.metric("Missing Pub Date", metrics["data_health"]["missing_pub_date_count"])
    c5.metric("Manual Cleanup Flags", metrics["data_health"]["manual_cleanup_count"])

    # Charts
    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        st.subheader("Publications by Category")
        if not metrics["by_category"].empty:
            donut = (
                alt.Chart(metrics["by_category"])
                .mark_arc(innerRadius=50, outerRadius=120)
                .encode(
                    theta=alt.Theta("count:Q"),
                    color=alt.Color("category:N", legend=alt.Legend(title="Category")),
                    tooltip=["category:N", "count:Q"],
                )
                .properties(height=300)
            )
            st.altair_chart(donut, use_container_width=True)
        else:
            st.info("No category data available.")

    with chart_col2:
        st.subheader("Publications by Year")
        if not metrics["by_year"].empty:
            year_bar = (
                alt.Chart(metrics["by_year"])
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("year:O", title="Year"),
                    y=alt.Y("count:Q", title="Publications"),
                    color=alt.value("#1565C0"),
                    tooltip=["year:O", "count:Q"],
                )
                .properties(height=300)
            )
            st.altair_chart(year_bar, use_container_width=True)
        else:
            st.info("No year data available.")

    st.subheader("Faculty-wise Analysis")
    if faculty_df.empty:
        st.info("No faculty analysis data available.")
    else:
        _safe_dataframe(faculty_df, "No faculty analytics available.")
        faculty_options = faculty_df["faculty_name"].tolist()
        selected_faculty = st.selectbox("Faculty Drill-down", faculty_options, key="faculty_drilldown")
        with session_scope(DB_PATH) as session:
            drilldown = get_faculty_drilldown(session, selected_faculty)
        k1, k2, k3, k4, k5, k6 = st.columns(6)
        k1.metric("Publications", drilldown["kpis"]["publications"])
        k2.metric("Journals", drilldown["kpis"]["journals"])
        k3.metric("Conferences", drilldown["kpis"]["conferences"])
        k4.metric("Book Chapters", drilldown["kpis"]["book_chapters"])
        k5.metric("Pending Reviews", drilldown["kpis"]["pending_reviews"])
        k6.metric("Rejected", drilldown["kpis"]["rejected"])
        st.caption(f"Analytics generated for `{selected_faculty}` as viewed by `{username}`.")
        if not drilldown["trend"].empty:
            st.line_chart(drilldown["trend"].set_index("year")["count"])
        _safe_dataframe(drilldown["latest"], "No latest records for selected faculty.")


@st.dialog("Edit Publication", width="large")
def _edit_publication_dialog(pub_id: int) -> None:
    with session_scope(DB_PATH) as read_session:
        core = read_session.get(PublicationCore, pub_id)
        legacy = read_session.get(Publication, pub_id) if not core else None
        record = core or legacy
        if not record:
            st.error(f"Publication {pub_id} not found.")
            return
        d = {
            "title": record.title or "",
            "faculty_name": record.faculty_name or "",
            "authors": record.authors or "",
            "publication_name": record.publication_name or "",
            "doi": record.doi or "",
            "pub_date": str(record.pub_date or ""),
            "paper_url": record.paper_url or "",
            "category": record.category or "",
            "publication_type": record.publication_type or "",
            "venue": record.venue or "",
            "indexing_source": record.indexing_source or "",
            "national_international": record.national_international or "",
        }
    c1, c2 = st.columns(2)
    new_title = c1.text_input("Title", value=d["title"], key=f"et_{pub_id}")
    new_faculty = c2.text_input("Faculty Name", value=d["faculty_name"], key=f"ef_{pub_id}")
    c1, c2 = st.columns(2)
    new_authors = c1.text_area("Authors", value=d["authors"], key=f"ea_{pub_id}", height=80)
    new_pub_name = c2.text_input("Publication Name", value=d["publication_name"], key=f"ep_{pub_id}")
    c1, c2 = st.columns(2)
    new_doi = c1.text_input("DOI", value=d["doi"], key=f"ed_{pub_id}")
    new_pub_date = c2.text_input("Publication Date (YYYY-MM-DD)", value=d["pub_date"], key=f"epd_{pub_id}")
    c1, c2 = st.columns(2)
    new_paper_url = c1.text_input("Paper URL", value=d["paper_url"], key=f"eu_{pub_id}")
    new_venue = c2.text_input("Venue", value=d["venue"], key=f"ev_{pub_id}")
    c1, c2 = st.columns(2)
    new_category = c1.text_input("Category", value=d["category"], key=f"ec_{pub_id}")
    new_pub_type = c2.text_input("Publication Type", value=d["publication_type"], key=f"ept_{pub_id}")
    c1, c2 = st.columns(2)
    new_indexing = c1.text_input("Indexing Source", value=d["indexing_source"], key=f"ei_{pub_id}")
    new_nat_int = _dropdown(c2, "National/International", _NAT_INT_OPTIONS, d["national_international"], f"eni_{pub_id}")

    if st.button("Save Changes", type="primary", key=f"save_{pub_id}"):
        with session_scope(DB_PATH) as session:
            record = session.get(PublicationCore, pub_id) or session.get(Publication, pub_id)
            if not record:
                st.error("Publication not found.")
                return
            record.title = new_title
            record.faculty_name = new_faculty
            record.authors = new_authors
            record.publication_name = new_pub_name
            record.doi = new_doi
            record.doi_normalized = normalize_doi(new_doi)
            record.pub_date = parse_date(new_pub_date)
            record.paper_url = new_paper_url
            record.venue = new_venue
            record.category = new_category
            record.publication_type = new_pub_type
            record.indexing_source = new_indexing
            record.national_international = new_nat_int or None
            record.updated_at = datetime.now(timezone.utc)
        _log_info(f"Publication {pub_id} updated by admin.")
        st.success(f"Publication {pub_id} updated successfully!")
        st.rerun()


@st.dialog("Confirm Delete")
def _delete_publication_dialog(pub_id: int) -> None:
    st.warning(f"⚠️ Are you sure you want to **permanently delete** Publication #{pub_id}?")
    st.caption("This action cannot be undone.")
    if st.button("Confirm Delete", type="primary", key=f"confirm_del_{pub_id}"):
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
                st.error("Publication not found.")
                return
        _log_info(f"Publication {pub_id} deleted by admin.")
        st.success(f"Publication {pub_id} deleted.")
        st.rerun()


def _publications_page() -> None:
    st.header("Publications Explorer")
    username = st.session_state["auth_username"]
    with session_scope(DB_PATH) as session:
        all_df = get_publications_df(session, PublicationFilters())
        filters = _build_filters(all_df)
        filtered_df = get_publications_df(session, filters)

        action = st.menu_button(
            "Actions",
            options=["Export Full DB (Official Format)", "Export Filtered (Official Format)", "Reset Filters"],
            icon=":material/menu:",
            type="secondary",
        )
        if action == "Reset Filters":
            _reset_filters()
            st.rerun()
        if action == "Export Full DB (Official Format)":
            try:
                payload, metadata = export_official_format_xlsx(
                    session,
                    username,
                    template_path=DEFAULT_EXCEL,
                    filters=None,
                )
                st.session_state["last_export_payload"] = {
                    "filename": "publications_official_full.xlsx",
                    "bytes": payload,
                    "rows": metadata["row_count"],
                }
                _log_info(
                    f"Official-format full export triggered by {username} with {metadata['row_count']} rows "
                    f"using template {metadata['template_path']}."
                )
            except FileNotFoundError as exc:
                st.error(str(exc))
                _log_error(f"Official export failed: {exc}")
        if action == "Export Filtered (Official Format)":
            try:
                payload, metadata = export_official_format_xlsx(
                    session,
                    username,
                    template_path=DEFAULT_EXCEL,
                    filters=filters,
                )
                st.session_state["last_export_payload"] = {
                    "filename": "publications_official_filtered.xlsx",
                    "bytes": payload,
                    "rows": metadata["row_count"],
                }
                _log_info(
                    f"Official-format filtered export triggered by {username} with {metadata['row_count']} rows "
                    f"using template {metadata['template_path']}."
                )
            except FileNotFoundError as exc:
                st.error(str(exc))
                _log_error(f"Official filtered export failed: {exc}")

        if not filtered_df.empty:
            st.dataframe(
                filtered_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "paper_url": column_config.LinkColumn("Paper URL", display_text="Open link"),
                    "doi": column_config.TextColumn("DOI"),
                    "pub_date": column_config.DateColumn("Publication Date"),
                },
                on_select="ignore",
                selection_mode="multi-row",
            )
        else:
            st.info("No publications match current filters.")

        export_info = st.session_state.get("last_export_payload")
        if export_info:
            st.download_button(
                f"Download {export_info['filename']} ({export_info['rows']} rows)",
                export_info["bytes"],
                file_name=export_info["filename"],
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
            )

    # Publication detail view
    if not filtered_df.empty:
        st.divider()
        pub_view_id = st.number_input("Publication ID", min_value=1, step=1, key="view_pub_id_input")
        if st.button("📄 View Details", key="view_pub_details_btn"):
            st.session_state["view_pub_id"] = int(pub_view_id)

        view_id = st.session_state.get("view_pub_id")
        if view_id:
            pub_row = filtered_df[filtered_df["id"] == view_id]
            if not pub_row.empty:
                with st.expander(f"📄 Publication #{view_id} — Full Details", expanded=True):
                    row = pub_row.iloc[0]
                    detail_cols = st.columns(2)
                    items = [(col, row[col]) for col in pub_row.columns if pd.notna(row[col]) and str(row[col]).strip()]
                    for i, (col, val) in enumerate(items):
                        detail_cols[i % 2].markdown(f"**{col.replace('_', ' ').title()}**: {val}")
            else:
                st.warning(f"Publication #{view_id} not in current filtered view. Try resetting filters.")

        # Admin: Edit & Delete
        if st.session_state["auth_role"] == "admin":
            st.divider()
            st.subheader("🔧 Manage Publication")
            manage_col1, manage_col2 = st.columns(2)
            if manage_col1.button("✏️ Edit Publication", key="edit_pub_btn", use_container_width=True):
                _edit_publication_dialog(int(pub_view_id))
            if manage_col2.button("🗑️ Delete Publication", key="delete_pub_btn", type="primary", use_container_width=True):
                _delete_publication_dialog(int(pub_view_id))


def _faculty_new_submission() -> None:
    st.header("New Submission")
    username = st.session_state["auth_username"]

    workbook_categories: dict[str, list[str]] = {}
    for config in SHEET_CONFIGS.values():
        cat = "Book Chapter" if config.category == "Book" else config.category
        workbook_categories.setdefault(cat, [])
        if config.publication_type not in workbook_categories[cat]:
            workbook_categories[cat].append(config.publication_type)
    # Add "Other" category
    if "Other" not in workbook_categories:
        workbook_categories["Other"] = ["Journal", "Conference", "Book Chapter"]

    category = st.selectbox("Category", list(workbook_categories.keys()), key="faculty_workbook_category")
    sub_category = st.selectbox(
        "Sub Category",
        workbook_categories.get(category, ["Journal"]),
        key="faculty_workbook_subcategory",
    )

    input_mode = st.radio("Input Mode", ["URL", "Manual"], horizontal=True, key="faculty_input_mode")
    source_input = ""
    if input_mode == "URL":
        source_input = st.text_input("Paper URL / DOI", key="faculty_source_input")

    faculty_name = st.text_input("Faculty Name", value=username, key="faculty_new_name_entry")

    if st.button("Parse/Prepare"):
        if input_mode == "Manual":
            st.session_state["ingestion_payload"] = {
                "faculty_name": faculty_name,
                "category": category,
                "publication_type": sub_category,
                "indexing_source": category,
            }
            st.session_state["ingestion_confidence"] = 0.3
            st.session_state["ingestion_warnings"] = []
            st.session_state["ingestion_input_method"] = InputMethod.MANUAL.value
            st.session_state["ingestion_source_input"] = None
        else:
            if not source_input.strip():
                st.error("Enter a URL or DOI before parsing.")
                return
            method = InputMethod.DOI if normalize_doi(source_input) else InputMethod.URL
            try:
                result = ingest_source(source_input=source_input, input_method=method.value, faculty_name=faculty_name)
                payload = dict(result.payload)
                payload["faculty_name"] = faculty_name
                payload["category"] = category
                payload["publication_type"] = sub_category
                payload["indexing_source"] = payload.get("indexing_source") or category
                st.session_state["ingestion_payload"] = payload
                st.session_state["ingestion_confidence"] = result.confidence_score
                st.session_state["ingestion_warnings"] = result.warnings
                st.session_state["ingestion_input_method"] = method.value
                st.session_state["ingestion_source_input"] = source_input
            except Exception as exc:
                _log_error(f"Ingestion failed for {username}: {exc}")
                st.error(f"Ingestion failed: {exc}")

    payload = dict(st.session_state.get("ingestion_payload", {}))
    if payload:
        st.subheader("Review and Edit Before Submit")
        payload["category"] = category
        payload["publication_type"] = sub_category
        payload["indexing_source"] = payload.get("indexing_source") or category

        st.caption(f"Category: {category} | Sub Category: {sub_category}")
        c1, c2 = st.columns(2)
        payload["faculty_name"] = c1.text_input(
            "Faculty Name",
            value=payload.get("faculty_name", faculty_name),
            key="faculty_new_name_review",
        )
        payload["title"] = c2.text_input("Paper Title", value=payload.get("title", ""), key="faculty_review_title")

        c1, c2 = st.columns(2)
        payload["publication_name"] = c1.text_input(
            "Journal/Conference/Book Name",
            value=payload.get("publication_name", payload.get("venue", "")),
            key="faculty_review_publication_name",
        )
        payload["authors"] = c2.text_area(
            "Authors",
            value=payload.get("authors", ""),
            key="faculty_review_authors",
            height=80,
        )

        c1, c2 = st.columns(2)
        pub_date_default = parse_date(payload.get("pub_date"))
        selected_pub_date = c1.date_input(
            "Publication Date",
            value=pub_date_default,
            key="faculty_review_pub_date",
            format="YYYY-MM-DD",
        )
        payload["pub_date"] = selected_pub_date.isoformat() if selected_pub_date else None
        payload["doi"] = c2.text_input("DOI", value=payload.get("doi", ""), key="faculty_review_doi")

        if sub_category in ("Journal", "Conference"):
            payload["national_international"] = _dropdown(
                st, "National/International", _NAT_INT_OPTIONS,
                payload.get("national_international", ""), "faculty_review_nat_int",
            )

        if sub_category == "Journal":
            c1, c2 = st.columns(2)
            payload["quartile"] = _dropdown(c1, "Quartile", _QUARTILE_OPTIONS, payload.get("quartile", ""), "faculty_review_quartile")
            payload["issn_isbn"] = c2.text_input("ISSN", value=payload.get("issn_isbn", ""), key="faculty_review_issn")

            c1, c2 = st.columns(2)
            payload["volume_issue"] = c1.text_input(
                "Volume/Issue",
                value=payload.get("volume_issue", ""),
                key="faculty_review_volume_issue_journal",
            )
            payload["official_venue_url"] = c2.text_input(
                "Journal Official URL",
                value=payload.get("official_venue_url", ""),
                key="faculty_review_official_url_journal",
            )
            payload["venue"] = payload.get("venue") or payload.get("publication_name")

        elif sub_category == "Conference":
            c1, c2 = st.columns(2)
            payload["venue"] = c1.text_input("Venue", value=payload.get("venue", ""), key="faculty_review_venue_conference")
            payload["conference_date"] = c2.text_input(
                "Conference Date",
                value=payload.get("conference_date", ""),
                key="faculty_review_conference_date",
            )

            c1, c2 = st.columns(2)
            payload["issn_isbn"] = c1.text_input("ISSN/ISBN", value=payload.get("issn_isbn", ""), key="faculty_review_issn_isbn_conf")
            payload["volume_issue"] = c2.text_input(
                "Volume/Issue",
                value=payload.get("volume_issue", ""),
                key="faculty_review_volume_issue_conference",
            )
            payload["official_venue_url"] = st.text_input(
                "Conference Official URL",
                value=payload.get("official_venue_url", ""),
                key="faculty_review_official_url_conference",
            )

        elif sub_category == "Book Chapter":
            c1, c2 = st.columns(2)
            payload["publisher"] = c1.text_input("Publisher", value=payload.get("publisher", ""), key="faculty_review_publisher")
            payload["issn_isbn"] = c2.text_input("ISBN", value=payload.get("issn_isbn", ""), key="faculty_review_isbn")
            payload["official_venue_url"] = st.text_input(
                "Book URL",
                value=payload.get("official_venue_url", ""),
                key="faculty_review_book_url",
            )
            payload["venue"] = payload.get("venue") or payload.get("publisher")

        # ── Publication Status Pipeline ──────────────────────────────
        st.divider()
        st.subheader("📋 Publication Status Pipeline")

        # Step 1: Accepted / Presented
        step1_label = "Is your paper presented/accepted?" if sub_category == "Conference" else "Is your paper accepted?"
        step1_options = _PRESENTED_OPTIONS if sub_category == "Conference" else _ACCEPTED_OPTIONS
        step1_key = "faculty_pipeline_accepted"
        payload["presented_accepted_flag"] = _dropdown(
            st, f"**Step 1:** {step1_label}", step1_options,
            payload.get("presented_accepted_flag", ""), step1_key,
        )
        is_accepted = payload["presented_accepted_flag"] in ("Yes", "Presented", "Accepted")

        if is_accepted:
            acceptance_proof = st.file_uploader(
                "📎 Upload acceptance/presentation proof",
                type=_PROOF_FILE_TYPES,
                key="faculty_pipeline_acceptance_proof",
                help="Upload acceptance letter, certificate, or screenshot (PDF/PNG/JPG)",
            )
            if acceptance_proof:
                payload["_acceptance_proof_pending"] = True
                st.session_state["_acceptance_proof_file"] = acceptance_proof
            if payload.get("certificate_ref"):
                st.caption(f"📄 Existing proof reference: {payload['certificate_ref']}")

        # Step 2: Published (only if accepted)
        if is_accepted:
            payload["research_published_flag"] = _dropdown(
                st, "**Step 2:** Is your paper published?", _YESNO_OPTIONS,
                payload.get("research_published_flag", ""), "faculty_pipeline_published",
            )
            is_published = payload["research_published_flag"] == "Yes"

            if is_published:
                payload["paper_url"] = st.text_input(
                    "🔗 Publication Link",
                    value=payload.get("paper_url", ""),
                    key="faculty_pipeline_pub_link",
                    help="Direct link to the published paper",
                )
                publication_proof = st.file_uploader(
                    "📎 Upload publication proof (optional)",
                    type=_PROOF_FILE_TYPES,
                    key="faculty_pipeline_pub_proof",
                    help="Upload first page, DOI screenshot, or publisher confirmation",
                )
                if publication_proof:
                    payload["_publication_proof_pending"] = True
                    st.session_state["_publication_proof_file"] = publication_proof
                if payload.get("attachment_ref"):
                    st.caption(f"📄 Existing attachment: {payload['attachment_ref']}")
        else:
            is_published = False

        # Step 3: Indexed (only if published)
        if is_accepted and is_published:
            payload["indexing_flag"] = _dropdown(
                st, "**Step 3:** Is your paper indexed?", _YESNO_OPTIONS,
                payload.get("indexing_flag", ""), "faculty_pipeline_indexed",
            )
            is_indexed = payload["indexing_flag"] == "Yes"

            if is_indexed:
                payload["indexing_source"] = _dropdown(
                    st, "Select Indexing", _INDEXING_OPTIONS,
                    payload.get("indexing_source", ""), "faculty_pipeline_indexing_type",
                )
                # Auto-derive category from indexing selection
                if payload["indexing_source"] and payload["indexing_source"] != "Other":
                    payload["category"] = payload["indexing_source"]

                indexing_proof = st.file_uploader(
                    "📎 Upload indexing proof",
                    type=_PROOF_FILE_TYPES,
                    key="faculty_pipeline_indexing_proof",
                    help="Upload Scopus/WoS listing screenshot or indexing certificate",
                )
                if indexing_proof:
                    payload["_indexing_proof_pending"] = True
                    st.session_state["_indexing_proof_file"] = indexing_proof
                if payload.get("indexing_proof"):
                    st.caption(f"📄 Existing indexing proof: {payload['indexing_proof']}")

                # Book-specific indexing details
                if sub_category == "Book Chapter" and is_indexed:
                    c1, c2, c3 = st.columns(3)
                    payload["book_indexed_ugc"] = _dropdown(c1, "Indexed in UGC", _YESNO_OPTIONS, payload.get("book_indexed_ugc", ""), "faculty_review_book_ugc")
                    payload["book_indexed_scopus"] = _dropdown(c2, "Indexed in Scopus", _YESNO_OPTIONS, payload.get("book_indexed_scopus", ""), "faculty_review_book_scopus")
                    payload["book_indexed_wos"] = _dropdown(c3, "Indexed in WoS", _YESNO_OPTIONS, payload.get("book_indexed_wos", ""), "faculty_review_book_wos")
        else:
            is_indexed = False

        # Show pipeline summary
        st.divider()
        cols = st.columns(3)
        cols[0].metric("Accepted", "✅ Yes" if is_accepted else "❌ No")
        cols[1].metric("Published", "✅ Yes" if is_published else "⏳ Pending" if is_accepted else "—")
        cols[2].metric("Indexed", "✅ Yes" if is_indexed else "⏳ Pending" if is_published else "—")

        confidence = float(st.session_state.get("ingestion_confidence", 0.0))
        warnings = st.session_state.get("ingestion_warnings", [])
        for warning in warnings:
            st.warning(warning)

        if st.button("Submit for Admin Review", type="primary"):
            # Required field validation
            missing_fields = []
            if not payload.get("title", "").strip():
                missing_fields.append("Paper Title")
            if not payload.get("publication_name", "").strip() and not payload.get("venue", "").strip():
                missing_fields.append("Journal/Conference/Book Name")
            if not payload.get("authors", "").strip():
                missing_fields.append("Authors")
            if not payload.get("faculty_name", "").strip():
                missing_fields.append("Faculty Name")
            if missing_fields:
                st.error(f"Please fill in the required fields: **{', '.join(missing_fields)}**")
                return

            # Save uploaded proof files
            if st.session_state.get("_acceptance_proof_file"):
                path = _save_uploaded_file(st.session_state["_acceptance_proof_file"])
                if path:
                    payload["certificate_ref"] = path
                st.session_state.pop("_acceptance_proof_file", None)
            if st.session_state.get("_publication_proof_file"):
                path = _save_uploaded_file(st.session_state["_publication_proof_file"])
                if path:
                    payload["attachment_ref"] = path
                st.session_state.pop("_publication_proof_file", None)
            if st.session_state.get("_indexing_proof_file"):
                path = _save_uploaded_file(st.session_state["_indexing_proof_file"])
                if path:
                    payload["indexing_proof"] = path
                st.session_state.pop("_indexing_proof_file", None)

            # Clean up internal flags
            payload.pop("_acceptance_proof_pending", None)
            payload.pop("_publication_proof_pending", None)
            payload.pop("_indexing_proof_pending", None)
            try:
                selected_method = st.session_state.get("ingestion_input_method")
                if not selected_method:
                    selected_method = InputMethod.MANUAL.value if input_mode == "Manual" else InputMethod.URL.value
                with session_scope(DB_PATH) as session:
                    submission = create_submission(
                        session=session,
                        submitted_by=username,
                        source_input=st.session_state.get("ingestion_source_input") or source_input,
                        source_input_method=InputMethod(selected_method),
                        payload=payload,
                        confidence_score=confidence,
                        as_draft=False,
                    )
                    st.success(f"Submission {submission.id} sent for review.")
                    _log_info(f"Submission {submission.id} created by {username}.")
                st.session_state["ingestion_payload"] = {}
                st.session_state["ingestion_input_method"] = None
                st.session_state["ingestion_source_input"] = None
            except ValueError as exc:
                st.error(str(exc))
                _log_error(f"Submission validation failed for {username}: {exc}")


def _faculty_my_submissions() -> None:
    st.header("My Submissions")
    username = st.session_state["auth_username"]
    with session_scope(DB_PATH) as session:
        rows = session.execute(
            select(PendingSubmission).where(PendingSubmission.submitted_by == username).order_by(PendingSubmission.created_at.desc())
        ).scalars()
        data = []
        for row in rows:
            data.append(
                {
                    "id": row.id,
                    "status": row.status,
                    "confidence": row.confidence_score,
                    "reviewed_by": row.reviewed_by,
                    "review_note": row.review_note,
                    "created_at": row.created_at,
                    "updated_at": row.updated_at,
                }
            )
    _safe_dataframe(pd.DataFrame(data), "No submissions yet.")


@st.dialog("Approve Submission")
def _approve_dialog(submission_id: int, username: str, payload: dict[str, Any]) -> None:
    note = st.text_area("Approval Note", key=f"approve_note_{submission_id}")
    override_soft_duplicate = st.checkbox("Override soft duplicate", key=f"override_{submission_id}")
    if st.button("Confirm Approval", type="primary"):
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
                message = result.warnings[0] if result.warnings else "Hard duplicate (DOI) found. Approval blocked."
                st.error(message)
            elif result.soft_duplicate and result.publication_id is None:
                st.warning("Soft duplicate found. Enable override to continue.")
            else:
                _log_info(f"Submission {submission_id} approved by {username}.")
                st.success(f"Approved. Publication ID: {result.publication_id}")
                st.rerun()
        except ValueError as exc:
            st.error(str(exc))
            _log_error(f"Approval validation failed for submission {submission_id}: {exc}")


@st.dialog("Reject Submission")
def _reject_dialog(submission_id: int, username: str) -> None:
    note = st.text_area("Rejection Note", key=f"reject_note_{submission_id}")
    if st.button("Confirm Rejection"):
        with session_scope(DB_PATH) as session:
            reject_submission(session, submission_id, username, note or "Rejected by admin.")
        _log_info(f"Submission {submission_id} rejected by {username}.")
        st.warning("Submission rejected.")
        st.rerun()


def _admin_review_queue() -> None:
    st.header("Review Queue")
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
                    "submitted_by": row.submitted_by,
                    "status": row.status,
                    "title": payload.get("title"),
                    "faculty_name": payload.get("faculty_name"),

                    "created_at": row.created_at,
                }
            )
        _safe_dataframe(pd.DataFrame(queue_rows), "No pending submissions.")

        submission_id = st.number_input("Open Submission ID", min_value=1, step=1)
        if st.button("Load Submission"):
            st.session_state["admin_submission_id"] = int(submission_id)

    sub_id = st.session_state.get("admin_submission_id")
    if sub_id:
        _admin_submission_detail(sub_id, username)


def _admin_submission_detail(submission_id: int, username: str) -> None:
    st.subheader(f"Submission Detail: {submission_id}")
    move_to_review = False
    with session_scope(DB_PATH) as session:
        submission = session.get(PendingSubmission, submission_id)
        if not submission:
            st.error("Submission not found.")
            return
        if submission.status == SubmissionStatus.SUBMITTED.value:
            move_to_review = st.button("Move to UNDER_REVIEW")
            if move_to_review:
                start_review(session, submission_id, username)
                _log_info(f"Submission {submission_id} moved to UNDER_REVIEW by {username}.")

    if move_to_review:
        st.success("Submission moved to UNDER_REVIEW.")
        st.rerun()

    with session_scope(DB_PATH) as session:
        submission = session.get(PendingSubmission, submission_id)
        if not submission:
            st.error("Submission not found.")
            return

        payload = dict(submission.parsed_payload_json or {})

        workbook_categories: dict[str, list[str]] = {}
        for config in SHEET_CONFIGS.values():
            cat = "Book Chapter" if config.category == "Book" else config.category
            workbook_categories.setdefault(cat, [])
            if config.publication_type not in workbook_categories[cat]:
                workbook_categories[cat].append(config.publication_type)
        if "Other" not in workbook_categories:
            workbook_categories["Other"] = ["Journal", "Conference", "Book Chapter"]

        category_options = list(workbook_categories.keys())
        current_category = payload.get("category")
        # Map legacy "Book" to "Book Chapter"
        if current_category == "Book":
            current_category = "Book Chapter"
        if current_category not in category_options:
            current_category = category_options[0]

        category = st.selectbox(
            "Category",
            category_options,
            index=category_options.index(current_category),
            key=f"admin_category_{submission_id}",
        )
        sub_options = workbook_categories.get(category, ["Journal"])
        current_sub_category = payload.get("publication_type")
        if current_sub_category not in sub_options:
            current_sub_category = sub_options[0]
        sub_category = st.selectbox(
            "Sub Category",
            sub_options,
            index=sub_options.index(current_sub_category),
            key=f"admin_sub_category_{submission_id}",
        )

        payload["category"] = category
        payload["publication_type"] = sub_category

        payload["faculty_name"] = st.text_input("Faculty Name", value=payload.get("faculty_name", ""), key=f"f_{submission_id}")
        payload["title"] = st.text_input("Paper Title", value=payload.get("title", ""), key=f"t_{submission_id}")
        payload["publication_name"] = st.text_input(
            "Journal/Conference/Book Name",
            value=payload.get("publication_name", payload.get("venue", "")),
            key=f"pn_{submission_id}",
        )
        payload["authors"] = st.text_area("Authors", value=payload.get("authors", ""), key=f"a_{submission_id}")
        payload["pub_date"] = st.text_input("Publication Date", value=str(payload.get("pub_date", "")), key=f"d_{submission_id}")
        payload["doi"] = st.text_input("DOI", value=payload.get("doi", ""), key=f"doi_{submission_id}")
        payload["paper_url"] = st.text_input("Paper URL", value=payload.get("paper_url", ""), key=f"url_{submission_id}")
        payload["indexing_source"] = st.text_input(
            "Indexing Source",
            value=payload.get("indexing_source", category),
            key=f"idx_{submission_id}",
        )

        if sub_category in ("Journal", "Conference"):
            payload["national_international"] = _dropdown(
                st, "National/International", _NAT_INT_OPTIONS,
                payload.get("national_international", ""), f"nat_{submission_id}",
            )

        if sub_category == "Journal":
            payload["quartile"] = _dropdown(st, "Quartile", _QUARTILE_OPTIONS, payload.get("quartile", ""), f"q_{submission_id}")
            payload["volume_issue"] = st.text_input(
                "Volume/Issue",
                value=payload.get("volume_issue", ""),
                key=f"vj_{submission_id}",
            )
            payload["official_venue_url"] = st.text_input(
                "Journal Official URL",
                value=payload.get("official_venue_url", ""),
                key=f"jou_{submission_id}",
            )
            payload["research_published_flag"] = _dropdown(
                st, "Research Published", _YESNO_OPTIONS,
                payload.get("research_published_flag", ""), f"jpub_{submission_id}",
            )
            payload["indexing_flag"] = _dropdown(
                st, "Indexing Flag", _YESNO_OPTIONS,
                payload.get("indexing_flag", ""), f"jif_{submission_id}",
            )
            payload["indexing_proof"] = st.text_input(
                "Indexing Proof",
                value=payload.get("indexing_proof", ""),
                key=f"jip_{submission_id}",
            )
            payload["issn_isbn"] = st.text_input("ISSN", value=payload.get("issn_isbn", ""), key=f"is_{submission_id}")
            payload["attachment_ref"] = st.text_input(
                "Attachment Reference",
                value=payload.get("attachment_ref", ""),
                key=f"jar_{submission_id}",
            )
            payload["venue"] = payload.get("venue") or payload.get("publication_name")

        elif sub_category == "Conference":
            payload["venue"] = st.text_input("Venue", value=payload.get("venue", ""), key=f"v_{submission_id}")
            payload["conference_date"] = st.text_input(
                "Conference Date",
                value=payload.get("conference_date", ""),
                key=f"cd_{submission_id}",
            )
            payload["presented_accepted_flag"] = _dropdown(
                st, "Presented/Accepted", _PRESENTED_OPTIONS,
                payload.get("presented_accepted_flag", ""), f"cpaf_{submission_id}",
            )
            payload["volume_issue"] = st.text_input(
                "Volume/Issue",
                value=payload.get("volume_issue", ""),
                key=f"cv_{submission_id}",
            )
            payload["official_venue_url"] = st.text_input(
                "Conference Official URL",
                value=payload.get("official_venue_url", ""),
                key=f"cou_{submission_id}",
            )
            payload["research_published_flag"] = _dropdown(
                st, "Research Published", _YESNO_OPTIONS,
                payload.get("research_published_flag", ""), f"cpub_{submission_id}",
            )
            payload["indexing_flag"] = _dropdown(
                st, "Indexing Flag", _YESNO_OPTIONS,
                payload.get("indexing_flag", ""), f"cif_{submission_id}",
            )
            payload["indexing_proof"] = st.text_input(
                "Indexing Proof",
                value=payload.get("indexing_proof", ""),
                key=f"cip_{submission_id}",
            )
            payload["issn_isbn"] = st.text_input("ISSN/ISBN", value=payload.get("issn_isbn", ""), key=f"is_{submission_id}")
            payload["certificate_ref"] = st.text_input(
                "Certificate Reference",
                value=payload.get("certificate_ref", ""),
                key=f"ccr_{submission_id}",
            )
            payload["attachment_ref"] = st.text_input(
                "Attachment Reference",
                value=payload.get("attachment_ref", ""),
                key=f"car_{submission_id}",
            )

        elif sub_category == "Book Chapter":
            payload["publisher"] = st.text_input("Publisher", value=payload.get("publisher", ""), key=f"bp_{submission_id}")
            payload["issn_isbn"] = st.text_input("ISBN", value=payload.get("issn_isbn", ""), key=f"is_{submission_id}")
            payload["official_venue_url"] = st.text_input(
                "Book URL",
                value=payload.get("official_venue_url", ""),
                key=f"bou_{submission_id}",
            )
            payload["book_indexed_ugc"] = st.text_input(
                "Indexed in UGC",
                value=payload.get("book_indexed_ugc", ""),
                key=f"bugc_{submission_id}",
            )
            payload["book_indexed_scopus"] = st.text_input(
                "Indexed in Scopus",
                value=payload.get("book_indexed_scopus", ""),
                key=f"bsc_{submission_id}",
            )
            payload["book_indexed_wos"] = st.text_input(
                "Indexed in WoS",
                value=payload.get("book_indexed_wos", ""),
                key=f"bwos_{submission_id}",
            )
            payload["attachment_ref"] = st.text_input(
                "Attachment Reference",
                value=payload.get("attachment_ref", ""),
                key=f"bar_{submission_id}",
            )
            payload["venue"] = payload.get("venue") or payload.get("publisher")

        c1, c2 = st.columns(2)
        if c1.button("Approve"):
            _approve_dialog(submission_id, username, payload)
        if c2.button("Reject"):
            _reject_dialog(submission_id, username)

        st.caption(f"Current status: {submission.status}")
        actions = session.execute(
            select(ReviewAction).where(ReviewAction.submission_id == submission_id).order_by(ReviewAction.created_at.asc())
        ).scalars()
        audit_rows = [{"action": a.action, "actor": a.actor, "note": a.note, "timestamp": a.created_at} for a in actions]
    st.subheader("Audit Trail")
    _safe_dataframe(pd.DataFrame(audit_rows), "No audit actions available.")


@st.dialog("Confirm Rebuild Migration")
def _migration_confirm_dialog(excel_path: str, username: str) -> None:
    upload_bytes = st.session_state.get("migration_upload_bytes")
    upload_name = st.session_state.get("migration_upload_name")
    use_upload = isinstance(upload_bytes, (bytes, bytearray)) and len(upload_bytes) > 0
    source_display = str(upload_name or "uploaded_workbook.xlsx") if use_upload else excel_path

    st.warning("This will backup the DB, wipe `publications`, and reimport from Excel.")
    st.caption(f"Source workbook: {source_display}")
    if st.button("Confirm Rebuild", type="primary"):
        if not source_display.strip():
            st.error("Provide an Excel file path or upload a workbook.")
            return

        temp_excel_path: str | None = None
        selected_excel_path = excel_path
        try:
            if use_upload:
                with NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
                    tmp.write(bytes(upload_bytes))
                    temp_excel_path = tmp.name
                selected_excel_path = temp_excel_path

            with session_scope(DB_PATH) as session:
                report = rebuild_publications_from_excel(
                    session=session,
                    db_path=DB_PATH,
                    excel_path=selected_excel_path,
                    status_path=MIGRATION_STATUS_PATH,
                )
        finally:
            if temp_excel_path and Path(temp_excel_path).exists():
                Path(temp_excel_path).unlink(missing_ok=True)

        st.session_state["migration_report"] = report
        _log_info(f"Migration rebuild run by {username}: imported={report.rows_imported}, skipped={report.rows_skipped}")
        st.success("Migration rebuild completed.")
        st.session_state["migration_upload_bytes"] = None
        st.session_state["migration_upload_name"] = None
        st.rerun()


def _admin_migration_page() -> None:
    st.header("Rebuild Publications from Excel")
    username = st.session_state["auth_username"]
    excel_path = st.text_input("Excel File Path", value=Path(DEFAULT_EXCEL).name)
    uploaded_workbook = st.file_uploader("Or Upload Excel Workbook", type=["xlsx"], key="migration_uploaded_workbook")

    if uploaded_workbook is not None:
        st.session_state["migration_upload_bytes"] = uploaded_workbook.getvalue()
        st.session_state["migration_upload_name"] = uploaded_workbook.name
        st.caption(f"Uploaded workbook selected: {uploaded_workbook.name}")

    if st.button("Rebuild Publications (Backup + Wipe + Reimport)", type="primary"):
        has_upload = bool(st.session_state.get("migration_upload_bytes"))
        if not has_upload and not excel_path.strip():
            st.error("Provide an Excel file path or upload a workbook.")
            return
        _migration_confirm_dialog(excel_path.strip(), username)

    report = st.session_state.get("migration_report")
    if report:
        st.subheader("Latest Migration Report")
        st.json(
            {
                "source_file": report.source_file,
                "db_backup_file": report.db_backup_file,
                "started_at_utc": report.started_at_utc,
                "ended_at_utc": report.ended_at_utc,
                "rows_read": report.rows_read,
                "rows_imported": report.rows_imported,
                "rows_skipped": report.rows_skipped,
                "sheet_summary": report.sheet_summary,
                "skip_reasons": report.skip_reasons,
            }
        )
        st.subheader("Post-import Quality Checks")
        _safe_dataframe(pd.DataFrame(report.quality_checks), "No quality checks produced.")


def _admin_system_checks_page() -> None:
    st.header("System Checks")
    if st.button("Run Checks", type="primary"):
        with session_scope(DB_PATH) as session:
            checks_df, summary = run_system_checks(session, migration_status_path=MIGRATION_STATUS_PATH, log_path=APP_LOG_PATH)
        st.session_state["system_checks_df"] = checks_df
        st.session_state["system_checks_summary"] = summary
        _log_info(f"System checks executed: failed={summary['failed_checks']}")

    checks_df = st.session_state.get("system_checks_df")
    summary = st.session_state.get("system_checks_summary")
    if checks_df is not None and summary is not None:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Checks", summary["total_checks"])
        c2.metric("Passed", summary["passed_checks"])
        c3.metric("Failed", summary["failed_checks"])
        _safe_dataframe(checks_df, "No checks data available.")
        payload = export_system_checks_xlsx(checks_df, summary)
        st.download_button(
            "Download Checks Report (XLSX)",
            payload,
            file_name="system_checks_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


def _render_navigation(role: str) -> None:
    pages = {
        "Shared": [
            st.Page(_dashboard_page, title="Dashboard", icon=":material/dashboard:", default=True),
            st.Page(_publications_page, title="Publications Explorer", icon=":material/table_view:"),
        ]
    }
    if role == "faculty":
        pages["Faculty"] = [
            st.Page(_faculty_new_submission, title="New Submission", icon=":material/add:"),
            st.Page(_faculty_my_submissions, title="My Submissions", icon=":material/history:"),
        ]
    else:
        pages["Admin"] = [
            st.Page(_admin_review_queue, title="Review Queue", icon=":material/rule:"),
            st.Page(_admin_migration_page, title="Migration", icon=":material/database:"),
            st.Page(_admin_system_checks_page, title="System Checks", icon=":material/health_and_safety:"),
        ]
    current = st.navigation(pages, expanded=True)
    current.run()


def main() -> None:
    global DB_PATH, DEFAULT_EXCEL, APP_LOG_PATH
    DB_PATH, DEFAULT_EXCEL, APP_LOG_PATH = _resolve_runtime_paths()

    st.set_page_config(page_title="Faculty Publication Manager", page_icon=":material/school:", layout="wide")
    _setup_logging()
    init_db(DB_PATH)
    _init_state()
    auth_session = _auth_sidebar()
    if auth_session is None:
        st.title("Faculty Publication Manager")
        st.info("Login from the sidebar to continue.")
        return
    role, _ = auth_session
    _render_navigation(role)


if __name__ == "__main__":
    main()
