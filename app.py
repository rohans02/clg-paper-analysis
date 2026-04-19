from __future__ import annotations

from datetime import date
import hmac
import logging
import os
from pathlib import Path
import re
import time
from typing import Any

import pandas as pd
import streamlit as st
from streamlit import column_config
from sqlalchemy import select

from publication_manager.db import init_db, session_scope
from publication_manager.enums import InputMethod, SubmissionStatus
from publication_manager.exporter import export_official_format_xlsx
from publication_manager.ingestion import ingest_source
from publication_manager.migration import MIGRATION_STATUS_PATH, SHEET_CONFIGS, rebuild_publications_from_excel
from publication_manager.models import PendingSubmission, ReviewAction
from publication_manager.normalization import normalize_doi
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
ADMIN_PASSWORD_ENV = "APP_ADMIN_PASSWORD"
ADMIN_MAX_FAILED_ATTEMPTS = 5
ADMIN_LOCKOUT_SECONDS = 300
FACULTY_NAME_PATTERN = re.compile(
    r"^(?:Dr\. ?(?:Ms\.|Mrs\.|Mr\.)? ?|Ms\. ?|Mr\. ?|Mrs\. ?)[A-Za-z][A-Za-z .'-]{1,}$"
)


def _setup_logging() -> None:
    logger = logging.getLogger("publication_manager")
    if logger.handlers:
        return
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler("app.log", encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


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


def _normalize_faculty_name(name: str) -> str:
    return " ".join(name.strip().split())


def _is_valid_faculty_name(name: str) -> bool:
    return bool(FACULTY_NAME_PATTERN.match(name))


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
        faculty_name_input = st.text_input(
            "Faculty Name",
            value="",
            placeholder="Dr. Ms. Firstname Lastname",
            help="Enter official faculty name with prefix (Dr./Ms./Mr./Mrs.).",
            key="faculty_login_name",
        )
        if st.button("Login as Faculty", key="faculty_login_btn", use_container_width=True):
            normalized_name = _normalize_faculty_name(faculty_name_input)
            if not _is_valid_faculty_name(normalized_name):
                st.sidebar.error("Invalid faculty name format. Include prefix like Dr./Ms./Mr./Mrs.")
            else:
                st.session_state["auth_is_authenticated"] = True
                st.session_state["auth_role"] = "faculty"
                st.session_state["auth_username"] = normalized_name
                _log_info(f"Faculty login success: {normalized_name}")
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
    with session_scope(DB_PATH) as session:
        metrics = get_dashboard_metrics(session)
        faculty_df = get_faculty_analysis_df(session)

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Publications", metrics["total_publications"])
    c2.metric("Pending Reviews", metrics["pending_reviews"])
    c3.metric("Missing DOI", metrics["data_health"]["missing_doi_count"])
    c4.metric("Missing Pub Date", metrics["data_health"]["missing_pub_date_count"])
    c5.metric("Manual Cleanup Flags", metrics["data_health"]["manual_cleanup_count"])

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

    st.subheader("By Category")
    _safe_dataframe(metrics["by_category"])
    st.subheader("By Year")
    _safe_dataframe(metrics["by_year"])


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
            payload, metadata = export_official_format_xlsx(
                session,
                username,
                template_path=str(Path(DEFAULT_EXCEL).resolve()),
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
        if action == "Export Filtered (Official Format)":
            payload, metadata = export_official_format_xlsx(
                session,
                username,
                template_path=str(Path(DEFAULT_EXCEL).resolve()),
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


def _faculty_new_submission() -> None:
    st.header("New Submission")
    username = st.session_state["auth_username"]

    workbook_categories: dict[str, list[str]] = {}
    for config in SHEET_CONFIGS.values():
        workbook_categories.setdefault(config.category, [])
        if config.publication_type not in workbook_categories[config.category]:
            workbook_categories[config.category].append(config.publication_type)

    category = st.selectbox("Category (Workbook)", list(workbook_categories.keys()), key="faculty_workbook_category")
    sub_category = st.selectbox(
        "Sub Category (Workbook)",
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
        payload["faculty_name"] = st.text_input(
            "Faculty Name",
            value=payload.get("faculty_name", faculty_name),
            key="faculty_new_name_review",
        )
        payload["title"] = st.text_input("Title", value=payload.get("title", ""), key="faculty_review_title")
        payload["publication_name"] = st.text_input(
            "Journal/Conference/Book Name",
            value=payload.get("publication_name", payload.get("venue", "")),
            key="faculty_review_publication_name",
        )
        payload["authors"] = st.text_area("Authors", value=payload.get("authors", ""), key="faculty_review_authors")
        payload["pub_date"] = st.text_input(
            "Publication Date",
            value=str(payload.get("pub_date", "")),
            key="faculty_review_pub_date",
        )
        payload["doi"] = st.text_input("DOI", value=payload.get("doi", ""), key="faculty_review_doi")
        payload["paper_url"] = st.text_input(
            "Paper URL",
            value=payload.get("paper_url", source_input),
            key="faculty_review_paper_url",
        )
        payload["indexing_source"] = st.text_input(
            "Indexing Source",
            value=payload.get("indexing_source", category),
            key="faculty_review_indexing_source",
        )

        if sub_category in ("Journal", "Conference"):
            payload["national_international"] = st.text_input(
                "National/International",
                value=payload.get("national_international", ""),
                key="faculty_review_nat_int",
            )

        if sub_category == "Journal":
            payload["quartile"] = st.text_input("Quartile", value=payload.get("quartile", ""), key="faculty_review_quartile")
            payload["volume_issue"] = st.text_input(
                "Volume/Issue",
                value=payload.get("volume_issue", ""),
                key="faculty_review_volume_issue_journal",
            )
            payload["official_venue_url"] = st.text_input(
                "Journal Official URL",
                value=payload.get("official_venue_url", ""),
                key="faculty_review_official_url_journal",
            )
            payload["research_published_flag"] = st.text_input(
                "Research Published (Yes/No)",
                value=payload.get("research_published_flag", ""),
                key="faculty_review_published_journal",
            )
            payload["indexing_flag"] = st.text_input(
                "Indexing Flag",
                value=payload.get("indexing_flag", ""),
                key="faculty_review_indexing_flag_journal",
            )
            payload["indexing_proof"] = st.text_input(
                "Indexing Proof",
                value=payload.get("indexing_proof", ""),
                key="faculty_review_indexing_proof_journal",
            )
            payload["issn_isbn"] = st.text_input("ISSN", value=payload.get("issn_isbn", ""), key="faculty_review_issn")
            payload["attachment_ref"] = st.text_input(
                "Attachment Reference",
                value=payload.get("attachment_ref", ""),
                key="faculty_review_attachment_journal",
            )
            payload["venue"] = payload.get("venue") or payload.get("publication_name")

        elif sub_category == "Conference":
            payload["venue"] = st.text_input("Venue", value=payload.get("venue", ""), key="faculty_review_venue_conference")
            payload["conference_date"] = st.text_input(
                "Conference Date",
                value=payload.get("conference_date", ""),
                key="faculty_review_conference_date",
            )
            payload["presented_accepted_flag"] = st.text_input(
                "Presented/Accepted",
                value=payload.get("presented_accepted_flag", ""),
                key="faculty_review_presented_accepted",
            )
            payload["volume_issue"] = st.text_input(
                "Volume/Issue",
                value=payload.get("volume_issue", ""),
                key="faculty_review_volume_issue_conference",
            )
            payload["official_venue_url"] = st.text_input(
                "Conference Official URL",
                value=payload.get("official_venue_url", ""),
                key="faculty_review_official_url_conference",
            )
            payload["research_published_flag"] = st.text_input(
                "Research Published (Yes/No)",
                value=payload.get("research_published_flag", ""),
                key="faculty_review_published_conference",
            )
            payload["indexing_flag"] = st.text_input(
                "Indexing Flag",
                value=payload.get("indexing_flag", ""),
                key="faculty_review_indexing_flag_conference",
            )
            payload["indexing_proof"] = st.text_input(
                "Indexing Proof",
                value=payload.get("indexing_proof", ""),
                key="faculty_review_indexing_proof_conference",
            )
            payload["issn_isbn"] = st.text_input("ISSN/ISBN", value=payload.get("issn_isbn", ""), key="faculty_review_issn_isbn_conf")
            payload["certificate_ref"] = st.text_input(
                "Certificate Reference",
                value=payload.get("certificate_ref", ""),
                key="faculty_review_certificate_ref",
            )
            payload["attachment_ref"] = st.text_input(
                "Attachment Reference",
                value=payload.get("attachment_ref", ""),
                key="faculty_review_attachment_conference",
            )

        elif sub_category == "Book Chapter":
            payload["publisher"] = st.text_input("Publisher", value=payload.get("publisher", ""), key="faculty_review_publisher")
            payload["issn_isbn"] = st.text_input("ISBN", value=payload.get("issn_isbn", ""), key="faculty_review_isbn")
            payload["official_venue_url"] = st.text_input(
                "Book URL",
                value=payload.get("official_venue_url", ""),
                key="faculty_review_book_url",
            )
            payload["book_indexed_ugc"] = st.text_input(
                "Indexed in UGC",
                value=payload.get("book_indexed_ugc", ""),
                key="faculty_review_book_ugc",
            )
            payload["book_indexed_scopus"] = st.text_input(
                "Indexed in Scopus",
                value=payload.get("book_indexed_scopus", ""),
                key="faculty_review_book_scopus",
            )
            payload["book_indexed_wos"] = st.text_input(
                "Indexed in WoS",
                value=payload.get("book_indexed_wos", ""),
                key="faculty_review_book_wos",
            )
            payload["attachment_ref"] = st.text_input(
                "Attachment Reference",
                value=payload.get("attachment_ref", ""),
                key="faculty_review_attachment_book",
            )
            payload["venue"] = payload.get("venue") or payload.get("publisher")

        confidence = float(st.session_state.get("ingestion_confidence", 0.0))
        warnings = st.session_state.get("ingestion_warnings", [])
        st.info(f"Confidence score: {confidence}")
        for warning in warnings:
            st.warning(warning)

        if st.button("Submit for Admin Review", type="primary"):
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
                    "confidence": row.confidence_score,
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
    with session_scope(DB_PATH) as session:
        submission = session.get(PendingSubmission, submission_id)
        if not submission:
            st.error("Submission not found.")
            return
        if submission.status == SubmissionStatus.SUBMITTED.value and st.button("Move to UNDER_REVIEW"):
            start_review(session, submission_id, username)
            _log_info(f"Submission {submission_id} moved to UNDER_REVIEW by {username}.")
            st.success("Submission moved to UNDER_REVIEW.")
            st.rerun()

        payload = dict(submission.parsed_payload_json)
        payload["faculty_name"] = st.text_input("Faculty Name", value=payload.get("faculty_name", ""), key=f"f_{submission_id}")
        payload["title"] = st.text_input("Title", value=payload.get("title", ""), key=f"t_{submission_id}")
        payload["publication_name"] = st.text_input(
            "Journal/Conference/Book Name",
            value=payload.get("publication_name", payload.get("venue", "")),
            key=f"pn_{submission_id}",
        )
        payload["authors"] = st.text_area("Authors", value=payload.get("authors", ""), key=f"a_{submission_id}")
        payload["category"] = st.text_input("Category", value=payload.get("category", ""), key=f"c_{submission_id}")
        payload["publication_type"] = st.text_input("Publication Type", value=payload.get("publication_type", ""), key=f"pt_{submission_id}")
        payload["venue"] = st.text_input("Venue", value=payload.get("venue", ""), key=f"v_{submission_id}")
        payload["conference_date"] = st.text_input("Conference Date", value=payload.get("conference_date", ""), key=f"cd_{submission_id}")
        payload["pub_date"] = st.text_input("Publication Date", value=str(payload.get("pub_date", "")), key=f"d_{submission_id}")
        payload["doi"] = st.text_input("DOI", value=payload.get("doi", ""), key=f"doi_{submission_id}")
        payload["paper_url"] = st.text_input("Paper URL", value=payload.get("paper_url", ""), key=f"url_{submission_id}")
        payload["indexing_source"] = st.text_input("Indexing Source", value=payload.get("indexing_source", ""), key=f"idx_{submission_id}")
        payload["quartile"] = st.text_input("Quartile", value=payload.get("quartile", ""), key=f"q_{submission_id}")
        payload["issn_isbn"] = st.text_input("ISSN/ISBN", value=payload.get("issn_isbn", ""), key=f"is_{submission_id}")

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
    st.warning("This will backup the DB, wipe `publications`, and reimport from Excel.")
    if st.button("Confirm Rebuild", type="primary"):
        with session_scope(DB_PATH) as session:
            report = rebuild_publications_from_excel(
                session=session,
                db_path=DB_PATH,
                excel_path=excel_path,
                status_path=MIGRATION_STATUS_PATH,
            )
        st.session_state["migration_report"] = report
        _log_info(f"Migration rebuild run by {username}: imported={report.rows_imported}, skipped={report.rows_skipped}")
        st.success("Migration rebuild completed.")
        st.rerun()


def _admin_migration_page() -> None:
    st.header("Rebuild Publications from Excel")
    username = st.session_state["auth_username"]
    excel_path = st.text_input("Excel File Path", value=str(Path(DEFAULT_EXCEL).resolve()))
    if st.button("Rebuild Publications (Backup + Wipe + Reimport)", type="primary"):
        _migration_confirm_dialog(excel_path, username)

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
            checks_df, summary = run_system_checks(session, migration_status_path=MIGRATION_STATUS_PATH, log_path="app.log")
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
