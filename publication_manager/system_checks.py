from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
import json
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from publication_manager.migration import load_migration_status


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: str
    value: Any


REQUIRED_COLUMNS = {
    "pending_submissions": {
        "id",
        "submitted_by",
        "status",
        "source_input",
        "source_input_method",
        "parsed_payload_json",
        "confidence_score",
        "created_at",
    },
    "publications": {
        "id",
        "faculty_name",
        "title",
        "category",
        "publication_type",
        "pub_date",
        "doi",
        "doi_normalized",
    },
    "publications_core": {
        "id",
        "faculty_name",
        "title",
        "category",
        "publication_type",
        "pub_date",
        "doi",
        "doi_normalized",
    },
    "review_actions": {
        "id",
        "submission_id",
        "actor",
        "action",
        "created_at",
    },
}


def _preferred_publication_table(session: Session) -> str:
    if _table_exists(session, "publications_core"):
        count = session.execute(text("SELECT COUNT(*) FROM publications_core")).scalar() or 0
        if int(count) > 0:
            return "publications_core"
    return "publications"


def _table_exists(session: Session, table_name: str) -> bool:
    row = session.execute(
        text("SELECT name FROM sqlite_master WHERE type='table' AND name=:table"),
        {"table": table_name},
    ).fetchone()
    return row is not None


def _table_columns(session: Session, table_name: str) -> set[str]:
    rows = session.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
    return {row[1] for row in rows}


def run_system_checks(
    session: Session,
    migration_status_path: str = "migration_status.json",
    log_path: str = "app.log",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    results: list[CheckResult] = []
    publication_table = _preferred_publication_table(session)

    sqlite_version = session.execute(text("select sqlite_version()")).scalar()
    results.append(CheckResult("db_connectivity", bool(sqlite_version), "SQLite connectivity and version check.", sqlite_version))

    for table in REQUIRED_COLUMNS:
        exists = _table_exists(session, table)
        results.append(CheckResult(f"table_exists_{table}", exists, f"Table `{table}` exists.", exists))
        if exists:
            cols = _table_columns(session, table)
            missing = sorted(list(REQUIRED_COLUMNS[table] - cols))
            results.append(
                CheckResult(
                    f"required_columns_{table}",
                    len(missing) == 0,
                    f"Required columns check for `{table}`.",
                    missing if missing else "ok",
                )
            )

    counts = {}
    for table in REQUIRED_COLUMNS:
        if _table_exists(session, table):
            count = session.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar() or 0
            counts[table] = int(count)
    results.append(CheckResult("record_count_sanity", all(v >= 0 for v in counts.values()), "All table counts are non-negative.", counts))

    bad_types = session.execute(
        text(
            f"SELECT COUNT(*) FROM {publication_table} "
            "WHERE publication_type IS NOT NULL AND trim(publication_type) <> '' "
            "AND publication_type NOT IN ('Journal', 'Conference', 'Book Chapter')"
        )
    ).scalar() or 0
    results.append(
        CheckResult(
            "enum_publication_type",
            bad_types == 0,
            f"Publication type values must be valid in `{publication_table}`.",
            int(bad_types),
        )
    )

    bad_category = session.execute(
        text(
            f"SELECT COUNT(*) FROM {publication_table} "
            "WHERE category IS NOT NULL AND trim(category) <> '' "
            "AND category NOT IN ('Scopus','WoS','UGC Care','Peer Reviewed','Book','International Conference','National Conference')"
        )
    ).scalar() or 0
    results.append(
        CheckResult(
            "enum_category",
            bad_category == 0,
            f"Category values must be valid in `{publication_table}`.",
            int(bad_category),
        )
    )

    bad_nat = session.execute(
        text(
            f"SELECT COUNT(*) FROM {publication_table} "
            "WHERE national_international IS NOT NULL AND trim(national_international) <> '' "
            "AND national_international NOT IN ('National','International')"
        )
    ).scalar() or 0
    results.append(
        CheckResult(
            "enum_national_international",
            bad_nat == 0,
            f"National/International values must be valid in `{publication_table}`.",
            int(bad_nat),
        )
    )

    migration_status = load_migration_status(migration_status_path)
    results.append(
        CheckResult(
            "migration_status_available",
            migration_status is not None,
            "Migration status file is present and parseable.",
            migration_status.get("ended_at_utc") if migration_status else None,
        )
    )

    recent_error_summary = ""
    log_file = Path(log_path)
    if log_file.exists():
        lines = log_file.read_text(encoding="utf-8", errors="ignore").splitlines()
        errors = [line for line in lines if "ERROR" in line][-25:]
        recent_error_summary = "\n".join(errors) if errors else "No recent errors."
    else:
        recent_error_summary = "Log file not found."
    results.append(
        CheckResult(
            "recent_error_summary",
            True,
            "Recent error lines from app log (if available).",
            recent_error_summary,
        )
    )

    df = pd.DataFrame([{"name": r.name, "passed": r.passed, "details": r.details, "value": r.value} for r in results])
    summary = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "total_checks": len(results),
        "passed_checks": int(df["passed"].sum()) if not df.empty else 0,
        "failed_checks": int((~df["passed"]).sum()) if not df.empty else 0,
    }
    return df, summary


def export_system_checks_xlsx(checks_df: pd.DataFrame, summary: dict[str, Any]) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        checks_df.to_excel(writer, index=False, sheet_name="checks")
        pd.DataFrame([summary]).to_excel(writer, index=False, sheet_name="summary")
    return output.getvalue()
