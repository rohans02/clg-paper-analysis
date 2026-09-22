# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-user Streamlit app ("Faculty Publication Manager") for a college department. Faculty submit publications (URL/DOI/manual), an admin reviews and approves them, and approved records land in a SQLite database. Admin can also rebuild the DB from the official Excel workbook and export back into that same workbook layout.

## Commands

```bash
# Environment (Windows; a .venv already exists in the repo)
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# Run the app (http://localhost:8501)
streamlit run app.py

# Tests (pytest, in-memory SQLite via tests/conftest.py `session` fixture)
pytest -q
pytest -q tests/test_workflow.py
pytest -q tests/test_workflow.py::test_name -k keyword
```

There is no linter or formatter configured. No CI.

## Configuration

`app.py` resolves settings via `_secret_or_env()`: `st.secrets` key first, then env var fallback.

| Secret key        | Env var              | Default                                            |
|-------------------|----------------------|----------------------------------------------------|
| `ADMIN_PASSWORD`  | `APP_ADMIN_PASSWORD` | none (admin login disabled without it)             |
| `DB_PATH`         | `APP_DB_PATH`        | `publication_manager.db` next to `app.py`          |
| `TEMPLATE_PATH`   | `APP_TEMPLATE_PATH`  | `Faculty Publications,A.Y. 2025-26,SEM-I & II.xlsx`|
| `LOG_PATH`        | `APP_LOG_PATH`       | `app.log`                                          |
| `FACULTY_PASSCODE`| `APP_FACULTY_PASSCODE`| none (faculty login is open without it)           |
| `AUTH_SECRET`     | `APP_AUTH_SECRET`    | falls back to the admin password, then a per-process random key |

Local secrets go in `.streamlit/secrets.toml`, which is gitignored; never commit it. The Excel workbook at the repo root is both the migration source and the export template; the official exporter fails if it is missing.

## Architecture

`app.py` (~1500 lines) is the whole UI. `publication_manager/` is the pure-Python domain layer with no Streamlit imports; everything there takes a SQLAlchemy `Session` and is what the tests exercise.

### Two parallel publication schemas (important)

The DB stores every approved/imported publication **twice**, and every write path must keep both in sync:

- **Legacy flat table** `publications` (`models.Publication`): one wide row per record.
- **"Lossless" normalized set**: `publications_core` (`PublicationCore`) plus one type-specific detail row (`publication_journal_details` / `publication_conference_details` / `publication_book_details`, chosen by `publication_type` in `lossless._insert_publication_details`) plus raw source capture in `publication_source_rows` / `publication_source_cells` and a header map in `template_schema_registry`.

`workflow.approve_submission` and `migration._persist_publication_records` both write the legacy row and then call `lossless.create_publication_core_from_payload` + a source-row recorder. `query.get_publications_df` prefers the core tables when `publications_core` has any rows and otherwise falls back to the legacy table. The admin edit/delete dialogs in `app.py` also touch both. When adding a field, add it to `Publication`, `PublicationCore` (or the right details table), both persist paths, the query join, the exporter, and `db._ensure_schema` if it needs an `ALTER TABLE` for existing DBs (there are no Alembic migrations; `init_db` does `create_all` plus hand-written column checks).

### Submission workflow (`workflow.py`)

State machine on `PendingSubmission.status`: `DRAFT -> SUBMITTED -> UNDER_REVIEW -> APPROVED|REJECTED`, enforced by `ALLOWED_TRANSITIONS`. `approve_submission` auto-advances SUBMITTED to UNDER_REVIEW, then runs duplicate checks against both schemas: a hard duplicate (same `doi_normalized`) blocks; a soft duplicate (title + faculty + year) needs `override_soft_duplicate=True`. Every transition appends a `ReviewAction` audit row. `create_submission` dedupes repeat submits per user via an `_insertion_fingerprint` stored inside `parsed_payload_json`.

Payloads are plain dicts; `normalization.normalize_doi` / `parse_date` are applied at every boundary and `doi_normalized` is the DB-level uniqueness key.

### Excel migration (`migration.py`)

`SHEET_CONFIGS` maps each sheet name of the official workbook to 0-based column indices (`SheetConfig`). Sheets not in that dict are skipped. `rebuild_publications_from_excel` backs up the DB to `backups/`, wipes all seven publication tables, reimports, runs `run_post_import_quality_checks`, and writes `migration_status.json` (read later by `system_checks`). `migrate_from_excel` is the same without backup/wipe and is what tests use. Header row is detected by scanning for `Sr. No.`; data starts on the row after it.

### Export (`exporter.py`)

`export_official_format_xlsx` opens the template workbook, clears data rows in each sheet named by `OFFICIAL_SHEET_MAP` (keyed by `(category, publication_type)`), rewrites rows from the query DataFrame, and refreshes the `Analysis` sheet. The generic `export_full_xlsx` / `export_filtered_xlsx` produce a flat two-sheet workbook instead.

### Category taxonomy (`taxonomy.py`)

`OFFICIAL_SHEET_MAP` is the single source of truth for which `(category, publication_type)` pairs exist in the workbook. The DB stores the workbook's own category values (for example `Book`), while the UI shows labels (`Book Chapter`); convert with `category_from_label` / `category_label`. `workflow._normalize_insert_payload` normalizes categories on every write. Pairs with no sheet (anything under `Other`, or mismatches) are still stored but reported in the export metadata's `unexported` list and shown as a warning in the UI.

### UI (`app.py`)

There is no sidebar. Sign-in lives on the landing page (`_sign_in_page`, centred with `_SIGN_IN_CSS`). Signed in, `st.navigation(..., position="top")` renders the pages as links in Streamlit's top bar, ending with a Sign out page that logs out and reruns; signed out, a hidden navigation with only the welcome page is registered so the browser never keeps a stale menu. Add a page in `_pages_for_role`. Role and username persist in `st.query_params` as an HMAC-signed token (`publication_manager/auth.py`): admin tokens live 2 hours, faculty 12, and sign-out revokes the token in a process-wide set. Admin login failures are throttled per client IP by `ADMIN_THROTTLE` (process-wide, not session state). An optional `FACULTY_PASSCODE` gates faculty login. Admin is a single shared `ADMIN_PASSWORD`; the admin's `auth_username` is always "admin".

Pages are listed in `_pages_for_role`: Overview and Publications for everyone, Submit a Publication and My Submissions for faculty, Review Queue and Import from Excel for admin. System checks are an expander on the import page, not a page. Tables use `st.dataframe` row selection (`on_select="rerun"`, single row) to open a record; nothing asks the user for an ID. Their widget keys come from `_table_key`, and any code that adds or removes rows must call `_bump_tables()` so a stale selection does not reopen the wrong record. The app bar (`_render_app_bar`) renders both a link row and a phone dropdown; CSS media queries show one. Excel downloads are built on render through `_cached_official_export` (cache keyed on filters plus a data fingerprint), so there is no prepare step. DOI input goes through `ingestion.resolve_doi_metadata` (Crossref) to fill title, authors, venue, date and type. Internal enum values are never shown directly: use `STATUS_LABELS`, `ACTION_LABELS`, `PUBLICATION_COLUMNS` and `category_label` when rendering. Paper links entered by faculty go through `ingestion.safe_get`, which rejects non-http(s) schemes and private addresses and re-validates redirects. Uses recent Streamlit APIs (`st.dialog`, `st.navigation`, `st.context.ip_address`, `width="stretch"`), so keep `streamlit>=1.49`. Uploaded proof files go to `uploads/` (gitignored). `.streamlit/config.toml` sets `showErrorDetails = "none"` and `toolbarMode = "minimal"`, so tracebacks only appear in the log.
