# Faculty Publication Manager (V1)

Local Streamlit app to manage faculty publication submissions with admin review governance.

## Key Workflow

- Faculty creates submission from URL/DOI/manual form.
- Submission is stored in `pending_submissions`.
- Admin reviews and approves/rejects.
- Only approved records are inserted into `publications`.
- Admin can run `Migration` to backup DB, wipe `publications`, and reimport corrected data from Excel.
- Admin can run `System Checks` and download a diagnostics XLSX report.

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Run

```bash
streamlit run app.py
```

## Streamlit Cloud Notes

- App is cloud-ready, but SQLite data on Streamlit Cloud is ephemeral unless you configure external persistence.
- Configure secrets in Streamlit Cloud `Secrets` panel when needed:

```toml
ADMIN_PASSWORD = "your-admin-password"
# Optional overrides
DB_PATH = "/tmp/publication_manager.db"
TEMPLATE_PATH = "Faculty Publications,A.Y. 2025-26,SEM-I & II.xlsx"
LOG_PATH = "/tmp/app.log"
```

- Equivalent environment variable overrides are also supported:
	- `APP_ADMIN_PASSWORD`
	- `APP_DB_PATH`
	- `APP_TEMPLATE_PATH`
	- `APP_LOG_PATH`

## Test

```bash
pytest -q
```

## Migration

- Open app as `admin`.
- Navigate to `Migration`.
- Provide workbook path.
- Run rebuild (`backup + wipe + reimport`) and inspect reconciliation + quality checks.

## Export

- `Export Full DB (Official Format)`: exports all approved records in the same official workbook layout used for migration.
- `Export Filtered (Official Format)`: exports only current filtered records in the same official workbook layout.
- Both official exports preserve source-style sheets and update the `Analysis` sheet from the exported dataset.
