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
