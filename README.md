# Faculty Publication Manager (V1)

Local Streamlit app to manage faculty publication submissions with admin review governance.

## Key Workflow

- Faculty sign in by name (optionally with a shared department passcode) and submit a publication from a link/DOI or by hand, attaching proofs.
- Submissions wait in `pending_submissions` until the admin approves or rejects them in the Review Queue.
- Only approved records are inserted into `publications` and appear in the Publications list.
- Admin can import the official Excel workbook (backup, wipe, reimport) from **Import from Excel**, and download the current data back in the official layout from **Publications**.
- Technical diagnostics live under **Import from Excel → Advanced diagnostics**.

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
# Optional: shared passcode faculty must enter to log in (unset = open faculty login)
FACULTY_PASSCODE = "department-passcode"
# Optional: key that signs login state kept in the URL (defaults to ADMIN_PASSWORD)
AUTH_SECRET = "long-random-string"
# Optional overrides
DB_PATH = "/tmp/publication_manager.db"
TEMPLATE_PATH = "Faculty Publications,A.Y. 2025-26,SEM-I & II.xlsx"
LOG_PATH = "/tmp/app.log"
```

- Equivalent environment variable overrides are also supported:
	- `APP_ADMIN_PASSWORD`
	- `APP_FACULTY_PASSCODE`
	- `APP_AUTH_SECRET`
	- `APP_DB_PATH`
	- `APP_TEMPLATE_PATH`
	- `APP_LOG_PATH`
- `.streamlit/secrets.toml` is gitignored. Never commit it; copy `.streamlit/secrets.toml.example` locally instead.

## Test

```bash
pytest -q
```

## Import from Excel

- Sign in as admin and open **Import from Excel**.
- Upload the official workbook and confirm. The app backs up the database, wipes `publications`, reimports, and shows what was skipped and why.

## Download as Excel

- On **Publications**, "Prepare all records" or "Prepare current filter" builds a workbook in the official layout, then a download button appears.
- Records whose "Indexed in" / type combination has no sheet in the official workbook are listed as unexported instead of being dropped silently.
- The `Analysis` sheet is recomputed from the exported rows.

## Security notes

- Login state is kept in the URL as a signed, expiring token (admin 2 h, faculty 12 h). Signing out revokes it. Do not share the address bar while signed in.
- Admin sign-in is throttled per client IP: 5 failures lock it for 5 minutes, across browser sessions.
- Paper links are fetched server-side only if they are http(s) and resolve to public addresses, with a 5 MB cap.
- `.streamlit/config.toml` hides error details and the developer toolbar from visitors.
