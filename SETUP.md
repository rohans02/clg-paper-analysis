# Faculty Publication Manager — Local Setup Guide

## One-Time Setup (5 minutes)

### Step 1: Install Python
- Download Python 3.11+ from [python.org/downloads](https://www.python.org/downloads/)
- During installation, **check "Add Python to PATH"** ✅
- Click Install

### Step 2: Download the Project
- Download the project as a ZIP from GitHub, or if Git is installed:
```
git clone https://github.com/rohans02/clg-paper-analysis.git
```

### Step 3: Open Terminal in Project Folder
- Open the `clg-paper-analysis` folder in File Explorer
- Click the address bar, type `cmd`, press Enter

### Step 4: Create Virtual Environment & Install Dependencies
```
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### Step 5: Configure Admin Password
Create a file at `.streamlit/secrets.toml` with:
```
ADMIN_PASSWORD = "mmcoe"
```

### Step 6: Run the App
```
streamlit run app.py
```
The app will open in your browser at `http://localhost:8501`

---

## Daily Usage

Every time you want to use the app, just:

1. Open terminal in the project folder
2. Run:
```
.venv\Scripts\activate
streamlit run app.py
```
3. Open `http://localhost:8501` in your browser
4. Press `Ctrl+C` in terminal to stop

---

## Notes
- All data is stored in `publication_manager.db` (SQLite) — this file stays on your laptop permanently
- To import publications from Excel, use the **Migration** page (Admin login required)
- Backup: just copy `publication_manager.db` to a safe location
