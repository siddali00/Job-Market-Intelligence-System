# Job Market Intelligence System

A complete data engineering pipeline that ingests job listings from multiple sources,
stores raw data safely, normalizes it for analytics, and serves labor-market insights
via a FastAPI backend and React dashboard.

## Team — Group 7
- Abdullah Khurram Vohra
- Syed Muhammad Abu Talib
- Burhan Ahmed
- Muhammad Ali Siddique

## Data pipeline (detailed)

For Bronze / Silver / Gold, Spark vs Pandas vs SQL, Prefect flows (`full_pipeline`, `ingest_flow`, `transform_flow`, `seed_historical_data`), UI metrics, and why the dashboard shows a capped subset of rows, see **[docs/DATA_PIPELINE.md](docs/DATA_PIPELINE.md)**.

---

## Architecture (Current)

```
Adzuna API  --+
Remotive API--+--> Bronze (raw files + JSON envelopes) --------------------------+
Kaggle -------+                                                                   |
                                                                               raw_pipeline.py
                                                                                     |
                                                                                     v
                                                                          unified_jobs_wide (Postgres)
                                                                                     |
                                                                          (next step: curated/gold)
                                                                                     |
                                                                                     v
                                                                            FastAPI --> React
```

Notes:
- **Default dashboard path:** Prefect `full_pipeline` → Bronze → **`jobs` / Silver** → **Gold** → FastAPI. Full detail: [docs/DATA_PIPELINE.md](docs/DATA_PIPELINE.md).
- **`seed_historical_data`** only runs Kaggle → **Bronze** (no Silver/Gold in that flow). Run `full_pipeline` or `transform_flow` with the same **`run_date`** as the Bronze partition to load those rows into `jobs` and refresh Gold.
- **`raw_pipeline.py` → `unified_jobs_wide`** is an **alternate** wide-table path; the routers under `serving/api/` query **`jobs` + Gold**, not `unified_jobs_wide`, unless you change that wiring.

---

## Local Development — Run Each Service Separately

Open **4 terminals** in the project root, each running one service.

### Prerequisites (once)

```powershell
# Copy env file and fill in your API keys
cp .env.example .env

# Create and activate virtual environment
python -m venv env
.\env\Scripts\activate

# Install Python dependencies
pip install -r requirements.txt
```

---

### Terminal 1 — FastAPI backend (auto-creates DB tables on start)

```powershell
.\env\Scripts\activate
uvicorn serving.api.main:app --reload --host 0.0.0.0 --port 8000
```

- API: http://localhost:8000
- Swagger docs: http://localhost:8000/docs
- Health check: http://localhost:8000/health

> Database tables are created automatically on first startup. No manual SQL needed.

---

### Terminal 2 — Prefect server (pipeline UI)

```powershell
.\env\Scripts\activate
prefect server start
```

- Prefect UI: http://localhost:4200
- Shows live flow runs, task logs, deployment schedules

---

### Terminal 3 — Pipeline (ingestion + transforms + schedule)

```powershell
.\env\Scripts\activate
python pipeline.py
```

This runs the full live pipeline (Adzuna + Remotive) and schedules recurring execution.
Use this for API-source operations.

Other options:

```powershell
python pipeline.py --once       # run once and exit (good for testing)
python pipeline.py --schedule   # register schedule only, skip immediate run
python pipeline.py --seed       # Kaggle ingestion-only seed (raw files + kaggle_raw_* tables)
```

Important behavior of `--seed`:
- Downloads/reuses selected Kaggle datasets.
- Writes normalized Kaggle bronze JSON batches under `data/bronze/kaggle/<date>/<dataset_key>/`.
- Writes relational raw copies to `kaggle_raw_*` tables (one table per source file).
- Does **not** populate `jobs` or `kaggle_job_details`.

### Terminal 3b — Raw normalization (new)

Run this after a seed (or any raw ingestion) to build a unified staging table:

```powershell
.\env\Scripts\activate
python raw_pipeline.py --truncate
```

Commands:

```powershell
python raw_pipeline.py --truncate        # clear and rebuild unified_jobs_wide
python raw_pipeline.py                   # append another normalization run
python raw_pipeline.py --run-date 2026-04-21   # scope adzuna/remotive bronze by date
```

What it builds:
- `unified_jobs_wide` (high-retention canonical staging table across Kaggle + Adzuna + Remotive raw data)

---

### Terminal 4 — React frontend

```powershell
cd serving\frontend
npm install        # first time only
npm run dev
```

- Dashboard: http://localhost:3000

---

## Services at a Glance

| Service | Command | URL |
|---|---|---|
| FastAPI backend | `uvicorn serving.api.main:app --reload` | http://localhost:8000/docs |
| Prefect UI | `prefect server start` | http://localhost:4200 |
| Ingestion runner | `python pipeline.py` / `python pipeline.py --seed` | — |
| Raw normalizer | `python raw_pipeline.py --truncate` | — |
| React dashboard | `npm run dev` (in serving/frontend) | http://localhost:3000 |

---

## Docker (optional, all-in-one)

```bash
docker-compose up --build
```

---

## Project Structure

```
├── ingestion/          API clients and dataset loaders (Bronze layer)
├── processing/         Raw->wide + Bronze->Silver->Gold transformations + validation
├── storage/            SQLAlchemy models and DB connection
├── orchestration/      Prefect flows and task definitions
├── serving/
│   ├── api/            FastAPI backend + routers
│   └── frontend/       React + Vite + TypeScript dashboard
├── ml/                 ML scaffold (feature engineering, training, inference stubs)
├── monitoring/         Structured JSON logging
├── config/             Pydantic settings (env var management)
├── pipeline.py         Pipeline runner and scheduler
├── raw_pipeline.py     Raw normalization runner -> unified_jobs_wide
└── data/               Local storage (bronze/, ge_reports/) — git-ignored
```

---

## Environment Variables

Copy `.env.example` to `.env` and fill in:

| Variable | Where to get it |
|---|---|
| `ADZUNA_APP_ID` / `ADZUNA_APP_KEY` | developer.adzuna.com |
| `KAGGLE_USERNAME` / `KAGGLE_KEY` | kaggle.com/settings |
| `POSTGRES_PASSWORD` | set any value |
| `DATABASE_URL` | update with your Postgres password |
| `VITE_API_URL` | leave as http://localhost:8000 |

---

## Triggering a Pipeline Run Manually

From the Prefect UI at http://localhost:4200, go to **Deployments** and click **Run**.

Or from the terminal:

```powershell
prefect deployment run full-pipeline/scheduled
```

Or directly (bypasses Prefect UI, good for quick testing):

```powershell
python pipeline.py --once
```

To run Kaggle ingestion only:

```powershell
python pipeline.py --seed
```

To normalize raw data into `unified_jobs_wide`:

```powershell
python raw_pipeline.py --truncate
```

---

## Recommended Daily Workflow

For local development with current ingestion-first setup:

1. Start backend: `uvicorn serving.api.main:app --reload --host 0.0.0.0 --port 8000`
2. Start Prefect: `prefect server start`
3. Run ingestion:
   - Kaggle/raw seed: `python pipeline.py --seed`
   - API sources: `python pipeline.py --once` (or `python pipeline.py` for schedule)
4. Build unified staging: `python raw_pipeline.py --truncate`
5. Start frontend: `npm run dev` in `serving/frontend`

This sequence ensures:
- raw files are preserved,
- raw DB tables are populated,
- `unified_jobs_wide` is refreshed for analysis/dashboard preparation.

---

## AI Usage Declaration

- Tool: GitHub Copilot / ChatGPT
- Used for: Boilerplate scaffolding, debugging syntax errors
- Extent: All logic, architecture, and design decisions made by the team
