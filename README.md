# Job Market Intelligence System

A complete data engineering pipeline that ingests job listings from multiple sources,
processes them through a Medallion architecture (Bronze -> Silver -> Gold), and serves
labor-market insights via a FastAPI backend and React JS dashboard.

## Team — Group 7
- Abdullah Khurram Vohra
- Syed Muhammad Abu Talib
- Burhan Ahmed
- Muhammad Ali Siddique

## Architecture

```
Adzuna API  --+
Remotive API--+--> Bronze (raw JSON) --> Silver (PostgreSQL) --> Gold (aggregates)
Kaggle -------+                                                        |
                                                                       v
                                                          FastAPI --> React Dashboard
```

Orchestrated by **Prefect** (self-hosted). PySpark used for Bronze -> Silver -> Gold transforms.

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

This runs the full pipeline **immediately** (Adzuna + Remotive -> Bronze -> Silver -> Gold),
then registers an **8-hour cron schedule** so it reruns automatically.

Other options:

```powershell
python pipeline.py --once       # run once and exit (good for testing)
python pipeline.py --schedule   # register schedule only, skip immediate run
python pipeline.py --seed       # one-time Kaggle historical seed (first run, DB empty)
```

> For the very first run with an empty database, run `--seed` first to load Kaggle datasets,
> then run `pipeline.py` for the live API data.

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
| Pipeline runner | `python pipeline.py` | — |
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
├── processing/         Bronze->Silver->Gold transformations + validation
├── storage/            SQLAlchemy models and DB connection
├── orchestration/      Prefect flows and task definitions
├── serving/
│   ├── api/            FastAPI backend + routers
│   └── frontend/       React + Vite + TypeScript dashboard
├── ml/                 ML scaffold (feature engineering, training, inference stubs)
├── monitoring/         Structured JSON logging
├── config/             Pydantic settings (env var management)
├── pipeline.py         Pipeline runner and scheduler
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

---

## AI Usage Declaration

- Tool: GitHub Copilot / ChatGPT
- Used for: Boilerplate scaffolding, debugging syntax errors
- Extent: All logic, architecture, and design decisions made by the team
