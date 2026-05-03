# Data pipeline and dashboard — technical reference

This document describes how data moves from ingestion through the medallion layers, which technologies run at each step, what the React UI displays, how metrics are derived, and how Prefect orchestrates work. It reflects the code in `ingestion/`, `processing/`, `orchestration/`, `storage/`, and `serving/`.

> **Direction of data:** Raw landings move **Bronze → Silver → Gold**. There is no “Silver to Bronze” step; Silver and Gold are downstream of Bronze.

---

## 1. End-to-end picture

1. **Ingest** — API clients and the Kaggle loader write **Bronze** (JSON batches under `data/bronze/`, plus optional raw files for Kaggle).
2. **Bronze → Silver** — `processing/bronze_to_silver.py` reads that day’s Bronze JSON, cleans and deduplicates, then inserts into PostgreSQL **Silver** tables (`jobs`, `job_skills`, and source-specific detail tables).
3. **Silver → Gold** — `processing/silver_to_gold.py` runs **SQL only** against PostgreSQL to refresh **Gold** aggregate tables used for charts and fast API reads.
4. **Serve** — FastAPI routers query Gold (and sometimes `jobs` for counts). The React app calls those endpoints.

`storage/models.py` documents the table map; the key idea is **one canonical job row per posting in `jobs`**, with **Gold** holding pre-computed rollups so the API does not scan millions of rows per request.

---

## 2. Medallion layers (what each layer is)

### Bronze — raw-ish, file-first

- **What it is:** Per-source JSON batches (and for Kaggle, downloaded CSV/XLSX under dataset folders) under `data/bronze/<source>/<YYYY-MM-DD>/…`.
- **Who writes it:** `ingestion/` — e.g. `AdzunaIngester`, `RemotiveIngester`, `KaggleDatasetLoader` via `BaseIngester.save_to_bronze()`.
- **Characteristics:** Preserves provider-specific fields in one envelope schema (see explicit Spark schema in `processing/bronze_to_silver.py`). Good for replay, audits, and fixing parsers without re-hitting APIs.

### Silver — cleaned relational “one job per row”

- **What it is:** Normalised PostgreSQL tables: `jobs` (all sources in one table), `job_skills` (skill strings per job), plus `adzuna_job_details`, `remotive_job_details`, `kaggle_job_details` for source-specific columns.
- **Who writes it:** `bronze_to_silver.run()` after cleaning, within-batch dedupe (`raw_hash`), and cross-run dedupe (skip hashes already in the DB).
- **Characteristics:** Typed fields, trimmed text, salary sanity, optional skill extraction from descriptions; each inserted job can carry a `run_id` from the pipeline run.

### Gold — analytics-ready aggregates

- **What it is:** Pre-aggregated tables for the dashboard, for example:
  - `daily_role_demand` — job counts per **calendar day × role**, plus 7-day and 30-day **moving averages** of that daily count series.
  - `daily_skill_demand` — same idea per **day × skill** (distinct jobs per skill via `job_skills`).
  - `salary_summary` — percentiles of **mid salary** `(salary_min + salary_max) / 2` per **role × country** (minimum sample size rules apply).
  - `remote_vs_onsite` — counts and **remote_ratio** per **day × role**.
  - `skill_cooccurrence` — pairs of skills appearing on the same job (threshold and cap; see §6).
  - `market_alerts` — entities whose **7-day average demand** is at least **twice** the **30-day average** on the as-of date.

- **Who writes it:** `silver_to_gold.run()` using PostgreSQL `SELECT`/`INSERT … ON CONFLICT` only (no Spark in this step).

---

## 3. Where Spark, Pandas, and SQL are used

### Apache Spark (PySpark)

| Area | Role |
|------|------|
| **Bronze → Silver** (`processing/bronze_to_silver.py`) | **Primary path:** read all Bronze JSON for a `run_date` with a fixed schema, clean, window dedupe on `raw_hash`, then collect to the driver and write rows via SQLAlchemy. Uses `local[*]` session. |
| **Kaggle historical load** (`ingestion/dataset_loader.py`) | **Preferred for large CSVs:** Spark reads CSVs, normalises columns, then streams batches to Bronze JSON (e.g. 50k rows per file) so driver memory stays bounded. |
| **ML features** (`ml/features.py`) | Optional **PySpark MLlib** pipeline: reads Silver from Postgres into Spark, builds encoders/assemblers for model training. Not required for the live dashboard. |

If Spark or Java is missing or fails, **Bronze → Silver** logs a warning and uses the **Pandas** implementation instead (`_run_pandas` in the same module).

### Pandas

| Area | Role |
|------|------|
| **Bronze → Silver fallback** | Same logic as Spark path: load JSON into a DataFrame, clean, hash, dedupe, write to Postgres. |
| **Kaggle loader** | Column sniffing, XLSX files, Spark fallback path, and chunked `read_csv` when Spark is unavailable. |
| **ML** (`ml/features.py`) | `build_feature_pandas()` for scikit-learn–style training when Spark is not used. |

### SQL (PostgreSQL)

| Area | Role |
|------|------|
| **Silver → Gold** (`processing/silver_to_gold.py`) | **All** gold recomputation: windowed averages, percentiles, co-occurrence query, alerts logic, upserts. |
| **API layer** (`serving/api/routers/*.py`) | Endpoints execute SQLAlchemy `text()` queries against Gold tables and `jobs` (e.g. overview totals). |
| **Silver writes** | `bronze_to_silver` persists rows through SQLAlchemy ORM / sessions (`storage/models.py`). |

---

## 4. Prefect flows: names, differences, scheduling

Definitions live in `orchestration/flows.py`.

### `full_pipeline` (scheduled production path)

- **Does:** Creates a new `run_id`, opens a `pipeline_runs` row, runs **`ingest_flow`** then **`transform_flow`**, returns combined results.
- **Schedule:** Every **8 hours** (`0 */8 * * *` UTC) — implemented by:
  - `python pipeline.py` (uses `full_pipeline.serve(...)`),
  - `python -m orchestration.schedules` (deployment `full-pipeline/scheduled`),
  - `orchestration/deploy_prefect_managed.py` (Managed cloud deploy with the same cron).
- **Why 8 hours:** Commented rationale — e.g. Remotive rate limits (on the order of a few calls per day); three runs per day stay within conservative limits while keeping data fresh.

### `ingest_flow`

- **Does:** `ingest_adzuna` + `ingest_remotive` → Bronze, then **`validate_bronze`**.
- **Relationship:** Not given its **own** cron in `orchestration/schedules.py`; it runs **inside** `full_pipeline`. You can still call `ingest_flow(run_id=...)` from Python for testing or custom deployments.

### `transform_flow`

- **Does:** `bronze_to_silver` → **`validate_silver`** → `silver_to_gold` → **`validate_gold`**, then updates the pipeline run summary from Bronze→Silver stats (`_close_run`).
- **Relationship:** Sub-flow of `full_pipeline`; can be run alone if you already have Bronze for `run_date` and want to reprocess (typical after a large Kaggle drop on that date).

### `seed_historical_data` (“historical seed”)

- **Does:** Runs **`ingest_kaggle` only** — downloads configured Kaggle datasets and writes **Bronze** (and related raw-store behaviour in the loader). Pipeline run is closed with an **ingest-only** summary: **it does not run Bronze→Silver or Silver→Gold** inside this flow.
- **Schedule:** **None** by default — **manual** / one-time (`seed-historical-data/one-time-seed` deployment). Use when you want bulk history on disk (and optionally in raw tables), then run a transform when ready.

**Practical note:** Bronze→Silver reads `data/bronze/<source>/<run_date>/`. After a seed, a **`transform_flow` (or `full_pipeline`)** with a matching **`run_date`** is what loads Kaggle envelopes from Bronze into `jobs` / Gold.

---

## 5. What the UI shows (routes and data sources)

React routes (`serving/frontend/src/App.tsx`):

| Page | Purpose | Main APIs / tables |
|------|---------|---------------------|
| **`/`** Dashboard | Snapshot cards, trending skills, top roles, salary highlights, co-occurrence teaser | `/api/overview/metrics`, `/api/skills/trending`, `/api/roles/top`, `/api/skills/cooccurrence`, `/api/salaries` → Gold + `jobs` |
| **`/skills`** Skill trends | Year-over-year skill demand, rankings | `/api/skills/yearly` (uses `daily_skill_demand` and related logic in router) |
| **`/salaries`** Salary explorer | Filterable salary bands by role/country | `/api/salaries` → `salary_summary` |
| **`/predictions`** | Salary prediction UI | `GET /api/predict/options` (dropdown JSON), `POST /api/predict/salary` (champion model in `model_package/`) |

The API’s own description in `serving/api/main.py` states it serves insights from the **medallion** pipeline (`jobs` + Gold).

---

## 6. How key metrics are calculated

Below matches `processing/silver_to_gold.py` and the routers.

### Role / skill demand and moving averages

- For each **(date, role)** or **(date, skill)** from `jobs` / `job_skills`, compute **daily job_count**.
- **moving_avg_7d** / **moving_avg_30d** are trailing window averages over those **daily** counts (partition by role or skill, order by date; windows of 7 and 30 **rows** — i.e. 7 and 30 calendar days present in the series, aligned with the previous Spark semantics documented in that file).

### Trending skills (API)

- Endpoint aggregates `daily_skill_demand` in the selected window and orders by **peak 7d average** (`MAX(moving_avg_7d)`), not only raw totals.

### Top roles (API)

- Sums `job_count` from `daily_role_demand` over the window, `ORDER BY total DESC`, then applies **`LIMIT :top_n`**.

### Salary summary

- For each **role × country** with enough rows: **median, p25, p75, p90** of **(salary_min + salary_max) / 2**, with positive finite salaries and `salary_max >= salary_min`. Rows need a minimum sample (see `HAVING COUNT(*) >= 3` in SQL).

### Remote vs onsite

- Per day and role: counts of `remote = true/false/null`, then **remote_ratio = remote / (remote + onsite)** (unknown excluded from ratio).

### Market alerts

- Built from the same demand series: on the **run date**, if **moving_avg_7d ≥ 2 × moving_avg_30d** (and 30d > 0), emit an alert with **spike_ratio** ≈ `ma7 / ma30`.

### Skill co-occurrence (Gold build)

- SQL joins `job_skills` to itself on `job_id`, keeps ordered pairs (`skill_a < skill_b`), **`HAVING COUNT(*) >= 5`**, **`ORDER BY co_count DESC LIMIT 1000`** — so Gold stores at most **1000** pairs even if more exist in Silver.

### Overview metrics (`/api/overview/metrics`)

- **`jobs_total`:** `COUNT(*)` from **`jobs`**.
- **`jobs_in_window`:** `jobs` with `posted_at` in the selected date range.
- Other fields: counts and ranges from Gold tables (`daily_role_demand`, `daily_skill_demand`, `salary_summary`, `skill_cooccurrence`, `market_alerts`, `remote_vs_onsite`) via SQL subqueries.

---

## 7. Why there can be millions of rows but only “thousands” on the UI

Several mechanisms stack:

1. **Full fidelity lives in PostgreSQL** — especially **`jobs`** (and `job_skills`), which can grow to **millions** of rows after large Kaggle seeds and repeated API ingests (minus deduplication).

2. **The UI is aggregate-first** — Charts and leaderboards read **Gold** time series and summaries, not one point per raw job. A line chart is one point per **day** per selected series, not per posting.

3. **Hard and soft caps**
   - API query params such as **`top_n`** (e.g. roles **1–50**, skills **1–100**, co-occurrence **1–200**) limit rows returned.
   - The dashboard client requests small numbers (e.g. **15** trending skills, **10** top roles, **12** co-occurrence pairs, **8** salary rows after client-side sort).
   - **Gold `skill_cooccurrence`** is capped at **1000** pairs at refresh time.

4. **Dedup and windows** — Bronze→Silver removes duplicate `raw_hash` values within a batch and across runs; date filters on the dashboard further narrow what contributes to visible totals.

So “millions in the warehouse, hundreds to low thousands in the browser” is expected: the product is designed to **summarise** the market, not to paginate through every posting in the UI.

---

## 8. Observability and run lineage

- **`pipeline_runs`** — One row per `full_pipeline` / `seed_historical_data` execution (timestamps, status, summary JSON).
- **`jobs.run_id`** — Ties rows back to the pipeline run that inserted them (when provided).
- **`pipeline_errors`** — Unified error logging for ingestion and transform issues.
- **Prefect UI** (e.g. `http://localhost:4200`) — Task-level logs, retries, and flow run history for `full_pipeline`, nested flows, and manual runs.

---

## 9. Related scripts (quick reference)

| Command / module | Use |
|------------------|-----|
| `python pipeline.py` | Immediate `full_pipeline` + register **8h** schedule (local Prefect). |
| `python pipeline.py --once` | Single `full_pipeline` run. |
| `python pipeline.py --seed` | `seed_historical_data` (Kaggle → Bronze; no Silver/Gold in that flow). |
| `python -m orchestration.schedules` | Apply deployments to your configured Prefect API / work pool. |
| `python -m orchestration.deploy_prefect_managed` | Push Git-sourced deployments to Prefect Managed. |

---

## 10. Note on `raw_pipeline.py` / `unified_jobs_wide`

The root `README.md` may still describe **`raw_pipeline.py`** and **`unified_jobs_wide`** as a parallel normalisation path. The **live FastAPI + React dashboard** described in `serving/api/main.py` reads **`jobs` and Gold tables** produced by **Prefect `full_pipeline` → `transform_flow`**. If you use `raw_pipeline.py`, treat it as an **alternate or exploratory** path unless you explicitly wire the UI to those tables.
