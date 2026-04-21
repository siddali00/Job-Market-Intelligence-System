"""
Bulk Kaggle dataset loader — historical job data seed.

Loads 6 complementary datasets covering 2020–2025 across different platforms,
roles, and geographies.  All datasets are one-time seeds; live data comes from
Adzuna + Remotive APIs on the recurring schedule.

Processing strategy
-------------------
PySpark reads each CSV (any size), applies column normalisation as Spark
transformations, then streams rows to the driver via toLocalIterator() and
writes them to Bronze JSON files in 50 K-record batches.  Driver memory stays
bounded regardless of dataset size (no cap / no full pandas load).

If PySpark / Java is unavailable the loader falls back to pandas read_csv with
chunksize=50_000, which is slightly slower but equally complete.

Dataset catalogue
-----------------
  lukebarousse   / data-analyst-job-postings-google-search  785 K  2022-23  Data
  asaniczka      / 1-3m-linkedin-jobs-and-skills-2024       1.3 M  2024     All
  asaniczka      / data-science-job-postings-and-skills      ~50 K  2024     DS/ML
  ruchi798       / data-science-job-salaries                ~600 K  2020-23  Salary
  christopherkverne / 100k-us-tech-jobs-winter-2024         100 K  2024     US Tech
  pratyushpuri   / global-ai-job-market-trend-2025           ~15 K  2025     AI/ML

Raw storage (Bronze)
---------------------
  • Downloaded CSV/JSON preserved as-is in  data/bronze/kaggle/<dataset>/
  • Normalised envelope written to           data/bronze/kaggle/<date>/page_NNNN.json
    (one page per 50 K records; large datasets produce several page files)
"""

import json
import os
from datetime import date
from pathlib import Path
from typing import Iterator

import pandas as pd

from config.settings import get_settings
from ingestion.base_ingester import BaseIngester
from monitoring.logger import get_logger

logger   = get_logger(__name__)
settings = get_settings()

BRONZE_BATCH_SIZE = 50_000   # rows per bronze JSON file — keeps files manageable

# ── Dataset registry ──────────────────────────────────────────────────────────

DATASETS: dict[str, tuple[str, str]] = {
    "lukebarousse":    ("lukebarousse/data-analyst-job-postings-google-search",
                        "785K data analyst/engineer postings 2022-23"),
    "linkedin_2024":   ("asaniczka/1-3m-linkedin-jobs-and-skills-2024",
                        "1.3M LinkedIn postings 2024, all tech roles"),
    "ds_postings_2024":("asaniczka/data-science-job-postings-and-skills",
                        "~50K data science postings 2024 with skills"),
    "ds_salaries":     ("ruchi798/data-science-job-salaries",
                        "~600K salary records 2020-23"),
    "us_tech_2024":    ("christopherkverne/100k-us-tech-jobs-winter-2024",
                        "100K US tech jobs Winter 2024"),
    "ai_market_2025":  ("pratyushpuri/global-ai-job-market-trend-2025",
                        "~15K global AI/ML job market trends 2025"),
}

DATASET_YEARS: dict[str, int] = {
    "lukebarousse":     2023,
    "linkedin_2024":    2024,
    "ds_postings_2024": 2024,
    "ds_salaries":      2023,
    "us_tech_2024":     2024,
    "ai_market_2025":   2025,
}


# ── Main loader ───────────────────────────────────────────────────────────────

class KaggleDatasetLoader(BaseIngester):
    source_name = "kaggle"

    def __init__(self, dataset_keys: list[str] | None = None) -> None:
        super().__init__()
        self.dataset_keys = dataset_keys or list(DATASETS.keys())
        self._total_records = 0

    def run(self) -> dict:
        self._set_kaggle_env()
        page = 1

        for key in self.dataset_keys:
            slug, description = DATASETS[key]
            logger.info("kaggle_dataset_start", extra={"key": key, "slug": slug})
            try:
                raw_dir = self._download(slug)
                page    = self._process_dataset(key, slug, raw_dir, page)
            except Exception as exc:
                logger.error("kaggle_dataset_failed",
                             extra={"key": key, "error": str(exc)})

        summary = {
            "source":          self.source_name,
            "total_records":   self._total_records,
            "datasets_loaded": self.dataset_keys,
            "date":            str(date.today()),
        }
        logger.info("kaggle_load_complete", extra=summary)
        return summary

    # ── Dataset processor (Spark → pandas fallback) ───────────────────────────

    def _process_dataset(self, key: str, slug: str, raw_dir: Path, page: int) -> int:
        """
        Process one dataset.  Returns the next available page number.

        Reads each file in the directory independently so that auxiliary files
        (job_skills.csv, job_summary.csv, etc.) with different schemas don't
        corrupt the column detection for the main job-postings file.

        Files without a recognisable job-title column are silently skipped.
        XLSX files are handled via pandas (Spark doesn't support XLSX natively).
        """
        year = DATASET_YEARS.get(key)
        total_for_dataset = 0

        # Collect all processable files
        csv_files  = sorted(raw_dir.glob("*.csv"))
        xlsx_files = sorted(raw_dir.glob("*.xlsx"))

        if not csv_files and not xlsx_files:
            raise FileNotFoundError(f"No CSV/XLSX files in {raw_dir}")

        # ── Process each CSV individually ─────────────────────────────────────
        for csv_path in csv_files:
            # Quick column sniff — skip non-job files before loading fully
            try:
                header_df = pd.read_csv(csv_path, nrows=0)
                col_map   = _column_map(key, list(header_df.columns))
                if not col_map.get("title"):
                    logger.info("kaggle_skip_file",
                                extra={"file": csv_path.name, "reason": "no title column"})
                    continue
            except Exception:
                continue

            try:
                page = self._process_spark_file(key, slug, csv_path, year, page)
            except Exception as exc:
                logger.warning("spark_fallback",
                               extra={"key": key, "file": csv_path.name, "reason": str(exc)})
                page = self._process_pandas_file(key, slug, csv_path, year, page)

            total_for_dataset += 0   # counts are added inside helpers via self._total_records

        # ── Process XLSX files via pandas (Spark can't read XLSX) ────────────
        for xlsx_path in xlsx_files:
            try:
                page = self._process_xlsx_file(key, slug, xlsx_path, year, page)
            except Exception as exc:
                logger.warning("xlsx_failed",
                               extra={"key": key, "file": xlsx_path.name, "error": str(exc)})

        return page

    # ── PySpark path (single file) ────────────────────────────────────────────

    def _process_spark_file(
        self, key: str, slug: str,
        csv_path: Path, year: int | None, page: int,
    ) -> int:
        """Read one CSV with Spark, stream rows to driver, write bronze in batches.
        Batch counter is per-dataset (resets to 1 for each key).
        Files are written to  data/bronze/kaggle/<date>/<key>/batch_NNNN.json
        """
        spark = _get_or_create_spark()

        df   = spark.read.csv(str(csv_path), header=True, inferSchema=False)
        cols = df.columns
        col_map = _column_map(key, cols)

        logger.info("spark_reading_csv",
                    extra={"key": key, "file": csv_path.name,
                           "partitions": df.rdd.getNumPartitions()})

        batch: list[dict] = []
        total = 0
        batch_num = 1   # per-dataset counter — readable filenames

        for spark_row in df.toLocalIterator():
            row    = spark_row.asDict()
            record = _build_envelope(key, row, slug, year, col_map)
            if record and record.get("title", "").strip():
                batch.append(record)
                if len(batch) >= BRONZE_BATCH_SIZE:
                    self.save_to_bronze(batch, page=batch_num, subfolder=key)
                    total    += len(batch)
                    batch     = []
                    batch_num += 1

        if batch:
            self.save_to_bronze(batch, page=batch_num, subfolder=key)
            total += len(batch)

        self._total_records += total
        logger.info("kaggle_file_done",
                    extra={"key": key, "file": csv_path.name,
                           "records": total, "engine": "spark"})
        return page   # outer page counter no longer used for kaggle (kept for API compat)

    # ── Pandas fallback path (single CSV file) ────────────────────────────────

    def _process_pandas_file(
        self, key: str, slug: str,
        csv_path: Path, year: int | None, page: int,
    ) -> int:
        """Read one CSV in 50K pandas chunks — no full-file memory load."""
        total     = 0
        col_map   = None
        batch_num = 1

        for chunk in pd.read_csv(csv_path, chunksize=BRONZE_BATCH_SIZE, low_memory=False):
            if col_map is None:
                col_map = _column_map(key, list(chunk.columns))

            records = []
            for row in chunk.to_dict(orient="records"):
                rec = _build_envelope(key, row, slug, year, col_map)
                if rec and rec.get("title", "").strip():
                    records.append(rec)

            if records:
                self.save_to_bronze(records, page=batch_num, subfolder=key)
                total     += len(records)
                batch_num += 1

        self._total_records += total
        logger.info("kaggle_file_done",
                    extra={"key": key, "file": csv_path.name,
                           "records": total, "engine": "pandas"})
        return page

    # ── XLSX path (pandas only — Spark can't read XLSX) ───────────────────────

    def _process_xlsx_file(
        self, key: str, slug: str,
        xlsx_path: Path, year: int | None, page: int,
    ) -> int:
        """
        Read one XLSX file in chunks via pandas.
        Uses openpyxl (installed as a project dependency).
        Skips sheets that don't have a recognisable title column.
        """
        total = 0

        try:
            df_full = pd.read_excel(xlsx_path, engine="openpyxl")
        except Exception as exc:
            logger.warning("xlsx_read_failed",
                           extra={"file": xlsx_path.name, "error": str(exc)})
            return page

        col_map = _column_map(key, list(df_full.columns))
        if not col_map.get("title"):
            logger.info("kaggle_skip_file",
                        extra={"file": xlsx_path.name, "reason": "no title column"})
            return page

        logger.info("xlsx_reading",
                    extra={"key": key, "file": xlsx_path.name, "rows": len(df_full)})

        batch_num = 1
        for start in range(0, len(df_full), BRONZE_BATCH_SIZE):
            chunk   = df_full.iloc[start : start + BRONZE_BATCH_SIZE]
            records = []
            for row in chunk.to_dict(orient="records"):
                rec = _build_envelope(key, row, slug, year, col_map)
                if rec and rec.get("title", "").strip():
                    records.append(rec)

            if records:
                self.save_to_bronze(records, page=batch_num, subfolder=key)
                total     += len(records)
                batch_num += 1

        self._total_records += total
        logger.info("kaggle_file_done",
                    extra={"key": key, "file": xlsx_path.name,
                           "records": total, "engine": "pandas_xlsx"})
        return page

    # ── Helpers ───────────────────────────────────────────────────────────────

    def fetch_page(self, page: int) -> list[dict]:
        return []   # not used — run() overrides

    def _set_kaggle_env(self) -> None:
        os.environ.setdefault("KAGGLE_USERNAME", settings.kaggle_username)
        os.environ.setdefault("KAGGLE_KEY",      settings.kaggle_key)

    def _download(self, slug: str) -> Path:
        """Download to bronze/kaggle/<slug>/ — idempotent (skips if already exists)."""
        safe_name = slug.replace("/", "__")
        raw_dir   = Path(settings.bronze_storage_path) / "kaggle" / safe_name
        raw_dir.mkdir(parents=True, exist_ok=True)

        existing = list(raw_dir.glob("*.csv")) + list(raw_dir.glob("*.json"))
        if existing:
            logger.info("kaggle_already_downloaded",
                        extra={"slug": slug, "files": len(existing)})
            return raw_dir

        from kaggle import KaggleApi
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(slug, path=str(raw_dir), unzip=True)
        logger.info("kaggle_download_ok", extra={"slug": slug, "dir": str(raw_dir)})
        return raw_dir


# ── Column resolution ─────────────────────────────────────────────────────────

def _column_map(key: str, cols: list[str]) -> dict[str, str | None]:
    """
    Return a mapping  field_name → actual_column_name  for a given dataset key.
    Each entry is None if the column cannot be resolved.
    """
    c = {c.lower(): c for c in cols}   # case-insensitive lookup helper

    def pick(*candidates: str) -> str | None:
        for candidate in candidates:
            if candidate in c:
                return c[candidate]
        return None

    common = {
        "title":      pick("job_title", "title", "position", "role"),
        "company":    pick("company_name", "company", "employer", "organization"),
        "location":   pick("job_location", "location", "city", "region"),
        "country":    pick("job_country", "country", "search_location", "search_country",
                           "company_location", "employee_residence"),
        "remote":     pick("job_work_from_home", "work_from_home", "remote_friendly",
                           "is_remote", "remote"),
        "posted_at":  pick("job_posted_date", "posted_at", "date_time", "first_seen",
                           "posting_date", "date_posted"),
        "sal_single": pick("salary_year_avg", "salary_yearly", "salary_avg",
                           "salary_standardized", "salary_in_usd", "salary_usd",
                           "annual_salary", "mean_salary", "salary"),
        "sal_min":    pick("min_salary", "salary_min", "salary_from", "low_salary",
                           "min_amount"),
        "sal_max":    pick("max_salary", "salary_max", "salary_to",   "high_salary",
                           "max_amount"),
        "skills":     pick("job_skills", "description_tokens", "required_skills",
                           "skills", "job_skills"),
        "description":pick("description", "job_description", "cleaned_description",
                           "summary"),
        "job_type":   pick("job_type", "employment_type", "work_type", "schedule_type"),
    }

    # Dataset-specific extras
    if key == "ds_salaries":
        common["work_year"]        = pick("work_year")
        common["experience_level"] = pick("experience_level")
        common["employment_type"]  = pick("employment_type")
        common["company_size"]     = pick("company_size")
        common["remote_ratio"]     = pick("remote_ratio")

    if key in ("linkedin_2024", "ds_postings_2024", "us_tech_2024"):
        common["job_link"] = pick("job_link", "job_url", "job_url_direct")

    return common


# ── Envelope builder ──────────────────────────────────────────────────────────

def _build_envelope(
    key: str, row: dict, slug: str, year: int | None,
    col_map: dict[str, str | None],
) -> dict | None:
    """Convert one raw CSV row to the standard ingestion envelope."""

    def g(field: str) -> str:
        col = col_map.get(field)
        if col is None:
            return ""
        v = row.get(col, "")
        if v is None or (isinstance(v, float) and v != v):   # NaN guard
            return ""
        return str(v).strip()

    def gf(field: str) -> float | None:
        return _safe_float(g(field))

    title    = g("title")
    if not title:
        return None

    company  = g("company")
    location = g("location")
    country  = g("country")[:100] if g("country") else None
    posted   = g("posted_at")
    desc     = g("description")[:3000]
    job_type = g("job_type")

    # Salary
    sal_single = gf("sal_single")
    sal_min    = gf("sal_min") or sal_single
    sal_max    = gf("sal_max") or sal_single

    # Remote flag
    if key == "ds_salaries":
        rr = _safe_float(g("remote_ratio"))
        is_remote = (rr >= 50) if rr is not None else None
    else:
        is_remote = _parse_bool(g("remote")) if col_map.get("remote") else _remote_from_type(job_type)

    # Skills
    skills_raw = g("skills")
    if key == "ds_salaries":
        skills: list[str] = []
    elif "," in (skills_raw or ""):
        skills = [s.strip().lower() for s in skills_raw.split(",") if s.strip()]
    else:
        skills = _parse_skills_list(skills_raw)

    # Dataset-year override for ds_salaries (work_year column is authoritative)
    ds_year = year
    if key == "ds_salaries":
        wy = _safe_float(g("work_year"))
        ds_year = int(wy) if wy else year

    # Extra metadata
    experience_level = g("experience_level") if key == "ds_salaries" else None
    employment_type  = (g("employment_type") if key == "ds_salaries"
                        else (job_type or None))
    company_size     = g("company_size")     if key == "ds_salaries" else None

    # Work-type string
    if is_remote is True:
        work_type = "Remote"
    elif is_remote is False:
        work_type = "On-site"
    else:
        work_type = None

    ext_id = str(row.get("job_id", row.get("job_link", row.get("job_url", ""))) or "")

    return {
        "source":                 "kaggle",
        "external_id":            ext_id,
        "title":                  title,
        "title_normalized_hint":  "",
        "company":                company,
        "location":               location,
        "country":                country,
        "description":            desc,
        "salary_min":             sal_min,
        "salary_max":             sal_max,
        "remote":                 is_remote,
        "skills_extracted":       skills,
        "posted_at":              posted,
        # Kaggle-detail fields
        "dataset_slug":           slug,
        "dataset_year":           ds_year,
        "work_type":              work_type,
        "experience_level":       experience_level,
        "employment_type":        employment_type,
        "company_size":           company_size,
    }


# ── PySpark session helper ────────────────────────────────────────────────────

def _get_or_create_spark():
    """
    Get or create a local SparkSession for CSV ingestion.

    Windows requirements (all handled here automatically):
      • Java 17  — install once with: winget install EclipseAdoptium.Temurin.17.JDK
      • winutils.exe at C:\\hadoop\\bin  — already placed by project setup
      • JAVA_HOME / HADOOP_HOME in .env  — already configured

    The path-with-spaces workaround: PySpark workers must be launched via a
    space-free path.  A directory junction C:\\jmenv -> <venv> is created once
    so PYSPARK_PYTHON never contains spaces (Windows requirement).

    NOTE: our loader uses only spark.read.csv + toLocalIterator — both are
    pure JVM operations that don't need Python worker processes, so the
    Python-worker socket issue on Windows is irrelevant for this use-case.
    """
    import os
    import subprocess
    import sys
    from pathlib import Path
    from dotenv import load_dotenv

    load_dotenv()

    # ── Java / Hadoop ──────────────────────────────────────────────────────────
    java_home    = os.environ.get("JAVA_HOME", "")
    hadoop_home  = os.environ.get("HADOOP_HOME", "C:\\hadoop")
    if java_home:
        os.environ["JAVA_HOME"]   = java_home
        os.environ["HADOOP_HOME"] = hadoop_home
        java_bin = str(Path(java_home) / "bin")
        if java_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + java_bin
        hadoop_bin = str(Path(hadoop_home) / "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] += os.pathsep + hadoop_bin

    # ── Python worker path (must be space-free on Windows) ────────────────────
    # Create junction C:\jmenv -> <venv dir> once if it doesn't exist.
    venv_dir = Path(sys.executable).parent.parent   # .../env
    junction  = Path("C:/jmenv")
    if not junction.exists() and " " in str(venv_dir):
        subprocess.run(
            ["cmd", "/c", f'mklink /J "{junction}" "{venv_dir}"'],
            capture_output=True,
        )

    py_exe = str(junction / "Scripts" / "python.exe") if junction.exists() else sys.executable
    py_exe = py_exe.replace("\\", "/")

    os.environ["PYSPARK_PYTHON"]        = py_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = py_exe
    os.environ["SPARK_LOCAL_IP"]        = "127.0.0.1"

    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName("KaggleLoader")
        .master("local[*]")
        .config("spark.driver.memory",           "4g")
        .config("spark.sql.shuffle.partitions",  "8")
        .config("spark.ui.showConsoleProgress",  "false")
        .config("spark.pyspark.python",           py_exe)
        .getOrCreate()
    )


# ── Shared helpers ────────────────────────────────────────────────────────────

def _safe_float(value) -> float | None:
    if value is None:
        return None
    try:
        s = str(value).strip().lower().replace(",", "").replace("$", "")
        if s.endswith("k"):
            return float(s[:-1]) * 1000
        f = float(s)
        return f if f > 0 else None
    except (ValueError, TypeError):
        return None


def _parse_bool(value) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in ("true", "1", "yes", "t"):
        return True
    if s in ("false", "0", "no", "f"):
        return False
    return None


def _remote_from_type(job_type: str) -> bool | None:
    if not job_type:
        return None
    jt = job_type.lower()
    if "remote" in jt:
        return True
    if "on-site" in jt or "onsite" in jt or "in-person" in jt:
        return False
    if "hybrid" in jt:
        return False
    return None


def _parse_skills_list(value) -> list[str]:
    if not value or value != value:
        return []
    s = str(value).strip()
    try:
        parsed = json.loads(s.replace("'", '"'))
        if isinstance(parsed, list):
            return [str(x).lower().strip() for x in parsed if x]
    except Exception:
        pass
    return [x.strip().lower() for x in s.strip("[]").split(",") if x.strip()]
