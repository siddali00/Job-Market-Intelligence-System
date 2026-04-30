"""
Bronze → Silver transformation (PySpark-powered).

Pipeline
────────
1. Read all Bronze JSON files for today's date partition into a Spark DataFrame.
2. Clean: trim whitespace, cast types, cap description length, sanitise salaries.
3. Deduplicate within batch using Spark (dropDuplicates on raw_hash).
4. Cross-run dedup: skip rows whose raw_hash already exists in the Silver DB.
5. Write new rows to PostgreSQL via SQLAlchemy (source-detail tables included).
6. Update PipelineRun stats in DB.

Fallback
────────
If PySpark / Java is not available, falls back to plain pandas so the pipeline
still runs. A WARNING is logged when the fallback is used.

PySpark setup (one-time)
────────────────────────
  - Java 11+ must be installed and JAVA_HOME set.
  - pip install pyspark>=3.4.0
  - On Windows: set HADOOP_HOME + download winutils.exe (only needed if Spark
    writes files; since we collect() and write via SQLAlchemy it is optional).
"""

import hashlib
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

from config.settings import get_settings
from monitoring.logger import get_logger
from storage.db import SessionLocal
from storage.models import (
    AdzunaJobDetail, Job, JobSkill,
    KaggleJobDetail, PipelineRun, RemotiveJobDetail, PipelineError,
)

logger   = get_logger(__name__)
settings = get_settings()

SKILL_KEYWORDS = [
    "python", "sql", "r", "scala", "java", "javascript", "typescript", "go",
    "rust", "c++", "spark", "hadoop", "kafka", "airflow", "prefect", "dbt",
    "pandas", "numpy", "scikit-learn", "tensorflow", "pytorch", "keras",
    "aws", "gcp", "azure", "docker", "kubernetes", "terraform", "fastapi",
    "flask", "django", "react", "vue", "postgres", "mysql", "mongodb",
    "redis", "elasticsearch", "tableau", "powerbi", "looker", "snowflake",
    "databricks", "bigquery", "redshift", "dask", "pyspark", "mlflow",
    "git", "linux", "bash", "excel", "power bi", "data engineering",
    "machine learning", "deep learning", "nlp", "llm",
]


# ── Public entry point ────────────────────────────────────────────────────────

def run(
    source: str | None = None,
    run_date: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """
    Main entry point called by Prefect.
    Tries PySpark first; falls back to pandas if unavailable.
    """
    run_date = run_date or str(date.today())

    try:
        return _run_spark(source, run_date, run_id)
    except Exception as exc:
        logger.warning(
            "spark_unavailable_pandas_fallback",
            extra={"error": str(exc)},
        )
        return _run_pandas(source, run_date, run_id)


# ══════════════════════════════════════════════════════════════════════════════
# PySpark path
# ══════════════════════════════════════════════════════════════════════════════

def _run_spark(source: str | None, run_date: str, run_id: str | None) -> dict:
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window

    bronze_root = Path(settings.bronze_storage_path)
    sources     = _list_sources(bronze_root, source, run_date)
    if not sources:
        return _empty_summary("spark", run_date)

    spark = _get_spark_session()

    # ── 1. Collect JSON file paths and per-source counts ──────────────────────
    # We read directly with spark.read.json() — pure JVM operation, no Python
    # workers needed, no type-merge conflicts when we supply an explicit schema.
    all_paths:  list[str]      = []
    raw_counts: dict[str, int] = {}

    for src in sources:
        partition_dir = bronze_root / src / run_date
        # Use **/*.json to pick up both flat files (adzuna, remotive) and
        # per-dataset subfolders (kaggle/<date>/<key>/batch_NNNN.json)
        json_files    = sorted(partition_dir.glob("**/*.json"))
        for jf in json_files:
            all_paths.append(str(jf))
        raw_counts[src] = 0   # filled in after read below

    if not all_paths:
        return _empty_summary("spark", run_date)

    # ── 2. Read all JSON files with explicit schema ────────────────────────────
    # Explicit schema avoids [CANNOT_MERGE_TYPE] when salary fields are int in
    # some files and float in others (happens across Kaggle/Adzuna/Remotive).
    df = (
        spark.read
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .schema(_BRONZE_SCHEMA)
        .json(all_paths)
    )

    # Backfill per-source raw counts using the source column
    for row in df.groupBy("source").count().collect():
        raw_counts[row["source"]] = row["count"]
    total_raw = sum(raw_counts.values())

    logger.info("spark_loaded_bronze",
                extra={"total_raw": total_raw, "by_source": raw_counts})

    # ── 3. Clean ──────────────────────────────────────────────────────────────
    df = _spark_clean(df, F)

    # ── 4. Add raw_hash column ────────────────────────────────────────────────
    df = df.withColumn(
        "raw_hash",
        F.sha2(
            F.concat_ws("|",
                F.lower(F.trim(F.col("title"))),
                F.lower(F.trim(F.coalesce(F.col("company"), F.lit("")))),
                F.coalesce(F.col("posted_at"), F.lit("")),
                F.col("source"),
            ),
            256,
        ),
    )

    total_before_dedup = df.count()

    # ── 5. Deduplicate WITHIN batch ───────────────────────────────────────────
    w = Window.partitionBy("raw_hash").orderBy(F.monotonically_increasing_id())
    df = (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )
    dupes_in_batch = total_before_dedup - df.count()
    logger.info("spark_dedup_in_batch", extra={"removed": dupes_in_batch})

    rows_after_dedup = total_before_dedup - dupes_in_batch
    # ── 6. Collect, then cross-run dedup in Python ────────────────────────────
    # Avoids a second createDataFrame call (which needs Python workers on Windows).
    # collect() can take many minutes on ~1M+ rows and looks "stuck" with no logs.
    logger.info(
        "spark_collecting_driver",
        extra={
            "rows": rows_after_dedup,
            "hint": "Serializing Spark rows to Python; then PostgreSQL row-by-row writes follow.",
        },
    )
    records = [row.asDict() for row in df.collect()]
    logger.info("spark_collect_done", extra={"rows": len(records)})

    existing_hashes = set(_fetch_existing_hashes())
    before_xrun     = len(records)
    if existing_hashes:
        records = [r for r in records if r.get("raw_hash") not in existing_hashes]
    already_in_db = before_xrun - len(records)

    # ── 7. Write to PostgreSQL ────────────────────────────────────────────────
    # Rows are already filtered against DB hashes — skip per-row SELECT in _upsert_job.
    inserted, skipped, errors = _write_records(records, run_id, trust_prefilter=True)

    summary = {
        "engine":             "spark",
        "stage":              "bronze_to_silver",
        "run_date":           run_date,
        "total_raw":          total_raw,
        "duplicates_removed": dupes_in_batch,
        "already_in_db":      already_in_db,
        "total_inserted":     inserted,
        "total_skipped":      skipped,
        "total_errors":       errors,
        "by_source":          raw_counts,
    }
    logger.info("bronze_to_silver_complete", extra=summary)
    return summary


# Explicit schema for bronze JSON files.
# Pinning salary fields to DoubleType prevents [CANNOT_MERGE_TYPE] errors that
# occur when Adzuna writes integer salaries and Kaggle writes float salaries.
def _make_bronze_schema():
    from pyspark.sql.types import (
        ArrayType, BooleanType, DoubleType, IntegerType,
        StringType, StructField, StructType,
    )
    S, D, B, I, A = StringType(), DoubleType(), BooleanType(), IntegerType(), ArrayType(StringType())
    return StructType([
        StructField("source",                 S, True),
        StructField("external_id",            S, True),
        StructField("title",                  S, True),
        StructField("title_normalized_hint",  S, True),
        StructField("company",                S, True),
        StructField("location",               S, True),
        StructField("country",                S, True),
        StructField("description",            S, True),
        StructField("salary_min",             D, True),   # always Double
        StructField("salary_max",             D, True),   # always Double
        StructField("remote",                 B, True),
        StructField("skills_extracted",       A, True),
        StructField("posted_at",              S, True),
        # Adzuna extras
        StructField("category",               S, True),
        StructField("source_url",             S, True),
        StructField("contract_type",          S, True),
        StructField("contract_time",          S, True),
        StructField("adref",                  S, True),
        # Remotive extras
        StructField("source_attribution",     S, True),
        StructField("job_type",               S, True),
        StructField("salary_raw",             S, True),
        StructField("url",                    S, True),
        # Kaggle extras
        StructField("dataset_slug",           S, True),
        StructField("dataset_year",           I, True),
        StructField("work_type",              S, True),
        StructField("experience_level",       S, True),
        StructField("employment_type",        S, True),
        StructField("company_size",           S, True),
    ])

_BRONZE_SCHEMA = _make_bronze_schema()


def _spark_clean(df, F):
    """Apply all cleaning transforms on the Spark DataFrame."""
    return (
        df
        # String trimming
        .withColumn("title",       F.trim(F.coalesce(F.col("title"),       F.lit(""))))
        .withColumn("company",     F.trim(F.coalesce(F.col("company"),     F.lit(""))))
        .withColumn("location",    F.trim(F.coalesce(F.col("location"),    F.lit(""))))
        .withColumn("description", F.trim(F.coalesce(F.col("description"), F.lit(""))))
        # Truncate description to 5000 chars
        .withColumn("description", F.substring(F.col("description"), 1, 5000))
        # Drop rows without a title
        .filter(F.length(F.trim(F.col("title"))) > 0)
        # Salary: null out zero / negative values
        .withColumn("salary_min",
            F.when(F.col("salary_min").cast("double") > 0,
                   F.col("salary_min").cast("double")).otherwise(F.lit(None))
        )
        .withColumn("salary_max",
            F.when(F.col("salary_max").cast("double") > 0,
                   F.col("salary_max").cast("double")).otherwise(F.lit(None))
        )
        # Salary sanity: max should be >= min
        .withColumn("salary_max",
            F.when(
                (F.col("salary_max").isNotNull()) &
                (F.col("salary_min").isNotNull()) &
                (F.col("salary_max") < F.col("salary_min")),
                F.col("salary_min"),
            ).otherwise(F.col("salary_max"))
        )
        # remote: coerce to boolean
        .withColumn("remote",
            F.when(F.col("remote").cast("boolean") == True,  F.lit(True))
             .when(F.col("remote").cast("boolean") == False, F.lit(False))
             .otherwise(F.lit(None))
        )
        # Drop the internal tag we added for counting
        .drop("_source_file")
    )


def _get_spark_session():
    import logging
    import os
    import subprocess
    import sys
    from pathlib import Path as _Path
    from dotenv import load_dotenv
    from pyspark.sql import SparkSession

    load_dotenv()

    # ── Java / Hadoop env ─────────────────────────────────────────────────────
    java_home   = os.environ.get("JAVA_HOME", "")
    hadoop_home = os.environ.get("HADOOP_HOME", "C:\\hadoop")
    if java_home:
        os.environ["JAVA_HOME"]   = java_home
        os.environ["HADOOP_HOME"] = hadoop_home
        for bin_dir in (_Path(java_home) / "bin", _Path(hadoop_home) / "bin"):
            s = str(bin_dir)
            if s not in os.environ.get("PATH", ""):
                os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + s

    # ── Space-free Python path (Windows requirement for PySpark workers) ──────
    venv_dir = _Path(sys.executable).parent.parent
    junction  = _Path("C:/jmenv")
    if not junction.exists() and " " in str(venv_dir):
        subprocess.run(["cmd", "/c", f'mklink /J "{junction}" "{venv_dir}"'],
                       capture_output=True)
    py_exe = str(junction / "Scripts" / "python.exe") if junction.exists() else sys.executable
    py_exe = py_exe.replace("\\", "/")

    os.environ["PYSPARK_PYTHON"]        = py_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = py_exe
    os.environ["SPARK_LOCAL_IP"]        = "127.0.0.1"

    spark = (
        SparkSession.builder
        .appName("JobMarketIntelligence-BronzeToSilver")
        .master("local[*]")
        .config("spark.driver.memory",           "4g")
        .config("spark.sql.shuffle.partitions",  "8")
        .config("spark.ui.showConsoleProgress",  "false")
        .config("spark.pyspark.python",           py_exe)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)
    return spark


# ══════════════════════════════════════════════════════════════════════════════
# Pandas fallback path  (identical logic, no Spark dependency)
# ══════════════════════════════════════════════════════════════════════════════

def _run_pandas(source: str | None, run_date: str, run_id: str | None) -> dict:
    import pandas as pd

    bronze_root = Path(settings.bronze_storage_path)
    sources     = _list_sources(bronze_root, source, run_date)
    if not sources:
        return _empty_summary("pandas", run_date)

    all_records: list[dict] = []
    raw_counts: dict[str, int] = {}

    for src in sources:
        partition_dir = bronze_root / src / run_date
        for jf in sorted(partition_dir.glob("**/*.json")):
            rows = json.loads(jf.read_text(encoding="utf-8"))
            all_records.extend(rows)
            raw_counts[src] = raw_counts.get(src, 0) + len(rows)

    if not all_records:
        return _empty_summary("pandas", run_date)

    df = pd.DataFrame(all_records)

    # ── Clean ─────────────────────────────────────────────────────────────────
    df["title"]       = df.get("title",       pd.Series(dtype=str)).fillna("").str.strip()
    df["company"]     = df.get("company",     pd.Series(dtype=str)).fillna("").str.strip()
    df["location"]    = df.get("location",    pd.Series(dtype=str)).fillna("").str.strip()
    df["description"] = df.get("description", pd.Series(dtype=str)).fillna("").str.strip()
    df["description"] = df["description"].str[:5000]
    df = df[df["title"].str.len() > 0].copy()

    for col in ["salary_min", "salary_max"]:
        df[col] = pd.to_numeric(df.get(col, pd.Series(dtype=float)), errors="coerce")
        df.loc[df[col] <= 0, col] = None

    # ── raw_hash ──────────────────────────────────────────────────────────────
    def _make_hash(row):
        parts = "|".join([
            str(row.get("title",    "") or "").strip().lower(),
            str(row.get("company",  "") or "").strip().lower(),
            str(row.get("posted_at","") or ""),
            str(row.get("source",   "") or ""),
        ])
        return hashlib.sha256(parts.encode()).hexdigest()

    df["raw_hash"] = df.apply(_make_hash, axis=1)

    # ── Dedup within batch ────────────────────────────────────────────────────
    before = len(df)
    df = df.drop_duplicates(subset=["raw_hash"], keep="first")
    dupes_in_batch = before - len(df)

    # ── Cross-run dedup ───────────────────────────────────────────────────────
    existing = _fetch_existing_hashes()
    df = df[~df["raw_hash"].isin(existing)]

    records   = df.to_dict(orient="records")
    inserted, skipped, errors = _write_records(records, run_id, trust_prefilter=True)

    summary = {
        "engine":             "pandas",
        "stage":              "bronze_to_silver",
        "run_date":           run_date,
        "total_raw":          len(all_records),
        "duplicates_removed": dupes_in_batch,
        "already_in_db":      len(existing),
        "total_inserted":     inserted,
        "total_skipped":      skipped,
        "total_errors":       errors,
        "by_source":          raw_counts,
    }
    logger.info("bronze_to_silver_complete", extra=summary)
    return summary


# ══════════════════════════════════════════════════════════════════════════════
# Shared DB write helpers
# ══════════════════════════════════════════════════════════════════════════════

def _write_records(
    records: list[dict],
    run_id: str | None,
    *,
    trust_prefilter: bool = False,
) -> tuple[int, int, int]:
    """
    Write a list of cleaned, deduplicated records to Silver tables.

    Uses a SAVEPOINT per record so that one bad row doesn't abort the entire
    batch — the previous SAWarning was caused by calling db.rollback() on the
    outer transaction, which wiped all pending state including the error log.

    When ``trust_prefilter`` is True, callers assert ``raw_hash`` was already
    checked against the DB (cross-run dedup). Skips an extra SELECT per row,
    which matters for large Kaggle loads.
    """
    inserted = skipped = errors = 0
    total = len(records)
    log_every = 10_000
    chunk_size = 2_000
    db = SessionLocal()

    try:
        for chunk_start in range(0, total, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total)
            chunk = records[chunk_start:chunk_end]

            # Open a transaction per chunk to avoid one massive long-running tx.
            db.begin()
            try:
                for i, raw in enumerate(chunk, start=chunk_start + 1):
                    if log_every and i % log_every == 0:
                        logger.info(
                            "silver_write_progress",
                            extra={"processed": i, "total": total, "inserted": inserted, "errors": errors},
                        )
                    savepoint = db.begin_nested()   # SAVEPOINT
                    try:
                        result = _upsert_job(db, raw, run_id, skip_hash_lookup=trust_prefilter)
                        savepoint.commit()          # RELEASE SAVEPOINT
                        if result == "inserted":
                            inserted += 1
                        else:
                            skipped += 1
                    except Exception as exc:
                        savepoint.rollback()        # ROLLBACK TO SAVEPOINT — chunk tx intact
                        errors += 1
                        # Log the error in its own savepoint so it persists
                        try:
                            err_sp = db.begin_nested()
                            db.add(PipelineError(
                                run_id        = run_id,
                                stage         = "bronze_to_silver",
                                error_type    = type(exc).__name__,
                                error_message = str(exc)[:500],
                                record_id     = str(raw.get("external_id", ""))[:250],
                            ))
                            err_sp.commit()
                        except Exception:
                            try:
                                err_sp.rollback()
                            except Exception:
                                pass

                db.commit()
                logger.info(
                    "silver_write_chunk_committed",
                    extra={"chunk_start": chunk_start + 1, "chunk_end": chunk_end, "total": total},
                )
            except Exception:
                db.rollback()
                raise
    finally:
        db.close()

    return inserted, skipped, errors


def _upsert_job(
    db,
    raw: dict,
    run_id: str | None,
    *,
    skip_hash_lookup: bool = False,
) -> str:
    raw_hash = str(raw.get("raw_hash") or _make_hash_from_raw(raw))

    if not skip_hash_lookup and db.query(Job).filter(Job.raw_hash == raw_hash).first():
        return "skipped"

    title        = (raw.get("title")   or "").strip()
    company_name = (raw.get("company") or "").strip()
    source       = raw.get("source", "unknown")

    salary_min = _to_float(raw.get("salary_min"))
    salary_max = _to_float(raw.get("salary_max"))

    location = (raw.get("location") or "").strip()
    country  = (raw.get("country")  or _infer_country(location) or "").upper() or None
    city     = _infer_city(location)

    remote = raw.get("remote")
    if remote is None:
        remote = _infer_remote(f"{title} {location} {raw.get('description','')}")

    posted_at = _parse_datetime(raw.get("posted_at") or "")

    pre_extracted = raw.get("skills_extracted") or []
    skills_found  = (
        [s.lower().strip() for s in pre_extracted if s]
        if pre_extracted
        else _extract_skills(raw.get("description") or "")
    )

    category_hint = raw.get("category") or ""
    title_norm    = raw.get("title_normalized_hint") or _normalize_title(title, category_hint)

    def _trunc(val, n: int) -> str | None:
        if val is None:
            return None
        return str(val)[:n]

    job = Job(
        source           = source,
        external_id      = _trunc(raw.get("external_id") or "", 255),
        raw_hash         = raw_hash,
        run_id           = run_id,
        title            = _trunc(title, 512),
        title_normalized = _trunc(title_norm, 255),
        company_name     = _trunc(company_name, 255),
        location         = _trunc(location, 255),
        country          = _trunc(country, 100),
        city             = _trunc(city, 100),
        remote           = remote,
        salary_min       = salary_min,
        salary_max       = salary_max,
        salary_currency  = "USD",
        salary_missing   = salary_min is None and salary_max is None,
        skill_missing    = len(skills_found) == 0,
        description      = (raw.get("description") or "")[:5000],
        posted_at        = posted_at,
    )
    db.add(job)
    db.flush()

    for sname in skills_found:
        db.add(JobSkill(job_id=job.id, skill_name=sname))   # denormalised inline

    _upsert_source_detail(db, job.id, source, raw)
    return "inserted"


def _upsert_source_detail(db, job_id: int, source: str, raw: dict) -> None:
    def _s(val, n: int) -> str | None:
        if val is None:
            return None
        v = str(val).strip()
        return v[:n] if v else None

    if source == "adzuna":
        db.add(AdzunaJobDetail(
            job_id              = job_id,
            contract_type       = _s(raw.get("contract_type"), 50),
            contract_time       = _s(raw.get("contract_time"), 50),
            salary_is_predicted = raw.get("salary_is_predicted"),
            category            = _s(raw.get("category"), 255),
            category_tag        = _s(raw.get("category_tag"), 100),
            country_code        = _s((raw.get("country") or "").lower(), 10),
            location_area       = _s(raw.get("location_area"), 255),
        ))
    elif source == "remotive":
        db.add(RemotiveJobDetail(
            job_id     = job_id,
            job_type   = _s(raw.get("job_type"), 50),
            category   = _s(raw.get("category"), 255),
            salary_raw = _s(raw.get("salary_raw"), 255),
            source_url = raw.get("url") or None,
        ))
    elif source.startswith("kaggle"):
        db.add(KaggleJobDetail(
            job_id            = job_id,
            dataset_slug      = _s(raw.get("dataset_slug"), 255),
            dataset_year      = _to_int(raw.get("dataset_year")),
            work_type         = _s(raw.get("work_type"), 50),
            experience_level  = _s(raw.get("experience_level"), 50),
            employment_type   = _s(raw.get("employment_type"), 50),
            company_size      = _s(raw.get("company_size"), 10),
        ))


# ── PipelineRun helpers ───────────────────────────────────────────────────────

def create_pipeline_run(run_id: str) -> None:
    """Insert a new PipelineRun row at the start of a pipeline execution."""
    db = SessionLocal()
    try:
        db.add(PipelineRun(run_id=run_id, started_at=datetime.utcnow(), status="running"))
        db.commit()
    finally:
        db.close()


def update_pipeline_run(run_id: str, summary: dict, status: str = "completed") -> None:
    """Update PipelineRun with final stats after transform completes."""
    db = SessionLocal()
    try:
        pr: PipelineRun | None = db.query(PipelineRun).filter(PipelineRun.run_id == run_id).first()
        if not pr:
            return
        pr.completed_at      = datetime.utcnow()
        pr.status            = status
        pr.jobs_inserted     = summary.get("total_inserted", 0)
        pr.jobs_skipped      = summary.get("total_skipped",  0)
        pr.duplicates_removed= summary.get("duplicates_removed", 0)
        pr.errors_count      = summary.get("total_errors",   0)
        pr.adzuna_raw_count  = summary.get("by_source", {}).get("adzuna",   0)
        pr.remotive_raw_count= summary.get("by_source", {}).get("remotive", 0)
        pr.kaggle_raw_count  = summary.get("by_source", {}).get("kaggle",   0)
        pr.jobs_total_in_db  = db.query(Job).count()
        db.commit()
    finally:
        db.close()


# ── Small helpers ─────────────────────────────────────────────────────────────

def _list_sources(bronze_root: Path, source: str | None, run_date: str) -> list[str]:
    if source:
        p = bronze_root / source / run_date
        return [source] if p.exists() else []
    if not bronze_root.exists():
        return []
    return [
        d.name
        for d in bronze_root.iterdir()
        if d.is_dir() and (d / run_date).exists()
    ]


def _fetch_existing_hashes() -> set[str]:
    """Pull all existing raw_hashes from Silver so we skip cross-run duplicates."""
    db = SessionLocal()
    try:
        rows = db.query(Job.raw_hash).all()
        return {r[0] for r in rows}
    finally:
        db.close()


def _empty_summary(engine: str, run_date: str) -> dict:
    summary = {
        "engine": engine, "stage": "bronze_to_silver", "run_date": run_date,
        "total_raw": 0, "duplicates_removed": 0,
        "total_inserted": 0, "total_skipped": 0, "total_errors": 0,
    }
    logger.warning("bronze_to_silver_no_input", extra=summary)
    return summary


def _make_hash_from_raw(raw: dict) -> str:
    parts = "|".join([
        str(raw.get("title",    "") or "").strip().lower(),
        str(raw.get("company",  "") or "").strip().lower(),
        str(raw.get("posted_at","") or ""),
        str(raw.get("source",   "") or ""),
    ])
    return hashlib.sha256(parts.encode()).hexdigest()


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        f = float(str(value).replace(",", "").replace("$", "").strip())
        return f if f > 0 else None
    except (ValueError, TypeError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(str(value)[:19], fmt)
        except ValueError:
            continue
    return None


def _infer_country(location: str) -> str | None:
    hints = {
        "US": ["usa", "united states", "new york", "san francisco", "seattle", "chicago"],
        "GB": ["uk", "united kingdom", "london", "manchester", "birmingham"],
        "CA": ["canada", "toronto", "vancouver", "montreal"],
        "AU": ["australia", "sydney", "melbourne"],
        "DE": ["germany", "berlin", "munich"],
        "IN": ["india", "bangalore", "hyderabad", "mumbai"],
        "FR": ["france", "paris"],
        "SG": ["singapore"],
        "NL": ["netherlands", "amsterdam"],
    }
    loc = location.lower()
    for code, keywords in hints.items():
        if any(k in loc for k in keywords):
            return code
    return None


def _infer_city(location: str) -> str | None:
    if not location:
        return None
    return [p.strip() for p in location.split(",")][0] or None


def _infer_remote(text: str) -> bool | None:
    t = text.lower()
    if any(w in t for w in ["remote", "work from home", "wfh", "fully remote", "home-based"]):
        return True
    if any(w in t for w in ["on-site", "onsite", "in-office", "office only", "must be in"]):
        return False
    return None


def _extract_skills(description: str) -> list[str]:
    desc = description.lower()
    return [s for s in SKILL_KEYWORDS if re.search(r"\b" + re.escape(s) + r"\b", desc)]


def _normalize_title(title: str, category_hint: str = "") -> str | None:
    t = (title + " " + category_hint).lower()
    mappings = [
        (["data engineer", "data pipeline", "etl", "data platform"],   "Data Engineer"),
        (["data scientist", "data science"],                             "Data Scientist"),
        (["data analyst", "business analyst", "bi analyst"],            "Data Analyst"),
        (["analytics engineer"],                                         "Analytics Engineer"),
        (["machine learning", "ml engineer", "mlops"],                  "ML Engineer"),
        (["ai engineer", "nlp engineer", "computer vision"],            "AI Engineer"),
        (["backend", "back-end", "software engineer", "software developer"], "Software Engineer"),
        (["frontend", "front-end", "react", "ui engineer"],             "Frontend Engineer"),
        (["full stack", "fullstack"],                                    "Full Stack Engineer"),
        (["devops", "platform engineer", "sre", "site reliability"],    "DevOps/Platform"),
        (["cloud engineer", "cloud architect"],                          "Cloud Engineer"),
        (["security engineer", "cybersecurity"],                         "Security Engineer"),
        (["database admin", "dba"],                                      "DBA"),
        (["product manager", "product owner"],                           "Product Manager"),
    ]
    for keywords, label in mappings:
        if any(kw in t for kw in keywords):
            return label
    return None
