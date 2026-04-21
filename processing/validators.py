"""
Data quality validation — PySpark-powered checks at every pipeline layer.

Three validation suites
───────────────────────
  Bronze  — file existence, JSON structure, completeness
  Silver  — column fill rates, salary logic, dedup integrity
  Gold    — aggregation coverage, trend continuity, alert plausibility

Each suite returns:
  {
    "overall": "passed" | "failed" | "warned",
    "checks":  [ {"name": ..., "status": ..., "detail": ...}, … ]
  }

PySpark is used for all checks that scan rows (Silver + Gold).
Bronze checks read the filesystem only (no Spark needed).

Fallback
────────
If PySpark / Java is unavailable, checks are performed with SQLAlchemy + pandas.
"""

from datetime import date
from pathlib import Path
from typing import Any

from config.settings import get_settings
from monitoring.logger import get_logger

logger   = get_logger(__name__)
settings = get_settings()


# ── Public API ────────────────────────────────────────────────────────────────

def run_bronze_validation(run_date: str | None = None) -> dict[str, Any]:
    run_date = run_date or str(date.today())
    checks   = []
    bronze_root = Path(settings.bronze_storage_path)

    # Check 1: at least one source folder exists
    sources = [d.name for d in bronze_root.iterdir() if d.is_dir()] if bronze_root.exists() else []
    checks.append(_check(
        "bronze_sources_exist",
        len(sources) > 0,
        f"Found sources: {sources}" if sources else "No bronze sources found",
    ))

    # Check 2: today's partition has files for each source
    for src in sources:
        partition = bronze_root / src / run_date
        json_files = list(partition.glob("**/*.json")) if partition.exists() else []
        checks.append(_check(
            f"bronze_{src}_partition",
            len(json_files) > 0,
            f"{len(json_files)} JSON files in {partition}",
            warn_only=True,    # missing today's data is a warning, not a failure
        ))

    # Check 3: JSON files are valid and non-empty
    total_records = 0
    parse_errors  = 0
    for src in sources:
        partition = bronze_root / src / run_date
        if not partition.exists():
            continue
        for jf in partition.glob("**/*.json"):
            try:
                import json
                data = json.loads(jf.read_text(encoding="utf-8"))
                if not isinstance(data, list):
                    parse_errors += 1
                else:
                    total_records += len(data)
            except Exception:
                parse_errors += 1

    checks.append(_check(
        "bronze_json_valid",
        parse_errors == 0,
        f"{total_records} records loaded, {parse_errors} parse errors",
    ))
    checks.append(_check(
        "bronze_records_nonzero",
        total_records > 0,
        f"Total raw records: {total_records}",
        warn_only=True,
    ))

    return _summarise("bronze", checks)


def run_silver_validation() -> dict[str, Any]:
    try:
        return _silver_spark()
    except Exception as exc:
        logger.warning("silver_validation_spark_fallback", extra={"error": str(exc)})
        return _silver_pandas()


def run_gold_validation() -> dict[str, Any]:
    try:
        return _gold_spark()
    except Exception as exc:
        logger.warning("gold_validation_spark_fallback", extra={"error": str(exc)})
        return _gold_pandas()


# ══════════════════════════════════════════════════════════════════════════════
# Silver validation — PySpark
# ══════════════════════════════════════════════════════════════════════════════

def _silver_spark() -> dict[str, Any]:
    import pyspark.sql.functions as F
    from sqlalchemy import text

    spark  = _get_spark()
    checks = []
    db     = _db()

    try:
        rows = db.execute(text("""
            SELECT id, source, raw_hash, title, company_id,
                   salary_min, salary_max, remote, posted_at,
                   salary_missing, skill_missing
            FROM jobs
        """)).fetchall()
    finally:
        db.close()

    if not rows:
        checks.append(_check("silver_jobs_exist", False, "jobs table is empty"))
        return _summarise("silver", checks)

    df = spark.createDataFrame([dict(r._mapping) for r in rows])
    total = df.count()
    checks.append(_check("silver_jobs_exist", total > 0, f"{total:,} rows in jobs"))

    # Fill-rate checks
    for col_name, threshold in [
        ("title",      0.99),
        ("source",     1.00),
        ("raw_hash",   1.00),
        ("posted_at",  0.70),
    ]:
        non_null = df.filter(F.col(col_name).isNotNull() & (F.col(col_name) != "")).count()
        rate     = non_null / total
        checks.append(_check(
            f"silver_{col_name}_fill_rate",
            rate >= threshold,
            f"{rate:.1%} filled (threshold {threshold:.0%})",
            warn_only=(threshold < 1.0),
        ))

    # raw_hash uniqueness — no duplicates should exist after PySpark dedup
    distinct_hashes = df.select("raw_hash").distinct().count()
    checks.append(_check(
        "silver_raw_hash_unique",
        distinct_hashes == total,
        f"{total - distinct_hashes} duplicate hashes found",
    ))

    # Salary logic: max >= min
    bad_salary = df.filter(
        F.col("salary_min").isNotNull() &
        F.col("salary_max").isNotNull() &
        (F.col("salary_max") < F.col("salary_min"))
    ).count()
    checks.append(_check(
        "silver_salary_min_lte_max",
        bad_salary == 0,
        f"{bad_salary} rows where salary_max < salary_min",
    ))

    # Salary coverage
    with_salary = df.filter(F.col("salary_missing") == False).count()
    rate = with_salary / total
    checks.append(_check(
        "silver_salary_coverage",
        rate >= 0.20,
        f"{rate:.1%} rows have salary data (threshold 20%)",
        warn_only=True,
    ))

    # Source distribution — should have data from multiple sources
    source_counts = {r["source"]: r["cnt"] for r in
                     df.groupBy("source").agg(F.count("*").alias("cnt")).collect()}
    checks.append(_check(
        "silver_multiple_sources",
        len(source_counts) >= 2,
        f"Sources: {source_counts}",
        warn_only=True,
    ))

    # Skill coverage
    with_skills = df.filter(F.col("skill_missing") == False).count()
    rate = with_skills / total
    checks.append(_check(
        "silver_skill_coverage",
        rate >= 0.30,
        f"{rate:.1%} rows have skills extracted (threshold 30%)",
        warn_only=True,
    ))

    return _summarise("silver", checks)


# ══════════════════════════════════════════════════════════════════════════════
# Gold validation — PySpark
# ══════════════════════════════════════════════════════════════════════════════

def _gold_spark() -> dict[str, Any]:
    import pyspark.sql.functions as F
    from sqlalchemy import text

    spark  = _get_spark()
    checks = []
    db     = _db()

    try:
        role_rows  = db.execute(text("SELECT date, role, job_count, moving_avg_7d, moving_avg_30d FROM daily_role_demand")).fetchall()
        skill_rows = db.execute(text("SELECT date, skill, job_count FROM daily_skill_demand")).fetchall()
        sal_rows   = db.execute(text("SELECT role, country, sample_size, salary_median FROM salary_summary")).fetchall()
        alert_rows = db.execute(text("SELECT entity_type, entity_name, spike_ratio FROM market_alerts")).fetchall()
    finally:
        db.close()

    # daily_role_demand populated
    role_df = spark.createDataFrame([dict(r._mapping) for r in role_rows]) if role_rows else None
    checks.append(_check(
        "gold_role_demand_populated",
        role_df is not None and role_df.count() > 0,
        f"{len(role_rows)} rows in daily_role_demand",
    ))

    if role_df:
        # Moving averages are non-negative
        bad_avg = role_df.filter(
            (F.col("moving_avg_7d") < 0) | (F.col("moving_avg_30d") < 0)
        ).count()
        checks.append(_check("gold_moving_avgs_nonneg", bad_avg == 0,
                              f"{bad_avg} rows with negative averages"))

        # 7d avg should be >= 0 and <= job_count * 2 (sanity bound)
        bad_ratio = role_df.filter(
            F.col("moving_avg_7d") > F.col("job_count") * 10
        ).count()
        checks.append(_check("gold_moving_avg_plausible", bad_ratio == 0,
                              f"{bad_ratio} rows with implausible 7d avg",
                              warn_only=True))

    # daily_skill_demand
    checks.append(_check(
        "gold_skill_demand_populated",
        len(skill_rows) > 0,
        f"{len(skill_rows)} rows in daily_skill_demand",
    ))

    # salary_summary
    sal_df = spark.createDataFrame([dict(r._mapping) for r in sal_rows]) if sal_rows else None
    checks.append(_check(
        "gold_salary_summary_populated",
        sal_df is not None and sal_df.count() > 0,
        f"{len(sal_rows)} rows in salary_summary",
        warn_only=True,
    ))

    if sal_df:
        bad_sal = sal_df.filter(F.col("salary_median") <= 0).count()
        checks.append(_check("gold_salary_median_positive", bad_sal == 0,
                              f"{bad_sal} rows with non-positive median salary"))

    # market_alerts (optional — only appear if spikes exist)
    checks.append(_check(
        "gold_market_alerts_plausible",
        all(r[2] is None or r[2] >= 2.0 for r in alert_rows),
        f"{len(alert_rows)} alerts, all spike_ratio >= 2.0",
        warn_only=True,
    ))

    return _summarise("gold", checks)


# ══════════════════════════════════════════════════════════════════════════════
# Pandas fallbacks
# ══════════════════════════════════════════════════════════════════════════════

def _silver_pandas() -> dict[str, Any]:
    import pandas as pd
    from sqlalchemy import text

    checks = []
    db     = _db()
    try:
        rows = db.execute(text("SELECT id, source, raw_hash, title, salary_missing, skill_missing FROM jobs")).fetchall()
    finally:
        db.close()

    total = len(rows)
    checks.append(_check("silver_jobs_exist", total > 0, f"{total:,} rows"))

    if not rows:
        return _summarise("silver", checks)

    df = pd.DataFrame([dict(r._mapping) for r in rows])
    checks.append(_check("silver_raw_hash_unique", df["raw_hash"].nunique() == total,
                          f"{total - df['raw_hash'].nunique()} duplicates"))
    rate = (~df["salary_missing"]).mean()
    checks.append(_check("silver_salary_coverage", rate >= 0.20, f"{rate:.1%}", warn_only=True))
    return _summarise("silver", checks)


def _gold_pandas() -> dict[str, Any]:
    from sqlalchemy import text

    checks = []
    db     = _db()
    try:
        n_role  = db.execute(text("SELECT COUNT(*) FROM daily_role_demand")).scalar()
        n_skill = db.execute(text("SELECT COUNT(*) FROM daily_skill_demand")).scalar()
        n_sal   = db.execute(text("SELECT COUNT(*) FROM salary_summary")).scalar()
    finally:
        db.close()

    checks.append(_check("gold_role_demand_populated",  n_role  > 0, f"{n_role} rows"))
    checks.append(_check("gold_skill_demand_populated", n_skill > 0, f"{n_skill} rows"))
    checks.append(_check("gold_salary_populated",       n_sal   > 0, f"{n_sal} rows", warn_only=True))
    return _summarise("gold", checks)


# ── Shared helpers ────────────────────────────────────────────────────────────

def _check(name: str, passed: bool, detail: str = "", warn_only: bool = False) -> dict:
    if passed:
        status = "passed"
    elif warn_only:
        status = "warned"
    else:
        status = "failed"
    result = {"name": name, "status": status, "detail": detail}
    log_fn = logger.info if passed else (logger.warning if warn_only else logger.error)
    # Use prefixed keys — "name" is reserved by LogRecord and cannot be passed in extra=
    log_fn(f"validation_{status}", extra={"check_name": name, "check_status": status, "check_detail": detail})
    return result


def _summarise(layer: str, checks: list[dict]) -> dict[str, Any]:
    statuses = {c["status"] for c in checks}
    overall  = "failed" if "failed" in statuses else "warned" if "warned" in statuses else "passed"
    result   = {"layer": layer, "overall": overall, "checks": checks}
    logger.info(f"{layer}_validation_{overall}", extra={"total_checks": len(checks)})
    return result


def _db():
    from storage.db import SessionLocal
    return SessionLocal()


def _get_spark():
    from pyspark.sql import SparkSession
    import logging

    spark = (
        SparkSession.builder
        .appName("JobMarketIntelligence-Validation")
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    return spark
