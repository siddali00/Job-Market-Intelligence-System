"""
Silver → Gold aggregation — PySpark-powered.

Data flow
─────────
  PostgreSQL Silver  →  SQLAlchemy fetch  →  Spark DataFrame
  →  PySpark transforms  →  collect()  →  SQLAlchemy upsert  →  PostgreSQL Gold

Why PySpark here?
─────────────────
  • Window functions (rowsBetween) give rolling averages in a single pass.
  • percentile_approx computes salary percentiles per-group without looping.
  • Skill co-occurrence is a Spark self-join — far faster than nested Python loops.
  • Market alert detection is a declarative filter on the already-computed windows.

Fallback
────────
  If PySpark / Java is unavailable the module falls back to plain SQLAlchemy +
  pandas so the pipeline still runs.  A WARNING is logged.

Gold tables computed
────────────────────
  daily_role_demand    — job count per role per day + 7d / 30d moving avg
  daily_skill_demand   — job count per skill per day + 7d / 30d moving avg
  salary_summary       — median / p25 / p75 / p90 per role × country
  remote_vs_onsite     — remote ratio per role per day
  skill_cooccurrence   — co-occurrence count per skill pair (min 5)
  market_alerts        — spike detection: 7d_avg ≥ 2× 30d_avg
"""

from datetime import date
from typing import Any

from monitoring.logger import get_logger
from sqlalchemy.dialects.postgresql import insert as pg_insert
from storage.db import SessionLocal
from storage.models import (
    DailyRoleDemand, DailySkillDemand, MarketAlert,
    RemoteVsOnsite, SalarySummary, SkillCooccurrence,
)

logger = get_logger(__name__)


# ── Public entry point ────────────────────────────────────────────────────────

def run(run_date: str | None = None) -> dict[str, Any]:
    run_date = run_date or str(date.today())
    try:
        return _run_spark(run_date)
    except Exception as exc:
        logger.warning("s2g_spark_fallback", extra={"error": str(exc)})
        return _run_pandas(run_date)


# ══════════════════════════════════════════════════════════════════════════════
# PySpark path
# ══════════════════════════════════════════════════════════════════════════════

def _run_spark(run_date: str) -> dict[str, Any]:
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window

    spark = _get_spark()

    # ── Load Silver tables from PostgreSQL ────────────────────────────────────
    jobs_df, job_skills_df = _load_silver(spark, F)

    results = {}

    # ── 1. Daily role demand + rolling averages ───────────────────────────────
    role_demand = (
        jobs_df
        .filter(F.col("posted_at").isNotNull())
        .filter(F.col("posted_at") <= run_date)
        .groupBy(
            F.to_date("posted_at").alias("date"),
            F.coalesce(F.col("title_normalized"), F.lit("Other")).alias("role"),
        )
        .agg(F.count("*").alias("job_count"))
    )

    w7  = Window.partitionBy("role").orderBy("date").rowsBetween(-6, 0)
    w30 = Window.partitionBy("role").orderBy("date").rowsBetween(-29, 0)

    role_demand = (
        role_demand
        .withColumn("moving_avg_7d",  F.round(F.avg("job_count").over(w7),  2))
        .withColumn("moving_avg_30d", F.round(F.avg("job_count").over(w30), 2))
    )

    results["daily_role_demand"] = _upsert_role_demand(role_demand.collect())

    # ── 2. Daily skill demand + rolling averages ──────────────────────────────
    skill_demand = (
        jobs_df.filter(F.col("posted_at").isNotNull())
        .join(job_skills_df, on="job_id")
        .filter(F.to_date("posted_at") <= run_date)
        .groupBy(
            F.to_date("posted_at").alias("date"),
            F.col("skill_name").alias("skill"),
        )
        .agg(F.countDistinct("job_id").alias("job_count"))
    )

    ws7  = Window.partitionBy("skill").orderBy("date").rowsBetween(-6, 0)
    ws30 = Window.partitionBy("skill").orderBy("date").rowsBetween(-29, 0)

    skill_demand = (
        skill_demand
        .withColumn("moving_avg_7d",  F.round(F.avg("job_count").over(ws7),  2))
        .withColumn("moving_avg_30d", F.round(F.avg("job_count").over(ws30), 2))
    )

    results["daily_skill_demand"] = _upsert_skill_demand(skill_demand.collect())

    # ── 3. Salary summary — percentiles per role × country ────────────────────
    salary_df = (
        jobs_df
        .filter(F.col("salary_min").isNotNull() & F.col("salary_max").isNotNull())
        .filter(F.col("salary_min") > 0)
        .filter(F.col("salary_max") >= F.col("salary_min"))
        .withColumn("mid_salary", (F.col("salary_min") + F.col("salary_max")) / 2.0)
        .withColumn("role",    F.coalesce(F.col("title_normalized"), F.lit("Other")))
        .withColumn("country", F.coalesce(F.col("country"),          F.lit("UNKNOWN")))
    )

    salary_summary = salary_df.groupBy("role", "country").agg(
        F.count("mid_salary").alias("sample_size"),
        F.round(F.percentile_approx("mid_salary", 0.50), 2).alias("salary_median"),
        F.round(F.percentile_approx("mid_salary", 0.25), 2).alias("salary_p25"),
        F.round(F.percentile_approx("mid_salary", 0.75), 2).alias("salary_p75"),
        F.round(F.percentile_approx("mid_salary", 0.90), 2).alias("salary_p90"),
    )

    results["salary_summary"] = _upsert_salary_summary(salary_summary.collect())

    # ── 4. Remote vs on-site per role per day ─────────────────────────────────
    remote_df = (
        jobs_df
        .filter(F.col("posted_at").isNotNull())
        .filter(F.to_date("posted_at") <= run_date)
        .withColumn("role", F.coalesce(F.col("title_normalized"), F.lit("Other")))
        .groupBy(F.to_date("posted_at").alias("date"), "role")
        .agg(
            F.sum(F.when(F.col("remote") == True,  1).otherwise(0)).alias("remote_count"),
            F.sum(F.when(F.col("remote") == False, 1).otherwise(0)).alias("onsite_count"),
            F.sum(F.when(F.col("remote").isNull(),  1).otherwise(0)).alias("unknown_count"),
        )
        .withColumn("total", F.col("remote_count") + F.col("onsite_count"))
        .withColumn("remote_ratio",
            F.when(F.col("total") > 0,
                   F.round(F.col("remote_count") / F.col("total"), 4))
            .otherwise(F.lit(None))
        )
    )

    results["remote_vs_onsite"] = _upsert_remote(remote_df.collect())

    # ── 5. Skill co-occurrence (self-join) ────────────────────────────────────
    # Alias job_skills twice — Spark self-join on job_id,
    # canonical ordering by skill name (alphabetical) to avoid (a,b)+(b,a) dupes.
    js_a = job_skills_df.select(
        F.col("job_id"),
        F.col("skill_name").alias("skill_a"),
    )
    js_b = job_skills_df.select(
        F.col("job_id"),
        F.col("skill_name").alias("skill_b"),
    )

    cooccurrence = (
        js_a.join(js_b, on="job_id")
        .filter(F.col("skill_a") < F.col("skill_b"))      # canonical pair ordering
        .groupBy("skill_a", "skill_b")
        .agg(F.count("*").alias("co_count"))
        .filter(F.col("co_count") >= 5)
        .orderBy(F.col("co_count").desc())
        .limit(1000)
    )

    results["skill_cooccurrence"] = _upsert_cooccurrence(cooccurrence.collect())

    # ── 6. Market alerts — demand spike: 7d_avg ≥ 2× 30d_avg ─────────────────
    def _alerts_from(demand_df, entity_col: str, entity_type: str):
        return (
            demand_df
            .filter(F.col("date") == run_date)
            .filter(F.col("moving_avg_30d") > 0)
            .filter(F.col("moving_avg_7d") >= F.col("moving_avg_30d") * 2)
            .withColumn("spike_ratio", F.round(F.col("moving_avg_7d") / F.col("moving_avg_30d"), 2))
            .withColumn("entity_type", F.lit(entity_type))
            .withColumnRenamed(entity_col, "entity_name")
            .select("entity_type", "entity_name", "moving_avg_7d", "moving_avg_30d", "spike_ratio")
        )

    role_alerts  = _alerts_from(role_demand,  "role",  "role")
    skill_alerts = _alerts_from(skill_demand, "skill", "skill")

    from functools import reduce
    all_alerts = reduce(lambda a, b: a.union(b), [role_alerts, skill_alerts])
    results["market_alerts"] = _upsert_alerts(all_alerts.collect(), run_date)

    logger.info("silver_to_gold_spark_complete", extra={"run_date": run_date, **results})
    return results


# ── Data loading helpers ──────────────────────────────────────────────────────

def _load_silver(spark, F):
    """
    Pull Silver tables from PostgreSQL via SQLAlchemy and create Spark DataFrames.
    (Avoids the need to configure a JDBC driver jar.)
    """
    from sqlalchemy import text

    db = SessionLocal()
    try:
        jobs_rows = db.execute(text("""
            SELECT
                j.id           AS job_id,
                j.title_normalized,
                j.country,
                j.remote,
                j.salary_min,
                j.salary_max,
                CAST(j.posted_at AS TEXT) AS posted_at
            FROM jobs j
        """)).fetchall()

        skill_rows = db.execute(text("""
            SELECT job_id, skill_name
            FROM job_skills
        """)).fetchall()
    finally:
        db.close()

    jobs_df = spark.createDataFrame(
        [dict(r._mapping) for r in jobs_rows],
    ) if jobs_rows else spark.createDataFrame([], schema="job_id INT, title_normalized STRING, country STRING, remote BOOLEAN, salary_min DOUBLE, salary_max DOUBLE, posted_at STRING")

    skill_df = spark.createDataFrame(
        [dict(r._mapping) for r in skill_rows],
    ) if skill_rows else spark.createDataFrame([], schema="job_id INT, skill_name STRING")

    return jobs_df, skill_df


def _get_spark():
    import logging
    import os
    import subprocess
    import sys
    from pathlib import Path as _Path
    from dotenv import load_dotenv
    from pyspark.sql import SparkSession

    load_dotenv()

    java_home   = os.environ.get("JAVA_HOME", "")
    hadoop_home = os.environ.get("HADOOP_HOME", "C:\\hadoop")
    if java_home:
        os.environ["JAVA_HOME"]   = java_home
        os.environ["HADOOP_HOME"] = hadoop_home
        for bin_dir in (_Path(java_home) / "bin", _Path(hadoop_home) / "bin"):
            s = str(bin_dir)
            if s not in os.environ.get("PATH", ""):
                os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + s

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
        .appName("JobMarketIntelligence-SilverToGold")
        .master("local[*]")
        .config("spark.driver.memory",           "4g")
        .config("spark.sql.shuffle.partitions",  "8")
        .config("spark.ui.showConsoleProgress",  "false")
        .config("spark.pyspark.python",           py_exe)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    return spark


# ── DB upsert helpers ─────────────────────────────────────────────────────────

def _upsert_role_demand(rows) -> int:
    if not rows:
        return 0
    records = [
        {
            "date": r["date"],
            "role": r["role"],
            "job_count": int(r["job_count"]),
            "moving_avg_7d": float(r["moving_avg_7d"] or 0),
            "moving_avg_30d": float(r["moving_avg_30d"] or 0),
        }
        for r in rows
    ]
    db = SessionLocal()
    try:
        stmt = pg_insert(DailyRoleDemand).values(records)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_daily_role_demand",
            set_={
                "job_count":      stmt.excluded.job_count,
                "moving_avg_7d":  stmt.excluded.moving_avg_7d,
                "moving_avg_30d": stmt.excluded.moving_avg_30d,
            },
        )
        db.execute(stmt)
        db.commit()
        return len(records)
    finally:
        db.close()


def _upsert_skill_demand(rows) -> int:
    if not rows:
        return 0
    records = [
        {
            "date": r["date"],
            "skill": r["skill"],
            "job_count": int(r["job_count"]),
            "moving_avg_7d": float(r["moving_avg_7d"] or 0),
            "moving_avg_30d": float(r["moving_avg_30d"] or 0),
        }
        for r in rows
    ]
    db = SessionLocal()
    try:
        stmt = pg_insert(DailySkillDemand).values(records)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_daily_skill_demand",
            set_={
                "job_count":      stmt.excluded.job_count,
                "moving_avg_7d":  stmt.excluded.moving_avg_7d,
                "moving_avg_30d": stmt.excluded.moving_avg_30d,
            },
        )
        db.execute(stmt)
        db.commit()
        return len(records)
    finally:
        db.close()


def _upsert_salary_summary(rows) -> int:
    records = [
        {
            "role": r["role"],
            "country": r["country"],
            "sample_size": int(r["sample_size"]),
            "salary_median": float(r["salary_median"]),
            "salary_p25": float(r["salary_p25"]),
            "salary_p75": float(r["salary_p75"]),
            "salary_p90": float(r["salary_p90"]),
        }
        for r in rows
        if int(r["sample_size"]) >= 3
    ]
    if not records:
        return 0
    db = SessionLocal()
    try:
        stmt = pg_insert(SalarySummary).values(records)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_salary_summary",
            set_={
                "sample_size":   stmt.excluded.sample_size,
                "salary_median": stmt.excluded.salary_median,
                "salary_p25":    stmt.excluded.salary_p25,
                "salary_p75":    stmt.excluded.salary_p75,
                "salary_p90":    stmt.excluded.salary_p90,
            },
        )
        db.execute(stmt)
        db.commit()
        return len(records)
    finally:
        db.close()


def _upsert_remote(rows) -> int:
    if not rows:
        return 0
    records = [
        {
            "date": r["date"],
            "role": r["role"],
            "remote_count": int(r["remote_count"]),
            "onsite_count": int(r["onsite_count"]),
            "unknown_count": int(r["unknown_count"]),
            "remote_ratio": float(r["remote_ratio"]) if r["remote_ratio"] is not None else None,
        }
        for r in rows
    ]
    db = SessionLocal()
    try:
        stmt = pg_insert(RemoteVsOnsite).values(records)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_remote_vs_onsite",
            set_={
                "remote_count":  stmt.excluded.remote_count,
                "onsite_count":  stmt.excluded.onsite_count,
                "unknown_count": stmt.excluded.unknown_count,
                "remote_ratio":  stmt.excluded.remote_ratio,
            },
        )
        db.execute(stmt)
        db.commit()
        return len(records)
    finally:
        db.close()


def _upsert_cooccurrence(rows) -> int:
    if not rows:
        return 0
    records = [
        {
            "skill_a": r["skill_a"],
            "skill_b": r["skill_b"],
            "co_count": int(r["co_count"]),
        }
        for r in rows
    ]
    db = SessionLocal()
    try:
        stmt = pg_insert(SkillCooccurrence).values(records)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_skill_cooccurrence",
            set_={"co_count": stmt.excluded.co_count},
        )
        db.execute(stmt)
        db.commit()
        return len(records)
    finally:
        db.close()


def _upsert_alerts(rows, run_date: str) -> int:
    if not rows:
        return 0
    alert_date = date.fromisoformat(run_date)
    records = [
        {
            "alert_date":    alert_date,
            "entity_type":   r["entity_type"],
            "entity_name":   r["entity_name"],
            "demand_7d_avg": float(r["moving_avg_7d"]),
            "demand_30d_avg": float(r["moving_avg_30d"]),
            "spike_ratio":   float(r["spike_ratio"]),
        }
        for r in rows
    ]
    db = SessionLocal()
    try:
        stmt = pg_insert(MarketAlert).values(records)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_market_alert",
            set_={
                "demand_7d_avg":  stmt.excluded.demand_7d_avg,
                "demand_30d_avg": stmt.excluded.demand_30d_avg,
                "spike_ratio":    stmt.excluded.spike_ratio,
            },
        )
        db.execute(stmt)
        db.commit()
        return len(records)
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# Pandas fallback  (same logic, no Spark)
# ══════════════════════════════════════════════════════════════════════════════

def _run_pandas(run_date: str) -> dict[str, Any]:
    """
    Pure SQLAlchemy + pandas fallback — identical aggregations expressed
    via SQL + pandas rolling windows.  Used when PySpark / Java is not available.
    """
    import pandas as pd
    from sqlalchemy import text

    db = SessionLocal()
    results = {}

    try:
        # Daily role demand
        rows = db.execute(text("""
            SELECT DATE(posted_at) AS date,
                   title_normalized AS role,
                   COUNT(*) AS job_count
            FROM jobs WHERE posted_at IS NOT NULL AND title_normalized IS NOT NULL AND DATE(posted_at) <= :d
            GROUP BY 1,2
        """), {"d": run_date}).fetchall()
        df = pd.DataFrame(rows, columns=["date","role","job_count"]).sort_values(["role","date"])
        df["moving_avg_7d"]  = df.groupby("role")["job_count"].transform(lambda x: x.rolling(7,  min_periods=1).mean().round(2))
        df["moving_avg_30d"] = df.groupby("role")["job_count"].transform(lambda x: x.rolling(30, min_periods=1).mean().round(2))
        results["daily_role_demand"] = _upsert_role_demand(df.to_dict("records"))

        # Daily skill demand
        rows = db.execute(text("""
            SELECT DATE(j.posted_at) AS date, js.skill_name AS skill, COUNT(DISTINCT j.id) AS job_count
            FROM jobs j JOIN job_skills js ON js.job_id=j.id
            WHERE j.posted_at IS NOT NULL AND DATE(j.posted_at) <= :d GROUP BY 1,2
        """), {"d": run_date}).fetchall()
        df = pd.DataFrame(rows, columns=["date","skill","job_count"]).sort_values(["skill","date"])
        df["moving_avg_7d"]  = df.groupby("skill")["job_count"].transform(lambda x: x.rolling(7,  min_periods=1).mean().round(2))
        df["moving_avg_30d"] = df.groupby("skill")["job_count"].transform(lambda x: x.rolling(30, min_periods=1).mean().round(2))
        results["daily_skill_demand"] = _upsert_skill_demand(df.to_dict("records"))

        # Salary summary
        rows = db.execute(text("""
            SELECT title_normalized AS role,
                   COALESCE(country,'UNKNOWN') AS country,
                   (salary_min+salary_max)/2.0 AS mid_salary
            FROM jobs WHERE salary_min IS NOT NULL AND salary_max IS NOT NULL AND salary_min > 0
              AND title_normalized IS NOT NULL
        """)).fetchall()
        df = pd.DataFrame(rows, columns=["role","country","mid_salary"])
        grouped = df.groupby(["role","country"])["mid_salary"].agg(
            sample_size="count",
            salary_median="median",
            salary_p25=lambda x: x.quantile(0.25),
            salary_p75=lambda x: x.quantile(0.75),
            salary_p90=lambda x: x.quantile(0.90),
        ).reset_index().round(2)
        results["salary_summary"] = _upsert_salary_summary(grouped.to_dict("records"))

        # Remote vs onsite
        rows = db.execute(text("""
            SELECT DATE(posted_at) AS date, title_normalized AS role,
                   SUM(CASE WHEN remote=TRUE THEN 1 ELSE 0 END) AS remote_count,
                   SUM(CASE WHEN remote=FALSE THEN 1 ELSE 0 END) AS onsite_count,
                   SUM(CASE WHEN remote IS NULL THEN 1 ELSE 0 END) AS unknown_count
            FROM jobs WHERE posted_at IS NOT NULL AND title_normalized IS NOT NULL AND DATE(posted_at)<=:d GROUP BY 1,2
        """), {"d": run_date}).fetchall()
        df = pd.DataFrame(rows, columns=["date","role","remote_count","onsite_count","unknown_count"])
        df["total"] = df["remote_count"] + df["onsite_count"]
        df["remote_ratio"] = (df["remote_count"] / df["total"].replace(0, None)).round(4)
        results["remote_vs_onsite"] = _upsert_remote(df.to_dict("records"))

        # Skill cooccurrence — alphabetical ordering avoids (a,b)+(b,a) duplicates
        rows = db.execute(text("""
            SELECT js1.skill_name AS skill_a, js2.skill_name AS skill_b, COUNT(*) AS co_count
            FROM job_skills js1
            JOIN job_skills js2 ON js1.job_id=js2.job_id AND js1.skill_name < js2.skill_name
            GROUP BY 1,2 HAVING COUNT(*)>=5 ORDER BY 3 DESC LIMIT 1000
        """)).fetchall()
        results["skill_cooccurrence"] = _upsert_cooccurrence([dict(r._mapping) for r in rows])

        # Market alerts
        for entity_type, table, col in [("role","daily_role_demand","role"),("skill","daily_skill_demand","skill")]:
            rows = db.execute(text(f"""
                SELECT '{entity_type}' AS entity_type, {col} AS entity_name,
                       moving_avg_7d, moving_avg_30d,
                       ROUND((moving_avg_7d / moving_avg_30d)::numeric, 2) AS spike_ratio
                FROM {table}
                WHERE date=:d AND moving_avg_30d>0 AND moving_avg_7d >= moving_avg_30d*2
            """), {"d": run_date}).fetchall()
            results.setdefault("market_alerts", 0)
            results["market_alerts"] += _upsert_alerts([dict(r._mapping) for r in rows], run_date)

        db.commit()
        logger.info("silver_to_gold_pandas_complete", extra={"run_date": run_date, **results})
        return results
    except Exception as exc:
        db.rollback()
        logger.error("silver_to_gold_failed", extra={"error": str(exc)})
        raise
    finally:
        db.close()
