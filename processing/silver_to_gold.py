"""
Silver → Gold aggregation — PostgreSQL only.

7d/30d moving averages and spike detection use the same window semantics as the
old PySpark path (``rowsBetween(-6,0)`` / ``(-29,0)`` on ordered date series),
implemented with ``AVG(...) OVER (ROWS BETWEEN … PRECEDING AND CURRENT ROW)``.
This avoids PySpark, JDBC, and Windows worker issues entirely.

Gold tables computed
────────────────────
  daily_role_demand    — job count per role per day + 7d / 30d moving avg
  daily_skill_demand   — job count per skill per day + 7d / 30d moving avg
  salary_summary       — median / p25 / p75 / p90 per role × country
  remote_vs_onsite     — remote ratio per role per day
  skill_cooccurrence   — co-occurrence count per skill pair (min 5)
  market_alerts        — spike detection: 7d_avg ≥ 2× 30d_avg
"""

import re
from datetime import date, datetime
from typing import Any

from monitoring.logger import get_logger
from storage.db import SessionLocal
from sqlalchemy import text

logger = get_logger(__name__)

# Window definitions match previous Spark: partition + order by calendar day,
# 7- and 30-row trailing averages (inclusive).
_SQL_ROLE_DEMAND = """
WITH base AS (
  SELECT DATE(posted_at) AS d,
         COALESCE(title_normalized, 'Other') AS role,
         COUNT(*)::integer AS job_count
  FROM jobs
  WHERE posted_at IS NOT NULL AND DATE(posted_at) <= CAST(:rd AS date)
  GROUP BY 1, 2
),
w AS (
  SELECT d,
         role,
         job_count,
         ROUND(AVG(job_count::numeric) OVER w7, 2)  AS moving_avg_7d,
         ROUND(AVG(job_count::numeric) OVER w30, 2) AS moving_avg_30d
  FROM base
  WINDOW
    w7  AS (PARTITION BY role ORDER BY d ROWS BETWEEN 6 PRECEDING  AND CURRENT ROW),
    w30 AS (PARTITION BY role ORDER BY d ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
)
SELECT d, role, job_count, moving_avg_7d, moving_avg_30d FROM w
"""
_SQL_SKILL_DEMAND = """
WITH base AS (
  SELECT DATE(j.posted_at) AS d,
         js.skill_name AS skill,
         COUNT(DISTINCT j.id)::integer AS job_count
  FROM jobs j
  JOIN job_skills js ON js.job_id = j.id
  WHERE j.posted_at IS NOT NULL AND DATE(j.posted_at) <= CAST(:rd AS date)
  GROUP BY 1, 2
),
w AS (
  SELECT d,
         skill,
         job_count,
         ROUND(AVG(job_count::numeric) OVER w7, 2)  AS moving_avg_7d,
         ROUND(AVG(job_count::numeric) OVER w30, 2) AS moving_avg_30d
  FROM base
  WINDOW
    w7  AS (PARTITION BY skill ORDER BY d ROWS BETWEEN 6 PRECEDING  AND CURRENT ROW),
    w30 AS (PARTITION BY skill ORDER BY d ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
)
SELECT d, skill, job_count, moving_avg_7d, moving_avg_30d FROM w
"""


# ── Public entry point ────────────────────────────────────────────────────────

def run(run_date: str | None = None) -> dict[str, Any]:
    run_date = run_date or str(date.today())
    return _run_sql(run_date)


def _run_sql(run_date: str) -> dict[str, Any]:
    run_date = _assert_iso_date(run_date)
    as_of = date.fromisoformat(run_date)
    results: dict[str, Any] = {}
    params = {"rd": run_date}

    role_rows = _fetch_sql(_SQL_ROLE_DEMAND, params)
    results["daily_role_demand"] = _upsert_role_demand(
        _demand_rows_to_payload(role_rows, "role")
    )
    skill_rows = _fetch_sql(_SQL_SKILL_DEMAND, params)
    results["daily_skill_demand"] = _upsert_skill_demand(
        _demand_rows_to_payload(skill_rows, "skill")
    )

    raw = _fetch_sql("""
        SELECT COALESCE(title_normalized, 'Other') AS role,
               COALESCE(country, 'UNKNOWN') AS country,
               COUNT(*) AS sample_size,
               ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (
                   ORDER BY (salary_min + salary_max) / 2.0)::numeric, 2) AS salary_median,
               ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (
                   ORDER BY (salary_min + salary_max) / 2.0)::numeric, 2) AS salary_p25,
               ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (
                   ORDER BY (salary_min + salary_max) / 2.0)::numeric, 2) AS salary_p75,
               ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (
                   ORDER BY (salary_min + salary_max) / 2.0)::numeric, 2) AS salary_p90
        FROM jobs
        WHERE salary_min IS NOT NULL AND salary_max IS NOT NULL
          AND salary_min > 0 AND salary_max >= salary_min
        GROUP BY 1, 2
        HAVING COUNT(*) >= 3
    """)
    results["salary_summary"] = _upsert_salary_summary([
        {"role": r[0], "country": r[1], "sample_size": int(r[2]),
         "salary_median": float(r[3]), "salary_p25": float(r[4]),
         "salary_p75": float(r[5]), "salary_p90": float(r[6])}
        for r in raw
    ])

    raw = _fetch_sql("""
        SELECT CAST(DATE(posted_at) AS TEXT) AS day,
               COALESCE(title_normalized, 'Other') AS role,
               SUM(CASE WHEN remote = TRUE  THEN 1 ELSE 0 END) AS remote_count,
               SUM(CASE WHEN remote = FALSE THEN 1 ELSE 0 END) AS onsite_count,
               SUM(CASE WHEN remote IS NULL THEN 1 ELSE 0 END) AS unknown_count
        FROM jobs
        WHERE posted_at IS NOT NULL AND DATE(posted_at) <= :d
        GROUP BY 1, 2
    """, {"d": run_date})
    results["remote_vs_onsite"] = _upsert_remote([
        {"date": r[0], "role": r[1],
         "remote_count": int(r[2]), "onsite_count": int(r[3]), "unknown_count": int(r[4]),
         "remote_ratio": round(int(r[2]) / (int(r[2]) + int(r[3])), 4)
                         if (int(r[2]) + int(r[3])) > 0 else None}
        for r in raw
    ])

    raw = _fetch_sql("""
        SELECT js1.skill_name AS skill_a, js2.skill_name AS skill_b, COUNT(*) AS co_count
        FROM job_skills js1
        JOIN job_skills js2 ON js1.job_id = js2.job_id AND js1.skill_name < js2.skill_name
        GROUP BY 1, 2
        HAVING COUNT(*) >= 5
        ORDER BY 3 DESC
        LIMIT 1000
    """)
    results["skill_cooccurrence"] = _upsert_cooccurrence([
        {"skill_a": r[0], "skill_b": r[1], "co_count": int(r[2])} for r in raw
    ])

    alert_payload = _collect_alerts_from_demand(
        role_rows, name_field="role", entity_type="role", as_of=as_of
    ) + _collect_alerts_from_demand(
        skill_rows, name_field="skill", entity_type="skill", as_of=as_of
    )
    results["market_alerts"] = _upsert_alerts(alert_payload, run_date)

    logger.info("silver_to_gold_sql_complete", extra={"run_date": run_date, **results})
    return results


def _as_date(v) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    raise TypeError(f"expected date-like, got {type(v).__name__}")


def _demand_rows_to_payload(
    rows, name_field: str
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows:
        m = r._mapping
        out.append({
            "date": _as_date(m["d"]),
            name_field: m[name_field],
            "job_count": int(m["job_count"]),
            "moving_avg_7d": float(m["moving_avg_7d"] or 0),
            "moving_avg_30d": float(m["moving_avg_30d"] or 0),
        })
    return out


def _collect_alerts_from_demand(
    rows, *, name_field: str, entity_type: str, as_of: date
) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    for r in rows:
        m = r._mapping
        d = _as_date(m["d"])
        if d != as_of:
            continue
        ma30 = float(m["moving_avg_30d"] or 0)
        if ma30 <= 0:
            continue
        ma7 = float(m["moving_avg_7d"] or 0)
        if ma7 < 2 * ma30:
            continue
        alerts.append({
            "entity_type": entity_type,
            "entity_name": str(m[name_field]),
            "moving_avg_7d": ma7,
            "moving_avg_30d": ma30,
            "spike_ratio": round(ma7 / ma30, 2),
        })
    return alerts


# ── SQL helper ────────────────────────────────────────────────────────────────

def _fetch_sql(query: str, params: dict | None = None):
    """Execute a read query and return all rows, closing the session immediately."""
    db = SessionLocal()
    try:
        return db.execute(text(query), params or {}).fetchall()
    finally:
        db.close()


def _assert_iso_date(s: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        raise ValueError(f"run_date must be YYYY-MM-DD, got {s!r}")
    return s


# ── DB upsert helpers ─────────────────────────────────────────────────────────

def _upsert_role_demand(rows) -> int:
    if not rows:
        return 0
    db = SessionLocal()
    try:
        stmt = text("""
            INSERT INTO daily_role_demand (date, role, job_count, moving_avg_7d, moving_avg_30d)
            VALUES (:date, :role, :job_count, :moving_avg_7d, :moving_avg_30d)
            ON CONFLICT (date, role) DO UPDATE SET
                job_count = EXCLUDED.job_count,
                moving_avg_7d = EXCLUDED.moving_avg_7d,
                moving_avg_30d = EXCLUDED.moving_avg_30d
        """)
        payload = [{
            "date": r["date"],
            "role": r["role"],
            "job_count": int(r["job_count"]),
            "moving_avg_7d": float(r["moving_avg_7d"] or 0),
            "moving_avg_30d": float(r["moving_avg_30d"] or 0),
        } for r in rows]
        db.execute(stmt, payload)
        db.commit()
        return len(payload)
    finally:
        db.close()


def _upsert_skill_demand(rows) -> int:
    if not rows:
        return 0
    db = SessionLocal()
    try:
        stmt = text("""
            INSERT INTO daily_skill_demand (date, skill, job_count, moving_avg_7d, moving_avg_30d)
            VALUES (:date, :skill, :job_count, :moving_avg_7d, :moving_avg_30d)
            ON CONFLICT (date, skill) DO UPDATE SET
                job_count = EXCLUDED.job_count,
                moving_avg_7d = EXCLUDED.moving_avg_7d,
                moving_avg_30d = EXCLUDED.moving_avg_30d
        """)
        payload = [{
            "date": r["date"],
            "skill": r["skill"],
            "job_count": int(r["job_count"]),
            "moving_avg_7d": float(r["moving_avg_7d"] or 0),
            "moving_avg_30d": float(r["moving_avg_30d"] or 0),
        } for r in rows]
        db.execute(stmt, payload)
        db.commit()
        return len(payload)
    finally:
        db.close()


def _upsert_salary_summary(rows) -> int:
    if not rows:
        return 0
    db = SessionLocal()
    try:
        stmt = text("""
            INSERT INTO salary_summary (
                role, country, sample_size, salary_median, salary_p25, salary_p75, salary_p90
            )
            VALUES (:role, :country, :sample_size, :salary_median, :salary_p25, :salary_p75, :salary_p90)
            ON CONFLICT (role, country) DO UPDATE SET
                sample_size = EXCLUDED.sample_size,
                salary_median = EXCLUDED.salary_median,
                salary_p25 = EXCLUDED.salary_p25,
                salary_p75 = EXCLUDED.salary_p75,
                salary_p90 = EXCLUDED.salary_p90
        """)
        payload = []
        for r in rows:
            if int(r["sample_size"]) < 3:
                continue
            payload.append({
                "role": r["role"],
                "country": r["country"],
                "sample_size": int(r["sample_size"]),
                "salary_median": float(r["salary_median"]),
                "salary_p25": float(r["salary_p25"]),
                "salary_p75": float(r["salary_p75"]),
                "salary_p90": float(r["salary_p90"]),
            })
        if payload:
            db.execute(stmt, payload)
        db.commit()
        return len(payload)
    finally:
        db.close()


def _upsert_remote(rows) -> int:
    if not rows:
        return 0
    db = SessionLocal()
    try:
        stmt = text("""
            INSERT INTO remote_vs_onsite (
                date, role, remote_count, onsite_count, unknown_count, remote_ratio
            )
            VALUES (:date, :role, :remote_count, :onsite_count, :unknown_count, :remote_ratio)
            ON CONFLICT (date, role) DO UPDATE SET
                remote_count = EXCLUDED.remote_count,
                onsite_count = EXCLUDED.onsite_count,
                unknown_count = EXCLUDED.unknown_count,
                remote_ratio = EXCLUDED.remote_ratio
        """)
        payload = [{
            "date": r["date"],
            "role": r["role"],
            "remote_count": int(r["remote_count"]),
            "onsite_count": int(r["onsite_count"]),
            "unknown_count": int(r["unknown_count"]),
            "remote_ratio": float(r["remote_ratio"]) if r["remote_ratio"] is not None else None,
        } for r in rows]
        db.execute(stmt, payload)
        db.commit()
        return len(payload)
    finally:
        db.close()


def _upsert_cooccurrence(rows) -> int:
    if not rows:
        return 0
    db = SessionLocal()
    try:
        stmt = text("""
            INSERT INTO skill_cooccurrence (skill_a, skill_b, co_count)
            VALUES (:skill_a, :skill_b, :co_count)
            ON CONFLICT (skill_a, skill_b) DO UPDATE SET
                co_count = EXCLUDED.co_count
        """)
        payload = [{
            "skill_a": r["skill_a"],
            "skill_b": r["skill_b"],
            "co_count": int(r["co_count"]),
        } for r in rows]
        db.execute(stmt, payload)
        db.commit()
        return len(payload)
    finally:
        db.close()


def _upsert_alerts(rows, run_date: str) -> int:
    if not rows:
        return 0
    db = SessionLocal()
    try:
        alert_date = date.fromisoformat(run_date)
        stmt = text("""
            INSERT INTO market_alerts (
                alert_date, entity_type, entity_name, demand_7d_avg, demand_30d_avg, spike_ratio
            )
            VALUES (:alert_date, :entity_type, :entity_name, :demand_7d_avg, :demand_30d_avg, :spike_ratio)
            ON CONFLICT (alert_date, entity_type, entity_name) DO UPDATE SET
                demand_7d_avg = EXCLUDED.demand_7d_avg,
                demand_30d_avg = EXCLUDED.demand_30d_avg,
                spike_ratio = EXCLUDED.spike_ratio
        """)
        payload = [{
            "alert_date": alert_date,
            "entity_type": r["entity_type"],
            "entity_name": r["entity_name"],
            "demand_7d_avg": float(r["moving_avg_7d"]),
            "demand_30d_avg": float(r["moving_avg_30d"]),
            "spike_ratio": float(r["spike_ratio"]),
        } for r in rows]
        db.execute(stmt, payload)
        db.commit()
        return len(payload)
    finally:
        db.close()


