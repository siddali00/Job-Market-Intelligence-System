"""
GET /api/overview/metrics — aggregated snapshot for the user dashboard
"""

from datetime import date, datetime, timedelta, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from storage.db import get_db

router = APIRouter()


@router.get("/metrics")
def get_overview_metrics(
    start_date: date | None = None,
    end_date: date | None = None,
    db: Session = Depends(get_db),
):
    """
    High-level counts and ranges. Optional ``start_date`` / ``end_date`` scope
    job counts to postings in that window; dimension counts are from full gold tables.
    """
    end = end_date or date.today()
    start = start_date or date(2020, 1, 1)
    if start > end:
        start, end = end, start

    core = text("""
        SELECT
            (SELECT COUNT(*)::bigint FROM jobs
             WHERE posted_at IS NOT NULL
               AND (posted_at::date) BETWEEN :start_d AND :end_d) AS jobs_in_window,
            (SELECT COUNT(*)::bigint FROM jobs) AS jobs_total,
            (SELECT MIN(date)::date FROM daily_role_demand) AS demand_min,
            (SELECT MAX(date)::date FROM daily_role_demand) AS demand_max,
            (SELECT COUNT(DISTINCT role)::bigint FROM daily_role_demand) AS distinct_roles,
            (SELECT COUNT(DISTINCT skill)::bigint FROM daily_skill_demand) AS distinct_skills,
            (SELECT COUNT(*)::bigint FROM salary_summary) AS salary_rows,
            (SELECT COUNT(DISTINCT country)::int FROM salary_summary) AS distinct_countries,
            (SELECT COUNT(*)::bigint FROM skill_cooccurrence) AS cooccurrence_rows
    """)
    row = db.execute(core, {"start_d": start, "end_d": end}).mappings().one()

    alert_start_7d = end - timedelta(days=6)
    sql_alerts = text("""
        SELECT
            (SELECT COUNT(*)::bigint FROM market_alerts WHERE alert_date = :end_d) AS alerts_today,
            (SELECT COUNT(*)::bigint FROM market_alerts
             WHERE alert_date BETWEEN :start_7d AND :end_d) AS alerts_7d,
            (SELECT MAX(alert_date) FROM market_alerts) AS last_alert_date
    """)
    a = db.execute(
        sql_alerts, {"end_d": end, "start_7d": alert_start_7d}
    ).mappings().one()

    sql_rem = text("""
        SELECT
            COALESCE(SUM(remote_count), 0)::bigint AS tr,
            COALESCE(SUM(onsite_count), 0)::bigint AS tos
        FROM remote_vs_onsite
    """)
    r = db.execute(sql_rem).one()
    tr, tos = int(r[0] or 0), int(r[1] or 0)
    total = tr + tos
    remote_pct = round(tr * 100.0 / total, 1) if total else None

    return {
        "window": {"start_date": str(start), "end_date": str(end)},
        "jobs_in_window": int(row["jobs_in_window"] or 0),
        "jobs_total": int(row["jobs_total"] or 0),
        "demand_date_range": {
            "min": str(row["demand_min"]) if row["demand_min"] else None,
            "max": str(row["demand_max"]) if row["demand_max"] else None,
        },
        "distinct_roles": int(row["distinct_roles"] or 0),
        "distinct_skills": int(row["distinct_skills"] or 0),
        "salary_summary_rows": int(row["salary_rows"] or 0),
        "distinct_countries_salary": int(row["distinct_countries"] or 0),
        "cooccurrence_rows": int(row["cooccurrence_rows"] or 0),
        "alerts_today": int(a["alerts_today"] or 0),
        "alerts_last_7d": int(a["alerts_7d"] or 0),
        "last_alert_date": str(a["last_alert_date"]) if a["last_alert_date"] else None,
        "remote": {
            "total_remote": tr,
            "total_onsite": tos,
            "remote_pct": remote_pct,
        },
        "data_freshness": datetime.now(timezone.utc).isoformat(),
    }
