"""
GET /api/roles/demand  — role demand over time with moving averages
GET /api/roles/top     — most in-demand roles for a given date range
"""

from datetime import date, datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from storage.db import get_db

router = APIRouter()


@router.get("/demand")
def get_role_demand(
    role: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    db: Session = Depends(get_db),
):
    """Time-series of daily job counts per role, with 7d and 30d moving averages."""
    start_date = start_date or date(2024, 1, 1)
    end_date = end_date or date.today()

    filters = ["date BETWEEN :start_date AND :end_date"]
    params: dict = {"start_date": start_date, "end_date": end_date}

    if role:
        filters.append("role ILIKE :role")
        params["role"] = f"%{role}%"

    sql = text(f"""
        SELECT date, role, job_count, moving_avg_7d, moving_avg_30d
        FROM daily_role_demand
        WHERE {' AND '.join(filters)}
        ORDER BY role, date
    """)
    rows = db.execute(sql, params).fetchall()

    return {
        "data": [
            {
                "date": str(r[0]),
                "role": r[1],
                "job_count": r[2],
                "moving_avg_7d": r[3],
                "moving_avg_30d": r[4],
            }
            for r in rows
        ],
        "data_freshness": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/top")
def get_top_roles(
    top_n: Annotated[int, Query(ge=1, le=50)] = 10,
    start_date: date | None = None,
    end_date: date | None = None,
    db: Session = Depends(get_db),
):
    """Most in-demand roles ranked by total job count over a date range."""
    start_date = start_date or date(2024, 1, 1)
    end_date = end_date or date.today()

    sql = text("""
        SELECT role, SUM(job_count) AS total
        FROM daily_role_demand
        WHERE date BETWEEN :start_date AND :end_date
        GROUP BY role
        ORDER BY total DESC
        LIMIT :top_n
    """)
    rows = db.execute(sql, {"start_date": start_date, "end_date": end_date, "top_n": top_n}).fetchall()

    return {
        "data": [{"role": r[0], "total_jobs": r[1]} for r in rows],
        "data_freshness": datetime.now(timezone.utc).isoformat(),
    }
