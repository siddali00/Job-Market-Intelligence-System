"""
GET /api/skills/trending  — top N skills ranked by growth rate over a date range
GET /api/skills/cooccurrence — skill pairs most often seen together
"""

from datetime import date, datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from storage.db import get_db

router = APIRouter()


@router.get("/trending")
def get_trending_skills(
    top_n: Annotated[int, Query(ge=1, le=100)] = 20,
    start_date: date | None = None,
    end_date: date | None = None,
    db: Session = Depends(get_db),
):
    """
    Returns the top N skills ordered by 7-day moving average (descending).
    Optionally filter to a specific date window.
    """
    start_date = start_date or date(2024, 1, 1)
    end_date = end_date or date.today()

    sql = text("""
        SELECT
            skill,
            SUM(job_count)          AS total_jobs,
            MAX(moving_avg_7d)      AS peak_7d_avg,
            MAX(moving_avg_30d)     AS peak_30d_avg
        FROM daily_skill_demand
        WHERE date BETWEEN :start_date AND :end_date
        GROUP BY skill
        ORDER BY peak_7d_avg DESC NULLS LAST
        LIMIT :top_n
    """)
    rows = db.execute(sql, {"start_date": start_date, "end_date": end_date, "top_n": top_n}).fetchall()

    return {
        "data": [
            {
                "skill": r[0],
                "total_jobs": r[1],
                "peak_7d_avg": r[2],
                "peak_30d_avg": r[3],
            }
            for r in rows
        ],
        "filters": {"start_date": str(start_date), "end_date": str(end_date), "top_n": top_n},
        "data_freshness": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/timeseries")
def get_skill_timeseries(
    skills: Annotated[list[str], Query()] = None,
    start_date: date | None = None,
    end_date: date | None = None,
    db: Session = Depends(get_db),
):
    """
    Returns daily job counts + 7d moving average for specified skills.
    Pass skill=python&skill=sql to get multiple series.
    """
    start_date = start_date or date(2024, 1, 1)
    end_date = end_date or date.today()
    skills = skills or ["python", "sql", "spark"]

    sql = text("""
        SELECT date, skill, job_count, moving_avg_7d
        FROM daily_skill_demand
        WHERE date BETWEEN :start_date AND :end_date
          AND skill = ANY(:skills)
        ORDER BY skill, date
    """)
    rows = db.execute(sql, {"start_date": start_date, "end_date": end_date, "skills": skills}).fetchall()

    return {
        "data": [{"date": str(r[0]), "skill": r[1], "job_count": r[2], "moving_avg_7d": r[3]} for r in rows],
        "data_freshness": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/cooccurrence")
def get_skill_cooccurrence(
    top_n: Annotated[int, Query(ge=1, le=200)] = 50,
    db: Session = Depends(get_db),
):
    """Returns skill pairs most frequently mentioned together."""
    sql = text("""
        SELECT skill_a, skill_b, co_count
        FROM skill_cooccurrence
        ORDER BY co_count DESC
        LIMIT :top_n
    """)
    rows = db.execute(sql, {"top_n": top_n}).fetchall()
    return {
        "data": [{"skill_a": r[0], "skill_b": r[1], "co_count": r[2]} for r in rows],
        "data_freshness": datetime.now(timezone.utc).isoformat(),
    }
