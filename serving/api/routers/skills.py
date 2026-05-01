"""
GET /api/skills/trending   — top N skills in a date range
GET /api/skills/timeseries — daily series for selected skills
GET /api/skills/yearly     — year-over-year: top 10 skills, or every skill (one line each)
GET /api/skills/cooccurrence — skill pairs most often seen together
"""

from datetime import date, datetime, timezone
from typing import Annotated, Literal

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


def _pivot_yearly_skill_rows(
    rows: list,
    *,
    compare: Literal["top10", "all"],
    data_freshness: str,
    ranking_fallback: list[dict],
) -> dict:
    """Build years, skills, chart, ranking from (year, skill, total_jobs) query rows."""
    if not rows:
        return {
            "compare": compare,
            "years": [],
            "skills": [],
            "chart": [],
            "ranking": ranking_fallback,
            "data_freshness": data_freshness,
        }

    per_year: dict[int, dict[str, int]] = {}
    skill_totals: dict[str, int] = {}
    years_set: set[int] = set()
    for y, skill, tot in rows:
        years_set.add(int(y))
        if int(y) not in per_year:
            per_year[int(y)] = {}
        per_year[int(y)][skill] = int(tot)
        skill_totals[skill] = skill_totals.get(skill, 0) + int(tot)

    years = sorted(years_set)
    skills = sorted(
        skill_totals.keys(),
        key=lambda s: skill_totals[s],
        reverse=True,
    )
    chart: list[dict] = []
    for y in years:
        row: dict = {"year": y}
        for s in skills:
            row[s] = per_year.get(y, {}).get(s, 0)
        chart.append(row)

    ranking = [{"skill": s, "total_jobs": skill_totals[s]} for s in skills]
    return {
        "compare": compare,
        "years": years,
        "skills": skills,
        "chart": chart,
        "ranking": ranking,
        "data_freshness": data_freshness,
    }


@router.get("/yearly")
def get_skills_yearly(
    compare: Literal["top10", "all"] = "top10",
    db: Session = Depends(get_db),
):
    """
    Year-over-year demand from ``daily_skill_demand`` (post-2000 dates only).

    * **top10** — one line per skill for the **10** highest all-time skill totals.
    * **all** — one line **per skill** for **every** skill in gold (same shape as top10, no cap).
    """
    fresh = datetime.now(timezone.utc).isoformat()

    sql_rank_all = text("""
        SELECT skill, SUM(job_count)::bigint AS total_jobs
        FROM daily_skill_demand
        WHERE date >= DATE '2000-01-01'
        GROUP BY skill
        ORDER BY total_jobs DESC
    """)
    ranking_rows = db.execute(sql_rank_all).fetchall()
    ranking_fallback = [{"skill": r[0], "total_jobs": int(r[1])} for r in ranking_rows]

    if compare == "all":
        sql = text("""
            SELECT
                EXTRACT(YEAR FROM d.date)::integer AS y,
                d.skill,
                SUM(d.job_count)::bigint AS total_jobs
            FROM daily_skill_demand d
            WHERE d.date >= DATE '2000-01-01'
            GROUP BY 1, 2
            ORDER BY 1, 3 DESC
        """)
        rows = db.execute(sql).fetchall()
        return _pivot_yearly_skill_rows(rows, compare="all", data_freshness=fresh, ranking_fallback=ranking_fallback)

    # top10
    sql = text("""
        WITH top_skills AS (
            SELECT skill
            FROM daily_skill_demand
            WHERE date >= DATE '2000-01-01'
            GROUP BY skill
            ORDER BY SUM(job_count) DESC
            LIMIT 10
        )
        SELECT
            EXTRACT(YEAR FROM d.date)::integer AS y,
            d.skill,
            SUM(d.job_count)::bigint AS total_jobs
        FROM daily_skill_demand d
        INNER JOIN top_skills t ON t.skill = d.skill
        WHERE d.date >= DATE '2000-01-01'
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
    """)
    rows = db.execute(sql).fetchall()
    return _pivot_yearly_skill_rows(rows, compare="top10", data_freshness=fresh, ranking_fallback=ranking_fallback)
