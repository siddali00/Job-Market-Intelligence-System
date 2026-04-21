"""
GET /api/salaries  — salary percentiles by role and/or country
"""

from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from storage.db import get_db

router = APIRouter()


@router.get("")
def get_salaries(
    role: str | None = None,
    country: str | None = None,
    db: Session = Depends(get_db),
):
    """
    Returns salary distribution (median, p25, p75, p90) by role × country.
    Filter by role and/or country via query params.
    """
    filters = []
    params: dict = {}

    if role:
        filters.append("role ILIKE :role")
        params["role"] = f"%{role}%"
    if country:
        filters.append("country = :country")
        params["country"] = country.upper()

    where_clause = ("WHERE " + " AND ".join(filters)) if filters else ""

    sql = text(f"""
        SELECT role, country, sample_size,
               salary_median, salary_p25, salary_p75, salary_p90
        FROM salary_summary
        {where_clause}
        ORDER BY sample_size DESC
    """)
    rows = db.execute(sql, params).fetchall()

    return {
        "data": [
            {
                "role": r[0],
                "country": r[1],
                "sample_size": r[2],
                "salary_median": r[3],
                "salary_p25": r[4],
                "salary_p75": r[5],
                "salary_p90": r[6],
            }
            for r in rows
        ],
        "filters": {"role": role, "country": country},
        "data_freshness": datetime.now(timezone.utc).isoformat(),
    }
