"""
GET /api/remote  — remote vs onsite ratios by role over time
"""

from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from storage.db import get_db

router = APIRouter()


@router.get("")
def get_remote_vs_onsite(
    role: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    db: Session = Depends(get_db),
):
    """
    Returns remote vs onsite counts and ratio per role, optionally filtered.
    """
    start_date = start_date or date(2024, 1, 1)
    end_date = end_date or date.today()

    filters = ["date BETWEEN :start_date AND :end_date"]
    params: dict = {"start_date": start_date, "end_date": end_date}

    if role:
        filters.append("role ILIKE :role")
        params["role"] = f"%{role}%"

    sql = text(f"""
        SELECT date, role, remote_count, onsite_count, remote_ratio
        FROM remote_vs_onsite
        WHERE {' AND '.join(filters)}
        ORDER BY role, date
    """)
    rows = db.execute(sql, params).fetchall()

    return {
        "data": [
            {
                "date": str(r[0]),
                "role": r[1],
                "remote_count": r[2],
                "onsite_count": r[3],
                "remote_ratio": r[4],
            }
            for r in rows
        ],
        "filters": {"role": role},
        "data_freshness": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/summary")
def get_remote_summary(db: Session = Depends(get_db)):
    """Overall remote vs onsite ratio across all roles and dates."""
    sql = text("""
        SELECT
            SUM(remote_count)  AS total_remote,
            SUM(onsite_count)  AS total_onsite,
            ROUND(
                SUM(remote_count)::numeric /
                NULLIF(SUM(remote_count) + SUM(onsite_count), 0) * 100,
                1
            ) AS remote_pct
        FROM remote_vs_onsite
    """)
    row = db.execute(sql).fetchone()
    return {
        "total_remote": row[0],
        "total_onsite": row[1],
        "remote_pct": row[2],
        "data_freshness": datetime.now(timezone.utc).isoformat(),
    }
