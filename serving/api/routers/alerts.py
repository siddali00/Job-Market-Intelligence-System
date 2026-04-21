"""
GET /api/alerts  — market alerts for spiking roles and skills
"""

from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from storage.db import get_db

router = APIRouter()


@router.get("")
def get_market_alerts(
    alert_date: date | None = None,
    entity_type: str | None = None,
    db: Session = Depends(get_db),
):
    """
    Returns roles/skills whose 7-day demand exceeds 2× their 30-day average.
    entity_type filter: 'role' | 'skill'
    Defaults to today's most recent alerts.
    """
    alert_date = alert_date or date.today()

    filters = ["alert_date = :alert_date"]
    params: dict = {"alert_date": alert_date}

    if entity_type in ("role", "skill"):
        filters.append("entity_type = :entity_type")
        params["entity_type"] = entity_type

    sql = text(f"""
        SELECT alert_date, entity_type, entity_name,
               demand_7d_avg, demand_30d_avg, spike_ratio
        FROM market_alerts
        WHERE {' AND '.join(filters)}
        ORDER BY spike_ratio DESC NULLS LAST
    """)
    rows = db.execute(sql, params).fetchall()

    return {
        "data": [
            {
                "alert_date": str(r[0]),
                "entity_type": r[1],
                "entity_name": r[2],
                "demand_7d_avg": r[3],
                "demand_30d_avg": r[4],
                "spike_ratio": r[5],
            }
            for r in rows
        ],
        "alert_date": str(alert_date),
        "data_freshness": datetime.now(timezone.utc).isoformat(),
    }
