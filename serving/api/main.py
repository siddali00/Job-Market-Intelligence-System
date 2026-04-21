"""
FastAPI application entry point.

Mounts all routers and configures CORS so the React frontend can call the API.
On startup, automatically creates all database tables if they don't exist yet.

Auto-generated docs available at:
  http://localhost:8000/docs   (Swagger UI)
  http://localhost:8000/redoc  (ReDoc)
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config.settings import get_settings
from monitoring.logger import get_logger
from serving.api.routers import skills, salaries, roles, remote, alerts, predict, health

settings = get_settings()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ───────────────────────────────────────────────────────────────
    logger.info("startup_begin", extra={"service": "api"})
    _init_db()
    _init_data_dirs()
    logger.info("startup_complete", extra={"service": "api"})
    yield
    # ── Shutdown ──────────────────────────────────────────────────────────────
    logger.info("shutdown", extra={"service": "api"})


def _init_db() -> None:
    """Create all tables that don't exist yet, then patch any missing columns (dev migration)."""
    from storage.db import engine
    from storage.models import Base
    from sqlalchemy import text, inspect
    try:
        Base.metadata.create_all(bind=engine)
        _patch_missing_columns(engine)
        logger.info("db_tables_ready")
    except Exception as exc:
        logger.error("db_init_failed", extra={"error": str(exc)})
        raise


def _patch_missing_columns(engine) -> None:
    """
    Add any columns that exist in ORM models but are missing from the live DB.
    Safe to call on every startup — uses ADD COLUMN IF NOT EXISTS.

    Also drops obsolete tables from the old schema (companies, skills,
    ingestion_errors, transform_errors) that were replaced in the current design.
    """
    from sqlalchemy import text, inspect

    insp = inspect(engine)
    existing_tables = set(insp.get_table_names())

    # ── Add missing columns on current tables ─────────────────────────────────
    additions = [
        # (table, column, ddl_type)
        ("jobs",       "run_id",       "VARCHAR(36)"),
        ("jobs",       "company_name", "VARCHAR(255)"),
        ("remote_vs_onsite", "unknown_count", "INTEGER DEFAULT 0"),
    ]

    with engine.connect() as conn:
        for table, column, col_ddl in additions:
            if table not in existing_tables:
                continue
            existing_cols = {c["name"] for c in insp.get_columns(table)}
            if column not in existing_cols:
                conn.execute(text(
                    f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS "{column}" {col_ddl}'
                ))
                logger.info("schema_patched", extra={"table": table, "column": column})
        conn.commit()

    # ── Drop obsolete tables from old schema ──────────────────────────────────
    obsolete = ["companies", "skills", "ingestion_errors", "transform_errors"]
    with engine.connect() as conn:
        for tbl in obsolete:
            if tbl in existing_tables:
                conn.execute(text(f'DROP TABLE IF EXISTS "{tbl}" CASCADE'))
                logger.info("obsolete_table_dropped", extra={"table": tbl})
        conn.commit()


def _init_data_dirs() -> None:
    """Ensure Bronze and report directories exist so ingesters can write immediately."""
    from pathlib import Path
    for path in [settings.bronze_storage_path, settings.ge_reports_path]:
        Path(path).mkdir(parents=True, exist_ok=True)


app = FastAPI(
    title="Job Market Intelligence API",
    description="REST API serving labour-market insights from the Medallion pipeline.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, tags=["health"])
app.include_router(skills.router, prefix="/api/skills", tags=["skills"])
app.include_router(salaries.router, prefix="/api/salaries", tags=["salaries"])
app.include_router(roles.router, prefix="/api/roles", tags=["roles"])
app.include_router(remote.router, prefix="/api/remote", tags=["remote"])
app.include_router(alerts.router, prefix="/api/alerts", tags=["alerts"])
app.include_router(predict.router, prefix="/api/predict", tags=["predictions"])
