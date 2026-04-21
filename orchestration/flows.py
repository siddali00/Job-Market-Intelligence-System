"""
Prefect flows for the Job Market Intelligence pipeline.

Flows
─────
  full_pipeline        — end-to-end: ingest → transform → validate (every 8h)
  ingest_flow          — pull from Adzuna + Remotive → Bronze
  transform_flow       — Bronze → Silver (PySpark) → Gold + validation
  seed_historical_data — one-time Kaggle historical load

Run tracking
────────────
  Every execution of full_pipeline / seed_historical_data creates a
  PipelineRun row in PostgreSQL (table: pipeline_runs).
  Each Job inserted in that run carries run_id so you can answer:
    "What was ingested in run X?"
    "When did we last update?"
  The Prefect UI at http://localhost:4200 shows per-task logs and timings.
"""

import uuid
from datetime import date, datetime, timezone

from prefect import flow, task, get_run_logger
from prefect.tasks import exponential_backoff


# ── Ingestion tasks ───────────────────────────────────────────────────────────

@task(
    name="ingest-adzuna",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
    tags=["ingestion", "adzuna"],
)
def ingest_adzuna(run_id: str, max_pages: int = 2) -> dict:
    """
    Pull from Adzuna using auto rotation slot (based on UTC hour).
      Slot 0 (00–08 UTC): us/gb/in  × data + software roles
      Slot 1 (08–16 UTC): ca/au/de  × ml/ai + infra roles
      Slot 2 (16–24 UTC): fr/sg/nl  × mixed roles
    Budget: 12 queries × 3 countries × 2 pages = 72 req/slot
            × 3 slots/day = 216/day  (free tier: 250 ✓, ~2-3 min runtime)
    """
    from ingestion.adzuna_client import AdzunaIngester
    ingester = AdzunaIngester(max_pages=max_pages, auto_slot=True)
    result   = ingester.run()
    result["run_id"] = run_id
    return result


@task(
    name="ingest-remotive",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
    tags=["ingestion", "remotive"],
)
def ingest_remotive(run_id: str) -> dict:
    """Single API call → all active remote tech jobs. 1 of 3 daily requests."""
    from ingestion.remotive_client import RemotiveIngester
    ingester = RemotiveIngester()
    result   = ingester.run()
    result["run_id"] = run_id
    return result


@task(
    name="ingest-kaggle-dataset",
    retries=2,
    retry_delay_seconds=exponential_backoff(backoff_factor=30),
    tags=["ingestion", "kaggle"],
)
def ingest_kaggle(run_id: str) -> dict:
    from ingestion.dataset_loader import KaggleDatasetLoader
    loader = KaggleDatasetLoader()
    result = loader.run()
    result["run_id"] = run_id
    return result


# ── Transform tasks ───────────────────────────────────────────────────────────

@task(
    name="bronze-to-silver",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
    tags=["transform"],
)
def bronze_to_silver(run_id: str, run_date: str | None = None) -> dict:
    """
    PySpark-powered Bronze → Silver transform.
    Cleans, deduplicates (within-batch + cross-run), then upserts to PostgreSQL.
    Falls back to pandas if PySpark / Java is unavailable.
    """
    from processing import bronze_to_silver as b2s
    return b2s.run(run_date=run_date, run_id=run_id)


@task(
    name="silver-to-gold",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
    tags=["transform"],
)
def silver_to_gold(run_id: str, run_date: str | None = None) -> dict:
    from processing import silver_to_gold as s2g
    return s2g.run(run_date=run_date)


@task(name="validate-bronze", retries=1, tags=["validation"])
def validate_bronze(run_date: str | None = None) -> dict:
    from processing.validators import run_bronze_validation
    return run_bronze_validation(run_date=run_date)


@task(name="validate-silver", retries=1, tags=["validation"])
def validate_silver() -> dict:
    from processing.validators import run_silver_validation
    return run_silver_validation()


@task(name="validate-gold", retries=1, tags=["validation"])
def validate_gold() -> dict:
    from processing.validators import run_gold_validation
    return run_gold_validation()


# ── PipelineRun lifecycle helpers ─────────────────────────────────────────────

def _new_run_id() -> str:
    return str(uuid.uuid4())


def _open_run(run_id: str) -> None:
    """Create a PipelineRun row at the start of a pipeline execution."""
    try:
        from processing.bronze_to_silver import create_pipeline_run
        create_pipeline_run(run_id)
    except Exception as exc:
        # Non-fatal — log it but don't abort the pipeline
        import logging
        logging.getLogger(__name__).warning(
            "pipeline_run_open_failed", extra={"run_id": run_id, "error": str(exc)}
        )


def _close_run(run_id: str, summary: dict, status: str = "completed") -> None:
    """Finalize the PipelineRun row with stats."""
    try:
        from processing.bronze_to_silver import update_pipeline_run
        update_pipeline_run(run_id, summary, status)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).warning(
            "pipeline_run_close_failed", extra={"run_id": run_id, "error": str(exc)}
        )


# ── Flows ─────────────────────────────────────────────────────────────────────

@flow(
    name="ingest-flow",
    description="Adzuna (rotation slot) + Remotive (all tech, single call) → Bronze.",
    log_prints=True,
)
def ingest_flow(run_id: str, adzuna_max_pages: int = 2) -> dict:
    logger   = get_run_logger()
    run_date = str(date.today())
    logger.info(f"ingest_flow  run_id={run_id}  date={run_date}")

    adzuna_result   = ingest_adzuna(run_id=run_id, max_pages=adzuna_max_pages)
    remotive_result = ingest_remotive(run_id=run_id)
    bronze_val      = validate_bronze(run_date=run_date)

    return {
        "run_date":         run_date,
        "adzuna":           adzuna_result,
        "remotive":         remotive_result,
        "bronze_validation": bronze_val,
    }


@flow(
    name="transform-flow",
    description="Bronze → Silver (PySpark dedup + clean) → Gold + validation.",
    log_prints=True,
)
def transform_flow(run_id: str, run_date: str | None = None) -> dict:
    logger   = get_run_logger()
    run_date = run_date or str(date.today())
    logger.info(f"transform_flow  run_id={run_id}  date={run_date}")

    b2s_result  = bronze_to_silver(run_id=run_id, run_date=run_date)
    silver_val  = validate_silver()

    if silver_val.get("overall") == "failed":
        logger.warning("Silver validation failed — continuing to Gold with caution")

    s2g_result  = silver_to_gold(run_id=run_id, run_date=run_date)
    gold_val    = validate_gold()

    _close_run(run_id, b2s_result)

    return {
        "run_date":         run_date,
        "bronze_to_silver": b2s_result,
        "silver_validation": silver_val,
        "silver_to_gold":   s2g_result,
        "gold_validation":  gold_val,
    }


@flow(
    name="full-pipeline",
    description=(
        "End-to-end: ingest (Adzuna + Remotive) → Bronze → Silver (PySpark) → Gold. "
        "Scheduled every 8 hours (3×/day, Remotive ToS ✓). "
        "Each run is tracked in pipeline_runs table with timestamps and counts."
    ),
    log_prints=True,
)
def full_pipeline(run_date: str | None = None) -> dict:
    logger   = get_run_logger()
    run_id   = _new_run_id()
    run_date = run_date or str(date.today())

    logger.info(f"full_pipeline  run_id={run_id}  date={run_date}")
    _open_run(run_id)

    try:
        ingest_result    = ingest_flow(run_id=run_id)
        transform_result = transform_flow(run_id=run_id, run_date=run_date)
        logger.info(f"full_pipeline complete  run_id={run_id}")
        return {"run_id": run_id, "ingest": ingest_result, "transform": transform_result}
    except Exception as exc:
        _close_run(run_id, {}, status="failed")
        raise


@flow(
    name="seed-historical-data",
    description="One-time Kaggle dataset seed (runs automatically when DB is empty).",
    log_prints=True,
)
def seed_historical_data() -> dict:
    logger   = get_run_logger()
    run_id   = _new_run_id()
    run_date = str(date.today())

    logger.info(f"seed_historical_data  run_id={run_id}")
    _open_run(run_id)

    try:
        kaggle_result = ingest_kaggle(run_id=run_id)
        b2s_result    = bronze_to_silver(run_id=run_id, run_date=run_date)
        _close_run(run_id, b2s_result)
        return {"run_id": run_id, "kaggle": kaggle_result, "bronze_to_silver": b2s_result}
    except Exception as exc:
        _close_run(run_id, {}, status="failed")
        raise


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    full_pipeline()
