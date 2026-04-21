"""
Raw -> Unified Wide normalization (ingestion-focused).

Purpose
-------
Build a source-agnostic, high-retention table for analysis prep without
touching the existing `jobs`/`kaggle_job_details` flow.

Input sources
-------------
- kaggle_raw_* tables (relational raw copies from Kaggle files)
- bronze JSON envelopes for adzuna/remotive

Output
------
- unified_jobs_wide (single table used for profiling, dedup, and downstream
  curated/dashboard modeling)
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text

from config.settings import get_settings
from monitoring.logger import get_logger
from storage.db import engine

logger = get_logger(__name__)
settings = get_settings()


def run(run_date: str | None = None, truncate: bool = False) -> dict[str, Any]:
    normalization_run_id = str(uuid.uuid4())
    _ensure_table()
    if truncate:
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE unified_jobs_wide"))

    inserted = 0
    inserted += _load_kaggle_raw(normalization_run_id)
    inserted += _load_api_bronze("adzuna", normalization_run_id, run_date=run_date)
    inserted += _load_api_bronze("remotive", normalization_run_id, run_date=run_date)
    flagged = _mark_exact_duplicates(normalization_run_id)

    summary = {
        "stage": "raw_to_wide",
        "normalization_run_id": normalization_run_id,
        "rows_inserted": inserted,
        "duplicates_flagged": flagged,
        "run_date_scope": run_date or "all_dates",
    }
    logger.info("raw_to_wide_complete", extra=summary)
    return summary


def _ensure_table() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS unified_jobs_wide (
      id BIGSERIAL PRIMARY KEY,
      normalization_run_id VARCHAR(36) NOT NULL,
      source VARCHAR(20) NOT NULL,
      dataset_key VARCHAR(100) NULL,
      source_file VARCHAR(255) NULL,
      source_record_id TEXT NULL,
      raw_table VARCHAR(255) NULL,
      raw_row_number BIGINT NULL,
      run_id VARCHAR(36) NULL,
      title TEXT NULL,
      title_norm VARCHAR(255) NULL,
      company_name TEXT NULL,
      location_raw TEXT NULL,
      country VARCHAR(100) NULL,
      city VARCHAR(100) NULL,
      remote_flag BOOLEAN NULL,
      posted_at_raw TEXT NULL,
      posted_at TIMESTAMP NULL,
      salary_min DOUBLE PRECISION NULL,
      salary_max DOUBLE PRECISION NULL,
      salary_currency VARCHAR(10) NULL,
      employment_type VARCHAR(80) NULL,
      description TEXT NULL,
      skills_text TEXT NULL,
      role_family VARCHAR(120) NULL,
      quality_score INTEGER NOT NULL DEFAULT 0,
      quality_flags TEXT NULL,
      dedup_key TEXT NULL,
      is_exact_duplicate BOOLEAN NOT NULL DEFAULT FALSE,
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_unified_jobs_wide_run ON unified_jobs_wide (normalization_run_id)"
        ))
        conn.execute(text(
            "CREATE INDEX IF NOT EXISTS idx_unified_jobs_wide_source ON unified_jobs_wide (source)"
        ))


def _load_kaggle_raw(normalization_run_id: str, batch_size: int = 2_000) -> int:
    inserted = 0
    with engine.begin() as conn:
        tables = [
            r[0]
            for r in conn.execute(text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='public' AND table_name LIKE 'kaggle_raw_%' ORDER BY 1"
            ))
        ]

    for table in tables:
        with engine.connect().execution_options(stream_results=True) as conn:
            cols = [r[0] for r in conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name=:t"
            ), {"t": table})]
            result = conn.execute(text(f'SELECT * FROM "{table}"'))
            batch: list[dict[str, Any]] = []
            for row in result.mappings():
                canonical = _canonical_from_kaggle_row(table, cols, row, normalization_run_id)
                if not canonical:
                    continue
                batch.append(canonical)
                if len(batch) >= batch_size:
                    _insert_wide_batch(batch)
                    inserted += len(batch)
                    batch = []
            if batch:
                _insert_wide_batch(batch)
                inserted += len(batch)
        logger.info("raw_to_wide_kaggle_table_loaded", extra={"table": table})
    return inserted


def _load_api_bronze(source: str, normalization_run_id: str, run_date: str | None = None) -> int:
    root = Path(settings.bronze_storage_path) / source
    if not root.exists():
        return 0

    if run_date:
        date_dirs = [root / run_date]
    else:
        date_dirs = [d for d in root.iterdir() if d.is_dir()]

    inserted = 0
    batch: list[dict[str, Any]] = []
    for d in sorted(date_dirs):
        if not d.exists():
            continue
        for jf in sorted(d.glob("**/*.json")):
            try:
                rows = json.loads(jf.read_text(encoding="utf-8"))
            except Exception:
                continue
            for i, raw in enumerate(rows, start=1):
                canonical = _canonical_from_api_row(source, raw, normalization_run_id, jf.name, i)
                if not canonical:
                    continue
                batch.append(canonical)
                if len(batch) >= 2_000:
                    _insert_wide_batch(batch)
                    inserted += len(batch)
                    batch = []
    if batch:
        _insert_wide_batch(batch)
        inserted += len(batch)
    return inserted


def _canonical_from_kaggle_row(
    table: str,
    cols: list[str],
    row: dict[str, Any],
    normalization_run_id: str,
) -> dict[str, Any] | None:
    def pick(*names: str) -> Any:
        for n in names:
            if n in cols:
                return row.get(n)
        return None

    title = _clean_text(pick("title", "job_title", "position", "role"))
    company = _clean_text(pick("company", "company_name", "employer", "organization"))
    location = _clean_text(pick("location", "job_location", "city", "region"))
    description = _clean_text(pick("description", "job_description", "cleaned_description", "summary"))
    posted_raw = _clean_text(pick("posted_at", "date_posted", "job_posted_date", "date_time", "first_seen"))
    salary_min = _to_float(pick("salary_min", "min_amount", "salary_from", "low_salary"))
    salary_max = _to_float(pick("salary_max", "max_amount", "salary_to", "high_salary"))
    if salary_min is None:
        salary_min = _to_float(pick("salary_in_usd", "salary", "salary_avg", "mean_salary"))
    if salary_max is None:
        salary_max = salary_min
    country = _clean_text(pick("country", "job_country", "company_location", "employee_residence"))
    source_record_id = _clean_text(pick("src_id", "job_id", "job_link", "job_url", "job_url_direct"))
    remote = _to_bool(pick("is_remote", "remote", "remote_friendly"))
    posted_at = _parse_dt(posted_raw)
    skills_text = _clean_text(pick("skills", "job_skills", "description_tokens", "required_skills"))
    employment_type = _clean_text(pick("employment_type", "job_type", "work_type", "schedule_type"))
    dataset_key = _clean_text(row.get("dataset_key"))
    source_file = _clean_text(row.get("source_file"))

    return _build_wide_row(
        normalization_run_id=normalization_run_id,
        source="kaggle",
        dataset_key=dataset_key,
        source_file=source_file,
        source_record_id=source_record_id,
        raw_table=table,
        raw_row_number=row.get("row_number"),
        run_id=row.get("run_id"),
        title=title,
        company=company,
        location=location,
        country=country,
        remote=remote,
        posted_raw=posted_raw,
        posted_at=posted_at,
        salary_min=salary_min,
        salary_max=salary_max,
        salary_currency=_clean_text(pick("currency", "salary_currency")) or "USD",
        employment_type=employment_type,
        description=description,
        skills_text=skills_text,
    )


def _canonical_from_api_row(
    source: str,
    raw: dict[str, Any],
    normalization_run_id: str,
    source_file: str,
    row_number: int,
) -> dict[str, Any] | None:
    title = _clean_text(raw.get("title"))
    company = _clean_text(raw.get("company"))
    location = _clean_text(raw.get("location"))
    country = _clean_text(raw.get("country"))
    description = _clean_text(raw.get("description"))
    posted_raw = _clean_text(raw.get("posted_at"))
    posted_at = _parse_dt(posted_raw)
    source_record_id = _clean_text(raw.get("external_id") or raw.get("id") or raw.get("url"))
    skills_text = ", ".join(raw.get("skills_extracted") or []) if isinstance(raw.get("skills_extracted"), list) else None

    return _build_wide_row(
        normalization_run_id=normalization_run_id,
        source=source,
        dataset_key=None,
        source_file=source_file,
        source_record_id=source_record_id,
        raw_table=f"bronze_{source}",
        raw_row_number=row_number,
        run_id=None,
        title=title,
        company=company,
        location=location,
        country=country,
        remote=_to_bool(raw.get("remote")),
        posted_raw=posted_raw,
        posted_at=posted_at,
        salary_min=_to_float(raw.get("salary_min")),
        salary_max=_to_float(raw.get("salary_max")),
        salary_currency="USD",
        employment_type=_clean_text(raw.get("job_type") or raw.get("contract_type")),
        description=description,
        skills_text=skills_text,
    )


def _build_wide_row(**kwargs: Any) -> dict[str, Any]:
    title = kwargs.get("title")
    company = kwargs.get("company")
    location = kwargs.get("location")
    posted_at = kwargs.get("posted_at")
    description = kwargs.get("description")

    city = _infer_city(location)
    country = kwargs.get("country") or _infer_country(location)
    role_family = _normalize_role(title or "")

    quality_flags: list[str] = []
    score = 0
    if title:
        score += 35
    else:
        quality_flags.append("missing_title")
    if company:
        score += 15
    else:
        quality_flags.append("missing_company")
    if location:
        score += 15
    else:
        quality_flags.append("missing_location")
    if posted_at:
        score += 10
    else:
        quality_flags.append("missing_posted_at")
    if description and len(description) >= 80:
        score += 15
    else:
        quality_flags.append("short_description")
    if kwargs.get("salary_min") is not None or kwargs.get("salary_max") is not None:
        score += 10
    else:
        quality_flags.append("missing_salary")

    dedup_key = "|".join([
        (title or "").strip().lower(),
        (company or "").strip().lower(),
        (location or "").strip().lower(),
        (posted_at.date().isoformat() if posted_at else ""),
    ])

    return {
        "normalization_run_id": _limit(kwargs.get("normalization_run_id"), 36),
        "source": _limit(kwargs.get("source"), 20),
        "dataset_key": _limit(kwargs.get("dataset_key"), 100),
        "source_file": _limit(kwargs.get("source_file"), 255),
        "source_record_id": kwargs.get("source_record_id"),
        "raw_table": _limit(kwargs.get("raw_table"), 255),
        "raw_row_number": kwargs.get("raw_row_number"),
        "run_id": _limit(kwargs.get("run_id"), 36),
        "title": title,
        "title_norm": _limit(_clean_text(title), 255),
        "company_name": company,
        "location_raw": location,
        "country": _limit(country, 100),
        "city": _limit(city, 100),
        "remote_flag": kwargs.get("remote"),
        "posted_at_raw": kwargs.get("posted_raw"),
        "posted_at": posted_at,
        "salary_min": kwargs.get("salary_min"),
        "salary_max": kwargs.get("salary_max"),
        "salary_currency": _limit(kwargs.get("salary_currency"), 10),
        "employment_type": _limit(kwargs.get("employment_type"), 80),
        "description": description,
        "skills_text": kwargs.get("skills_text"),
        "role_family": _limit(role_family, 120),
        "quality_score": score,
        "quality_flags": ",".join(quality_flags) if quality_flags else None,
        "dedup_key": dedup_key,
        "is_exact_duplicate": False,
    }


def _insert_wide_batch(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    sql = text("""
        INSERT INTO unified_jobs_wide (
          normalization_run_id, source, dataset_key, source_file, source_record_id,
          raw_table, raw_row_number, run_id, title, title_norm, company_name,
          location_raw, country, city, remote_flag, posted_at_raw, posted_at,
          salary_min, salary_max, salary_currency, employment_type, description,
          skills_text, role_family, quality_score, quality_flags, dedup_key, is_exact_duplicate
        ) VALUES (
          :normalization_run_id, :source, :dataset_key, :source_file, :source_record_id,
          :raw_table, :raw_row_number, :run_id, :title, :title_norm, :company_name,
          :location_raw, :country, :city, :remote_flag, :posted_at_raw, :posted_at,
          :salary_min, :salary_max, :salary_currency, :employment_type, :description,
          :skills_text, :role_family, :quality_score, :quality_flags, :dedup_key, :is_exact_duplicate
        )
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)


def _mark_exact_duplicates(normalization_run_id: str) -> int:
    with engine.begin() as conn:
        conn.execute(text("""
            WITH ranked AS (
              SELECT id, ROW_NUMBER() OVER (
                PARTITION BY dedup_key
                ORDER BY quality_score DESC, id ASC
              ) AS rn
              FROM unified_jobs_wide
              WHERE normalization_run_id = :rid
            )
            UPDATE unified_jobs_wide u
            SET is_exact_duplicate = CASE WHEN r.rn > 1 THEN TRUE ELSE FALSE END
            FROM ranked r
            WHERE u.id = r.id
        """), {"rid": normalization_run_id})

        flagged = conn.execute(text("""
            SELECT COUNT(*) FROM unified_jobs_wide
            WHERE normalization_run_id = :rid AND is_exact_duplicate = TRUE
        """), {"rid": normalization_run_id}).scalar() or 0
    return int(flagged)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _limit(value: Any, size: int) -> str | None:
    s = _clean_text(value)
    if s is None:
        return None
    return s[:size]


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        s = str(value).replace(",", "").replace("$", "").strip()
        if not s:
            return None
        f = float(s)
        return f if f > 0 else None
    except Exception:
        return None


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"1", "true", "t", "yes", "y"}:
        return True
    if s in {"0", "false", "f", "no", "n"}:
        return False
    return None


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d",
        "%d/%m/%Y",
    ):
        try:
            return datetime.strptime(s[:19], fmt)
        except Exception:
            continue
    return None


def _infer_city(location: str | None) -> str | None:
    if not location:
        return None
    return location.split(",")[0].strip() or None


def _infer_country(location: str | None) -> str | None:
    if not location:
        return None
    loc = location.lower()
    mapping = {
        "US": ["us", "usa", "united states", "new york", "san francisco", "seattle"],
        "GB": ["uk", "united kingdom", "london", "manchester"],
        "IN": ["india", "bangalore", "hyderabad"],
        "CA": ["canada", "toronto", "vancouver"],
        "DE": ["germany", "berlin", "munich"],
        "AU": ["australia", "sydney", "melbourne"],
    }
    for code, hints in mapping.items():
        if any(h in loc for h in hints):
            return code
    return None


def _normalize_role(title: str) -> str | None:
    t = (title or "").lower()
    rules = [
        ("Data Engineer", ["data engineer", "etl", "pipeline"]),
        ("Data Scientist", ["data scientist"]),
        ("Data Analyst", ["data analyst", "business analyst", "bi analyst"]),
        ("ML Engineer", ["ml engineer", "machine learning", "mlops"]),
        ("AI Engineer", ["ai engineer", "nlp", "computer vision"]),
        ("Software Engineer", ["software engineer", "software developer", "backend", "full stack"]),
        ("Frontend Engineer", ["frontend", "front-end", "react"]),
        ("DevOps/Platform", ["devops", "platform engineer", "sre"]),
        ("Product Manager", ["product manager", "product owner"]),
    ]
    for label, keys in rules:
        if any(k in t for k in keys):
            return label
    return None
