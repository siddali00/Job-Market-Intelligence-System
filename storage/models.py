"""
SQLAlchemy ORM models — Silver + Gold + Observability layers.

═══════════════════════════════════════════════════════════════════════════════
TABLE MAP  (13 tables total)
═══════════════════════════════════════════════════════════════════════════════

OBSERVABILITY
  pipeline_runs          ← every pipeline execution (timestamps, counts, status)
  pipeline_errors        ← all ingestion + transform errors (merged from two old tables)

SILVER (cleaned, normalised, deduplicated)
  jobs                   ← unified job postings from ALL sources
  job_skills             ← skill names per job (denormalised — no skills FK table)

  Source-specific detail tables (1:1 with jobs):
  adzuna_job_details     ← contract_type, contract_time, category, salary_is_predicted
  remotive_job_details   ← job_type, category, salary_raw
  kaggle_job_details     ← dataset_slug, experience_level, company_size …

GOLD (pre-aggregated for dashboard)
  daily_role_demand      ← job count per role per day + 7d/30d moving avg
  daily_skill_demand     ← job count per skill per day + 7d/30d moving avg
  salary_summary         ← median / p25 / p75 / p90 per role × country
  remote_vs_onsite       ← remote ratio per role per day
  market_alerts          ← demand spike detection (7d avg > 2× 30d avg)
  skill_cooccurrence     ← how often skill_a and skill_b appear together

═══════════════════════════════════════════════════════════════════════════════
DESIGN DECISIONS
═══════════════════════════════════════════════════════════════════════════════
  • company_name stored inline on `jobs` (denormalised).
    Avoids a round-trip find-or-create per insert while still enabling
    per-company analytics via GROUP BY company_name.
  • skill_name stored inline on `job_skills`.
    Eliminates the skills reference table + FK lookup, and makes
    silver_to_gold skill aggregations a direct scan instead of a join.
  • All job postings go into ONE `jobs` table (source column differentiates).
    Analytics queries work across all sources without UNION.
  • `pipeline_runs.run_id` stamped on every Job row for full auditability.
  • `pipeline_errors` replaces two old tables (ingestion_errors, transform_errors).
"""

import uuid
from datetime import datetime
from sqlalchemy import (
    Boolean, Column, Date, DateTime, Float, ForeignKey,
    Integer, String, Text, UniqueConstraint, func,
)
from sqlalchemy.orm import relationship
from storage.db import Base


# ─── Observability ────────────────────────────────────────────────────────────

class PipelineRun(Base):
    """
    Tracks every execution of full_pipeline / seed_historical_data.
    Powers the "data freshness" dashboard widget and audit trail.
    """
    __tablename__ = "pipeline_runs"

    run_id       = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    started_at   = Column(DateTime,  nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime,  nullable=True)
    status       = Column(String(20), nullable=False, default="running")
    # running | completed | failed | partial

    # Raw record counts from each source (filled during ingestion)
    adzuna_raw_count   = Column(Integer, default=0)
    remotive_raw_count = Column(Integer, default=0)
    kaggle_raw_count   = Column(Integer, default=0)

    # Silver layer outcomes (filled during transform)
    jobs_inserted     = Column(Integer, default=0)
    jobs_skipped      = Column(Integer, default=0)
    duplicates_removed= Column(Integer, default=0)
    errors_count      = Column(Integer, default=0)

    # Total jobs in DB after this run completes
    jobs_total_in_db = Column(Integer, nullable=True)

    notes = Column(Text, nullable=True)

    jobs   = relationship("Job",          back_populates="pipeline_run")
    errors = relationship("PipelineError", back_populates="pipeline_run")


class PipelineError(Base):
    """
    Unified error log for both ingestion and transform failures.
    Replaces the old ingestion_errors + transform_errors pair.
    """
    __tablename__ = "pipeline_errors"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    run_id        = Column(String(36), ForeignKey("pipeline_runs.run_id"), nullable=True, index=True)
    stage         = Column(String(50),  nullable=False)   # ingest_adzuna | ingest_remotive | bronze_to_silver | silver_to_gold
    error_type    = Column(String(100), nullable=True)
    error_message = Column(Text,        nullable=True)
    record_id     = Column(String(255), nullable=True)    # external_id or similar
    raw_payload   = Column(Text,        nullable=True)    # optional for ingest errors
    occurred_at   = Column(DateTime, server_default=func.now())

    pipeline_run = relationship("PipelineRun", back_populates="errors")


# ─── Silver Layer ─────────────────────────────────────────────────────────────

class Job(Base):
    __tablename__ = "jobs"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    source      = Column(String(50),  nullable=False)          # adzuna | remotive | kaggle
    external_id = Column(String(255), nullable=True)
    raw_hash    = Column(String(64),  nullable=False, unique=True)

    # Which pipeline run created this row
    run_id       = Column(String(36), ForeignKey("pipeline_runs.run_id"), nullable=True, index=True)
    pipeline_run = relationship("PipelineRun", back_populates="jobs")

    title            = Column(String(512), nullable=False)
    title_normalized = Column(String(255), nullable=True)   # canonical role family

    # Denormalised: company name stored directly (no FK to companies table)
    company_name = Column(String(255), nullable=True)

    location   = Column(String(255), nullable=True)
    country    = Column(String(100), nullable=True)
    city       = Column(String(100), nullable=True)
    remote     = Column(Boolean,     nullable=True)

    salary_min      = Column(Float, nullable=True)
    salary_max      = Column(Float, nullable=True)
    salary_currency = Column(String(10), nullable=True)

    salary_missing = Column(Boolean, default=False)
    skill_missing  = Column(Boolean, default=False)

    description = Column(Text,     nullable=True)
    posted_at   = Column(DateTime, nullable=True)
    ingested_at = Column(DateTime, server_default=func.now())

    job_skills = relationship("JobSkill", back_populates="job", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("raw_hash", name="uq_jobs_raw_hash"),
    )


class JobSkill(Base):
    """Skill name per job — denormalised (no foreign key to a skills table)."""
    __tablename__ = "job_skills"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    job_id     = Column(Integer, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    skill_name = Column(String(100), nullable=False)   # inline — no skills FK

    job = relationship("Job", back_populates="job_skills")

    __table_args__ = (
        UniqueConstraint("job_id", "skill_name", name="uq_job_skill"),
    )


# ─── Per-source detail tables ─────────────────────────────────────────────────

class AdzunaJobDetail(Base):
    """Adzuna-specific fields — 1:1 with jobs."""
    __tablename__ = "adzuna_job_details"

    id                  = Column(Integer, primary_key=True, autoincrement=True)
    job_id              = Column(Integer, ForeignKey("jobs.id", ondelete="CASCADE"),
                                 nullable=False, unique=True)
    contract_type       = Column(String(50),  nullable=True)
    contract_time       = Column(String(50),  nullable=True)
    salary_is_predicted = Column(Boolean,     nullable=True)
    category            = Column(String(255), nullable=True)
    category_tag        = Column(String(100), nullable=True)
    country_code        = Column(String(10),  nullable=True)
    location_area       = Column(Text,        nullable=True)

    job = relationship("Job", backref="adzuna_detail", uselist=False)


class RemotiveJobDetail(Base):
    """Remotive-specific fields — 1:1 with jobs. All Remotive jobs are remote."""
    __tablename__ = "remotive_job_details"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    job_id     = Column(Integer, ForeignKey("jobs.id", ondelete="CASCADE"),
                        nullable=False, unique=True)
    job_type   = Column(String(50),  nullable=True)
    category   = Column(String(255), nullable=True)
    salary_raw = Column(String(255), nullable=True)
    source_url = Column(Text,        nullable=True)   # required by Remotive ToS

    job = relationship("Job", backref="remotive_detail", uselist=False)


class KaggleJobDetail(Base):
    """Kaggle-dataset-specific fields — 1:1 with jobs."""
    __tablename__ = "kaggle_job_details"

    id               = Column(Integer, primary_key=True, autoincrement=True)
    job_id           = Column(Integer, ForeignKey("jobs.id", ondelete="CASCADE"),
                              nullable=False, unique=True)
    dataset_slug     = Column(String(255), nullable=True)
    dataset_year     = Column(Integer,     nullable=True)
    work_type        = Column(String(50),  nullable=True)
    experience_level = Column(String(50),  nullable=True)
    employment_type  = Column(String(50),  nullable=True)
    company_size     = Column(String(10),  nullable=True)

    job = relationship("Job", backref="kaggle_detail", uselist=False)


# ─── Unified Wide Staging ─────────────────────────────────────────────────────

class UnifiedJobWide(Base):
    """
    High-retention unified staging table built from raw sources (Kaggle/Adzuna/Remotive).
    Keeps broad coverage for profiling, dedup and later curated modeling.
    """
    __tablename__ = "unified_jobs_wide"

    id                   = Column(Integer, primary_key=True, autoincrement=True)
    normalization_run_id = Column(String(36), nullable=False, index=True)
    source               = Column(String(20), nullable=False, index=True)   # kaggle | adzuna | remotive
    dataset_key          = Column(String(100), nullable=True)
    source_file          = Column(String(255), nullable=True)
    source_record_id     = Column(Text, nullable=True)
    raw_table            = Column(String(255), nullable=True)
    raw_row_number       = Column(Integer, nullable=True)
    run_id               = Column(String(36), nullable=True)

    title                = Column(Text, nullable=True)
    title_norm           = Column(String(255), nullable=True)
    company_name         = Column(Text, nullable=True)
    location_raw         = Column(Text, nullable=True)
    country              = Column(String(100), nullable=True)
    city                 = Column(String(100), nullable=True)
    remote_flag          = Column(Boolean, nullable=True)
    posted_at_raw        = Column(Text, nullable=True)
    posted_at            = Column(DateTime, nullable=True)

    salary_min           = Column(Float, nullable=True)
    salary_max           = Column(Float, nullable=True)
    salary_currency      = Column(String(10), nullable=True)
    employment_type      = Column(String(80), nullable=True)

    description          = Column(Text, nullable=True)
    skills_text          = Column(Text, nullable=True)
    role_family          = Column(String(120), nullable=True)

    quality_score        = Column(Integer, nullable=False, default=0)
    quality_flags        = Column(Text, nullable=True)
    dedup_key            = Column(Text, nullable=True)
    is_exact_duplicate   = Column(Boolean, nullable=False, default=False)
    created_at           = Column(DateTime, server_default=func.now())


# ─── Gold Layer ───────────────────────────────────────────────────────────────

class DailyRoleDemand(Base):
    """Job count per role per day with rolling averages."""
    __tablename__ = "daily_role_demand"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    date           = Column(Date,    nullable=False)
    role           = Column(String(255), nullable=False)
    job_count      = Column(Integer, nullable=False, default=0)
    moving_avg_7d  = Column(Float,   nullable=True)
    moving_avg_30d = Column(Float,   nullable=True)
    computed_at    = Column(DateTime, server_default=func.now())

    __table_args__ = (UniqueConstraint("date", "role", name="uq_daily_role_demand"),)


class DailySkillDemand(Base):
    """Job count per skill per day with rolling averages."""
    __tablename__ = "daily_skill_demand"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    date           = Column(Date,    nullable=False)
    skill          = Column(String(100), nullable=False)
    job_count      = Column(Integer, nullable=False, default=0)
    moving_avg_7d  = Column(Float,   nullable=True)
    moving_avg_30d = Column(Float,   nullable=True)
    computed_at    = Column(DateTime, server_default=func.now())

    __table_args__ = (UniqueConstraint("date", "skill", name="uq_daily_skill_demand"),)


class SalarySummary(Base):
    """Salary statistics per role × country."""
    __tablename__ = "salary_summary"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    role          = Column(String(255), nullable=False)
    country       = Column(String(100), nullable=True)
    sample_size   = Column(Integer, nullable=False, default=0)
    salary_median = Column(Float, nullable=True)
    salary_p25    = Column(Float, nullable=True)
    salary_p75    = Column(Float, nullable=True)
    salary_p90    = Column(Float, nullable=True)
    computed_at   = Column(DateTime, server_default=func.now())

    __table_args__ = (UniqueConstraint("role", "country", name="uq_salary_summary"),)


class RemoteVsOnsite(Base):
    """Remote ratio per role per day."""
    __tablename__ = "remote_vs_onsite"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    date          = Column(Date,    nullable=False)
    role          = Column(String(255), nullable=False)
    remote_count  = Column(Integer, nullable=False, default=0)
    onsite_count  = Column(Integer, nullable=False, default=0)
    unknown_count = Column(Integer, nullable=False, default=0)
    remote_ratio  = Column(Float,   nullable=True)
    computed_at   = Column(DateTime, server_default=func.now())

    __table_args__ = (UniqueConstraint("date", "role", name="uq_remote_vs_onsite"),)


class SkillCooccurrence(Base):
    """How often skill_a and skill_b appear in the same job posting."""
    __tablename__ = "skill_cooccurrence"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    skill_a     = Column(String(100), nullable=False)
    skill_b     = Column(String(100), nullable=False)
    co_count    = Column(Integer, nullable=False, default=0)
    computed_at = Column(DateTime, server_default=func.now())

    __table_args__ = (UniqueConstraint("skill_a", "skill_b", name="uq_skill_cooccurrence"),)


class MarketAlert(Base):
    """Demand spike: 7-day average > 2× 30-day average for a role or skill."""
    __tablename__ = "market_alerts"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    alert_date     = Column(Date,    nullable=False)
    entity_type    = Column(String(20),  nullable=False)   # role | skill
    entity_name    = Column(String(255), nullable=False)
    demand_7d_avg  = Column(Float, nullable=True)
    demand_30d_avg = Column(Float, nullable=True)
    spike_ratio    = Column(Float, nullable=True)
    computed_at    = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("alert_date", "entity_type", "entity_name", name="uq_market_alert"),
    )
