"""
Prefect deployment definitions with cron schedules.

Deploys:
  full-pipeline    → every 8 hours  (0 */8 * * *)  — 3x/day
  seed-historical  → on-demand only (no schedule)

Schedule rationale:
  Remotive ToS allows a maximum of 4 requests/day.
  Running every 8h (3x/day) keeps us safely under that limit.
  Adzuna's free tier (250 req/day) is not a concern at this frequency.

**$0 / self-hosted (recommended):** run Prefect Server + a process worker on your
EC2 (same venv as the API). Set ``PREFECT_API_URL`` to your server
(e.g. ``http://127.0.0.1:4200/api``). Work pool name defaults to ``ec2-process``
(override with env ``PREFECT_WORK_POOL``). If you still have
``PREFECT_WORK_POOL=jm-process`` from Cloud Managed, it is ignored when
``PREFECT_API_URL`` points at localhost so deployments target ``ec2-process``.
See README / team notes for steps.

**Prefect Cloud Managed** (Hobby): use ``python -m orchestration.deploy_prefect_managed``
and a ``prefect:managed`` pool — heavy deps often fail; self-hosted avoids that.

Run locally against your active Prefect API profile::

  python -m orchestration.schedules
"""

import os
import sys

from dotenv import load_dotenv
from prefect.client.schemas.schedules import CronSchedule
from prefect.deployments import Deployment

from orchestration.flows import full_pipeline, seed_historical_data

_DEFAULT_SELF_HOSTED_POOL = "ec2-process"
_CLOUD_MANAGED_POOL = "jm-process"


def _api_url_looks_local() -> bool:
    api = (os.environ.get("PREFECT_API_URL") or "").lower()
    return bool(api) and ("127.0.0.1" in api or "localhost" in api)


def _resolve_work_pool() -> str:
    pool = (os.environ.get("PREFECT_WORK_POOL") or "").strip()
    if not pool:
        pool = _DEFAULT_SELF_HOSTED_POOL
    # .env often keeps Cloud Managed pool after switching to self-hosted server.
    if pool == _CLOUD_MANAGED_POOL and _api_url_looks_local():
        print(
            "Note: PREFECT_WORK_POOL=jm-process targets Prefect Cloud Managed; "
            f"PREFECT_API_URL is local — using work pool {_DEFAULT_SELF_HOSTED_POOL!r} instead.",
            file=sys.stderr,
        )
        return _DEFAULT_SELF_HOSTED_POOL
    return pool


def create_deployments() -> list[Deployment]:
    load_dotenv()
    pool = _resolve_work_pool()

    pipeline_deployment = Deployment.build_from_flow(
        flow=full_pipeline,
        name="scheduled",
        work_pool_name=pool,
        schedules=[CronSchedule(cron="0 */8 * * *", timezone="UTC")],
        tags=["production"],
        description="Runs the full ingestion + transformation pipeline every 8 hours (3x/day). Remotive ToS max is 4x/day.",
    )

    seed_deployment = Deployment.build_from_flow(
        flow=seed_historical_data,
        name="one-time-seed",
        work_pool_name=pool,
        schedules=[],   # no schedule — triggered manually
        tags=["setup"],
        description="One-time historical data seed from Kaggle. Run manually at project start.",
    )

    return [pipeline_deployment, seed_deployment]


if __name__ == "__main__":
    for deployment in create_deployments():
        deployment.apply()
        print(f"Deployed: {deployment.name}")
