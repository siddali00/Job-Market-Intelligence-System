"""
Prefect deployment definitions with cron schedules.

Deploys:
  full-pipeline    → every 8 hours  (0 */8 * * *)  — 3x/day
  seed-historical  → on-demand only (no schedule)

Schedule rationale:
  Remotive ToS allows a maximum of 4 requests/day.
  Running every 8h (3x/day) keeps us safely under that limit.
  Adzuna's free tier (250 req/day) is not a concern at this frequency.

Run:  python -m orchestration.schedules

Work pool: On Prefect Cloud Hobby tier, create a *Managed* pool (not ``process``):
``prefect work-pool create jm-process --type prefect:managed``
"""

from prefect.client.schemas.schedules import CronSchedule
from prefect.deployments import Deployment

from orchestration.flows import full_pipeline, seed_historical_data


def create_deployments() -> list[Deployment]:
    pipeline_deployment = Deployment.build_from_flow(
        flow=full_pipeline,
        name="scheduled",
        work_pool_name="jm-process",
        schedules=[CronSchedule(cron="0 */8 * * *", timezone="UTC")],
        parameters={
            "adzuna_query": "data engineer",
        },
        tags=["production"],
        description="Runs the full ingestion + transformation pipeline every 8 hours (3x/day). Remotive ToS max is 4x/day.",
    )

    seed_deployment = Deployment.build_from_flow(
        flow=seed_historical_data,
        name="one-time-seed",
        work_pool_name="jm-process",
        schedules=[],   # no schedule — triggered manually
        tags=["setup"],
        description="One-time historical data seed from Kaggle. Run manually at project start.",
    )

    return [pipeline_deployment, seed_deployment]


if __name__ == "__main__":
    for deployment in create_deployments():
        deployment.apply()
        print(f"Deployed: {deployment.name}")
