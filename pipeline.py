"""
Run the Job Market Intelligence pipeline.

Usage
-----
  python pipeline.py               # run once now, then register 8-hour schedule (serves forever)
  python pipeline.py --once        # run once and exit
  python pipeline.py --schedule    # register schedule only, skip immediate run
  python pipeline.py --seed        # one-time Kaggle historical seed (run when DB is empty)

Prerequisites (each in its own terminal before running this)
  Terminal 1:  uvicorn serving.api.main:app --reload
  Terminal 2:  prefect server start

The Prefect UI at http://localhost:4200 shows live run status.
"""

from dotenv import load_dotenv
load_dotenv()   # must be first — loads PREFECT_API_URL from .env

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(description="Job Market Intelligence pipeline runner")
    parser.add_argument("--once",     action="store_true", help="Run once and exit")
    parser.add_argument("--schedule", action="store_true", help="Register schedule only (no immediate run)")
    parser.add_argument("--seed",     action="store_true", help="Run one-time Kaggle historical seed")
    args = parser.parse_args()

    from orchestration.flows import full_pipeline, seed_historical_data

    if args.seed:
        print("Running Kaggle historical seed (downloads datasets, runs once)...")
        result = seed_historical_data()
        print(f"Seed complete  run_id={result.get('run_id')}")
        return

    if args.once:
        print("Running full pipeline (Adzuna + Remotive -> Bronze -> Silver -> Gold)...")
        result = full_pipeline()
        print(f"Done  run_id={result.get('run_id')}")
        return

    if not args.schedule:
        print("Running immediate pipeline run first...")
        try:
            result = full_pipeline()
            print(f"Initial run complete  run_id={result.get('run_id')}\n")
        except Exception as exc:
            print(f"Warning: initial run failed: {exc}")
            print("The schedule will retry automatically.\n")

    print("Registering scheduled deployment 'full-pipeline/scheduled' (every 8 hours)...")
    print("Watch at: http://localhost:4200  ->  Deployments tab")
    print("Press Ctrl+C to stop.\n")
    full_pipeline.serve(
        name="scheduled",
        cron="0 */8 * * *",
        parameters={},
    )


if __name__ == "__main__":
    main()
