"""
Run raw-data normalization into unified wide schema.

Examples:
  python raw_pipeline.py
  python raw_pipeline.py --run-date 2026-04-21
  python raw_pipeline.py --truncate
"""

from __future__ import annotations

import argparse
import json

from dotenv import load_dotenv

from processing.raw_to_wide import run as run_raw_to_wide


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Normalize raw ingested data to unified wide table.")
    parser.add_argument("--run-date", help="Optional bronze date partition (YYYY-MM-DD) for adzuna/remotive scope.")
    parser.add_argument("--truncate", action="store_true", help="Truncate unified_jobs_wide before loading.")
    args = parser.parse_args()

    result = run_raw_to_wide(run_date=args.run_date, truncate=args.truncate)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
