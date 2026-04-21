"""
Abstract base class for all data ingesters.

Provides:
- save_to_bronze()  — write raw JSON to partitioned filesystem
- retry logic       — exponential backoff via httpx
- pagination loop   — subclasses implement fetch_page()
"""

import abc
import hashlib
import json
import os
import time
from datetime import date
from pathlib import Path
from typing import Any

import httpx
from monitoring.logger import get_logger
from config.settings import get_settings

logger = get_logger(__name__)
settings = get_settings()


class BaseIngester(abc.ABC):
    """
    All source-specific ingesters extend this class.

    Subclasses must implement:
        source_name  (str)  — e.g. "adzuna"
        fetch_page(page: int) -> list[dict]
    """

    source_name: str = "unknown"
    max_retries: int = 3
    base_delay: float = 2.0      # seconds; doubles on each retry

    def __init__(self) -> None:
        self.client = httpx.Client(timeout=30.0)

    # ── Public entry point ────────────────────────────────────────────────────

    def run(self) -> dict[str, Any]:
        """
        Paginate through all results and save each page to Bronze.
        Returns a summary dict for logging.
        """
        logger.info("ingestion_start", extra={"source": self.source_name})
        total_records = 0
        total_pages = 0
        page = 1

        while True:
            records = self._fetch_with_retry(page)
            if not records:
                break
            self.save_to_bronze(records, page)
            total_records += len(records)
            total_pages += 1
            logger.info(
                "page_ingested",
                extra={"source": self.source_name, "page": page, "records": len(records)},
            )
            page += 1

        summary = {
            "source": self.source_name,
            "total_records": total_records,
            "total_pages": total_pages,
            "date": str(date.today()),
        }
        logger.info("ingestion_complete", extra=summary)
        return summary

    # ── Abstract interface ────────────────────────────────────────────────────

    @abc.abstractmethod
    def fetch_page(self, page: int) -> list[dict]:
        """
        Fetch one page of raw records from the source.
        Return an empty list when there are no more pages.
        """

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _fetch_with_retry(self, page: int) -> list[dict]:
        delay = self.base_delay
        for attempt in range(1, self.max_retries + 1):
            try:
                return self.fetch_page(page)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 429:
                    retry_after = int(exc.response.headers.get("Retry-After", delay))
                    logger.warning(
                        "rate_limited",
                        extra={"source": self.source_name, "wait_s": retry_after, "attempt": attempt},
                    )
                    time.sleep(retry_after)
                elif attempt == self.max_retries:
                    logger.error(
                        "fetch_failed",
                        extra={"source": self.source_name, "page": page, "error": str(exc)},
                    )
                    raise
                else:
                    logger.warning(
                        "fetch_retry",
                        extra={"source": self.source_name, "attempt": attempt, "error": str(exc)},
                    )
                    time.sleep(delay)
                    delay *= 2
            except Exception as exc:
                if attempt == self.max_retries:
                    logger.error(
                        "fetch_failed",
                        extra={"source": self.source_name, "page": page, "error": str(exc)},
                    )
                    raise
                logger.warning(
                    "fetch_retry",
                    extra={"source": self.source_name, "attempt": attempt, "error": str(exc)},
                )
                time.sleep(delay)
                delay *= 2
        return []

    def save_to_bronze(
        self,
        records: list[dict],
        page: int,
        subfolder: str | None = None,
    ) -> Path:
        """
        Persist records as JSON to:
            data/bronze/{source}/{YYYY-MM-DD}/[subfolder/]batch_{page:04d}.json

        Bronze layout:
            data/bronze/
              adzuna/   2024-04-20/  batch_0001.json   ← Adzuna envelope (no subfolder)
              remotive/ 2024-04-20/  batch_0001.json   ← Remotive envelope
              kaggle/
                lukebarousse__data-analyst-.../        ← raw CSV (preserved)
                asaniczka__1-3m-linkedin-.../          ← raw CSV (preserved)
                2024-04-20/
                  lukebarousse/   batch_0001.json      ← per-dataset subfolder
                                  batch_0002.json
                  ds_salaries/    batch_0001.json
                  ai_market_2025/ batch_0001.json
        """
        today = str(date.today())
        directory = Path(settings.bronze_storage_path) / self.source_name / today
        if subfolder:
            directory = directory / subfolder
        directory.mkdir(parents=True, exist_ok=True)
        file_path = directory / f"batch_{page:04d}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, default=str)
        logger.info(
            "bronze_written",
            extra={
                "source": self.source_name,
                "path": str(file_path),
                "records": len(records),
            },
        )
        return file_path

    @staticmethod
    def make_raw_hash(*parts: str) -> str:
        """
        Deterministic dedup key: sha256 of concatenated string parts.
        Used during Bronze→Silver to detect duplicates across sources.
        """
        combined = "|".join(str(p).strip().lower() for p in parts if p)
        return hashlib.sha256(combined.encode()).hexdigest()

    def close(self) -> None:
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
