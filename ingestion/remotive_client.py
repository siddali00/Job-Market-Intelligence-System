"""
Remotive Remote Jobs API ingester.

Docs:  https://github.com/remotive-com/remote-jobs-api
API:   GET https://remotive.com/api/remote-jobs
Auth:  None — free public API, no key required.

Rate limits (per ToS):
  - Maximum 4 requests per day (we schedule every 8h = 3x/day to stay safely under)
  - Never exceed 2 requests per minute — enforced by a 35s sleep between calls
  - Must link back to the Remotive listing URL and credit Remotive as source

Coverage:
  Fetches ALL active remote jobs in a single call (no category filter) so we use
  only 1 of our 3 daily requests and get every category at once.

  Categories returned in the data (for reference):
    software-dev, data, devops, product, design, finance, marketing,
    customer-support, sales, hr, legal, writing, all-others

Fields captured per job:
  id, title, company_name, category, job_type, candidate_required_location,
  salary (parsed → salary_min / salary_max), description, url, publication_date
"""

import re
import time

from ingestion.base_ingester import BaseIngester
from monitoring.logger import get_logger

logger = get_logger(__name__)

REMOTIVE_URL   = "https://remotive.com/api/remote-jobs"
CATEGORIES_URL = "https://remotive.com/api/remote-jobs/categories"

# Minimum seconds between consecutive HTTP calls (2/min limit → 35s gap)
REQUEST_INTERVAL_SECONDS = 35

# Keywords matched against the category display name (Remotive returns full strings
# like "Software Development", "DevOps / Sysadmin" — not slugs).
TECH_CATEGORY_KEYWORDS = {
    "software", "data", "devops", "sysadmin", "product", "design",
    "finance", "engineer", "developer", "dev", "tech", "cloud",
    "security", "machine learning", "ai", "analytics", "backend",
    "frontend", "fullstack", "full stack", "mobile", "it ",
}


class RemotiveIngester(BaseIngester):
    source_name = "remotive"

    def __init__(
        self,
        categories: list[str] | None = None,
        search: str | None = None,
        limit: int | None = None,
    ) -> None:
        """
        Args:
            categories: optional list of category slugs to keep.
                        Defaults to TECH_CATEGORIES (all tech-relevant ones).
                        Pass None to keep every category.
            search:     optional keyword search applied server-side.
            limit:      optional cap on results returned by the API.
        """
        super().__init__()
        self.categories = set(categories) if categories else None
        self.search     = search
        self.limit      = limit
        self._done      = False
        self._last_request_time: float = 0.0

    def run(self) -> dict:
        """Single API call → all active remote jobs → filter → save to Bronze."""
        records = self._fetch_with_retry(page=1)
        if records:
            self.save_to_bronze(records, page=1)

        summary = {
            "source":     self.source_name,
            "total_records": len(records),
            "categories_kept": sorted(self.categories) if self.categories else list(TECH_CATEGORY_KEYWORDS),
        }
        logger.info("remotive_run_complete", extra=summary)
        return summary

    def fetch_page(self, page: int) -> list[dict]:
        if page > 1 or self._done:
            return []

        self._respect_rate_limit()

        params: dict = {}
        if self.search:
            params["search"] = self.search
        if self.limit is not None:
            params["limit"] = self.limit
        # No category param → API returns ALL categories in one shot

        response = self.client.get(REMOTIVE_URL, params=params)
        response.raise_for_status()
        data = response.json()

        self._last_request_time = time.monotonic()
        self._done = True

        all_jobs  = data.get("jobs", [])
        job_count = data.get("job-count", len(all_jobs))
        logger.info("remotive_fetched_all",
                    extra={"total_from_api": job_count})

        # Filter to tech-relevant categories.
        # Remotive returns display names like "Software Development", "DevOps / Sysadmin"
        # so we do keyword matching rather than exact slug comparison.
        if self.categories:
            # caller passed an explicit list — exact match
            kept = [j for j in all_jobs
                    if j.get("category", "").lower() in self.categories]
        else:
            # default: keep any job whose category string contains a tech keyword
            kept = [
                j for j in all_jobs
                if any(kw in j.get("category", "").lower()
                       for kw in TECH_CATEGORY_KEYWORDS)
            ]
            if not kept:
                # If nothing matched (API changed categories), keep everything
                kept = all_jobs
        logger.info("remotive_filtered",
                    extra={"kept": len(kept), "dropped": job_count - len(kept)})

        return [self._normalise(job) for job in kept]

    # ── Private helpers ───────────────────────────────────────────────────────

    def _normalise(self, job: dict) -> dict:
        """
        Map a raw Remotive job object to the common ingestion envelope.
        Preserves the source URL and Remotive attribution as required by ToS.
        """
        salary_raw = job.get("salary") or ""
        salary_min, salary_max = _parse_salary_string(salary_raw)

        return {
            "source": self.source_name,
            "source_attribution": "Remotive (remotive.com)",   # required by Remotive ToS
            "external_id": str(job.get("id", "")),
            "title": job.get("title", ""),
            "company": job.get("company_name", ""),
            "category": job.get("category", ""),               # e.g. "Software Development"
            "job_type": job.get("job_type", ""),               # full_time | contract | part_time | freelance | internship
            "location": job.get("candidate_required_location", "Worldwide"),
            "country": None,                                   # freetext on Remotive — resolved in Bronze→Silver
            "description": job.get("description", ""),
            "salary_raw": salary_raw,                          # original string e.g. "$40,000 - $50,000"
            "salary_min": salary_min,
            "salary_max": salary_max,
            "remote": True,                                    # all Remotive listings are remote by definition
            "url": job.get("url", ""),                        # must be preserved and linked back per ToS
            "posted_at": job.get("publication_date", ""),
        }

    def _respect_rate_limit(self) -> None:
        """Enforce a minimum gap between requests to stay under 2/min."""
        if self._last_request_time:
            elapsed = time.monotonic() - self._last_request_time
            if elapsed < REQUEST_INTERVAL_SECONDS:
                wait = REQUEST_INTERVAL_SECONDS - elapsed
                logger.info("remotive_rate_limit_wait", extra={"wait_s": round(wait, 1)})
                time.sleep(wait)

    @classmethod
    def fetch_categories(cls) -> list[dict]:
        """
        Fetch the list of valid Remotive job categories.
        Returns a list of {id, name, slug} dicts.
        """
        import httpx
        resp = httpx.get(CATEGORIES_URL, timeout=15.0)
        resp.raise_for_status()
        return resp.json().get("jobs", [])


# ── Salary string parser ──────────────────────────────────────────────────────

def _parse_salary_string(salary_str: str) -> tuple[float | None, float | None]:
    """
    Parse Remotive's freetext salary string into (min, max) floats.

    Handles formats like:
        "$40,000 - $50,000"
        "$120k - $150k"
        "€60,000"
        "80000"
        ""  → (None, None)
    """
    if not salary_str:
        return None, None

    # Normalise: remove currency symbols, spaces, commas
    cleaned = re.sub(r"[€£¥\$,\s]", "", salary_str)

    # Expand shorthand: 120k → 120000
    cleaned = re.sub(r"(\d+\.?\d*)k", lambda m: str(int(float(m.group(1)) * 1000)), cleaned, flags=re.IGNORECASE)

    # Extract all numeric values
    numbers = [float(n) for n in re.findall(r"\d+\.?\d*", cleaned) if float(n) > 100]

    if not numbers:
        return None, None
    if len(numbers) == 1:
        # Single value — treat as both min and max
        return numbers[0], numbers[0]
    return min(numbers), max(numbers)
