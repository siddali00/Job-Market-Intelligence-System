"""
Adzuna Jobs API ingester.

Docs:       https://developer.adzuna.com/docs/search
Auth:       ADZUNA_APP_ID + ADZUNA_APP_KEY  (free tier: 250 req/day)
Results:    Up to 50 results per page

╔══════════════════════════════════════════════════════════════════════════╗
║  COVERAGE                                                                ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Regions (11):  us gb in ca au de fr sg nl nz za                        ║
║  Queries (24):  4 categories × 6 roles each                             ║
║                 data_roles, software_roles, ml_ai_roles, infra_roles    ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Budget strategy — ROTATION SLOTS                                        ║
║  The 24h pipeline runs 3×/day (every 8h). Each run covers a             ║
║  different slice so ALL regions+queries are hit within 24h.             ║
║                                                                          ║
║  Slot 0 (00:00 UTC): us / gb / in  ×  data + software roles             ║
║  Slot 1 (08:00 UTC): ca / au / de  ×  ml/ai + infra roles               ║
║  Slot 2 (16:00 UTC): fr / sg / nl  ×  mixed roles                       ║
║                                                                          ║
║  Each slot: ~3 countries × 4 queries × 4 pages = 48 req  (limit 250✓)  ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Remote detection (no native param on free tier):                       ║
║  1. Dedicated "remote" search pass  (what_and=remote)                   ║
║  2. Keyword scan of title + description + location                      ║
║  3. Adzuna response: contract_time, contract_type, salary_is_predicted  ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

import re
import time
from datetime import datetime, timezone

import httpx
from config.settings import get_settings
from ingestion.base_ingester import BaseIngester
from monitoring.logger import get_logger

logger = get_logger(__name__)
settings = get_settings()

ADZUNA_BASE_URL  = "https://api.adzuna.com/v1/api/jobs"
RESULTS_PER_PAGE = 50

# ── All supported Adzuna country codes ────────────────────────────────────────
ALL_COUNTRIES = ["us", "gb", "in", "ca", "au", "de", "fr", "sg", "nl", "nz", "za"]

# ── Query groups by category ──────────────────────────────────────────────────
QUERY_GROUPS = {
    "data_roles": [
        "data engineer",
        "data analyst",
        "data architect",
        "analytics engineer",
        "business intelligence engineer",
        "data platform engineer",
    ],
    "software_roles": [
        "software engineer",
        "backend developer",
        "full stack developer",
        "frontend developer",
        "api developer",
        "software developer",
    ],
    "ml_ai_roles": [
        "machine learning engineer",
        "data scientist",
        "AI engineer",
        "NLP engineer",
        "MLOps engineer",
        "computer vision engineer",
    ],
    "infra_roles": [
        "devops engineer",
        "cloud engineer",
        "site reliability engineer",
        "platform engineer",
        "security engineer",
        "database administrator",
    ],
}

ALL_QUERIES = [q for group in QUERY_GROUPS.values() for q in group]

# ── Rotation slots — each run covers a different slice ────────────────────────
ROTATION_SLOTS = [
    # Slot 0 — 00:00 UTC: US/GB/IN × data + software roles
    {
        "countries": ["us", "gb", "in"],
        "queries": QUERY_GROUPS["data_roles"] + QUERY_GROUPS["software_roles"],
    },
    # Slot 1 — 08:00 UTC: CA/AU/DE × ML/AI + infra roles
    {
        "countries": ["ca", "au", "de"],
        "queries": QUERY_GROUPS["ml_ai_roles"] + QUERY_GROUPS["infra_roles"],
    },
    # Slot 2 — 16:00 UTC: FR/SG/NL × mixed (one group each)
    {
        "countries": ["fr", "sg", "nl"],
        "queries": QUERY_GROUPS["data_roles"][:3] + QUERY_GROUPS["ml_ai_roles"][:3]
                 + QUERY_GROUPS["software_roles"][:3] + QUERY_GROUPS["infra_roles"][:3],
    },
]

# Keywords used to infer remote work from title / description / location
REMOTE_POSITIVE_TERMS = [
    r"\bremote\b", r"work from home", r"\bwfh\b", r"fully remote",
    r"remote.first", r"distributed team", r"anywhere in the world",
    r"work anywhere", r"home.based", r"telework",
]
REMOTE_NEGATIVE_TERMS = [
    r"on.?site only", r"no remote", r"must be in office", r"office.based",
    r"in.person only",
]


class AdzunaIngester(BaseIngester):
    source_name = "adzuna"

    def __init__(
        self,
        countries: list[str] | None = None,
        queries: list[str] | None = None,
        max_pages: int = 2,
        auto_slot: bool = True,
    ) -> None:
        """
        Args:
            countries  : list of Adzuna country codes. If None + auto_slot=True,
                         determined by current UTC hour.
            queries    : list of search strings. If None + auto_slot=True,
                         determined by current UTC hour.
            max_pages  : pages per country+query combination (50 results/page).
                         Default 2 → 12 queries × 3 countries × 2 pages = 72 req/slot
                         × 3 slots/day = 216/day (free tier: 250 ✓, run time ~2-3 min)
            auto_slot  : if True and countries/queries are not provided, picks
                         the rotation slot based on hour of day automatically.
        """
        super().__init__()
        self._validate_config()

        if countries is None and queries is None and auto_slot:
            slot = self._current_slot()
            self.countries = slot["countries"]
            self.queries   = slot["queries"]
            logger.info("adzuna_auto_slot",
                        extra={"slot": ROTATION_SLOTS.index(slot),
                               "countries": self.countries,
                               "queries": self.queries})
        else:
            self.countries = countries or ROTATION_SLOTS[0]["countries"]
            self.queries   = queries   or ROTATION_SLOTS[0]["queries"]

        self.max_pages = max_pages

    def _validate_config(self) -> None:
        if not settings.adzuna_app_id or not settings.adzuna_app_key:
            raise ValueError(
                "ADZUNA_APP_ID and ADZUNA_APP_KEY must be set in your .env. "
                "Register free at https://developer.adzuna.com"
            )

    # ── Main entry point ──────────────────────────────────────────────────────

    def run(self) -> dict:
        """
        Fetch max_pages per country+query combo (no separate remote pass —
        remote is inferred from title/description in Bronze→Silver).

        Budget: 12 queries × 3 countries × 2 pages = 72 req/slot
                × 3 slots/day = 216/day  (free tier 250 ✓, ~2-3 min runtime)
        """
        total_records = 0
        page_num      = 1

        for country in self.countries:
            for query in self.queries:
                records = self._fetch_combo(country, query)
                if records:
                    self.save_to_bronze(records, page=page_num)
                    total_records += len(records)
                    page_num += 1
                time.sleep(0.4)  # polite pause between combos

        summary = {
            "source":        self.source_name,
            "total_records": total_records,
            "countries":     self.countries,
            "queries":       self.queries,
        }
        logger.info("adzuna_run_complete", extra=summary)
        return summary

    # ── Per-combo fetch ───────────────────────────────────────────────────────

    def _fetch_combo(self, country: str, query: str) -> list[dict]:
        """Fetch up to max_pages for one country + query combination."""
        all_records: list[dict] = []

        for page in range(1, self.max_pages + 1):
            try:
                records = self._fetch_page(country, query, page)
                if not records:
                    break
                all_records.extend(records)
                logger.info("adzuna_page_ok",
                            extra={"country": country, "query": query,
                                   "page": page, "records": len(records)})
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 429:
                    wait = int(exc.response.headers.get("Retry-After", 60))
                    logger.warning("adzuna_rate_limited", extra={"wait_s": wait})
                    time.sleep(wait)
                else:
                    logger.error("adzuna_http_error",
                                 extra={"status": exc.response.status_code,
                                        "country": country, "query": query})
                break
            except Exception as exc:
                logger.error("adzuna_fetch_error",
                             extra={"country": country, "query": query, "error": str(exc)})
                break

        return all_records

    def _fetch_page(self, country: str, query: str, page: int) -> list[dict]:
        url = f"{ADZUNA_BASE_URL}/{country}/search/{page}"
        params: dict = {
            "app_id":           settings.adzuna_app_id,
            "app_key":          settings.adzuna_app_key,
            "results_per_page": RESULTS_PER_PAGE,
            "what":             query,
            "content-type":     "application/json",
        }

        response = self.client.get(url, params=params)
        response.raise_for_status()
        results = response.json().get("results", [])

        return [self._normalise(job, country) for job in results]

    # ── Normalisation ─────────────────────────────────────────────────────────

    def _normalise(self, job: dict, country: str) -> dict:
        title       = job.get("title", "")
        description = job.get("description", "")
        location    = job.get("location", {}).get("display_name", "")

        return {
            "source":              self.source_name,
            "external_id":         str(job.get("id", "")),
            "title":               title,
            "company":             job.get("company", {}).get("display_name", ""),
            "location":            location,
            "location_area":       job.get("location", {}).get("area", []),
            "country":             country.upper(),
            "description":         description,
            "salary_min":          job.get("salary_min"),
            "salary_max":          job.get("salary_max"),
            "salary_is_predicted": bool(job.get("salary_is_predicted", 0)),
            "contract_type":       job.get("contract_type", ""),   # permanent | contract
            "contract_time":       job.get("contract_time", ""),   # full_time | part_time
            "category":            job.get("category", {}).get("label", ""),
            "category_tag":        job.get("category", {}).get("tag", ""),
            "remote":              _infer_remote(title, description, location),
            "url":                 job.get("redirect_url", ""),
            "posted_at":           job.get("created", ""),
        }

    # ── Slot rotation helper ──────────────────────────────────────────────────

    @staticmethod
    def _current_slot() -> dict:
        """Return the rotation slot for the current UTC hour."""
        hour = datetime.now(timezone.utc).hour
        if hour < 8:
            return ROTATION_SLOTS[0]
        elif hour < 16:
            return ROTATION_SLOTS[1]
        else:
            return ROTATION_SLOTS[2]

    # Not used — run() is fully overridden
    def fetch_page(self, page: int) -> list[dict]:
        return []


# ── Remote inference ──────────────────────────────────────────────────────────

def _infer_remote(title: str, description: str, location: str) -> bool | None:
    """
    Infer whether a job is remote from keyword matching in title, description, location.
    Returns True / False / None (unknown).
    """
    text = f"{title} {description} {location}".lower()

    for pattern in REMOTE_NEGATIVE_TERMS:
        if re.search(pattern, text):
            return False

    for pattern in REMOTE_POSITIVE_TERMS:
        if re.search(pattern, text):
            return True

    return None
