"""
Salary inference using the bundled champion model (`model_package/`).

Implements the mapping sequence from `model_package/example_backend_app.py`
and `model_package/BACKEND_INTEGRATION_GUIDE.md`: build a 100-column vector
aligned with `feature_names.pkl`, then call `HistGradientBoostingRegressor.predict`.

The champion artifact was pickled with numpy 2.x; loading requires numpy>=2
(see `requirements.txt`).
"""

from __future__ import annotations

import os
import pickle
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from monitoring.logger import get_logger

logger = get_logger(__name__)

MODEL_VERSION = "champion_hist_gbrt_joblib"
_RANGE_FRACTION = 0.12

# Industry values present in `feature_names.pkl` (Automotive from UI JSON is absent → map to Other).
_INDUSTRY_MODEL_KEYS = frozenset(
    col.removeprefix("ui_industry_")
    for col in (
        "ui_industry_Consulting",
        "ui_industry_Consulting And Business Services",
        "ui_industry_Education",
        "ui_industry_Education And Schools",
        "ui_industry_Energy",
        "ui_industry_Finance",
        "ui_industry_Gaming",
        "ui_industry_Healthcare",
        "ui_industry_Internet And Software",
        "ui_industry_It-Jobs",
        "ui_industry_Other",
        "ui_industry_Retail",
        "ui_industry_Tech",
        "ui_industry_Telecom",
        "ui_industry_Unknown",
    )
)


def model_package_dir() -> Path:
    override = (os.environ.get("SALARY_MODEL_PACKAGE_DIR") or "").strip()
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[1] / "model_package"


@lru_cache(maxsize=1)
def _feature_columns() -> tuple[str, ...]:
    path = model_package_dir() / "feature_names.pkl"
    with open(path, "rb") as f:
        cols = pickle.load(f)
    if not isinstance(cols, list) or len(cols) != 100:
        raise ValueError(f"expected 100 feature names in {path}, got {type(cols)} len={len(cols) if hasattr(cols, '__len__') else 'n/a'}")
    return tuple(str(c) for c in cols)


@lru_cache(maxsize=1)
def _valid_ui_titles() -> frozenset[str]:
    return frozenset(c.removeprefix("ui_title_") for c in _feature_columns() if c.startswith("ui_title_"))


@lru_cache(maxsize=1)
def _champion_model():
    import joblib

    path = model_package_dir() / "champion_salary_model.joblib"
    return joblib.load(path)


def _normalize_industry(industry: str) -> str:
    s = (industry or "").strip()
    if not s:
        return "Other"
    if s not in _INDUSTRY_MODEL_KEYS:
        return "Other"
    return s


def build_feature_vector(payload: dict[str, Any]) -> pd.DataFrame:
    """
    Map API / UI payload to a single-row DataFrame with exact column order from `feature_names.pkl`.
    """
    feature_columns = _feature_columns()
    vector: dict[str, float] = {col: 0.0 for col in feature_columns}

    yoe = float(payload.get("years_of_experience") or 0.0)
    vector["ui_experience_years"] = max(0.0, min(30.0, yoe))

    for skill in payload.get("selected_skills") or []:
        key = f"skill_{str(skill).strip()}"
        if key in vector:
            vector[key] = 1.0

    job_title = str(payload.get("job_title") or "other").strip().lower()
    title_key = f"ui_title_{job_title}"
    if title_key in vector:
        vector[title_key] = 1.0

    industry = _normalize_industry(str(payload.get("industry") or "Other"))
    ind_key = f"ui_industry_{industry}"
    if ind_key in vector:
        vector[ind_key] = 1.0

    remote_status = str(payload.get("remote_status") or "On-site").strip()
    if remote_status == "Remote" and "ui_remote_Remote" in vector:
        vector["ui_remote_Remote"] = 1.0

    return pd.DataFrame([vector], columns=list(feature_columns))


def predict_salary_champion(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Point prediction plus a symmetric display band around the annual salary estimate.

    Returns keys: salary_min, salary_max, model_version, point_estimate
    Raises FileNotFoundError if artifacts are missing; other errors propagate for logging.
    """
    pkg = model_package_dir()
    for name in ("champion_salary_model.joblib", "feature_names.pkl"):
        if not (pkg / name).is_file():
            raise FileNotFoundError(f"missing {pkg / name}")

    X = build_feature_vector(payload)
    model = _champion_model()
    pred = float(model.predict(X)[0])
    margin = abs(pred) * _RANGE_FRACTION

    return {
        "salary_min": round(pred - margin, 2),
        "salary_max": round(pred + margin, 2),
        "model_version": MODEL_VERSION,
        "point_estimate": round(pred, 2),
    }


def predict_salary(
    title: str,
    location: str,
    skills: list[str],
    remote: bool | None = None,
) -> dict[str, Any] | None:
    """
    Legacy signature: map free-text to champion payload with conservative defaults.

    Prefer calling ``predict_salary_champion`` with structured dropdown fields from the UI.
    """
    t = (title or "").strip().lower() or "other"
    remote_status = "Remote" if remote else "On-site"
    valid_titles = _valid_ui_titles()
    payload = {
        "years_of_experience": 3.0,
        "job_title": t if t in valid_titles else "other",
        "industry": "Tech",
        "remote_status": remote_status,
        "selected_skills": [s.strip().lower() for s in (skills or []) if s and str(s).strip()],
    }
    try:
        return predict_salary_champion(payload)
    except Exception as exc:
        logger.warning("predict_salary_legacy_failed", extra={"error": str(exc)})
        return None
