"""
Inference stub for salary prediction.

Called by serving/api/routers/predict.py.
Returns None until a model has been trained and registered.

To activate:
  1. Run: python -m ml.train
  2. This function will automatically load the latest registered model.
"""

from __future__ import annotations
from typing import Any

from monitoring.logger import get_logger

logger = get_logger(__name__)

_cached_model = None


def predict_salary(
    title: str,
    location: str,
    skills: list[str],
    remote: bool | None = None,
) -> dict[str, Any] | None:
    """
    Predict salary range for a job posting.

    Returns dict with keys: salary_min, salary_max, confidence, model_version
    Returns None if no model is available.
    """
    global _cached_model

    if _cached_model is None:
        from ml.registry import load_model
        _cached_model = load_model()

    if _cached_model is None:
        return None

    try:
        import pandas as pd
        from ml.features import build_feature_matrix

        # Build a single-row feature vector matching training schema
        sample = pd.DataFrame([{
            "role": _normalize_title(title),
            "country": _infer_country(location),
            "remote": remote or False,
            "skill_count": len(skills),
            "skills": ",".join(skills),
        }])
        feature_cols = _cached_model.feature_names_in_ if hasattr(_cached_model, "feature_names_in_") else []
        if not feature_cols:
            return None

        # Align to training feature columns
        for col in feature_cols:
            if col not in sample.columns:
                sample[col] = 0
        sample = sample[feature_cols]

        pred = float(_cached_model.predict(sample)[0])
        margin = pred * 0.15  # ±15% as an approximation for min/max

        return {
            "salary_min": round(pred - margin, 2),
            "salary_max": round(pred + margin, 2),
            "confidence": 0.70,
            "model_version": "latest",
        }

    except Exception as exc:
        logger.error("predict_salary_error", extra={"error": str(exc)})
        return None


def _normalize_title(title: str) -> str:
    title_lower = title.lower()
    mappings = [
        (["data engineer"], "Data Engineer"),
        (["data scientist"], "Data Scientist"),
        (["data analyst"], "Data Analyst"),
        (["machine learning", "ml engineer"], "ML Engineer"),
        (["software engineer", "software developer"], "Software Engineer"),
        (["frontend", "front-end"], "Frontend Engineer"),
        (["devops", "platform"], "DevOps/Platform"),
    ]
    for keywords, label in mappings:
        if any(kw in title_lower for kw in keywords):
            return label
    return "Other"


def _infer_country(location: str) -> str:
    loc_lower = location.lower()
    if any(w in loc_lower for w in ["usa", "united states", "new york", "san francisco"]):
        return "US"
    if any(w in loc_lower for w in ["uk", "london", "united kingdom"]):
        return "GB"
    if "remote" in loc_lower:
        return "UNKNOWN"
    return "UNKNOWN"
