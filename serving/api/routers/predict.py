"""
POST /api/predict/salary  — annual salary estimate (champion model in `model_package/`)

GET /api/predict/options — UI dropdown source (`ui_dropdown_options.json`)
"""

import json
import logging
from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ml.predict import model_package_dir, predict_salary_champion

router = APIRouter()
logger = logging.getLogger(__name__)


class SalaryPredictRequest(BaseModel):
    """Structured payload aligned with `model_package/BACKEND_INTEGRATION_GUIDE.md`."""

    years_of_experience: float = Field(ge=0.0, le=30.0, default=0.0)
    job_title: str = Field(min_length=1)
    industry: str = Field(min_length=1)
    remote_status: Literal["Remote", "On-site"]
    selected_skills: list[str] = Field(default_factory=list)


class SalaryPredictResponse(BaseModel):
    predicted_salary_min: float | None
    predicted_salary_max: float | None
    predicted_point: float | None = None
    model_version: str | None
    status: str
    message: str


@router.get("/options")
def get_predict_options():
    """Expose `model_package/ui_dropdown_options.json` for dropdowns (strict categorical inputs)."""
    path = model_package_dir() / "ui_dropdown_options.json"
    if not path.is_file():
        raise HTTPException(status_code=500, detail=f"Missing options file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


@router.post("/salary", response_model=SalaryPredictResponse)
def predict_salary(request: SalaryPredictRequest):
    """
    Predict annual salary using the bundled HistGradientBoostingRegressor.

    Requires `model_package/champion_salary_model.joblib` + `feature_names.pkl`.
    The champion artifact expects **numpy >= 2** (see project `requirements.txt`).
    """
    payload = {
        "years_of_experience": request.years_of_experience,
        "job_title": request.job_title.strip().lower(),
        "industry": request.industry.strip(),
        "remote_status": request.remote_status,
        "selected_skills": [s.strip().lower() for s in request.selected_skills if s.strip()],
    }
    try:
        result = predict_salary_champion(payload)
        return SalaryPredictResponse(
            predicted_salary_min=result.get("salary_min"),
            predicted_salary_max=result.get("salary_max"),
            predicted_point=result.get("point_estimate"),
            model_version=result.get("model_version"),
            status="ok",
            message="Prediction generated successfully.",
        )
    except FileNotFoundError as exc:
        logger.warning("predict_salary_missing_artifact", extra={"error": str(exc)})
        return SalaryPredictResponse(
            predicted_salary_min=None,
            predicted_salary_max=None,
            predicted_point=None,
            model_version=None,
            status="model_unavailable",
            message=str(exc),
        )
    except Exception as exc:
        err = str(exc)
        logger.exception("predict_salary_failed", extra={"error": err})
        hint = ""
        if "BitGenerator" in err or "numpy.random" in err:
            hint = (
                " Install numpy>=2 and scikit-learn>=1.5 (see requirements.txt) so the "
                "champion joblib artifact can load."
            )
        return SalaryPredictResponse(
            predicted_salary_min=None,
            predicted_salary_max=None,
            predicted_point=None,
            model_version=None,
            status="error",
            message=err + hint,
        )
