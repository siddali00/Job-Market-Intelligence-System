"""
POST /api/predict/salary  — salary prediction (ML stub)

Returns a graceful "model not yet trained" response until the ml/ module
is implemented. Wire-up is already complete — just fill in ml/predict.py.
"""

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class SalaryPredictRequest(BaseModel):
    title: str
    location: str
    skills: list[str] = []
    remote: bool | None = None


class SalaryPredictResponse(BaseModel):
    predicted_salary_min: float | None
    predicted_salary_max: float | None
    confidence: float | None
    model_version: str | None
    status: str
    message: str


@router.post("/salary", response_model=SalaryPredictResponse)
def predict_salary(request: SalaryPredictRequest):
    """
    Predict salary range for a job based on title, location, and skills.

    Currently returns a stub response. To activate:
      1. Train a model using ml/train.py
      2. Register it with ml/registry.py
      3. Implement ml/predict.py predict_salary()
    """
    try:
        from ml.predict import predict_salary as ml_predict
        result = ml_predict(
            title=request.title,
            location=request.location,
            skills=request.skills,
            remote=request.remote,
        )
        if result is not None:
            return SalaryPredictResponse(
                predicted_salary_min=result.get("salary_min"),
                predicted_salary_max=result.get("salary_max"),
                confidence=result.get("confidence"),
                model_version=result.get("model_version"),
                status="ok",
                message="Prediction generated successfully.",
            )
    except ImportError:
        pass
    except Exception:
        pass

    return SalaryPredictResponse(
        predicted_salary_min=None,
        predicted_salary_max=None,
        confidence=None,
        model_version=None,
        status="model_not_trained",
        message=(
            "Predictive model has not been trained yet. "
            "Run ml/train.py after sufficient data has been collected."
        ),
    )
