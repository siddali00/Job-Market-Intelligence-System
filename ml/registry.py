"""
MLflow model registry stubs.

Handles experiment tracking, model versioning, and loading for inference.
MLflow stores runs in data/mlruns/ locally (configured via MLFLOW_TRACKING_URI).
"""

from __future__ import annotations
from typing import Any

from config.settings import get_settings
from monitoring.logger import get_logger

logger = get_logger(__name__)
settings = get_settings()

EXPERIMENT_NAME = "job-market-salary-prediction"


def log_model(pipeline: Any, model_type: str, metrics: dict, feature_names: list[str]) -> str:
    """
    Log a trained sklearn pipeline to MLflow.
    Returns the run_id (used as model_version for inference).
    """
    import mlflow
    import mlflow.sklearn

    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    mlflow.set_experiment(EXPERIMENT_NAME)

    with mlflow.start_run() as run:
        mlflow.log_param("model_type", model_type)
        mlflow.log_param("feature_count", len(feature_names))
        for metric_name, value in metrics.items():
            if isinstance(value, (int, float)):
                mlflow.log_metric(metric_name, value)
        mlflow.sklearn.log_model(pipeline, artifact_path="model", registered_model_name="salary_predictor")
        run_id = run.info.run_id

    logger.info("mlflow_run_logged", extra={"run_id": run_id, "metrics": metrics})
    return run_id


def load_model(run_id: str | None = None) -> Any | None:
    """
    Load the latest registered model (or a specific run_id) from MLflow.
    Returns None if no model has been trained yet.
    """
    import mlflow
    import mlflow.sklearn
    from mlflow.exceptions import MlflowException

    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)

    try:
        if run_id:
            model_uri = f"runs:/{run_id}/model"
        else:
            model_uri = f"models:/salary_predictor/latest"
        return mlflow.sklearn.load_model(model_uri)
    except MlflowException:
        logger.warning("mlflow_model_not_found", extra={"run_id": run_id})
        return None
    except Exception as exc:
        logger.error("mlflow_load_error", extra={"error": str(exc)})
        return None
