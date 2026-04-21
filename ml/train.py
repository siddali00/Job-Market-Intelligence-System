"""
ML model training stub.

Trains a salary prediction model using the feature matrix from ml/features.py.
Model and metrics are logged to MLflow.

Usage:
    python -m ml.train --model ridge --test-size 0.2

This is a stub — implement when you have enough labelled data (salary_min/max).
The pipeline and MLflow wiring are already complete.
"""

import argparse
import sys
from typing import Any

from monitoring.logger import get_logger

logger = get_logger(__name__)

SUPPORTED_MODELS = ["ridge", "random_forest", "gradient_boosting"]


def train(model_type: str = "ridge", test_size: float = 0.2) -> dict[str, Any]:
    """
    Train a salary regression model.

    Steps:
      1. Build feature matrix from Gold tables
      2. Split train/test
      3. Fit model
      4. Evaluate (MAE, RMSE, R²)
      5. Log to MLflow
      6. Save to registry

    Returns metrics dict.
    """
    from ml.features import build_feature_matrix

    logger.info("ml_train_start", extra={"model_type": model_type})

    df = build_feature_matrix()
    if df.empty or len(df) < 100:
        logger.warning("insufficient_data", extra={"rows": len(df), "required": 100})
        return {"status": "skipped", "reason": "insufficient_data", "rows": len(df)}

    # ── Feature / target split ────────────────────────────────────────────────
    target_col = "salary_mid"
    X = df.drop(columns=[target_col])
    y = df[target_col]

    # ── Train / test split ────────────────────────────────────────────────────
    from sklearn.model_selection import train_test_split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42)

    # ── Model selection ───────────────────────────────────────────────────────
    from sklearn.linear_model import Ridge
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import mean_absolute_error, root_mean_squared_error, r2_score

    model_map = {
        "ridge": Ridge(alpha=1.0),
        "random_forest": RandomForestRegressor(n_estimators=100, random_state=42),
        "gradient_boosting": GradientBoostingRegressor(n_estimators=100, random_state=42),
    }
    if model_type not in model_map:
        raise ValueError(f"Unknown model type: {model_type}. Choose from {SUPPORTED_MODELS}")

    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("model", model_map[model_type]),
    ])

    pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_test)

    metrics = {
        "mae": round(mean_absolute_error(y_test, y_pred), 2),
        "rmse": round(root_mean_squared_error(y_test, y_pred), 2),
        "r2": round(r2_score(y_test, y_pred), 4),
        "train_size": len(X_train),
        "test_size": len(X_test),
    }
    logger.info("ml_train_metrics", extra=metrics)

    # ── MLflow logging ────────────────────────────────────────────────────────
    from ml.registry import log_model
    model_version = log_model(pipeline, model_type, metrics, feature_names=list(X.columns))
    logger.info("ml_model_registered", extra={"version": model_version})

    return {"status": "trained", "model_type": model_type, "model_version": model_version, **metrics}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Job Market salary prediction model")
    parser.add_argument("--model", default="ridge", choices=SUPPORTED_MODELS)
    parser.add_argument("--test-size", type=float, default=0.2)
    args = parser.parse_args()

    result = train(model_type=args.model, test_size=args.test_size)
    print(result)
    if result.get("status") == "skipped":
        sys.exit(1)
