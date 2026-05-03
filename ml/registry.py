"""
Persist trained sklearn pipelines to disk (joblib + JSON metadata).

The live salary UI uses ``model_package/`` champion weights via ``ml.predict``;
this module supports ``python -m ml.train`` without pulling in MLflow (which
conflicted with numpy>=2 for this project).
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import joblib

from config.settings import get_settings
from monitoring.logger import get_logger

logger = get_logger(__name__)
settings = get_settings()


def _artifacts_dir() -> Path:
    p = Path(settings.ml_artifacts_path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def log_model(
    pipeline: Any,
    model_type: str,
    metrics: dict,
    feature_names: list[str],
) -> str:
    """
    Save ``pipeline`` as ``{run_id}.joblib`` and metrics/features as ``{run_id}.json``.
    Returns ``run_id`` (UUID) for use as ``model_version``.
    """
    run_id = str(uuid.uuid4())
    root = _artifacts_dir()
    joblib.dump(pipeline, root / f"{run_id}.joblib")
    meta = {
        "model_type": model_type,
        "metrics": metrics,
        "feature_names": feature_names,
    }
    (root / f"{run_id}.json").write_text(
        json.dumps(meta, indent=2, default=str),
        encoding="utf-8",
    )
    logger.info("ml_artifact_saved", extra={"run_id": run_id, "model_type": model_type})
    return run_id


def load_model(run_id: str | None = None) -> Any | None:
    """
    Load a pipeline from ``ml_artifacts_path``.

    If ``run_id`` is given, load that file; otherwise load the most recently
    modified ``*.joblib`` in the directory.
    """
    root = _artifacts_dir()
    if run_id:
        path = root / f"{run_id}.joblib"
        if path.is_file():
            return joblib.load(path)
        logger.warning("ml_artifact_not_found", extra={"run_id": run_id})
        return None

    paths = sorted(root.glob("*.joblib"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not paths:
        logger.warning("ml_artifact_no_models", extra={"dir": str(root)})
        return None
    return joblib.load(paths[0])
