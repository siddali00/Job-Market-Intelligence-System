"""
Register flows for Prefect Cloud *managed* workers.

``Deployment.build_from_flow(...).apply()`` stores your *local* machine path
(e.g. ``C:\\Projects\\...``). Managed runners execute on Prefect Linux hosts and
cannot see that path → FileNotFoundError under ``/opt/prefect/``.

This script re-registers deployments with **Git** as the code source so
managed execution can ``git clone`` your repo.

Prerequisites
-------------
1. Push this project to GitHub (or GitLab) — the default branch must contain
   ``orchestration/flows.py``.
2. ``prefect cloud login`` (same workspace as ``jm-process``).
3. Work pool ``jm-process`` of type ``prefect:managed`` already exists.

Environment (set before running)
--------------------------------
``PREFECT_GIT_REPOSITORY`` — HTTPS clone URL, required. Examples::

  https://github.com/YOUR_ORG/Job-Market-Intelligence-System.git

Private repo: embed a PAT (rotate if leaked)::

  https://YOUR_TOKEN@github.com/YOUR_ORG/your-repo.git

``PREFECT_GIT_BRANCH`` — optional, default ``main``.

``PREFECT_WORK_POOL`` — optional, default ``jm-process``.

After deploy
------------
Managed jobs do **not** load your laptop ``.env``. In Prefect Cloud, open each
deployment → configure **environment variables** or **secrets** for at least
``DATABASE_URL``, ``ADZUNA_APP_ID``, ``ADZUNA_APP_KEY``, ``KAGGLE_USERNAME``,
``KAGGLE_KEY``, and any other vars your flows need. Your RDS security group must
allow Prefect’s egress to Postgres if you use Managed (or use 0.0.0.0/5432 only
for dev).

Run::

  set PREFECT_GIT_REPOSITORY=https://github.com/ORG/repo.git
  python -m orchestration.deploy_prefect_managed

Then trigger **one-time-seed** once from the UI for historical Kaggle load, or
wait for **scheduled** ``full_pipeline`` cron.

**Job variables:** ``pip_packages`` comes from ``requirements.txt`` but **never**
re-installs **prefect** / **griffe** (breaks Managed workers). PySpark needs Java
on the worker. See: https://docs.prefect.io/latest/guides/managed-execution/
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _managed_pip_packages() -> list[str]:
    """
    Lines from ``requirements.txt`` for Managed ``pip_packages``.

    **Omits** ``prefect`` and ``griffe``: the Managed image already bundles
    Prefect; reinstalling pinned versions often makes the worker exit with code 1
    before any task logs appear.
    """
    skip_roots = {"prefect", "griffe"}

    def _root_pkg(spec: str) -> str:
        s = spec.split("#", 1)[0].strip()
        s = s.split("[", 1)[0].strip()
        for sep in ("===", "==", ">=", "<=", "~=", "!=", "<", ">"):
            if sep in s:
                s = s.split(sep, 1)[0].strip()
                break
        return s.lower()

    req = Path(__file__).resolve().parent.parent / "requirements.txt"
    if not req.is_file():
        return ["pandas", "sqlalchemy", "psycopg2-binary", "python-dotenv", "kaggle"]
    out: list[str] = []
    for raw in req.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if _root_pkg(line) in skip_roots:
            continue
        out.append(line)
    return out


def _require_git_url() -> tuple[str, str, str]:
    repo = os.environ.get("PREFECT_GIT_REPOSITORY", "").strip()
    if not repo:
        print(
            "ERROR: Set PREFECT_GIT_REPOSITORY to your Git clone URL "
            "(HTTPS). Example: https://github.com/org/repo.git",
            file=sys.stderr,
        )
        sys.exit(1)
    branch = (os.environ.get("PREFECT_GIT_BRANCH") or "main").strip() or "main"
    pool = (os.environ.get("PREFECT_WORK_POOL") or "jm-process").strip()
    return repo, branch, pool


def main() -> None:
    repo, branch, pool = _require_git_url()

    from prefect.runner.storage import GitRepository

    from orchestration.flows import full_pipeline, seed_historical_data

    storage = GitRepository(url=repo, branch=branch)
    jv = {"pip_packages": _managed_pip_packages()}

    full_pipeline.from_source(
        source=storage,
        entrypoint="orchestration/flows.py:full_pipeline",
    ).deploy(
        name="scheduled",
        work_pool_name=pool,
        cron="0 */8 * * *",
        tags=["production"],
        description="Ingest + transform every 8h (Prefect Managed, Git source).",
        job_variables=jv,
    )

    seed_historical_data.from_source(
        source=storage,
        entrypoint="orchestration/flows.py:seed_historical_data",
    ).deploy(
        name="one-time-seed",
        work_pool_name=pool,
        tags=["setup"],
        description="Kaggle historical seed — run manually once.",
        job_variables=jv,
    )

    print(f"Deployed to pool {pool!r} from {repo!r} branch {branch!r}")
    print(f"job_variables pip_packages: {len(jv['pip_packages'])} entries (prefect/griffe omitted for Managed)")


if __name__ == "__main__":
    main()
