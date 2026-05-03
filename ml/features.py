"""
Feature engineering for ML salary-prediction model — PySpark MLlib pipeline.

What this does
──────────────
  Reads Silver tables from PostgreSQL → builds a Spark DataFrame →
  applies a PySpark ML Pipeline (StringIndexer → OneHotEncoder →
  VectorAssembler → StandardScaler) → returns the feature matrix as a
  Spark DataFrame (for Spark ML training) or as a pandas DataFrame
  (for scikit-learn training).

Feature set
───────────
  Categorical (one-hot encoded):
    role     (title_normalized)   — Data Engineer, ML Engineer, …
    country                       — US, GB, IN, …
    source                        — adzuna, remotive, kaggle
  Numeric:
    remote           (bool → 0/1)
    skill_count      (#skills attached to the job)
    top-30 skill flags  (skill_<name> = 0/1)
  Target:
    salary_mid  =  (salary_min + salary_max) / 2

Usage
─────
  from ml.features import build_feature_spark, build_feature_pandas

  # For PySpark ML training:
  spark_df, spark, label_col = build_feature_spark()

  # For scikit-learn training (falls back automatically if Spark unavailable):
  df = build_feature_pandas()
  X  = df.drop(columns=["salary_mid"])
  y  = df["salary_mid"]
"""

from __future__ import annotations

from monitoring.logger import get_logger

logger = get_logger(__name__)


# ── PySpark MLlib path ────────────────────────────────────────────────────────

def build_feature_spark(min_salary: float = 10_000, top_n_skills: int = 30):
    """
    Build a PySpark DataFrame with a 'features' VectorAssembler column
    and a 'salary_mid' label column, ready for Spark ML estimators.

    Returns (assembled_df, spark_session, label_col="salary_mid").
    Raises if PySpark / Java is unavailable (caller should fall back).
    """
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import (
        StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler,
        Imputer,
    )

    spark = _get_spark()

    # ── Load Silver data ──────────────────────────────────────────────────────
    raw_df, skill_counts, top_skills = _load_silver_spark(spark, F, min_salary, top_n_skills)

    if raw_df.count() == 0:
        logger.warning("features_no_data", extra={"min_salary": min_salary})
        return raw_df, spark, "salary_mid"

    # ── Build skill flag columns ──────────────────────────────────────────────
    for skill in top_skills:
        col_name = f"skill_{skill.replace(' ', '_').replace('-', '_')}"
        raw_df = raw_df.withColumn(
            col_name,
            F.when(F.array_contains(F.col("skills_array"), skill), 1.0).otherwise(0.0),
        )

    skill_flag_cols = [
        f"skill_{s.replace(' ', '_').replace('-', '_')}" for s in top_skills
    ]

    # ── Categorical encoding: StringIndexer → OneHotEncoder ──────────────────
    cat_cols    = ["role", "country", "source"]
    idx_cols    = [f"{c}_idx" for c in cat_cols]
    enc_cols    = [f"{c}_enc" for c in cat_cols]

    indexers = [
        StringIndexer(inputCol=c, outputCol=i, handleInvalid="keep")
        for c, i in zip(cat_cols, idx_cols)
    ]
    encoder = OneHotEncoder(inputCols=idx_cols, outputCols=enc_cols, handleInvalid="keep")

    # ── Numeric columns ───────────────────────────────────────────────────────
    numeric_cols = ["remote_int", "skill_count"] + skill_flag_cols

    # Impute missing numerics with median
    imputer = Imputer(
        inputCols=numeric_cols,
        outputCols=[f"{c}_imp" for c in numeric_cols],
        strategy="median",
    )
    imputed_cols = [f"{c}_imp" for c in numeric_cols]

    # ── Assemble all features into one vector ─────────────────────────────────
    assembler = VectorAssembler(
        inputCols=enc_cols + imputed_cols,
        outputCol="raw_features",
        handleInvalid="keep",
    )

    # ── Scale (StandardScaler — zero mean=False since OHE columns are sparse) ─
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withMean=False,
        withStd=True,
    )

    pipeline = Pipeline(stages=indexers + [encoder, imputer, assembler, scaler])

    model    = pipeline.fit(raw_df)
    final_df = model.transform(raw_df)

    logger.info("features_spark_built", extra={
        "rows": final_df.count(),
        "cat_cols": cat_cols,
        "numeric_cols": numeric_cols[:5],
        "top_skills": len(top_skills),
    })

    return final_df.select("features", "salary_mid"), spark, "salary_mid"


def _load_silver_spark(spark, F, min_salary: float, top_n_skills: int):
    """
    Pull Silver data from PostgreSQL via SQLAlchemy, create Spark DataFrames.
    Returns (base_df, skill_counts_df, top_skills_list).
    """
    from collections import Counter
    from sqlalchemy import text

    db = _db()
    try:
        job_rows = db.execute(text("""
            SELECT
                j.id,
                COALESCE(j.title_normalized, 'Other') AS role,
                COALESCE(j.country, 'UNKNOWN')         AS country,
                j.source,
                CASE WHEN j.remote = TRUE THEN 1.0
                     WHEN j.remote = FALSE THEN 0.0
                     ELSE NULL END                     AS remote_int,
                j.salary_min,
                j.salary_max,
                (j.salary_min + j.salary_max) / 2.0   AS salary_mid
            FROM jobs j
            WHERE j.salary_min IS NOT NULL
              AND j.salary_max IS NOT NULL
              AND (j.salary_min + j.salary_max) / 2.0 >= :min_salary
              AND j.salary_currency = 'USD'
        """), {"min_salary": min_salary}).fetchall()

        skill_rows = db.execute(text("""
            SELECT js.job_id, s.name AS skill_name
            FROM job_skills js JOIN skills s ON s.id = js.skill_id
        """)).fetchall()
    finally:
        db.close()

    # Build skill lookup: job_id → [skills]
    from collections import defaultdict
    job_skills: dict[int, list[str]] = defaultdict(list)
    all_skills: list[str] = []
    for r in skill_rows:
        job_skills[r[0]].append(r[1])
        all_skills.append(r[1])

    top_skills = [s for s, _ in Counter(all_skills).most_common(top_n_skills)]

    # Enrich job rows with skill_count and skills_array
    enriched = []
    for r in job_rows:
        d = dict(r._mapping)
        skills = job_skills.get(d["id"], [])
        d["skill_count"] = float(len(skills))
        d["skills_array"] = skills
        enriched.append(d)

    df = spark.createDataFrame(enriched) if enriched else spark.createDataFrame([])
    return df, None, top_skills


# ── Pandas fallback path ──────────────────────────────────────────────────────

def build_feature_pandas(min_salary: float = 10_000, top_n_skills: int = 30):
    """
    Build a pandas feature matrix.
    Called automatically if PySpark is unavailable, or directly by scikit-learn callers.

    Returns a DataFrame with columns:
      salary_mid, remote, skill_count, role_*, country_*, source_*, skill_*
    """
    import pandas as pd
    from collections import Counter
    from sqlalchemy import text

    db = _db()
    try:
        job_rows = db.execute(text("""
            SELECT
                j.id,
                COALESCE(j.title_normalized, 'Other') AS role,
                COALESCE(j.country, 'UNKNOWN')        AS country,
                j.source,
                COALESCE(j.remote, FALSE)             AS remote,
                j.salary_min, j.salary_max,
                (j.salary_min + j.salary_max) / 2.0  AS salary_mid
            FROM jobs j
            WHERE j.salary_min IS NOT NULL
              AND j.salary_max IS NOT NULL
              AND (j.salary_min + j.salary_max) / 2.0 >= :min_salary
        """), {"min_salary": min_salary}).fetchall()

        skill_rows = db.execute(text("""
            SELECT js.job_id, s.name AS skill_name
            FROM job_skills js JOIN skills s ON s.id = js.skill_id
        """)).fetchall()
    finally:
        db.close()

    if not job_rows:
        logger.warning("features_no_data_pandas")
        return pd.DataFrame()

    df = pd.DataFrame([dict(r._mapping) for r in job_rows])

    # Skills
    from collections import defaultdict
    job_skills: dict = defaultdict(list)
    all_skills: list = []
    for r in skill_rows:
        job_skills[r[0]].append(r[1])
        all_skills.append(r[1])

    top_skills = [s for s, _ in Counter(all_skills).most_common(top_n_skills)]
    df["skill_count"] = df["id"].map(lambda i: len(job_skills.get(i, [])))
    df["skills_str"]  = df["id"].map(lambda i: ",".join(job_skills.get(i, [])))

    for skill in top_skills:
        col = f"skill_{skill.replace(' ','_').replace('-','_')}"
        df[col] = df["skills_str"].str.contains(r"\b" + skill.replace(" ", r"\s") + r"\b",
                                                  case=False, regex=True).astype(int)

    # One-hot encode
    role_dummies    = pd.get_dummies(df["role"],    prefix="role")
    country_dummies = pd.get_dummies(df["country"], prefix="country")
    source_dummies  = pd.get_dummies(df["source"],  prefix="source")

    skill_cols = [c for c in df.columns if c.startswith("skill_")]
    features   = pd.concat([
        df[["salary_mid", "remote", "skill_count"] + skill_cols].reset_index(drop=True),
        role_dummies.reset_index(drop=True),
        country_dummies.reset_index(drop=True),
        source_dummies.reset_index(drop=True),
    ], axis=1)

    logger.info("features_pandas_built", extra={"rows": len(features), "cols": len(features.columns)})
    return features


# ── Unified entry point ───────────────────────────────────────────────────────

def build_feature_matrix(min_salary: float = 10_000, top_n_skills: int = 30):
    """
    Try PySpark MLlib first; fall back to pandas automatically.
    Returns a pandas DataFrame in both cases (for sklearn compatibility).
    """
    try:
        spark_df, spark, _ = build_feature_spark(min_salary, top_n_skills)
        pdf = spark_df.toPandas()
        logger.info("features_built_via_spark", extra={"rows": len(pdf)})
        return pdf
    except Exception as exc:
        logger.warning("features_spark_fallback", extra={"error": str(exc)})
        return build_feature_pandas(min_salary, top_n_skills)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _db():
    from storage.db import SessionLocal
    return SessionLocal()


def _get_spark():
    from pyspark.sql import SparkSession
    import logging

    spark = (
        SparkSession.builder
        .appName("JobMarketIntelligence-MLFeatures")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    return spark
