from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Database ──────────────────────────────────────────────────────────────
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "jobmarket"
    postgres_user: str = "postgres"
    postgres_password: str = "changeme"
    database_url: str = "postgresql://postgres:changeme@localhost:5432/jobmarket"

    # ── Adzuna API ────────────────────────────────────────────────────────────
    adzuna_app_id: str = ""
    adzuna_app_key: str = ""
    adzuna_country: str = "us"

    # ── Kaggle ────────────────────────────────────────────────────────────────
    kaggle_username: str = ""
    kaggle_key: str = ""

    # ── Storage Paths ─────────────────────────────────────────────────────────
    bronze_storage_path: str = "./data/bronze"
    ge_reports_path: str = "./data/ge_reports"

    # ── FastAPI ───────────────────────────────────────────────────────────────
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    cors_origins: str = "http://localhost:3000"

    # ── Prefect ───────────────────────────────────────────────────────────────
    prefect_api_url: str = "http://localhost:4200/api"

    # ── MLflow ────────────────────────────────────────────────────────────────
    mlflow_tracking_uri: str = "./data/mlruns"

    @property
    def cors_origins_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",")]


@lru_cache
def get_settings() -> Settings:
    return Settings()
