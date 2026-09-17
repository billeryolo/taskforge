from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "TaskForge"
    env: Literal["dev", "test", "prod"] = "dev"
    log_level: str = "INFO"
    log_json: bool = True
    sentry_dsn: str | None = None

    database_url: str = "postgresql+psycopg://taskforge:taskforge@localhost:5432/taskforge"
    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str = "redis://localhost:6379/1"
    celery_result_backend: str = "redis://localhost:6379/2"

    # Where generated PDFs / processed images land. A Docker volume in compose.
    storage_dir: Path = Path("./storage")

    mail_backend: Literal["smtp", "memory"] = "smtp"
    smtp_host: str = "localhost"
    smtp_port: int = 1025
    mail_from: str = "reports@taskforge.dev"
    report_recipients: list[str] = ["ops@taskforge.dev"]
    # Celery rate limit for outbound email, per worker process. None disables it (tests).
    email_rate_limit: str | None = "120/m"

    cache_default_ttl: int = 60
    # A job still "running" after this many seconds is assumed lost and re-queued by Beat.
    stuck_job_seconds: int = 15 * 60
    artifact_retention_days: int = 7

    @field_validator("database_url")
    @classmethod
    def force_psycopg_driver(cls, v: str) -> str:
        """Managed Postgres hands out ``postgresql://``; pin the psycopg (v3) driver."""
        for prefix in ("postgres://", "postgresql://"):
            if v.startswith(prefix):
                return "postgresql+psycopg://" + v[len(prefix) :]
        return v


@lru_cache
def get_settings() -> Settings:
    return Settings()
