"""
Centralized configuration, loaded from environment variables (.env).

Nothing in this project should read os.environ directly anywhere else —
every setting flows through this one module so there's a single place
to see everything that's configurable.
"""
import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def _normalize_host(host: str) -> str:
    """Accept a Databricks host with or without a scheme/trailing slash
    and normalize it to https://<host>, no trailing slash."""
    host = host.strip().rstrip("/")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    return host


@dataclass(frozen=True)
class Settings:
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    http_timeout_seconds: int = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
    max_retries: int = int(os.getenv("HTTP_MAX_RETRIES", "4"))

    databricks_host: str = _normalize_host(os.getenv("DATABRICKS_HOST", ""))
    databricks_token: str = os.getenv("DATABRICKS_TOKEN", "")
    databricks_catalog: str = os.getenv("DATABRICKS_CATALOG", "jobpulse")
    databricks_schema: str = os.getenv("DATABRICKS_SCHEMA", "raw_landing")
    databricks_volume: str = os.getenv("DATABRICKS_VOLUME", "landing")

    @property
    def volume_root(self) -> str:
        """Base Volume path, e.g. /Volumes/jobpulse/raw_landing/landing"""
        return f"/Volumes/{self.databricks_catalog}/{self.databricks_schema}/{self.databricks_volume}"


settings = Settings()
