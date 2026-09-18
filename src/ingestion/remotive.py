"""
Remotive source client.

Remotive returns ALL currently active remote listings in one response
(no pagination). Their docs ask integrators not to poll more than
~4x/day since the underlying data doesn't change faster than that —
we honor this by calling it once per scheduled run, not in a loop.
"""
import logging
from typing import Any

from src.ingestion.base import fetch_json

logger = logging.getLogger(__name__)

REMOTIVE_URL = "https://remotive.com/api/remote-jobs"
SOURCE_NAME = "remotive"


def fetch_all_jobs() -> list[dict[str, Any]]:
    """Fetch all currently active listings from Remotive."""
    data = fetch_json(REMOTIVE_URL)
    jobs = data.get("jobs", [])
    logger.info("Fetched %d jobs from %s", len(jobs), SOURCE_NAME)
    return jobs
