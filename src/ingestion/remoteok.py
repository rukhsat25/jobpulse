"""
RemoteOK source client.

Confirmed quirk (Stage 1): the raw response array's first element is
an API-terms/legal notice, not a job posting. We filter by requiring
an 'id' field rather than trusting array position — position-based
assumptions break silently the moment an API adds another metadata
element.

Confirmed (Stage 2 exercise run): RemoteOK has NO `remote` field at
all — it's a remote-jobs-only board, same as Remotive, so `is_remote`
is hardcoded true for this source downstream, not read from a field.
"""
import logging
from typing import Any

from src.ingestion.base import fetch_json

logger = logging.getLogger(__name__)

REMOTEOK_URL = "https://remoteok.com/api"
SOURCE_NAME = "remoteok"


def fetch_all_jobs() -> list[dict[str, Any]]:
    """Fetch all currently active listings from RemoteOK."""
    data = fetch_json(REMOTEOK_URL)
    jobs = [item for item in data if "id" in item]
    skipped = len(data) - len(jobs)
    if skipped:
        logger.debug("Skipped %d non-job elements from RemoteOK response", skipped)
    logger.info("Fetched %d jobs from %s", len(jobs), SOURCE_NAME)
    return jobs
