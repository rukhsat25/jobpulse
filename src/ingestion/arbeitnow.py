"""
Arbeitnow source client.

Unlike Remotive, Arbeitnow paginates (100 results/page) via a
`links.next` URL in each response. We follow that chain until it's
exhausted — this is our one source that demonstrates real pagination.
"""
import logging
from typing import Any

from src.ingestion.base import fetch_json

logger = logging.getLogger(__name__)

ARBEITNOW_URL = "https://arbeitnow.com/api/job-board-api"
SOURCE_NAME = "arbeitnow"

# Safety cap: if pagination logic has a bug, never loop forever.
MAX_PAGES = 50


def fetch_all_jobs() -> list[dict[str, Any]]:
    """Fetch all pages of currently active listings from Arbeitnow."""
    all_jobs: list[dict[str, Any]] = []
    url: str | None = ARBEITNOW_URL
    page_count = 0

    while url and page_count < MAX_PAGES:
        data = fetch_json(url)
        page_jobs = data.get("data", [])
        all_jobs.extend(page_jobs)
        page_count += 1

        links = data.get("links") or {}
        next_url = links.get("next")
        url = next_url if next_url else None

        logger.debug(
            "Arbeitnow page %d: %d jobs (running total %d)",
            page_count, len(page_jobs), len(all_jobs),
        )

    if page_count >= MAX_PAGES:
        logger.warning(
            "Hit MAX_PAGES=%d safety cap for Arbeitnow — pagination may be incomplete",
            MAX_PAGES,
        )

    logger.info("Fetched %d jobs from %s across %d pages", len(all_jobs), SOURCE_NAME, page_count)
    return all_jobs
