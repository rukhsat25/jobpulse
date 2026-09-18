"""
Shared HTTP fetching helpers used by every source client.

Centralizing retry/backoff logic here means each source client only
has to know about ITS OWN response shape, not how to survive a flaky
network — that concern is handled once, here.
"""
from __future__ import annotations

import logging
import random
import time
from typing import Any

import requests

from src.utils.config import settings

logger = logging.getLogger(__name__)

USER_AGENT = "Mozilla/5.0 (compatible; JobPulseIngestion/0.1)"

# Worth retrying: 429 (rate limited) and 5xx (server-side failure).
# NOT retried: other 4xx codes (400/401/403/404) — those mean something
# is wrong with OUR request; retrying identically just fails identically.
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class SourceFetchError(Exception):
    """Raised when a source cannot be fetched after all retries are exhausted,
    or when a non-retryable failure occurs."""


def fetch_json(
    url: str,
    *,
    params: dict[str, Any] | None = None,
    max_retries: int | None = None,
) -> Any:
    """
    GET a URL and return parsed JSON, with exponential backoff + jitter
    on retryable failures (timeouts, connection errors, 429, 5xx).

    Raises SourceFetchError if retries are exhausted or a non-retryable
    error occurs.
    """
    retries = max_retries if max_retries is not None else settings.max_retries
    headers = {"User-Agent": USER_AGENT}

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(
                url, params=params, headers=headers, timeout=settings.http_timeout_seconds
            )
        except requests.exceptions.Timeout:
            logger.warning("Timeout on attempt %d/%d for %s", attempt, retries, url)
            _backoff_sleep(attempt)
            continue
        except requests.exceptions.ConnectionError as exc:
            logger.warning(
                "Connection error on attempt %d/%d for %s: %s", attempt, retries, url, exc
            )
            _backoff_sleep(attempt)
            continue

        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError as exc:
                # Valid HTTP response, invalid JSON body. Not retryable —
                # a broken response body won't fix itself on attempt 2.
                raise SourceFetchError(f"Malformed JSON from {url}: {exc}") from exc

        if resp.status_code in RETRYABLE_STATUS_CODES:
            logger.warning(
                "Retryable status %d on attempt %d/%d for %s",
                resp.status_code, attempt, retries, url,
            )
            _backoff_sleep(attempt)
            continue

        raise SourceFetchError(
            f"Non-retryable status {resp.status_code} from {url}: {resp.text[:200]}"
        )

    raise SourceFetchError(f"Exhausted {retries} retries fetching {url}")


def _backoff_sleep(attempt: int) -> None:
    """Exponential backoff with jitter: ~1s, 2s, 4s, 8s... capped at 30s,
    plus randomness so multiple failing clients don't retry in lockstep."""
    base = min(2 ** (attempt - 1), 30)
    jitter = random.uniform(0, base * 0.25)
    time.sleep(base + jitter)
