"""
Ingestion entrypoint: pulls every configured source once and lands
each as a timestamped raw JSON file in a Databricks Unity Catalog
Volume, under raw/<source>/<date>/<filename>.json.

Why a Volume and not local disk? Databricks compute (where Bronze
ingestion runs from Stage 6 onward) is cloud-hosted and can't reach a
laptop. A Unity Catalog Volume is cloud storage, free under Databricks
Free Edition, and directly readable by Spark on Databricks via a
plain filesystem-style path. See Stage 3 notes for the full reasoning.
"""
import json
import logging
from datetime import datetime, timezone

from src.ingestion import arbeitnow, remoteok, remotive
from src.ingestion.base import SourceFetchError
from src.utils import databricks_files
from src.utils.config import settings
from src.utils.logging_setup import configure_logging

logger = logging.getLogger(__name__)

SOURCE_FETCHERS = {
    "remotive": remotive.fetch_all_jobs,
    "arbeitnow": arbeitnow.fetch_all_jobs,
    "remoteok": remoteok.fetch_all_jobs,
}

# Per-source minimum interval between pulls, in hours.
# Remotive's docs ask consumers not to poll more than ~4x/day.
MIN_PULL_INTERVAL_HOURS = {
    "remotive": 4.0,
}


def _source_root(source: str) -> str:
    return f"{settings.volume_root}/raw/{source}"


def _hours_since_last_pull(source: str) -> float | None:
    """Hours since the last successfully landed file for this source,
    or None if we've never landed anything for it yet."""
    latest_ms = databricks_files.latest_modified_ms(_source_root(source))
    if latest_ms is None:
        return None

    last_modified = datetime.fromtimestamp(latest_ms / 1000, tz=timezone.utc)
    elapsed = datetime.now(timezone.utc) - last_modified
    return elapsed.total_seconds() / 3600


def _should_skip(source: str) -> bool:
    """Whether this source's minimum pull interval hasn't elapsed yet."""
    min_interval = MIN_PULL_INTERVAL_HOURS.get(source, 0.0)
    if min_interval <= 0:
        return False

    hours_since = _hours_since_last_pull(source)
    if hours_since is None:
        return False  # never pulled before — nothing to guard against

    if hours_since < min_interval:
        logger.info(
            "Skipping %s: last pulled %.1fh ago, minimum interval is %.1fh",
            source, hours_since, min_interval,
        )
        return True

    return False


def land_raw(source: str, jobs: list[dict], run_ts: datetime) -> str:
    """Write one source's fetched jobs to a timestamped raw JSON file
    in the Databricks Volume. Returns the volume path written to."""
    date_partition = run_ts.strftime("%Y-%m-%d")
    filename = f"{source}_{run_ts.strftime('%Y%m%dT%H%M%SZ')}.json"
    volume_path = f"{_source_root(source)}/{date_partition}/{filename}"

    payload = {
        "source": source,
        "fetched_at": run_ts.isoformat(),
        "record_count": len(jobs),
        "records": jobs,
    }
    data = json.dumps(payload).encode("utf-8")
    return databricks_files.upload_bytes(volume_path, data)


def run() -> dict[str, int | str]:
    """
    Run ingestion for every source. Returns per-source record counts
    (or "skipped" if the source's minimum pull interval hadn't elapsed).

    Design choice: one source failing does NOT stop the others.
    """
    configure_logging()
    run_ts = datetime.now(timezone.utc)
    results: dict[str, int | str] = {}
    failures: list[str] = []

    for source, fetch_fn in SOURCE_FETCHERS.items():
        if _should_skip(source):
            results[source] = "skipped"
            continue

        try:
            jobs = fetch_fn()
            volume_path = land_raw(source, jobs, run_ts)
            results[source] = len(jobs)
            logger.info("Landed %s -> %s (%d records)", source, volume_path, len(jobs))
        except SourceFetchError as exc:
            logger.error("Ingestion failed for source=%s: %s", source, exc)
            failures.append(source)
            results[source] = 0

    logger.info("Ingestion run complete. Results: %s", results)
    if failures:
        logger.warning("Sources that failed this run: %s", failures)

    return results


if __name__ == "__main__":
    run()
