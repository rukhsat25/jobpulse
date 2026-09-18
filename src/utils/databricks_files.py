"""
Thin wrapper around the Databricks Files REST API for reading/writing
files in a Unity Catalog Volume.

Why plain `requests` and not the Databricks SDK? The SDK wraps these
same REST calls with a nicer interface, but hand-rolling it here keeps
dependencies minimal and makes the actual HTTP contract visible while
we're still learning it. We'll pull in the SDK properly once we need
Databricks Connect for running Spark jobs from Stage 5 onward.
"""
from __future__ import annotations

import logging
from typing import Any

import requests

from src.utils.config import settings

logger = logging.getLogger(__name__)


def _headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {settings.databricks_token}"}
    if extra:
        headers.update(extra)
    return headers


def ensure_directory(volume_path: str) -> None:
    """
    Create a directory (and any missing parents) in the Volume.
    Documented as idempotent by Databricks — succeeds silently if the
    directory already exists.
    """
    url = f"{settings.databricks_host}/api/2.0/fs/directories{volume_path}"
    resp = requests.put(url, headers=_headers(), timeout=30)
    resp.raise_for_status()


def upload_bytes(volume_path: str, data: bytes, overwrite: bool = True) -> str:
    """
    Upload raw bytes to a file path inside a Unity Catalog Volume.

    We explicitly create the parent directory first rather than
    assuming the file PUT auto-creates it — the docs only guarantee
    mkdir -p behavior on the directories endpoint itself, so we don't
    rely on unstated behavior for the file endpoint.

    Returns the volume path written to.
    """
    parent = volume_path.rsplit("/", 1)[0]
    ensure_directory(parent)

    url = f"{settings.databricks_host}/api/2.0/fs/files{volume_path}"
    resp = requests.put(
        url,
        headers=_headers({"Content-Type": "application/octet-stream"}),
        params={"overwrite": str(overwrite).lower()},
        data=data,
        timeout=60,
    )
    resp.raise_for_status()
    logger.debug("Uploaded %d bytes to %s", len(data), volume_path)
    return volume_path


def list_directory(volume_path: str) -> list[dict[str, Any]]:
    """
    List immediate contents of a directory in the Volume.
    Returns [] if the directory doesn't exist yet — that's not an
    error for us, it just means nothing has landed there yet.
    """
    url = f"{settings.databricks_host}/api/2.0/fs/directories{volume_path}"
    resp = requests.get(url, headers=_headers(), timeout=30)
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    return resp.json().get("contents", [])


def latest_modified_ms(source_root: str) -> int | None:
    """
    Find the most recent last_modified timestamp (ms since epoch) among
    files landed under source_root/<date>/ subdirectories.

    Two-level walk: list date-partition subdirectories, take the
    lexicographically latest (safe because we use YYYY-MM-DD), then
    list files within it and take the max last_modified. Returns None
    if nothing has ever landed under source_root.
    """
    date_dirs = [e for e in list_directory(source_root) if e.get("is_directory")]
    if not date_dirs:
        return None

    latest_date_dir = max(date_dirs, key=lambda e: e["name"])
    files = list_directory(latest_date_dir["path"])
    file_entries = [f for f in files if not f.get("is_directory")]
    if not file_entries:
        return None

    return max(f["last_modified"] for f in file_entries)
