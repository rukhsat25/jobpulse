"""
Stage 1 exploration script.

Pulls a small sample from each job-board API and saves the raw JSON
to data/samples/ so we can inspect real response shapes before
building the ingestion pipeline in Stage 2.

This is a throwaway/inspection script, not production ingestion code
(no retries, no logging framework yet — that comes in Stage 2).
Also used to regenerate data/samples/*.json locally, which Stage 4's
local PySpark exploration script reads from.
"""
import json
import time
from pathlib import Path

import requests

SAMPLES_DIR = Path(__file__).resolve().parents[2] / "data" / "samples"
SAMPLES_DIR.mkdir(parents=True, exist_ok=True)

# RemoteOK blocks requests without a browser-like User-Agent (returns 403 otherwise)
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; JobPulseExplorer/0.1)"}


def save_sample(name: str, data) -> None:
    path = SAMPLES_DIR / f"{name}.json"
    path.write_text(json.dumps(data, indent=2))
    print(f"Saved {path} ({len(json.dumps(data))} bytes)")


def explore_remotive() -> None:
    print("\n--- Remotive ---")
    resp = requests.get(
        "https://remotive.com/api/remote-jobs",
        params={"limit": 5},
        headers=HEADERS,
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    print("Top-level keys:", list(data.keys()))
    jobs = data.get("jobs", [])
    print(f"Sample job count: {len(jobs)}")
    if jobs:
        print("First job's keys:", list(jobs[0].keys()))
    save_sample("remotive_sample", data)


def explore_arbeitnow() -> None:
    print("\n--- Arbeitnow ---")
    resp = requests.get(
        "https://arbeitnow.com/api/job-board-api",
        headers=HEADERS,
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    print("Top-level keys:", list(data.keys()))
    jobs = data.get("data", [])
    print(f"Sample job count: {len(jobs)}")
    if jobs:
        print("First job's keys:", list(jobs[0].keys()))
    save_sample("arbeitnow_sample", data)


def explore_remoteok() -> None:
    print("\n--- RemoteOK ---")
    resp = requests.get(
        "https://remoteok.com/api",
        headers=HEADERS,
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    print(f"Raw array length: {len(data)}")
    print("Element [0] (expected: legal/info notice, not a job):", data[0])
    real_jobs = [item for item in data if "id" in item]
    print(f"Actual job postings (have an 'id' field): {len(real_jobs)}")
    if real_jobs:
        print("First real job's keys:", list(real_jobs[0].keys()))
    save_sample("remoteok_sample", data)


if __name__ == "__main__":
    explore_remotive()
    time.sleep(1)
    explore_arbeitnow()
    time.sleep(1)
    explore_remoteok()
