"""
Minimal logging configuration.

This is deliberately basic — a formatted stream handler at a
configurable level. Stage 16 (Monitoring & Logging) builds real
pipeline-run metrics (row counts, freshness, success/failure records)
on top of this; this stage just makes sure every module's logger
output is readable and timestamped.
"""
import logging

from src.utils.config import settings


def configure_logging() -> None:
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
