"""
Logging setup using Loguru.
Two sinks: daily rotating file (DEBUG+) and stderr (INFO+).
A short run_id is injected into every log record for cross-log tracing.
"""
from __future__ import annotations

import sys
import uuid
from pathlib import Path

from loguru import logger


def init_logger(log_dir: Path, retention_days: int) -> str:
    """
    Configure Loguru sinks and return an 8-char run ID bound to all records.
    Call once at the start of main().
    """
    run_id = uuid.uuid4().hex[:8]

    logger.remove()  # Remove Loguru's default stderr handler

    # File sink: one file per day, retained for N days
    logger.add(
        str(log_dir / "etl_{time:YYYY-MM-DD}.log"),
        rotation="00:00",
        retention=f"{retention_days} days",
        format=(
            "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | "
            "run_id={extra[run_id]} | {name}:{function}:{line} | {message}"
        ),
        level="DEBUG",
        encoding="utf-8",
        enqueue=True,       # thread-safe async write
        backtrace=True,     # full traceback on exceptions
        diagnose=True,      # variable values in tracebacks
    )

    # Stderr sink: human-readable INFO+ for Task Scheduler capture
    logger.add(
        sys.stderr,
        format="{time:HH:mm:ss} | {level:<8} | run_id={extra[run_id]} | {message}",
        level="INFO",
        colorize=False,     # keep clean for Windows Task Scheduler logs
    )

    logger.configure(extra={"run_id": run_id})
    logger.info(f"Logger initialised. run_id={run_id}")
    return run_id
