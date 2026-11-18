"""Configuration loaded from environment variables (12-factor style)."""
from __future__ import annotations

import os
from pathlib import Path


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
DB_PATH = Path(os.environ.get("DB_PATH", DATA_DIR / "app.db"))
STORAGE_DIR = Path(os.environ.get("STORAGE_DIR", DATA_DIR / "artifacts"))

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = _int("REDIS_PORT", 6379)
REDIS_DB = _int("REDIS_DB", 0)
STREAM_KEY = os.environ.get("STREAM_KEY", "verify:jobs")
GROUP_NAME = os.environ.get("GROUP_NAME", "verifiers")

# Hard upload limit: 2 MiB.
MAX_CONTENT_LENGTH = _int("MAX_CONTENT_LENGTH", 2 * 1024 * 1024)

# Reconciler / worker tuning (overridable for tests).
DISPATCH_INTERVAL = _int("DISPATCH_INTERVAL_SECONDS", 2)
WORKER_POLL_MS = _int("WORKER_POLL_MS", 2000)
CLAIM_MIN_IDLE_MS = _int("CLAIM_MIN_IDLE_MS", 10_000)
PROCESSING_STALE_SECONDS = _int("PROCESSING_STALE_SECONDS", 120)
MAX_ENQUEUE_ATTEMPTS = _int("MAX_ENQUEUE_ATTEMPTS", 20)
READ_FAIL_MAX_BACKOFF = _int("READ_FAIL_MAX_BACKOFF", 60)

# Test hook: pause before finalising a result (lets us kill the worker
# while a message is still pending-ack). 0 disables.
PRE_FINALIZE_DELAY = _int("PRE_FINALIZE_DELAY_MS", 0)

# Test hook: pretend the stored file is unreadable when set to "1".
SIMULATE_READ_FAILURE = os.environ.get("SIMULATE_READ_FAILURE", "") == "1"


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
