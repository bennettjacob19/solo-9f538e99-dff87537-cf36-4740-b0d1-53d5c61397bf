"""SQLite persistence layer.

The database is the single source of truth for artifact state. All state
transitions use conditional UPDATEs inside ``BEGIN IMMEDIATE`` transactions so
that redelivery of a job (at-least-once semantics of Redis Streams) can never
produce a second conclusion or overwrite an already confirmed result.
"""
from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Iterable, Optional

from . import config

# Artifact lifecycle:
#   pending    -> processing -> passed / failed
#   processing -> pending     (reconciler reset when the stream message is gone)
#   pending stays pending while enqueue keeps failing (retried forever)
ST_PENDING = "pending"
ST_PROCESSING = "processing"
ST_PASSED = "passed"
ST_FAILED = "failed"
TERMINAL_STATES = (ST_PASSED, ST_FAILED)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS artifacts (
    id               TEXT PRIMARY KEY,
    original_name    TEXT NOT NULL,
    stored_path      TEXT NOT NULL,
    expected_sha256  TEXT NOT NULL,
    actual_sha256    TEXT,
    status           TEXT NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    last_error       TEXT,
    stream_id        TEXT,
    worker_id        TEXT,
    next_enqueue_at  REAL NOT NULL DEFAULT 0,
    enqueue_attempts INTEGER NOT NULL DEFAULT 0,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_artifacts_status ON artifacts(status);
"""

_local = threading.local()
_init_lock = threading.Lock()
_initialized = False


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(
        config.DB_PATH,
        timeout=30.0,
        isolation_level=None,  # we manage transactions explicitly
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def get_conn() -> sqlite3.Connection:
    """One connection per thread; the dispatcher and each worker are threads."""
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = _connect()
        _local.conn = conn
    return conn


def init_db() -> None:
    global _initialized
    with _init_lock:
        if _initialized:
            return
        config.ensure_dirs()
        conn = get_conn()
        conn.executescript(_SCHEMA)
        _initialized = True


def create_artifact(original_name: str, stored_path: Path,
                    expected_sha256: str) -> str:
    """Persist an accepted upload in ``pending`` state.

    The row is committed *before* anything touches Redis, so an enqueue
    failure (or a web-process crash) leaves a durable pending record that the
    reconciler keeps dispatching.
    """
    init_db()
    artifact_id = uuid.uuid4().hex
    now = time.time()
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO artifacts (id, original_name, stored_path, expected_sha256,
                               status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (artifact_id, original_name, str(stored_path), expected_sha256,
         ST_PENDING, now, now),
    )
    return artifact_id


def list_by_status(status: str) -> list[sqlite3.Row]:
    init_db()
    conn = get_conn()
    return list(conn.execute(
        "SELECT * FROM artifacts WHERE status = ? ORDER BY created_at",
        (status,),
    ))


def get_artifact(artifact_id: str) -> Optional[sqlite3.Row]:
    init_db()
    conn = get_conn()
    return conn.execute(
        "SELECT * FROM artifacts WHERE id = ?", (artifact_id,)
    ).fetchone()


def fetch_due_pending(limit: int = 100) -> list[sqlite3.Row]:
    """Pending rows that still need a stream message (never enqueued, or a
    previous enqueue attempt failed and the backoff has elapsed)."""
    init_db()
    conn = get_conn()
    now = time.time()
    return list(conn.execute(
        """
        SELECT * FROM artifacts
        WHERE status = ? AND stream_id IS NULL AND next_enqueue_at <= ?
        ORDER BY created_at
        LIMIT ?
        """,
        (ST_PENDING, now, limit),
    ))


def attach_stream_id(artifact_id: str, stream_id: str) -> bool:
    """Atomically bind a freshly produced stream message to one pending row.

    Returns False if the row is no longer eligible (e.g. concurrently
    handled), in which case the caller deletes the orphan stream message.
    """
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT status, stream_id FROM artifacts WHERE id = ?",
            (artifact_id,),
        ).fetchone()
        if row is None or row["status"] != ST_PENDING or row["stream_id"] is not None:
            conn.execute("COMMIT")
            return False
        now = time.time()
        conn.execute(
            """
            UPDATE artifacts
            SET stream_id = ?, enqueue_attempts = enqueue_attempts + 1,
                updated_at = ?
            WHERE id = ?
            """,
            (stream_id, now, artifact_id),
        )
        conn.execute("COMMIT")
        return True
    except Exception:
        conn.execute("ROLLBACK")
        raise


def record_enqueue_failure(artifact_id: str, error: str) -> None:
    """Back off (capped exponential) so a broken Redis is retried forever
    without hot-looping. The row stays pending and unbound."""
    conn = get_conn()
    now = time.time()
    row = conn.execute(
        "SELECT enqueue_attempts FROM artifacts WHERE id = ?", (artifact_id,)
    ).fetchone()
    attempts = (row["enqueue_attempts"] if row else 0) + 1
    backoff = min(2 ** min(attempts, 6), config.READ_FAIL_MAX_BACKOFF)
    conn.execute(
        """
        UPDATE artifacts
        SET enqueue_attempts = ?, last_error = ?, next_enqueue_at = ?,
            updated_at = ?
        WHERE id = ? AND status = ? AND stream_id IS NULL
        """,
        (attempts, error[:500], now + backoff, now, artifact_id, ST_PENDING),
    )


def reset_stuck_processing(ids: Iterable[str]) -> int:
    """Reconciler: rows stuck in ``processing`` whose stream message no
    longer exists go back to ``pending`` with no stream binding, so they are
    re-enqueued and redelivered. Only non-terminal rows are touched."""
    ids = list(ids)
    if not ids:
        return 0
    conn = get_conn()
    now = time.time()
    placeholders = ",".join("?" for _ in ids)
    cur = conn.execute(
        f"""
        UPDATE artifacts
        SET status = ?, stream_id = NULL, worker_id = NULL,
            last_error = 'stream message lost, re-dispatching',
            updated_at = ?
        WHERE id IN ({placeholders}) AND status = ?
        """,
        (ST_PENDING, now, *ids, ST_PROCESSING),
    )
    return cur.rowcount


def claim_for_processing(artifact_id: str, worker_id: str) -> Optional[sqlite3.Row]:
    """Worker: move one row to ``processing`` iff it is still ``pending``.

    This is the first idempotency gate: a redelivered message for a row that
    another worker already claimed (or that already has a conclusion) simply
    returns None and the message gets ACKed as a harmless duplicate.
    """
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM artifacts WHERE id = ?", (artifact_id,)
        ).fetchone()
        if row is None or row["status"] != ST_PENDING:
            conn.execute("COMMIT")
            return None
        now = time.time()
        conn.execute(
            """
            UPDATE artifacts
            SET status = ?, worker_id = ?, attempts = attempts + 1,
                updated_at = ?
            WHERE id = ? AND status = ?
            """,
            (ST_PROCESSING, worker_id, now, artifact_id, ST_PENDING),
        )
        conn.execute("COMMIT")
        return conn.execute(
            "SELECT * FROM artifacts WHERE id = ?", (artifact_id,)
        ).fetchone()
    except Exception:
        conn.execute("ROLLBACK")
        raise


def takeover_for_processing(artifact_id: str, worker_id: str,
                            stream_id: str) -> Optional[sqlite3.Row]:
    """Claim a message obtained via XAUTOCLAIM (its original consumer died or
    stalled while the message was still pending-ack).

    Rules:
      * terminal rows            -> None (duplicate; caller ACKs, never rewrites)
      * pending rows             -> normal claim (reconciler may have reset it
                                    and produced a newer message; only accept
                                    when this message is the one bound to it)
      * processing rows          -> accepted only when the row's bound stream
                                    message is exactly the claimed one, proving
                                    this is the same job being continued, not a
                                    stale message from an older dispatch cycle
    """
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM artifacts WHERE id = ?", (artifact_id,)
        ).fetchone()
        if row is None or row["status"] in TERMINAL_STATES:
            conn.execute("COMMIT")
            return None
        if row["status"] == ST_PENDING:
            if row["stream_id"] != stream_id:
                # A newer dispatch superseded this claimed message.
                conn.execute("COMMIT")
                return None
        elif row["status"] == ST_PROCESSING:
            if row["stream_id"] != stream_id:
                # Stale message from a previous dispatch cycle; the row now
                # belongs to another live message. Do not touch it.
                conn.execute("COMMIT")
                return None
        else:
            conn.execute("COMMIT")
            return None
        now = time.time()
        conn.execute(
            """
            UPDATE artifacts
            SET status = ?, worker_id = ?, attempts = attempts + 1,
                updated_at = ?
            WHERE id = ?
            """,
            (ST_PROCESSING, worker_id, now, artifact_id),
        )
        conn.execute("COMMIT")
        return conn.execute(
            "SELECT * FROM artifacts WHERE id = ?", (artifact_id,)
        ).fetchone()
    except Exception:
        conn.execute("ROLLBACK")
        raise


def finalize_artifact(artifact_id: str, worker_id: str, status: str,
                      actual_sha256: Optional[str], error: Optional[str]) -> bool:
    """Write the conclusion iff the row is still ours (``processing``).

    Second idempotency gate: confirmed ``passed``/``failed`` rows can never be
    rewritten by a duplicate delivery or a takeover, because their status no
    longer matches the WHERE clause.
    """
    assert status in TERMINAL_STATES
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT status FROM artifacts WHERE id = ?", (artifact_id,)
        ).fetchone()
        if row is None or row["status"] != ST_PROCESSING:
            conn.execute("COMMIT")
            return False
        now = time.time()
        conn.execute(
            """
            UPDATE artifacts
            SET status = ?, actual_sha256 = ?, last_error = ?,
                worker_id = ?, updated_at = ?
            WHERE id = ? AND status = ?
            """,
            (status, actual_sha256, error, worker_id, now,
             artifact_id, ST_PROCESSING),
        )
        conn.execute("COMMIT")
        return True
    except Exception:
        conn.execute("ROLLBACK")
        raise


def record_read_failure(artifact_id: str, worker_id: str, error: str) -> bool:
    """A file read/hash failure is *not* a digest mismatch: the row returns to
    ``pending`` with capped exponential backoff and is retried.

    The stream binding is kept until the worker ACKs the old message (see
    :func:`detach_after_ack`); this ordering guarantees there is never a
    window where both the old PEL entry and a freshly published retry message
    exist for the same job.
    """
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT status, attempts FROM artifacts WHERE id = ?",
            (artifact_id,),
        ).fetchone()
        if row is None or row["status"] != ST_PROCESSING:
            conn.execute("COMMIT")
            return False
        attempts = row["attempts"]
        backoff = min(2 ** min(attempts, 6), config.READ_FAIL_MAX_BACKOFF)
        now = time.time()
        conn.execute(
            """
            UPDATE artifacts
            SET status = ?, worker_id = ?,
                last_error = ?, next_enqueue_at = ?, updated_at = ?
            WHERE id = ? AND status = ?
            """,
            (ST_PENDING, worker_id,
             f"read failure, will retry: {error}"[:500],
             now + backoff, now, artifact_id, ST_PROCESSING),
        )
        conn.execute("COMMIT")
        return True
    except Exception:
        conn.execute("ROLLBACK")
        raise


def detach_after_ack(artifact_id: str, stream_id: str) -> None:
    """Clear the stream binding of a retry-pending row, but only while the
    row is still bound to *this* message. Called after XACK so the reconciler
    can publish a fresh retry message without ever duplicating one."""
    conn = get_conn()
    conn.execute(
        """
        UPDATE artifacts
        SET stream_id = NULL, updated_at = ?
        WHERE id = ? AND status = ? AND stream_id = ?
        """,
        (time.time(), artifact_id, ST_PENDING, stream_id),
    )
