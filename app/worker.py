"""Verification worker.

One process per replica (docker compose runs two). Both replicas join the
same Redis Streams consumer group under distinct consumer names.

Reliability contract:
  * New jobs arrive through XREADGROUP; jobs left unacknowledged by a dead
    worker are taken over with XAUTOCLAIM after CLAIM_MIN_IDLE_MS.
  * Every state transition is a conditional SQL UPDATE, so processing the
    same message twice never produces a second conclusion and never rewrites
    a confirmed passed/failed row.
  * The stream message is ACKed only after the DB transaction that stores
    the conclusion (or the retry decision) has committed. A kill at any point
    therefore leaves either an old-version row + a pending message (retried)
    or a committed conclusion (duplicate delivery becomes a no-op).
  * File read errors are retryable: the row returns to ``pending`` and is
    re-dispatched; only a successful read whose digest differs is ``failed``.
"""
from __future__ import annotations

import logging
import os
import signal
import socket
import time

import redis

from . import checksums, config, db, queue

log = logging.getLogger("worker")


class VerificationWorker:
    def __init__(self, name: str | None = None) -> None:
        self.worker_id = (
            name
            or os.environ.get("WORKER_NAME")
            or f"{socket.gethostname()}-{os.getpid()}"
        )
        self._r = queue.get_redis()
        self._running = True

    def stop(self, *_args) -> None:
        log.info("shutdown signal received")
        self._running = False

    # ------------------------------------------------------------------ #
    def _handle_message(self, stream_id: str, fields: dict,
                        takeover: bool = False) -> None:
        artifact_id = queue.parse_artifact_id(fields)
        if not artifact_id:
            log.error("malformed message %s: %r -> ACK and drop",
                      stream_id, fields)
            queue.ack(self._r, stream_id)
            return

        if takeover:
            row = db.takeover_for_processing(
                artifact_id, self.worker_id, stream_id)
        else:
            row = db.claim_for_processing(artifact_id, self.worker_id)
        if row is None:
            # Duplicate delivery / takeover of an already decided job or a
            # superseded message. The conditional claim found no eligible row,
            # so there is nothing to do: ACK removes the duplicate from the
            # PEL. Confirmed results are never touched.
            log.info("artifact %s not eligible (takeover=%s); ACK %s",
                     artifact_id, takeover, stream_id)
            queue.ack(self._r, stream_id)
            return

        log.info("worker %s %s artifact %s (msg %s, attempt %d)",
                 self.worker_id,
                 "took over" if takeover else "processing",
                 artifact_id, stream_id, row["attempts"])

        if config.PRE_FINALIZE_DELAY:
            # Test hook: widen the window in which SIGKILL leaves the message
            # pending-ack while the DB row is already ``processing``.
            time.sleep(config.PRE_FINALIZE_DELAY / 1000.0)
            if not self._running:
                return

        try:
            if config.SIMULATE_READ_FAILURE:
                raise OSError("SIMULATE_READ_FAILURE is set")
            actual = checksums.sha256_file(row["stored_path"])
        except OSError as exc:
            # Read failure -> retry, NOT a digest mismatch. Commit the
            # pending+backoff state first, ACK the old message, then unbind so
            # the reconciler publishes exactly one fresh retry message.
            log.warning("read failure for %s: %s (will retry)",
                        artifact_id, exc)
            db.record_read_failure(artifact_id, self.worker_id, str(exc))
            queue.ack(self._r, stream_id)
            db.detach_after_ack(artifact_id, stream_id)
            return

        expected = row["expected_sha256"].strip().lower()
        if actual == expected:
            status, error = db.ST_PASSED, None
            log.info("artifact %s PASSED", artifact_id)
        else:
            status, error = (
                db.ST_FAILED,
                f"sha256 mismatch: expected {expected}, got {actual}",
            )
            log.info("artifact %s FAILED digest check", artifact_id)

        finalized = db.finalize_artifact(
            artifact_id, self.worker_id, status, actual, error
        )
        if not finalized:
            # Another worker/attempt already wrote a conclusion. Never
            # overwrite it; just retire this duplicate message.
            log.warning("artifact %s had a concurrent conclusion; "
                        "keeping the confirmed result", artifact_id)
        queue.ack(self._r, stream_id)

    # ------------------------------------------------------------------ #
    def run(self) -> None:
        db.init_db()
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)

        while True:
            try:
                queue.ensure_group(self._r)
                break
            except redis.RedisError as exc:
                log.warning("waiting for Redis: %s", exc)
                time.sleep(config.DISPATCH_INTERVAL)
                if not self._running:
                    return

        log.info("worker %s joined group %s on %s",
                 self.worker_id, config.GROUP_NAME, config.STREAM_KEY)
        while self._running:
            try:
                # 1) Take over idle messages from dead/stuck consumers.
                for stream_id, fields in queue.claim_pending(
                        self._r, self.worker_id):
                    if not self._running:
                        break
                    log.info("claimed pending message %s", stream_id)
                    self._handle_message(stream_id, fields, takeover=True)

                # 2) Consume new messages (blocking up to WORKER_POLL_MS).
                for stream_id, fields in queue.read_jobs(
                        self._r, self.worker_id):
                    if not self._running:
                        break
                    self._handle_message(stream_id, fields)
            except redis.RedisError as exc:
                log.warning("Redis error: %s; retrying", exc)
                time.sleep(config.DISPATCH_INTERVAL)
            except Exception:
                log.exception("unexpected worker error; continuing")
                time.sleep(1)
        log.info("worker %s exited", self.worker_id)


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    VerificationWorker().run()


if __name__ == "__main__":
    main()
