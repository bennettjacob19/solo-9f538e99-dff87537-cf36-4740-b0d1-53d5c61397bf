"""Durable dispatch loop running inside the web process.

Three jobs, forever, until shutdown:

1. Dispatch guarantee: every ``pending`` row without a stream binding is
   XADDed to Redis. If XADD fails (Redis down, enqueue failure) the row stays
   pending with capped backoff and is retried — an accepted upload can never
   get stuck because the queue was unavailable at upload time. Rows surviving
   a web restart are picked up automatically because the DB is the source of
   truth.

2. Stale-binding repair: a ``pending`` row may still carry a stream id whose
   message can never be delivered again — most importantly when a worker
   crashes after ACKing a read-failure retry but before clearing the binding.
   Such rows are unbound (conditionally, see :func:`db.clear_stale_binding`)
   and therefore fall back into the normal dispatch path. The scan is
   re-entrant and safe to run concurrently from several web replicas.

3. Orphan reaping: rows stuck in ``processing`` whose stream message no
   longer exists in the group PEL are reset to ``pending`` and re-dispatched.
"""
from __future__ import annotations

import logging
import threading
import time

import redis

from . import config, db, queue

log = logging.getLogger("reconciler")


class Reconciler(threading.Thread):
    daemon = True

    def __init__(self) -> None:
        super().__init__(name="reconciler", daemon=True)
        self._stop = threading.Event()
        self._r = queue.get_redis()

    def stop(self) -> None:
        self._stop.set()

    # ------------------------------------------------------------------ #
    def _dispatch_pending(self) -> None:
        try:
            rows = db.fetch_due_pending()
        except Exception:
            log.exception("failed to fetch pending artifacts")
            return
        for row in rows:
            if self._stop.is_set():
                return
            artifact_id = row["id"]
            try:
                stream_id = queue.publish_job(self._r, artifact_id)
            except redis.RedisError as exc:
                log.warning("enqueue failed for %s: %s", artifact_id, exc)
                try:
                    db.record_enqueue_failure(artifact_id, str(exc))
                except Exception:
                    log.exception("failed to record enqueue failure")
                # Redis is likely down: back off the whole loop too.
                self._stop.wait(min(2.0, config.DISPATCH_INTERVAL))
                continue
            # XADD succeeded; bind the message to the row atomically. If the
            # row is no longer eligible (defensive: concurrent change), remove
            # the orphan message so it cannot resurrect a finished job. The
            # worker's conditional claim also rejects a message whose id does
            # not match the row's binding, so even a delivery that beats the
            # XDEL cannot produce a second valid task.
            if db.attach_stream_id(artifact_id, stream_id):
                log.info("dispatched artifact %s -> %s",
                         artifact_id, stream_id)
            else:
                log.warning("artifact %s rejected binding; deleting orphan %s",
                            artifact_id, stream_id)
                try:
                    queue.delete_message(self._r, stream_id)
                except redis.RedisError:
                    log.exception("failed to delete orphan message %s",
                                  stream_id)

    def _reap_stale_bindings(self) -> None:
        """Unbind pending rows whose stream message can no longer be
        delivered, so the dispatcher publishes a fresh one.

        A binding is stale when its message is absent from the group PEL
        *and* can never be delivered again:

          * it already sits at/below ``last-delivered-id`` (it was delivered
            and then ACKed — e.g. the worker crashed right after XACK but
            before ``detach_after_ack``), or
          * it does not exist in the stream at all (Redis data loss, XDEL,
            or a binding left over from a wiped Redis).

        Bindings whose message is still undelivered (freshly XADDed, id above
        the watermark) or still in the PEL (owned by a worker, possibly a
        read-failure retry awaiting takeover) are left untouched.

        Everything here is safe under concurrent scans: the snapshot is read
        conservatively and the actual unbind is a conditional UPDATE that
        only one scanner can win (see :func:`db.clear_stale_binding`).
        """
        try:
            rows = db.fetch_bound_pending()
        except Exception:
            log.exception("failed to fetch bound pending artifacts")
            return
        if not rows:
            return
        try:
            snapshot = queue.group_pel_snapshot(self._r)
        except redis.RedisError:
            log.warning("Redis unavailable while scanning stale bindings; "
                        "skipping")
            return
        if snapshot is None:
            # Group not created yet (or Redis was wiped). Without a group no
            # message can be delivered; ensure_group recreates it on the next
            # loop iteration and the following scan repairs the rows. Do not
            # guess here.
            log.debug("consumer group missing; deferring stale-binding scan")
            return
        pel_ids, watermark = snapshot

        for row in rows:
            artifact_id = row["id"]
            stream_id = row["stream_id"]
            if stream_id in pel_ids:
                continue
            try:
                if queue.stream_id_lte(stream_id, watermark):
                    stale = True
                else:
                    # Not delivered yet according to the watermark. It is
                    # only deliverable if the entry actually exists.
                    stale = not queue.message_exists(self._r, stream_id)
            except redis.RedisError:
                log.warning("stale-binding check failed for %s (%s); "
                            "skipping this round", artifact_id, stream_id)
                return
            except ValueError:
                # Corrupt/foreign stream id in the row: unbind it so the job
                # can move on; it cannot be matched against any message.
                log.warning("invalid stream binding %r on %s",
                            stream_id, artifact_id)
                stale = True
            if not stale:
                continue
            try:
                cleared = db.clear_stale_binding(artifact_id, stream_id)
            except Exception:
                log.exception("failed to clear stale binding of %s",
                              artifact_id)
                continue
            if cleared:
                log.warning(
                    "artifact %s bound to undeliverable message %s; "
                    "unbound for re-dispatch", artifact_id, stream_id)
            # Losing the race is normal and harmless: another scanner (or the
            # worker's own detach_after_ack) already repaired the row. The
            # conditional UPDATE guarantees exactly one winner.

    def _reap_orphans(self) -> None:
        try:
            stuck = db.get_conn().execute(
                """
                SELECT id, stream_id, updated_at FROM artifacts
                WHERE status = ? AND updated_at < ?
                """,
                (db.ST_PROCESSING,
                 time.time() - config.PROCESSING_STALE_SECONDS),
            ).fetchall()
        except Exception:
            log.exception("failed to scan stuck processing rows")
            return
        if not stuck:
            return
        try:
            live_ids = queue.pending_entry_ids(self._r)
        except redis.RedisError:
            log.warning("Redis unavailable while reaping orphans; skipping")
            return
        orphan_ids = [
            row["id"] for row in stuck
            # A row with NULL binding or whose message vanished from the PEL
            # is orphaned. Messages still pending are owned by some worker and
            # must be left alone (their takeover is handled via XAUTOCLAIM).
            if not row["stream_id"] or row["stream_id"] not in live_ids
        ]
        if orphan_ids:
            n = db.reset_stuck_processing(orphan_ids)
            log.warning("re-dispatched %d orphaned processing rows: %s",
                        n, orphan_ids)

    # ------------------------------------------------------------------ #
    def run(self) -> None:
        db.init_db()
        log.info("reconciler started")
        while not self._stop.is_set():
            try:
                # Best-effort: the group is needed by workers, not by XADD.
                # Never gate dispatch on this, otherwise a Redis outage at
                # boot would block the (retried) enqueue path entirely.
                try:
                    queue.ensure_group(self._r)
                except redis.RedisError as exc:
                    log.debug("ensure_group skipped: %s", exc)
                # Repair stale bindings before dispatching so a row that is
                # unbound this round can get its fresh message immediately.
                self._reap_stale_bindings()
                self._dispatch_pending()
                self._reap_orphans()
            except Exception:
                log.exception("reconciler loop error")
            self._stop.wait(config.DISPATCH_INTERVAL)
        log.info("reconciler stopped")
