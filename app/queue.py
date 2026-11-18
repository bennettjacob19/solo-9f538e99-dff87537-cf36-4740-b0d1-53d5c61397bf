"""Redis Streams primitives: consumer group, publish, read, takeover.

Jobs are delivered through one stream + one consumer group (``verifiers``),
so the two worker replicas share the partitions: every job goes to exactly
one worker at a time, while pending messages of a dead worker become
claimable by the other via XAUTOCLAIM.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

import redis

from . import config

log = logging.getLogger("queue")


def get_redis() -> redis.Redis:
    return redis.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        db=config.REDIS_DB,
        decode_responses=True,
        socket_connect_timeout=2,
        socket_timeout=5,
        # One quick retry is enough; the reconciler/worker loops already
        # provide durable retry with backoff. Retrying inside the client
        # would only blur failure accounting.
        retry=redis.retry.Retry(redis.backoff.NoBackoff(), 0),
    )


def ensure_group(r: redis.Redis) -> None:
    """Create the consumer group idempotently.

    MKSTREAM creates the stream on first use; BUSYGROUP means another process
    (the second worker, or a previous boot) already created the group.
    """
    try:
        r.xgroup_create(config.STREAM_KEY, config.GROUP_NAME,
                        id="$", mkstream=True)
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise
        log.info("consumer group %s already exists", config.GROUP_NAME)


def publish_job(r: redis.Redis, artifact_id: str) -> str:
    """XADD one job message. Raises redis.RedisError on failure so the
    reconciler can record the failure and retry later."""
    return r.xadd(
        config.STREAM_KEY,
        {"artifact_id": artifact_id},
        maxlen=100_000,
        approximate=True,
    )


def delete_message(r: redis.Redis, stream_id: str) -> None:
    """Remove an orphan message (one whose DB row rejected the binding)."""
    r.xdel(config.STREAM_KEY, stream_id)


def read_jobs(r: redis.Redis, consumer: str, count: int = 5,
              block_ms: Optional[int] = None) -> list[tuple[str, dict]]:
    """Blocking group read for new messages. Returns [(stream_id, fields)]."""
    if block_ms is None:
        block_ms = config.WORKER_POLL_MS
    resp = r.xreadgroup(
        groupname=config.GROUP_NAME,
        consumername=consumer,
        streams={config.STREAM_KEY: ">"},
        count=count,
        block=block_ms,
    )
    out: list[tuple[str, dict]] = []
    if not resp:
        return out
    for _stream, entries in resp:
        for stream_id, fields in entries:
            out.append((stream_id, fields))
    return out


def claim_pending(r: redis.Redis, consumer: str,
                  min_idle_ms: Optional[int] = None) -> list[tuple[str, dict]]:
    """Take over messages whose owning consumer has not ACKed them.

    XAUTOCLAIM atomically transfers ownership of idle pending entries to this
    consumer, which is exactly the "dead worker's unacknowledged messages are
    continued by another worker" requirement. Idle time defaults to
    CLAIM_MIN_IDLE_MS so a healthy worker that is simply busy never loses
    in-flight work.
    """
    if min_idle_ms is None:
        min_idle_ms = config.CLAIM_MIN_IDLE_MS
    try:
        # redis-py returns (next_cursor, [(id, fields), ...], [deleted...]);
        # tolerate both dict and list/tuple field shapes across versions.
        resp = r.xautoclaim(
            name=config.STREAM_KEY,
            groupname=config.GROUP_NAME,
            consumername=consumer,
            min_idle_time=min_idle_ms,
            start_id="0",
            count=10,
        )
    except redis.ResponseError as exc:
        # Empty stream / no PEL yet: nothing to claim.
        log.debug("xautoclaim returned error: %s", exc)
        return []
    if not resp or len(resp) < 2:
        return []
    out: list[tuple[str, dict]] = []
    for item in resp[1]:
        sid, fields = item
        if isinstance(fields, (list, tuple)):
            fields = dict(zip(fields[0::2], fields[1::2]))
        out.append((sid, fields))
    return out


def ack(r: redis.Redis, stream_id: str) -> None:
    r.xack(config.STREAM_KEY, config.GROUP_NAME, stream_id)


def pending_entry_ids(r: redis.Redis) -> set[str]:
    """All stream IDs currently in the group's pending-entries list.

    Used by the reconciler to decide whether a ``processing`` row still has a
    live stream message (Redis itself retained it) or the message is gone
    (e.g. Redis was wiped) and the row must be re-dispatched. Paged with an
    exclusive cursor so more than 1000 PEL entries cannot loop forever.
    """
    ids: set[str] = set()
    start = "-"
    while True:
        resp = r.xpending_range(
            name=config.STREAM_KEY,
            groupname=config.GROUP_NAME,
            min=start,
            max="+",
            count=1000,
            idle=None,
        )
        if not resp:
            break
        for item in resp:
            ids.add(item["message_id"])
        if len(resp) < 1000:
            break
        # Next page starts strictly after the last returned message id.
        start = "(" + resp[-1]["message_id"]
    return ids


def parse_artifact_id(fields: dict) -> Optional[str]:
    """Defensive parsing: messages are produced by us as hash fields, but a
    malformed entry must not crash the worker."""
    aid = fields.get("artifact_id")
    if aid:
        return aid
    raw = fields.get("data") or fields.get("payload")
    if raw:
        try:
            return json.loads(raw).get("artifact_id")
        except (ValueError, TypeError):
            return None
    return None
