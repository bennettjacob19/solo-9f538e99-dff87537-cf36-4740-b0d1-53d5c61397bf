"""Regression tests for the read-failure crash window and its repair.

Background
----------
When a hash read fails, the worker commits ``pending`` + backoff, then ACKs
the old stream message, then clears the row's ``stream_id``. A crash between
the ACK and the detach leaves a ``pending`` row bound to a message that is no
longer in the PEL (already delivered + ACKed, so it can never be delivered
again). Before the fix, the dispatcher only picked rows with a NULL binding,
so such a row stayed "待校验" forever.

What is checked here
--------------------
  U1  concurrent clear_stale_binding: exactly one scanner wins; the row ends
      up pending and unbound (no duplicate task, no lost job)
  U2  confirmed (passed/failed) rows can never be flipped back to pending
  U3  the unbind is conditional on the exact stale binding
  U4  reconciler classification: live/in-PEL bindings are kept, delivered+
      ACKed or vanished bindings are unbound, non-pending rows untouched
  U5  missing consumer group defers repair instead of guessing
  U6  concurrent attach_stream_id yields exactly one binding
  U7  claim rejects a message whose id is not the row's binding
  E1  real crash in the ACK→detach window, then recovery to passed
  E2  confirmed row with an old binding is not touched by later scans
  E3  two web reconcilers scanning the same broken row still produce exactly
      one re-execution and one conclusion
  E4  steady state: no leftovers in the PEL

Run: .venv/bin/python tests/recovery_test.py
"""
from __future__ import annotations

import hashlib
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUNTIME = ROOT / "tests" / "runtime-recovery"
REDIS_BIN = os.environ.get(
    "REDIS_BIN", "/tmp/redis-build/redis-7.4.2/src/redis-server")
PYTHON = str(ROOT / ".venv" / "bin" / "python")
PORT = 6398
WEB_PORT = 8098
WEB2_PORT = 8097
BASE = f"http://127.0.0.1:{WEB_PORT}"
BASE2 = f"http://127.0.0.1:{WEB2_PORT}"
STREAM = "verify:jobs"
GROUP = "verifiers"

results: list[tuple[str, bool, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")


# --------------------------------------------------------------------------- #
# Unit-level section (in-process, temp SQLite + fake Redis)
# --------------------------------------------------------------------------- #
class FakeRedis:
    """Minimal stand-in for the Redis calls used by the reconciler scan."""

    def __init__(self, watermark: str, pel: set[str],
                 entries: set[str], groups_missing: bool = False) -> None:
        self.watermark = watermark
        self.pel = pel
        self.entries = entries
        self.groups_missing = groups_missing

    def xinfo_groups(self, _key):
        if self.groups_missing:
            import redis
            raise redis.ResponseError(
                "NOGROUP No such key '%s' or consumer group" % _key)
        return [{"name": GROUP, "last-delivered-id": self.watermark}]

    def xpending_range(self, name, groupname, min, max, count, idle):
        assert name == STREAM and groupname == GROUP
        return [{"message_id": mid} for mid in sorted(self.pel)]

    def xrange(self, _name, min, max, count):
        return [(min, {"artifact_id": "x"})] if min in self.entries else []


def unit_tests() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="recovery-unit-"))
    sys.path.insert(0, str(ROOT))

    from app import config, db, queue  # noqa: F401  (reconciler needs both)
    from app.reconciler import Reconciler

    # Point the process-local modules at a throwaway DB. Set the config
    # attributes directly (not os.environ) so the e2e subprocesses below
    # inherit a clean environment.
    config.DATA_DIR = tmp / "data"
    config.STORAGE_DIR = tmp / "data" / "artifacts"
    config.DB_PATH = tmp / "data" / "app.db"
    db.init_db()

    def new_row(status: str = db.ST_PENDING, stream_id: str | None = None):
        aid = db.create_artifact("f.bin", config.STORAGE_DIR / "f",
                                 "0" * 64)
        if stream_id is not None or status != db.ST_PENDING:
            conn = db.get_conn()
            conn.execute(
                "UPDATE artifacts SET status=?, stream_id=?, updated_at=?"
                " WHERE id=?",
                (status, stream_id, time.time(), aid))
        return aid

    def row(aid: str) -> dict:
        return dict(db.get_artifact(aid))

    # ---- U1: concurrent repair has exactly one winner ------------------ #
    aid = new_row(stream_id="7-0")
    outcomes: list[bool] = []
    barrier = threading.Barrier(12)
    lock = threading.Lock()

    def try_clear():
        barrier.wait()
        ok = db.clear_stale_binding(aid, "7-0")
        with lock:
            outcomes.append(ok)

    threads = [threading.Thread(target=try_clear) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    r = row(aid)
    record("U1 concurrent stale-binding clear has exactly one winner",
           outcomes.count(True) == 1 and r["status"] == "pending"
           and r["stream_id"] is None,
           f"winners={outcomes.count(True)} status={r['status']} "
           f"stream={r['stream_id']}")

    # ---- U2: confirmed rows can never go back to pending --------------- #
    aid_p = new_row(stream_id="5-0")
    assert db.claim_for_processing(aid_p, "w", "5-0")
    assert db.finalize_artifact(aid_p, "w", db.ST_PASSED, "a" * 64, None)
    passed_before = row(aid_p)
    cleared_p = db.clear_stale_binding(aid_p, "5-0")
    passed_after = row(aid_p)

    aid_f = new_row(stream_id="5-1")
    assert db.claim_for_processing(aid_f, "w", "5-1")
    assert db.finalize_artifact(aid_f, "w", db.ST_FAILED, "b" * 64, "bad")
    failed_before = row(aid_f)
    cleared_f = db.clear_stale_binding(aid_f, "5-1")
    failed_after = row(aid_f)

    record("U2 confirmed rows are never reverted by the repair",
           not cleared_p and not cleared_f
           and passed_after["status"] == passed_before["status"] == "passed"
           and passed_after["updated_at"] == passed_before["updated_at"]
           and passed_after["actual_sha256"] == "a" * 64
           and failed_after["status"] == failed_before["status"] == "failed"
           and failed_after["updated_at"] == failed_before["updated_at"],
           f"passed={passed_after['status']} failed={failed_after['status']} "
           f"cleared=({cleared_p},{cleared_f})")

    # ---- U3: clear is conditional on the exact binding ----------------- #
    aid = new_row(stream_id="8-0")
    wrong = db.clear_stale_binding(aid, "9-0")
    r = row(aid)
    right = db.clear_stale_binding(aid, "8-0")
    r2 = row(aid)
    record("U3 stale-binding clear only matches the exact binding",
           not wrong and r["stream_id"] == "8-0"
           and right and r2["stream_id"] is None,
           f"wrong={wrong} right={right}")

    # ---- U4: reconciler classification --------------------------------- #
    a_live_undelivered = new_row(stream_id="20-0")     # > watermark, exists
    a_in_pel = new_row(stream_id="8-0")                # owned by a consumer
    a_delivered_acked = new_row(stream_id="7-0")       # the crash-window case
    a_vanished = new_row(stream_id="30-0")             # entry gone
    a_processing = new_row(status=db.ST_PROCESSING, stream_id="7-0")
    a_passed = new_row(status=db.ST_PASSED, stream_id="7-0")
    a_failed = new_row(status=db.ST_FAILED, stream_id="7-0")

    fake = FakeRedis(watermark="10-0", pel={"8-0"}, entries={"20-0"})
    rec = Reconciler()
    rec._r = fake
    rec._reap_stale_bindings()

    live = row(a_live_undelivered)
    inpel = row(a_in_pel)
    acked = row(a_delivered_acked)
    gone = row(a_vanished)
    record("U4 scan unbinds only undeliverable pending bindings",
           live["stream_id"] == "20-0" and live["status"] == "pending"
           and inpel["stream_id"] == "8-0" and inpel["status"] == "pending"
           and acked["stream_id"] is None and acked["status"] == "pending"
           and gone["stream_id"] is None and gone["status"] == "pending"
           and row(a_processing)["status"] == "processing"
           and row(a_processing)["stream_id"] == "7-0"
           and row(a_passed)["status"] == "passed"
           and row(a_passed)["stream_id"] == "7-0"
           and row(a_failed)["status"] == "failed"
           and row(a_failed)["stream_id"] == "7-0",
           f"live={live['stream_id']} inpel={inpel['stream_id']} "
           f"acked={acked['stream_id']} gone={gone['stream_id']}")

    # ---- U5: no consumer group yet -> defer, never guess --------------- #
    aid = new_row(stream_id="7-0")
    rec._r = FakeRedis(watermark="0-0", pel=set(), entries=set(),
                       groups_missing=True)
    rec._reap_stale_bindings()
    record("U5 missing consumer group defers the scan without unbinding",
           row(aid)["stream_id"] == "7-0", f"stream={row(aid)['stream_id']}")

    # ---- U6: concurrent dispatch binds exactly one message ------------- #
    aid = new_row()
    attaches: list[bool] = []

    def try_attach(mid: str):
        barrier.wait()
        ok = db.attach_stream_id(aid, mid)
        with lock:
            attaches.append(ok)

    barrier = threading.Barrier(8)
    threads = [threading.Thread(target=try_attach, args=(f"4{i}-0",))
               for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    r = row(aid)
    record("U6 concurrent dispatchers bind exactly one message",
           attaches.count(True) == 1 and r["stream_id"] is not None,
           f"bindings={attaches.count(True)} stream={r['stream_id']}")

    # ---- U7: a foreign message may not claim a bound row --------------- #
    aid = new_row(stream_id="50-0")
    foreign = db.claim_for_processing(aid, "w", "51-0")
    r_foreign = row(aid)
    own = db.claim_for_processing(aid, "w", "50-0")
    record("U7 a message different from the row binding cannot claim it",
           foreign is None and r_foreign["status"] == "pending"
           and own is not None and r_foreign["stream_id"] == "50-0",
           f"foreign={foreign is not None} own={own is not None}")

    shutil.rmtree(tmp, ignore_errors=True)


# --------------------------------------------------------------------------- #
# End-to-end section (real processes)
# --------------------------------------------------------------------------- #
def wait_tcp(host: str, port: int, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def wait_http(url: str, timeout: float) -> bool:
    import requests
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(url, timeout=2).status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(0.2)
    return False


def cleanup_leftovers() -> None:
    """Kill leftovers from an aborted run. Redis may need a moment to release
    the listening socket after SIGKILL/SIGTERM, so retry the whole sweep."""
    me = os.getpid()
    for attempt in range(20):
        for proc in Path("/proc").iterdir():
            if not proc.name.isdigit() or int(proc.name) == me:
                continue
            try:
                raw = (proc / "cmdline").read_bytes().decode(
                    "utf-8", "replace").replace("\x00", " ")
            except OSError:
                continue
            if "runtime-recovery" in raw or (
                    "app." in raw and f"REDIS_PORT={PORT}" in raw):
                try:
                    os.kill(int(proc.name), signal.SIGKILL)
                except OSError:
                    pass
        out = subprocess.run(["pgrep", "-f", f"redis-server.*--port {PORT}"],
                             capture_output=True, text=True)
        for pid in out.stdout.split():
            try:
                os.kill(int(pid), signal.SIGKILL)
            except OSError:
                pass
        if not wait_tcp("127.0.0.1", PORT, 0.3):
            return
        time.sleep(0.5)
    raise RuntimeError(f"redis port {PORT} still occupied")


def main() -> int:
    print("== unit: concurrent repair and classification ==")
    unit_tests()

    cleanup_leftovers()
    if RUNTIME.exists():
        shutil.rmtree(RUNTIME)
    (RUNTIME / "data").mkdir(parents=True)
    (RUNTIME / "redis").mkdir(parents=True)

    import requests
    import redis

    env = os.environ.copy()
    env.update({
        "REDIS_HOST": "127.0.0.1", "REDIS_PORT": str(PORT),
        "DATA_DIR": str(RUNTIME / "data"), "PORT": str(WEB_PORT),
        "CLAIM_MIN_IDLE_MS": "2000", "PROCESSING_STALE_SECONDS": "60",
        # A slow-ish scan interval gives the test a deterministic window in
        # which the broken binding exists before the reconciler repairs it.
        "DISPATCH_INTERVAL_SECONDS": "3",
        "READ_FAIL_MAX_BACKOFF": "1",
        "WORKER_POLL_MS": "500",
    })
    logr = open(RUNTIME / "redis.log", "ab", buffering=0)
    redis_p = subprocess.Popen(
        [REDIS_BIN, "--port", str(PORT), "--dir", str(RUNTIME / "redis"),
         "--appendonly", "no", "--save", "", "--daemonize", "no"],
        stdout=logr, stderr=subprocess.STDOUT)
    assert wait_tcp("127.0.0.1", PORT, 10)
    rc = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True)

    procs: list[tuple[str, subprocess.Popen, object]] = []
    log_files = {}

    def start(module: str, name: str, extra: dict | None = None,
              port: str | None = None) -> subprocess.Popen:
        e = dict(env)
        e["PORT"] = port or str(WEB_PORT)
        if extra:
            e.update(extra)
        log_files[name] = open(RUNTIME / f"{name}.log", "ab", buffering=0)
        p = subprocess.Popen([PYTHON, "-m", module], cwd=str(ROOT), env=e,
                             stdout=log_files[name],
                             stderr=subprocess.STDOUT)
        procs.append((name, p, log_files[name]))
        return p

    def stop(p: subprocess.Popen, sig: int = signal.SIGTERM) -> None:
        if p.poll() is None:
            p.send_signal(sig)
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=5)

    db_path = RUNTIME / "data" / "app.db"

    def q(sql: str, args: tuple = ()) -> list[dict]:
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in conn.execute(sql, args)]
        finally:
            conn.close()

    def artifact(aid: str) -> dict:
        rows = q("SELECT * FROM artifacts WHERE id=?", (aid,))
        return rows[0] if rows else {}

    def pel() -> list[dict]:
        return rc.xpending_range(STREAM, GROUP, "-", "+", 100)

    def upload(content: bytes, digest: str) -> str:
        r = requests.post(
            f"{BASE}/upload", data={"expected_sha256": digest},
            files={"file": ("r.bin", content, "application/octet-stream")},
            timeout=15, allow_redirects=False)
        assert r.status_code == 302, r.status_code
        return q("SELECT id FROM artifacts ORDER BY created_at DESC "
                 "LIMIT 1")[0]["id"]

    def wait_broken_state(aid: str, timeout: float = 15.0) -> dict | None:
        """Poll for the exact post-crash state: row pending but bound to a
        message that is not in the PEL (already ACKed)."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            row = artifact(aid)
            ids = None
            if row.get("status") == "pending" and row.get("stream_id"):
                try:
                    ids = {p["message_id"] for p in pel()}
                except redis.RedisError:
                    ids = None
            if ids is not None and row["stream_id"] not in ids:
                return row
            time.sleep(0.05)
        return None

    try:
        web = start("app.web", "web")
        assert wait_http(f"{BASE}/healthz", 20)
        crashy = start("app.worker", "crashy", {
            "WORKER_NAME": "crashy",
            "SIMULATE_READ_FAILURE": "1",
            "EXIT_AFTER_ACK_BEFORE_DETACH": "1",
        })

        print("== E1 crash after ACK, before detach ==")
        c1 = b"crash-window " + uuid.uuid4().bytes
        h1 = hashlib.sha256(c1).hexdigest()
        aid1 = upload(c1, h1)

        broken = wait_broken_state(aid1, 20)
        crashy.wait(timeout=20)
        record("E1a worker crashed in the ACK->detach window "
               "(pending + stale binding)",
               broken is not None and crashy.returncode == 1,
               f"row={{{broken.get('status')},{broken.get('stream_id')}}} "
               f"rc={crashy.returncode}" if broken else
               f"broken state not observed rc={crashy.returncode}")

        recoverer = start("app.worker", "recoverer",
                          {"WORKER_NAME": "recoverer"})
        deadline = time.time() + 40
        final = artifact(aid1)
        while time.time() < deadline and final.get("status") != "passed":
            time.sleep(0.3)
            final = artifact(aid1)
        dl = requests.get(f"{BASE}/artifacts/{aid1}/download", timeout=10)
        n_rows = q("SELECT COUNT(*) AS n FROM artifacts")[0]["n"]
        record("E1b reconciler re-dispatched the stale binding; job passed once",
               final.get("status") == "passed"
               and final.get("actual_sha256") == h1
               and final.get("worker_id") == "recoverer"
               and final.get("attempts") == 2 and n_rows == 1
               and dl.status_code == 200 and dl.content == c1,
               f"status={final.get('status')} attempts={final.get('attempts')}"
               f" worker={final.get('worker_id')} dl={dl.status_code} "
               f"rows={n_rows}")

        web_log = (RUNTIME / "web.log").read_text(errors="replace")
        record("E1c the repair path logged the unbind of that artifact",
               f"artifact {aid1} bound to undeliverable message" in web_log,
               "log marker " + ("found" if "undeliverable" in web_log
                                else "missing"))

        print("== E2 confirmed rows are immune to later scans ==")
        before = artifact(aid1)
        # Simulate an old binding left on a confirmed row (finalize keeps the
        # binding; a web crash right before ACK leaves it in place).
        conn = sqlite3.connect(db_path, timeout=10)
        conn.execute("UPDATE artifacts SET stream_id='1-1' WHERE id=?",
                     (aid1,))
        conn.commit()
        conn.close()
        time.sleep(7)  # > 2 scan intervals
        after = artifact(aid1)
        dl2 = requests.get(f"{BASE}/artifacts/{aid1}/download", timeout=10)
        record("E2 confirmed result is never reset to pending by the scan",
               after["status"] == "passed"
               and after["updated_at"] == before["updated_at"]
               and after["actual_sha256"] == before["actual_sha256"]
               and after["attempts"] == before["attempts"] == 2
               and after["enqueue_attempts"] == before["enqueue_attempts"]
               and dl2.status_code == 200 and dl2.content == c1,
               f"status={after['status']} attempts={after['attempts']} "
               f"updated_at_changed={after['updated_at'] != before['updated_at']}")

        print("== E3 two web reconcilers race on one broken row ==")
        stop(recoverer)
        crashy2 = start("app.worker", "crashy2", {
            "WORKER_NAME": "crashy2",
            "SIMULATE_READ_FAILURE": "1",
            "EXIT_AFTER_ACK_BEFORE_DETACH": "1",
        })
        c3 = b"concurrent-scan " + uuid.uuid4().bytes
        h3 = hashlib.sha256(c3).hexdigest()
        aid3 = upload(c3, h3)
        broken3 = wait_broken_state(aid3, 20)
        crashy2.wait(timeout=20)
        record("E3a second crash window reproduced",
               broken3 is not None,
               f"row={{{broken3.get('status')},{broken3.get('stream_id')}}}"
               if broken3 else "not observed")

        # Start a second web process (second reconciler, same DB/Redis) and a
        # fresh worker at the same time, so both scanners see the stale row.
        web2 = start("app.web", "web2", port=str(WEB2_PORT))
        assert wait_http(f"{BASE2}/healthz", 20)
        recoverer2 = start("app.worker", "recoverer2",
                           {"WORKER_NAME": "recoverer2"})
        deadline = time.time() + 45
        final3 = artifact(aid3)
        while time.time() < deadline and final3.get("status") != "passed":
            time.sleep(0.3)
            final3 = artifact(aid3)
        dl3 = requests.get(f"{BASE}/artifacts/{aid3}/download", timeout=10)
        n_rows3 = q("SELECT COUNT(*) AS n FROM artifacts")[0]["n"]
        record("E3b concurrent scans still yield one retry and one conclusion",
               final3.get("status") == "passed"
               and final3.get("actual_sha256") == h3
               and final3.get("worker_id") == "recoverer2"
               and final3.get("attempts") == 2 and n_rows3 == 2
               and dl3.status_code == 200 and dl3.content == c3,
               f"status={final3.get('status')} "
               f"attempts={final3.get('attempts')} "
               f"worker={final3.get('worker_id')} dl={dl3.status_code} "
               f"rows={n_rows3}")

        # Give both scanners time to settle; nothing may be dispatched anew.
        before3 = artifact(aid3)
        time.sleep(7)
        after3 = artifact(aid3)
        record("E3c settled rows are not re-queued again",
               after3 == before3,
               f"changed={after3 != before3}")

        time.sleep(2)
        record("E4 stream PEL empty at steady state", len(pel()) == 0,
               f"pel={len(pel())}")
    finally:
        for _name, p, fh in procs:
            stop(p, signal.SIGKILL if p.poll() is None else signal.SIGTERM)
            try:
                fh.close()
            except Exception:
                pass
        stop(redis_p)
        try:
            logr.close()
        except Exception:
            pass

    print("\n================ SUMMARY ================")
    n_fail = 0
    for name, ok, detail in results:
        print(f"{'PASS' if ok else 'FAIL':4} {name}  {detail}")
        if not ok:
            n_fail += 1
    print(f"\n{len(results) - n_fail}/{len(results)} checks passed")
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
