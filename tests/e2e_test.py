"""End-to-end verification of the file-check platform without Docker.

It runs the exact same application modules as the docker-compose deployment:
a real redis-server (source build), one `app.web` process and two
`app.worker` processes, exercising:

  T1  happy path: upload -> passed -> download allowed
  T2  enqueue failure (Redis down at upload) -> reconciler keeps dispatching
  T3  digest mismatch -> failed, download forbidden
  T4  >2 MiB upload rejected (413)
  T5  unreadable file -> retry (pending), restored file -> passed
  T6  worker SIGKILL mid-job -> the other worker XAUTOCLAIMs and finishes
  T7  duplicate redelivery does not create/overwrite a conclusion
  T8  full restart keeps confirmed states and download eligibility

Run:  .venv/bin/python tests/e2e_test.py
"""
from __future__ import annotations

import hashlib
import io
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import redis
import requests

ROOT = Path(__file__).resolve().parent.parent
RUNTIME = ROOT / "tests" / "runtime"
REDIS_BIN = os.environ.get("REDIS_BIN", "/tmp/redis-build/redis-7.4.2/src/redis-server")
REDIS_CLI = os.environ.get("REDIS_CLI", "/tmp/redis-build/redis-7.4.2/src/redis-cli")
PYTHON = str(ROOT / ".venv" / "bin" / "python")
PORT = 6399
WEB_PORT = 8099
BASE = f"http://127.0.0.1:{WEB_PORT}"

PASS, FAIL = "PASS", "FAIL"
results: list[tuple[str, str, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    results.append((name, PASS if ok else FAIL, detail))
    print(f"[{PASS if ok else FAIL}] {name} {detail}")


def wait_for(url: str, timeout: float = 25.0, expect: int = 200) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=2)
            if r.status_code == expect:
                return True
        except requests.RequestException:
            pass
        time.sleep(0.4)
    return False


def wait_status(aid: str, wanted: set[str], timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = requests.get(BASE, timeout=5)
        r.raise_for_status()
        # Lightweight check via the page is awkward; use a tiny JSON through
        # sqlite directly instead.
        row = sql_json(aid)
        if row and row["status"] in wanted:
            return row
        time.sleep(0.5)
    return row if (row := sql_json(aid)) else {}


def sql_json(aid: str) -> dict:
    import sqlite3
    conn = sqlite3.connect(RUNTIME / "data" / "app.db")
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM artifacts WHERE id=?", (aid,)).fetchone()
    conn.close()
    return dict(row) if row else {}


def list_rows() -> list[dict]:
    import sqlite3
    conn = sqlite3.connect(RUNTIME / "data" / "app.db")
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("SELECT * FROM artifacts")]
    conn.close()
    return rows


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def upload(content: bytes, expected: str, filename: str = "bin.dat"):
    files = {"file": (filename, io.BytesIO(content), "application/octet-stream")}
    return requests.post(f"{BASE}/upload",
                         data={"expected_sha256": expected},
                         files=files, timeout=15, allow_redirects=False)


def start_redis(extra: list[str] | None = None) -> subprocess.Popen:
    Path(RUNTIME / "redis").mkdir(parents=True, exist_ok=True)
    cmd = [REDIS_BIN, "--port", str(PORT),
           "--dir", str(RUNTIME / "redis"),
           "--appendonly", "yes", "--appendfsync", "everysec",
           "--save", "", "--daemonize", "no"]
    if extra:
        cmd += extra
    log = open(RUNTIME / "redis.log", "ab", buffering=0)
    p = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
    if not wait_tcp("127.0.0.1", PORT, 15):
        raise RuntimeError("redis did not start")
    return p


def wait_tcp(host: str, port: int, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.3)
    return False


def base_env() -> dict:
    env = os.environ.copy()
    env.update({
        "REDIS_HOST": "127.0.0.1",
        "REDIS_PORT": str(PORT),
        "DATA_DIR": str(RUNTIME / "data"),
        "PORT": str(WEB_PORT),
        "CLAIM_MIN_IDLE_MS": "3000",
        "PROCESSING_STALE_SECONDS": "8",
        "DISPATCH_INTERVAL_SECONDS": "1",
        "READ_FAIL_MAX_BACKOFF": "1",
        "LOG_LEVEL": "INFO",
    })
    return env


def start_service(module: str, logname: str, env_extra: dict | None = None,
                  pre_delay_ms: int = 0) -> subprocess.Popen:
    env = base_env()
    if env_extra:
        env.update(env_extra)
    if pre_delay_ms:
        env["PRE_FINALIZE_DELAY_MS"] = str(pre_delay_ms)
    log = open(RUNTIME / logname, "ab", buffering=0)
    p = subprocess.Popen([PYTHON, "-m", module],
                         cwd=str(ROOT), env=env,
                         stdout=log, stderr=subprocess.STDOUT)
    return p


def stop(p: subprocess.Popen, name: str, sig: int = signal.SIGTERM) -> None:
    if p.poll() is not None:
        return
    p.send_signal(sig)
    try:
        p.wait(timeout=10)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=5)
    print(f"  stopped {name} (rc={p.returncode})")


def rclient() -> redis.Redis:
    return redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True)


def _cleanup_leftovers() -> None:
    """Kill processes from a previous aborted run so the 'Redis down'
    scenario is genuine and the web port is free."""
    patterns = ("app.web", "app.worker")
    me = os.getpid()
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit() or int(proc.name) == me:
            continue
        try:
            raw = (proc / "cmdline").read_bytes().decode(
                "utf-8", "replace").replace("\x00", " ")
        except (OSError, PermissionError):
            continue
        if any(p in raw for p in patterns):
            try:
                os.kill(int(proc.name), signal.SIGKILL)
                print(f"  cleaned leftover pid {proc.name}: {raw.strip()}")
            except OSError:
                pass
    # Leftover redis on our test port: identify via listening socket.
    try:
        out = subprocess.run(["pgrep", "-f", f"redis-server.*--port {PORT}"],
                             capture_output=True, text=True)
        for pid in out.stdout.split():
            os.kill(int(pid), signal.SIGKILL)
            print(f"  cleaned leftover redis pid {pid}")
    except (FileNotFoundError, OSError):
        pass
    deadline = time.time() + 5
    while time.time() < deadline:
        if not wait_tcp("127.0.0.1", PORT, 0.5) and not wait_tcp(
                "127.0.0.1", WEB_PORT, 0.5):
            return
        time.sleep(0.3)
    raise RuntimeError("test ports 6399/8099 still occupied after cleanup")


# --------------------------------------------------------------------------- #
def main() -> int:
    _cleanup_leftovers()
    if RUNTIME.exists():
        shutil.rmtree(RUNTIME)
    (RUNTIME / "data").mkdir(parents=True)

    print("== T2 first: upload while Redis is DOWN ==")
    web_no_redis = start_service("app.web", "web_no_redis.log")
    assert wait_for(f"{BASE}/healthz", 20), "web never came up without redis"
    content_t2 = b"enqueue-failure-case-" + uuid.uuid4().bytes
    h_t2 = sha256_bytes(content_t2)
    resp = upload(content_t2, h_t2, "t2.bin")
    record("T2a upload accepted while Redis down (302)",
           resp.status_code == 302, f"status={resp.status_code}")
    import sqlite3
    conn = sqlite3.connect(RUNTIME / "data" / "app.db")
    aid_t2 = conn.execute("SELECT id FROM artifacts ORDER BY created_at DESC LIMIT 1").fetchone()[0]
    conn.close()
    # Poll (up to 15s) for the reconciler's enqueue-failure accounting,
    # instead of assuming a fixed sleep covers the first dispatch cycle.
    deadline = time.time() + 15
    r0 = sql_json(aid_t2)
    while time.time() < deadline and r0.get("enqueue_attempts", 0) < 1:
        time.sleep(0.5)
        r0 = sql_json(aid_t2)
    record("T2b row stays pending & unbound while enqueue fails",
           r0["status"] == "pending" and r0["stream_id"] is None
           and r0["enqueue_attempts"] >= 1,
           f"status={r0.get('status')} attempts={r0.get('enqueue_attempts')}")

    print("== starting Redis + two workers; reconciler must dispatch T2 ==")
    redis_p = start_redis()
    r = rclient()
    w1 = start_service("app.worker", "worker1.log",
                       env_extra={"WORKER_NAME": "worker-1"})
    w2 = start_service("app.worker", "worker2.log",
                       env_extra={"WORKER_NAME": "worker-2"})
    r0 = wait_status(aid_t2, {"passed", "failed"}, 30)
    record("T2c pending job dispatched & passed after Redis recovery",
           r0.get("status") == "passed" and r0.get("actual_sha256") == h_t2,
           f"status={r0.get('status')}")

    print("== T1 happy path ==")
    c1 = b"hello verified world " + uuid.uuid4().bytes
    h1 = sha256_bytes(c1)
    upload(c1, h1, "hello.txt")
    conn = sqlite3.connect(RUNTIME / "data" / "app.db")
    aid1 = conn.execute("SELECT id FROM artifacts ORDER BY created_at DESC LIMIT 1").fetchone()[0]
    conn.close()
    row1 = wait_status(aid1, {"passed"}, 20)
    dl = requests.get(f"{BASE}/artifacts/{aid1}/download", timeout=10)
    record("T1 passed + downloadable with identical bytes",
           row1.get("status") == "passed"
           and dl.status_code == 200 and dl.content == c1
           and dl.headers.get("X-SHA256") == h1,
           f"dl={dl.status_code} status={row1.get('status')}")

    print("== T3 digest mismatch ==")
    c3 = b"mismatch case"
    upload(c3, "00" * 32, "mismatch.bin")
    conn = sqlite3.connect(RUNTIME / "data" / "app.db")
    aid3 = conn.execute("SELECT id FROM artifacts ORDER BY created_at DESC LIMIT 1").fetchone()[0]
    conn.close()
    row3 = wait_status(aid3, {"failed"}, 20)
    dl3 = requests.get(f"{BASE}/artifacts/{aid3}/download", timeout=10)
    record("T3 mismatch -> failed & download 403",
           row3.get("status") == "failed" and dl3.status_code == 403
           and row3.get("actual_sha256") == sha256_bytes(c3)
           and "mismatch" in (row3.get("last_error") or ""),
           f"status={row3.get('status')} dl={dl3.status_code}")

    print("== T4 oversize upload (>2 MiB) ==")
    n_rows_before = len(list_rows())
    big = b"x" * (2 * 1024 * 1024 + 1)
    resp4 = upload(big, sha256_bytes(big), "big.bin")
    record("T4 oversized body rejected and no row created",
           resp4.status_code in (413, 302) and len(list_rows()) == n_rows_before,
           f"status={resp4.status_code} rows_before={n_rows_before} "
           f"rows_after={len(list_rows())}")

    print("== T5 unreadable file -> retry, then recover ==")
    c5 = b"retry me " + uuid.uuid4().bytes
    h5 = sha256_bytes(c5)
    # Restart both workers with the read-failure injection enabled. Uploads
    # meanwhile stay accepted by the web process.
    stop(w1, "worker-1")
    stop(w2, "worker-2")
    w1 = start_service("app.worker", "worker1f.log",
                       env_extra={"WORKER_NAME": "worker-1",
                                  "SIMULATE_READ_FAILURE": "1"})
    w2 = start_service("app.worker", "worker2f.log",
                       env_extra={"WORKER_NAME": "worker-2",
                                  "SIMULATE_READ_FAILURE": "1"})
    upload(c5, h5, "retry.bin")
    conn = sqlite3.connect(RUNTIME / "data" / "app.db")
    aid5 = conn.execute("SELECT id FROM artifacts ORDER BY created_at DESC LIMIT 1").fetchone()[0]
    conn.close()
    deadline = time.time() + 25
    saw_retry = False
    while time.time() < deadline:
        rr = sql_json(aid5)
        if rr["status"] == "pending" and rr["attempts"] >= 1 and rr["last_error"]:
            saw_retry = True
            break
        time.sleep(0.5)
    record("T5a read failure keeps job retryable (pending), never failed",
           saw_retry and sql_json(aid5)["status"] != "failed",
           f"row={sql_json(aid5)['status']} "
           f"err={sql_json(aid5)['last_error']} "
           f"attempts={sql_json(aid5)['attempts']}")
    # Restart workers without the injection; the retry must then pass.
    stop(w1, "worker-1")
    stop(w2, "worker-2")
    w1 = start_service("app.worker", "worker1g.log",
                       env_extra={"WORKER_NAME": "worker-1"})
    w2 = start_service("app.worker", "worker2g.log",
                       env_extra={"WORKER_NAME": "worker-2"})
    row5 = wait_status(aid5, {"passed"}, 40)
    record("T5b after read recovers the retried job passes",
           row5.get("status") == "passed"
           and row5.get("actual_sha256") == h5,
           f"status={row5.get('status')} attempts={row5.get('attempts')}")

    print("== T6 worker SIGKILL -> takeover by the other worker ==")
    stop(w1, "worker-1")
    stop(w2, "worker-2")
    # Slow finalizer so SIGKILL lands while msg is pending-ack and row is
    # processing. Only this fresh worker-1 runs initially.
    w1 = start_service("app.worker", "worker1b.log",
                       env_extra={"WORKER_NAME": "worker-1"},
                       pre_delay_ms=4000)
    c6 = b"takeover case " + uuid.uuid4().bytes
    h6 = sha256_bytes(c6)
    upload(c6, h6, "takeover.bin")
    conn = sqlite3.connect(RUNTIME / "data" / "app.db")
    aid6 = conn.execute("SELECT id FROM artifacts ORDER BY created_at DESC LIMIT 1").fetchone()[0]
    conn.close()
    deadline = time.time() + 15
    while time.time() < deadline and sql_json(aid6)["status"] != "processing":
        time.sleep(0.3)
    # Confirm the message sits in the PEL, then kill -9.
    pel = r.xpending_range("verify:jobs", "verifiers", "-", "+", 10)
    pel_ids = {p["message_id"] for p in pel}
    row6 = sql_json(aid6)
    in_pel = row6.get("stream_id") in pel_ids
    w1.send_signal(signal.SIGKILL)
    w1.wait(timeout=5)
    print(f"  worker-1 SIGKILLed; row processing={row6['status']} "
          f"msg_in_pel={in_pel}")
    record("T6a kill happened with row processing and message unacked",
           row6["status"] == "processing" and in_pel,
           f"status={row6['status']} pel={sorted(pel_ids)}")
    # Start the second worker; XAUTOCLAIM (idle 3s) must take over.
    w2 = start_service("app.worker", "worker2b.log",
                       env_extra={"WORKER_NAME": "worker-2"},
                       pre_delay_ms=0)
    row6b = wait_status(aid6, {"passed"}, 35)
    dl6 = requests.get(f"{BASE}/artifacts/{aid6}/download", timeout=10)
    record("T6b worker-2 claimed the orphan message and job passed once",
           row6b.get("status") == "passed"
           and row6b.get("worker_id", "").endswith("worker-1") is False
           and dl6.status_code == 200 and dl6.content == c6,
           f"status={row6b.get('status')} worker={row6b.get('worker_id')} dl={dl6.status_code}")
    pel_after = r.xpending_range("verify:jobs", "verifiers", "-", "+", 100)
    record("T6c no leftover pending entries for the taken-over job",
           all(p["message_id"] != row6["stream_id"] for p in pel_after),
           f"pel_size={len(pel_after)}")

    print("== T7 duplicate redelivery is idempotent ==")
    before = sql_json(aid1)
    r.xadd("verify:jobs", {"artifact_id": aid1})
    time.sleep(6)  # let a worker consume the duplicate
    after = sql_json(aid1)
    record("T7 duplicate message changes nothing on the confirmed row",
           after["status"] == before["status"] == "passed"
           and after["actual_sha256"] == before["actual_sha256"]
           and after["updated_at"] == before["updated_at"],
           f"updated_at before={before['updated_at']} after={after['updated_at']}")

    print("== T8 full restart preserves confirmed state & eligibility ==")
    stop(web_no_redis, "web")
    stop(w2, "worker-2")
    stop(redis_p, "redis")
    time.sleep(1)
    redis_p = start_redis()
    web2 = start_service("app.web", "web2.log")
    w3 = start_service("app.worker", "worker3.log",
                       env_extra={"WORKER_NAME": "worker-3"})
    w4 = start_service("app.worker", "worker4.log",
                       env_extra={"WORKER_NAME": "worker-4"})
    assert wait_for(f"{BASE}/healthz", 25)
    time.sleep(4)
    dl1 = requests.get(f"{BASE}/artifacts/{aid1}/download", timeout=10)
    dl3 = requests.get(f"{BASE}/artifacts/{aid3}/download", timeout=10)
    rows = {x["id"]: x for x in list_rows()}
    record("T8 states survive restart; passed stays downloadable, failed stays 403",
           rows[aid1]["status"] == "passed" and dl1.status_code == 200
           and dl1.content == c1
           and rows[aid3]["status"] == "failed" and dl3.status_code == 403
           and rows[aid_t2]["status"] == "passed"
           and rows[aid6]["status"] == "passed",
           f"dl1={dl1.status_code} dl3={dl3.status_code}")

    # Final PEL should be empty (everything acknowledged).
    time.sleep(2)
    pel_final = r.xpending_range("verify:jobs", "verifiers", "-", "+", 100)
    record("T9 stream PEL empty at steady state",
           len(pel_final) == 0, f"pel={len(pel_final)}")

    stop(w3, "worker-3")
    stop(w4, "worker-4")
    stop(web2, "web")
    stop(redis_p, "redis")

    print("\n================ SUMMARY ================")
    n_fail = 0
    for name, ok, detail in results:
        print(f"{ok:4} {name}  {detail}")
        if ok == FAIL:
            n_fail += 1
    print(f"\n{len(results) - n_fail}/{len(results)} checks passed")
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
