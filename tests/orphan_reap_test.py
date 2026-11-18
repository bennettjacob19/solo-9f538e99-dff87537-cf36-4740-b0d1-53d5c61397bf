"""Focused check: reconciler reaps `processing` rows whose stream message
vanished (e.g. Redis data loss) and re-dispatches them.

Run: .venv/bin/python tests/orphan_reap_test.py
"""
from __future__ import annotations

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

ROOT = Path(__file__).resolve().parent.parent
RUNTIME = ROOT / "tests" / "runtime-orphan"
REDIS_BIN = "/tmp/redis-build/redis-7.4.2/src/redis-server"
PYTHON = str(ROOT / ".venv" / "bin" / "python")
PORT = 6401
WEB_PORT = 8101


def wait_tcp(host: str, port: int, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.3)
    return False


def main() -> int:
    # Clean leftovers on our ports.
    subprocess.run(["pkill", "-f", f"redis-server.*--port {PORT}"])
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            cmd = (proc / "cmdline").read_bytes().replace(b"\x00", b" ").decode()
        except OSError:
            continue
        if "runtime-orphan" in cmd and "app." in cmd:
            try:
                os.kill(int(proc.name), signal.SIGKILL)
            except OSError:
                pass

    if RUNTIME.exists():
        shutil.rmtree(RUNTIME)
    (RUNTIME / "data").mkdir(parents=True)
    (RUNTIME / "redis").mkdir(parents=True)

    env = os.environ.copy()
    env.update({
        "REDIS_HOST": "127.0.0.1", "REDIS_PORT": str(PORT),
        "DATA_DIR": str(RUNTIME / "data"), "PORT": str(WEB_PORT),
        "CLAIM_MIN_IDLE_MS": "2000", "PROCESSING_STALE_SECONDS": "4",
        "DISPATCH_INTERVAL_SECONDS": "1",
    })
    logr = open(RUNTIME / "redis.log", "ab", buffering=0)
    redis_p = subprocess.Popen(
        [REDIS_BIN, "--port", str(PORT), "--dir",
         str(RUNTIME / "redis"), "--appendonly", "no", "--save", "",
         "--daemonize", "no"],
        stdout=logr, stderr=subprocess.STDOUT)
    assert wait_tcp("127.0.0.1", PORT, 10)

    logs = {}
    procs = []

    def start(module: str, name: str, extra: dict | None = None):
        e = dict(env)
        if extra:
            e.update(extra)
        logs[name] = open(RUNTIME / f"{name}.log", "ab", buffering=0)
        p = subprocess.Popen([PYTHON, "-m", module], cwd=str(ROOT),
                             env=e, stdout=logs[name],
                             stderr=subprocess.STDOUT)
        procs.append((name, p))
        return p

    start("app.web", "web")
    assert wait_tcp("127.0.0.1", WEB_PORT, 15)
    w1 = start("app.worker", "w1", {"WORKER_NAME": "w1",
                                    "PRE_FINALIZE_DELAY_MS": "30000"})

    import hashlib
    import requests
    content = b"orphan reap case " + uuid.uuid4().bytes
    digest = hashlib.sha256(content).hexdigest()
    r = requests.post(
        f"http://127.0.0.1:{WEB_PORT}/upload",
        data={"expected_sha256": digest},
        files={"file": ("o.bin", content, "application/octet-stream")},
        timeout=15, allow_redirects=False)
    assert r.status_code == 302, r.status_code

    import sqlite3
    def row():
        conn = sqlite3.connect(RUNTIME / "data" / "app.db")
        conn.row_factory = sqlite3.Row
        x = conn.execute("SELECT * FROM artifacts ORDER BY created_at DESC LIMIT 1").fetchone()
        conn.close()
        return dict(x)

    deadline = time.time() + 15
    while time.time() < deadline and row()["status"] != "processing":
        time.sleep(0.3)
    art = row()
    print("row stuck at:", art["status"], "stream:", art["stream_id"])
    assert art["status"] == "processing"

    # Kill -9 the worker, then FLUSHALL so the stream/PEL vanish while the
    # SQLite row remains stuck in processing.
    w1.send_signal(signal.SIGKILL)
    w1.wait(timeout=5)
    rc = redis.Redis(host="127.0.0.1", port=PORT, decode_responses=True)
    rc.flushall()
    print("redis flushed; PEL gone, DB row still processing ->",
          row()["status"])

    # Start a fresh worker: nothing to claim (PEL empty). The reconciler must
    # detect the orphan after PROCESSING_STALE_SECONDS, reset to pending and
    # re-dispatch; the worker then passes the job.
    start("app.worker", "w2", {"WORKER_NAME": "w2"})
    deadline = time.time() + 45
    final = row()
    while time.time() < deadline and final["status"] != "passed":
        time.sleep(0.5)
        final = row()
    print("final:", final["status"], "attempts:", final["attempts"],
          "worker:", final["worker_id"])
    ok = (final["status"] == "passed"
          and final["actual_sha256"] == digest
          and final["attempts"] >= 2)
    print("ORPHAN_REAP", "PASS" if ok else "FAIL")

    for name, p in procs:
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
    redis_p.send_signal(signal.SIGTERM)
    redis_p.wait(timeout=10)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
