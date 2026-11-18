"""Flask web UI: upload (file + expected SHA-256), status lists, downloads.

Only artifacts whose confirmed status is ``passed`` can be downloaded.
Uploads are persisted (file + DB row in ``pending``) before any queue
interaction; the reconciler thread guarantees eventual dispatch even when
Redis is down or the process restarts.
"""
from __future__ import annotations

import logging
import os
import uuid
from pathlib import Path

from flask import (Flask, Response, abort, flash, redirect, render_template,
                   request, send_file, url_for)
from werkzeug.utils import secure_filename

from . import checksums, config, db
from .reconciler import Reconciler

log = logging.getLogger("web")


def create_app() -> Flask:
    config.ensure_dirs()
    db.init_db()

    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = config.MAX_CONTENT_LENGTH
    app.secret_key = os.environ.get("FLASK_SECRET", "dev-only-secret")

    reconciler = Reconciler()
    reconciler.start()

    # ------------------------------------------------------------------ #
    @app.errorhandler(413)
    def too_large(_err):
        flash(f"文件超过 {config.MAX_CONTENT_LENGTH // (1024 * 1024)} MiB 限制，"
              "上传未受理。", "error")
        return redirect(url_for("index"))

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/")
    def index():
        groups = {
            "pending": db.list_by_status(db.ST_PENDING)
            + db.list_by_status(db.ST_PROCESSING),
            "passed": db.list_by_status(db.ST_PASSED),
            "failed": db.list_by_status(db.ST_FAILED),
        }
        return render_template(
            "index.html",
            groups=groups,
            limit_mib=config.MAX_CONTENT_LENGTH / (1024 * 1024),
        )

    @app.post("/upload")
    def upload():
        f = request.files.get("file")
        expected = (request.form.get("expected_sha256") or "").strip().lower()
        if f is None or not f.filename:
            flash("未选择文件。", "error")
            return redirect(url_for("index"))
        if not checksums.is_valid_sha256(expected):
            flash("预期 SHA-256 必须是 64 位十六进制字符串。", "error")
            return redirect(url_for("index"))

        original_name = secure_filename(f.filename) or "upload.bin"
        artifact_id = uuid.uuid4().hex
        stored_path = config.STORAGE_DIR / artifact_id


        # Stream to disk first, then create the durable pending DB row.
        # MAX_CONTENT_LENGTH already guarantees <= 2 MiB; Werkzeug aborts
        # with 413 mid-stream if the body is larger.
        f.save(stored_path)
        try:
            db.create_artifact(original_name, stored_path, expected)
        except Exception:
            log.exception("failed to persist upload %s", artifact_id)
            stored_path.unlink(missing_ok=True)
            raise
        log.info("accepted upload %s (%s), pending verification",
                 artifact_id, original_name)
        flash(f"已受理上传 {artifact_id[:8]}…，校验任务将自动派发。", "ok")
        return redirect(url_for("index"))

    @app.get("/artifacts/<artifact_id>/download")
    def download(artifact_id):
        row = db.get_artifact(artifact_id)
        if row is None:
            abort(404)
        # Download eligibility is decided solely by the confirmed DB status.
        if row["status"] != db.ST_PASSED:
            abort(403, description="仅校验通过的文件可下载")
        path = Path(row["stored_path"])
        if not path.is_file():
            # A passed row whose file vanished is an operational incident;
            # never fall back to another artifact.
            log.error("passed artifact %s file missing at %s",
                      artifact_id, path)
            abort(404)
        resp: Response = send_file(
            path, as_attachment=True, download_name=row["original_name"]
        )
        resp.headers["X-Artifact-Id"] = artifact_id
        resp.headers["X-SHA256"] = row["actual_sha256"] or ""
        return resp

    return app


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    app = create_app()
    # waitress gives a production-grade threaded WSGI server; the reconciler
    # runs as its own daemon thread inside this process.
    from waitress import serve
    serve(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")),
          threads=8)


if __name__ == "__main__":
    main()
