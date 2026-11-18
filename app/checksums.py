"""SHA-256 helpers and validation rules."""
from __future__ import annotations

import hashlib
import os
import re

_CHUNK = 1024 * 1024
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def is_valid_sha256(value: str) -> bool:
    return bool(value) and bool(_SHA256_RE.match(value.strip().lower()))


def sha256_file(path: str) -> str:
    """Hash a stored file.

    Raises OSError on any read failure (missing file, permission error, I/O
    error). Callers must treat that as a retryable infrastructure failure,
    never as a digest mismatch.
    """
    h = hashlib.sha256()
    # opener with O_RDONLY so nothing surprising is executed; follow symlinks
    # only inside the storage dir (uploads are written there directly).
    fd = os.open(path, os.O_RDONLY)
    try:
        while True:
            chunk = os.read(fd, _CHUNK)
            if not chunk:
                break
            h.update(chunk)
    finally:
        os.close(fd)
    return h.hexdigest()
