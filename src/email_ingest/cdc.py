"""Two-step change detection against the ``source_files`` ledger.

For each scanned file we produce a ``CdcVerdict`` that the orchestrator
acts on:

* ``NEW``           — never seen before; needs to be processed and inserted.
* ``UNCHANGED``     — already done with matching ``(size, mtime_ns)``; no I/O.
* ``METADATA_ONLY`` — ``(size, mtime_ns)`` changed but bytes are identical
  (e.g. ``touch`` or re-upload of the same content); refresh metadata,
  do not re-process.
* ``CHANGED``       — bytes differ from what we last processed, or the
  previous attempt was ``in_progress`` / ``failed``; re-process.

The two-step name comes from the I/O profile:

1. **Step 1 (free)** — compare the scanner's ``(size, mtime_ns)`` against
   the ledger row. If they match a row in status ``done``, we return
   ``UNCHANGED`` without touching the bytes.
2. **Step 2 (expensive)** — only when step 1 says "maybe", we hash the
   file and compare to ``content_sha256``.

The hash from step 2 is returned in the verdict so the caller can persist
it without rehashing.
"""

from __future__ import annotations

import enum
import hashlib
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from email_ingest.identity import compute_source_id
from email_ingest.scanner import ScannedFile
from email_ingest.state import SourceStatus


# Read in 1 MiB chunks. Comfortably larger than most emails, small enough
# not to balloon RSS on giant archives.
_HASH_CHUNK_BYTES = 1024 * 1024


class CdcDecision(str, enum.Enum):
    NEW = "new"
    UNCHANGED = "unchanged"
    METADATA_ONLY = "metadata_only"
    CHANGED = "changed"


@dataclass(frozen=True)
class CdcVerdict:
    decision: CdcDecision
    source_id: str
    scanned: ScannedFile
    # Populated whenever we had to read the file in step 2. ``None`` for
    # the cheap-path ``UNCHANGED`` case.
    content_sha256: Optional[str] = None
    # The prior ledger row, if any. Useful for the orchestrator when it
    # needs to update vs insert.
    previous: Optional[sqlite3.Row] = None


def classify(conn: sqlite3.Connection, scanned: ScannedFile) -> CdcVerdict:
    """Return the CDC verdict for one scanned file.

    Pure decision logic — does not mutate the DB. The caller is responsible
    for translating the verdict into INSERT / UPDATE statements.
    """
    source_id = compute_source_id(
        scanned.namespace, scanned.partition, scanned.relpath
    )

    row = conn.execute(
        "SELECT * FROM source_files WHERE source_id = ?", (source_id,)
    ).fetchone()

    if row is None:
        content_hash = _hash_file(scanned.abspath)
        return CdcVerdict(
            decision=CdcDecision.NEW,
            source_id=source_id,
            scanned=scanned,
            content_sha256=content_hash,
            previous=None,
        )

    # Step 1: cheap path. Only short-circuit if the previous run actually
    # finished. We don't trust a stale (size, mtime) for in_progress /
    # failed rows.
    if (
        row["status"] == SourceStatus.DONE
        and row["size_bytes"] == scanned.size_bytes
        and row["mtime_ns"] == scanned.mtime_ns
    ):
        return CdcVerdict(
            decision=CdcDecision.UNCHANGED,
            source_id=source_id,
            scanned=scanned,
            content_sha256=None,
            previous=row,
        )

    # Step 2: bytes-level check.
    content_hash = _hash_file(scanned.abspath)

    if (
        row["status"] == SourceStatus.DONE
        and row["content_sha256"] == content_hash
    ):
        # Touched but content unchanged (e.g. mtime bumped by an OS copy).
        return CdcVerdict(
            decision=CdcDecision.METADATA_ONLY,
            source_id=source_id,
            scanned=scanned,
            content_sha256=content_hash,
            previous=row,
        )

    return CdcVerdict(
        decision=CdcDecision.CHANGED,
        source_id=source_id,
        scanned=scanned,
        content_sha256=content_hash,
        previous=row,
    )


def _hash_file(path: Path) -> str:
    """SHA-256 of a file's bytes, streamed in chunks."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(_HASH_CHUNK_BYTES)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()
