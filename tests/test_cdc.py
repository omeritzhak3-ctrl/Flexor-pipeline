"""CDC tests.

Exercises every leg of the two-step change-detection state machine:

* backfill (never seen) -> NEW
* incremental no-op (size+mtime match, status done) -> UNCHANGED
* mtime bumped, bytes identical (touched re-upload) -> METADATA_ONLY
* bytes differ from last processing -> CHANGED
* previous attempt in_progress / failed -> CHANGED (forces re-process)
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from email_ingest.cdc import CdcDecision, classify
from email_ingest.identity import compute_source_id, content_sha256
from email_ingest.scanner import ScannedFile
from email_ingest.state import SourceStatus, open_db, transaction


def _scanned(tmp_path: Path, content: bytes = b"hello") -> ScannedFile:
    """Materialize a file under a partition layout and return its record."""
    abspath = tmp_path / "ns" / "timestamp=2024-07-15" / "x.eml"
    abspath.parent.mkdir(parents=True, exist_ok=True)
    abspath.write_bytes(content)
    st = abspath.stat()
    return ScannedFile(
        namespace="ns",
        partition="timestamp=2024-07-15",
        relpath="x.eml",
        abspath=abspath,
        size_bytes=st.st_size,
        mtime_ns=st.st_mtime_ns,
    )


def _insert_ledger(
    conn,
    scanned: ScannedFile,
    *,
    status: str,
    content_hash: str,
    size_override: int | None = None,
    mtime_override: int | None = None,
) -> str:
    source_id = compute_source_id(
        scanned.namespace, scanned.partition, scanned.relpath
    )
    with transaction(conn):
        conn.execute(
            """
            INSERT INTO source_files(
                source_id, namespace, partition, relpath,
                size_bytes, mtime_ns, content_sha256,
                status, first_seen_at, last_processed_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_id,
                scanned.namespace,
                scanned.partition,
                scanned.relpath,
                size_override if size_override is not None else scanned.size_bytes,
                mtime_override if mtime_override is not None else scanned.mtime_ns,
                content_hash,
                status,
                "2024-07-15T00:00:00Z",
                "2024-07-15T00:00:01Z",
            ),
        )
    return source_id


# ---------------------------------------------------------------------------


def test_backfill_new_file(tmp_path: Path) -> None:
    """Empty ledger -> file is NEW; hash is computed and returned."""
    conn = open_db(tmp_path / "state.db")
    try:
        scanned = _scanned(tmp_path, b"payload")

        verdict = classify(conn, scanned)

        assert verdict.decision is CdcDecision.NEW
        assert verdict.previous is None
        assert verdict.content_sha256 == content_sha256(b"payload")
        assert verdict.source_id == compute_source_id(
            "ns", "timestamp=2024-07-15", "x.eml"
        )
    finally:
        conn.close()


def test_incremental_no_op_short_circuits(tmp_path: Path) -> None:
    """Second run with size+mtime+status=done -> UNCHANGED, no hash done."""
    conn = open_db(tmp_path / "state.db")
    try:
        scanned = _scanned(tmp_path, b"payload")
        _insert_ledger(
            conn,
            scanned,
            status=SourceStatus.DONE,
            content_hash=content_sha256(b"payload"),
        )

        verdict = classify(conn, scanned)

        assert verdict.decision is CdcDecision.UNCHANGED
        # Critical: the cheap path must not have hashed the bytes.
        assert verdict.content_sha256 is None
        assert verdict.previous is not None
        assert verdict.previous["status"] == SourceStatus.DONE
    finally:
        conn.close()


def test_touch_same_content_is_metadata_only(tmp_path: Path) -> None:
    """mtime/size diverged but bytes match -> METADATA_ONLY, not reprocessing."""
    conn = open_db(tmp_path / "state.db")
    try:
        scanned = _scanned(tmp_path, b"payload")
        real_hash = content_sha256(b"payload")
        # Pretend the previously-seen file had a different mtime but the
        # same content hash.
        _insert_ledger(
            conn,
            scanned,
            status=SourceStatus.DONE,
            content_hash=real_hash,
            mtime_override=scanned.mtime_ns - 10_000_000,
        )

        verdict = classify(conn, scanned)

        assert verdict.decision is CdcDecision.METADATA_ONLY
        assert verdict.content_sha256 == real_hash
        assert verdict.previous is not None
    finally:
        conn.close()


def test_reupload_with_new_content_is_changed(tmp_path: Path) -> None:
    """Same path, new bytes -> CHANGED."""
    conn = open_db(tmp_path / "state.db")
    try:
        scanned = _scanned(tmp_path, b"NEW payload")
        # Ledger thinks the file used to be smaller with a different hash.
        _insert_ledger(
            conn,
            scanned,
            status=SourceStatus.DONE,
            content_hash=content_sha256(b"OLD"),
            size_override=3,
            mtime_override=scanned.mtime_ns - 10_000_000,
        )

        verdict = classify(conn, scanned)

        assert verdict.decision is CdcDecision.CHANGED
        assert verdict.content_sha256 == content_sha256(b"NEW payload")
    finally:
        conn.close()


def test_in_progress_row_forces_recheck(tmp_path: Path) -> None:
    """If the previous run died mid-way, we re-hash and reprocess regardless
    of whether stat metadata matches."""
    conn = open_db(tmp_path / "state.db")
    try:
        scanned = _scanned(tmp_path, b"payload")
        _insert_ledger(
            conn,
            scanned,
            status=SourceStatus.IN_PROGRESS,
            content_hash=content_sha256(b"payload"),
        )

        verdict = classify(conn, scanned)

        # Same content -> we'd treat it as METADATA_ONLY since hashes
        # match... but content matches a non-done row, so the second-step
        # logic must demote that to CHANGED (re-process required).
        # Our implementation makes the conservative choice: any non-done
        # status forces a content-level check, and a match there is
        # *only* METADATA_ONLY if the prior was done.
        assert verdict.decision in {CdcDecision.CHANGED, CdcDecision.METADATA_ONLY}
        # Must always come with the freshly computed hash, never the
        # cheap-path skip.
        assert verdict.content_sha256 == content_sha256(b"payload")

        # Pin the exact behavior we want: in_progress forces re-process.
        assert verdict.decision is CdcDecision.CHANGED
    finally:
        conn.close()


def test_failed_row_forces_recheck(tmp_path: Path) -> None:
    conn = open_db(tmp_path / "state.db")
    try:
        scanned = _scanned(tmp_path, b"payload")
        _insert_ledger(
            conn,
            scanned,
            status=SourceStatus.FAILED,
            content_hash=content_sha256(b"payload"),
        )

        verdict = classify(conn, scanned)
        assert verdict.decision is CdcDecision.CHANGED
    finally:
        conn.close()


def test_size_change_skips_cheap_path(tmp_path: Path) -> None:
    """If size diverges, the cheap (size, mtime) check must NOT short-circuit
    even when status is done."""
    conn = open_db(tmp_path / "state.db")
    try:
        scanned = _scanned(tmp_path, b"payload-CHANGED")
        _insert_ledger(
            conn,
            scanned,
            status=SourceStatus.DONE,
            content_hash=content_sha256(b"OLD"),
            size_override=3,  # different size on disk vs ledger
        )

        verdict = classify(conn, scanned)
        assert verdict.decision is CdcDecision.CHANGED
        assert verdict.content_sha256 == content_sha256(b"payload-CHANGED")
    finally:
        conn.close()
