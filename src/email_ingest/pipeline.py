"""End-to-end orchestrator.

Per-source-file protocol (mirrors the plan's crash-safety section):

1. Run CDC over the scanned file. Skip work if ``UNCHANGED``; bump
   metadata only if ``METADATA_ONLY``.
2. Mark the ``source_files`` row as ``in_progress`` in its own transaction
   (this is the breadcrumb the Phase-5 recovery code latches onto).
3. Unpack the bytes (in memory; see P1 notes about streaming).
4. For every extracted email leaf:
     a. Compute ``email_id`` (tenant-scoped over canonicalized bytes).
     b. Atomically stage the bytes to the content-addressed pool. Writes
        are idempotent — if a previous attempt got this far, the rename
        is a no-op.
5. In a *single* DB transaction:
     a. Insert ``emails`` rows (``INSERT OR IGNORE``).
     b. Insert ``lineage`` rows (``INSERT OR IGNORE`` on UNIQUE constraint).
     c. Insert ``skipped`` rows.
     d. Update the ``source_files`` row to ``done`` with the freshly
        observed (size, mtime, content_sha256) and ``last_processed_at``.
     e. Append one ``manifest.jsonl`` line per email staged or re-linked
        in this run (fsync'd before commit).
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from email_ingest.cdc import CdcDecision, CdcVerdict, classify
from email_ingest.config import PipelineConfig
from email_ingest.identity import (
    compute_email_id,
    content_sha256 as content_sha256_of,
    pool_relpath,
)
from email_ingest.scanner import ScannedFile, scan_bucket
from email_ingest.staging import (
    ManifestLine,
    ManifestLineage,
    append_manifest_lines,
    stage_email_bytes,
)
from email_ingest.state import SourceStatus, transaction
from email_ingest.unpacker import (
    ExtractedEmail,
    Skipped,
    unpack_source_file,
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@dataclass
class RunStats:
    files_scanned: int = 0
    files_new: int = 0
    files_changed: int = 0
    files_unchanged: int = 0
    files_metadata_only: int = 0
    files_failed: int = 0
    emails_staged: int = 0      # newly written to the pool in this run
    emails_relinked: int = 0    # existing pool entry, new lineage row in this run
    skipped: int = 0

    def merge_decision(self, decision: CdcDecision) -> None:
        if decision is CdcDecision.NEW:
            self.files_new += 1
        elif decision is CdcDecision.CHANGED:
            self.files_changed += 1
        elif decision is CdcDecision.UNCHANGED:
            self.files_unchanged += 1
        elif decision is CdcDecision.METADATA_ONLY:
            self.files_metadata_only += 1


def run_pipeline(config: PipelineConfig, conn: sqlite3.Connection) -> RunStats:
    """Process every file currently visible under ``config.bucket_root``.

    Idempotent: calling twice with no on-disk changes only does the cheap
    CDC stat pass.
    """
    stats = RunStats()
    for scanned in scan_bucket(config.bucket_root):
        stats.files_scanned += 1
        verdict = classify(conn, scanned)
        stats.merge_decision(verdict.decision)

        if verdict.decision is CdcDecision.UNCHANGED:
            continue
        if verdict.decision is CdcDecision.METADATA_ONLY:
            _refresh_metadata(conn, verdict)
            continue

        try:
            _process_source(config, conn, verdict, stats)
        except Exception:
            _mark_failed(conn, verdict.source_id)
            stats.files_failed += 1
            raise
    return stats


# ---------------------------------------------------------------------------
# Per-source-file flow
# ---------------------------------------------------------------------------


def _process_source(
    config: PipelineConfig,
    conn: sqlite3.Connection,
    verdict: CdcVerdict,
    stats: RunStats,
) -> None:
    now = _utcnow()

    # Step 2: mark in_progress (own transaction).
    _upsert_source_in_progress(conn, verdict, observed_at=now)

    # Step 3: unpack from disk.
    unpacked = unpack_source_file(verdict.scanned.abspath, verdict.scanned.relpath)

    # Step 4: stage every leaf to the pool (idempotent).
    staged_records: List[_StagedEmail] = []
    for email in unpacked.emails:
        record = _stage_one(config, verdict.scanned.namespace, email)
        staged_records.append(record)

    # Step 5: single DB+manifest transaction.
    _commit_results(
        config=config,
        conn=conn,
        verdict=verdict,
        staged=staged_records,
        skipped=unpacked.skipped,
        observed_at=now,
        stats=stats,
    )


@dataclass
class _StagedEmail:
    """Bookkeeping for one email leaf that has been written to the pool."""

    email: ExtractedEmail
    email_id: str
    raw_sha256: str
    pool_path: str          # pool-relative, e.g. "ab/abc...eml"
    suffix: str             # ".eml" or ".html"


def _stage_one(
    config: PipelineConfig, namespace: str, email: ExtractedEmail
) -> _StagedEmail:
    email_id = compute_email_id(namespace, email.raw_bytes)
    raw_sha = content_sha256_of(email.raw_bytes)
    suffix = ".html" if email.format == "html" else ".eml"
    pool_rel = stage_email_bytes(
        config, email_id, email.raw_bytes, suffix=suffix
    )
    return _StagedEmail(
        email=email,
        email_id=email_id,
        raw_sha256=raw_sha,
        pool_path=pool_rel,
        suffix=suffix,
    )


def _commit_results(
    *,
    config: PipelineConfig,
    conn: sqlite3.Connection,
    verdict: CdcVerdict,
    staged: List[_StagedEmail],
    skipped: List[Skipped],
    observed_at: str,
    stats: RunStats,
) -> None:
    """Perform the single per-source-file commit (DB + manifest)."""
    source_id = verdict.source_id
    namespace = verdict.scanned.namespace
    partition = verdict.scanned.partition

    # The set of email_ids we've already inserted in *this* call; used to
    # collapse duplicates within a single source file (two MBOX members
    # with identical canonical bytes, for instance).
    seen_in_this_source: set[str] = set()
    manifest_lines: List[ManifestLine] = []

    with transaction(conn):
        for s in staged:
            existed_before = _email_exists(conn, s.email_id)

            if not existed_before and s.email_id not in seen_in_this_source:
                conn.execute(
                    """
                    INSERT INTO emails(
                        email_id, namespace, content_sha256, pool_path,
                        size_bytes, format, first_staged_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        s.email_id,
                        namespace,
                        s.raw_sha256,
                        f"emails/{s.pool_path}",
                        len(s.email.raw_bytes),
                        s.email.format,
                        observed_at,
                    ),
                )
                stats.emails_staged += 1
            else:
                stats.emails_relinked += 1

            seen_in_this_source.add(s.email_id)

            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO lineage(
                    email_id, source_id, partition, internal_path,
                    container_depth, discovered_at)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (
                    s.email_id,
                    source_id,
                    partition,
                    s.email.internal_path,
                    s.email.container_depth,
                    observed_at,
                ),
            )

            # Only manifest lines for *newly recorded* lineage; a re-run
            # that hits the UNIQUE constraint shouldn't double-log.
            if cursor.rowcount > 0:
                manifest_lines.append(
                    ManifestLine(
                        email_id=s.email_id,
                        pool_path=f"emails/{s.pool_path}",
                        format=s.email.format,
                        lineage=[
                            ManifestLineage(
                                source=verdict.scanned.relpath,
                                internal_path=s.email.internal_path,
                                depth=s.email.container_depth,
                            )
                        ],
                    )
                )

        for sk in skipped:
            conn.execute(
                """
                INSERT INTO skipped(
                    source_id, internal_path, reason, details, skipped_at)
                VALUES(?, ?, ?, ?, ?)
                """,
                (source_id, sk.internal_path, sk.reason, sk.details, observed_at),
            )
            stats.skipped += 1

        conn.execute(
            """
            UPDATE source_files
               SET status = ?,
                   size_bytes = ?,
                   mtime_ns = ?,
                   content_sha256 = ?,
                   last_processed_at = ?
             WHERE source_id = ?
            """,
            (
                SourceStatus.DONE,
                verdict.scanned.size_bytes,
                verdict.scanned.mtime_ns,
                verdict.content_sha256,
                observed_at,
                source_id,
            ),
        )

        # Manifest append is part of the same logical step — if it raises,
        # the surrounding transaction rolls back.
        append_manifest_lines(config, namespace, partition, manifest_lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _upsert_source_in_progress(
    conn: sqlite3.Connection, verdict: CdcVerdict, *, observed_at: str
) -> None:
    """Insert a NEW row or update an existing one to ``in_progress``.

    Runs in its own transaction so a crash between marking and committing
    leaves a recoverable breadcrumb.
    """
    scanned = verdict.scanned
    with transaction(conn):
        if verdict.previous is None:
            conn.execute(
                """
                INSERT INTO source_files(
                    source_id, namespace, partition, relpath,
                    size_bytes, mtime_ns, content_sha256,
                    status, first_seen_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    verdict.source_id,
                    scanned.namespace,
                    scanned.partition,
                    scanned.relpath,
                    scanned.size_bytes,
                    scanned.mtime_ns,
                    verdict.content_sha256,
                    SourceStatus.IN_PROGRESS,
                    observed_at,
                ),
            )
        else:
            conn.execute(
                """
                UPDATE source_files
                   SET size_bytes = ?,
                       mtime_ns = ?,
                       content_sha256 = ?,
                       status = ?
                 WHERE source_id = ?
                """,
                (
                    scanned.size_bytes,
                    scanned.mtime_ns,
                    verdict.content_sha256,
                    SourceStatus.IN_PROGRESS,
                    verdict.source_id,
                ),
            )


def _refresh_metadata(conn: sqlite3.Connection, verdict: CdcVerdict) -> None:
    """METADATA_ONLY path: update (size, mtime) but do not re-process."""
    now = _utcnow()
    with transaction(conn):
        conn.execute(
            """
            UPDATE source_files
               SET size_bytes = ?,
                   mtime_ns = ?,
                   last_processed_at = ?
             WHERE source_id = ?
            """,
            (
                verdict.scanned.size_bytes,
                verdict.scanned.mtime_ns,
                now,
                verdict.source_id,
            ),
        )


def _mark_failed(conn: sqlite3.Connection, source_id: str) -> None:
    """Flip status to ``failed`` so the next run knows to retry it."""
    try:
        with transaction(conn):
            conn.execute(
                "UPDATE source_files SET status = ? WHERE source_id = ?",
                (SourceStatus.FAILED, source_id),
            )
    except sqlite3.Error:
        # If we can't even record the failure, there's nothing else to do.
        pass


def _email_exists(conn: sqlite3.Connection, email_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM emails WHERE email_id = ?", (email_id,)
    ).fetchone()
    return row is not None


def _utcnow() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


__all__ = ["RunStats", "run_pipeline"]
