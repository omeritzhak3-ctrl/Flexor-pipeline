"""End-to-end edge-case coverage.

Each test maps to one of the edge cases listed in the assignment and
pins the behavior documented in the design plan.

Edge cases covered here:

1. Same filename appears in different date partitions
2. ZIP containing another ZIP containing email files
3. Two different MBOXes that unpack into files with identical internal names
4. A password-protected ZIP
5. A corrupted/unreadable ZIP
6. Non-email files mixed in with emails (.png / .xlsx)
7. An empty container (ZIP and MBOX with no contents)
10. A deeply nested container chain (ZIP -> ZIP -> MBOX -> emails)

Edge cases 8 (pipeline crash mid-run) and 9 (same file re-uploaded) live
in ``test_crash_recovery.py`` and ``test_pipeline_happy_path.py``
respectively.

The final test, ``test_comprehensive_edge_case_bucket``, materializes
every edge case into a single bucket and asserts the aggregate totals
match the per-case behavior.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable

import pytest

from conftest import (
    EML_A,
    EML_B,
    EML_C,
    EML_D,
    HTML_BODY,
    EdgeCaseBucket,
    force_encrypted_flag,
    make_empty_zip,
    make_mbox,
    make_zip,
    write_file,
)
from email_ingest.config import PipelineConfig
from email_ingest.identity import compute_email_id
from email_ingest.pipeline import run_pipeline
from email_ingest.state import SkipReason, SourceStatus, open_db


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(cfg: PipelineConfig):
    conn = open_db(cfg.db_path)
    try:
        return run_pipeline(cfg, conn), conn
    except Exception:
        conn.close()
        raise


def _skip_reasons(conn: sqlite3.Connection) -> set[str]:
    return {
        r["reason"]
        for r in conn.execute("SELECT reason FROM skipped").fetchall()
    }


def _source_statuses(conn: sqlite3.Connection) -> dict[str, str]:
    return {
        r["relpath"]: r["status"]
        for r in conn.execute("SELECT relpath, status FROM source_files").fetchall()
    }


# ---------------------------------------------------------------------------
# Edge case 1 — same filename in different partitions
# ---------------------------------------------------------------------------


def test_edge_case_1_same_filename_in_different_partitions(
    cfg: PipelineConfig,
) -> None:
    """Both files are kept; each gets its own source_files row. Because
    the bytes differ, two distinct emails land in the pool with separate
    lineage rows."""
    write_file(cfg.bucket_root / "ns" / "timestamp=2024-07-15" / "email.eml", EML_A)
    write_file(cfg.bucket_root / "ns" / "timestamp=2024-07-16" / "email.eml", EML_B)

    stats, conn = _run(cfg)
    try:
        assert stats.files_scanned == 2
        assert stats.files_new == 2
        assert conn.execute("SELECT COUNT(*) FROM source_files").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM lineage").fetchone()[0] == 2
        # Identical filename, distinct partitions, distinct sources.
        relpaths = {
            r["relpath"]
            for r in conn.execute("SELECT relpath FROM source_files").fetchall()
        }
        assert relpaths == {"email.eml"}
    finally:
        conn.close()


def test_edge_case_1b_same_filename_same_content_dedupes_across_partitions(
    cfg: PipelineConfig,
) -> None:
    """If the bytes also match, we collapse to one emails row but keep
    two lineage rows (one per partition)."""
    write_file(cfg.bucket_root / "ns" / "timestamp=2024-07-15" / "email.eml", EML_A)
    write_file(cfg.bucket_root / "ns" / "timestamp=2024-07-16" / "email.eml", EML_A)

    stats, conn = _run(cfg)
    try:
        assert stats.emails_staged == 1
        assert stats.emails_relinked == 1
        assert conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM lineage").fetchone()[0] == 2

        partitions = {
            r["partition"]
            for r in conn.execute("SELECT partition FROM lineage").fetchall()
        }
        assert partitions == {"timestamp=2024-07-15", "timestamp=2024-07-16"}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Edge case 2 — zip containing another zip
# ---------------------------------------------------------------------------


def test_edge_case_2_zip_in_zip(cfg: PipelineConfig) -> None:
    inner = make_zip({"deep.eml": EML_A})
    outer = make_zip({"nested.zip": inner, "top.eml": EML_B})
    write_file(cfg.bucket_root / "ns" / "timestamp=2024-07-15" / "wrapper.zip", outer)

    stats, conn = _run(cfg)
    try:
        assert stats.files_scanned == 1
        assert stats.emails_staged == 2

        rows = conn.execute(
            "SELECT internal_path, container_depth FROM lineage ORDER BY internal_path"
        ).fetchall()
        assert [(r["internal_path"], r["container_depth"]) for r in rows] == [
            ("wrapper.zip!nested.zip!deep.eml", 2),
            ("wrapper.zip!top.eml", 1),
        ]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Edge case 3 — colliding internal MBOX names
# ---------------------------------------------------------------------------


def test_edge_case_3_two_mboxes_with_colliding_internal_names(
    cfg: PipelineConfig,
) -> None:
    """Two MBOXes each produce an "0"-indexed message, but the content
    differs; we end up with two distinct emails whose lineage paths
    unambiguously trace back to the right source."""
    p = cfg.bucket_root / "ns" / "timestamp=2024-07-15"
    write_file(p / "a.mbox", make_mbox([EML_A, EML_B]))
    write_file(p / "b.mbox", make_mbox([EML_C, EML_D]))

    stats, conn = _run(cfg)
    try:
        assert stats.emails_staged == 4

        rows = conn.execute(
            "SELECT internal_path FROM lineage ORDER BY internal_path"
        ).fetchall()
        paths = [r["internal_path"] for r in rows]
        assert paths == [
            "a.mbox#0",
            "a.mbox#1",
            "b.mbox#0",
            "b.mbox#1",
        ]

        # Sanity: no two emails share the same email_id.
        ids = {
            r["email_id"]
            for r in conn.execute("SELECT email_id FROM emails").fetchall()
        }
        assert len(ids) == 4
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Edge case 4 — password-protected ZIP
# ---------------------------------------------------------------------------


def test_edge_case_4_password_protected_zip(cfg: PipelineConfig) -> None:
    """Source is recorded as done with zero emails extracted; one
    ``password_protected`` skip row is logged."""
    payload = force_encrypted_flag(make_zip({"secret.eml": EML_A}))
    write_file(cfg.bucket_root / "ns" / "timestamp=2024-07-15" / "locked.zip", payload)

    stats, conn = _run(cfg)
    try:
        assert stats.emails_staged == 0
        assert stats.skipped == 1

        assert _skip_reasons(conn) == {SkipReason.PASSWORD_PROTECTED}
        assert _source_statuses(conn) == {"locked.zip": SourceStatus.DONE}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Edge case 5 — corrupt ZIP
# ---------------------------------------------------------------------------


def test_edge_case_5_corrupt_zip(cfg: PipelineConfig) -> None:
    """Truncated ZIP. Source goes to done, one ``corrupt_archive`` skip
    row; the pipeline keeps going for everything else."""
    truncated = make_zip({"victim.eml": EML_A})[: 16]
    p = cfg.bucket_root / "ns" / "timestamp=2024-07-15"
    write_file(p / "bad.zip", truncated)
    write_file(p / "good.eml", EML_B)

    stats, conn = _run(cfg)
    try:
        assert stats.emails_staged == 1  # good.eml
        assert stats.skipped == 1

        assert _skip_reasons(conn) == {SkipReason.CORRUPT_ARCHIVE}
        statuses = _source_statuses(conn)
        assert statuses["bad.zip"] == SourceStatus.DONE
        assert statuses["good.eml"] == SourceStatus.DONE
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Edge case 6 — non-email files mixed in
# ---------------------------------------------------------------------------


def test_edge_case_6_noise_files_marked_not_an_email(cfg: PipelineConfig) -> None:
    p = cfg.bucket_root / "ns" / "timestamp=2024-07-15"
    write_file(p / "noise.png", b"\x89PNG\r\n")
    write_file(p / "noise.xlsx", b"PK\x03\x04xlsx-fake")
    write_file(p / "good.eml", EML_A)
    # Also a non-email file inside a zip:
    write_file(
        p / "mixed.zip",
        make_zip({"document.pdf": b"%PDF-1.4", "real.eml": EML_B}),
    )

    stats, conn = _run(cfg)
    try:
        # good.eml + mixed.zip!real.eml = 2 emails
        assert stats.emails_staged == 2
        # noise.png + noise.xlsx + mixed.zip!document.pdf = 3 not_an_email
        assert _skip_reasons(conn) == {SkipReason.NOT_AN_EMAIL}
        skipped_paths = {
            r["internal_path"]
            for r in conn.execute("SELECT internal_path FROM skipped").fetchall()
        }
        assert skipped_paths == {"noise.png", "noise.xlsx", "mixed.zip!document.pdf"}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Edge case 7 — empty container
# ---------------------------------------------------------------------------


def test_edge_case_7_empty_zip_and_mbox(cfg: PipelineConfig) -> None:
    p = cfg.bucket_root / "ns" / "timestamp=2024-07-15"
    write_file(p / "empty.zip", make_empty_zip())
    write_file(p / "empty.mbox", b"")

    stats, conn = _run(cfg)
    try:
        assert stats.emails_staged == 0
        assert stats.skipped == 2
        assert _skip_reasons(conn) == {SkipReason.EMPTY_CONTAINER}

        # Both sources are marked done — empty isn't an error.
        assert set(_source_statuses(conn).values()) == {SourceStatus.DONE}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Edge case 10 — deeply nested container chain
# ---------------------------------------------------------------------------


def test_edge_case_10_deeply_nested_chain(cfg: PipelineConfig) -> None:
    """ZIP -> ZIP -> ZIP -> MBOX -> emails. Lineage path accumulates and
    container_depth ends at 4 for the leaves."""
    mbox_blob = make_mbox([EML_A, EML_B])
    z3 = make_zip({"mail.mbox": mbox_blob})
    z2 = make_zip({"z3.zip": z3})
    z1 = make_zip({"z2.zip": z2})
    write_file(cfg.bucket_root / "ns" / "timestamp=2024-07-15" / "deep.zip", z1)

    stats, conn = _run(cfg)
    try:
        assert stats.emails_staged == 2
        rows = conn.execute(
            "SELECT internal_path, container_depth FROM lineage ORDER BY internal_path"
        ).fetchall()
        assert [(r["internal_path"], r["container_depth"]) for r in rows] == [
            ("deep.zip!z2.zip!z3.zip!mail.mbox#0", 4),
            ("deep.zip!z2.zip!z3.zip!mail.mbox#1", 4),
        ]
    finally:
        conn.close()


def test_edge_case_10b_depth_cap_enforced(
    cfg: PipelineConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the chain exceeds MAX_CONTAINER_DEPTH the offending container
    is skipped with ``depth_limit_exceeded`` and the pipeline continues."""
    from email_ingest import unpacker as up

    monkeypatch.setattr(up, "MAX_CONTAINER_DEPTH", 2)

    inner = make_zip({"deep.eml": EML_A})
    mid = make_zip({"inner.zip": inner})
    outer = make_zip({"mid.zip": mid})
    write_file(cfg.bucket_root / "ns" / "timestamp=2024-07-15" / "root.zip", outer)

    stats, conn = _run(cfg)
    try:
        # Top opens to mid (depth 1), mid opens to inner (depth 2);
        # opening inner would push the .eml to depth 3 > 2.
        assert stats.emails_staged == 0
        assert _skip_reasons(conn) == {SkipReason.DEPTH_LIMIT_EXCEEDED}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Comprehensive — every documented edge case at once
# ---------------------------------------------------------------------------


def test_comprehensive_edge_case_bucket(edge_case_bucket: EdgeCaseBucket) -> None:
    """Materialize every edge case into one bucket and verify the totals.

    This is the integration test the plan calls out. If a future change
    silently breaks one of the documented behaviors, the aggregate
    numbers will move and this test will catch it.
    """
    ecb = edge_case_bucket
    stats, conn = _run(ecb.cfg)
    try:
        # All scanned source files end up in source_files (regardless of
        # whether they produced emails or skips).
        assert (
            conn.execute("SELECT COUNT(*) FROM source_files").fetchone()[0]
            == ecb.expected_sources
        )

        # And all of them end at status = done; the pipeline doesn't
        # leave anything dangling.
        assert set(_source_statuses(conn).values()) == {SourceStatus.DONE}

        # Email totals.
        assert (
            conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
            == ecb.expected_emails
        )
        assert stats.skipped == ecb.expected_skipped

        # Skip reasons present: every documented reason that the bucket
        # exercises should be represented.
        reasons = _skip_reasons(conn)
        assert reasons == {
            SkipReason.NOT_AN_EMAIL,
            SkipReason.EMPTY_CONTAINER,
            SkipReason.CORRUPT_ARCHIVE,
            SkipReason.PASSWORD_PROTECTED,
            SkipReason.UNSUPPORTED_FORMAT_DEFERRED,
        }

        # Manifest cross-check: for every email currently in the DB,
        # there's at least one manifest line in some partition under its
        # namespace pointing at it.
        manifest_ids: set[str] = set()
        for manifest in ecb.cfg.manifests_root.rglob("manifest.jsonl"):
            for line in manifest.read_text().splitlines():
                record = json.loads(line)
                manifest_ids.add(record["email_id"])
        db_ids = {
            r["email_id"]
            for r in conn.execute("SELECT email_id FROM emails").fetchall()
        }
        assert manifest_ids == db_ids
    finally:
        conn.close()


def test_comprehensive_idempotent_second_run(
    edge_case_bucket: EdgeCaseBucket,
) -> None:
    """The whole edge-case bucket should be a no-op on the second run."""
    ecb = edge_case_bucket
    stats, conn = _run(ecb.cfg)
    conn.close()

    stats2, conn = _run(ecb.cfg)
    try:
        assert stats2.files_scanned == ecb.expected_sources
        assert stats2.files_unchanged == ecb.expected_sources
        assert stats2.emails_staged == 0
        assert stats2.emails_relinked == 0
        assert stats2.skipped == 0
    finally:
        conn.close()
