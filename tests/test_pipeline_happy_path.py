"""End-to-end happy-path tests for the pipeline orchestrator.

We build synthetic buckets in ``tmp_path`` and verify:

* Every leaf email lands in the pool, exactly once, content-addressed.
* The ``emails`` / ``lineage`` / ``skipped`` / ``source_files`` tables end
  up in the expected shape.
* Per-partition manifests contain one line per (source, email) pair.
* A second run is a no-op (CDC short-circuits).
* Same content across two partitions: one ``emails`` row, two lineage
  rows, two manifest entries (one per partition).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from conftest import (
    EML_A,
    EML_B,
    EML_C,
    make_mbox as _make_mbox,
    make_zip as _make_zip,
    write_file as _write,
)
from email_ingest.config import PipelineConfig
from email_ingest.identity import compute_email_id, pool_relpath
from email_ingest.pipeline import run_pipeline
from email_ingest.state import SourceStatus, open_db


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_processes_provided_fixture_shape(cfg: PipelineConfig) -> None:
    """A realistic bucket: an EML, an HTML, a ZIP, and an MBOX in one
    partition; another EML in a second partition."""
    p1 = cfg.bucket_root / "ns_a" / "timestamp=2024-07-15"
    _write(p1 / "simple.eml", EML_A)
    _write(p1 / "page.html", b"<html><body>hi</body></html>")
    _write(p1 / "batch.zip", _make_zip({"invoice.eml": EML_B, "receipt.eml": EML_C}))
    _write(p1 / "convs.mbox", _make_mbox([EML_A, EML_B]))  # A & B reappear

    p2 = cfg.bucket_root / "ns_a" / "timestamp=2024-07-16"
    _write(p2 / "another.eml", b"From: d@example.com\r\nSubject: D\r\n\r\nbodyD\r\n")

    conn = open_db(cfg.db_path)
    try:
        stats = run_pipeline(cfg, conn)
    finally:
        conn.close()

    # 5 source files
    assert stats.files_scanned == 5
    assert stats.files_new == 5
    assert stats.files_unchanged == 0

    # Email leaves:
    #   simple.eml          -> A
    #   page.html           -> html
    #   batch.zip           -> B, C
    #   convs.mbox          -> A (dupe), B (dupe)
    #   another.eml         -> D
    # Distinct contents in the pool: A, B, C, html, D == 5
    conn = open_db(cfg.db_path)
    try:
        n_emails = conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0]
        assert n_emails == 5

        # Lineage rows: 4 + 1 + 2 + 2 from p1 plus 1 in p2 = unique
        # (email_id, source_id, internal_path) triples.
        n_lineage = conn.execute("SELECT COUNT(*) FROM lineage").fetchone()[0]
        # simple(1) + page(1) + batch(2) + convs(2) + another(1) = 7
        assert n_lineage == 7

        # All source rows should be 'done'.
        rows = conn.execute(
            "SELECT relpath, status FROM source_files ORDER BY relpath"
        ).fetchall()
        assert {r["status"] for r in rows} == {SourceStatus.DONE}

        # Email A appears in two sources (simple.eml and convs.mbox#0) in
        # the same partition; both should produce lineage rows under the
        # same email_id.
        email_id_a = compute_email_id("ns_a", EML_A)
        lineage_for_a = conn.execute(
            "SELECT source_id, internal_path FROM lineage WHERE email_id=?",
            (email_id_a,),
        ).fetchall()
        ips = {r["internal_path"] for r in lineage_for_a}
        assert ips == {"simple.eml", "convs.mbox#0"}
    finally:
        conn.close()

    # Pool sanity: every emails.pool_path corresponds to a real file.
    conn = open_db(cfg.db_path)
    try:
        for row in conn.execute("SELECT pool_path FROM emails").fetchall():
            pool_rel = row["pool_path"]
            assert pool_rel.startswith("emails/")
            target = cfg.staging_root / pool_rel
            assert target.exists(), f"missing pool file: {target}"
    finally:
        conn.close()

    # Manifest sanity: partition 1 has 6 lines (4 sources contributing
    # 1+1+2+2 lineage rows). Partition 2 has 1.
    m1 = cfg.manifests_root / "ns_a" / "timestamp=2024-07-15" / "manifest.jsonl"
    m2 = cfg.manifests_root / "ns_a" / "timestamp=2024-07-16" / "manifest.jsonl"
    assert m1.exists()
    assert m2.exists()
    lines1 = [json.loads(l) for l in m1.read_text().splitlines()]
    lines2 = [json.loads(l) for l in m2.read_text().splitlines()]
    assert len(lines1) == 6
    assert len(lines2) == 1
    # Each manifest line has the canonical shape.
    sample = lines1[0]
    assert set(sample) == {"email_id", "pool_path", "format", "lineage"}
    assert sample["pool_path"].startswith("emails/")


def test_idempotent_second_run_is_noop(cfg: PipelineConfig) -> None:
    p1 = cfg.bucket_root / "ns" / "timestamp=2024-07-15"
    _write(p1 / "x.eml", EML_A)

    conn = open_db(cfg.db_path)
    try:
        first = run_pipeline(cfg, conn)
        assert first.files_new == 1
        assert first.emails_staged == 1

        second = run_pipeline(cfg, conn)
    finally:
        conn.close()

    assert second.files_scanned == 1
    assert second.files_unchanged == 1
    assert second.files_new == 0
    assert second.emails_staged == 0
    assert second.emails_relinked == 0


def test_cross_partition_dedup(cfg: PipelineConfig) -> None:
    """Same email bytes uploaded to two partitions: one emails row, two
    lineage rows, two manifests with one entry each."""
    p1 = cfg.bucket_root / "ns" / "timestamp=2024-07-15" / "x.eml"
    p2 = cfg.bucket_root / "ns" / "timestamp=2024-07-16" / "x.eml"
    _write(p1, EML_A)
    _write(p2, EML_A)

    conn = open_db(cfg.db_path)
    try:
        stats = run_pipeline(cfg, conn)
    finally:
        conn.close()

    assert stats.files_new == 2
    assert stats.emails_staged == 1  # second one is a relink
    assert stats.emails_relinked == 1

    conn = open_db(cfg.db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM lineage").fetchone()[0] == 2

        partitions = {
            r["partition"]
            for r in conn.execute("SELECT partition FROM lineage").fetchall()
        }
        assert partitions == {"timestamp=2024-07-15", "timestamp=2024-07-16"}
    finally:
        conn.close()

    # One manifest per partition, each with one line.
    for partition in ("timestamp=2024-07-15", "timestamp=2024-07-16"):
        path = cfg.manifests_root / "ns" / partition / "manifest.jsonl"
        lines = path.read_text().splitlines()
        assert len(lines) == 1


def test_cross_tenant_does_not_dedup(cfg: PipelineConfig) -> None:
    """Two customers uploading the same email each get their own staged copy."""
    _write(cfg.bucket_root / "ns_a" / "timestamp=2024-07-15" / "x.eml", EML_A)
    _write(cfg.bucket_root / "ns_b" / "timestamp=2024-07-15" / "x.eml", EML_A)

    conn = open_db(cfg.db_path)
    try:
        run_pipeline(cfg, conn)
        assert conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0] == 2
        # Two distinct pool files (different email_ids).
        rows = conn.execute("SELECT pool_path FROM emails").fetchall()
        assert len({r["pool_path"] for r in rows}) == 2
    finally:
        conn.close()


def test_skipped_rows_recorded(cfg: PipelineConfig) -> None:
    p1 = cfg.bucket_root / "ns" / "timestamp=2024-07-15"
    _write(p1 / "noise.png", b"\x89PNG\r\n")
    _write(p1 / "bad.zip", b"not a zip")
    _write(p1 / "junk.msg", b"\xd0\xcf\x11\xe0")
    _write(p1 / "good.eml", EML_A)

    conn = open_db(cfg.db_path)
    try:
        stats = run_pipeline(cfg, conn)
    finally:
        conn.close()

    assert stats.emails_staged == 1
    assert stats.skipped == 3

    conn = open_db(cfg.db_path)
    try:
        reasons = {
            r["reason"]
            for r in conn.execute("SELECT reason FROM skipped").fetchall()
        }
        assert reasons == {"not_an_email", "corrupt_archive", "unsupported_format_deferred"}
    finally:
        conn.close()


def test_re_upload_with_new_content_reprocesses(cfg: PipelineConfig) -> None:
    p = cfg.bucket_root / "ns" / "timestamp=2024-07-15" / "x.eml"
    _write(p, EML_A)
    conn = open_db(cfg.db_path)
    try:
        run_pipeline(cfg, conn)
        assert conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0] == 1

        # Overwrite with new content; bump mtime by re-writing.
        import os
        import time
        time.sleep(0.01)
        _write(p, EML_B)
        # Force mtime change beyond filesystem resolution
        future = time.time() + 1
        os.utime(p, (future, future))

        stats = run_pipeline(cfg, conn)
        assert stats.files_changed == 1

        # Now two distinct emails for the same tenant.
        assert conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0] == 2
        # And two lineage rows referencing the same source_id.
        n_lineage = conn.execute("SELECT COUNT(*) FROM lineage").fetchone()[0]
        assert n_lineage == 2
    finally:
        conn.close()
