"""Crash-recovery tests.

We cover three scenarios:

1. Manual ledger state — an ``in_progress`` row + leftover ``tmp/`` files
   from a previous (simulated) crash. Verify ``recover_startup`` resets
   the row and wipes the tmp directory.

2. A "soft" crash where the main per-source transaction raises mid-way
   (between staging and commit) and ``_mark_failed`` runs. Verify the
   next pipeline invocation completes cleanly with no duplicates.

3. A "hard" crash where even ``_mark_failed`` doesn't run, so the row is
   stuck at ``in_progress``. Verify recovery + the next run resolves it.

The key invariant we're proving in all three cases: pool entries are
content-addressed and idempotent, so a re-attempt produces the same
``email_id -> pool_path`` mapping and no duplicate ``emails`` rows.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from email_ingest import pipeline as pipeline_mod
from email_ingest.config import PipelineConfig
from email_ingest.identity import compute_email_id, pool_relpath
from email_ingest.pipeline import recover_startup, run_pipeline
from email_ingest.state import SourceStatus, open_db, transaction


EML = (
    b"From: a@example.com\r\n"
    b"To: b@example.com\r\n"
    b"Subject: hi\r\n\r\n"
    b"body\r\n"
)


@pytest.fixture
def cfg(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(
        bucket_root=tmp_path / "bucket", state_root=tmp_path / "state"
    )


def _write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


# ---------------------------------------------------------------------------
# 1. Direct unit test of recover_startup
# ---------------------------------------------------------------------------


class TestRecoverStartup:
    def test_resets_in_progress_rows(self, cfg: PipelineConfig) -> None:
        conn = open_db(cfg.db_path)
        try:
            with transaction(conn):
                conn.execute(
                    """
                    INSERT INTO source_files(
                        source_id, namespace, partition, relpath,
                        size_bytes, mtime_ns, content_sha256,
                        status, first_seen_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "sid-stuck",
                        "ns",
                        "timestamp=2024-07-15",
                        "x.eml",
                        10,
                        1,
                        "deadbeef",
                        SourceStatus.IN_PROGRESS,
                        "2024-07-15T00:00:00Z",
                    ),
                )

            report = recover_startup(cfg, conn)
            assert report.in_progress_reset == 1

            status = conn.execute(
                "SELECT status FROM source_files WHERE source_id=?", ("sid-stuck",)
            ).fetchone()[0]
            assert status == SourceStatus.DISCOVERED
        finally:
            conn.close()

    def test_does_not_touch_done_or_failed_rows(self, cfg: PipelineConfig) -> None:
        conn = open_db(cfg.db_path)
        try:
            with transaction(conn):
                for sid, status in [
                    ("sid-done", SourceStatus.DONE),
                    ("sid-failed", SourceStatus.FAILED),
                    ("sid-discovered", SourceStatus.DISCOVERED),
                ]:
                    conn.execute(
                        """
                        INSERT INTO source_files(
                            source_id, namespace, partition, relpath,
                            size_bytes, mtime_ns, content_sha256,
                            status, first_seen_at)
                        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            sid,
                            "ns",
                            "timestamp=2024-07-15",
                            f"{sid}.eml",
                            10,
                            1,
                            "deadbeef",
                            status,
                            "2024-07-15T00:00:00Z",
                        ),
                    )

            report = recover_startup(cfg, conn)
            assert report.in_progress_reset == 0

            rows = {
                r["source_id"]: r["status"]
                for r in conn.execute(
                    "SELECT source_id, status FROM source_files"
                ).fetchall()
            }
            assert rows == {
                "sid-done": SourceStatus.DONE,
                "sid-failed": SourceStatus.FAILED,
                "sid-discovered": SourceStatus.DISCOVERED,
            }
        finally:
            conn.close()

    def test_wipes_tmp_directory(self, cfg: PipelineConfig) -> None:
        # Pre-create the state dirs and drop a junk file in tmp/.
        cfg.tmp_root.mkdir(parents=True, exist_ok=True)
        junk = cfg.tmp_root / "abc123.eml"
        junk.write_bytes(b"half-written")
        nested = cfg.tmp_root / "nested-dir"
        nested.mkdir()
        (nested / "more-junk").write_bytes(b"x")

        conn = open_db(cfg.db_path)
        try:
            report = recover_startup(cfg, conn)
        finally:
            conn.close()

        assert report.tmp_files_wiped == 2  # one file + one dir
        assert cfg.tmp_root.exists()
        assert list(cfg.tmp_root.iterdir()) == []

    def test_no_op_when_clean(self, cfg: PipelineConfig) -> None:
        conn = open_db(cfg.db_path)
        try:
            report = recover_startup(cfg, conn)
        finally:
            conn.close()
        assert report.in_progress_reset == 0
        assert report.tmp_files_wiped == 0


# ---------------------------------------------------------------------------
# 2. "Soft" crash: manifest append raises -> _mark_failed runs
# ---------------------------------------------------------------------------


def test_soft_crash_between_staging_and_commit_recovers(
    cfg: PipelineConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Simulate a failure in the last step of the per-source transaction.

    Expected sequence:
      1st run: pool file written, then manifest append raises, txn rolls
               back, _mark_failed marks the source as 'failed'.
      2nd run: CDC sees status != done -> CHANGED, source is reprocessed,
               pool write is a no-op (idempotent), DB rows are inserted
               normally, source goes to 'done'. No duplicates anywhere.
    """
    _write(cfg.bucket_root / "ns" / "timestamp=2024-07-15" / "x.eml", EML)

    def boom(*args, **kwargs):
        raise RuntimeError("simulated crash before commit")

    # 1st run: explode at the manifest step.
    monkeypatch.setattr(pipeline_mod, "append_manifest_lines", boom)
    conn = open_db(cfg.db_path)
    try:
        with pytest.raises(RuntimeError, match="simulated crash"):
            run_pipeline(cfg, conn)

        # The source row should be marked failed; emails and lineage
        # tables should be empty (txn rolled back).
        source_row = conn.execute(
            "SELECT status, relpath FROM source_files"
        ).fetchone()
        assert source_row["status"] == SourceStatus.FAILED
        assert conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM lineage").fetchone()[0] == 0
    finally:
        conn.close()

    # Pool file must still exist (atomic write happened before the crash).
    expected_email_id = compute_email_id("ns", EML)
    pool_target = cfg.pool_root / pool_relpath(expected_email_id)
    assert pool_target.exists()

    # 2nd run: un-patch and re-run. Should complete cleanly.
    monkeypatch.undo()
    conn = open_db(cfg.db_path)
    try:
        stats = run_pipeline(cfg, conn)
        assert stats.files_changed == 1
        assert stats.emails_staged == 1
        assert stats.skipped == 0

        # Ledger sanity: exactly one row, status done, no duplicates.
        rows = conn.execute(
            "SELECT status, content_sha256 FROM source_files"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["status"] == SourceStatus.DONE

        # Emails / lineage: exactly one each.
        assert conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM lineage").fetchone()[0] == 1
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 3. "Hard" crash: process killed; _mark_failed never runs
# ---------------------------------------------------------------------------


def test_hard_crash_leaves_in_progress_then_recovers(
    cfg: PipelineConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Worst-case simulation: row stuck at in_progress + orphan tmp file."""
    _write(cfg.bucket_root / "ns" / "timestamp=2024-07-15" / "x.eml", EML)

    def boom(*args, **kwargs):
        raise RuntimeError("kill -9 simulation")

    monkeypatch.setattr(pipeline_mod, "append_manifest_lines", boom)
    monkeypatch.setattr(pipeline_mod, "_mark_failed", lambda conn, source_id: None)

    conn = open_db(cfg.db_path)
    try:
        with pytest.raises(RuntimeError):
            run_pipeline(cfg, conn)

        # Row is stuck at in_progress because _mark_failed was a no-op.
        status = conn.execute(
            "SELECT status FROM source_files"
        ).fetchone()["status"]
        assert status == SourceStatus.IN_PROGRESS
    finally:
        conn.close()

    # Sprinkle a leftover junk file into tmp/ as if a previous rename
    # didn't complete.
    cfg.tmp_root.mkdir(parents=True, exist_ok=True)
    (cfg.tmp_root / "leftover.eml").write_bytes(b"half-written bytes")

    # Un-patch everything and re-run.
    monkeypatch.undo()
    conn = open_db(cfg.db_path)
    try:
        stats = run_pipeline(cfg, conn)

        # Recovery: 1 in_progress row reset, 1 tmp file wiped.
        assert stats.recovered_in_progress == 1
        assert stats.recovered_tmp_files == 1

        # The source then re-processes cleanly (CDC sees a non-done row
        # and routes it through the CHANGED path).
        assert stats.files_changed == 1
        assert stats.emails_staged == 1

        assert conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM lineage").fetchone()[0] == 1
        status = conn.execute(
            "SELECT status FROM source_files"
        ).fetchone()["status"]
        assert status == SourceStatus.DONE

        # tmp/ exists but is now empty.
        assert cfg.tmp_root.exists()
        assert list(cfg.tmp_root.iterdir()) == []
    finally:
        conn.close()
