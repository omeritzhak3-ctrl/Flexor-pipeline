"""State / schema migration tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from email_ingest.state import (
    SCHEMA_VERSION,
    SkipReason,
    SourceStatus,
    open_db,
    transaction,
)


def _table_names(conn) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    return {r[0] for r in rows}


def _index_names(conn) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'"
    ).fetchall()
    return {r[0] for r in rows}


def test_open_creates_schema(tmp_path: Path) -> None:
    conn = open_db(tmp_path / "state" / "state.db")
    try:
        tables = _table_names(conn)
        assert {"source_files", "emails", "lineage", "skipped"}.issubset(tables)

        indices = _index_names(conn)
        assert {
            "idx_source_lookup",
            "idx_lineage_email",
            "idx_lineage_partition",
        }.issubset(indices)

        version = conn.execute("PRAGMA user_version").fetchone()[0]
        assert version == SCHEMA_VERSION
    finally:
        conn.close()


def test_open_is_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "state.db"
    conn1 = open_db(db)
    conn1.close()

    conn2 = open_db(db)
    try:
        assert conn2.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert {"source_files", "emails", "lineage", "skipped"}.issubset(
            _table_names(conn2)
        )
    finally:
        conn2.close()


def test_foreign_keys_enforced(tmp_path: Path) -> None:
    conn = open_db(tmp_path / "state.db")
    try:
        with pytest.raises(Exception):
            with transaction(conn):
                conn.execute(
                    """
                    INSERT INTO lineage(email_id, source_id, partition,
                                        internal_path, container_depth,
                                        discovered_at)
                    VALUES(?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "no-such-email",
                        "no-such-source",
                        "timestamp=2024-07-15",
                        "x",
                        0,
                        "2024-07-15T00:00:00Z",
                    ),
                )
    finally:
        conn.close()


def test_transaction_rolls_back_on_error(tmp_path: Path) -> None:
    conn = open_db(tmp_path / "state.db")
    try:
        with pytest.raises(RuntimeError):
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
                        "sid",
                        "ns",
                        "timestamp=2024-07-15",
                        "x.eml",
                        10,
                        1,
                        "deadbeef",
                        SourceStatus.DISCOVERED,
                        "2024-07-15T00:00:00Z",
                    ),
                )
                raise RuntimeError("boom")

        rows = conn.execute("SELECT COUNT(*) FROM source_files").fetchone()[0]
        assert rows == 0
    finally:
        conn.close()


def test_transaction_commits_on_success(tmp_path: Path) -> None:
    conn = open_db(tmp_path / "state.db")
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
                    "sid",
                    "ns",
                    "timestamp=2024-07-15",
                    "x.eml",
                    10,
                    1,
                    "deadbeef",
                    SourceStatus.DISCOVERED,
                    "2024-07-15T00:00:00Z",
                ),
            )

        rows = conn.execute(
            "SELECT status FROM source_files WHERE source_id=?", ("sid",)
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["status"] == SourceStatus.DISCOVERED
    finally:
        conn.close()


def test_skip_reason_enum_values_stable() -> None:
    """Pin the wire-level enum strings so the manifest format stays stable."""
    assert SkipReason.PASSWORD_PROTECTED == "password_protected"
    assert SkipReason.CORRUPT_ARCHIVE == "corrupt_archive"
    assert SkipReason.UNSUPPORTED_FORMAT_DEFERRED == "unsupported_format_deferred"
    assert SkipReason.NOT_AN_EMAIL == "not_an_email"
    assert SkipReason.EMPTY_CONTAINER == "empty_container"
    assert SkipReason.DEPTH_LIMIT_EXCEEDED == "depth_limit_exceeded"


def test_source_status_enum_values_stable() -> None:
    assert SourceStatus.DISCOVERED == "discovered"
    assert SourceStatus.IN_PROGRESS == "in_progress"
    assert SourceStatus.DONE == "done"
    assert SourceStatus.FAILED == "failed"
