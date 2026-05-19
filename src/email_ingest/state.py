"""SQLite state management for the ingestion pipeline.

The DB is the source of truth for:

* the *source file ledger* (what we've seen in the bucket and what its
  CDC fingerprint is),
* the *emails* table (one row per unique tenant-scoped email),
* the *lineage* table (every place an email_id was discovered),
* the *skipped* table (anything we deliberately didn't stage, with reason).

We deliberately keep this layer thin: it owns connection setup, schema
migrations, and a transaction context manager. Higher layers compose their
own multi-statement transactions.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

# Bumped whenever the schema below changes. Migrations are append-only.
SCHEMA_VERSION = 1


_SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS source_files (
  source_id          TEXT PRIMARY KEY,
  namespace          TEXT NOT NULL,
  partition          TEXT NOT NULL,
  relpath            TEXT NOT NULL,
  size_bytes         INTEGER NOT NULL,
  mtime_ns           INTEGER NOT NULL,
  content_sha256     TEXT NOT NULL,
  status             TEXT NOT NULL,
  first_seen_at      TEXT NOT NULL,
  last_processed_at  TEXT
);

CREATE TABLE IF NOT EXISTS emails (
  email_id           TEXT PRIMARY KEY,
  namespace          TEXT NOT NULL,
  content_sha256     TEXT NOT NULL,
  pool_path          TEXT NOT NULL,
  size_bytes         INTEGER NOT NULL,
  format             TEXT NOT NULL,
  first_staged_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lineage (
  lineage_id         INTEGER PRIMARY KEY AUTOINCREMENT,
  email_id           TEXT NOT NULL REFERENCES emails(email_id),
  source_id          TEXT NOT NULL REFERENCES source_files(source_id),
  partition          TEXT NOT NULL,
  internal_path      TEXT NOT NULL,
  container_depth    INTEGER NOT NULL,
  discovered_at      TEXT NOT NULL,
  UNIQUE(email_id, source_id, internal_path)
);

CREATE TABLE IF NOT EXISTS skipped (
  skipped_id         INTEGER PRIMARY KEY AUTOINCREMENT,
  source_id          TEXT NOT NULL REFERENCES source_files(source_id),
  internal_path      TEXT NOT NULL,
  reason             TEXT NOT NULL,
  details            TEXT,
  skipped_at         TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_source_lookup
  ON source_files(namespace, partition, relpath);
CREATE INDEX IF NOT EXISTS idx_lineage_email
  ON lineage(email_id);
CREATE INDEX IF NOT EXISTS idx_lineage_partition
  ON lineage(partition);
"""


# Closed enum of skip reasons. Centralized so callers don't drift; we'll
# enforce it at the application layer rather than via SQL CHECK so that
# schema migrations stay simple.
class SkipReason:
    PASSWORD_PROTECTED = "password_protected"
    CORRUPT_ARCHIVE = "corrupt_archive"
    UNSUPPORTED_FORMAT_DEFERRED = "unsupported_format_deferred"
    NOT_AN_EMAIL = "not_an_email"
    EMPTY_CONTAINER = "empty_container"
    DEPTH_LIMIT_EXCEEDED = "depth_limit_exceeded"
    UNREADABLE = "unreadable"


# Closed enum of source_files.status values.
class SourceStatus:
    DISCOVERED = "discovered"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    FAILED = "failed"


# ---------------------------------------------------------------------------
# Connection / migration
# ---------------------------------------------------------------------------


def open_db(db_path: Path) -> sqlite3.Connection:
    """Open (creating if missing) and migrate the SQLite database.

    Returns a connection with sensible pragmas set:

    * ``foreign_keys=ON`` — we rely on FKs from lineage/skipped to source_files.
    * ``journal_mode=WAL`` — crash-safety friendly; multiple readers + one
      writer is exactly the access pattern we want for this pipeline.
    * ``synchronous=NORMAL`` — durable across application crashes, only at
      risk on OS/power crash; an acceptable trade-off for this use case.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")

    _migrate(conn)
    return conn


def _migrate(conn: sqlite3.Connection) -> None:
    """Apply schema migrations idempotently."""
    current = _get_user_version(conn)
    if current >= SCHEMA_VERSION:
        return

    with conn:  # implicit transaction
        if current < 1:
            conn.executescript(_SCHEMA_V1)
        # future migrations: if current < 2: ...

        conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")


def _get_user_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("PRAGMA user_version").fetchone()
    return int(row[0]) if row is not None else 0


# ---------------------------------------------------------------------------
# Transaction helper
# ---------------------------------------------------------------------------


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Run a block in a single explicit transaction.

    We use ``BEGIN IMMEDIATE`` so concurrent writers fail fast instead of
    interleaving and producing surprising lock errors mid-statement.
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    else:
        conn.commit()
