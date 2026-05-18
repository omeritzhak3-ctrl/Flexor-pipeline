"""Tenant-scoped identity and canonicalization for the ingestion pipeline.

Three identifiers matter:

* ``source_id`` — identifies a *bucket file* (the thing the customer uploaded).
  Derived from ``(namespace, partition, relpath)`` so the same physical file
  re-uploaded under a different partition / namespace is treated as distinct.

* ``content_sha256`` — raw SHA-256 of the bytes we extracted. Kept for
  debugging and for the CDC ledger's "did the file actually change" check.

* ``email_id`` — the tenant-scoped content identity used for dedup. It is
  ``sha256(namespace || 0x00 || canonical_bytes)`` so two customers uploading
  the exact same email each get their own staged copy, while duplicates
  *within* a single customer collapse to one row.

The canonicalization step normalizes the few framing differences that vary
between MBOX members and standalone EMLs (line endings, trailing
``From `` separators, BOMs) so we don't see spurious dedup misses just
because one copy travelled through a different container.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import PurePosixPath


# ---------------------------------------------------------------------------
# Canonicalization
# ---------------------------------------------------------------------------

# MBOX-style "From " separator that begins each message. When we extract a
# message from an MBOX we don't include this line, but we still strip it
# defensively here in case a file already contains one.
_MBOX_FROM_LINE_RE = re.compile(rb"^From [^\r\n]*\r?\n")

# UTF-8 BOM that some HTML exports prepend.
_UTF8_BOM = b"\xef\xbb\xbf"


def canonicalize_email_bytes(raw: bytes) -> bytes:
    """Normalize email bytes for stable content hashing.

    The goal is *only* to neutralize framing/encoding artifacts that have no
    semantic meaning. We intentionally do **not** touch headers or body
    content — downstream normalization owns that.

    Steps:

    1. Strip UTF-8 BOM if present.
    2. Strip a leading ``From `` separator (MBOX framing leakage).
    3. Normalize line endings to LF (``\\n``).
    4. Strip trailing whitespace/newlines.
    """
    if raw.startswith(_UTF8_BOM):
        raw = raw[len(_UTF8_BOM):]

    raw = _MBOX_FROM_LINE_RE.sub(b"", raw, count=1)

    # CRLF -> LF, then any lone CR -> LF. Order matters.
    raw = raw.replace(b"\r\n", b"\n").replace(b"\r", b"\n")

    return raw.rstrip(b"\r\n\t ")


# ---------------------------------------------------------------------------
# IDs
# ---------------------------------------------------------------------------


def content_sha256(data: bytes) -> str:
    """SHA-256 of arbitrary bytes, lowercase hex."""
    return hashlib.sha256(data).hexdigest()


def compute_email_id(namespace: str, raw_email_bytes: bytes) -> str:
    """Tenant-scoped content id used as ``emails.email_id``.

    ``email_id = sha256(namespace || 0x00 || canonical_bytes)``.

    The NUL separator prevents trivial cross-tenant collisions like
    ``namespace="ab", body="cd..."`` vs ``namespace="abcd", body="..."``.
    """
    if not namespace:
        raise ValueError("namespace must be non-empty")
    canonical = canonicalize_email_bytes(raw_email_bytes)
    h = hashlib.sha256()
    h.update(namespace.encode("utf-8"))
    h.update(b"\x00")
    h.update(canonical)
    return h.hexdigest()


def compute_source_id(namespace: str, partition: str, relpath: str) -> str:
    """Stable id for a bucket file as identified by its location.

    Keyed on ``(namespace, partition, relpath)``. Two physically identical
    files uploaded to different partitions are *different* source files
    (they will, however, still dedupe to the same ``email_id`` downstream).
    """
    if not namespace or not partition or not relpath:
        raise ValueError("namespace, partition, and relpath must all be non-empty")

    # Normalize relpath to POSIX-style forward slashes so the id is stable
    # across operating systems.
    normalized_rel = PurePosixPath(relpath.replace("\\", "/")).as_posix()

    h = hashlib.sha256()
    h.update(namespace.encode("utf-8"))
    h.update(b"\x00")
    h.update(partition.encode("utf-8"))
    h.update(b"\x00")
    h.update(normalized_rel.encode("utf-8"))
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Pool path
# ---------------------------------------------------------------------------


def pool_relpath(email_id: str, suffix: str = ".eml") -> str:
    """Return the in-pool relative path for a given ``email_id``.

    The pool is sharded by the first two hex characters of the id so a
    single directory doesn't grow unbounded::

        staging/emails/ab/ab12cd...ef.eml
    """
    if len(email_id) < 3:
        raise ValueError("email_id is too short to shard")
    shard = email_id[:2]
    return f"{shard}/{email_id}{suffix}"
