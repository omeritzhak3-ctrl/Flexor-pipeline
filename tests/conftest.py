"""Shared programmatic test fixtures.

This is the *only* place we keep the recipes for synthesizing bucket
content. The pipeline tests never carry binary fixtures in the repo —
everything is rebuilt at test time, which keeps the diff history clean
and makes it obvious what each test depends on.

Two flavours of helper live here:

* **plain functions** (``make_zip``, ``make_mbox``, etc.) — pure builders
  that any test can import and compose;
* **pytest fixtures** (``cfg``, ``edge_case_bucket``) — wired into the
  pytest fixture graph for ergonomics.
"""

from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence

import pytest

from email_ingest.config import PipelineConfig


# ---------------------------------------------------------------------------
# Sample email bodies. Each has distinct content so each gets a distinct
# email_id under the same tenant. ``EML_A`` is reused across tests where
# we want to demonstrate dedup.
# ---------------------------------------------------------------------------

EML_A = (
    b"From: alice@example.com\r\n"
    b"To: bob@example.com\r\n"
    b"Subject: A\r\n\r\n"
    b"body A\r\n"
)
EML_B = (
    b"From: bob@example.com\r\n"
    b"To: alice@example.com\r\n"
    b"Subject: B\r\n\r\n"
    b"body B\r\n"
)
EML_C = (
    b"From: carol@example.com\r\n"
    b"To: dan@example.com\r\n"
    b"Subject: C\r\n\r\n"
    b"body C\r\n"
)
EML_D = (
    b"From: dan@example.com\r\n"
    b"To: erin@example.com\r\n"
    b"Subject: D\r\n\r\n"
    b"body D\r\n"
)

HTML_BODY = b"<!doctype html><html><body><p>Hello there</p></body></html>"


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def write_file(path: Path, content: bytes) -> Path:
    """Write ``content`` to ``path``, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def make_zip(members: Mapping[str, bytes], *, compression: int = zipfile.ZIP_DEFLATED) -> bytes:
    """Return ZIP bytes containing the given ``{name: data}`` members."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression) as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


def make_empty_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w"):
        pass
    return buf.getvalue()


def make_mbox(messages: Sequence[bytes]) -> bytes:
    """Concatenate messages into an MBOX with synthetic ``From `` separators."""
    parts = []
    for i, msg in enumerate(messages):
        sep = f"From sender{i}@example.com Mon Jul 15 10:00:0{i} 2024\n".encode()
        parts.append(sep + msg.rstrip(b"\n") + b"\n\n")
    return b"".join(parts)


def force_encrypted_flag(zip_bytes: bytes) -> bytes:
    """Flip general-purpose bit 0x0001 on every header in the archive.

    Python can't *write* encrypted ZIPs, but flipping the flag in both
    the local-file header (sig PK\\x03\\x04, flag at offset 6..8) and
    the central directory entry (sig PK\\x01\\x02, flag at offset 8..10)
    makes ``zipfile.ZipFile.read`` raise the
    ``'File ... is encrypted'`` RuntimeError we map to
    ``ZipPasswordProtected``.
    """
    out = bytearray(zip_bytes)

    def flip(sig: bytes, flag_offset: int) -> None:
        i = 0
        while True:
            i = out.find(sig, i)
            if i == -1:
                return
            off = i + flag_offset
            flag = int.from_bytes(out[off : off + 2], "little") | 0x0001
            out[off : off + 2] = flag.to_bytes(2, "little")
            i += len(sig)

    flip(b"PK\x03\x04", 6)
    flip(b"PK\x01\x02", 8)
    return bytes(out)


# ---------------------------------------------------------------------------
# pytest fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def cfg(tmp_path: Path) -> PipelineConfig:
    """Fresh per-test ``PipelineConfig`` rooted under ``tmp_path``."""
    return PipelineConfig(
        bucket_root=tmp_path / "bucket", state_root=tmp_path / "state"
    )


# ---------------------------------------------------------------------------
# Comprehensive edge-case bucket
# ---------------------------------------------------------------------------


@dataclass
class EdgeCaseBucket:
    """Convenience handle returned by the ``edge_case_bucket`` fixture.

    Lets a test access ``cfg`` *and* the per-case expectations without
    re-deriving them.
    """

    cfg: PipelineConfig

    # The fixture documents how many emails and skips a clean run should
    # produce. These are the assertions the comprehensive test pins.
    expected_emails: int = 0
    expected_skipped: int = 0
    expected_sources: int = 0


@pytest.fixture
def edge_case_bucket(cfg: PipelineConfig) -> EdgeCaseBucket:
    """Materialize *every* documented edge case into a single bucket.

    Layout::

        ns/
          timestamp=2024-07-15/
            simple.eml                  # plain leaf
            page.html                   # html leaf
            noise.png                   # non-email noise -> not_an_email
            noise.xlsx                  # non-email noise -> not_an_email
            empty.zip                   # empty container -> empty_container
            empty.mbox                  # empty container -> empty_container
            bad.zip                     # truncated bytes -> corrupt_archive
            locked.zip                  # encrypted flag set -> password_protected
            nested.zip                  # zip-in-zip with mbox at the bottom
            colliding_a.mbox            # internal names collide with b
            colliding_b.mbox
            deferred.msg                # unsupported_format_deferred
            duplicate.eml               # same bytes as one in p2
          timestamp=2024-07-16/
            duplicate.eml               # cross-partition duplicate of above
    """
    p1 = cfg.bucket_root / "ns" / "timestamp=2024-07-15"
    p2 = cfg.bucket_root / "ns" / "timestamp=2024-07-16"

    # --- single emails ---
    write_file(p1 / "simple.eml", EML_A)
    write_file(p1 / "page.html", HTML_BODY)

    # --- noise files ---
    write_file(p1 / "noise.png", b"\x89PNG\r\n\x1a\n")
    write_file(p1 / "noise.xlsx", b"PK\x03\x04\x14\x00...xlsx-like...")

    # --- empty containers ---
    write_file(p1 / "empty.zip", make_empty_zip())
    write_file(p1 / "empty.mbox", b"")

    # --- corrupt and password-protected ---
    truncated = make_zip({"victim.eml": EML_B})[: 32]
    write_file(p1 / "bad.zip", truncated)
    write_file(p1 / "locked.zip", force_encrypted_flag(make_zip({"secret.eml": EML_B})))

    # --- deep nesting + zip-in-zip + mbox-in-zip ---
    inner_mbox = make_mbox([EML_B, EML_C])
    inner_zip = make_zip({"mail.mbox": inner_mbox})
    write_file(p1 / "nested.zip", make_zip({"inner.zip": inner_zip}))

    # --- colliding internal-name MBOXes (each mbox member is index 0/1) ---
    write_file(p1 / "colliding_a.mbox", make_mbox([EML_D]))
    write_file(p1 / "colliding_b.mbox", make_mbox([HTML_BODY + b"\n# Different bytes\n"]))

    # --- deferred unsupported format ---
    write_file(p1 / "deferred.msg", b"\xd0\xcf\x11\xe0...msg-like...")

    # --- cross-partition duplicate ---
    write_file(p1 / "duplicate.eml", EML_A)         # same as simple.eml in p1
    write_file(p2 / "duplicate.eml", EML_A)         # same again in p2

    # Distinct emails staged in the pool:
    #   EML_A (simple.eml, p1/duplicate.eml, p2/duplicate.eml)
    #   HTML  (page.html)
    #   EML_B (nested.zip!inner.zip!mail.mbox#0)
    #   EML_C (nested.zip!inner.zip!mail.mbox#1)
    #   EML_D (colliding_a.mbox#0)
    #   mbox-html-ish (colliding_b.mbox#0)         # not html format; mbox always emits .eml
    # = 6 distinct emails
    expected_emails = 6

    # Skips:
    #   noise.png    -> not_an_email
    #   noise.xlsx   -> not_an_email
    #   empty.zip    -> empty_container
    #   empty.mbox   -> empty_container
    #   bad.zip      -> corrupt_archive
    #   locked.zip   -> password_protected
    #   deferred.msg -> unsupported_format_deferred
    # = 7 skips
    expected_skipped = 7

    # Source files:
    #   p1: 14 (simple, page, noise.png, noise.xlsx, empty.zip,
    #            empty.mbox, bad.zip, locked.zip, nested.zip,
    #            colliding_a.mbox, colliding_b.mbox, deferred.msg,
    #            duplicate.eml)
    #   Wait, that's 13. Let me recount.
    #     simple, page, noise.png, noise.xlsx, empty.zip, empty.mbox,
    #     bad.zip, locked.zip, nested.zip, colliding_a.mbox,
    #     colliding_b.mbox, deferred.msg, duplicate.eml = 13.
    #   p2: 1 (duplicate.eml)
    # = 14 sources
    expected_sources = 14

    return EdgeCaseBucket(
        cfg=cfg,
        expected_emails=expected_emails,
        expected_skipped=expected_skipped,
        expected_sources=expected_sources,
    )
