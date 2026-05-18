"""ZIP handler tests.

We build ZIPs programmatically in-memory so the test suite carries no
binary fixtures and we control exactly what's inside each archive.
"""

from __future__ import annotations

import io
import zipfile

import pytest

from email_ingest.state import SkipReason
from email_ingest.unpacker import unpack_bytes
from email_ingest.unpacker.zip_handler import (
    MemberTooLarge,
    ZipCorrupt,
    ZipPasswordProtected,
    open_zip_members,
)


EML_BYTES = (
    b"From: a@example.com\r\n"
    b"To: b@example.com\r\n"
    b"Subject: hi\r\n\r\n"
    b"body\r\n"
)


def _make_zip(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Low-level handler
# ---------------------------------------------------------------------------


class TestOpenZipMembers:
    def test_lists_members_sorted(self) -> None:
        data = _make_zip({"b.eml": b"B", "a.eml": b"A"})
        members = open_zip_members(data)
        assert [m.name for m in members] == ["a.eml", "b.eml"]
        assert [m.raw_bytes for m in members] == [b"A", b"B"]

    def test_skips_directory_entries(self) -> None:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("sub/", b"")
            zf.writestr("sub/x.eml", EML_BYTES)
        members = open_zip_members(buf.getvalue())
        assert [m.name for m in members] == ["sub/x.eml"]

    def test_skips_traversal_attacks(self) -> None:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("../escape.eml", EML_BYTES)
            zf.writestr("/absolute.eml", EML_BYTES)
            zf.writestr("ok.eml", EML_BYTES)
        members = open_zip_members(buf.getvalue())
        assert [m.name for m in members] == ["ok.eml"]

    def test_bad_zip_raises_corrupt(self) -> None:
        with pytest.raises(ZipCorrupt):
            open_zip_members(b"not a zip at all")

    def test_truncated_zip_raises_corrupt(self) -> None:
        data = _make_zip({"x.eml": EML_BYTES})
        with pytest.raises(ZipCorrupt):
            open_zip_members(data[: len(data) // 2])

    def test_password_protected_raises(self) -> None:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("secret.eml", EML_BYTES)
            zf.setpassword(b"hunter2")
            # Re-add encrypted: stdlib's ZipFile can only WRITE unencrypted,
            # but it CAN READ encrypted archives. We synthesize one by
            # marking the existing entry's general purpose bit. Simpler:
            # use ZipFile's setpassword on read side. Use a known-good
            # encrypted blob instead.
        # Build an encrypted ZIP via the lower-level path. Python's stdlib
        # cannot write encrypted ZIPs, so we forge the encrypted flag bit
        # on an existing member to trigger the read-side password check.
        forged = _force_encrypted_flag(buf.getvalue())

        with pytest.raises(ZipPasswordProtected):
            open_zip_members(forged)

    def test_member_size_cap(self, monkeypatch) -> None:
        from email_ingest.unpacker import zip_handler as zh

        monkeypatch.setattr(zh, "MAX_MEMBER_SIZE_BYTES", 4)
        data = _make_zip({"big.eml": b"123456789"})
        with pytest.raises(MemberTooLarge):
            zh.open_zip_members(data)


def _force_encrypted_flag(zip_bytes: bytes) -> bytes:
    """Flip the 'encrypted' bit on every header in the archive.

    Python can't *write* encrypted ZIPs, but flipping general-purpose bit
    flag 0x0001 on a member makes ``zipfile.ZipFile.read`` raise the
    'File ... is encrypted' RuntimeError we map to ZipPasswordProtected.

    ZIP has two places that carry the flag for each member:
      * Local file header  (sig PK\\x03\\x04, flag at offset 6..8)
      * Central directory entry (sig PK\\x01\\x02, flag at offset 8..10)

    ``zipfile`` reads the central directory to populate ``ZipInfo``, so
    we have to flip both for the read side to see the file as encrypted.
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
# Driver-level behavior
# ---------------------------------------------------------------------------


class TestUnpackZip:
    def test_flat_zip_yields_emails(self) -> None:
        data = _make_zip({"invoice.eml": EML_BYTES, "receipt.eml": EML_BYTES + b"x"})
        result = unpack_bytes(data, "batch.zip")
        assert len(result.emails) == 2
        paths = sorted(e.internal_path for e in result.emails)
        assert paths == ["batch.zip!invoice.eml", "batch.zip!receipt.eml"]
        assert all(e.container_depth == 1 for e in result.emails)
        assert all(e.format == "eml" for e in result.emails)
        assert result.skipped == []

    def test_zip_in_zip(self) -> None:
        inner = _make_zip({"deep.eml": EML_BYTES})
        outer = _make_zip({"nested.zip": inner, "top.eml": EML_BYTES})
        result = unpack_bytes(outer, "batch.zip")

        emails_by_path = {e.internal_path: e for e in result.emails}
        assert set(emails_by_path) == {
            "batch.zip!top.eml",
            "batch.zip!nested.zip!deep.eml",
        }
        assert emails_by_path["batch.zip!top.eml"].container_depth == 1
        assert emails_by_path["batch.zip!nested.zip!deep.eml"].container_depth == 2
        assert result.skipped == []

    def test_corrupt_zip_skipped(self) -> None:
        result = unpack_bytes(b"not a zip", "bad.zip")
        assert result.emails == []
        assert len(result.skipped) == 1
        assert result.skipped[0].reason == SkipReason.CORRUPT_ARCHIVE
        assert result.skipped[0].internal_path == "bad.zip"

    def test_password_protected_skipped(self) -> None:
        data = _make_zip({"secret.eml": EML_BYTES})
        forged = _force_encrypted_flag(data)
        result = unpack_bytes(forged, "locked.zip")
        assert result.emails == []
        assert len(result.skipped) == 1
        assert result.skipped[0].reason == SkipReason.PASSWORD_PROTECTED

    def test_empty_zip_skipped(self) -> None:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            pass
        result = unpack_bytes(buf.getvalue(), "empty.zip")
        assert result.emails == []
        assert len(result.skipped) == 1
        assert result.skipped[0].reason == SkipReason.EMPTY_CONTAINER

    def test_zip_with_only_noise_is_not_empty(self) -> None:
        """A zip containing only non-email files should yield not_an_email
        skips, not 'empty_container'."""
        data = _make_zip({"picture.png": b"\x89PNG\r\n", "report.xlsx": b"xlsx"})
        result = unpack_bytes(data, "junk.zip")
        assert result.emails == []
        reasons = {s.reason for s in result.skipped}
        assert reasons == {SkipReason.NOT_AN_EMAIL}

    def test_depth_limit(self, monkeypatch) -> None:
        from email_ingest import unpacker as up

        monkeypatch.setattr(up, "MAX_CONTAINER_DEPTH", 2)

        # Build z3 (depth 3 from root): outer -> mid -> inner -> deep.eml
        innermost = _make_zip({"deep.eml": EML_BYTES})
        middle = _make_zip({"inner.zip": innermost})
        outer = _make_zip({"mid.zip": middle})

        result = up.unpack_bytes(outer, "root.zip")

        # First-level (root.zip -> mid.zip) goes to depth 1 (ok).
        # Second-level (mid.zip -> inner.zip) goes to depth 2 (ok).
        # Third-level (inner.zip -> deep.eml) would push depth to 3 > 2,
        # so inner.zip is skipped with depth_limit_exceeded.
        assert result.emails == []
        assert any(
            s.reason == SkipReason.DEPTH_LIMIT_EXCEEDED for s in result.skipped
        )
