"""Registry / classifier tests for the unpacker driver."""

from __future__ import annotations

from pathlib import Path

import pytest

from email_ingest.state import SkipReason
from email_ingest.unpacker import (
    ExtractedEmail,
    unpack_bytes,
    unpack_source_file,
)
from email_ingest.unpacker.eml_handler import is_eml
from email_ingest.unpacker.html_handler import is_html
from email_ingest.unpacker.mbox_handler import is_mbox
from email_ingest.unpacker.unsupported import is_unsupported
from email_ingest.unpacker.zip_handler import is_zip


EML = b"From: a@b\nSubject: s\n\nbody\n"


class TestExtensionPredicates:
    @pytest.mark.parametrize("name", ["a.eml", "A.EML", "weird.name.eml"])
    def test_eml_matches(self, name: str) -> None:
        assert is_eml(name)

    @pytest.mark.parametrize("name", ["a.html", "A.HTM", "x.HTML"])
    def test_html_matches(self, name: str) -> None:
        assert is_html(name)

    @pytest.mark.parametrize("name", ["a.mbox", "ARCHIVE.MBOX"])
    def test_mbox_matches(self, name: str) -> None:
        assert is_mbox(name)

    @pytest.mark.parametrize("name", ["x.zip", "X.ZIP"])
    def test_zip_matches(self, name: str) -> None:
        assert is_zip(name)

    @pytest.mark.parametrize("name", ["x.msg", "X.PST"])
    def test_unsupported_matches(self, name: str) -> None:
        assert is_unsupported(name)

    @pytest.mark.parametrize("name", ["x.png", "x", "x.xlsx", "x.eml.bak"])
    def test_no_handler_matches(self, name: str) -> None:
        assert not is_eml(name)
        assert not is_html(name)
        assert not is_mbox(name)
        assert not is_zip(name)
        assert not is_unsupported(name)


class TestDriverClassification:
    def test_eml_passthrough(self) -> None:
        result = unpack_bytes(EML, "simple_email.eml")
        assert len(result.emails) == 1
        e: ExtractedEmail = result.emails[0]
        assert e.raw_bytes == EML
        assert e.internal_path == "simple_email.eml"
        assert e.container_depth == 0
        assert e.format == "eml"
        assert result.skipped == []

    def test_html_passthrough(self) -> None:
        body = b"<html><body>hi</body></html>"
        result = unpack_bytes(body, "newsletter.html")
        assert len(result.emails) == 1
        assert result.emails[0].format == "html"
        assert result.emails[0].container_depth == 0
        assert result.skipped == []

    def test_msg_marked_unsupported_deferred(self) -> None:
        result = unpack_bytes(b"\xd0\xcf\x11\xe0", "x.msg")
        assert result.emails == []
        assert [s.reason for s in result.skipped] == [
            SkipReason.UNSUPPORTED_FORMAT_DEFERRED
        ]

    def test_pst_marked_unsupported_deferred(self) -> None:
        result = unpack_bytes(b"!BDN\x00", "archive.pst")
        assert result.emails == []
        assert [s.reason for s in result.skipped] == [
            SkipReason.UNSUPPORTED_FORMAT_DEFERRED
        ]

    def test_noise_files_marked_not_an_email(self) -> None:
        result = unpack_bytes(b"\x89PNG\r\n", "noise.png")
        assert result.emails == []
        assert [s.reason for s in result.skipped] == [SkipReason.NOT_AN_EMAIL]

    def test_no_extension_marked_not_an_email(self) -> None:
        result = unpack_bytes(b"???", "README")
        assert result.emails == []
        assert [s.reason for s in result.skipped] == [SkipReason.NOT_AN_EMAIL]


class TestUnpackSourceFile:
    def test_reads_from_disk(self, tmp_path: Path) -> None:
        p = tmp_path / "simple_email.eml"
        p.write_bytes(EML)
        result = unpack_source_file(p, "simple_email.eml")
        assert len(result.emails) == 1
        assert result.emails[0].raw_bytes == EML

    def test_unreadable_file_skips_gracefully(self, tmp_path: Path) -> None:
        result = unpack_source_file(tmp_path / "missing.eml", "missing.eml")
        assert result.emails == []
        assert [s.reason for s in result.skipped] == [SkipReason.UNREADABLE]
