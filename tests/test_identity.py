"""Identity & canonicalization tests.

Covers the three rules we care about for dedup:

1. Identical content under the same tenant produces the same email_id
   (i.e. it's deterministic + stable).
2. Identical content under different tenants produces *different* email_ids
   (tenant scoping is enforced).
3. The canonicalization rules collapse only the framing differences they
   are supposed to (CRLF vs LF, BOM, leading ``From `` line) and not the
   content itself.
"""

from __future__ import annotations

import pytest

from email_ingest.identity import (
    canonicalize_email_bytes,
    compute_email_id,
    compute_source_id,
    content_sha256,
    pool_relpath,
)


EML_BODY = (
    b"From: alice@example.com\r\n"
    b"To: bob@example.com\r\n"
    b"Subject: Hello\r\n"
    b"\r\n"
    b"Hi Bob.\r\n"
)


class TestCanonicalize:
    def test_crlf_to_lf(self) -> None:
        assert canonicalize_email_bytes(b"a\r\nb\r\n") == b"a\nb"

    def test_lone_cr_to_lf(self) -> None:
        assert canonicalize_email_bytes(b"a\rb\r") == b"a\nb"

    def test_strips_utf8_bom(self) -> None:
        bom_then_html = b"\xef\xbb\xbf<html></html>\n"
        assert canonicalize_email_bytes(bom_then_html) == b"<html></html>"

    def test_strips_mbox_from_separator(self) -> None:
        body = b"From alice@example.com Mon Jul 15 10:00:00 2024\r\n" + EML_BODY
        canonical = canonicalize_email_bytes(body)
        assert not canonical.startswith(b"From ")
        assert b"Subject: Hello" in canonical

    def test_strips_trailing_whitespace(self) -> None:
        assert canonicalize_email_bytes(b"hello\n\n\n   ") == b"hello"

    def test_does_not_strip_internal_content(self) -> None:
        canonical = canonicalize_email_bytes(EML_BODY)
        assert b"alice@example.com" in canonical
        assert b"Hi Bob." in canonical


class TestEmailId:
    def test_stable_across_calls(self) -> None:
        a = compute_email_id("tenant_a", EML_BODY)
        b = compute_email_id("tenant_a", EML_BODY)
        assert a == b

    def test_tenant_scoped(self) -> None:
        """Same email bytes under two tenants -> two distinct email_ids."""
        a = compute_email_id("tenant_a", EML_BODY)
        b = compute_email_id("tenant_b", EML_BODY)
        assert a != b

    def test_line_ending_difference_does_not_change_id(self) -> None:
        """An MBOX-vs-EML copy of the same email must collapse to one id."""
        crlf_version = EML_BODY
        lf_version = EML_BODY.replace(b"\r\n", b"\n")
        with_mbox_prefix = (
            b"From alice@example.com Mon Jul 15 10:00:00 2024\r\n" + EML_BODY
        )

        ids = {
            compute_email_id("t", crlf_version),
            compute_email_id("t", lf_version),
            compute_email_id("t", with_mbox_prefix),
        }
        assert len(ids) == 1

    def test_different_content_different_id(self) -> None:
        a = compute_email_id("t", EML_BODY)
        b = compute_email_id("t", EML_BODY + b"PS: extra paragraph\n")
        assert a != b

    def test_namespace_required(self) -> None:
        with pytest.raises(ValueError):
            compute_email_id("", EML_BODY)

    def test_no_cross_tenant_collision_via_concatenation(self) -> None:
        """The NUL separator prevents 'ab' + 'cd...' colliding with 'abcd' + '...'.

        Without the separator, concatenating the namespace and the canonical
        bytes could produce the same hash input for two different
        (namespace, content) pairs. This test pins that down.
        """
        a = compute_email_id("ab", b"cd" + EML_BODY)
        b = compute_email_id("abcd", EML_BODY)
        assert a != b


class TestSourceId:
    def test_deterministic(self) -> None:
        a = compute_source_id("ns", "timestamp=2024-07-15", "batch.zip")
        b = compute_source_id("ns", "timestamp=2024-07-15", "batch.zip")
        assert a == b

    def test_partition_separates(self) -> None:
        a = compute_source_id("ns", "timestamp=2024-07-15", "x.eml")
        b = compute_source_id("ns", "timestamp=2024-07-16", "x.eml")
        assert a != b

    def test_namespace_separates(self) -> None:
        a = compute_source_id("ns1", "timestamp=2024-07-15", "x.eml")
        b = compute_source_id("ns2", "timestamp=2024-07-15", "x.eml")
        assert a != b

    def test_windows_path_normalized(self) -> None:
        a = compute_source_id("ns", "timestamp=2024-07-15", "sub\\x.eml")
        b = compute_source_id("ns", "timestamp=2024-07-15", "sub/x.eml")
        assert a == b

    def test_inputs_required(self) -> None:
        with pytest.raises(ValueError):
            compute_source_id("", "p", "r")
        with pytest.raises(ValueError):
            compute_source_id("n", "", "r")
        with pytest.raises(ValueError):
            compute_source_id("n", "p", "")


class TestContentSha256:
    def test_known_value(self) -> None:
        assert content_sha256(b"") == (
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        )


class TestPoolRelpath:
    def test_shards_by_first_two_chars(self) -> None:
        email_id = "ab12cd34" + "0" * 56
        assert pool_relpath(email_id) == f"ab/{email_id}.eml"

    def test_custom_suffix(self) -> None:
        email_id = "cd" + "0" * 62
        assert pool_relpath(email_id, ".html") == f"cd/{email_id}.html"

    def test_rejects_short_id(self) -> None:
        with pytest.raises(ValueError):
            pool_relpath("ab")
