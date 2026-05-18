"""MBOX handler tests."""

from __future__ import annotations

from email_ingest.state import SkipReason
from email_ingest.unpacker import unpack_bytes
from email_ingest.unpacker.mbox_handler import iter_mbox_messages


def _mbox(messages: list[bytes]) -> bytes:
    """Concatenate messages with the MBOX 'From ' separator."""
    parts = []
    for i, msg in enumerate(messages):
        sep = f"From sender{i}@example.com Mon Jul 15 10:00:0{i} 2024\n".encode()
        parts.append(sep + msg.rstrip(b"\n") + b"\n\n")
    return b"".join(parts)


MSG_1 = b"From: a@example.com\nSubject: one\n\nbody one\n"
MSG_2 = b"From: b@example.com\nSubject: two\n\nbody two\n"
MSG_3 = b"From: c@example.com\nSubject: three\n\nbody three\n"


class TestIterMboxMessages:
    def test_yields_each_message(self) -> None:
        data = _mbox([MSG_1, MSG_2, MSG_3])
        messages = list(iter_mbox_messages(data))
        assert [m.index for m in messages] == [0, 1, 2]
        subjects = [m.raw_bytes for m in messages]
        assert b"Subject: one" in subjects[0]
        assert b"Subject: two" in subjects[1]
        assert b"Subject: three" in subjects[2]

    def test_does_not_include_from_separator(self) -> None:
        data = _mbox([MSG_1])
        [msg] = list(iter_mbox_messages(data))
        assert not msg.raw_bytes.startswith(b"From sender")
        assert msg.raw_bytes.startswith(b"From: a@example.com")


class TestUnpackMbox:
    def test_iteration_yields_indexed_internal_paths(self) -> None:
        data = _mbox([MSG_1, MSG_2])
        result = unpack_bytes(data, "conversations.mbox")
        paths = sorted(e.internal_path for e in result.emails)
        assert paths == [
            "conversations.mbox#0",
            "conversations.mbox#1",
        ]
        assert all(e.container_depth == 1 for e in result.emails)
        assert all(e.format == "eml" for e in result.emails)

    def test_empty_mbox_is_empty_container(self) -> None:
        result = unpack_bytes(b"", "empty.mbox")
        assert result.emails == []
        assert len(result.skipped) == 1
        assert result.skipped[0].reason == SkipReason.EMPTY_CONTAINER

    def test_mbox_inside_zip(self) -> None:
        import io
        import zipfile

        mbox_bytes = _mbox([MSG_1, MSG_2])
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("inbox.mbox", mbox_bytes)

        result = unpack_bytes(buf.getvalue(), "archive.zip")
        paths = sorted(e.internal_path for e in result.emails)
        assert paths == [
            "archive.zip!inbox.mbox#0",
            "archive.zip!inbox.mbox#1",
        ]
        # zip hop + mbox hop = depth 2
        assert all(e.container_depth == 2 for e in result.emails)

    def test_two_mboxes_with_colliding_internal_names_dont_clash(self) -> None:
        """The plan's #3 edge case: two MBOXes both produce "001"-style
        names internally. Since we identify by content hash, this just
        means we end up with N distinct emails whose internal paths
        unambiguously trace back to the right MBOX."""
        mbox_a = _mbox([MSG_1])
        mbox_b = _mbox([MSG_2])

        result_a = unpack_bytes(mbox_a, "a.mbox")
        result_b = unpack_bytes(mbox_b, "b.mbox")

        paths = (
            [e.internal_path for e in result_a.emails]
            + [e.internal_path for e in result_b.emails]
        )
        # Same trailing "#0" but the prefix disambiguates.
        assert paths == ["a.mbox#0", "b.mbox#0"]
        # And the bytes are different so dedup downstream would not
        # collapse them.
        assert result_a.emails[0].raw_bytes != result_b.emails[0].raw_bytes
