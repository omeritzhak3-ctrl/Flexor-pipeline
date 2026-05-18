"""MBOX container handler.

MBOX is a concatenation of RFC 822 messages separated by ``From ``
lines. We parse it with the stdlib ``mailbox`` module rather than
re-implementing the split logic — that handles the various quirks
("From " line escaping, etc.) for free.

Each message yields one leaf with internal path
``<mbox_path>#<index>`` where ``index`` is 0-based and reflects the
position within the MBOX. Names of individual messages are deliberately
*not* used for identity: two MBOXes both producing "001"-style names
must not collide, and content-hashing handles that for us.
"""

from __future__ import annotations

import mailbox
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


@dataclass(frozen=True)
class MboxMessage:
    """One message extracted from an MBOX, with its position recorded."""

    raw_bytes: bytes
    index: int


def is_mbox(name: str) -> bool:
    return name.lower().endswith(".mbox")


def iter_mbox_messages(mbox_bytes: bytes) -> Iterator[MboxMessage]:
    """Yield each message in the MBOX as raw bytes.

    The stdlib ``mailbox.mbox`` operates on filesystem paths, not file
    objects, so we materialize the bytes to a temp file. The temp file is
    cleaned up on iterator exhaustion.
    """
    # NamedTemporaryFile with delete=False so we can close it before
    # mailbox opens it (Windows compat), then unlink in a finally.
    tmp = tempfile.NamedTemporaryFile(suffix=".mbox", delete=False)
    try:
        tmp.write(mbox_bytes)
        tmp.flush()
        tmp.close()

        box = mailbox.mbox(tmp.name, create=False)
        try:
            for idx, key in enumerate(box.keys()):
                msg = box.get_message(key)
                # as_bytes() gives us the canonical wire form. We
                # explicitly do NOT prepend the "From " separator — that's
                # MBOX framing, not part of the email body.
                yield MboxMessage(raw_bytes=msg.as_bytes(), index=idx)
        finally:
            box.close()
    finally:
        try:
            Path(tmp.name).unlink()
        except FileNotFoundError:
            pass


def iter_mbox_messages_from_file(path: Path) -> Iterator[MboxMessage]:
    """Convenience wrapper for callers that already have a file on disk."""
    box = mailbox.mbox(str(path), create=False)
    try:
        for idx, key in enumerate(box.keys()):
            yield MboxMessage(raw_bytes=box.get_message(key).as_bytes(), index=idx)
    finally:
        box.close()
