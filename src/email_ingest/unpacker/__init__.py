"""Recursive unpacker driver.

The driver runs a breadth-first worklist over ``(bytes, internal_path,
container_depth)`` items. Per item:

* a *leaf* classifier (``.eml`` / ``.html``) appends an
  ``ExtractedEmail`` to the result;
* a *container* classifier (``.zip`` / ``.mbox``) opens the container
  and pushes its children onto the worklist with ``container_depth + 1``;
* anything unsupported or unreadable produces a ``Skipped`` row with a
  documented reason and the worklist continues.

Identity is computed downstream from the bytes; the unpacker never names
extracted emails, only locates them via ``internal_path``. That's what
lets two MBOXes both containing "001.eml"-style messages dedupe
correctly without colliding on filename.

Internal path notation::

    batch.zip!nested.zip!conv.mbox#3
    └── source ──┘└─ child ─┘└── mbox index ──

* ``!`` separates container hops.
* ``#N`` appends a 0-based index for the Nth message in an MBOX.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from email_ingest.config import MAX_CONTAINER_DEPTH
from email_ingest.state import SkipReason
from email_ingest.unpacker.eml_handler import EML_FORMAT, is_eml
from email_ingest.unpacker.html_handler import HTML_FORMAT, is_html
from email_ingest.unpacker.mbox_handler import is_mbox, iter_mbox_messages
from email_ingest.unpacker.unsupported import is_unsupported
from email_ingest.unpacker.zip_handler import (
    MemberTooLarge,
    ZipCorrupt,
    ZipPasswordProtected,
    is_zip,
    open_zip_members,
)


__all__ = [
    "ExtractedEmail",
    "Skipped",
    "UnpackResult",
    "unpack_source_file",
    "unpack_bytes",
]


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExtractedEmail:
    """One email leaf extracted from a source file."""

    raw_bytes: bytes
    internal_path: str
    container_depth: int
    format: str           # eml | html


@dataclass(frozen=True)
class Skipped:
    """One thing we deliberately did not stage, with a documented reason."""

    internal_path: str
    reason: str           # one of state.SkipReason.*
    details: str = ""


@dataclass
class UnpackResult:
    emails: List[ExtractedEmail] = field(default_factory=list)
    skipped: List[Skipped] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


@dataclass
class _Item:
    raw_bytes: bytes
    internal_path: str
    container_depth: int


def unpack_source_file(
    source_path: Path, source_relpath: str
) -> UnpackResult:
    """Public entry point: unpack a single source file from disk.

    ``source_relpath`` is the file's path inside its partition (the same
    string the scanner emits). It becomes the *root* segment of every
    extracted email's ``internal_path``.
    """
    try:
        data = source_path.read_bytes()
    except OSError as exc:
        result = UnpackResult()
        result.skipped.append(
            Skipped(
                internal_path=source_relpath,
                reason=SkipReason.UNREADABLE,
                details=str(exc),
            )
        )
        return result
    return unpack_bytes(data, source_relpath)


def unpack_bytes(data: bytes, root_internal_path: str) -> UnpackResult:
    """Recursively unpack ``data`` as if it lived at ``root_internal_path``.

    Exposed separately so tests can drive the worklist without writing
    bytes to disk.
    """
    result = UnpackResult()
    worklist: deque[_Item] = deque(
        [_Item(raw_bytes=data, internal_path=root_internal_path, container_depth=0)]
    )

    while worklist:
        item = worklist.popleft()
        _process_item(item, worklist, result)

    return result


def _process_item(
    item: _Item, worklist: deque[_Item], result: UnpackResult
) -> None:
    name = item.internal_path
    base = _basename_of(name)

    if is_eml(base):
        result.emails.append(_leaf(item, EML_FORMAT))
        return
    if is_html(base):
        result.emails.append(_leaf(item, HTML_FORMAT))
        return
    if is_mbox(base):
        _handle_mbox(item, result)
        return
    if is_zip(base):
        _handle_zip(item, worklist, result)
        return
    if is_unsupported(base):
        result.skipped.append(
            Skipped(
                internal_path=name,
                reason=SkipReason.UNSUPPORTED_FORMAT_DEFERRED,
                details=f"format not yet implemented: {base}",
            )
        )
        return

    result.skipped.append(
        Skipped(internal_path=name, reason=SkipReason.NOT_AN_EMAIL)
    )


def _leaf(item: _Item, fmt: str) -> ExtractedEmail:
    return ExtractedEmail(
        raw_bytes=item.raw_bytes,
        internal_path=item.internal_path,
        container_depth=item.container_depth,
        format=fmt,
    )


def _handle_mbox(item: _Item, result: UnpackResult) -> None:
    child_depth = item.container_depth + 1
    if child_depth > MAX_CONTAINER_DEPTH:
        result.skipped.append(
            Skipped(
                internal_path=item.internal_path,
                reason=SkipReason.DEPTH_LIMIT_EXCEEDED,
                details=f"depth {child_depth} > {MAX_CONTAINER_DEPTH}",
            )
        )
        return

    count = 0
    try:
        for msg in iter_mbox_messages(item.raw_bytes):
            result.emails.append(
                ExtractedEmail(
                    raw_bytes=msg.raw_bytes,
                    internal_path=f"{item.internal_path}#{msg.index}",
                    container_depth=child_depth,
                    format=EML_FORMAT,
                )
            )
            count += 1
    except Exception as exc:
        # mailbox raises a variety of errors on truly broken inputs.
        # Treat them all as "corrupt" so callers see one clean reason.
        result.skipped.append(
            Skipped(
                internal_path=item.internal_path,
                reason=SkipReason.CORRUPT_ARCHIVE,
                details=f"mbox: {exc}",
            )
        )
        return

    if count == 0:
        result.skipped.append(
            Skipped(
                internal_path=item.internal_path,
                reason=SkipReason.EMPTY_CONTAINER,
            )
        )


def _handle_zip(
    item: _Item, worklist: deque[_Item], result: UnpackResult
) -> None:
    child_depth = item.container_depth + 1
    if child_depth > MAX_CONTAINER_DEPTH:
        result.skipped.append(
            Skipped(
                internal_path=item.internal_path,
                reason=SkipReason.DEPTH_LIMIT_EXCEEDED,
                details=f"depth {child_depth} > {MAX_CONTAINER_DEPTH}",
            )
        )
        return

    try:
        members = open_zip_members(item.raw_bytes)
    except ZipPasswordProtected as exc:
        result.skipped.append(
            Skipped(
                internal_path=item.internal_path,
                reason=SkipReason.PASSWORD_PROTECTED,
                details=str(exc),
            )
        )
        return
    except ZipCorrupt as exc:
        result.skipped.append(
            Skipped(
                internal_path=item.internal_path,
                reason=SkipReason.CORRUPT_ARCHIVE,
                details=str(exc),
            )
        )
        return
    except MemberTooLarge as exc:
        result.skipped.append(
            Skipped(
                internal_path=item.internal_path,
                reason=SkipReason.CORRUPT_ARCHIVE,
                details=f"member too large: {exc}",
            )
        )
        return

    if not members:
        result.skipped.append(
            Skipped(
                internal_path=item.internal_path,
                reason=SkipReason.EMPTY_CONTAINER,
            )
        )
        return

    for member in members:
        worklist.append(
            _Item(
                raw_bytes=member.raw_bytes,
                internal_path=f"{item.internal_path}!{member.name}",
                container_depth=child_depth,
            )
        )


def _basename_of(internal_path: str) -> str:
    """Return the trailing segment (after the last ``!`` or ``/``)."""
    # Strip mbox-index suffix if present (shouldn't normally appear here,
    # but defensive).
    head = internal_path.split("#", 1)[0]
    head = head.rsplit("!", 1)[-1]
    head = head.rsplit("/", 1)[-1]
    return head
