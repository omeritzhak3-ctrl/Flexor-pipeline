"""ZIP container handler.

Reads a ZIP from raw bytes and yields ``(name, member_bytes)`` for each
regular-file member, in a stable order. The caller is responsible for
re-classifying each member (a ZIP can contain ZIPs, MBOXes, EMLs, or
junk) and for tracking nesting depth.

Three failure modes are translated into structured outcomes rather than
exceptions:

* ``ZipPasswordProtected``  — the archive (or a member) needs a password.
* ``ZipCorrupt``            — bytes are not a parseable ZIP, or a member
  fails its CRC check.
* ``MemberTooLarge``        — a member's declared size exceeds the
  configured per-member cap (defense against zip bombs).

The caller maps these to ``skipped`` rows. We never raise out of the
unpacker driver — one bad archive should never tank a whole run.
"""

from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from typing import Iterator, List

from email_ingest.config import MAX_MEMBER_SIZE_BYTES


class ZipPasswordProtected(Exception):
    """Raised when the ZIP (or a member) requires a password."""


class ZipCorrupt(Exception):
    """Raised when the ZIP is malformed or a member fails its CRC."""


class MemberTooLarge(Exception):
    """Raised when a ZIP member's declared size exceeds the cap."""


@dataclass(frozen=True)
class ZipMember:
    name: str            # member path inside the zip, POSIX-style
    raw_bytes: bytes


def open_zip_members(zip_bytes: bytes) -> List[ZipMember]:
    """Return all regular-file members of a ZIP in deterministic order.

    Raises ``ZipCorrupt`` / ``ZipPasswordProtected`` / ``MemberTooLarge``
    instead of leaking the underlying ``zipfile`` exceptions.
    """
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile as exc:
        raise ZipCorrupt(str(exc)) from exc

    try:
        members: List[ZipMember] = []
        # Sort by name for deterministic processing order. Skip directory
        # entries (their name ends with "/") and any name that escapes
        # the archive via absolute or ``..`` traversal — we never write
        # member files to disk by name (the pool is content-addressed),
        # but defense-in-depth costs nothing.
        for info in sorted(zf.infolist(), key=lambda i: i.filename):
            if info.is_dir():
                continue
            if _is_unsafe_member_name(info.filename):
                continue
            if info.file_size > MAX_MEMBER_SIZE_BYTES:
                raise MemberTooLarge(
                    f"member {info.filename!r} declares {info.file_size} bytes"
                )
            try:
                data = zf.read(info)
            except RuntimeError as exc:
                # ``zipfile`` raises RuntimeError("File ... is encrypted...")
                # when a password is required. There's no dedicated
                # exception class, so we sniff the message.
                if "encrypted" in str(exc).lower() or "password" in str(exc).lower():
                    raise ZipPasswordProtected(str(exc)) from exc
                raise ZipCorrupt(str(exc)) from exc
            except zipfile.BadZipFile as exc:
                raise ZipCorrupt(str(exc)) from exc
            members.append(ZipMember(name=info.filename, raw_bytes=data))
        return members
    finally:
        zf.close()


def is_zip(name: str) -> bool:
    return name.lower().endswith(".zip")


def _is_unsafe_member_name(name: str) -> bool:
    """Reject absolute paths and parent-directory traversal."""
    if name.startswith("/") or name.startswith("\\"):
        return True
    parts = name.replace("\\", "/").split("/")
    return any(part == ".." for part in parts)


# Re-exported for symmetry with mbox_handler.iter_mbox_messages.
def iter_zip_members(zip_bytes: bytes) -> Iterator[ZipMember]:
    yield from open_zip_members(zip_bytes)
