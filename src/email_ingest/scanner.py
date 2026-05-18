"""Bucket scanner.

Walks ``<bucket_root>/<namespace>/timestamp=YYYY-MM-DD/...`` and yields one
``ScannedFile`` per file found, with the metadata CDC needs (size,
mtime_ns, absolute path on disk, relative path inside the partition).

The scanner is intentionally cheap — it only ``stat()``s files. It does
not read file contents or compute hashes; that's CDC's job (and only when
the cheap check says we need to look further).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


# A partition directory is exactly ``timestamp=YYYY-MM-DD``. Anything else
# at the namespace level is ignored (forward-compat for non-partition
# sidecars the customer might park there).
_PARTITION_RE = re.compile(r"^timestamp=\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class ScannedFile:
    """One bucket file the scanner discovered.

    ``relpath`` is relative to the partition directory (so an MBOX at
    ``namespace_a/timestamp=2024-07-15/conversations.mbox`` has
    ``relpath="conversations.mbox"``). Subdirectories inside a partition
    are allowed and preserved in ``relpath``.
    """

    namespace: str
    partition: str           # e.g. "timestamp=2024-07-15"
    relpath: str             # POSIX-style, relative to the partition dir
    abspath: Path
    size_bytes: int
    mtime_ns: int


def scan_bucket(bucket_root: Path) -> Iterator[ScannedFile]:
    """Yield every file under ``<bucket_root>/<namespace>/timestamp=*/...``.

    Order is deterministic (sorted by namespace, then partition, then
    relpath) so test snapshots and crash-recovery replays are stable.
    """
    if not bucket_root.exists():
        return
    if not bucket_root.is_dir():
        raise NotADirectoryError(f"bucket_root is not a directory: {bucket_root}")

    for namespace_dir in sorted(_iter_subdirs(bucket_root)):
        namespace = namespace_dir.name
        for partition_dir in sorted(_iter_subdirs(namespace_dir)):
            if not _PARTITION_RE.match(partition_dir.name):
                # Silently skip non-partition siblings; they're not ours.
                continue
            partition = partition_dir.name
            yield from _walk_partition(namespace, partition, partition_dir)


def _walk_partition(
    namespace: str, partition: str, partition_dir: Path
) -> Iterator[ScannedFile]:
    """Walk a single partition directory, recursing into sub-folders."""
    # Use os.walk for cheap depth-first traversal; sort at each level for
    # determinism.
    for dirpath, dirnames, filenames in os.walk(partition_dir):
        dirnames.sort()
        for filename in sorted(filenames):
            abspath = Path(dirpath) / filename
            try:
                st = abspath.stat()
            except FileNotFoundError:
                # Race with a deleting writer; ignore.
                continue
            if not _is_regular_file(st.st_mode):
                continue

            rel = abspath.relative_to(partition_dir).as_posix()
            yield ScannedFile(
                namespace=namespace,
                partition=partition,
                relpath=rel,
                abspath=abspath,
                size_bytes=st.st_size,
                mtime_ns=st.st_mtime_ns,
            )


def _iter_subdirs(parent: Path) -> Iterator[Path]:
    for entry in parent.iterdir():
        if entry.is_dir():
            yield entry


def _is_regular_file(mode: int) -> bool:
    # Equivalent to stat.S_ISREG without importing stat for a one-liner.
    return (mode & 0o170000) == 0o100000
