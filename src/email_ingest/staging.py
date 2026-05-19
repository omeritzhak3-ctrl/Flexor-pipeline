"""Atomic staging into the content-addressed pool + per-partition manifests.

Pool layout::

    state/staging/emails/<aa>/<email_id>.<ext>

``<aa>`` shards by the first two hex chars of the id so a single directory
never grows unbounded. Writes are atomic: bytes go to ``state/tmp/<uuid>``
first, get ``fsync``ed, then ``rename``d into the pool. Pool entries are
*immutable* and *idempotent*: writing the same id twice is a no-op.

Manifests are JSONL append logs, one file per partition. Each line is a
self-describing record of "this email_id is present in this partition,
came from this source via this internal path".
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import List, Sequence

from email_ingest.config import PipelineConfig
from email_ingest.identity import pool_relpath


# ---------------------------------------------------------------------------
# Pool staging
# ---------------------------------------------------------------------------


def stage_email_bytes(
    config: PipelineConfig,
    email_id: str,
    raw_bytes: bytes,
    *,
    suffix: str = ".eml",
) -> str:
    """Atomically place ``raw_bytes`` in the content-addressed pool.

    Returns the pool-relative path (e.g. ``ab/ab12...ef.eml``). Safe to
    call multiple times for the same id: if the target already exists we
    skip the rename and return its path.
    """
    pool_rel = pool_relpath(email_id, suffix=suffix)
    target = config.pool_root / pool_rel

    if target.exists():
        return pool_rel

    target.parent.mkdir(parents=True, exist_ok=True)
    config.tmp_root.mkdir(parents=True, exist_ok=True)

    tmp_path = config.tmp_root / f"{uuid.uuid4().hex}{suffix}"
    try:
        with tmp_path.open("wb") as f:
            f.write(raw_bytes)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, target)
        _fsync_dir(target.parent)
    finally:
        # If the rename happened, tmp_path is already gone. If it didn't
        # (because of an exception mid-write), make sure we don't leak.
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass

    return pool_rel


def email_in_pool(config: PipelineConfig, email_id: str, *, suffix: str = ".eml") -> bool:
    return (config.pool_root / pool_relpath(email_id, suffix=suffix)).exists()


# ---------------------------------------------------------------------------
# Manifest append
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ManifestLineage:
    """One entry inside a manifest line's ``lineage`` array."""

    source: str          # relpath of the source file inside its partition
    internal_path: str   # full path inside the source, with `!`/`#` accumulators
    depth: int           # container_depth


@dataclass(frozen=True)
class ManifestLine:
    email_id: str
    pool_path: str       # pool-relative, e.g. "emails/ab/ab12...eml"
    format: str          # eml | html
    lineage: Sequence[ManifestLineage]

    def to_jsonl(self) -> str:
        return json.dumps(
            {
                "email_id": self.email_id,
                "pool_path": self.pool_path,
                "format": self.format,
                "lineage": [
                    {
                        "source": l.source,
                        "internal_path": l.internal_path,
                        "depth": l.depth,
                    }
                    for l in self.lineage
                ],
            },
            separators=(",", ":"),
        )


def append_manifest_lines(
    config: PipelineConfig,
    namespace: str,
    partition: str,
    lines: List[ManifestLine],
) -> Path:
    """Append one JSONL record per line to the partition's manifest.

    Returns the manifest path. fsync is called after the write so the
    append survives a crash.
    """
    if not lines:
        return _manifest_path(config, namespace, partition)

    manifest_dir = config.manifests_root / namespace / partition
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / "manifest.jsonl"

    blob = ("\n".join(line.to_jsonl() for line in lines) + "\n").encode("utf-8")
    with manifest_path.open("ab") as f:
        f.write(blob)
        f.flush()
        os.fsync(f.fileno())
    return manifest_path


def _manifest_path(config: PipelineConfig, namespace: str, partition: str) -> Path:
    return config.manifests_root / namespace / partition / "manifest.jsonl"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fsync_dir(path: Path) -> None:
    """Best-effort directory fsync so the rename is durable.

    Not supported on every platform; we swallow ``OSError`` because the
    rename itself is already atomic on POSIX and the fsync is belt-and-
    braces.
    """
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except (OSError, NotImplementedError):
        pass


__all__ = [
    "ManifestLineage",
    "ManifestLine",
    "append_manifest_lines",
    "email_in_pool",
    "stage_email_bytes",
]
