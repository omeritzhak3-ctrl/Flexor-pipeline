"""Scanner tests.

These run against synthetic bucket trees laid down in ``tmp_path``. We
keep them filesystem-real (no mocks) because the whole point of the
scanner is to faithfully describe what's on disk.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from email_ingest.scanner import scan_bucket


def _write(path: Path, content: bytes = b"x") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def test_walks_partition_directories(tmp_path: Path) -> None:
    bucket = tmp_path / "bucket"
    _write(bucket / "ns_a" / "timestamp=2024-07-15" / "a.eml", b"a")
    _write(bucket / "ns_a" / "timestamp=2024-07-15" / "b.eml", b"bb")
    _write(bucket / "ns_a" / "timestamp=2024-07-16" / "c.eml", b"ccc")

    files = list(scan_bucket(bucket))
    rels = [(f.namespace, f.partition, f.relpath) for f in files]
    assert rels == [
        ("ns_a", "timestamp=2024-07-15", "a.eml"),
        ("ns_a", "timestamp=2024-07-15", "b.eml"),
        ("ns_a", "timestamp=2024-07-16", "c.eml"),
    ]


def test_captures_size_and_mtime(tmp_path: Path) -> None:
    bucket = tmp_path / "bucket"
    payload = b"hello world\n"
    p = _write(bucket / "ns" / "timestamp=2024-07-15" / "x.eml", payload)

    [scanned] = list(scan_bucket(bucket))
    assert scanned.size_bytes == len(payload)
    assert scanned.mtime_ns == p.stat().st_mtime_ns
    assert scanned.abspath == p


def test_walks_multiple_namespaces_sorted(tmp_path: Path) -> None:
    bucket = tmp_path / "bucket"
    _write(bucket / "ns_b" / "timestamp=2024-07-15" / "b.eml")
    _write(bucket / "ns_a" / "timestamp=2024-07-15" / "a.eml")

    namespaces = [f.namespace for f in scan_bucket(bucket)]
    assert namespaces == ["ns_a", "ns_b"]


def test_ignores_non_partition_directories(tmp_path: Path) -> None:
    bucket = tmp_path / "bucket"
    _write(bucket / "ns" / "timestamp=2024-07-15" / "x.eml")
    _write(bucket / "ns" / "_metadata" / "ignored.txt")
    _write(bucket / "ns" / "timestamp=bad-format" / "y.eml")

    rels = [f.relpath for f in scan_bucket(bucket)]
    assert rels == ["x.eml"]


def test_recurses_into_partition_subdirectories(tmp_path: Path) -> None:
    """A customer might park nested folders inside a partition; preserve them
    in ``relpath`` so identity stays unique per location."""
    bucket = tmp_path / "bucket"
    _write(bucket / "ns" / "timestamp=2024-07-15" / "sub" / "deep.eml")

    [scanned] = list(scan_bucket(bucket))
    assert scanned.relpath == "sub/deep.eml"


def test_missing_bucket_yields_nothing(tmp_path: Path) -> None:
    bucket = tmp_path / "does-not-exist"
    assert list(scan_bucket(bucket)) == []


def test_bucket_must_be_directory(tmp_path: Path) -> None:
    bucket = tmp_path / "not-a-dir"
    bucket.write_bytes(b"oops")
    with pytest.raises(NotADirectoryError):
        list(scan_bucket(bucket))


def test_empty_partition_yields_nothing(tmp_path: Path) -> None:
    bucket = tmp_path / "bucket"
    (bucket / "ns" / "timestamp=2024-07-15").mkdir(parents=True)
    assert list(scan_bucket(bucket)) == []
