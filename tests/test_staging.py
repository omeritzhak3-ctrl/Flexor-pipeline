"""Staging tests: atomic pool writes + manifest appends."""

from __future__ import annotations

import json
from pathlib import Path

from email_ingest.config import PipelineConfig
from email_ingest.identity import pool_relpath
from email_ingest.staging import (
    ManifestLine,
    ManifestLineage,
    append_manifest_lines,
    email_in_pool,
    stage_email_bytes,
)


EMAIL_ID = "ab12cd34" + "0" * 56


def _config(tmp_path: Path) -> PipelineConfig:
    return PipelineConfig(bucket_root=tmp_path / "bucket", state_root=tmp_path / "state")


class TestStageEmailBytes:
    def test_writes_to_sharded_pool(self, tmp_path: Path) -> None:
        cfg = _config(tmp_path)
        rel = stage_email_bytes(cfg, EMAIL_ID, b"hello world")
        assert rel == pool_relpath(EMAIL_ID)
        target = cfg.pool_root / rel
        assert target.exists()
        assert target.read_bytes() == b"hello world"

    def test_idempotent_does_not_overwrite(self, tmp_path: Path) -> None:
        """Pool entries are immutable. A second call with the same id is a no-op."""
        cfg = _config(tmp_path)
        stage_email_bytes(cfg, EMAIL_ID, b"original")
        # If staging tried to overwrite, the bytes would change. Pass
        # different bytes the second time to prove it doesn't.
        stage_email_bytes(cfg, EMAIL_ID, b"DIFFERENT")
        target = cfg.pool_root / pool_relpath(EMAIL_ID)
        assert target.read_bytes() == b"original"

    def test_tmp_cleared_after_rename(self, tmp_path: Path) -> None:
        cfg = _config(tmp_path)
        stage_email_bytes(cfg, EMAIL_ID, b"x")
        # tmp may exist but should be empty (we created it, then renamed
        # the only file out of it).
        if cfg.tmp_root.exists():
            assert list(cfg.tmp_root.iterdir()) == []

    def test_email_in_pool_predicate(self, tmp_path: Path) -> None:
        cfg = _config(tmp_path)
        assert not email_in_pool(cfg, EMAIL_ID)
        stage_email_bytes(cfg, EMAIL_ID, b"x")
        assert email_in_pool(cfg, EMAIL_ID)

    def test_html_suffix(self, tmp_path: Path) -> None:
        cfg = _config(tmp_path)
        rel = stage_email_bytes(cfg, EMAIL_ID, b"<html/>", suffix=".html")
        assert rel.endswith(".html")
        assert (cfg.pool_root / rel).exists()


class TestAppendManifestLines:
    def test_writes_jsonl(self, tmp_path: Path) -> None:
        cfg = _config(tmp_path)
        line = ManifestLine(
            email_id=EMAIL_ID,
            pool_path=f"emails/{pool_relpath(EMAIL_ID)}",
            format="eml",
            lineage=[
                ManifestLineage(
                    source="batch.zip",
                    internal_path="batch.zip!invoice.eml",
                    depth=1,
                )
            ],
        )
        path = append_manifest_lines(cfg, "ns", "timestamp=2024-07-15", [line])
        assert path == (
            cfg.manifests_root / "ns" / "timestamp=2024-07-15" / "manifest.jsonl"
        )
        records = [json.loads(l) for l in path.read_text().splitlines()]
        assert records == [
            {
                "email_id": EMAIL_ID,
                "pool_path": f"emails/{pool_relpath(EMAIL_ID)}",
                "format": "eml",
                "lineage": [
                    {
                        "source": "batch.zip",
                        "internal_path": "batch.zip!invoice.eml",
                        "depth": 1,
                    }
                ],
            }
        ]

    def test_appends_without_truncating(self, tmp_path: Path) -> None:
        cfg = _config(tmp_path)
        line1 = ManifestLine(
            email_id=EMAIL_ID,
            pool_path="emails/x",
            format="eml",
            lineage=[ManifestLineage("a", "a", 0)],
        )
        line2 = ManifestLine(
            email_id="cd" + "0" * 62,
            pool_path="emails/y",
            format="html",
            lineage=[ManifestLineage("b", "b", 0)],
        )
        append_manifest_lines(cfg, "ns", "timestamp=2024-07-15", [line1])
        append_manifest_lines(cfg, "ns", "timestamp=2024-07-15", [line2])

        path = (
            cfg.manifests_root / "ns" / "timestamp=2024-07-15" / "manifest.jsonl"
        )
        records = [json.loads(l) for l in path.read_text().splitlines()]
        assert len(records) == 2
        assert records[0]["email_id"] == EMAIL_ID
        assert records[1]["email_id"] == "cd" + "0" * 62

    def test_empty_list_is_noop(self, tmp_path: Path) -> None:
        cfg = _config(tmp_path)
        append_manifest_lines(cfg, "ns", "timestamp=2024-07-15", [])
        manifest = (
            cfg.manifests_root / "ns" / "timestamp=2024-07-15" / "manifest.jsonl"
        )
        assert not manifest.exists()
