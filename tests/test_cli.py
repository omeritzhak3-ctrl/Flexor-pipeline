"""CLI smoke tests.

We don't aim for exhaustive coverage of the CLI — the heavy lifting is
already pinned by pipeline / orchestrator tests. These tests confirm
that the argparse wiring, exit codes, and headline output behave as
documented.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from conftest import EML_A, write_file
from email_ingest.cli import main
from email_ingest.config import PipelineConfig
from email_ingest.state import open_db


def _setup_bucket(tmp_path: Path) -> tuple[Path, Path]:
    bucket = tmp_path / "bucket"
    state = tmp_path / "state"
    write_file(bucket / "ns" / "timestamp=2024-07-15" / "x.eml", EML_A)
    return bucket, state


def test_run_command_processes_bucket(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    bucket, state = _setup_bucket(tmp_path)
    exit_code = main(
        ["run", "--mode", "backfill", "--bucket", str(bucket), "--state", str(state)]
    )
    assert exit_code == 0

    out = capsys.readouterr().out.strip()
    assert "mode=backfill" in out
    assert "scanned=1" in out
    assert "new=1" in out
    assert "emails_staged=1" in out
    assert "skipped=0" in out

    # State on disk should match the headline.
    config = PipelineConfig(bucket_root=bucket, state_root=state)
    conn = open_db(config.db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM emails").fetchone()[0] == 1
    finally:
        conn.close()


def test_run_command_incremental_is_noop_after_backfill(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    bucket, state = _setup_bucket(tmp_path)
    assert main(
        ["run", "--mode", "backfill", "--bucket", str(bucket), "--state", str(state)]
    ) == 0
    capsys.readouterr()

    assert main(
        ["run", "--mode", "incremental", "--bucket", str(bucket), "--state", str(state)]
    ) == 0

    out = capsys.readouterr().out
    assert "mode=incremental" in out
    assert "unchanged=1" in out
    assert "emails_staged=0" in out


def test_run_command_rejects_missing_bucket(tmp_path: Path) -> None:
    exit_code = main(
        [
            "run",
            "--mode",
            "backfill",
            "--bucket",
            str(tmp_path / "nope"),
            "--state",
            str(tmp_path / "state"),
        ]
    )
    assert exit_code == 2


def test_help_subcommand_required(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main([])  # no subcommand
    assert exc_info.value.code == 2
