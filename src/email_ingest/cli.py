"""Command-line entrypoint.

Usage::

    python -m email_ingest run \\
        --mode {backfill,incremental} \\
        --bucket test_data \\
        --state state/

``backfill`` and ``incremental`` route to the same orchestrator because
the pipeline is CDC-driven: the first run naturally processes everything,
subsequent runs only touch new/changed files. The flag is preserved for
operational clarity (and so a future implementation can branch on it —
e.g. parallel ingest for backfill, single-stream for incremental).
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

from email_ingest.config import PipelineConfig
from email_ingest.pipeline import RunStats, run_pipeline
from email_ingest.state import open_db


LOG = logging.getLogger("email_ingest")


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    _configure_logging(args.verbose)

    if args.command == "run":
        return _cmd_run(args)
    parser.error(f"unknown command: {args.command}")
    return 2


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------


def _cmd_run(args: argparse.Namespace) -> int:
    bucket = Path(args.bucket).resolve()
    state = Path(args.state).resolve()

    if not bucket.exists():
        LOG.error("bucket path does not exist: %s", bucket)
        return 2

    config = PipelineConfig(bucket_root=bucket, state_root=state)
    LOG.info("starting pipeline (mode=%s)", args.mode)
    LOG.info("  bucket = %s", config.bucket_root)
    LOG.info("  state  = %s", config.state_root)

    conn = open_db(config.db_path)
    try:
        stats = run_pipeline(config, conn)
    finally:
        conn.close()

    _print_summary(stats, mode=args.mode)
    return 0


# ---------------------------------------------------------------------------
# Argparse wiring
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="email_ingest",
        description="Email ingestion pipeline: discover, unpack, dedupe, "
        "and stage emails from date-partitioned customer buckets.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="increase logging verbosity (-v info, -vv debug)",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser(
        "run",
        help="Run a single pipeline pass over a bucket.",
    )
    run.add_argument(
        "--mode",
        choices=("backfill", "incremental"),
        default="incremental",
        help="Operational mode. Both share the same code path; this flag "
        "is recorded in logs and reserved for future divergence.",
    )
    run.add_argument(
        "--bucket",
        required=True,
        help="Root of the customer bucket (e.g. test_data/).",
    )
    run.add_argument(
        "--state",
        required=True,
        help="Root of the pipeline state directory (DB + staging).",
    )
    return parser


def _configure_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _print_summary(stats: RunStats, *, mode: str) -> None:
    """Single-line summary on stdout + per-counter detail to stderr.

    Stdout is kept terse and machine-readable for pipelining; logs go
    through stderr.
    """
    headline = (
        f"mode={mode} scanned={stats.files_scanned} "
        f"new={stats.files_new} changed={stats.files_changed} "
        f"unchanged={stats.files_unchanged} metadata_only={stats.files_metadata_only} "
        f"failed={stats.files_failed} "
        f"emails_staged={stats.emails_staged} relinked={stats.emails_relinked} "
        f"skipped={stats.skipped}"
    )
    print(headline)

    LOG.info("pipeline run complete")
    for key, value in asdict(stats).items():
        LOG.info("  %s = %s", key, value)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
