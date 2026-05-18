# Email Ingestion Pipeline

A Python pipeline that discovers email files in date-partitioned customer
buckets, recursively unpacks containers (ZIP, MBOX), deduplicates by content
hash, preserves full lineage, and stages clean individual emails to a
content-addressed pool. State is backed by SQLite for CDC and crash-safety.

> **Status:** work in progress. This README is a skeleton; the full design
> document, architecture diagram, edge-case table, and P1 scaling section
> land in Phase 7 of the plan.

## Repo layout

```
flexor/
  pyproject.toml
  README.md
  AI_PROCESS.md
  src/email_ingest/
    cli.py
    config.py
    state.py
    identity.py
    scanner.py
    cdc.py
    unpacker/
      zip_handler.py
      mbox_handler.py
      eml_handler.py
      html_handler.py
      unsupported.py
    staging.py
    pipeline.py
  tests/
  test_data/                # provided fixtures
  state/                    # gitignored runtime state
```

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Run

```bash
# Backfill (process everything currently in the bucket)
python -m email_ingest run --mode backfill --bucket test_data --state state/

# Incremental (only new/changed files since last run)
python -m email_ingest run --mode incremental --bucket test_data --state state/
```

## Test

```bash
pytest
```

## Design document

To be expanded in Phase 7. The current source of truth for design decisions
is the planning file under `.cursor/plans/`.

## AI process

See [`AI_PROCESS.md`](AI_PROCESS.md).
