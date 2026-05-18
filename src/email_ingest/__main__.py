"""Allow `python -m email_ingest ...` invocation."""

from email_ingest.cli import main


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
