"""Phase 0 smoke test: the package imports and exposes a version."""

import email_ingest


def test_package_has_version() -> None:
    assert isinstance(email_ingest.__version__, str)
    assert email_ingest.__version__
