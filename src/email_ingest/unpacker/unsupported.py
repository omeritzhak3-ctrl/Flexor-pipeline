"""Recognized-but-deferred formats: MSG, PST.

We keep the classifier honest by recognizing these extensions so they
land in the ``skipped`` table with reason ``unsupported_format_deferred``
rather than the generic ``not_an_email``. That makes it a one-line change
to add real handlers later: just point the registry at a new module.
"""

from __future__ import annotations


UNSUPPORTED_EXTENSIONS = frozenset({".msg", ".pst"})


def is_unsupported(name: str) -> bool:
    lower = name.lower()
    return any(lower.endswith(ext) for ext in UNSUPPORTED_EXTENSIONS)
