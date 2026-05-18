"""Passthrough leaf handler for ``.eml`` files.

EML files are RFC 822 single-message emails. They are leaves of the
worklist: we hand the bytes through unchanged and let identity /
canonicalization handle dedup.
"""

from __future__ import annotations


EML_FORMAT = "eml"


def is_eml(name: str) -> bool:
    return name.lower().endswith(".eml")
