"""Passthrough leaf handler for ``.html`` / ``.htm`` email exports.

HTML email exports are leaves: a single, self-contained email rendered as
HTML. We trust the extension at P0; richer MIME sniffing to reject
non-email HTML pages is a P1 enhancement.
"""

from __future__ import annotations


HTML_FORMAT = "html"


def is_html(name: str) -> bool:
    lower = name.lower()
    return lower.endswith(".html") or lower.endswith(".htm")
