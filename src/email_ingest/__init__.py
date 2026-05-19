"""Email ingestion pipeline.

Discovers files in date-partitioned customer buckets, recursively unpacks
container formats, deduplicates by content hash, preserves full lineage, and
stages clean individual emails to a content-addressed pool backed by SQLite.
"""

__version__ = "0.1.0"
