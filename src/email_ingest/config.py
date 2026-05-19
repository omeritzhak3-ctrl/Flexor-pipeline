"""Runtime configuration for the email ingestion pipeline.

Values are intentionally simple module-level constants for the P0 build;
they would migrate to env vars / a config file in production.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


# Hard cap on nested-container depth. Exceeding it produces a `skipped` row
# with reason `depth_limit_exceeded` instead of risking a zip-bomb style blowup.
MAX_CONTAINER_DEPTH: int = 8

# Per-file size guard for archive members during extraction (bytes). 256 MiB
# is more than enough for realistic emails while keeping a sane upper bound.
MAX_MEMBER_SIZE_BYTES: int = 256 * 1024 * 1024


@dataclass(frozen=True)
class PipelineConfig:
    """Filesystem layout the pipeline operates over."""

    bucket_root: Path
    state_root: Path

    @property
    def db_path(self) -> Path:
        return self.state_root / "state.db"

    @property
    def staging_root(self) -> Path:
        return self.state_root / "staging"

    @property
    def pool_root(self) -> Path:
        return self.staging_root / "emails"

    @property
    def manifests_root(self) -> Path:
        return self.staging_root / "manifests"

    @property
    def tmp_root(self) -> Path:
        return self.state_root / "tmp"
