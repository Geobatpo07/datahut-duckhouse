"""The connector contract: one method, same shape for every family."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from ingestion.config.models import SourceConfig


class Connector(Protocol):
    """Extracts one source's resources to Parquet, one file per table."""

    def build_resources(self, source: SourceConfig, work_dir: Path) -> dict[str, Path]:
        """Return ``{prefixed_table_name: written_parquet_path}``, one
        entry per table/resource, written in one call per resource -- this
        is what gives the engine's "one Iceberg commit per table per run"
        its foundation."""
        ...
