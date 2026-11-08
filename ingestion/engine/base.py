"""The engine contract: one source in, one typed result out."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from ingestion.config.models import SourceConfig


@dataclass
class TableResult:
    """One table's outcome within a run."""

    table: str
    verb: str  # "created" | "overwritten" | "appended" | "merged"
    rows: int
    commits: int = 1


@dataclass
class RunResult:
    """One source's outcome -- the tables it wrote, in order."""

    source: str
    tables: list[TableResult] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        return sum(t.rows for t in self.tables)


class Ingestor(Protocol):
    """Runs one configured source end to end: extract, then write through
    the right sink for its ``write_disposition``, logging every table."""

    def run(self, source: SourceConfig) -> RunResult: ...
