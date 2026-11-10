"""Pydantic models for one ``sources.yml`` entry (Vague 1b, task 1; moved
here unchanged from ``ingestion/dlt/registry.py`` in the ``feat/dhd-ingest``
restructuring, step A -- see ``docs/INGESTION_ARCHITECTURE.md``).

This module is deliberately inert: it never imports ``dlt``, a connector,
a sink, or the engine -- only pydantic and stdlib. Cross-field validation
*rules* (which combinations are rejected or warned about) live in
``rules.py`` and are called from here, not defined here, so a reviewer can
read "what shape is valid" (this file) separately from "why" (``rules.py``).
"""

from __future__ import annotations

import os
from typing import Literal

from pydantic import BaseModel, Field, model_validator

from ingestion.config import rules

SQL_DIALECTS = ("sqlite", "postgresql", "mysql", "sqlserver")
FILE_FORMATS = ("csv", "json", "parquet")


class SourcesConfigError(RuntimeError):
    """``sources.yml`` is missing, malformed, or fails schema validation."""


class IncrementalConfig(BaseModel):
    """A cursor for pulling only new/changed rows (task 3's incremental extraction).

    ``type`` is a dlt cursor type hint (e.g. ``timestamp``, ``bigint``) -- see
    ``connectors/_dlt_sources.py``'s use of ``dlt.sources.incremental``.
    """

    column: str
    type: str = "timestamp"


class SourceConfig(BaseModel):
    """One ``sources.yml`` entry. ``model_config`` forbids unknown fields so a
    typo (e.g. ``write_disposiiton``) fails ``dhd ingest validate`` instead of
    silently doing nothing."""

    model_config = {"extra": "forbid", "populate_by_name": True}

    name: str
    family: Literal["sql", "files", "rest"]
    connector: str
    secret_ref: str | None = None
    schedule: str | None = None  # cron; informative only, nothing reads it yet
    resources: list[str] = Field(default_factory=list)
    incremental: IncrementalConfig | None = None
    write_disposition: Literal["append", "merge", "replace"] = "replace"
    primary_key: list[str] | None = None
    # Which sink writes this source's tables (ADR-0006, decision 5):
    # "auto" (default) picks direct whenever this process can reach the
    # catalog/storage directly, flight otherwise, with no size threshold
    # (docs/spikes/NESSIE_SPIKES_2.md, T6 found no crossover point).
    # Overridable per run with `dhd ingest run --sink`.
    sink: Literal["auto", "flight", "direct"] = "auto"
    schema_contract: Literal["evolve", "freeze", "discard_value"] = "evolve"
    target_prefix: str | None = None
    # How many rows per Iceberg commit when the engine streams a table in
    # chunks (fix/ingest-batching-grpc, part A): more chunks trade lower
    # peak memory for more commits -- see docs/DLT_EVAL.md's chunked-batches
    # measurement (10M rows: 899 MiB/1 commit vs 362 MiB/20 commits). Default
    # matches that measurement's own batch size. ``null`` disables chunking
    # (one commit, the pre-fix behavior) -- ``write_disposition: replace``
    # always ignores this and commits once regardless (see
    # ``ingestion/sinks/base.py``'s docstring for why).
    batch_rows: int | None = 500_000
    # Reserved for the later "50 instances" phase (per-tenant source copies).
    # Deliberately unused here -- no instance-aware logic exists yet.
    instance: str | None = None

    # -- sql family -----------------------------------------------------
    dialect: str | None = None
    schema_name: str | None = Field(default=None, alias="schema")

    # -- files family -----------------------------------------------------
    path: str | None = None
    format: str | None = None
    http_url: str | None = None  # optional: download before reading `format`

    # -- rest family -----------------------------------------------------
    base_url: str | None = None
    # Free-form, passed through to dlt's rest_api_source per-resource endpoint
    # config (headers/params/paginator) -- kept generic rather than modeled
    # field-by-field, since those shapes are already dlt's own declarative
    # schema (see connectors/_dlt_sources.py).
    rest_options: dict | None = None

    # True if this source talks to a real external network/service -- such
    # tests are marked `network` and excluded by default (task 4).
    network: bool = False

    @property
    def prefix(self) -> str:
        return self.target_prefix or self.name

    def effective_batch_rows(self, resolved_sink: str) -> int | None:
        """``batch_rows``, defaulted by the *resolved* sink (ADR-0006,
        decision 5) when this source didn't set it explicitly in
        ``sources.yml``. Flight's cost grows with the whole re-exported
        DuckDB database, so batching only adds more of that same cost per
        run (docs/DLT_EVAL.md, section 6) -- its default is a single
        commit. Direct keeps the existing 500,000 default (same
        measurement). An explicit ``batch_rows`` (including an explicit
        ``null``, "disable chunking") always wins, regardless of sink --
        checked via ``model_fields_set``, not ``self.batch_rows``'s value,
        so an explicit ``batch_rows: 500000`` is distinguishable from
        never having set it at all.
        """
        if "batch_rows" in self.model_fields_set:
            return self.batch_rows
        return 500_000 if resolved_sink == "direct" else None

    def redact(self, message: str) -> str:
        """Strip this source's secret *value* (never logged by name --
        ``secret_ref`` already is just the env var name) out of an error
        message before it reaches a log, the run log, or the CLI."""
        if not self.secret_ref:
            return message
        value = os.getenv(self.secret_ref)
        if not value:
            return message
        return message.replace(value, "***")

    @property
    def connection_url(self) -> str:
        """sql family: read ``secret_ref``'s environment variable."""
        if not self.secret_ref:
            raise SourcesConfigError(f"'{self.name}' has no secret_ref configured.")
        url = os.getenv(self.secret_ref)
        if not url:
            raise SourcesConfigError(
                f"Source '{self.name}': environment variable '{self.secret_ref}' "
                f"is not set. See .devcontainer/.env.example."
            )
        return url

    @model_validator(mode="after")
    def _apply_rules(self) -> SourceConfig:
        rules.check_write_disposition(self)
        rules.check_family_fields(self)
        rules.check_batch_rows(self)
        return self
