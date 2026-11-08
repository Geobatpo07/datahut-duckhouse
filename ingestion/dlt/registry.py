"""Source registry v2 (Vague 1b, task 1): a validated schema for ``sources.yml``.

Replaces the ad hoc ``Source`` dataclass from Vague 1 with a pydantic model
that captures everything the three connector families (``sql``/``files``/
``rest``, see ``connectors/``) and the exploitation layer (run log, grouped
commits, schema contracts -- task 3) need to operate without a code change
per source. ``dhd sources validate``/``dhd sources list`` (``cli.py``) are
thin wrappers over :func:`load_sources`.

Line numbers in error messages come from re-walking the YAML's parse tree
(:func:`_entries_with_lines`) -- ``yaml.safe_load`` alone discards them.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, ValidationError, model_validator

SOURCES_YML = Path(__file__).parent / "sources.yml"

SQL_DIALECTS = ("sqlite", "postgresql", "mysql", "sqlserver")
FILE_FORMATS = ("csv", "json", "parquet")


class SourcesConfigError(RuntimeError):
    """``sources.yml`` is missing, malformed, or fails schema validation."""


class IncrementalConfig(BaseModel):
    """A cursor for pulling only new/changed rows (task 3's incremental extraction).

    ``type`` is a dlt cursor type hint (e.g. ``timestamp``, ``bigint``) -- see
    ``connectors/sql.py``'s use of ``dlt.sources.incremental``.
    """

    column: str
    type: str = "timestamp"


class SourceConfig(BaseModel):
    """One ``sources.yml`` entry. ``model_config`` forbids unknown fields so a
    typo (e.g. ``write_disposiiton``) fails ``dhd sources validate`` instead of
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
    schema_contract: Literal["evolve", "freeze", "discard_value"] = "evolve"
    target_prefix: str | None = None
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
    # schema (see connectors/rest.py).
    rest_options: dict | None = None

    # True if this source talks to a real external network/service -- such
    # tests are marked `network` and excluded by default (task 4).
    network: bool = False

    @property
    def prefix(self) -> str:
        return self.target_prefix or self.name

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
    def _check_family_fields(self) -> SourceConfig:
        if self.write_disposition == "merge" and not self.primary_key:
            raise ValueError(
                f"'{self.name}': write_disposition 'merge' needs 'primary_key'."
            )
        if self.family == "sql":
            if not self.dialect:
                raise ValueError(f"'{self.name}': family 'sql' needs 'dialect'.")
            if self.dialect not in SQL_DIALECTS:
                raise ValueError(
                    f"'{self.name}': unknown dialect '{self.dialect}'. Choose "
                    f"from: {', '.join(SQL_DIALECTS)}."
                )
            if not self.secret_ref:
                raise ValueError(f"'{self.name}': family 'sql' needs 'secret_ref'.")
        elif self.family == "files":
            if not self.path and not self.http_url:
                raise ValueError(
                    f"'{self.name}': family 'files' needs 'path' or 'http_url'."
                )
            if not self.format:
                raise ValueError(f"'{self.name}': family 'files' needs 'format'.")
            if self.format not in FILE_FORMATS:
                raise ValueError(
                    f"'{self.name}': unknown format '{self.format}'. Choose "
                    f"from: {', '.join(FILE_FORMATS)}."
                )
            if not self.resources:
                # files family: exactly one resource, named after the source.
                self.resources = [self.name]
        elif self.family == "rest":
            if not self.base_url:
                raise ValueError(f"'{self.name}': family 'rest' needs 'base_url'.")
            if not self.resources:
                raise ValueError(f"'{self.name}': family 'rest' needs 'resources'.")
        return self


def _entries_with_lines(text: str) -> list[tuple[int, dict]]:
    """Parse ``sources:``'s list items, keeping each one's 1-based start line."""
    root = yaml.compose(text, Loader=yaml.SafeLoader)
    if root is None:
        return []
    sources_node = None
    for key_node, value_node in root.value:
        if key_node.value == "sources":
            sources_node = value_node
            break
    if sources_node is None or not sources_node.value:
        return []

    loader = yaml.SafeLoader(text)
    try:
        return [
            (
                item_node.start_mark.line + 1,
                loader.construct_mapping(item_node, deep=True),
            )
            for item_node in sources_node.value
        ]
    finally:
        loader.dispose()


def load_sources(path: Path = SOURCES_YML) -> list[SourceConfig]:
    """Parse and validate every entry in ``sources.yml``.

    Raises :class:`SourcesConfigError` with one readable ``line N: field
    'x': message`` line per problem -- all of them, not just the first, so
    ``dhd sources validate`` can report everything wrong in one pass.
    """
    try:
        text = path.read_text()
    except OSError as exc:
        raise SourcesConfigError(f"Could not read {path}: {exc}") from exc

    entries = _entries_with_lines(text)
    sources: list[SourceConfig] = []
    errors: list[str] = []
    seen_names: set[str] = set()

    for line, raw in entries:
        if not isinstance(raw, dict):
            errors.append(f"line {line}: entry is not a mapping: {raw!r}")
            continue
        try:
            config = SourceConfig.model_validate(raw)
        except ValidationError as exc:
            for err in exc.errors():
                field = ".".join(str(part) for part in err["loc"]) or "<entry>"
                errors.append(f"line {line}: field '{field}': {err['msg']}")
            continue
        if config.name in seen_names:
            errors.append(f"line {line}: duplicate source name '{config.name}'")
            continue
        seen_names.add(config.name)
        sources.append(config)

    if errors:
        raise SourcesConfigError(
            f"{path} has {len(errors)} error(s):\n"
            + "\n".join(f"  - {e}" for e in errors)
        )
    return sources


def get_source(name: str, sources: list[SourceConfig] | None = None) -> SourceConfig:
    for source in sources if sources is not None else load_sources():
        if source.name == name:
            return source
    raise SourcesConfigError(f"No source named '{name}' in sources.yml.")


def prefixed_table_name(source_name: str, resource_name: str) -> str:
    return f"{source_name}_{resource_name}"
