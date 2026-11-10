"""Load and validate ``sources.yml`` (Vague 1b, task 1; moved here from
``ingestion/dlt/registry.py`` -- see ``docs/INGESTION_ARCHITECTURE.md``).

Line numbers in error messages come from re-walking the YAML's parse tree
(:func:`_entries_with_lines`) -- ``yaml.safe_load`` alone discards them.
Resolving a secret is the model's own job (``SourceConfig.connection_url``
reads an env var *name*) -- this module never touches a secret *value*.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import yaml
from pydantic import ValidationError

from ingestion.config.models import SourceConfig, SourcesConfigError

SOURCES_YML = Path(__file__).parent / "sources.yml"

__all__ = [
    "SOURCES_YML",
    "SourceConfig",
    "SourcesConfigError",
    "load_sources",
    "get_source",
    "prefixed_table_name",
]


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


def load_sources(
    path: Path = SOURCES_YML, collect_warnings: list[str] | None = None
) -> list[SourceConfig]:
    """Parse and validate every entry in ``sources.yml``.

    Raises :class:`SourcesConfigError` with one readable ``line N: field
    'x': message`` line per problem -- all of them, not just the first, so
    ``dhd ingest validate`` can report everything wrong in one pass.

    Non-fatal problems (e.g. incremental + write_disposition 'replace', see
    ``ingestion.config.rules``) are raised as :class:`UserWarning` instead
    of an error. Pass ``collect_warnings`` (an empty list) to capture them
    as ``"line N: message"`` strings instead of letting them print to
    stderr via Python's normal warning machinery -- this is what ``dhd
    ingest validate`` does, so they show up next to the entry they came
    from.
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
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            try:
                config = SourceConfig.model_validate(raw)
            except ValidationError as exc:
                for err in exc.errors():
                    field = ".".join(str(part) for part in err["loc"]) or "<entry>"
                    errors.append(f"line {line}: field '{field}': {err['msg']}")
                continue
        for caught_warning in caught:
            message = f"line {line}: {caught_warning.message}"
            if collect_warnings is not None:
                collect_warnings.append(message)
            else:
                warnings.warn(message, caught_warning.category, stacklevel=2)
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
