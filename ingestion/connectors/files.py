"""The ``files`` connector family (Vague 1b, task 2): local CSV/JSON/Parquet,
optionally fetched over HTTP first (``source.http_url``). Always exactly
one resource per source (the file itself), named ``source.resources[0]``
(defaulted to ``source.name`` by ``ingestion.config.rules`` if omitted).
Never imports ``dlt`` -- plain pyarrow readers.
"""

from __future__ import annotations

import json
import urllib.request
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.json as pajson
import pyarrow.parquet as papq

from ingestion.config.models import SourceConfig, SourcesConfigError
from ingestion.config.registry import prefixed_table_name

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CACHE_DIR = Path(__file__).parent.parent / "_cache"


class FileNotFoundInRegistryError(RuntimeError):
    """A files-family source's ``path`` does not exist on disk."""


def _flatten(record: dict[str, Any], parent_key: str = "") -> dict[str, Any]:
    """Collapse nested dicts to ``parent_child`` keys (lists are left alone)."""
    out: dict[str, Any] = {}
    for key, value in record.items():
        flat_key = f"{parent_key}_{key}" if parent_key else key
        if isinstance(value, dict):
            out.update(_flatten(value, flat_key))
        else:
            out[flat_key] = value
    return out


def _resolve_path(source: SourceConfig) -> Path:
    if source.http_url:
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cached = _CACHE_DIR / f"{source.name}.{source.format}"
        request = urllib.request.Request(
            source.http_url, headers={"User-Agent": "datahut-duckhouse/0.1"}
        )
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            cached.write_bytes(response.read())
        return cached

    path = Path(source.path or "")
    if not path.is_absolute():
        path = REPO_ROOT / path
    if not path.exists():
        raise FileNotFoundInRegistryError(
            f"Source '{source.name}': path '{source.path}' does not exist "
            f"(resolved to {path})."
        )
    return path


def _read(path: Path, fmt: str) -> pa.Table:
    if fmt == "csv":
        return pacsv.read_csv(path)
    if fmt == "parquet":
        return papq.read_table(path)
    if fmt == "json":
        try:
            # Try pyarrow's own line-delimited JSON reader first -- fast path.
            return pajson.read_json(path)
        except pa.ArrowInvalid:
            # Not line-delimited -- a plain JSON array of records (our fixture).
            records = json.loads(path.read_text())
            if not isinstance(records, list):
                records = [records]
            return pa.Table.from_pylist([_flatten(r) for r in records])
    raise SourcesConfigError(f"Unsupported files format: '{fmt}'.")


def build_resources(source: SourceConfig, work_dir: Path) -> dict[str, Path]:
    """Read ``source``'s single resource and write it to Parquet under ``work_dir``."""
    path = _resolve_path(source)
    arrow_table = _read(path, source.format or "")

    resource_name = source.resources[0] if source.resources else source.name
    out_name = prefixed_table_name(source.prefix, resource_name)
    work_dir.mkdir(parents=True, exist_ok=True)
    out_path = work_dir / f"{out_name}.parquet"
    papq.write_table(arrow_table, out_path)
    return {out_name: out_path}
