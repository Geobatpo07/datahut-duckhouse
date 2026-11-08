"""The ``rest`` connector family (Vague 1b, task 2): dlt's declarative
``rest_api_source`` (no custom per-API code -- ``source.base_url`` +
``source.resources`` + the free-form ``source.rest_options`` passed
straight through to dlt's own endpoint/paginator config), via
``_dlt_sources.py`` so this module itself never imports ``dlt`` directly.
Verified live against two key-free public APIs, see
``ingestion/config/sources.yml``'s ``jsonplaceholder`` (single page) and
``open_brewery_db`` (paginated) entries.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as papq

from ingestion.config.models import SourceConfig
from ingestion.config.registry import prefixed_table_name
from ingestion.connectors import _dlt_sources
from ingestion.connectors.files import _flatten


def _build_config(source: SourceConfig) -> dict[str, Any]:
    opts = source.rest_options or {}
    client: dict[str, Any] = {"base_url": source.base_url}
    if "headers" in opts:
        client["headers"] = opts["headers"]

    resources = []
    for name in source.resources:
        endpoint: dict[str, Any] = {"path": name}
        if "params" in opts:
            endpoint["params"] = opts["params"]
        if "paginator" in opts:
            endpoint["paginator"] = opts["paginator"]
        resources.append({"name": name, "endpoint": endpoint})
    return {"client": client, "resources": resources}


def build_resources(source: SourceConfig, work_dir: Path) -> dict[str, Path]:
    rest_source = _dlt_sources.rest_api_source(_build_config(source))

    work_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}
    for resource_name, resource in rest_source.resources.items():
        rows = list(resource)
        if not rows:
            continue
        arrow_table = pa.Table.from_pylist([_flatten(row) for row in rows])
        out_name = prefixed_table_name(source.prefix, resource_name)
        out_path = work_dir / f"{out_name}.parquet"
        papq.write_table(arrow_table, out_path)
        written[out_name] = out_path
    return written
