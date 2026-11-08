"""``dhd ingest-dlt``: load dlt-extracted sources into Iceberg (Vague 1 step 4;
exploitation layer -- run log, write_disposition dispatch, schema contracts
-- added in Vague 1b task 3).

Bridges ``ingestion/dlt/pipeline.py`` (extraction) to the serverless
``pyiceberg`` engine (``datahut_duckhouse/ingest.py``) -- never
``HybridBackend``/``xorq``, and always the pyiceberg engine regardless of
``--engine``/``DHD_ENGINE`` (see ``cli.py``'s ``ingest-dlt`` command for why).
Each table's write is one Iceberg commit (create/append/overwrite/upsert),
whatever the source's ``write_disposition`` -- see
``tests/test_ingest_dlt.py``'s snapshot-count test.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pyarrow.parquet as papq

_DLT_DIR = Path(__file__).resolve().parent.parent / "ingestion" / "dlt"


class IngestDltError(RuntimeError):
    """A clean, CLI-facing error for ``dhd ingest-dlt`` failures."""


def _load_pipeline():
    """Import ``ingestion/dlt/pipeline.py`` by path.

    Not ``import ingestion.dlt.pipeline``: see that module's own docstring --
    ``ingestion/__init__.py`` is unrelated pre-existing code that imports
    submodules which do not exist on disk and raises ``ModuleNotFoundError``
    on import. Going straight at ``ingestion/dlt/`` avoids triggering it.
    """
    dlt_dir = str(_DLT_DIR)
    if dlt_dir not in sys.path:
        sys.path.insert(0, dlt_dir)
    import pipeline  # type: ignore[import-not-found]

    return pipeline


def _resolve_sources(pipeline, source_name: str):
    """Shared by :func:`run_ingest_dlt` and :func:`plan_ingest_dlt`: parse
    ``sources.yml`` and narrow it to ``source_name`` ('all' for every
    source)."""
    try:
        sources = pipeline.load_sources()
    except pipeline.SourcesConfigError as exc:
        raise IngestDltError(str(exc)) from exc

    if source_name != "all":
        sources = [s for s in sources if s.name == source_name]
        if not sources:
            raise IngestDltError(
                f"No source named '{source_name}' in ingestion/dlt/sources.yml."
            )
    return sources


def plan_ingest_dlt(source_name: str) -> list:
    """Validate ``sources.yml`` and resolve ``source_name`` without extracting,
    connecting to Iceberg, or writing anything -- the ``--dry-run`` path."""
    pipeline = _load_pipeline()
    return _resolve_sources(pipeline, source_name)


_VERBS = {"replace": "overwritten", "append": "appended", "merge": "merged"}


def run_ingest_dlt(source_name: str) -> dict[str, tuple[str, int]]:
    """Extract ``source_name`` ('all' for every source) and load it into
    Iceberg via the pyiceberg engine, applying each source's
    ``write_disposition`` and ``schema_contract`` (task 3).

    Returns ``{table_name: (verb, row_count)}`` where ``verb`` is "created"
    (table did not exist yet) or "overwritten"/"appended"/"merged" (per
    ``write_disposition``, it did).
    """
    from datahut_duckhouse.ingest import (
        IngestUnavailableError,
        SchemaContractViolationError,
    )
    from datahut_duckhouse.ingest import connect as ingest_connect
    from datahut_duckhouse.ingest_run_log import RunLog

    pipeline = _load_pipeline()
    sources = _resolve_sources(pipeline, source_name)

    try:
        backend = ingest_connect()
    except IngestUnavailableError as exc:
        raise IngestDltError(str(exc)) from exc

    run_log = RunLog(backend)
    existing = set(backend.list_tables())
    results: dict[str, tuple[str, int]] = {}
    for source in sources:
        t0 = time.time()
        try:
            written = pipeline.extract_source(source)
        except (pipeline.MissingDriverError, pipeline.SourcesConfigError) as exc:
            run_log.record(source.name, "*", t0, time.time(), 0, "error", str(exc))
            raise IngestDltError(str(exc)) from exc
        except pipeline.ConnectorError as exc:
            run_log.record(source.name, "*", t0, time.time(), 0, "error", str(exc))
            raise IngestDltError(str(exc)) from exc

        for table_name, parquet_path in written.items():
            table_t0 = time.time()
            try:
                data = papq.read_table(parquet_path)
                if table_name in existing:
                    if source.schema_contract:
                        data = backend.apply_schema_contract(
                            table_name, data, source.schema_contract
                        )
                    if source.write_disposition == "merge":
                        backend.upload_table(
                            table_name,
                            data,
                            mode="merge",
                            primary_key=source.primary_key,
                        )
                    elif source.write_disposition == "append":
                        backend.upload_table(table_name, data, mode="append")
                    else:  # "replace"
                        backend.upload_table(table_name, data, mode="overwrite")
                    verb = _VERBS[source.write_disposition]
                else:
                    backend.create_table(table_name, data)
                    existing.add(table_name)
                    verb = "created"
            except SchemaContractViolationError as exc:
                run_log.record(
                    source.name, table_name, table_t0, time.time(), 0, "error", str(exc)
                )
                raise IngestDltError(str(exc)) from exc
            results[table_name] = (verb, data.num_rows)
            run_log.record(
                source.name, table_name, table_t0, time.time(), data.num_rows, "ok"
            )
    return results
