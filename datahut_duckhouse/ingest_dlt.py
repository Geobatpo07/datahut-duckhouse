"""``dhd ingest-dlt``: load dlt-extracted sources into Iceberg (Vague 1, step 4).

Bridges ``ingestion/dlt/pipeline.py`` (extraction) to the serverless
``pyiceberg`` engine (``datahut_duckhouse/ingest.py``) -- never
``HybridBackend``/``xorq``, and always the pyiceberg engine regardless of
``--engine``/``DHD_ENGINE`` (see ``cli.py``'s ``ingest-dlt`` command for why).
Loads overwrite each table on every run, so re-running this command never
duplicates rows; not configurable here on purpose -- see README.
"""

from __future__ import annotations

import sys
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


def run_ingest_dlt(source_name: str) -> dict[str, tuple[str, int]]:
    """Extract ``source_name`` ('all' for every source) and load it into
    Iceberg via the pyiceberg engine.

    Returns ``{table_name: (verb, row_count)}`` where ``verb`` is "created"
    (table did not exist yet) or "overwritten" (it did).
    """
    pipeline = _load_pipeline()
    from datahut_duckhouse.ingest import IngestUnavailableError
    from datahut_duckhouse.ingest import connect as ingest_connect

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

    try:
        backend = ingest_connect()
    except IngestUnavailableError as exc:
        raise IngestDltError(str(exc)) from exc

    existing = set(backend.list_tables())
    results: dict[str, tuple[str, int]] = {}
    for source in sources:
        try:
            written = pipeline.extract_source(source)
        except (pipeline.MissingDriverError, pipeline.SourcesConfigError) as exc:
            raise IngestDltError(str(exc)) from exc
        for table_name, parquet_path in written.items():
            data = papq.read_table(parquet_path)
            if table_name in existing:
                backend.upload_table(table_name, data, mode="overwrite")
                verb = "overwritten"
            else:
                backend.create_table(table_name, data)
                existing.add(table_name)
                verb = "created"
            results[table_name] = (verb, data.num_rows)
    return results
