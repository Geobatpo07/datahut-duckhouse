"""The three ingestion-registry connector families (Vague 1b, task 2): ``sql``,
``files``, ``rest``. Each module exposes a single ``extract_source(source,
work_dir) -> dict[str, Path]`` (prefixed resource name -> written Parquet
file) -- ``ingestion/dlt/pipeline.py`` dispatches to the right one by
``source.family``, so adding a source needs no code change in any of them.
"""

from __future__ import annotations
