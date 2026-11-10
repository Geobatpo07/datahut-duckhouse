"""Writes straight to Iceberg via pyiceberg, bypassing the Flight server
-- gated by ``catalog_guard.check_same_catalog``, demoted (ADR-0006,
decision 5) from "the reason a direct write is allowed" to a safety net
run underneath the real gate, ``router.check_direct_capacity`` (called by
whoever picks this sink, before it's even constructed). The check stays
cheap and worth keeping: capacity proves this process *can* reach the
catalog and storage directly; it does not prove this process resolves to
the *same* catalog identity as the rest of the deployment (a stale local
override -- e.g. a custom ``ICEBERG_WAREHOUSE_PATH`` -- would pass
capacity and still land data somewhere ``dhd query`` never looks).

Historically ``merge`` only: no Flight action/``UploadMode`` reaches
``HybridBackend``'s inherited ``upsert()`` over the wire. Now also
``replace``/``append`` (ADR-0006, decision 5: direct is the default
whenever it's possible, for every write_disposition, not just merge).

Takes an already-connected pyiceberg engine (duck-typed: anything with
``list_tables``/``create_table``/``upload_table``/``apply_schema_contract``,
in practice ``datahut_duckhouse.ingest.PyIcebergEngine``) rather than
connecting itself -- this module never imports it, keeping ``ingestion``
decoupled from the ``dhd`` CLI package.
"""

from __future__ import annotations

import pyarrow as pa

from ingestion.sinks.base import (
    SchemaContractViolationError,
    SinkError,
    with_commit_retry,
)
from ingestion.sinks.catalog_guard import check_same_catalog

_MODE_BY_DISPOSITION = {"replace": "overwrite", "append": "append", "merge": "merge"}
_VERB_BY_DISPOSITION = {
    "replace": "overwritten",
    "append": "appended",
    "merge": "merged",
}


class DirectSink:
    """Writes any ``write_disposition`` straight to Iceberg via pyiceberg
    -- gated by ``catalog_guard.check_same_catalog`` (see module
    docstring for why that check is kept, not removed)."""

    write_dispositions = ("replace", "append", "merge")

    def __init__(self, pyiceberg_engine, flight_backend_for_guard) -> None:
        check_same_catalog(flight_backend_for_guard, pyiceberg_engine)
        self.engine = pyiceberg_engine

    def list_tables(self) -> list[str]:
        return self.engine.list_tables()

    def apply_schema_contract(
        self, table_name: str, data: pa.Table, contract: str
    ) -> pa.Table:
        try:
            return self.engine.apply_schema_contract(table_name, data, contract)
        except RuntimeError as exc:
            raise SchemaContractViolationError(str(exc)) from exc

    def write(
        self,
        table_name: str,
        data: pa.Table,
        write_disposition: str,
        primary_key: list[str] | None = None,
    ) -> str:
        if write_disposition not in _MODE_BY_DISPOSITION:
            raise SinkError(
                f"DirectSink cannot write write_disposition={write_disposition!r} "
                f"(only {', '.join(_MODE_BY_DISPOSITION)})."
            )
        exists = table_name in self.list_tables()
        if not exists:
            with_commit_retry(
                lambda: self.engine.create_table(table_name, data),
                table_name=table_name,
            )
            return "created"
        mode = _MODE_BY_DISPOSITION[write_disposition]
        with_commit_retry(
            lambda: self.engine.upload_table(
                table_name, data, mode=mode, primary_key=primary_key
            ),
            table_name=table_name,
        )
        return _VERB_BY_DISPOSITION[write_disposition]
