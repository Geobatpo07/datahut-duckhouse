"""``merge`` has no Flight-level equivalent today (``HybridBackend``
inherits a working ``upsert()`` *method* unmodified from xorq's
``PyIcebergBackend``, but no Flight action or ``UploadMode`` reaches it
over the wire), so this sink still writes it directly via pyiceberg from
the client -- gated by ``catalog_guard.check_same_catalog``.

Takes an already-connected pyiceberg engine (duck-typed: anything with
``list_tables``/``create_table``/``upload_table``/``apply_schema_contract``,
in practice ``datahut_duckhouse.ingest.PyIcebergEngine``) rather than
connecting itself -- this module never imports it, keeping ``ingestion``
decoupled from the ``dhd`` CLI package.
"""

from __future__ import annotations

import pyarrow as pa

from ingestion.sinks.base import SchemaContractViolationError, SinkError
from ingestion.sinks.catalog_guard import check_same_catalog


class DirectSink:
    """Writes ``merge`` straight to Iceberg via pyiceberg -- gated by
    ``catalog_guard.check_same_catalog``."""

    write_dispositions = ("merge",)

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
        if write_disposition != "merge":
            raise SinkError(
                f"DirectSink only writes 'merge' (got {write_disposition!r})."
            )
        exists = table_name in self.list_tables()
        if not exists:
            self.engine.create_table(table_name, data)
            return "created"
        self.engine.upload_table(
            table_name, data, mode="merge", primary_key=primary_key
        )
        return "merged"
