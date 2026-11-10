"""The engine layer: the one :class:`~ingestion.engine.base.Ingestor`
implementation (``dlt_engine.DltIngestor``) and the incremental-cursor
state store (``state.py``). Deliberately empty at package level -- nothing
here imports ``dlt_engine`` eagerly, so ``connectors/sql.py``'s
``from ingestion.engine import state`` never pulls in a connectors ->
engine -> connectors import cycle."""
