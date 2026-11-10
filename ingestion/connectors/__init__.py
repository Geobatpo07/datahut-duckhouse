"""The ``sql``/``files``/``rest`` connector families, behind a single
contract (``base.Connector.build_resources``) and a single extension
point (``registry.register``). Never imports a sink or the run log --
only ``ingestion.config`` and, for ``sql``, ``ingestion.engine.state``
(the incremental cursor store)."""
