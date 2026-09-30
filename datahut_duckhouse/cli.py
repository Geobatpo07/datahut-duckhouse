"""``dhd`` - the DataHut-DuckHouse command-line client (roadmap Phase 2).

A thin wrapper over the Arrow Flight server: ingest and query real data without
writing Python. No tenant concept yet - that is Phase 6, behind the roadmap's
bascule trigger.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import click
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.json as pajson
import pyarrow.parquet as papq

from datahut_duckhouse import __version__
from datahut_duckhouse.connection import FlightUnavailableError, connect, flight_target

ENGINES = ("flight", "pyiceberg")

_READERS = {
    ".csv": pacsv.read_csv,
    ".parquet": papq.read_table,
    ".pq": papq.read_table,
    ".json": pajson.read_json,
    ".ndjson": pajson.read_json,
}


def _load_source(source: str) -> pa.Table:
    path = Path(source)
    if not path.is_file():
        raise click.ClickException(f"Source file not found: {source}")
    reader = _READERS.get(path.suffix.lower())
    if reader is None:
        raise click.ClickException(
            f"Unsupported source type '{path.suffix}'. "
            f"Supported: {', '.join(sorted(_READERS))}"
        )
    try:
        return reader(str(path))
    except Exception as exc:  # noqa: BLE001 - surface a clean message
        raise click.ClickException(f"Could not read {source}: {exc}") from exc


def _json_default(value: object):
    from datetime import date, datetime
    from decimal import Decimal

    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def _render(table: pa.Table, fmt: str) -> None:
    if fmt == "json":
        click.echo(json.dumps(table.to_pylist(), default=_json_default, indent=2))
        return
    if fmt == "csv":
        import io

        buf = io.BytesIO()
        pacsv.write_csv(table, buf)
        click.echo(buf.getvalue().decode())
        return

    # default: aligned text table
    cols = table.column_names
    rows = [[_fmt_cell(v) for v in row.values()] for row in table.to_pylist()]
    widths = [
        max(len(cols[i]), *(len(r[i]) for r in rows)) if rows else len(cols[i])
        for i in range(len(cols))
    ]
    sep = "  "
    click.echo(sep.join(c.ljust(w) for c, w in zip(cols, widths, strict=True)))
    click.echo(sep.join("-" * w for w in widths))
    for r in rows:
        click.echo(sep.join(c.ljust(w) for c, w in zip(r, widths, strict=True)))
    click.echo(f"\n({table.num_rows} row{'s' if table.num_rows != 1 else ''})")


def _fmt_cell(value: object) -> str:
    if value is None:
        return "NULL"
    return str(value)


def _resolve_engine(ctx: click.Context) -> str:
    engine = (ctx.obj or {}).get("engine") or os.getenv("DHD_ENGINE") or "flight"
    if engine not in ENGINES:
        raise click.ClickException(
            f"Unknown engine '{engine}'. Choose from: {', '.join(ENGINES)}."
        )
    return engine


def _connect_or_die(ctx: click.Context):
    engine = _resolve_engine(ctx)
    try:
        if engine == "pyiceberg":
            from datahut_duckhouse.ingest import IngestUnavailableError
            from datahut_duckhouse.ingest import connect as ingest_connect

            try:
                return ingest_connect()
            except IngestUnavailableError as exc:
                raise click.ClickException(str(exc)) from exc
        return connect()
    except FlightUnavailableError as exc:
        raise click.ClickException(str(exc)) from exc


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__, prog_name="dhd")
@click.option(
    "--engine",
    type=click.Choice(ENGINES),
    default=None,
    help="Ingestion/query engine. 'flight' drives the Arrow Flight server (xorq); "
    "'pyiceberg' talks straight to Iceberg on MinIO, serverless and xorq-free. "
    "Env: DHD_ENGINE. Default: flight.",
)
@click.pass_context
def cli(ctx: click.Context, engine: str | None) -> None:
    """DataHut-DuckHouse CLI - drive the Flight server (Iceberg + DuckDB)."""
    ctx.obj = {"engine": engine}


@cli.command("list-tables")
@click.pass_context
def list_tables(ctx: click.Context) -> None:
    """List tables known to the server (Iceberg namespace, reflected in DuckDB)."""
    backend = _connect_or_die(ctx)
    names = sorted(backend.list_tables())
    if not names:
        if _resolve_engine(ctx) == "pyiceberg":
            click.echo(f"(no tables in namespace '{backend.namespace}')")
        else:
            host, port = flight_target()
            click.echo(f"(no tables on {host}:{port})")
        return
    for name in names:
        click.echo(name)


@cli.command("create-table")
@click.argument("name")
@click.argument("source", type=click.Path())
@click.pass_context
def create_table(ctx: click.Context, name: str, source: str) -> None:
    """Create table NAME in Iceberg from SOURCE (.csv/.parquet/.json)."""
    backend = _connect_or_die(ctx)
    if name in set(backend.list_tables()):
        raise click.ClickException(
            f"Table '{name}' already exists. Use `dhd insert {name} {source}` "
            f"to add rows."
        )
    data = _load_source(source)
    backend.create_table(name, data)
    click.echo(f"Created '{name}' - {data.num_rows} rows, {data.num_columns} columns.")


@cli.command()
@click.argument("table")
@click.argument("source", type=click.Path())
@click.option(
    "--mode",
    type=click.Choice(["append", "overwrite"]),
    default="append",
    show_default=True,
)
@click.pass_context
def insert(ctx: click.Context, table: str, source: str, mode: str) -> None:
    """Insert rows from SOURCE into an existing TABLE."""
    backend = _connect_or_die(ctx)
    if table not in set(backend.list_tables()):
        raise click.ClickException(
            f"Table '{table}' does not exist. Create it first with "
            f"`dhd create-table {table} {source}`."
        )
    data = _load_source(source)
    backend.con.upload_table(table, data, mode=mode)
    verb = "Appended" if mode == "append" else "Overwrote"
    click.echo(f"{verb} '{table}' with {data.num_rows} rows.")


@cli.command()
@click.argument("sql")
@click.option(
    "--limit", type=int, default=None, help="Cap the number of rows returned."
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["table", "csv", "json"]),
    default="table",
    show_default=True,
)
@click.pass_context
def query(ctx: click.Context, sql: str, limit: int | None, fmt: str) -> None:
    """Run a read-only SQL query against the reflected DuckDB views."""
    backend = _connect_or_die(ctx)
    try:
        expr = backend.sql(sql)
        if limit is not None:
            expr = expr.limit(limit)
        result = expr.to_pyarrow()
    except Exception as exc:  # noqa: BLE001 - CLI: clean message, not a traceback
        raise click.ClickException(f"Query failed: {exc}") from exc
    _render(result, fmt)


@cli.group()
def branch() -> None:
    """Nessie-style catalog branching - NOT active yet (ADR-0002, roadmap Phase 3)."""


_NESSIE_MSG = (
    "Branching is not wired yet. It needs the Nessie catalog (ADR-0002), which "
    "is roadmap Phase 3 - the server currently uses an embedded SQL catalog with "
    "no branch model. This command is a placeholder so the CLI surface is stable."
)


@branch.command("create")
@click.argument("name")
def branch_create(name: str) -> None:
    """(stub) Create a catalog branch."""
    raise click.ClickException(_NESSIE_MSG)


@branch.command("list")
def branch_list() -> None:
    """(stub) List catalog branches."""
    raise click.ClickException(_NESSIE_MSG)


@branch.command("switch")
@click.argument("name")
def branch_switch(name: str) -> None:
    """(stub) Switch the working catalog branch."""
    raise click.ClickException(_NESSIE_MSG)


def main() -> None:
    try:
        cli()
    except KeyboardInterrupt:  # pragma: no cover
        click.echo("Interrupted.", err=True)
        sys.exit(130)


if __name__ == "__main__":
    main()
