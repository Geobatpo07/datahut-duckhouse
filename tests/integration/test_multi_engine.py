"""Integration tests: real SQLite/Postgres/MySQL Chinook sources end to end.

Not run by default -- excluded via `-m "not integration"` in pyproject.toml's
addopts (see the `integration` marker). Needs:

    docker compose -f docker-compose.test.yml up -d
    uv run python scripts/fetch_chinook.py
    uv run pytest -m integration

SQL Server is scaffolded (tests/docker/chinook/mssql/, the `mssql` compose
profile) but **explicitly not run or verified in this pass** -- see
tests/docker/chinook/README.md. There is no SQL Server coverage below.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow.parquet as papq
import pytest
import sqlalchemy as sa
from click.testing import CliRunner

pytestmark = pytest.mark.integration

# Per-dialect casing: sqlite/mysql load scripts use PascalCase identifiers,
# postgres uses snake_case (see tests/docker/chinook/*/Chinook_*.sql).
_COLUMNS = {
    "sqlite_chinook": {
        "customer_table": "Customer",
        "invoice_table": "Invoice",
        "customer_id": "CustomerId",
        "country": "Country",
        "total": "Total",
        "invoice_date": "InvoiceDate",
        "first_name": "FirstName",
        "last_name": "LastName",
    },
    "pg_chinook": {
        "customer_table": "customer",
        "invoice_table": "invoice",
        "customer_id": "customer_id",
        "country": "country",
        "total": "total",
        "invoice_date": "invoice_date",
        "first_name": "first_name",
        "last_name": "last_name",
    },
    "mysql_chinook": {
        "customer_table": "Customer",
        "invoice_table": "Invoice",
        "customer_id": "CustomerId",
        "country": "Country",
        "total": "Total",
        "invoice_date": "InvoiceDate",
        "first_name": "FirstName",
        "last_name": "LastName",
    },
}


def _reflected_table_names(source) -> tuple[str, ...]:
    if source.resources:
        return tuple(source.resources)
    engine = sa.create_engine(source.connection_url)
    inspector = sa.inspect(engine)
    return tuple(inspector.get_table_names(schema=source.schema_name))


def _qualified(source, table_name: str) -> str:
    if source.dialect == "postgresql" and source.schema_name:
        return f"{source.schema_name}.{table_name}"
    return table_name


class TestRowCountsMatchSource:
    def test_every_table_row_count_matches(self, pipeline, source, tmp_path):
        written = pipeline.extract_source(source, work_dir=tmp_path)
        table_names = _reflected_table_names(source)
        assert table_names, f"no tables found for source {source.name!r}"

        engine = sa.create_engine(source.connection_url)
        with engine.connect() as conn:
            for table_name in table_names:
                prefixed = pipeline.prefixed_table_name(source.name, table_name)
                assert prefixed in written, f"{prefixed} missing from extraction"
                parquet_rows = papq.read_table(written[prefixed]).num_rows
                db_rows = conn.execute(
                    sa.text(f"SELECT count(*) FROM {_qualified(source, table_name)}")
                ).scalar()
                assert parquet_rows == db_rows, (
                    f"{prefixed}: {parquet_rows} rows in parquet, "
                    f"{db_rows} rows in {source.name}"
                )


class TestTypesPreserved:
    """customer_id 1 ("Luís Gonçalves", Brazil) and invoice_id 1 are the same
    seed rows in every Chinook load script -- a fixed, known-accented,
    known-decimal cross-engine fixture."""

    def test_accented_text_and_numeric_types(self, pipeline, source, tmp_path):
        cols = _COLUMNS[source.name]
        written = pipeline.extract_source(
            source,
            work_dir=tmp_path,
        )

        customer_rows = papq.read_table(
            written[pipeline.prefixed_table_name(source.name, cols["customer_table"])]
        ).to_pylist()
        customer = next(r for r in customer_rows if r[cols["customer_id"]] == 1)
        assert customer[cols["first_name"]].encode("utf-8") == b"Lu\xc3\xads"
        assert customer[cols["last_name"]].encode("utf-8") == b"Gon\xc3\xa7alves"

        invoice_rows = papq.read_table(
            written[pipeline.prefixed_table_name(source.name, cols["invoice_table"])]
        ).to_pylist()
        invoice = invoice_rows[0]
        total = invoice[cols["total"]]
        # decimal128(10, 2) in pyarrow -> python Decimal, not float: exact,
        # not approximate, precision.
        from decimal import Decimal

        assert isinstance(total, Decimal)
        assert total == Decimal("1.98")

        import datetime

        assert isinstance(
            invoice[cols["invoice_date"]], (datetime.date, datetime.datetime)
        )


class TestSalesByCountryConsistentAcrossSources:
    def test_join_result_matches_across_engines(self, pipeline, tmp_path):
        results: dict[str, dict[str, object]] = {}
        for source_name in ("sqlite_chinook", "pg_chinook", "mysql_chinook"):
            source = pipeline.get_source(source_name)
            cols = _COLUMNS[source_name]
            work_dir = tmp_path / source_name
            written = pipeline.extract_source(
                pipeline.SourceConfig(
                    name=source.name,
                    family="sql",
                    connector="sql_database",
                    secret_ref=source.secret_ref,
                    dialect=source.dialect,
                    schema=source.schema_name,
                    resources=[cols["customer_table"], cols["invoice_table"]],
                ),
                work_dir=work_dir,
            )
            invoice_pq = written[
                pipeline.prefixed_table_name(source_name, cols["invoice_table"])
            ]
            customer_pq = written[
                pipeline.prefixed_table_name(source_name, cols["customer_table"])
            ]
            con = duckdb.connect()
            query = f"""
                SELECT c."{cols['country']}" AS country,
                       SUM(i."{cols['total']}") AS total
                FROM read_parquet(?) AS i
                JOIN read_parquet(?) AS c
                  ON i."{cols['customer_id']}" = c."{cols['customer_id']}"
                GROUP BY c."{cols['country']}"
                ORDER BY country
            """
            rows = con.execute(query, [str(invoice_pq), str(customer_pq)]).fetchall()
            results[source_name] = dict(rows)

        sqlite_result = results["sqlite_chinook"]
        for other_name in ("pg_chinook", "mysql_chinook"):
            other = results[other_name]
            assert set(other) == set(sqlite_result), (
                f"{other_name} has a different set of countries than sqlite_chinook: "
                f"{set(other) ^ set(sqlite_result)}"
            )
            for country, total in sqlite_result.items():
                assert other[country] == total, (
                    f"{other_name}/{country}: {other[country]} != "
                    f"sqlite_chinook/{country}: {total}"
                )


class TestNoDuplicatesOnRerun:
    """`dhd ingest-dlt` loads overwrite, so re-running never duplicates rows
    (step 4's documented default) -- exercised here against a real source,
    through the actual CLI command, not just the pyiceberg engine directly."""

    def test_ingest_dlt_rerun_does_not_duplicate(self, source, monkeypatch, tmp_path):
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "")
        monkeypatch.setenv("ICEBERG_WAREHOUSE_PATH", str(tmp_path / "wh"))
        monkeypatch.setenv("ICEBERG_NAMESPACE", "default")
        monkeypatch.delenv("ICEBERG_CATALOG_URI", raising=False)
        monkeypatch.delenv("DHD_ENGINE", raising=False)
        monkeypatch.chdir(Path(__file__).parent.parent.parent)

        from datahut_duckhouse import cli as cli_mod

        runner = CliRunner()
        table_name = (
            f"{source.name}_Customer"
            if source.name != "pg_chinook"
            else "pg_chinook_customer"
        )

        r1 = runner.invoke(
            cli_mod.cli,
            ["--engine", "pyiceberg", "ingest-dlt", "--source", source.name],
        )
        assert r1.exit_code == 0, r1.output

        r2 = runner.invoke(
            cli_mod.cli,
            [
                "--engine",
                "pyiceberg",
                "query",
                f"SELECT count(*) AS n FROM {table_name}",
            ],
        )
        assert r2.exit_code == 0, r2.output
        first_count = r2.output

        r3 = runner.invoke(
            cli_mod.cli,
            ["--engine", "pyiceberg", "ingest-dlt", "--source", source.name],
        )
        assert r3.exit_code == 0, r3.output

        r4 = runner.invoke(
            cli_mod.cli,
            [
                "--engine",
                "pyiceberg",
                "query",
                f"SELECT count(*) AS n FROM {table_name}",
            ],
        )
        assert r4.exit_code == 0, r4.output
        assert r4.output == first_count, (
            "row count changed after a second `dhd ingest-dlt` run -- "
            f"first: {first_count!r}, second: {r4.output!r}"
        )
