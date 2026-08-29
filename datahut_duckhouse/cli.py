"""
dhd — CLI pour DataHut-DuckHouse.

Exerce le chemin Flight complet (ADR-0001, Phase 1 de docs/ROADMAP.md) :
`create-table`/`insert` passent par `do_put` (upload_table côté client),
`query`/`list-tables` par `do_get`/`get_flight_info`, tous deux fournis
génériquement par `xorq.flight.FlightServerDelegate` et délégués côté serveur
à `HybridBackend` (Iceberg comme unique cible d'écriture, DuckDB en vues).

Cet outil ne connaît aucune notion de tenant — c'est un choix délibéré de la
Phase 2 de la roadmap : le multi-tenant (ADR-0004/0005) n'est construit que
lorsqu'un vrai signal de bascule apparaît (voir docs/ROADMAP.md).
"""
import sys

import click
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pa_parquet

from .connection import get_connection


def _read_source(path: str) -> pa.Table:
    """Charge un fichier source (.csv ou .parquet) en pyarrow.Table."""
    if path.endswith(".parquet"):
        return pa_parquet.read_table(path)
    if path.endswith(".csv"):
        return pa_csv.read_csv(path)
    raise click.ClickException(
        f"Format de fichier non supporté : '{path}' (attendu .csv ou .parquet)"
    )


@click.group()
@click.version_option()
def cli():
    """dhd — CLI pour DataHut-DuckHouse (Phase 2, voir docs/ROADMAP.md)."""


@cli.command("create-table")
@click.argument("name")
@click.argument("source", type=click.Path(exists=True, dir_okay=False))
def create_table_cmd(name: str, source: str):
    """Crée NAME à partir du fichier SOURCE (.csv ou .parquet).

    Écrit dans Iceberg (ADR-0001) ; si NAME existe déjà, le serveur route
    automatiquement vers un insert plutôt qu'une création (voir la note dans
    la commande `insert`).
    """
    data = _read_source(source)
    con = get_connection()
    con.create_table(name, data)
    click.echo(f"Table '{name}' créée ({data.num_rows} lignes).")


@cli.command("insert")
@click.argument("name")
@click.argument("source", type=click.Path(exists=True, dir_okay=False))
def insert_cmd(name: str, source: str):
    """Insère le contenu de SOURCE dans la table NAME existante.

    Note d'implémentation : côté client, `insert` et `create-table` envoient
    exactement le même appel (`upload_table`, un `do_put` Flight). C'est le
    serveur qui décide, à réception, s'il s'agit d'une création ou d'un ajout
    selon que la table existe déjà dans le catalogue (voir
    `FlightServerDelegate.do_put` dans le package `xorq`, et `.tables` sur
    `HybridBackend`). Cette commande existe séparément de `create-table`
    pour la clarté de l'usage en CLI, pas parce que le comportement diffère.
    """
    data = _read_source(source)
    con = get_connection()
    con.create_table(name, data)
    click.echo(f"{data.num_rows} lignes ajoutées à '{name}'.")


@cli.command("query")
@click.argument("sql")
@click.option(
    "--limit",
    default=20,
    show_default=True,
    help="Nombre de lignes à afficher (aucune limite si 0).",
)
def query_cmd(sql: str, limit: int):
    """Exécute SQL en lecture (vues DuckDB reflétant Iceberg, ADR-0001).

    À valider en conditions réelles : ce chemin (do_get sur une expression
    SQL sérialisée) est distinct de celui déjà vérifié pour l'écriture
    Iceberg dans cette session — pas encore testé de bout en bout ici.
    """
    con = get_connection()
    expr = con.sql(sql)
    df = con.to_pandas(expr, limit=(limit or None))
    click.echo(df.to_string(index=False))


@cli.command("list-tables")
def list_tables_cmd():
    """Liste les tables disponibles sur le serveur connecté."""
    con = get_connection()
    tables = con.list_tables()
    if not tables:
        click.echo("Aucune table.")
        return
    for t in tables:
        click.echo(t)


@cli.group("branch")
def branch_group():
    """Gestion des branches Nessie — pas encore actif (voir Phase 3 de la roadmap)."""


def _branch_not_ready(name: str = ""):
    click.echo(
        "Les branches ne sont pas encore actives : le catalogue Nessie "
        "(ADR-0002) n'est pas encore câblé au serveur Flight.\n"
        "Voir docs/ROADMAP.md, Phase 3.",
        err=True,
    )
    sys.exit(1)


@branch_group.command("create")
@click.argument("name")
def branch_create_cmd(name: str):
    """Crée une branche NAME (pas encore implémenté)."""
    _branch_not_ready(name)


@branch_group.command("list")
def branch_list_cmd():
    """Liste les branches (pas encore implémenté)."""
    _branch_not_ready()


@branch_group.command("switch")
@click.argument("name")
def branch_switch_cmd(name: str):
    """Bascule sur la branche NAME (pas encore implémenté)."""
    _branch_not_ready(name)


if __name__ == "__main__":
    cli()
