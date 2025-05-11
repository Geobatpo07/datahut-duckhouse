import os
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from pyiceberg.schema import Schema
from pyiceberg.types import *
from pyiceberg.table import Table

from flight_server.app.utils import (
    get_iceberg_catalog,
    get_iceberg_namespace,
    get_iceberg_warehouse_path
)

catalog = get_iceberg_catalog()
namespace = get_iceberg_namespace()
warehouse = get_iceberg_warehouse_path()

# Mapping simplifié PyArrow → Iceberg types
PYARROW_TO_ICEBERG = {
    pa.string(): StringType(),
    pa.int64(): LongType(),
    pa.int32(): IntegerType(),
    pa.float64(): DoubleType(),
    pa.float32(): FloatType(),
    pa.bool_(): BooleanType(),
    pa.timestamp('ns'): TimestampType(),
}

def infer_iceberg_schema_from_arrow(arrow_schema: pa.Schema) -> Schema:
    iceberg_fields = []
    for i, field in enumerate(arrow_schema):
        pa_type = field.type
        iceberg_type = PYARROW_TO_ICEBERG.get(pa_type, StringType())  # défaut : string
        iceberg_fields.append((field.name, iceberg_type))

    return Schema(*iceberg_fields)

def write_arrow_to_iceberg(table_name: str, arrow_table: pa.Table):
    identifier = (namespace, table_name)

    if not catalog.table_exists(identifier):
        print(f"Création d'une nouvelle table Iceberg : {table_name}")

        iceberg_schema = infer_iceberg_schema_from_arrow(arrow_table.schema)

        catalog.create_table(
            identifier=identifier,
            schema=iceberg_schema,
            location=f"{warehouse}{table_name}/"
        )
    else:
        print(f"Table Iceberg '{table_name}' déjà existante.")

    # Préparer le fichier .parquet temporaire
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_name = f"iceberg_temp_{table_name}_{timestamp}.parquet"
    pq.write_table(arrow_table, file_name)

    # Charger la table et append les données
    table: Table = catalog.load_table(identifier)

    # Append en transaction (simple)
    table.append_files([file_name])
    print(f"{arrow_table.num_rows} lignes ajoutées à Iceberg : {table_name}")

    os.remove(file_name)
