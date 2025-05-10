import duckdb
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Obtenir le chemin vers la base DuckDB
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "ingestion/data/duckhouse.duckdb")

def main():
    if not os.path.exists(DUCKDB_PATH):
        print(f"Fichier DuckDB introuvable : {DUCKDB_PATH}")
        return

    # Connexion
    print(f"Connexion à DuckDB : {DUCKDB_PATH}")
    conn = duckdb.connect(DUCKDB_PATH)

    # Liste des tables disponibles
    print("\nTables dans la base :")
    result = conn.execute("SHOW TABLES").fetchall()
    for table in result:
        print(f" - {table[0]}")

    # Affichage des premières lignes de la table "rev"
    print("\nAperçu de la table 'rev' :")
    try:
        df = conn.execute("SELECT * FROM rev LIMIT 5").fetchdf()
        print(df)
    except Exception as e:
        print(f"Erreur lors de la lecture de 'rev' : {e}")

    # Exemple de requête analytique
    print("\n Total de lignes dans 'rev' :")
    try:
        total = conn.execute("SELECT COUNT(*) FROM rev").fetchone()[0]
        print(f"Total de lignes : {total}")
    except Exception as e:
        print(f"Erreur dans la requête : {e}")

if __name__ == "__main__":
    main()
