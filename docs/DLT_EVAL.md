# dlt evaluation — Vague 1b (registre de sources, 3 familles)

Résultats mesurés, pas estimés, des 5 expériences demandées. Machine :
Windows 11 Pro, Python 3.13.18, `dlt` 1.32.0, `pyiceberg` 0.11.1, `pyarrow`
20.0.0. Postgres/MySQL : conteneurs Docker déjà en service
(`docker-compose.test.yml`). Serveur Flight (expérience 5 uniquement) :
conteneur Linux, Python 3.13.18, déjà en service (`docker-compose.yml`).

Les commandes exactes sont données pour chaque expérience ; tout ce qui
n'a pas été exécuté est marqué **non vérifié**.

## 1. Incrémental sur Postgres Chinook

Préparation (schéma réel modifié, car Chinook n'a pas de colonne de
suivi) :

```sql
ALTER TABLE customer ADD COLUMN updated_at timestamptz DEFAULT now();
```

Source dédiée `pg_chinook_customers_incremental` (`ingestion/dlt/sources.yml`) :
`write_disposition: merge`, `primary_key: [customer_id]`, `incremental:
{column: updated_at, type: timestamp}`.

```bash
uv run dhd --engine pyiceberg ingest-dlt --source pg_chinook_customers_incremental
```

- **Run 1** (chargement initial) : 59 lignes (= le compte Chinook connu).
- Puis, directement en SQL sur le conteneur Postgres : `UPDATE customer
  SET first_name = 'LUIS-UPDATED', updated_at = now() WHERE customer_id =
  1;` et `INSERT INTO customer (customer_id, ..., updated_at) VALUES (60,
  ..., now());`.
- **Run 2** : `60 lignes` au total, **sans doublon** ; `customer_id=1`
  affiche bien `LUIS-UPDATED` dans Iceberg.
- Puis `DELETE FROM customer WHERE customer_id = 60;` sur Postgres.
- **Run 3** : toujours `60 lignes`, et `customer_id=60` **toujours
  présent** dans Iceberg. **Preuve directe que les suppressions ne sont
  pas détectées** par ce mécanisme (incrémental + merge) : rien dans le
  curseur `updated_at` ne signale qu'une ligne a disparu côté source.

**Limite trouvée en cours de route** (non anticipée) : le filtre
incrémental est en borne inférieure *inclusive* (`>=`), pas strict. Comme
toutes les 59 lignes initiales partagent exactement le même `updated_at`
(backfill en une seule transaction), le run 2 a en réalité ré-extrait
**60 lignes** (58 inchangées + les 2 modifiées/nouvelles), pas 2 comme on
pourrait s'y attendre. Sans conséquence ici parce que `write_disposition:
merge` est idempotent (ré-appliquer une ligne identique ne change rien) --
mais ce serait une vraie source de doublons avec `write_disposition:
append`. Documenté dans [docs/INGESTION.md](INGESTION.md).

Un bug réel a été trouvé et corrigé en cours de route : le curseur
incrémental (une valeur `datetime`) était re-sérialisé en texte brut par
`json.dumps(..., default=str)`, ce que `dlt.sources.incremental` refusait
ensuite ("Could not coerce start_value ... object of type str") -- corrigé
dans `connectors/sql.py::_decode_cursor` (voir le commit
`fix(dlt): decode a JSON-stringified incremental cursor before reuse`),
avec un test de régression.

## 2. Évolution de schéma

```sql
ALTER TABLE customer ADD COLUMN loyalty_tier text DEFAULT 'bronze';
```

Rejoué contre les données réelles (élargies) de Postgres, une table de
test par mode (`apply_schema_contract` appelé directement, même code que
`dhd ingest-dlt`) :

| Mode | Résultat observé |
|---|---|
| `evolve` | Réussit. Le schéma Iceberg gagne `loyalty_tier` ; les anciennes lignes ont `NULL` pour cette colonne. |
| `freeze` | Rejeté proprement : `'schema_freeze_test': incoming data has column(s) not in the table's schema (loyalty_tier) and this source's schema_contract is 'freeze'.` |
| `discard_value` | Réussit silencieusement. `loyalty_tier` est supprimée avant l'écriture ; le schéma de la table reste inchangé (14 colonnes, pas 15). |

Comportement sous-jacent de pyiceberg 0.11.1, vérifié indépendamment :
ajouter/écraser avec une colonne inconnue lève `ValueError` en suggérant
`union_by_name` ; une colonne *manquante* (l'inverse) ne lève jamais rien,
pyiceberg la remplit avec `NULL` automatiquement.

## 3. Reprise après échec

```bash
# lancé en arrière-plan (Python subprocess, pas un simple `&` bash --
# voir la note plus bas), tué après N secondes avec un vrai SIGKILL/
# TerminateProcess (pas un arrêt propre)
uv run dhd --engine pyiceberg ingest-dlt --source sqlite_chinook
```

11 tables au total pour `sqlite_chinook`. Un run tué à ~2.5s a laissé
exactement 4 tables commitées (`Album`, `Artist`, `Customer`, `Employee`)
et 7 absentes -- pas de table à moitié écrite : chaque table est un commit
Iceberg atomique (confirmé indépendamment par
`tests/test_ingest_dlt.py::TestOneCommitPerTablePerRun`, qui compte les
snapshots). Relancer la **même** commande (write_disposition `replace`
par défaut pour cette source) a produit l'état final attendu -- les 11
tables, chacune avec le compte de lignes exact connu (Album 347, Artist
275, Customer 59, Employee 8, Genre 25, Invoice 412, InvoiceLine 2240,
MediaType 5, Playlist 18, PlaylistTrack 8715, Track 3503) -- **sans aucun
doublon**.

**Important : cette absence de doublon est une propriété de
`write_disposition: replace` (et de `merge`), pas une garantie générale.**
Pour `write_disposition: append`, relancer après une table déjà commitée
la duplique forcément (`append` n'est jamais idempotent) --
vérifié par `tests/test_ingest_dlt.py::test_append_accumulates_on_rerun`,
qui montre deux runs successifs en `append` accumuler les deux jeux de
lignes. Une vraie reprise sûre sous `append` demande de la combiner avec
`incremental` **et** de préférer `merge` dès qu'une clé primaire existe.

Note technique : un simple `cmd &` suivi de `kill` dans Git Bash sur
Windows cible le sous-shell, pas forcément le processus Python réel
(reparenté) -- `subprocess.Popen(...).kill()` en Python cible le bon PID
directement et a donné des résultats reproductibles.

## 4. Volume

Table synthétique (`id: int64`, `name: string`, `amount: float64`, `flag:
bool`), écrite via le connecteur `files` (Parquet local) puis chargée en
un seul commit (`write_disposition: replace`), mémoire pic mesurée par
échantillonnage `psutil` (RSS, toutes les 50ms) pendant toute la durée.

| Lignes | Extraction | Chargement (1 commit) | Total | RSS pic | Commits |
|---|---|---|---|---|---|
| 1 000 000 | 0.831s | 1.515s | 2.345s | 339.1 MiB | 1 |
| 10 000 000 | 3.114s | 4.671s | 7.785s | 899.3 MiB | 1 |

Les deux ont été exécutés (le second, optionnel selon le brief, l'a été
car le temps le permettait largement). Comportement sous-linéaire mais
proche du linéaire entre 1M et 10M (x10 lignes -> x3.3 temps, x2.65
mémoire) -- cohérent avec un coût dominé par la lecture/écriture Parquet
et la construction du buffer Arrow, pas par du travail quadratique.

## 5. (Optionnel) Écriture directe vs via Flight

Fait, dans le temps imparti (comparaison unique, pas répétée -- à prendre
comme indicatif, pas définitif). 100 000 lignes synthétiques (CSV), client
et serveur Flight sur la même version Python (3.13.18, vérifié
`docker exec flight-server python -c "import sys; print(sys.version)"`
avant de lancer).

| Opération | `pyiceberg` direct | via Flight (xorq/HybridBackend) |
|---|---|---|
| `create-table` (1 commit) | 4.054s | 5.454s |
| `insert --mode append` (1 commit) | 3.851s | 3.409s |

`create-table` est ~35% plus lent via Flight (aller-retour gRPC +
sérialisation Arrow Flight + traitement côté serveur xorq/HybridBackend
en plus de l'écriture Iceberg elle-même) ; `insert` a été, cette fois-ci,
légèrement *plus rapide* via Flight -- un seul échantillon par scénario,
donc cette inversion est probablement du bruit (variance de démarrage
`uv run`, état du cache disque), pas un signal fiable. La contrainte de
writer unique de `HybridBackend` (déjà documentée, ADR-0001) n'a pas été
ré-explorée ici -- `HybridBackend` n'a pas été modifié, conformément à la
consigne.

**Ce test a laissé une table `synthetic_100k_flight` dans l'entrepôt
MinIO partagé du conteneur** (comme d'autres tables de vérification
laissées par les sessions précédentes : `matrix_test_311`, `segfault_probe`,
etc.) -- `dhd` n'a pas de commande `drop-table`, donc je ne l'ai pas
supprimée moi-même plutôt que de bricoler un accès catalogue hors `dhd`.

## Non vérifié

- SQL Server (`mssql_chinook`) : toujours scaffolded, jamais lancé (voir
  `tests/docker/chinook/README.md`, inchangé depuis Vague 1).
- Les 2 sources REST (`jsonplaceholder`, `open_brewery_db`) sont vérifiées
  en extraction seule (`-m network`, voir tests/test_connectors_rest.py) ;
  elles n'ont pas été chargées dans Iceberg via `dhd ingest-dlt` dans le
  cadre de cette évaluation (seulement validées à la main une fois, voir
  les commits de la tâche 2).
- L'expérience 5 n'a qu'un échantillon par scénario -- pas de confiance
  statistique, voir ci-dessus.
- Volume au-delà de 10M lignes.
- Comportement de `schema_contract` combiné à `write_disposition: merge`
  (testé seulement avec `replace`/`append` en pratique ici ; couvert en
  unitaire pour `merge` dans `tests/test_ingest_dlt.py`, mais pas rejoué
  ici contre une vraie source Postgres élargie).

## Décision recommandée

**dlt seul, avec la garde ajoutée ici (registre + contrats de schéma +
merge idempotent), convient pour ce volume et ce nombre de sources.** Pas
besoin d'un autre outil : le report inclusif de l'incrémental n'est
dangereux qu'avec `append` -- la règle simple "primary key connue =>
`merge`" l'élimine. La seule vraie lacune structurelle est l'absence de
détection des suppressions, intrinsèque à toute extraction par curseur
(dlt ou autre) sans CDC côté source -- hors de portée de ce brief
("ne pas implémenter les instances clients", et CDC serait une tâche à
part entière).
