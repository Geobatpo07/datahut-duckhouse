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
bool`), écrite une fois en Parquet local, mémoire pic mesurée par
échantillonnage `psutil` (RSS, toutes les 50ms) pendant toute la durée.

**Précision sur ce qui est mesuré** (demandée en relecture) : pour une
source `files`, "extraction" (`connectors/files.py::extract_source`)
signifie lire le fichier Parquet local **entièrement en mémoire**
(`pyarrow.parquet.read_table`) puis le ré-écrire tel quel dans le
répertoire de travail -- ce n'est **pas** une extraction réseau/SGBD en
flux (celle-ci streamerait des lots depuis un curseur). Puis "chargement"
relit **à nouveau tout le fichier en mémoire** (`papq.read_table`) avant
un unique appel `iceberg_table.append()`. Le premier scénario ci-dessous
matérialise donc les 10M lignes en mémoire deux fois de suite avant de
toucher Iceberg -- ce que confirme son pic RSS.

| Lignes | Extraction | Chargement (1 commit) | Total | RSS pic | Commits |
|---|---|---|---|---|---|
| 1 000 000 | 0.831s | 1.515s | 2.345s | 339.1 MiB | 1 |
| 10 000 000 | 3.114s | 4.671s | 7.785s | 899.3 MiB | 1 |

Les deux ont été exécutés (le second, optionnel selon le brief, l'a été
car le temps le permettait largement). Comportement sous-linéaire mais
proche du linéaire entre 1M et 10M (x10 lignes -> x3.3 temps, x2.65
mémoire) -- cohérent avec un coût dominé par la lecture/écriture Parquet
et la construction du buffer Arrow, pas par du travail quadratique.

### Même volume, par tranches (chunks), 10M lignes

Pour mesurer l'effet du chargement par lots plutôt qu'en mémoire entière :
lecture du même fichier Parquet (10M lignes, `row_group_size=500000`) via
`pyarrow.parquet.ParquetFile.iter_batches(batch_size=500_000)` -- un vrai
flux, jamais plus d'un lot en mémoire à la fois -- et un `append` Iceberg
**par lot** (donc plusieurs commits, volontairement, pour cette seule
mesure -- ce n'est pas le comportement de `dhd ingest-dlt` en production,
qui garde un commit par table et par run, voir tâche 3).

| Lignes | Lots | Total | RSS pic | Commits |
|---|---|---|---|---|
| 10 000 000 | 20 (500 000/lot) | 11.446s | **362.2 MiB** | 20 |

**Comparaison directe (10M lignes) :** 899.3 MiB (1 commit, tout en
mémoire) contre 362.2 MiB (20 commits, par tranches) -- **-60% de pic
mémoire**, au prix de 20 commits au lieu d'1 et d'un temps total légèrement
plus long (11.4s contre 7.8s, overhead de 19 commits supplémentaires).
Compromis mémoire/commits réel, pas gratuit dans les deux sens -- à choisir
selon la contrainte qui compte le plus pour une source donnée (mémoire
disponible vs coût par commit Iceberg).

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
consigne. **Mise à jour en relecture** : qui écrit réellement dans Iceberg
pour `dhd ingest-dlt` (toujours le processus client, jamais ce même
serveur Flight, pour les 3 `write_disposition`) est maintenant détaillé
dans [docs/INGESTION.md](INGESTION.md#who-actually-writes-to-iceberg-single-writer-adr-0001),
avec la vérification concrète (catalogues SQLite physiquement séparés
aujourd'hui -- fragmentation, pas concurrence) et une alternative proposée
(non implémentée).

**`dhd drop-table <nom>` existe maintenant** (ajouté en relecture) et a
servi à supprimer la table `synthetic_100k_flight` que ce test avait
laissée dans l'entrepôt MinIO partagé du conteneur.

## 6. `batch_rows` en production (fix/ingest-batching-grpc, part A)

Les sections 4-5 ci-dessus mesuraient un `iceberg_table.append()`
**pyiceberg direct**, sans passer par Flight. `batch_rows` est maintenant
câblé dans `ingestion.engine.dlt_engine` (chaque lot devient son propre
commit, un par appel à `sink.write`, *jamais* pour `replace` -- voir
`ingestion/sinks/base.py`). Remesuré ici avec le **vrai chemin de
production** (`DltIngestor.run()` -> `FlightSink` -> `do_put` -> vrai
`HybridBackend` en mémoire, serveur Flight in-process sur un port local,
même technique que `tests/conftest.py::flight_server`, pas de mock) :
même table synthétique que la section 4 (`id: int64`, `name: string`,
`amount: float64`, `flag: bool`), 10M lignes, `write_disposition:
append`, RSS pic échantillonné par `psutil` toutes les 50ms, chaque
scénario lancé dans un processus `python` neuf (`scripts/spikes/` --
non, ce script n'est pas commité, voir ci-dessous) pour une mesure
RSS propre.

| `batch_rows` | Lots | Total | RSS pic | Commits |
|---|---|---|---|---|
| `null` (désactivé) | 1 | 14.880s | 1138.7 MiB | 1 |
| `500_000` | 20 | 20.179s | 1511.7 MiB | 20 |

**Résultat inattendu, vérifié, qui inverse la conclusion de la section
4 : `batch_rows` est ici *plus lent et consomme plus de mémoire*, pas
moins -- 20 commits coûtent plus cher que 1, dans ce chemin précis.**
Root cause identifiée en lisant `flight_server/app/backends/
hybrid_backend.py` (code serveur xorq/HybridBackend, non modifié,
conformément à la consigne) : `create_table`/`insert` appellent, à
*chaque* écriture, sans condition :

1. `_reflect_views()` (ligne ~236-241) : relit **la table entière**
   depuis Iceberg (`iceberg_table.scan().to_arrow()`) pour la
   réenregistrer comme vue DuckDB -- pas seulement le lot qui vient
   d'être écrit.
2. `_create_snapshot()` : `CHECKPOINT` puis `EXPORT DATABASE ... (FORMAT
   PARQUET)` -- un export complet de la base DuckDB entière sur disque.

Avec 1 commit, ce coût (relire + exporter ~10M lignes) n'est payé
qu'une fois. Avec 20 commits sur une table qui grossit de 500k lignes à
chaque lot, ce même travail -- proportionnel à la taille *courante* de
la table, pas à la taille du lot -- est payé 20 fois sur une table de
taille croissante, confirmé par la trace RSS (croissance quasi-linéaire
et monotone pendant toute la durée du run, jamais de redescente entre
deux lots -- pas en dents de scie comme on l'attendrait si la mémoire
du lot précédent était libérée) :

```
t= 0.0s  rss= 239.8 MiB      (baseline)
t= 1.0s  rss= 457.0 MiB
t= 3.1s  rss= 696.7 MiB
t= 6.2s  rss=1130.1 MiB
t=11.3s  rss=1492.7 MiB      (dernier lot)
```

**Pas une régression de ce correctif** -- `_reflect_views`/
`_create_snapshot` tournent déjà après *chaque* `create_table`/`insert`
aujourd'hui, avec ou sans `batch_rows` ; brancher `batch_rows` ne fait
qu'augmenter le nombre d'écritures par run, donc le nombre de fois où
ce coût déjà existant est payé. Avant ce correctif, une source ne
pouvait de toute façon committer qu'une fois par table par run -- ce
coût n'était donc jamais multiplié.

**Conséquence pratique pour le choix de `batch_rows` :** préférer peu
de lots gros (ou désactiver `batch_rows` -- `null`) tant que
`HybridBackend` reflète/snapshotte sur *chaque* écriture plutôt que sur
le run entier. Le gain mémoire théorique d'Arrow (section 4 : -60% en
pyiceberg direct) reste réel côté *client*, mais est aujourd'hui dominé
côté *serveur* par ce coût à chaque commit dès qu'il y a plus de
quelques lots. Proposé, non appliqué (hors périmètre -- interdiction de
toucher `HybridBackend`) : ne reflex/snapshotter qu'après le *dernier*
lot d'un run, ou rendre ces deux opérations asynchrones/différées.

Script de mesure non commité (one-off, dans le répertoire scratch de la
session, pas dans le dépôt) ; commande et méthode reproductibles
ci-dessus si besoin de la relancer.

## 7. Erreur gRPC « metadata size » (fix/ingest-batching-grpc, part B)

**Reproduite en direct**, serveur Flight in-process réel (même technique
que `tests/conftest.py::flight_server`, pas de mock) : créer une table
de 4000 colonnes `int64`, puis tenter d'y `append`-er 4000 colonnes de
même nom mais en `string` (incompatibilité de schéma réelle, pas une
valeur artificiellement énorme). Message client exact obtenu :

```
gRPC returned resource exhausted error, with message: received initial
metadata size exceeds limit. gRPC client debug context:
UNKNOWN:Error received from peer ipv4:127.0.0.1:49677
{created_time:"...", grpc_status:8,
 grpc_message:"received initial metadata size exceeds limit"}.
Client context: OK
```

327 caractères côté client -- c'est tout ce qui arrive ; `grpc_status:8`
= `RESOURCE_EXHAUSTED`. La vraie exception pyiceberg (incompatibilité de
schéma sur 4000 colonnes, avec un tableau diff et une trace qui dépasse
largement 4 Ko une fois sérialisés) n'atteint jamais le client -- **elle
est masquée, pas seulement tronquée.**

**Cause racine confirmée en lisant `xorq/flight/server.py`** (dépendance
tierce, non modifiée) :

```python
# gRPC caps the status trailers at 8 KB, and the message travels in them
# twice (percent-encoded grpc-message plus the binary status detail):
# 4 000 ASCII characters already overflow, [...]
STATUS_MESSAGE_BUDGET = 1024

def bounded_status(f):
    """Re-raise any failure as a FlightServerError that fits the status.
    A plain Python exception reaches the client only if its message plus
    traceback fit gRPC's metadata cap; past that, the client sees
    "received initial metadata size exceeds limit" and nothing else."""
    ...
```

`do_exchange` est décoré avec `@bounded_status` ; **`do_put` ne l'est
pas.** C'est tout le bug : n'importe quelle exception serveur pendant un
`do_put` dont le message+trace dépasse ~4 Ko sur le fil remplace la
vraie cause par ce message gRPC opaque.

**Hypothèses du brief, testées :**
- *Message d'erreur trop long renvoyé par le serveur* -- **confirmé**,
  c'est la cause exacte (ci-dessus).
- *Schéma très large* -- confirmé comme déclencheur dans cette
  reproduction (4000 colonnes), mais le schéma lui-même n'est pas la
  cause : c'est le message d'erreur *décrivant* le désaccord de schéma
  qui est trop long, pas le schéma transmis normalement (qui voyage
  dans le corps IPC, pas en métadonnées gRPC).
- *Valeur volumineuse passée en métadonnée* -- non confirmé comme
  mécanisme direct ; la seule métadonnée gRPC réelle en jeu ici est le
  statut d'erreur du serveur, pas une valeur de colonne.

**Une écriture de DONNÉES réelle est tout aussi exposée que le journal**
(vérifié ci-dessus : la reproduction est une vraie `append` de données,
pas le run log) -- les deux passent par le même `do_put`. Et oui,
l'erreur masque la vraie cause d'un autre échec : sans accès aux logs du
serveur, le client ne voit jamais que c'était un désaccord de schéma.

**Corrigé côté client** (la cause est serveur/xorq, donc pas touchée --
voir `ingestion/engine/dlt_engine.py::_write_chunk` et
`ingestion/runs/store.py`) :
1. Le moteur détecte la signature `"metadata size exceeds limit"` dans
   une `pyarrow.lib.ArrowException` levée pendant une écriture et la
   relève comme un `SinkError` explicite ("vérifie les logs du serveur
   Flight") au lieu de laisser planter tout le run de façon opaque et
   non journalisée.
2. `RunLog.record()` borne désormais la longueur d'un `error` enregistré
   (4000 caractères) -- hygiène générale, indépendante de la cause
   précise ci-dessus (une valeur de ligne ne déclenche pas ce bug
   directement, mais rien n'empêche qu'un message d'erreur déjà énorme
   serve ensuite de valeur à journaliser).

**Proposé côté serveur xorq, non appliqué** (hors périmètre -- ce
dépôt ne doit pas modifier `xorq`) : ajouter `@bounded_status` à
`do_put`, exactement comme `do_exchange` l'a déjà.

**Piège Windows en bonus (non lié au bug principal) :** le script de
reproduction direct (pyiceberg sans Flight, pour mesurer la taille réelle
du message masqué) a une fois crashé avec `UnicodeEncodeError: 'charmap'
codec can't encode character '❌'` -- pyiceberg imprime un emoji
❌ dans son tableau diff de comparaison de schéma, qui casse sur une
console Windows en code page héritée (cp1252/charmap). Observé une
seule fois (pas reproduit de façon fiable au deuxième essai, qui a
donné le `ArrowInvalid`/gRPC attendu à la place) -- noté comme piège
Windows possible, pas creusé davantage (bibliothèque tierce,
non modifiée).

## Non vérifié

- SQL Server (`mssql_chinook`) : toujours scaffolded, jamais lancé (voir
  `tests/docker/chinook/README.md`, inchangé depuis Vague 1).
- ~~Les 2 sources REST ... chargées dans Iceberg via `dhd ingest-dlt`~~ --
  fait en relecture (point 3) : `dhd --engine pyiceberg ingest-dlt --source
  jsonplaceholder`/`open_brewery_db` vérifiés en live (10/100/200 lignes
  chargées, re-run sans doublon), avec test d'intégration dédié
  (`tests/test_connectors_rest.py::TestRestSourcesActuallyLoadIntoIceberg`).
- L'expérience 5 n'a qu'un échantillon par scénario -- pas de confiance
  statistique, voir ci-dessus.
- Volume au-delà de 10M lignes.
- Comportement de `schema_contract` combiné à `write_disposition: merge`
  (testé seulement avec `replace`/`append` en pratique ici ; couvert en
  unitaire pour `merge` dans `tests/test_ingest_dlt.py`, mais pas rejoué
  ici contre une vraie source Postgres élargie).
- `batch_rows` (section 6) : le test d'interruption réel (kill -9 d'un
  processus) à 10M lignes -- seul un échec simulé (sink.write qui lève)
  a été testé, pas un vrai process tué entre deux lots sur ce volume.
  `write_disposition: merge` à 10M lignes avec `batch_rows` n'a pas été
  mesuré pour la mémoire/durée (seul `append` l'a été ci-dessus) ; le
  coût `_reflect_views`/`_create_snapshot` par commit s'y applique
  probablement de la même façon mais ce n'est pas vérifié. Pas d'autre
  valeur de `batch_rows` que 500 000 et `null` essayée.
- Erreur gRPC (section 7) : le seuil exact (nombre de colonnes/octets)
  où ça bascule n'a pas été cherché par dichotomie -- seulement vérifié
  que 4000 colonnes le déclenche de façon fiable et qu'un message court
  ne le déclenche pas. Pas reproduit via Docker/MinIO réel (seulement
  in-process) -- improbable que ça change quelque chose (le bug est
  dans la couche gRPC/xorq, pas le stockage), mais pas vérifié.
  L'incident Windows `UnicodeEncodeError` (pyiceberg) n'est vérifié
  qu'une fois, pas de façon fiable.

## Décision recommandée

**dlt seul, avec la garde ajoutée ici (registre + contrats de schéma +
merge idempotent), convient pour ce volume et ce nombre de sources.** Pas
besoin d'un autre outil : le report inclusif de l'incrémental n'est
dangereux qu'avec `append`, et c'est désormais rejeté à la validation --
la règle qui reste est "primary key connue => `merge`". La seule vraie
lacune structurelle est l'absence de détection des suppressions,
intrinsèque à toute extraction par curseur (dlt ou autre) sans CDC côté
source -- hors de portée de ce brief ("ne pas implémenter les instances
clients", et CDC serait une tâche à part entière).

**Second point structurel, non résolu ici** (voir "Qui écrit réellement
dans Iceberg" ci-dessus et dans docs/INGESTION.md) : `dhd ingest-dlt`
écrit directement via pyiceberg, jamais via le serveur Flight -- ce qui
évite aujourd'hui une vraie collision d'écriture concurrente seulement
parce que les deux catalogues sont des fichiers SQLite physiquement
différents (fragmentation du catalogue, pas concurrence). Ça tient tant
que `ICEBERG_CATALOG_URI` n'est pas partagé entre les deux chemins ; le
jour où il le sera, cette architecture aura besoin d'un vrai arbitrage
(writer unique réel, probablement en repassant par Flight) avant d'être
utilisée à plusieurs processus en même temps.
