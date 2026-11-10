# Ingestion architecture (`feat/dhd-ingest`, step A)

How `ingestion/` is laid out, why, and how to extend it. For how to *use*
`dhd ingest` as an operator, see [docs/INGESTION.md](INGESTION.md) -- this
document is for anyone changing the package itself.

## Layers

```
ingestion/
  __init__.py        API publique : list_sources, validate_registry,
                      extract_source, run_source (+ les exceptions)
  config/             Ce qu'est une source -- rien d'exécutable.
    models.py           SourceConfig, IncrementalConfig (pydantic)
    rules.py             Règles de validation croisées
    registry.py           Charge/valide sources.yml
    sources.yml
  connectors/          Comment extraire une source par famille.
    base.py               Protocole Connector.build_resources(source, work_dir)
    registry.py            Le point d'extension : family -> connecteur
    sql.py, files.py, rest.py
    _dlt_sources.py        Le seul autre module (avec engine/dlt_engine.py)
                            qui importe dlt
  engine/               Orchestre extraction + écriture + journal.
    base.py                Protocole Ingestor, RunResult, TableResult
    dlt_engine.py           DltIngestor -- la seule implémentation
    state.py                Curseur incrémental, isolé par (source, resource)
  sinks/                Où les lignes atterrissent réellement.
    base.py                Protocole Sink, SinkError, SchemaContractViolationError
    flight_sink.py          replace/append -- écrit via le serveur Flight
    direct_sink.py          merge -- écrit en direct via pyiceberg
    catalog_guard.py        Preuve que client et serveur visent le même catalogue
  runs/                 Le journal d'exécution.
    models.py              Schéma d'une ligne (_ingest_runs)
    store.py                RunLog, avec repli SQLite local
```

`datahut_duckhouse/ingest_dlt.py` (et bientôt `ingestion/cli.py`, étape B)
est la seule couche qui parle à la fois à `ingestion` et à `click`/au
reste de `dhd` -- elle traduit les exceptions du domaine en erreurs CLI
avec le bon code de sortie, et n'importe que l'API publique de haut
niveau de `ingestion` (jamais `ingestion.connectors...`,
`ingestion.engine...`, etc. directement).

## Règles de dépendance (vérifiées par un test)

`tests/ingestion/test_dependency_rules.py` parse les imports de chaque
module (`ast`, pas de dépendance à `import-linter`) et fait échouer la
suite si l'une de ces règles est violée :

1. `config/` n'importe ni `dlt`, ni `connectors/`, ni `sinks/`, ni `engine/`.
2. Seuls `engine/dlt_engine.py` et `connectors/_dlt_sources.py` importent `dlt`.
3. `connectors/` ne connaît ni `sinks/` ni `runs/`.
4. `sinks/` ne connaît ni `dlt` ni `connectors/`.
5. `datahut_duckhouse/cli.py` et `ingest_dlt.py` n'importent que l'API
   publique de `ingestion/` (jamais un sous-module directement).
6. Aucun cycle d'import entre les sous-modules de `ingestion/`.

Une violation de ces règles fait échouer `pytest` avant même d'écrire une
seule ligne de code applicatif qui la contournerait silencieusement.

## Le contrat du Sink

Chaque `write_disposition` fixe à la fois la *sémantique* et le
*comportement en cas d'échec en cours de route* :

| `write_disposition` | Sémantique | Écrit par | Reprise après un crash |
|---|---|---|---|
| `replace` | Création ou remplacement complet | `FlightSink` (serveur Flight) | Sûr : Iceberg n'expose un commit qu'une fois terminé -- un envoi partiel n'est simplement jamais commité. |
| `append` | Ajout sans déduplication | `FlightSink` | Pas sûr tel quel : si le commit a atterri avant l'interruption, une relance naïve duplique ces lignes -- rien dans les données ne permet de le détecter après coup. |
| `merge` | Upsert par `primary_key` (pyiceberg `Table.upsert`) | `DirectSink` (direct, gardé par `catalog_guard`) | Sûr : idempotent quel que soit le nombre de fois où les mêmes lignes extraites sont réappliquées. |

`merge` n'a pas d'équivalent côté Flight aujourd'hui : `HybridBackend`
hérite une méthode `upsert()` fonctionnelle, inchangée, de
`PyIcebergBackend` (xorq), mais aucune action Flight ni `UploadMode` ne
l'atteint par le réseau. `DirectSink` écrit donc en direct, mais seulement
après que `catalog_guard.check_same_catalog` a prouvé que le client et le
serveur Flight visent le même catalogue -- sinon, code de sortie 2.

## Comment ajouter...

**(a) une source** : `uv run python scripts/new_source.py <famille> <nom>`,
remplir les `TODO` dans `ingestion/config/sources.yml`, lancer `dhd
ingest validate`. Aucun changement de code.

**(b) une famille de connecteur** : créer `ingestion/connectors/ma_famille.py`
avec une fonction `build_resources(source, work_dir) -> dict[str, Path]`
(voir `connectors/base.py`'s `Connector`) ; l'enregistrer dans
`connectors/registry.py` (`register("ma_famille", ma_famille)`) ; ajouter
`"ma_famille"` au `Literal` de `SourceConfig.family`
(`config/models.py`). Si elle a besoin de `dlt`, passer par un nouveau
constructeur dans `connectors/_dlt_sources.py`, pas un import direct.

**(c) un autre moteur que dlt** : implémenter
`ingestion.engine.base.Ingestor` (une méthode : `run(source) ->
RunResult`) dans un nouveau module sous `engine/` ; le brancher à la place
de `DltIngestor` dans `ingestion/__init__.py`'s `run_source`. Les
connecteurs et les sinks n'ont besoin d'aucun changement -- ils ne
connaissent pas l'engine.

**(d) une autre destination (sink)** : implémenter
`ingestion.sinks.base.Sink` (`list_tables`, `write`) dans un nouveau
module sous `sinks/` ; le brancher dans `DltIngestor._sink_for` (ou son
équivalent) selon le critère qui doit le sélectionner (aujourd'hui :
`write_disposition`). Un nouveau sink n'a jamais besoin de connaître
`dlt` ni les connecteurs.
