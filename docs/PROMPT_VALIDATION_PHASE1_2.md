# Prompt — Validation et finalisation Phase 1 (Flight/HybridBackend) & Phase 2 (CLI `dhd`)

## Contexte

Tu travailles sur **DataHut-DuckHouse**, une plateforme analytics hybride
(Iceberg + DuckDB + Trino + dbt), sur la branche
`feature/adr-0001-canonical-hybrid-backend`. Les phases 0, 1 et 2 de la
roadmap ont été implémentées et testées dans un environnement sandbox
restreint (réseau limité, pas d'accès à `extensions.duckdb.org`). Ta mission
est de **valider en conditions réelles** ce qui n'a pas pu l'être, et de
finir ce qui reste ouvert — pas de repartir de zéro.

## Références obligatoires — à lire avant toute modification

Ces documents contiennent les décisions et contraintes qui s'appliquent
directement à ce travail. Ne pas les relire mène à re-découvrir des
problèmes déjà tranchés ou déjà corrigés.

- **`docs/adr/0001-canonical-hybrid-backend.md`** — décision centrale :
  `HybridBackend` est l'unique backend, Iceberg est l'unique cible
  d'écriture, DuckDB ne fait que refléter via des vues. **Lire en
  particulier la section "Post-implementation note (2026-08-29)"** : elle
  documente que l'API réelle du package `xorq` installé (`xorq==0.2.4`)
  diffère de ce que le code supposait à l'origine (pas de `xorq.registry`,
  pas de `xorq.duckdb.connect`, `FlightServer(make_connection=...)` et non
  `client=...`), et que `do_get`/`do_put`/`get_flight_info` sont déjà
  fournis génériquement par `xorq.flight.FlightServerDelegate` — ne pas les
  réimplémenter.
- **`docs/adr/0002-iceberg-catalog-nessie.md`** — Nessie comme catalogue
  cible (pas encore implémenté, Phase 3). Concerne indirectement ce travail
  : la commande `dhd branch` du CLI est un stub explicite tant que cet ADR
  n'est pas mis en œuvre.
- **`docs/adr/0003-tenant-metadata-postgres.md`**, **`0004`**,
  **`0005`** — pas concernés par ce travail (Phases 4+, derrière le point
  de bascule vers le multi-tenant). Ne pas y toucher, ne pas anticiper leur
  implémentation.
- **`docs/ROADMAP.md`** — sections **Phase 0** (fait), **Phase 1** (fait,
  à valider), **Phase 2** (fait, à valider). Le point de bascule vers le
  multi-tenant (Phase 6+) reste hors de portée de cette tâche.

## Ce qui a déjà été fait (ne pas refaire)

- `flight_server/app/app.py` et `query_orchestrator.py` supprimés ; logique
  d'analyse de requête sauvée dans `flight_server/app/query_diagnostics.py`
  (diagnostic seulement, hors chemin critique).
- `HybridBackend.create_table`/`insert` n'acceptent plus de paramètre
  `target` — toute écriture va dans Iceberg (ADR-0001).
- `flight_server/app/hybrid_backend.py`, `xorq_config.py`, `utils.py`,
  `app_xorq.py` corrigés pour correspondre à la vraie API `xorq` installée
  (voir la note post-implémentation de l'ADR-0001 pour le détail des bugs
  trouvés et corrigés).
- Package CLI `datahut_duckhouse/` créé (`cli.py`, `connection.py`),
  entrypoint `dhd` déclaré dans `pyproject.toml`, dépendance `click`
  ajoutée.
- Tests : `tests/test_hybrid_backend.py`, `tests/test_query_diagnostics.py`,
  `tests/test_utils.py`, `tests/test_cli.py` — 31 tests, tous passent dans
  le sandbox où ils ont été écrits (mais plusieurs chemins n'y ont pas pu
  être exercés bout en bout, voir ci-dessous).

## Ce qu'il reste à valider — dans l'ordre

1. **Installation propre** :
   ```bash
   uv sync
   ```
   ⚠️ **Point d'attention avant cette étape** : le `pyproject.toml` tel que
   laissé par ce travail utilise encore le format Poetry
   (`[tool.poetry.dependencies]`, `[tool.poetry.scripts]` pour l'entrypoint
   `dhd`). Si le passage à `uv` n'a pas encore été fait sur ce fichier,
   commencer par le convertir au format PEP 621 (`[project]`,
   `[project.dependencies]`, `[project.scripts]`) avant `uv sync` — sinon
   l'entrypoint `dhd` ne sera pas généré correctement. Une fois converti,
   confirmer que `uv sync` installe tout sans erreur (le sandbox précédent
   a dû installer plusieurs dépendances manuellement une à une :
   `xorq==0.2.4`, `pyiceberg`, `pyiceberg[sql-sqlite]`, `duckdb==1.3.2`,
   `s3fs`, `python-dotenv`, `click` — `uv sync` devrait toutes les couvrir
   via `pyproject.toml`/`uv.lock`, mais ce n'a jamais été vérifié en un seul
   passage).

2. **Suite de tests complète** :
   ```bash
   uv run pytest -v
   ```
   Le sandbox précédent a dû exclure `tests/test_trino_client.py` (module
   `trino` absent de son environnement) — vérifier que ce test passe aussi
   ici.

3. **Démarrage réel du serveur Flight** (bloqué dans le sandbox précédent
   par l'extension DuckDB `iceberg`, téléchargement refusé) :
   ```bash
   uv run python -m flight_server.app.app_xorq
   ```
   Confirmer qu'il démarre et reste bloquant sur le port configuré
   (`FLIGHT_SERVER_PORT`, défaut `8815`) sans crasher. Le warehouse Iceberg
   par défaut est maintenant `s3://duckhouse-warehouse/` (MinIO) — s'assurer
   que le service `minio` du `docker-compose.yml` tourne avant de lancer le
   serveur, sinon la connexion au catalogue échouera à la première écriture
   réelle (la construction du catalogue lui-même ne nécessite pas MinIO,
   seule l'écriture/lecture de données en a besoin — voir ADR-0001).

4. **Chemin CLI → serveur → Iceberg, bout en bout**, serveur lancé (étape 3)
   dans un terminal, puis dans un autre :
   ```bash
   uv run dhd create-table demo path/vers/un_fichier.csv
   uv run dhd list-tables
   uv run dhd query "SELECT * FROM demo" --limit 5
   uv run dhd insert demo path/vers/un_autre_fichier.csv
   ```
   Ce chemin complet (réseau + Iceberg + vues DuckDB) n'a **jamais** été
   testé de bout en bout — seule la couche `HybridBackend`/Iceberg a été
   validée directement (sans passer par Flight/CLI), et le CLI a été testé
   avec la connexion mockée (voir `tests/test_cli.py`). C'est la validation
   la plus importante de cette tâche.

5. Si `dhd branch create <nom>` est appelé, il doit échouer clairement avec
   un message expliquant que Nessie n'est pas câblé (ADR-0002, Phase 3) —
   confirmer que le comportement observé correspond à l'intention, pas
   improviser une implémentation partielle de Nessie ici.

## Points ouverts connus — ne pas les "corriger" sans decision explicite

- ~~`warehouse_path` est un chemin local, pas une URI S3.~~ **Résolu depuis** (voir
  ADR-0001, section "Update (2026-08-31): MinIO/S3 wiring resolved") :
  `HybridBackend` détecte désormais un `warehouse_path` en `s3://`/`s3a://` et route
  vers `_connect_s3_iceberg`, qui construit le catalogue avec les vraies propriétés
  `s3.*` de pyiceberg et configure DuckDB (`httpfs`, adressage path-style requis par
  MinIO). **Non vérifié** : un vrai aller-retour d'écriture/lecture contre un MinIO
  réellement lancé — seule la construction du catalogue (métadonnées, sans I/O
  réseau) a pu être testée. C'est maintenant l'objet du point 4 ci-dessus.
- **`Backend.do_connect` (client Flight) n'a pas de timeout visible** dans
  `xorq==0.2.4` — un serveur injoignable peut faire bloquer indéfiniment
  `dhd` plutôt que d'échouer proprement. Documenté dans
  `datahut_duckhouse/connection.py`. Ne pas ajouter de solution de contournement
  complexe (retry, thread avec timeout) sans validation — un simple
  message clair dans le README suffit pour l'instant si le problème se
  confirme.
- ~~**`scripts/ingest_flight.py` et `scripts/pipeline.py` sont cassés**~~ **Corrigé**
  (2026-08-31) : réécrits contre `datahut_duckhouse.connection.get_connection()`, le
  même chemin vérifié que le CLI. `pipeline.py` avait un bug plus profond qu'un simple
  `target=` obsolète : il construisait un `FlightDescriptor.for_path(...)` que le vrai
  `do_put` du serveur ne sait pas parser (il attend un descripteur `for_command(...)`) —
  il n'aurait jamais fonctionné contre le vrai serveur. Voir `tests/test_scripts.py`.
  **Non vérifié** : ces deux scripts n'ont été testés qu'avec la connexion mockée, pas
  contre un vrai serveur Flight — à couvrir par l'étape 4 ci-dessus si le temps le
  permet.

## Ce qu'il ne faut PAS faire dans cette tâche

- Ne pas commencer la Phase 3 (Nessie) ou la Phase 4 (Postgres) — seulement
  valider/finir les Phases 1 et 2.
- Ne pas toucher aux ADR-0004/0005 (isolation tenant, auth) — hors de portée
  tant que le point de bascule décrit dans `docs/ROADMAP.md` n'est pas
  atteint.
- Ne pas réintroduire de code mocké masquant un vrai problème d'API — la
  découverte de cette session (voir note ADR-0001) vient justement du fait
  que les tests mockaient `xo` entièrement. Toute nouvelle assertion de
  test doit correspondre à un appel réellement exercé contre le vrai
  package installé, pas à une supposition sur son API.

## Critère d'acceptation

La tâche est terminée quand :
- `uv sync` et `uv run pytest -v` passent sans exclusion.
- Le serveur Flight démarre réellement et reste actif.
- Le cycle `dhd create-table` → `dhd list-tables` → `dhd query` →
  `dhd insert` fonctionne de bout en bout contre le serveur réel, avec des
  résultats cohérents (les lignes insérées apparaissent bien dans la
  requête).
- Tout écart par rapport à ce qui précède est documenté (nouvelle note dans
  l'ADR-0001 ou nouvelle entrée dans `docs/ROADMAP.md`), pas juste corrigé
  silencieusement.
