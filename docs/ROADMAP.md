# DataHut-DuckHouse — Roadmap vers un SaaS

**Statut :** Document vivant, à réviser à chaque changement de phase.
**Dernière mise à jour :** 2026-08-29

## Principe directeur

Construire d'abord un moteur de données mono-utilisateur solide et dogfoodable (CLI),
avant tout travail spécifique au multi-tenant (isolation, auth, quotas, facturation).
Le travail SaaS (ADR-0004, ADR-0005 et ce qui suivra) est coûteux à construire et
inutile tant qu'il n'y a pas de second utilisateur réel dont les données doivent être
protégées des tiennes. Le déclencheur explicite de bascule est documenté en fin de
roadmap — ce n'est pas une date, c'est une condition.

Ce document ne remplace pas les ADR : il séquence *quand* implémenter des décisions déjà
prises (ADR-0001 à 0005) et celles qui restent à écrire. Toute décision technique
durable continue de vivre dans `docs/adr/`.

---

## Phase 0 — Fondation cohérente ✅ Fait

- Iceberg comme seule cible d'écriture dans `HybridBackend` (ADR-0001)
- Retrait de `app.py` et `query_orchestrator.py`, logique d'analyse sauvée dans
  `query_diagnostics.py`
- Tests, Makefile, `validate_project.py`, README mis à jour en cohérence

**Sortie de phase :** mergée sur `feature/adr-0001-canonical-hybrid-backend`, prête à
push.

---

## Phase 1 — Boucle Flight complète

**Objectif :** un client Flight peut écrire *et relire* des données via
`HybridBackend`, ce qui n'est le cas d'aucun chemin aujourd'hui.

- [ ] Implémenter `do_get`, `list_flights`, `get_flight_info` sur `HybridBackend`
- [ ] Rewiring de `app_xorq.py` / `xorq_config.py` pour enregistrer `HybridBackend`
      comme backend unique du serveur Flight (au lieu de deux backends parallèles)
- [ ] Test d'intégration : write via Flight → read via Flight → même résultat qu'un
      accès direct au backend

**Dépendances :** Phase 0.
**Sortie de phase :** un `docker-compose up` + un client Flight quelconque peut faire un
aller-retour complet sur une table.

---

## Phase 2 — CLI

**Objectif :** un outil en ligne de commande qui exerce le chemin Flight complet,
utilisable pour un usage réel (toi, AYITISTATS) sans notion de tenant.

- [ ] `dhd create-table <name> <source>`
- [ ] `dhd insert <table> <source> [--mode append|overwrite]`
- [ ] `dhd query <sql>` — lecture via les vues DuckDB reflétées
- [ ] `dhd list-tables`
- [ ] `dhd branch <create|list|switch>` — expose le modèle Nessie (voir Phase 3) une
      fois disponible ; jusque-là, no-op documenté ou message clair "pas encore actif"
- [ ] Distribution simple : `poetry install` + entrypoint, pas encore de packaging PyPI

**Dépendances :** Phase 1.
**Sortie de phase :** tu peux ingérer et interroger des données réelles (AYITISTATS,
par exemple) sans écrire de Python à chaque fois.

---

## Phase 3 — Nessie (ADR-0002)

**Objectif :** le catalogue partagé remplace le catalogue SQL embarqué, condition
préalable à tout writer concurrent.

- [ ] Service `nessie` dans `docker-compose.yml`, backing store embarqué pour le dev
- [ ] `HybridBackend.do_connect` bascule sur `catalog_type="nessie"`
- [ ] Trino pointé vers le même catalogue Nessie que le Flight server
- [ ] `dhd branch` (Phase 2) devient fonctionnel

**Dépendances :** Phase 1. Peut être menée en parallèle de la Phase 2 si le temps le
permet — pas de dépendance stricte entre les deux, seulement un ordre logique de valeur
livrée.
**Sortie de phase :** Trino et le Flight server voient un état de catalogue identique ;
une branche peut être créée, utilisée, et rejetée sans toucher `main`.

---

## Phase 4 — Postgres pour `xorq-service` (ADR-0003)

**Objectif :** le registre tenant/dataset/billing survit à un redémarrage.

- [ ] Service `postgres` dans `docker-compose.yml`
- [ ] Schéma : `tenants`, `datasets`, `catalogs`, `query_metrics`, `billing_events`
- [ ] Migration (Alembic ou équivalent)
- [ ] `XORQOrchestrator` réécrit pour lire/écrire via Postgres, même API publique

**Dépendances :** aucune dépendance dure sur les phases précédentes — peut être fait à
tout moment. Placé ici parce que sa valeur reste faible tant qu'il n'y a qu'un seul
tenant (toi), et qu'il vaut mieux ne pas construire de persistance pour des données qui
n'existent pas encore en usage réel.
**Sortie de phase :** `xorq-service` peut redémarrer sans perte de données.

---

## Phase 5 — Dogfooding

**Objectif :** valider le moteur en usage réel avant d'investir dans le multi-tenant.

- [ ] Migrer un vrai jeu de données AYITISTATS (ou un autre projet perso) sur la
      plateforme via le CLI
- [ ] Observer : le modèle Iceberg-only tient-il en pratique ? Le coût par écriture
      (commit Iceberg systématique, snapshot) est-il acceptable au volume réel ?
- [ ] Ajuster `HybridBackend`/le CLI selon les frictions rencontrées

**Dépendances :** Phases 1-3 (Nessie utile mais Phase 3 peut rester en cours si le
dogfooding ne teste pas encore le branching).
**Sortie de phase :** un cas d'usage réel tourne en continu sans intervention manuelle
récurrente. C'est la preuve que la fondation (Phase 0-3) est solide avant d'y empiler
du multi-tenant.

---

## 🚦 Point de bascule — condition de passage au SaaS

**Ne pas commencer la Phase 6 avant qu'une des conditions suivantes soit vraie :**
- Un second utilisateur concret (personne, équipe, projet externe à toi) a besoin
  d'utiliser la plateforme sans voir tes données ou celles d'un autre tenant, **ou**
- Tu as une visibilité concrète sur un premier client/utilisateur payant à onboarder
  dans un horizon proche.

Tant qu'aucune des deux n'est vraie, rester en Phase 5 et continuer à durcir le moteur
mono-tenant est le meilleur usage du temps. Construire l'isolation et l'auth avant ce
signal, c'est de l'infrastructure spéculative — le risque n'est pas "on n'y arrivera
pas", c'est "on optimise le mauvais problème en premier".

---

## Phase 6 — Isolation tenant (ADR-0004)

- [ ] Namespace Iceberg par tenant, vérifié dans `HybridBackend`
- [ ] Branche Nessie par tenant, provisionnée par `create_tenant.py`
- [ ] Test de frontière : un appel scopé à un tenant ne peut pas nommer une table d'un
      autre namespace, même explicitement
- [ ] Étape de promotion branche → `main` définie (manuelle pour commencer)

**Dépendances :** Phase 3 (Nessie), Phase 4 (Postgres), et le déclencheur ci-dessus.

## Phase 7 — Authentification & autorisation (ADR-0005)

- [ ] Table `tenant_tokens` dans Postgres
- [ ] Validation de token sur `xorq-service` et sur le serveur Flight
- [ ] Câblage avec la frontière de namespace de la Phase 6
- [ ] Cache court-TTL pour éviter un aller-retour Postgres par appel
- [ ] Process documenté d'émission/rotation de token dans `create_tenant.py`

**Dépendances :** Phase 4, Phase 6 (l'isolation doit exister pour que l'auth ait
quelque chose à protéger).

## Phase 8 — Quotas & rate limiting

- [ ] Limites dures par tenant (requêtes/min, volume ingéré, taille de stockage)
- [ ] Rejet explicite (429/erreur Flight dédiée) plutôt que dégradation silencieuse
- [ ] Metrics exposées par tenant, en s'appuyant sur `track_query_metric` existant

**Dépendances :** Phase 7 (les quotas n'ont de sens qu'une fois l'identité du
tenant fiable).

## Phase 9 — Cycle de vie tenant complet

- [ ] Onboarding : provisioning automatique (namespace + branche + token) à la création
- [ ] Suspension : accès coupé sans perte de données, réactivable
- [ ] Offboarding : export des données du tenant + suppression, avec délai de
      rétention documenté (pertinent si un tenant UE impose des considérations RGPD)
- [ ] Facturation : passage de `billing_events` (déjà tracé) à une intégration
      paiement réelle si le produit va jusque-là

**Dépendances :** Phase 6, 7, 8.

## Phase 10 — Observabilité

- [ ] Audit de `monitoring.py` existant (portée actuelle non encore évaluée)
- [ ] Logs structurés par tenant
- [ ] Alerting sur les seuils de quota (Phase 8) et les échecs de promotion de branche
      (Phase 6)
- [ ] Traces distribuées Flight → Trino → dbt, au minimum pour le débogage d'incident

**Dépendances :** peut commencer dès la Phase 6, s'approfondit au fil des phases
suivantes. Ne pas attendre la fin de la Phase 9 pour s'y mettre — c'est le genre de
travail qui coûte cher à rattraper après coup.

---

## Hors roadmap pour l'instant

- Interface web / dashboard tenant-facing
- Facturation automatisée (Stripe ou équivalent) — dépend d'un vrai besoin commercial
- Support multi-région / réplication géographique
- SLA formels

Ces éléments ne sont pas écartés, seulement volontairement absents tant que les phases
0-5 ne sont pas éprouvées et qu'aucun signal de bascule (voir ci-dessus) ne s'est
présenté.
