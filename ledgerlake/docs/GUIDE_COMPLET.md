# 🗺️ LedgerLake — Guide Complet de Construction

> Financial Data Lakehouse : Kafka → Bronze → Silver → Gold (dbt/Spark) → Analytics/ML/BI

---

## Philosophie de construction

Comme pour CreditPulse, on construit en **paliers testables**, du plus simple au plus avancé. Pas de Spark cluster distribué le jour 1 — on commence petit et fonctionnel, on complexifie ensuite.

```
Palier A — Ingestion + Bronze
   → Kafka produit des événements, ils atterrissent bruts dans le lac (MinIO/S3)

Palier B — Silver (nettoyage)
   → Spark (ou pandas au début) transforme Bronze en données propres

Palier C — Gold (couche métier) avec dbt
   → Tables analytiques prêtes pour BI/ML (customer_360, loan_portfolio_analysis...)

Palier D — Data Quality
   → Great Expectations valide chaque étape du pipeline

Palier E — Orchestration
   → Airflow enchaîne tout automatiquement (DAG financial_daily_pipeline)

Palier F — Gouvernance
   → OpenMetadata cartographie les tables, leur origine, leur propriétaire

Palier G — Observabilité + Industrialisation
   → Prometheus/Grafana, Docker Compose, CI/CD

Palier H (bonus) — Branchement vers Analytics/ML
   → Power BI / dashboard, et connexion vers CreditPulse
```

---

## PALIER A — Ingestion Kafka + Couche Bronze

### But
Simuler des événements financiers réalistes, les publier dans Kafka, et les stocker bruts (sans aucune transformation) dans un data lake.

### A.1 — Producers Kafka (`ingestion/producers/`)
- `transaction_producer.py` : génère des transactions simulées (`transaction_id`, `customer_id`, `amount`, `merchant`, `country`, `timestamp`) et les publie sur le topic `transactions-events`
- `customer_producer.py` : événements clients (création de compte, mise à jour profil) → topic `customers-events`
- `loan_producer.py` : événements de crédit → topic `loan-events`

**Pourquoi plusieurs topics et pas un seul ?** Chaque type d'événement a un schéma et une fréquence différents. Les séparer permet à chaque consumer de ne traiter que ce qui le concerne — c'est une pratique standard en architecture event-driven.

### A.2 — Stockage Bronze (`bronze/`)
- Choix technique : **MinIO** en local (compatible API S3, gratuit, tourne dans Docker) — tu factures ça comme "expérience S3" en entretien car l'API est identique
- Un consumer Kafka simple (`ingestion/bronze_writer.py`, à créer) lit les topics et écrit **sans transformation** dans le lake, partitionné par date :
```
bronze/
  transactions/year=2026/month=08/day=03/transactions_<batch_id>.json
  customers/year=2026/month=08/day=03/...
  loans/year=2026/month=08/day=03/...
```
- Format JSON pour commencer (plus simple à débugger), tu passeras en Parquet au Palier B

### Règle d'or de la couche Bronze
**On ne modifie jamais Bronze.** C'est ton historique brut immuable — si un bug apparaît plus tard dans Silver/Gold, tu dois pouvoir rejouer depuis Bronze sans avoir perdu d'information.

### ✅ Checkpoint Palier A
Tu lances tes 3 producers, tu vois les messages arriver dans Kafka (via `kafka-console-consumer` ou l'UI Kafka), et les fichiers JSON bruts apparaissent dans MinIO, correctement partitionnés par date.

---

## PALIER B — Silver (nettoyage et standardisation)

### But
Transformer les données brutes et hétérogènes en données propres, typées, et standardisées.

### B.1 — Jobs Spark (`spark_jobs/`)
- `bronze_to_silver_transactions.py` :
  - Lit les fichiers JSON de Bronze
  - Nettoyage : supprime les doublons (`dropDuplicates` sur `transaction_id`), filtre les valeurs nulles critiques
  - Typage : `amount` en `DoubleType`, `timestamp` en `TimestampType`
  - Standardisation : normalise les valeurs (`"MA"`, `"Morocco"`, `"Maroc"` → `"Morocco"`)
  - Écrit en **Parquet** (format colonnaire, bien plus performant que JSON pour l'analytique) partitionné par date dans `silver/transactions/`
- `bronze_to_silver_customers.py` : même logique pour les événements clients

**Pourquoi Spark et pas juste pandas ?** Pandas charge tout en mémoire — ça casse au-delà de quelques millions de lignes. Spark distribue le calcul. Même en local (mode standalone), l'API est identique à un vrai cluster de production — c'est ce qui compte pour ton CV.

### B.2 — Enrichissement
Ajoute des colonnes dérivées utiles pour la suite :
- `customer_segment` (à partir de règles simples au début : `total_transactions > seuil`)
- `country_region` (mapping pays → région)

### ✅ Checkpoint Palier B
Un job Spark tourne en local, tu peux lire les fichiers Parquet de Silver avec `spark.read.parquet(...)` et voir des données propres, typées, sans doublons.

---

## PALIER C — Gold (couche métier avec dbt)

### But
Créer les tables analytiques finales, celles que les analystes, dashboards et modèles ML vont réellement utiliser.

### C.1 — Setup dbt (`dbt_project/`)
- dbt se connecte à ton entrepôt analytique (DuckDB en local pour débuter — léger et rapide, ou PostgreSQL si tu veux rester cohérent avec CreditPulse)
- Les données Silver (Parquet) sont chargées comme sources dbt

### C.2 — Modèles Gold (`dbt_project/models/gold/`)
- **`customer_360.sql`** : vue à 360° du client — `customer_id`, `age`, `country`, `total_transactions`, `average_transaction`, `credit_score`, `risk_level`
- **`loan_portfolio_analysis.sql`** : `loan_id`, `customer_id`, `amount`, `default_probability`, `risk_category`
- **`fraud_detection_metrics.sql`** : `customer_id`, `number_transactions`, `suspicious_operations`, `fraud_score`
- **`customer_monthly_spending.sql`** : agrégation mensuelle des dépenses par client (`SUM(amount)`, `COUNT(*)` groupés par `customer_id`)

Chaque modèle dbt = un fichier SQL versionné, testé (`dbt test`), documenté (`dbt docs generate`). C'est ça, la vraie valeur : la traçabilité et la reproductibilité des transformations métier.

### ✅ Checkpoint Palier C
`dbt run` exécute tous les modèles sans erreur, `dbt test` passe, et tu peux interroger `customer_360` avec une requête SQL simple pour voir des données métier cohérentes.

---

## PALIER D — Data Quality

### But
Garantir qu'aucune donnée incorrecte ne progresse dans le pipeline — critique en finance.

### D.1 — Great Expectations (`data_quality/great_expectations/`)
Suites de validation à chaque étape :
- **Sur Bronze → Silver** : `amount > 0`, `customer_id NOT NULL`, `currency` dans une liste de devises acceptées, `transaction_date` valide (pas dans le futur, pas antérieure à une date de lancement)
- **Sur Silver → Gold** : cohérence des agrégations (ex : `total_transactions >= 0`)

### D.2 — Intégration dans le pipeline
Le pipeline devient :
```
Kafka → Bronze → [Quality Check] → Silver → [Quality Check] → Gold
```
Si une validation échoue, le pipeline s'arrête et alerte (au lieu de laisser passer des données corrompues vers les tables métier).

### ✅ Checkpoint Palier D
Injecte volontairement une donnée invalide (ex: `amount = -50`) dans Bronze → le pipeline doit la bloquer avant Silver et log une erreur claire.

---

## PALIER E — Orchestration avec Airflow

### But
Automatiser l'enchaînement complet, comme en production.

### E.1 — DAG principal (`orchestration/dags/financial_daily_pipeline.py`)
Tâches dans l'ordre :
1. `extract_and_validate_bronze` — vérifie que les nouveaux fichiers Bronze sont arrivés
2. `spark_bronze_to_silver` — déclenche les jobs Spark du Palier B
3. `quality_check_silver` — Great Expectations
4. `dbt_run_gold` — déclenche `dbt run` du Palier C
5. `quality_check_gold` — validation finale
6. `notify_completion` — log ou notification de succès

### ✅ Checkpoint Palier E
Le DAG tourne dans l'UI Airflow (localhost:8080) de bout en bout, chaque tâche passe au vert, et tu peux relancer une tâche individuelle en cas d'échec sans tout rejouer.

---

## PALIER F — Data Governance

### But
Rendre la plateforme "découvrable" : n'importe qui dans l'entreprise doit pouvoir savoir d'où vient une table, qui la possède, et comment elle a été construite.

### F.1 — OpenMetadata (`governance/`)
- Connecte OpenMetadata à ton entrepôt (PostgreSQL/DuckDB) et à Airflow
- Documente pour chaque table Gold : `owner` (ex: "Risk Department"), `source` (ex: "Kafka transactions-events → Spark → dbt"), description métier

### ✅ Checkpoint Palier F
Dans l'UI OpenMetadata, tu retrouves `customer_360` avec sa lignée complète (data lineage) : Kafka → Bronze → Silver → Gold, visible graphiquement.

---

## PALIER G — Observabilité + Industrialisation

### G.1 — Prometheus + Grafana (`observability/`)
- Métriques pipeline : nombre de messages Kafka traités, durée des jobs Spark, taux d'échec des tâches Airflow
- Dashboard Grafana : volume de transactions/jour, taux de fraude détecté, latence du pipeline

### G.2 — Docker Compose
Un seul `docker-compose.yml` qui lance : Kafka+Zookeeper, MinIO, Spark (mode standalone), Airflow, dbt (via un conteneur ou en local), Prometheus, Grafana

### G.3 — CI/CD
GitHub Actions : lint SQL (sqlfluff), tests dbt automatiques à chaque push, build des images Docker

### ✅ Checkpoint Palier G
`docker-compose up` démarre toute la plateforme.

---

## PALIER H (Bonus) — Analytics & Connexion à CreditPulse

- `analytics/` : connexion Power BI (ou Metabase, gratuit, pour une démo locale) sur les tables Gold
- **Le lien avec CreditPulse** : `loan_portfolio_analysis` et `customer_360` deviennent les données d'entrée du modèle XGBoost de CreditPulse, remplaçant les données simulées — c'est l'histoire complète "plateforme de données → moteur de décision" que tu racontes en entretien.

---

## 🏗️ Architecture finale (vue d'ensemble)

```
                    Banking Systems
             (Core Banking, Payment Gateway,
                Mobile Banking, CRM, Risk)
                          │
                          │
                     Apache Kafka
        (topics: transactions / customers / loans / fraud)
                          │
        ==============================================
                     BRONZE LAYER
              Raw events, JSON, MinIO/S3
              partitionné par year/month/day
        ==============================================
                          │
                     Spark Jobs
           (nettoyage, dédup, typage, standardisation)
                          │
        ==============================================
                     SILVER LAYER
                Cleaned data, Parquet
        ==============================================
                          │
                      dbt Models
        ==============================================
                     GOLD LAYER
        customer_360 / loan_portfolio_analysis /
        fraud_detection_metrics / monthly_spending
        ==============================================
                 │                        │
            Power BI/BI              ML Models
            Dashboards            (CreditPulse, Fraude)

   Transverse :
   Great Expectations (qualité) — à chaque frontière de couche
   Airflow (orchestration) — enchaîne tout le pipeline
   OpenMetadata (gouvernance) — traçabilité et data lineage
   Prometheus/Grafana (observabilité)
```

## 📋 Table de suivi

| Palier | Contenu | Différenciant Data Eng | Durée réaliste |
|---|---|---|---|
| A | Kafka + Bronze | ⭐⭐ Base solide | 1-2 semaines |
| B | Spark + Silver | ⭐⭐⭐ Très fort (Spark rare chez les juniors) | 2 semaines |
| C | dbt + Gold | ⭐⭐⭐ Très fort | 2 semaines |
| D | Great Expectations | ⭐⭐ Fort (critique en finance) | 1 semaine |
| E | Airflow | ⭐⭐ Attendu | 1 semaine |
| F | OpenMetadata (gouvernance) | ⭐⭐⭐ Très rare chez les juniors, très valorisé | 1 semaine |
| G | Observabilité + Docker | ⭐⭐ Attendu | 1 semaine |
| H | Connexion CreditPulse | ⭐⭐⭐ Raconte une vraie histoire de plateforme | optionnel |

---

## Comment on travaille ensemble à partir de maintenant

On suit les paliers dans l'ordre, comme pour CreditPulse. Chaque fichier du zip fourni contient un commentaire d'en-tête expliquant précisément ce qu'il doit contenir. Dis-moi quand tu veux démarrer le **Palier A.1 (les producers Kafka)** et on code ensemble.
