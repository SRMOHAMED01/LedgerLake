# LedgerLake — Financial Data Lakehouse

Plateforme de données financières event-driven suivant l'architecture Medallion
(Bronze/Silver/Gold) : Kafka → Bronze (raw) → Spark (Silver, clean) → dbt (Gold,
métier) → Analytics/ML/BI. Gouvernée (OpenMetadata), testée en continu
(Great Expectations), orchestrée (Airflow), monitorée (Prometheus/Grafana).

## Structure du projet

- `ingestion/`        → Producers Kafka (transactions, customers, loans)
- `bronze/`             → Convention de stockage des données brutes (MinIO/S3)
- `spark_jobs/`          → Jobs Spark Bronze → Silver
- `silver/`               → Convention de stockage des données nettoyées (Parquet)
- `dbt_project/`           → Modèles dbt Silver → Gold (tables métier)
- `data_quality/`           → Suites Great Expectations
- `orchestration/`            → DAGs Airflow
- `governance/`                 → Config OpenMetadata (data lineage, catalog)
- `observability/`                → Prometheus + Grafana
- `analytics/`                       → Connexion BI / ML (dont CreditPulse)
- `docs/GUIDE_COMPLET.md`              → Guide de construction détaillé, palier par palier

## Comment démarrer

Lis `docs/GUIDE_COMPLET.md` en premier — il explique l'ordre de construction
(Palier A à H) et pourquoi cet ordre est important.

## Démarrage rapide (une fois le code écrit)

```bash
cp .env.example .env
docker-compose up
```

## Lien avec CreditPulse

Ce projet et CreditPulse (moteur de scoring crédit) sont complémentaires :
LedgerLake produit des données propres et gouvernées (`customer_360`,
`loan_portfolio_analysis`) qui peuvent directement alimenter le modèle
XGBoost de CreditPulse à la place de données simulées. Voir Palier H.
