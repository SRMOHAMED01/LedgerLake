# LedgerLake — Financial Data Lakehouse

See [root README](../README.md) for full documentation.

## Quick Start

```bash
cp .env.example .env
docker-compose up -d
```

## Structure

- `ingestion/` → Kafka producers + bronze_writer
- `spark_jobs/` → Bronze → Silver (Delta Lake)
- `dbt_project/` → Silver → Gold (dbt + DuckDB)
- `data_quality/` → Great Expectations
- `orchestration/dags/` → Airflow DAGs
- `governance/` → OpenMetadata connectors
- `observability/` → Prometheus + Grafana
- `analytics/` → CreditPulse export, Metabase

## Full Documentation

- [GUIDE_COMPLET.md](docs/GUIDE_COMPLET.md) — Step-by-step construction guide (Palier A → H)
- [INTERVIEW_GUIDE.md](docs/INTERVIEW_GUIDE.md) — Interview preparation notes