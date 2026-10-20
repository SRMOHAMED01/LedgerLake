# LedgerLake — Financial Data Lakehouse

> **A production-grade financial data platform demonstrating end-to-end data engineering skills.**

## What is LedgerLake?

LedgerLake is a **Financial Data Lakehouse** following the **Medallion Architecture** (Bronze → Silver → Gold). It simulates a real financial institution's data platform: Kafka ingests events → Bronze stores raw → Spark cleans → dbt models business tables → Analytics/ML consumes.

**Built to demonstrate:** Data engineering skills for interviews — architecture, streaming, distributed processing, analytics engineering, data quality, orchestration, governance, and observability.

## Architecture

```
Banking Systems → Kafka → Bronze (MinIO/S3) → Spark → Silver (Parquet)
                                                    ↓
                                              dbt → Gold (DuckDB)
                                                    ↓
                                           Analytics / CreditPulse ML
    Transverse: Great Expectations (quality) | Airflow (orchestration)
               OpenMetadata (governance) | Prometheus/Grafana (observability)
```

## Tech Stack

| Layer | Technology |
|-------|------------|
| Streaming | Apache Kafka |
| Storage | MinIO (S3-compatible), Delta Lake |
| Compute | Apache Spark (local mode) |
| Transformation | dbt (DuckDB target) |
| Quality | Great Expectations |
| Orchestration | Apache Airflow |
| Governance | OpenMetadata |
| Observability | Prometheus + Grafana |
| CI/CD | GitHub Actions |
| Containerization | Docker Compose |

## Key Features

- **Event-driven ingestion** — 3 Kafka topics (transactions, customers, loans) with deterministic producers
- **Medallion architecture** — Bronze (raw JSON) → Silver (clean Parquet) → Gold (business tables)
- **Data quality gates** — Great Expectations at every layer boundary
- **Delta Lake** — ACID transactions, time travel, schema evolution
- **dbt models** — 4 Gold tables: `customer_360`, `loan_portfolio_analysis`, `fraud_detection_metrics`, `customer_monthly_spending`
- **Airflow DAG** — Full pipeline orchestration with TaskGroups, SLA monitoring, backfill support
- **OpenMetadata** — Data catalog + lineage (Kafka → Bronze → Silver → Gold)
- **Prometheus/Grafana** — Service health dashboards
- **CI/CD** — GitHub Actions: lint (ruff, sqlfluff), dbt test, Spark tests, Docker build
- **E2E test** — 10-step integration test covering the full pipeline

## Quick Start

```bash
# 1. Clone
git clone https://github.com/SRMOHAMED01/LedgerLake.git
cd LedgerLake

# 2. Configure
cp ledgerlake/.env.example ledgerlake/.env

# 3. Start all services (16 containers)
make up

# 4. Verify
make health

# 5. Run pipeline
make run-pipeline
# Or trigger in Airflow UI: http://localhost:8081 (admin/admin)

# 6. Run tests
make test
```

## Access URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8081 | admin / admin |
| MinIO Console | http://localhost:9001 | ledgerlake / changeme |
| Spark Master | http://localhost:8080 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| OpenMetadata | http://localhost:8585 | admin / admin |

## Project Structure

```
LedgerLake/
├── Makefile                    # make up/down/test/lint/health/ci
├── README.md                   # This file
└── ledgerlake/
    ├── ingestion/              # Kafka producers + bronze_writer
    │   ├── producers/          # 3 event producers (transactions, customers, loans)
    │   ├── contracts/          # JSON schema validation
    │   └── bronze_writer.py    # Kafka → MinIO consumer
    ├── spark_jobs/             # Bronze → Silver (3 jobs, Delta Lake)
    │   ├── bronze_to_silver_*.py
    │   └── tests/              # 40+ unit tests
    ├── dbt_project/            # Silver → Gold (dbt models)
    │   ├── models/staging/     # 3 staging views
    │   └── models/gold/        # 4 Gold tables
    ├── data_quality/           # Great Expectations validation
    ├── orchestration/dags/     # Airflow DAGs (daily + backfill)
    ├── governance/             # OpenMetadata ingestion (4 connectors)
    ├── observability/          # Prometheus + Grafana
    ├── analytics/             # CreditPulse export, Metabase
    ├── tests/integration/     # E2E pipeline test
    ├── docker-compose.yml      # 16 services
    ├── .github/workflows/     # CI/CD pipeline
    └── .env.example           # Environment template
```

## What This Demonstrates

| Skill | Evidence |
|-------|----------|
| **Data Architecture** | Medallion (Bronze/Silver/Gold), Delta Lake, S3-compatible storage |
| **Streaming** | Event-driven Kafka, multi-topic, schema validation |
| **Distributed Processing** | Apache Spark, local mode (same API as production cluster) |
| **Analytics Engineering** | dbt models, tests, versioning, documentation |
| **Data Quality** | Great Expectations at every layer boundary |
| **Orchestration** | Airflow DAG with TaskGroups, SLA, backfill, retry logic |
| **Governance** | OpenMetadata catalog + data lineage |
| **Observability** | Prometheus/Grafana dashboards, health checks |
| **CI/CD** | GitHub Actions: 5 jobs (lint, SQL lint, dbt test, Spark tests, Docker build) |
| **Finance Domain** | Credit risk, fraud detection, customer analytics |

## Interview Talking Points

- "I built a complete financial data lakehouse from scratch, following Medallion architecture"
- "Great Expectations at both layer boundaries — data doesn't progress if invalid"
- "OpenMetadata for data lineage — full Kafka → Bronze → Silver → Gold traceability"
- "Airflow DAG with TaskGroups, SLA monitoring (3h), and backfill capability"
- "CI/CD with 5 jobs: Python lint, SQL lint, dbt test, Spark tests, Docker build"
- "Prometheus/Grafana dashboards for pipeline health monitoring"
- "Delta Lake for ACID guarantees and time travel on Silver layer"

## License

MIT

---

**Built by:** Mohamed SERRAR
**GitHub:** https://github.com/SRMOHAMED01/LedgerLake