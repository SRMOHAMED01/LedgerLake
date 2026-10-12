# governance/ — OpenMetadata (Palier F)

## Architecture

OpenMetadata is deployed via Docker Compose with 3 ingestion connectors:

| Connector | Service Name | Purpose |
|-----------|--------------|---------|
| MinIO Bronze | `ledgerlake-minio` | Catalog raw Bronze files (JSON) |
| MinIO Silver | `ledgerlake-minio-silver` | Catalog cleaned Silver files (Parquet) |
| DuckDB Gold | `ledgerlake-gold-duckdb` | Catalog Gold tables with full schema |
| Airflow | `ledgerlake-airflow` | Capture DAG/task lineage |

## Quick Start

```bash
# 1. Start all services
docker-compose up -d

# 2. Wait for services to be healthy (2-3 min)
docker-compose ps

# 3. Run ingestion workflows
cd ledgerlake
chmod +x governance/openmetadata-ingestion/run-ingestion.sh
./governance/openmetadata-ingestion/run-ingestion.sh

# 4. Open OpenMetadata UI
# http://localhost:8585 (admin / admin)
```

## What You'll See

1. **Services → Databases** → `ledgerlake-gold-duckdb` → `main` schema
   - `customer_360` (with PII tag)
   - `customer_monthly_spending`
   - `fraud_detection_metrics`
   - `loan_portfolio_analysis`

2. **Services → Storage** → `ledgerlake-minio` / `ledgerlake-minio-silver`
   - Bronze: `transactions/`, `customers/`, `loans/` (partitioned by date)
   - Silver: same structure, Parquet format

3. **Services → Pipelines** → `ledgerlake-airflow` → `financial_daily_pipeline`
   - Tasks: `run_producers`, `run_bronze_writer`, `extract_and_validate_bronze`, etc.
   - Lineage: DAG → task dependencies

4. **Table Lineage** (click any Gold table → Lineage tab)
   - `customer_360` → `stg_customers` + `stg_transactions` + `stg_loans`
   - → Silver tables → Bronze files → Kafka topics

## Manual Enrichment (via UI or API)

After ingestion, enrich each Gold table:

| Table | Owner | Description | Tags |
|-------|-------|-------------|------|
| `customer_360` | Risk Department | 360° client profile for risk scoring | `PII`, `Risk`, `Analytics` |
| `loan_portfolio_analysis` | Credit Risk | Loan portfolio risk analysis | `Risk`, `Credit`, `Regulatory` |
| `fraud_detection_metrics` | Fraud Team | Heuristic fraud indicators | `Fraud`, `Security`, `Monitoring` |
| `customer_monthly_spending` | Marketing | Monthly spend aggregates per client | `Analytics`, `Marketing` |

**API example:**
```bash
curl -X PATCH "http://localhost:8585/api/v1/tables/name/customer_360?fields=owner,tags" \
  -H "Content-Type: application/json" \
  -d '{"owner": {"name": "Risk Department"}, "tags": [{"label": "PII"}, {"label": "Risk"}]}'
```

## Files

```
governance/openmetadata-ingestion/
├── minio-bronze.yaml       # Bronze layer S3 connector
├── minio-silver.yaml       # Silver layer S3 connector
├── duckdb-gold.yaml        # Gold layer DuckDB connector
├── airflow-lineage.yaml    # Airflow DAG lineage connector
└── run-ingestion.sh        # Runner script
```

## Production Notes

- **DuckDB**: Single-file DB works for demo. Use PostgreSQL in production.
- **Auth**: Currently `no-auth`. Enable JWT/OIDC for production.
- **Scheduling**: Run ingestion daily via Airflow DAG or cron.
- **Lineage**: Airflow connector captures task-level lineage. For column-level, add OpenMetadata SQL parser.
- **Secrets**: Store credentials in OpenMetadata secrets manager, not in YAML.