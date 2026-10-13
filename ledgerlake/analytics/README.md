# analytics/ — BI & ML Integration (Palier H)

## CreditPulse Integration

LedgerLake's Gold layer tables directly feed CreditPulse's XGBoost credit scoring model.

### Data Flow

```
LedgerLake Gold Tables          CreditPulse
─────────────────────          ─────────────
customer_360           ──►    Features: age, country, total_transactions,
                              average_transaction, credit_score, risk_level,
                              active_loans_count, defaults_count

loan_portfolio_analysis ──►  Labels: default_probability, risk_category,
                              loan_amount, duration_months, loan_status
```

### Export for CreditPulse

```bash
# Export Gold tables as Parquet for CreditPulse training
cd ledgerlake/dbt_project
dbt run --profiles-dir .

# Using DuckDB CLI
duckdb /tmp/duckdb/ledgerlake.duckdb -c "
  COPY customer_360 TO 's3://analytics/creditpulse/customer_360.parquet' (FORMAT PARQUET);
  COPY loan_portfolio_analysis TO 's3://analytics/creditpulse/loan_portfolio.parquet' (FORMAT PARQUET);
"
```

### CreditPulse Training Script

```python
# CreditPulse/train.py (simplified)
import duckdb
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split

# Load from LedgerLake
conn = duckdb.connect("s3://analytics/creditpulse/*.parquet")
customers = conn.execute("SELECT * FROM customer_360").df()
loans = conn.execute("SELECT * FROM loan_portfolio_analysis").df()

# Feature engineering
features = customers.merge(loans, on="customer_id", how="left")
X = features[["age", "total_transactions", "average_transaction", 
              "credit_score", "active_loans_count", "loan_amount", "duration_months"]]
y = (features["loan_status"] == "DEFAULTED").astype(int)

# Train
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
model = xgb.XGBClassifier(n_estimators=100, max_depth=5)
model.fit(X_train, y_train)

# Save model
model.save_model("creditpulse_model.json")
```

---

## Metabase Dashboard (Free BI)

### Quick Start

```bash
# Add to docker-compose.yml
metabase:
  image: metabase/metabase:v0.49
  ports:
    - "3001:3000"
  environment:
    - MB_DB_TYPE=postgres
    - MB_DB_DBNAME=metabase
    - MB_DB_PORT=5432
    - MB_DB_USER=metabase
    - MB_DB_PASS=metabase
    - MB_DB_HOST=postgres-metabase
  depends_on:
    - postgres-metabase

postgres-metabase:
  image: postgres:13
  environment:
    - POSTGRES_USER=metabase
    - POSTGRES_PASSWORD=metabase
    - POSTGRES_DB=metabase
  volumes:
    - metabase_data:/var/lib/postgresql/data
```

### Dashboards to Build

| Dashboard | Questions | Gold Tables |
|-----------|-----------|-------------|
| **Risk Overview** | Default rate by segment, portfolio risk distribution | `loan_portfolio_analysis`, `customer_360` |
| **Fraud Monitoring** | Daily fraud alerts, high-risk customers | `fraud_detection_metrics` |
| **Customer Analytics** | Monthly spend trends, segment breakdown | `customer_monthly_spending`, `customer_360` |
| **Operational** | Pipeline health, data freshness, volumes | All + Airflow metrics |

---

## Power BI / Tableau

### Connection (DuckDB)

```python
# Python connector for BI tools
import duckdb
conn = duckdb.connect("s3://analytics/gold/*.parquet")

# Or use ODBC/JDBC with DuckDB
# DuckDB ODBC driver available at: https://duckdb.org/docs/api/odbc
```

### Recommended Views

```sql
-- Create BI-friendly views in dbt
-- models/gold/bi_views.sql

CREATE VIEW bi_customer_risk AS
SELECT 
  customer_id,
  age,
  country,
  customer_segment,
  total_transactions,
  average_transaction,
  credit_score,
  risk_level,
  active_loans_count,
  defaults_count,
  CASE WHEN defaults_count > 0 THEN 1 ELSE 0 END AS has_defaulted
FROM customer_360;

CREATE VIEW bi_loan_performance AS
SELECT
  loan_id,
  customer_id,
  loan_amount,
  duration_months,
  loan_status,
  default_probability,
  risk_category,
  CASE WHEN loan_status IN ('DEFAULTED', 'REJECTED') THEN 1 ELSE 0 END AS is_bad_loan
FROM loan_portfolio_analysis;
```

---

## Data Freshness SLA

| Layer | Freshness Target | Monitoring |
|-------|------------------|------------|
| Bronze | < 5 min after event | Kafka lag, MinIO file timestamps |
| Silver | < 30 min after Bronze | Spark job duration, file counts |
| Gold | < 1 hour after Silver | dbt run duration, row counts |

---

## Files

```
analytics/
├── README.md                    # This file
├── creditpulse/
│   ├── export_gold.py           # Export script for CreditPulse
│   └── feature_mapping.md       # Feature ↔ Gold column mapping
├── metabase/
│   ├── dashboards/              # Metabase dashboard JSON exports
│   └── questions/               # Saved questions
└── powerbi/
    └── ledgerlake.pbit          # Power BI template
```