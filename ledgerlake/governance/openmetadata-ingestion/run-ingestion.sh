#!/bin/bash
# OpenMetadata Ingestion Runner for LedgerLake
# Run this from the ledgerlake/ directory after all services are up

set -e

echo "=========================================="
echo "LedgerLake OpenMetadata Ingestion"
echo "=========================================="

# Wait for OpenMetadata to be ready
echo "Waiting for OpenMetadata API..."
until curl -s http://localhost:8585/api/v1/system/version > /dev/null; do
  echo "  OpenMetadata not ready, waiting 10s..."
  sleep 10
done
echo "OpenMetadata is ready!"

# Install openmetadata-ingestion if not present
if ! command -v metadata &> /dev/null; then
  echo "Installing openmetadata-ingestion..."
  pip install "openmetadata-ingestion[airflow,duckdb,s3]" --quiet
fi

# Run ingestion workflows
WORKFLOWS=(
  "governance/openmetadata-ingestion/minio-bronze.yaml"
  "governance/openmetadata-ingestion/minio-silver.yaml"
  "governance/openmetadata-ingestion/duckdb-gold.yaml"
  "governance/openmetadata-ingestion/airflow-lineage.yaml"
)

for workflow in "${WORKFLOWS[@]}"; do
  echo ""
  echo "------------------------------------------"
  echo "Running: $workflow"
  echo "------------------------------------------"
  if python -m metadata.cmd.ingest --config "$workflow"; then
    echo "✓ Success: $workflow"
  else
    echo "✗ Failed: $workflow (continuing...)"
  fi
done

echo ""
echo "=========================================="
echo "All ingestion workflows completed"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Open OpenMetadata UI: http://localhost:8585"
echo "2. Login: admin / admin"
echo "3. Explore: Services -> Databases -> ledgerlake-gold-duckdb"
echo "4. Check lineage: Tables -> customer_360 -> Lineage tab"