# Makefile — LedgerLake common operations
# Usage: make <target>

.PHONY: help up down logs ps clean test lint dbt-run dbt-test ingestion

# Default target
help:
	@echo "LedgerLake — Financial Data Lakehouse"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make up           Start all services"
	@echo "  make down         Stop all services"
	@echo "  make restart      Restart all services"
	@echo "  make logs         Follow logs (use SERVICE=name for specific)"
	@echo "  make ps           Show service status"
	@echo "  make clean        Stop and remove volumes (DATA LOSS!)"
	@echo ""
	@echo "Development:"
	@echo "  make test         Run all tests (Python + Spark + dbt)"
	@echo "  make lint         Run linters (ruff + sqlfluff)"
	@echo "  make dbt-run      Run dbt models"
	@echo "  make dbt-test     Run dbt tests"
	@echo "  make ingestion    Run OpenMetadata ingestion"
	@echo ""
	@echo "Access URLs:"
	@echo "  Airflow:        http://localhost:8081 (admin/admin)"
	@echo "  MinIO Console:  http://localhost:9001 (ledgerlake/changeme)"
	@echo "  Spark Master:   http://localhost:8080"
	@echo "  Prometheus:     http://localhost:9090"
	@echo "  Grafana:        http://localhost:3000 (admin/admin)"
	@echo "  OpenMetadata:   http://localhost:8585 (admin/admin)"

# Infrastructure
up:
	docker-compose -f ledgerlake/docker-compose.yml up -d

down:
	docker-compose -f ledgerlake/docker-compose.yml down

restart: down up

logs:
	docker-compose -f ledgerlake/docker-compose.yml logs -f $(SERVICE)

ps:
	docker-compose -f ledgerlake/docker-compose.yml ps

clean:
	docker-compose -f ledgerlake/docker-compose.yml down -v --remove-orphans

# Development
test: lint spark-tests dbt-test

lint:
	cd ledgerlake && ruff check ingestion/ spark_jobs/ --output-format=github
	cd ledgerlake && ruff format --check ingestion/ spark_jobs/
	cd ledgerlake && sqlfluff lint dbt_project/models

spark-tests:
	cd ledgerlake/spark_jobs && python -m pytest tests/ -v --tb=short

dbt-run:
	cd ledgerlake/dbt_project && dbt run --profiles-dir .

dbt-test:
	cd ledgerlake/dbt_project && dbt test --profiles-dir .

ingestion:
	cd ledgerlake && ./governance/openmetadata-ingestion/run-ingestion.sh

# Pipeline execution
run-pipeline:
	cd ledgerlake && python -m ingestion.producers.transaction_producer --count 1000
	cd ledgerlake && python -m ingestion.producers.customer_producer --count 1000
	cd ledgerlake && python -m ingestion.producers.loan_producer --count 1000
	cd ledgerlake && python -m ingestion.bronze_writer
	cd ledgerlake/spark_jobs && python bronze_to_silver_transactions.py
	cd ledgerlake/spark_jobs && python bronze_to_silver_customers.py
	cd ledgerlake/spark_jobs && python bronze_to_silver_loans.py
	cd ledgerlake/dbt_project && dbt run --profiles-dir .
	cd ledgerlake/dbt_project && dbt test --profiles-dir .

# Health checks
health:
	@echo "Checking service health..."
	@curl -sf http://localhost:8081/health && echo "Airflow: OK" || echo "Airflow: FAIL"
	@curl -sf http://localhost:9000/minio/health/live && echo "MinIO: OK" || echo "MinIO: FAIL"
	@curl -sf http://localhost:8080 && echo "Spark Master: OK" || echo "Spark Master: FAIL"
	@curl -sf http://localhost:9090/-/healthy && echo "Prometheus: OK" || echo "Prometheus: FAIL"
	@curl -sf http://localhost:3000/api/health && echo "Grafana: OK" || echo "Grafana: FAIL"
	@curl -sf http://localhost:8585/api/v1/system/version && echo "OpenMetadata: OK" || echo "OpenMetadata: FAIL"

# CI simulation
ci: lint spark-tests dbt-test
	docker-compose -f ledgerlake/docker-compose.yml build --parallel