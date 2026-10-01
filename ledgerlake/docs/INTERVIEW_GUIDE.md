# LedgerLake — Project and Internship Interview Guide

This guide explains what the repository actually implements, how its pieces fit together, why the tools were chosen, and how to discuss it honestly in an interview. The project is a financial-data-lakehouse portfolio project that is **partly implemented and partly a roadmap**. Be clear about that distinction: it demonstrates real ingestion and transformation code, while several platform features are still planned.

## 1. The project in one minute

LedgerLake is a small, event-driven data platform for simulated financial data. It generates customer, transaction, and loan events, publishes them to Kafka, stores the original messages in MinIO as a raw Bronze layer, and uses Apache Spark to clean and validate them into Parquet Silver datasets. dbt then defines business-oriented Gold models for customer analytics, loan risk, spending, and fraud indicators.

The design follows the **Medallion architecture**:

- **Bronze:** retain the received event payloads as the replayable raw record.
- **Silver:** validate, type, deduplicate, standardize, and separate rejected records.
- **Gold:** express business-facing metrics and datasets in SQL.

The repository also contains a runnable Great Expectations validation CLI, plus plans/configuration stubs for Airflow, OpenMetadata, Prometheus/Grafana, and BI/ML. Those pieces are not yet integrated into a finished, automated end-to-end platform.

## 2. Architecture and data flow

```text
Customer / transaction / loan producer scripts
                   │ JSON events, one topic per event family
                   ▼
                Kafka
                   │ bronze_writer consumes and batches messages
                   ▼
      MinIO S3-compatible object storage
      Bronze: raw JSON/JSONL objects by entity and date
                   │ Spark reads Bronze history
                   ▼
       Silver Parquet datasets + rejected datasets
                   │ dbt reads Silver sources and builds SQL models
                   ▼
    Gold: customer, loan, fraud, monthly-spending analytics
                   │ planned consumers
                   ▼
             BI dashboards / CreditPulse
```

The end-to-end diagram describes the intended design. In the checked-in implementation, the producers, Bronze writer, Spark jobs, dbt models/configuration, offline tests, and a Great Expectations CLI for validating Spark DataFrames exist. The Airflow DAG is currently a specification in comments; Great Expectations is not yet wired into Airflow, while OpenMetadata deployment, Grafana dashboards, and BI/CreditPulse integration are not implemented. Docker Compose launches only ZooKeeper, Kafka, and MinIO at present.

## 3. Tools and why they are used

| Tool / concept | Role in LedgerLake | Why it fits | Current status |
|---|---|---|---|
| **Kafka** | Carries customer, transaction, and loan events on separate topics. | Decouples event producers from storage/processing consumers; buffers events and allows consumers to resume from offsets. | Compose configuration and producer/writer code exist. |
| **MinIO / S3 API** | Stores Bronze and is the configured object-store target for Spark/dbt. | Runs locally and uses an S3-compatible interface, making the prototype portable to an S3-style object store. | Compose service and code configuration exist; startup requires environment variables. |
| **Bronze/Silver/Gold (Medallion)** | Separates raw history, cleaned data, and business models. | Makes processing traceable and allows transformations to be rebuilt from raw input. | Bronze and Silver code exists; Gold SQL models exist. |
| **Apache Spark / PySpark** | Reads Bronze JSON, validates and transforms records, writes Silver Parquet. | DataFrame operations can scale beyond a single in-memory pandas process; Spark can run locally for development and on a cluster later. | Jobs use local Spark mode; a Spark cluster is not included in Compose. |
| **Parquet** | Columnar Silver storage. | Efficient for analytical scans and typed columns; supports date partitioning. | Written by Spark jobs. |
| **dbt + DuckDB** | Organizes Silver source definitions and SQL staging/Gold models. | Keeps analytical business logic in version-controlled SQL, exposes dependencies, and supports local analytical workflows. DuckDB is lightweight for a portfolio demo. | Project, profile, sources, and models exist; dbt execution is not orchestrated. |
| **JSON Schema** | Defines versioned event contracts for the three event families. | Catches malformed or incompatible events near their source and makes event shape explicit. | Contracts and producer-side validation exist. |
| **Great Expectations** | CLI quality gates for Bronze inputs and Silver/Gold datasets. | Adds reusable assertions at data-layer boundaries; failures return a non-zero exit code for a scheduler to act on. | Spark DataFrame checks are implemented in `data_quality/great_expectations/quality_checks.py`; automatic Airflow integration and persisted GX validation results remain future work. Spark jobs also validate and quarantine rows. |
| **Airflow** | Intended scheduler and dependency manager for daily execution. | Makes multi-step pipelines repeatable, observable, and selectively rerunnable. | DAG file contains a commented specification, not executable tasks. |
| **OpenMetadata** | Intended data catalog, ownership, and lineage. | Helps users discover datasets, definitions, owners, and upstream/downstream dependencies. | Documentation only. |
| **Prometheus / Grafana** | Intended metrics collection and dashboards. | Prometheus scrapes operational metrics; Grafana visualizes them. | Prometheus config is partial; dashboards/exporters are not present. |
| **Docker Compose** | Local service orchestration. | Gives a reproducible way to bring up supporting services without installing each one directly. | Only ZooKeeper, Kafka, and MinIO are defined currently. |
| **Python `unittest` / mocks** | Offline regression tests for ingestion and Spark transformations. | Tests important behavior without requiring Kafka and MinIO for ingestion tests; local Spark tests exercise data logic. | Test files exist; run them in a configured environment. |

## 4. File-by-file tour

### Repository-level documentation

- [README.md](../../README.md) — French portfolio introduction: the problem, intended solution, architecture, skills demonstrated, and relationship to CreditPulse. It is an overview, not a complete implementation status report.
- [README.md](../README.md) — Project directory map, high-level pipeline, and pointers to the construction guide. Its quick-start mentions `.env.example`; that file is not present in the current workspace, so do not assume the quick-start is ready as written.
- [GUIDE_COMPLET.md](GUIDE_COMPLET.md) — French staged build plan (Paliers A–H). Useful for the intended roadmap, but some passages are stale: for example, the Bronze writer is described as future work even though code and tests now exist.
- [INTERVIEW_GUIDE.md](INTERVIEW_GUIDE.md) — This English explanation and interview preparation reference.

### Ingestion and event contracts

- [ingestion/README.md](../ingestion/README.md) — Documents producer usage, scenarios, topic names, Bronze writer batching/recovery behavior, and offline test commands. Its at-least-once guarantee is important to understand.
- [ingestion/requirements.txt](../ingestion/requirements.txt) — Python packages for ingestion: `confluent-kafka` for Kafka, `boto3` for S3-compatible MinIO, `python-dotenv` for local configuration, and `jsonschema` for event validation.
- [producers/event_factory.py](../ingestion/producers/event_factory.py) — Shared logic for deterministic customer registries, event creation, schema validation, Kafka producer settings, and publishing. A seed makes test/demo data reproducible. Loan events maintain an ordered lifecycle; transaction scenarios can deliberately create duplicates, conflicts, late events, or invalid values for testing.
- [producers/customer_producer.py](../ingestion/producers/customer_producer.py) — Creates customer account/profile events and sends them to `customers-events`.
- [producers/transaction_producer.py](../ingestion/producers/transaction_producer.py) — Creates transactions linked to the shared deterministic customer registry and sends them to `transactions-events`. Selected invalid scenarios intentionally bypass producer validation so downstream handling can be tested.
- [producers/loan_producer.py](../ingestion/producers/loan_producer.py) — Emits ordered loan lifecycle events to `loan-events`, linked to the same customer registry.
- [contracts/customer.v1.schema.json](../ingestion/contracts/customer.v1.schema.json) — Version 1 customer event contract: required identity/event fields, age, country, and segment restrictions.
- [contracts/transaction.v1.schema.json](../ingestion/contracts/transaction.v1.schema.json) — Version 1 transaction contract: IDs, positive amount, currency/type allow-lists, merchant, country, and timestamps.
- [contracts/loan.v1.schema.json](../ingestion/contracts/loan.v1.schema.json) — Version 1 loan contract: event/lifecycle type, IDs, positive amount, duration, status, and timestamps.
- [ingestion/bronze_writer.py](../ingestion/bronze_writer.py) — Kafka consumer that batches raw messages by topic and partition, then writes newline-delimited payloads to MinIO paths partitioned by date. It commits a partition’s Kafka offset only after its object write succeeds, with automatic commits disabled. This gives **at-least-once**, not exactly-once: an object may be written and then replayed if the process fails before the offset commit. Spark deduplication is therefore still necessary.
- [tests/test_event_factory.py](../ingestion/tests/test_event_factory.py) — Offline tests for deterministic generation, contracts, customer relationships, event IDs, loan lifecycle order, scenarios, serialization, and Kafka delivery-error handling.
- [tests/test_bronze_writer.py](../ingestion/tests/test_bronze_writer.py) — Mock-based tests for write-before-commit ordering, partition-specific commits, payload preservation, failures/replay, flush behavior, rebalance, tombstones, and bucket behavior.

### Storage conventions

- [bronze/README.md](../bronze/README.md) — Describes the intended raw object naming/partitioning convention and immutability principle. It is documentation; the actual Bronze objects live in MinIO, not in this repository folder.
- [silver/README.md](../silver/README.md) — Describes the intended cleaned Silver datasets and Parquet layout. It is also a convention document, not the data itself.

### Spark transformation layer

- [spark_jobs/README.md](../spark_jobs/README.md) — Explains full-history rebuilds, replay handling, non-atomic overwrite limitations, and how to run the tests.
- [spark_jobs/requirements.txt](../spark_jobs/requirements.txt) — Pins PySpark and `python-dotenv` versions used by the Spark jobs.
- [bronze_to_silver_transactions.py](../spark_jobs/bronze_to_silver_transactions.py) — Reads raw transactions, removes exact row replays, detects conflicting records sharing a transaction ID, validates required values/types, standardizes country labels, derives date/region fields, writes accepted rows to partitioned Parquet, and sends rejected records to a quarantine path with a reason. It checks reconciliation counts and refuses empty/all-rejected input rather than replacing good Silver data.
- [bronze_to_silver_customers.py](../spark_jobs/bronze_to_silver_customers.py) — Applies the analogous checks to customer events: types age/timestamps, detects conflicting event IDs, standardizes country, derives region/segment/date fields, and writes accepted and rejected datasets.
- [bronze_to_silver_loans.py](../spark_jobs/bronze_to_silver_loans.py) — Validates loan IDs, customer IDs, amounts, duration, status, and timestamps; detects exact replays and conflicting event IDs; writes accepted and quarantined Parquet outputs.
- [tests/test_transactions.py](../spark_jobs/tests/test_transactions.py) — Tests transaction deduplication/rebuilds, conflicting IDs, rejected reasons, totals, transformations, and failure safety.
- [tests/test_customers.py](../spark_jobs/tests/test_customers.py) — Tests customer replay handling, validation/quarantine, conflicts, derived fields, and preserving existing Silver data on invalid input.
- [tests/test_loans.py](../spark_jobs/tests/test_loans.py) — Tests loan replay handling, validation/quarantine, conflicts, derived fields, and rebuild safety.

A design trade-off to mention: the Spark jobs rebuild Silver from retained Bronze history using overwrite. This is simple and repeatable for a small demo, but overwrite is not atomic and assumes one writer at a time. The Spark README identifies a transactional table format such as Iceberg, Delta, or Hudi as a future improvement for safer concurrent/production updates.

### dbt analytics layer

- [dbt_project/README.md](../dbt_project/README.md) — Briefly describes Silver-to-Gold dbt transformations.
- [dbt_project.yml](../dbt_project/dbt_project.yml) — Names/configures the dbt project and sets staging models to views and Gold models to tables.
- [profiles.yml](../dbt_project/profiles.yml) — Configures a local DuckDB target and S3-compatible access for Silver Parquet. Keep credentials in environment configuration in a real deployment; do not expose secrets in an interview or commit them.
- [models/sources.yml](../dbt_project/models/sources.yml) — Declares customer, transaction, and loan Silver datasets as dbt sources, including their external Parquet locations.
- [models/staging/stg_customers.sql](../dbt_project/models/staging/stg_customers.sql) — Selects and renames customer fields into a consistent staging interface. The comment notes that a latest-record/SCD policy matters; Gold currently selects the latest customer event.
- [models/staging/stg_transactions.sql](../dbt_project/models/staging/stg_transactions.sql) — Provides a clean naming layer for transaction fields, including transaction type, location, and timestamp.
- [models/staging/stg_loans.sql](../dbt_project/models/staging/stg_loans.sql) — Renames loan columns and exposes event time as loan update time; Gold chooses the latest status per loan.
- [models/gold/customer_360.sql](../dbt_project/models/gold/customer_360.sql) — Joins latest customer state to transaction and loan aggregates; produces spend/count features, a rule-based risk level, and a simple illustrative credit score.
- [models/gold/customer_monthly_spending.sql](../dbt_project/models/gold/customer_monthly_spending.sql) — Groups customer transactions by month and calculates spending and transaction count.
- [models/gold/fraud_detection_metrics.sql](../dbt_project/models/gold/fraud_detection_metrics.sql) — Creates heuristic customer-level suspicious-operation and fraud-score indicators from unusually large payments and number of countries. This is a demonstration heuristic, **not a production fraud model**.
- [models/gold/loan_portfolio_analysis.sql](../dbt_project/models/gold/loan_portfolio_analysis.sql) — Selects the latest event for each loan and assigns illustrative default probabilities/risk categories using simple status, amount, and duration rules. These are not calibrated probabilities or credit decisions.
- [models/gold/schema.yml](../dbt_project/models/gold/schema.yml) — Documents selected Gold models and configures some generic uniqueness/not-null tests.
- [tests/README.md](../dbt_project/tests/README.md) — Explains the implemented Gold model tests and how to run them; these check required fields, keys, categories, metric bounds, and monthly aggregation grain.

### Quality, orchestration, governance, monitoring, and BI roadmap

- [data_quality/great_expectations/quality_checks.py](../data_quality/great_expectations/quality_checks.py) — Great Expectations CLI for Spark DataFrames, with Bronze checks for transactions/customers/loans, Silver transaction checks before dbt, and structural checks for exported Gold datasets. It fails with a non-zero exit status when expectations fail; it is not yet called by Airflow.
- [data_quality/great_expectations/requirements.txt](../data_quality/great_expectations/requirements.txt) — Great Expectations Spark extra and the Spark dependencies needed by the CLI.
- [data_quality/great_expectations/README.md](../data_quality/great_expectations/README.md) — Installation instructions, example commands, coverage, and the note that dbt Gold tables must be exported to Parquet before this Spark-based CLI can validate them directly.
- [orchestration/dags/financial_daily_pipeline.py](../orchestration/dags/financial_daily_pipeline.py) — Describes the intended Airflow task order (Bronze checks, parallel Spark jobs, Silver checks, dbt run/test, notification), but presently contains comments rather than an executable DAG.
- [governance/README.md](../governance/README.md) — Roadmap for OpenMetadata connectors, table ownership, sensitivity tags, and lineage. OpenMetadata is not configured in the current Compose file.
- [observability/prometheus/prometheus.yml](../observability/prometheus/prometheus.yml) — Partial Prometheus scrape configuration for anticipated Kafka and Airflow exporters. Those exporter services are not currently defined in Compose.
- [observability/grafana/dashboards/README.md](../observability/grafana/dashboards/README.md) — Describes intended dashboard content; dashboard definitions are not implemented.
- [analytics/README.md](../analytics/README.md) — Roadmap for a BI dashboard and for feeding Gold features to the separate CreditPulse project. Neither connection is implemented here yet.
- [docker-compose.yml](../docker-compose.yml) — Defines ZooKeeper, Kafka, and MinIO. Spark, Airflow, OpenMetadata, Prometheus, and Grafana are explicitly TODO. MinIO reads a `.env` file, so configure required values before starting the stack.

## 5. Important engineering decisions to explain

### Why keep Bronze raw?
Raw data is the source of truth for replay and debugging. If a cleaning rule is wrong, Silver and Gold can be rebuilt without losing the original event. The writer therefore does not parse or transform payload contents.

### Why use both schema validation and Spark validation?
Producer-side JSON Schema validation catches malformed events early. It is not enough by itself: data may come from other producers, old versions, or deliberate bad-data test scenarios. Spark validates again at the layer boundary and records rejection reasons rather than silently dropping bad data.

### Why at-least-once delivery?
The Bronze writer writes the object before committing the Kafka offset. Committing first could lose a message if storage failed. Writing first may cause a replay if the process fails before commit, so duplicates are possible; this safer failure mode is at-least-once and downstream deduplication handles it. Exactly-once across Kafka and object storage would require additional transactional/idempotent storage design.

### Why separate accepted and rejected records?
Quarantine preserves visibility into bad inputs and makes data-quality issues measurable. It avoids hiding problems by dropping invalid rows without explanation.

### Why use dbt for Gold?
dbt lets the team express analytics logic in SQL, create a model dependency graph with `source()` and `ref()`, configure materializations, and add tests/documentation. Spark focuses on raw-to-clean processing; dbt focuses on business definitions.

### What are the main current limitations?
1. The full orchestration and service stack is not wired up; Compose starts only Kafka dependencies and MinIO.
2. Great Expectations validation is runnable manually, but Airflow integration and persisted validation results are not implemented; OpenMetadata, Grafana dashboards, and BI/CreditPulse are roadmap items or partial config.
3. Gold risk and fraud values are hand-written heuristics for demonstration, not validated financial or ML outputs.
4. Spark uses local mode and full-history overwrite, rather than a distributed cluster and atomic incremental table updates.
5. Bronze writer delivery is at-least-once; duplicate/replay handling remains necessary downstream.
6. The repository documentation has some stale roadmap passages, so describe the code that exists rather than repeating every aspiration as a completed feature.

## 6. A ready-to-use interview answer

> “LedgerLake is my financial data lakehouse project. I used a Medallion design: Python producers create versioned customer, transaction, and loan events and publish them to separate Kafka topics. A Kafka consumer batches the untouched payloads into date-partitioned MinIO Bronze objects and commits offsets only after a successful write, so the delivery guarantee is at-least-once. PySpark rebuilds Silver from Bronze: it casts and validates fields, removes exact replays, quarantines invalid rows with reasons, detects conflicting IDs, standardizes values, and writes partitioned Parquet. dbt then builds staging views and Gold SQL models for customer 360, loan portfolio, fraud indicators, and monthly spending. I added offline tests around ingestion failure/replay behavior and Spark transformations. I also added a manual Great Expectations CLI for validating Bronze and downstream datasets, but it is not yet part of an automated Airflow DAG. OpenMetadata, monitoring dashboards, and BI integration remain planned. The biggest production improvement I would make is moving Silver writes to an atomic table format such as Iceberg or Delta and then implementing orchestration and monitored quality gates.”

Adjust the last sentences to match what you have personally run and verified. Do not say the entire pipeline runs with one command unless you have actually demonstrated it.

## 7. Likely interview questions

**What happens if MinIO is unavailable while Bronze is being written?**  The writer propagates the storage error and does not commit the corresponding partition offset. After recovery/restart, Kafka can replay it, provided the event is still retained. This protects against loss but can create a duplicate object in an uncertain write/commit failure, hence at-least-once semantics.

**How do you distinguish a duplicate from a conflict?**  An identical replay of a full record is collapsed. If the same business identifier appears with different record content, the Spark job quarantines the conflicting versions instead of choosing one arbitrarily.

**Why JSON in Bronze and Parquet in Silver?**  Bronze preserves the original event format for audit and replay. Silver is typed and queried analytically, so Parquet's columnar layout is more suitable and compact for analytical reads.

**Is the fraud score a real model?**  No. It is a transparent heuristic for demonstrating feature engineering and a downstream Gold model. A real model would require trusted labels, feature validation, backtesting, calibration, monitoring, and governance.

**What would you build next?**  First add end-to-end integration tests and environment/configuration documentation; then implement executable Airflow tasks and quality gates; use transactional Iceberg/Delta tables for safer incremental writes; add metrics/alerts; and finally publish Gold datasets to a BI tool or CreditPulse.

## 8. Test commands documented by the project

Run from the repository root after installing the appropriate requirements and configuring the local services/environment. These are project-documented commands; the presence of a command does not mean it has already been run in your environment.

- Ingestion unit tests: `python -B -m unittest discover -s ledgerlake/ingestion/tests -v`
- Spark transformation tests: `python -B -m unittest discover -s ledgerlake/spark_jobs/tests -v`
- Reproducible producer examples are in [ingestion/README.md](../ingestion/README.md).

For an interview, be prepared to say which commands you personally ran, what passed, and what setup they required.
