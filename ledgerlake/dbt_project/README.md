# dbt_project/ — Modèles Silver → Gold

Transforms Silver Parquet sources into business-facing Gold tables using dbt
and DuckDB. Install the adapter with `pip install -r requirements.txt`, make the
Silver datasets available at the locations configured in `models/sources.yml`,
then run `dbt build --target dev` from this directory to build models and run
their tests. `dbt test --target dev` reruns the tests only.

Risk and fraud outputs are illustrative rule-based metrics, not production
credit or fraud decisions.
