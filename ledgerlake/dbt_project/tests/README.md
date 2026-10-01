# Gold model tests

Gold model tests are defined in `models/gold/schema.yml` and in the singular SQL
tests in this directory. Generic tests cover key uniqueness, required values,
and allowed risk/status categories. Singular tests enforce measures and scores
within their expected ranges and check the composite grain of monthly spending.

Run from the `ledgerlake/dbt_project` directory after configuring DuckDB and
making the Silver Parquet sources available:

```bash
dbt test --target dev
```

The risk and fraud outputs are deliberately simple demo heuristics; these tests
check their shape and bounds, not their predictive quality or suitability for
real financial decisions.
