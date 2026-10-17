# vstone-databricks-pipeline

End-to-end data pipeline on Databricks Free Edition using the medallion
architecture (Bronze → Silver → Gold), deployed with Databricks Asset Bundles.

## Repository layout

| Path | Purpose |
|---|---|
| `databricks.yml` | Bundle definition: variables (catalog, schema, volume) and `dev` / `prod` targets |
| `resources/` | Job and pipeline definitions (`*.yml`), included by `databricks.yml` |
| `src/notebooks/` | Databricks notebooks (chunking, ingestion, transformations) |
| `src/pipelines/` | Lakeflow Declarative Pipeline source code |
| `src/jobs/` | Python code run as job tasks |
| `tests/` | Unit tests (pytest + `pyspark.testing`) |
| `.github/workflows/` | GitHub Actions CI/CD |

## Getting started

1. Install the Databricks CLI and log in:
   `databricks auth login --host <workspace-url>`
2. Validate the bundle: `databricks bundle validate` (uses the `dev` target by default).

Credentials are never stored in this repo. Locally they live in `~/.databrickscfg`;
in CI they come from the `DATABRICKS_HOST` and `DATABRICKS_TOKEN` GitHub secrets.
