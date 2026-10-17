# CLAUDE.md – VStone Databricks Pipeline

This file gives Claude Code the context and rules for this repository. Read it at the start of every session.

## About the developer
- Aman, newly joined data engineer, **new to Databricks and data engineering**.
- This is a training project that must be built **individually**. Aman must understand and be able to explain every line in evaluations.
- Always **explain why** each change is made, in simple terms. Prefer small, reviewable steps over large changes.
- When the spec is ambiguous or contradictory, **stop and flag it** and suggest confirming with the trainer.

## Working rules for Claude Code
- **Never** commit to `main` or `dev` directly. Work on the correct feature branch and open a pull request into `dev`. Aman reviews and merges.
- **Never** write, print, or commit secrets (tokens, passwords, hosts with tokens). Credentials live only in `~/.databrickscfg` (local) and GitHub Actions secrets `DATABRICKS_HOST` / `DATABRICKS_TOKEN`.
- Keep `.gitignore` covering: `.databrickscfg`, `.env`, `*.token`, `.databricks/`, `__pycache__/`, `.venv/`.
- Ask before: deploying (`databricks bundle deploy`), running jobs, deleting files/tables, or changing anything in the Databricks workspace.
- Prefer `databricks bundle validate` before any deploy. Deploy to the `dev` target unless told otherwise.
- Write a clear PR description: what changed, why, how it was tested, and anything Aman should learn from it.

## Project summary
End-to-end pipeline on **Databricks Free Edition** using **medallion architecture (Bronze → Silver → Gold)**, ingesting CSV, JSON and XML, for business analytics.
- Unity Catalog governance; Row Level Security, Column Level Security, Dynamic Data Masking.
- Orchestration via Databricks Jobs; deployment via **Databricks Asset Bundles (DABs)**.
- Unit tests, monitoring, and sanity checks for every ingestion.
- All dataset files (except metadata files) must flow all the way to Gold.

### Environment constraints
- **Databricks Free Edition only** (not the trial). **Serverless compute only** (no managed clusters).
- "DLT" in the spec = **Lakeflow Declarative Pipelines** in the current UI.

### Deliverable standards
- Best-practice code and naming conventions. Prefer PySpark / Databricks-native solutions.
- **No hard coding** – use config files, bundle variables, job/notebook parameters.
- Reusable framework components preferred.
- Unit tests + evidence for all code; sanity check per ingestion.
- Notebooks must be **idempotent / rerunnable** (multiple-run scenarios).
- Email notification on job failure (optional, preferred).
- Audit columns mandatory in all layers (e.g. `load_dt`, `source_file`).
- Table and column descriptions in every layer.

### Data modelling
- Delta Lake tables; primary optimisation **Liquid Clustering**.
- Benchmark against partitioning + Z-ordering on duplicate tables; document results.
- Gold: dimensional model (fact + dimensions) with PK/FK; **all dimensions SCD2**.

## Repository
- Private GitHub repo `vstone-databricks-pipeline` (owner `919ashishyadav-star`).
- Branches: `main` (production), `dev` (integration), feature branches:
  `feature/data-profiling`, `feature/bronze-layer`, `feature/silver-layer`, `feature/gold-layer`.
- Merge to `dev` only after a milestone is complete, tested and reviewed. No direct commits to `main`.
- GitHub Actions CI/CD for automated testing and deployment.
- Required structure:
```
project-root/
├── .github/workflows/databricks-ci-cd.yml
├── databricks.yml
├── src/
│   ├── notebooks/
│   ├── pipelines/
│   └── jobs/
├── tests/
├── resources/
└── README.md
```

### Testing
- Use `pyspark.testing` (`assertDataFrameEqual`, `assertSchemaEqual`), `DataFrame.transform`, and pytest.
- Notebook tasks: suitable environment configs and dependencies; disallow retries where the spec requires.

## Day-by-day plan
- **Day 1 – Setup + Data Chunking** (`feature/data-profiling`): repo structure, Git integration, initial `databricks.yml`, initial GitHub Actions workflow, requirements + assumptions doc, dataset profiling, land raw data in a **Volume**, split into 4 chunks (chunk 1 CSV first load, chunk 2 CSV incremental, chunk 3 → JSON, chunk 4 → XML). Notebook + Job named **"Data Chunking"**, rerunnable.
- **Day 2 – Bronze** (`feature/bronze-layer`): chunk 1 CSV → Bronze Delta via **COPY INTO**; audit columns; descriptions; job in DABs; unit tests.
- **Day 3 – Bronze**: chunk 3 JSON via **Auto Loader**; chunk 4 XML via **PySpark read/write** (native `format("xml")`, `rowTag`); chunk 2 CSV via **Lakeflow Declarative Pipelines**. ⚠️ Spec says "4th part" for DLT CSV – assumed chunk 2, pending trainer confirmation.
- **Day 4 – Evaluation 1** + **Silver** (`feature/silver-layer`): nulls, dedupe, standard column names, malformed records → quarantine; Pandas/Python UDF for header standardisation.
- **Day 5 – Silver**: business rules (date standardisation; adapt "currency normalisation" to weather data); Delta ACID + time travel evidence.
- **Day 6 – Gold** (`feature/gold-layer`): fact + SCD2 dimensions via Lakeflow Declarative Pipelines; aggregate layer; Genie space.
- **Day 7 – Performance**: dataset-appropriate metric; Liquid Clustering vs partition/Z-order benchmark; incremental loads; **MERGE INTO** for late-arriving data.
- **Days 8–9**: job orchestration; UC RLS/CLS; DBU usage dashboard from system tables; integration testing in `dev`.
- **Day 10**: merge `dev` → `main`; demo and documentation.

## Dataset: Rain Forecast (Kaggle `thanhlamdev/rain-forecast-dataset`)
- **122 CSV files, one per day**: `combined_YYYY_MM_DD.csv` for Apr 2019 (30), Oct 2019 (31), Apr 2020 (30), Oct 2020 (31).
- Local source path: `C:\FILES\FINAL` (all 122 files, no subfolders).
- Each file (when complete): 540,000 rows (90 × 250 grid × 24 hours), ~167 MB, 36 columns. Total on disk ≈ **18.1 GB** (19,447,327,603 bytes); 63,157,500 rows actual vs 65,880,000 expected.
- Columns:
  - Keys: `row` (0–89), `col` (0–249), `year`, `month`, `day`, `hour` (0–23).
  - Weather model variables (likely ERA5): `CAPE, CIN, EWSS, IE, ISOR, KX, PEV, R250, R500, R850, SLHF, SLOR, SSHF, TCLW, TCW, TCWV, U250, U850, V250, V850`.
  - Satellite brightness temperatures (likely Himawari-8, Kelvin): `B09B, B10B, B11B, B12B, B14B, B16B, I2B, IRB, WVB`.
  - `label`: rain (1) / no rain (0); rain is **0.1358%** of rows (full data; replaces the earlier ~0.2–0.3% sample estimate).
- Profiling (Databricks notebook `src/notebooks/01_data_profiling.py`, full data in the landing volume): **0 duplicate natural keys across all 63,157,500 rows**; rain rate 0.1358%. (HTML export of the run to be added as `docs/profiling/01_data_profiling_results.html`.)
- Profiling (3 sample files): no nulls, no duplicates on the natural key, clean column names. `CIN = 9999` is a "no value" sentinel. Satellite band value `0` is physically impossible → treat as missing.
- Profiling (full scan of all 122 files, see `docs/profiling/file_row_counts.csv`): **122 files, 63,157,500 rows; 19 files have missing whole hours** (mostly Oct 2019; 2019-10-20 has only 1 hour).
  - All 122 files match `combined_YYYY_MM_DD.csv`; every day of each month is present; **no metadata files**.
  - **Incomplete days:** 103 files are complete (540,000 rows, 24 hours); **19 are missing whole hours** (rows always = 22,500 × hours present; no partial hours). 16 are in Oct 2019 (chunk 2), plus 2020-04-06, 2020-04-13 and 2020-10-27. Worst case `combined_2019_10_20.csv` has only hour 10 (22,500 rows). Missing hours are absent rows, not nulls.
  - **Missing columns:** `combined_2019_04_27.csv` to `combined_2019_04_30.csv` (4 files, chunk 1) have **27 columns** – all 9 satellite bands (`B09B`…`WVB`) are absent. The other 118 files have 36 columns.
  - No empty, `NaN`, `null`/`NA`/`None` values anywhere; every row has the header's column count; every `hour` is 0–23.
  - Hidden missing values (sentinels): `CIN = 9999` in 47.2% of rows; satellite bands = 0 in 2.18% of rows that have bands (1,327,500 rows), always in whole-hour blocks – e.g. 2019-04-26 and 2019-10-14 have 23 of 24 hours with all bands 0.

## Decisions and assumptions
1. Natural key: `row, col, year, month, day, hour`.
2. Chunking by month:
   | Chunk | Files | Format | Bronze method |
   |---|---|---|---|
   | 1 | Apr 2019 | CSV | COPY INTO (first load) |
   | 2 | Oct 2019 | CSV | Lakeflow Declarative Pipelines (incremental) |
   | 3 | Apr 2020 | JSON | Auto Loader |
   | 4 | Oct 2020 | XML | PySpark read/write |
3. Chunking is parameterised: paths come from job parameters / widgets and the chunk-to-month mapping from `src/config/chunk_plan.json`, so a subset or the full dataset is a config change. **CSV chunks (1, 2) are copied as-is** (no Spark); **Spark is used only to convert chunk 3 to JSON and chunk 4 to XML**.
4. Capture the source file name as an audit column **in Bronze, from file metadata** (`_metadata.file_name`). Chunk outputs have **no** `source_file` column; instead each output file keeps the source day's name (decision 11).
5. Silver: `CIN = 9999` → null; satellite bands = 0 → missing/quarantine.
6. No metadata files found (full file check of `C:\FILES\FINAL`) → all 122 CSVs go to Gold.
7. Gold idea: weather-observation fact with date, hour and grid-cell dimensions; aggregates like rain frequency by month, hour, grid region.
8. Gold aggregates must use **rates, not raw counts** (e.g. rain rows ÷ observed rows), because 19 days have missing hours – raw counts would make those days/months look drier.
9. **No `mode: development` on the dev target.** Development mode renames every schema/volume to `dev_<user>_<name>`, which broke the shared `vstone_landing` name (`presets.name_prefix: ""` is ignored by the CLI). Names already include `dev`, and the useful dev-mode behaviours are set via presets instead (`trigger_pause_status: PAUSED`, `pipelines_development: true`, tag `environment: dev`). Side effect: dev jobs get no automatic `[dev <user>]` prefix, so job names must include the environment to tell dev and prod apart in the shared workspace.
10. **The dev target owns the shared landing area** (`vstone_landing` schema + `source_files` volume, defined under `targets.dev` in `resources/unity_catalog.yml`, with `lifecycle.prevent_destroy: true`). If both targets declared it, the second deploy would fail ("already exists") and destroying either target would delete the source files for both. Consequence: **always deploy dev before prod**, and never remove these resources from the dev target.
11. **One output file per source day with a fixed name** (e.g. `chunk3_json/combined_2020_04_01.json.gz`). COPY INTO and Auto Loader track files by path, so Spark's random `part-…` names would be re-ingested after a rerun; fixed names make reruns safe. Chunking mode `skip_existing` (default) leaves existing outputs alone; `overwrite` regenerates them at the same paths.
12. **Schema drift is handled in Bronze, not in chunking.** The 4 Apr 2019 files with 27 columns are copied unchanged into chunk 1, so Bronze (COPY INTO) must match columns by name. JSON/XML conversion reads source files grouped by header with explicit schemas and combines them by name (`src/common/source_reader.py`).
13. **gzip for JSON and XML only.** Measured on 20,000 rows: JSON ≈ 1.8× CSV, XML ≈ 3.6× CSV; uncompressed ≈ 8.4 GB (chunk 3) and ≈ 17.4 GB (chunk 4) vs ≈ 0.7 GB and ≈ 0.8 GB gzipped. CSV chunks stay uncompressed (identical to the source). Dev chunk total ≈ 10 GB.
14. **XML tags are `records` (root) / `record` (row)**, not `row`, because the dataset has a column named `row` – `<row><row>0</row>…</row>` would break reading.
15. **Row-count sanity check for every chunk**: rows per output file must equal rows per source file, file counts must match, and no unexpected files may exist; otherwise the job fails (and emails).

## Open questions (do not assume – flag to Aman)
Kept in sync with §7 of `docs/requirements_and_assumptions.md` (same numbering).
- **Q1** Must all 122 files (~18.1 GB) be used on Free Edition, or is a subset acceptable? (Current assumption: full dataset; chunk outputs add ~18 GB per environment.)
- **Q2** Confirm the DLT chunk is chunk 2 (spec says "4th part").
- **Q3** How to handle the 4 files without satellite columns (`combined_2019_04_27.csv` to `04_30`): keep the rows with satellite bands as null, or quarantine them in Silver? (Not yet decided.)
- **Q4** Are the 19 incomplete days acceptable as-is? (Current assumption: yes – absent data, not errors; Gold uses rates, decision 8.)
- **Q5** Are the schema and volume names acceptable? (Current assumption: yes – already deployed to dev; renaming = change bundle variables + redeploy.)

## Lessons from reference materials
- XML: sanitise column names (no spaces/hyphens); XML round trips lose types and sort columns – use **explicit schemas** in Bronze.
- Don't trust `inferSchema` blindly.
- Spark writes folders of part files; `coalesce(1)` only when a single file is truly needed.
- Volume paths: `/Volumes/<catalog>/<schema>/<volume>/...`.

## Current status (as of 2026-10-17, Day 1 on `feature/data-profiling`)
- **Tooling:** repo cloned at `C:\projects\vstone-databricks-pipeline`; GitHub CLI authenticated; Databricks CLI v1.21.0 logged in (profile `DEFAULT`, OAuth).
- **Repo:** required structure in place; `databricks.yml` (variables + `dev`/`prod` targets) and `resources/unity_catalog.yml`; CI workflow validates the bundle on PRs to `dev`/`main` (secrets `DATABRICKS_HOST`/`DATABRICKS_TOKEN` set, last run passed).
- **PR:** `feature/data-profiling` → `dev` is open (PR #1); not merged.
- **Workspace (dev deployed):** schemas `vstone_landing`, `vstone_dev_bronze`, `vstone_dev_silver`, `vstone_dev_gold`; volumes `vstone_landing.source_files` and `vstone_dev_bronze.raw_files`, all with comments. **prod not deployed.**
- **Source files uploaded:** all 122 CSVs are in `/Volumes/workspace/vstone_landing/source_files/` (uploaded 2026-10-17 with `databricks fs cp`; verified: 122 files, 19,447,327,603 bytes, every file's size matches `C:\FILES\FINAL`, 0 failures).
- **Profiling done (local):** full scan of all 122 local files (`docs/profiling/file_row_counts.csv`, findings in the Dataset section).
- **Docs:** `docs/requirements_and_assumptions.md` written (goal, constraints, naming, chunk plan, profiling, decisions 1–15, open questions Q1–Q5).
- **Profiling notebook** `src/notebooks/01_data_profiling.py` ran successfully in Databricks (0 duplicate keys, rain rate 0.1358%). HTML export still to be added to `docs/profiling/`.
- **Data Chunking (code written, not deployed or run):** `src/notebooks/02_data_chunking.py`, config `src/config/chunk_plan.json`, shared modules `src/common/` (reader by column name, chunk-plan validation), job `resources/data_chunking_job.yml` (`[<env>] Data Chunking`, serverless, no retries, email on failure), unit tests in `tests/` (run in CI).
- **Next:** deploy the job to dev and run it (ask first); check the sanity-check output; then Day 2 Bronze on `feature/bronze-layer`.
