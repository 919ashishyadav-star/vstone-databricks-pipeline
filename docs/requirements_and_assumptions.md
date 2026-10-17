# Requirements and Assumptions – VStone Databricks Pipeline

| | |
|---|---|
| **Status** | Day 1 (Setup + Data Chunking), branch `feature/data-profiling` |
| **Last updated** | 2026-10-17 |
| **Audience** | Trainer / reviewer new to the project |

This document explains what the project must deliver, the constraints it runs under,
how the data is organised, what we learned from profiling the dataset, and every
decision or assumption made so far. Items that still need the trainer's confirmation
are listed in [Open questions](#7-open-questions-for-the-trainer).

---

## 1. Project goal

Build an end-to-end data pipeline on Databricks that takes the Kaggle **Rain Forecast**
dataset from raw files to analytics-ready tables, using the **medallion architecture**
(Bronze → Silver → Gold).

The pipeline must:

- Ingest the data in **three file formats** (CSV, JSON, XML) using different Databricks
  ingestion methods (COPY INTO, Auto Loader, Lakeflow Declarative Pipelines, PySpark).
- Clean and standardise it (Silver), then model it as a **dimensional model** with a
  fact table and **SCD2 dimensions** for business analytics (Gold).
- Be governed with **Unity Catalog** (row-level security, column-level security,
  dynamic data masking).
- Be orchestrated with **Databricks Jobs** and deployed with **Databricks Asset
  Bundles (DABs)**, with CI/CD in **GitHub Actions**.
- Include unit tests, monitoring, and a sanity check for every ingestion.
- Carry **every dataset file** (except metadata files – none exist, see §5) all the way to Gold.

### Deliverable standards

- Best-practice code and naming; PySpark / Databricks-native solutions preferred.
- **No hard coding** – names and paths come from bundle variables, config files, or
  job/notebook parameters.
- Reusable framework components where possible.
- Notebooks are **idempotent / rerunnable** (safe to run multiple times).
- **Audit columns** in every layer (e.g. `load_dt`, `source_file`).
- **Table and column descriptions** in every layer.
- Delta Lake tables, optimised primarily with **Liquid Clustering**, benchmarked against
  partitioning + Z-ordering on duplicate tables.
- Email notification on job failure (optional, preferred).

---

## 2. Environment constraints

| Constraint | Impact |
|---|---|
| **Databricks Free Edition** only (not the trial) | One workspace, one usable catalog (`workspace`); the user **cannot create new catalogs** (no `CREATE CATALOG` privilege, no metastore storage root). Dev and prod therefore share one catalog and are separated by schema name. |
| **Serverless compute only** | No managed clusters; jobs and pipelines must run on serverless. |
| "DLT" in the spec | Means **Lakeflow Declarative Pipelines** (the current name in the Databricks UI). |
| Credentials | Never in the repo. Locally in `~/.databrickscfg` (OAuth login); in CI from GitHub secrets `DATABRICKS_HOST` / `DATABRICKS_TOKEN`. |

---

## 3. Medallion design and naming convention

### Layers

| Layer | Purpose |
|---|---|
| **Landing** | Original source files, stored once and shared by all environments. |
| **Bronze** | Raw data ingested as-is from the chunk files, plus audit columns. |
| **Silver** | Cleaned and standardised data: sentinel values handled, duplicates removed, malformed rows quarantined, standard column names. |
| **Gold** | Dimensional model (fact + SCD2 dimensions) and rate-based aggregates. |

### Naming convention

- Lowercase `snake_case`.
- Schemas: `<project>_<environment>_<layer>` – the project first groups all schemas
  together; the environment is next because dev and prod share one catalog.
- The landing schema has no environment because it is shared: `vstone_landing`.
- All names are **bundle variables** in `databricks.yml`; code never hard-codes them.

| Object | dev | prod | Bundle variable |
|---|---|---|---|
| Catalog | `workspace` | `workspace` | `catalog` |
| Landing schema (shared) | `vstone_landing` | `vstone_landing` | `landing_schema` |
| Landing volume (shared) | `vstone_landing.source_files` | *(same)* | `landing_volume` |
| Bronze schema | `vstone_dev_bronze` | `vstone_prod_bronze` | `bronze_schema` |
| Chunk-output volume | `vstone_dev_bronze.raw_files` | `vstone_prod_bronze.raw_files` | `raw_volume` |
| Silver schema | `vstone_dev_silver` | `vstone_prod_silver` | `silver_schema` |
| Gold schema | `vstone_dev_gold` | `vstone_prod_gold` | `gold_schema` |

The schemas and volumes are defined as bundle resources in `resources/unity_catalog.yml`
and are created by `databricks bundle deploy`. **Dev is deployed; prod is not yet.**

### Data flow

```
/Volumes/workspace/vstone_landing/source_files/          122 original CSVs (~18.1 GB, stored once)
        │   "Data Chunking" job (run per environment)
        ▼
/Volumes/workspace/vstone_<env>_bronze/raw_files/chunks/
        ├── chunk1_csv/   ──► Bronze (COPY INTO)
        ├── chunk2_csv/   ──► Bronze (Lakeflow Declarative Pipelines)
        ├── chunk3_json/  ──► Bronze (Auto Loader)
        └── chunk4_xml/   ──► Bronze (PySpark read/write)
                                   │
                                   ▼
                     vstone_<env>_silver  ──►  vstone_<env>_gold
```

---

## 4. Chunk plan

The 122 daily files are split **by month** into four chunks, one per ingestion method.

| Chunk | Files | Days | Rows | Size | Format | Bronze ingestion method |
|---|---|---|---|---|---|---|
| 1 | Apr 2019 | 30 | 16,200,000 | 4.50 GB | CSV | COPY INTO (first load) |
| 2 | Oct 2019 | 31 | 14,107,500 | 4.08 GB | CSV | Lakeflow Declarative Pipelines (incremental) |
| 3 | Apr 2020 | 30 | 16,155,000 | 4.67 GB | JSON | Auto Loader |
| 4 | Oct 2020 | 31 | 16,695,000 | 4.86 GB | XML | PySpark read/write (`format("xml")`, `rowTag`) |
| **Total** | | **122** | **63,157,500** | **18.1 GB** | | |

Chunks 3 and 4 are converted from CSV to JSON and XML by the "Data Chunking" job, so each
ingestion method has its own input format. Chunk 2 being the Declarative Pipelines chunk
is an **assumption** (see open questions).

---

## 5. Dataset profiling summary

**Dataset:** Kaggle `thanhlamdev/rain-forecast-dataset` – hourly weather-model and
satellite readings on a 90 × 250 grid, with a rain / no-rain label.

- **122 CSV files**, one per day, named `combined_YYYY_MM_DD.csv`, for Apr 2019, Oct 2019,
  Apr 2020 and Oct 2020. Every day of each month is present. **No metadata files.**
- **63,157,500 rows** in total (65,880,000 expected if every file were complete),
  **~18.1 GB** (19,447,327,603 bytes).
- A complete file has **540,000 rows** (90 × 250 grid cells × 24 hours) and **36 columns**:
  - Keys: `row` (0–89), `col` (0–249), `year`, `month`, `day`, `hour` (0–23).
  - 20 weather-model variables (likely ERA5), e.g. `CAPE`, `CIN`, `TCWV`, `U850`.
  - 9 satellite brightness temperatures in Kelvin (likely Himawari-8): `B09B, B10B, B11B,
    B12B, B14B, B16B, I2B, IRB, WVB`.
  - `label`: 1 = rain, 0 = no rain. Rain is rare (~0.2–0.3% of rows in sampled files).

Per-file results: [`docs/profiling/file_row_counts.csv`](profiling/file_row_counts.csv).

### Findings

| # | Finding | Detail | Why it matters |
|---|---|---|---|
| F1 | **19 incomplete files** | 103 files are complete; 19 are **missing whole hours** (rows always = 22,500 × hours present – no partial hours). 16 are in Oct 2019 (chunk 2), plus 2020-04-06, 2020-04-13 and 2020-10-27. `combined_2019_10_20.csv` has **only 1 hour** (22,500 rows). | Missing hours are **absent rows, not nulls**. Counts per day/month are not comparable → use rates in Gold (decision 8). Good candidate for a per-file sanity check. |
| F2 | **4 files missing columns** | `combined_2019_04_27.csv` to `combined_2019_04_30.csv` (chunk 1) have **27 columns**: all 9 satellite bands are absent. | Bronze needs an explicit schema that tolerates these columns being missing. |
| F3 | **`CIN = 9999` sentinel** | 9999 means "no value" for CIN. Present in **47.2%** of rows. | Treated as null in Silver (decision 5); nearly half the CIN values are effectively missing. |
| F4 | **Satellite band = 0** | A brightness temperature of 0 K is physically impossible → missing. Found in **2.18%** of rows that have bands (1,327,500 rows), always in whole-hour blocks (e.g. 2019-04-26 and 2019-10-14: 23 of 24 hours). | Treated as missing / quarantined in Silver (decision 5). |
| F5 | **No explicit nulls** | No empty, `NaN`, `null`, `NA` or `None` values in any file; every row has the header's column count; every `hour` is 0–23. | Missing data appears only as F1–F4, never as blank values. |
| F6 | **Clean keys (sampled)** | No duplicates on the natural key and clean column names in the 3 sampled files. | Duplicate check across all 122 files will be repeated in Bronze/Silver. |

---

## 6. Decisions and assumptions

1. **Natural key:** `row, col, year, month, day, hour` (one reading per grid cell per hour).
2. **Chunking by month** into four chunks, one per ingestion method – see the
   [chunk plan](#4-chunk-plan).
3. **Chunking is done in PySpark** and parameterised (paths, file lists), so using a
   subset or the full dataset is a configuration change, not a code change.
4. **The source file name/date is captured as an audit column** (e.g. `source_file`).
5. **Silver handling of sentinels:** `CIN = 9999` → null; satellite band = 0 → treated as
   missing / quarantined.
6. **No metadata files exist** (full check of all 122 files) → **all 122 CSVs flow to Gold**.
7. **Gold design (initial idea):** a weather-observation fact table with date, hour and
   grid-cell dimensions; aggregates such as rain frequency by month, hour and grid region.
8. **Gold aggregates use rates, not raw counts** (e.g. rain rows ÷ observed rows). Because
   19 days have missing hours, raw counts would make those days and months look drier
   than they were.
9. **The dev target does not use `mode: development`.** Development mode renames every
   schema/volume to `dev_<user>_<name>`, which broke the shared `vstone_landing` name
   (setting `presets.name_prefix: ""` is ignored by the CLI). Names already include
   `dev`, and the useful dev-mode behaviours are set via presets instead (paused job
   schedules, pipelines in development mode, tag `environment: dev`).
   *Side effect:* dev jobs get no automatic `[dev <user>]` name prefix, so job names
   must include the environment to tell dev and prod apart in the shared workspace.
10. **The dev target owns the shared landing area** (`vstone_landing` schema and
    `source_files` volume, protected with `lifecycle.prevent_destroy`). If both targets
    declared it, the second deploy would fail ("already exists") and destroying either
    target would delete the source files for both. *Consequence:* **always deploy dev
    before prod**, and never remove these resources from the dev target.

---

## 7. Open questions for the trainer

| # | Question | Current assumption | Impact if different |
|---|---|---|---|
| Q1 | Must all 122 files (~18.1 GB) be used on Free Edition, or is a subset acceptable? | Full dataset; the upload of all 122 files to the landing volume is in progress. Chunk outputs would add ~18 GB per environment. | A subset is a config change (decision 3). |
| Q2 | The spec says "4th part" for the DLT CSV load. Is the Declarative Pipelines chunk **chunk 2** (Oct 2019 CSV)? | Chunk 2. | Chunk-to-method mapping in §4 changes. |
| Q3 | How should the **4 files without satellite columns** (2019-04-27 to 04-30) be handled? | Not yet decided: keep rows with satellite bands as null, or quarantine them in Silver. | Affects Silver rules and Gold completeness for late April 2019. |
| Q4 | Are the **19 incomplete days** acceptable as-is? | Yes – treated as absent data, not errors; Gold uses rates (decision 8). | If not, those days would need to be excluded or flagged in Gold. |
| Q5 | Are the proposed **schema and volume names** (§3) acceptable? | Yes – already deployed to dev. | Renaming means changing bundle variables and redeploying. |
