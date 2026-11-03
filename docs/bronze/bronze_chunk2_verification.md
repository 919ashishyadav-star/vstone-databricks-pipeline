# Bronze chunk 2 (CSV, Lakeflow Declarative Pipeline) – verification

> **Assumption (Q2):** the spec's "4th part" for the declarative pipeline load is taken to be **chunk 2**
> (Oct 2019 CSV). Built on this assumption while waiting for the trainer's confirmation.

| | |
|---|---|
| **Status** | Code, tests and bundle resources written; **not yet deployed or run** |
| **Table** | `workspace.vstone_dev_bronze.rain_forecast_chunk2_csv` (streaming table owned by the pipeline) |
| **Pipeline** | `[dev] Bronze Declarative Pipeline (chunk 2)` – `resources/bronze_chunk2_pipeline.yml`, `src/pipelines/bronze_chunk2_csv.py` |
| **Jobs** | `[dev] Bronze Declarative Pipeline` (update + checks), `[dev] Bronze chunk 2 incremental demo` (dev only) |
| **Source** | `/Volumes/workspace/vstone_dev_bronze/raw_files/chunks/chunk2_csv/` – 31 files, 14,107,500 rows expected |
| **Decisions** | 31–38 in `docs/requirements_and_assumptions.md` |

## Planned evidence

Each section is filled in from the job runs (the demo notebook's exit value is saved as
`chunk2_demo_evidence.json` in this folder).

### 1. Incremental load in two drops (demo updates A–C)

| Update | Files available | Type | Expected | Actual |
|---|---|---|---|---|
| A | days 01–15 | full refresh | 15 files loaded | _pending_ |
| B | days 01–31 | normal | only days 16–31 loaded; drop 1 rows unchanged | _pending_ |
| C | days 01–31 | normal | 0 rows | _pending_ |

### 2. Column-order proof through the real pipeline (update D)

Test file `column_order_test/combined_test_column_order.csv`: the first 10 data rows of
`combined_2019_10_01.csv` with the **36 columns in reversed order** (`label, WVB, …, col, row`).
If Auto Loader matched by position, `row` would receive `label` and `col` would receive `WVB` (not an
integer → rescued), so the rows would not equal the original.

| Check | Expected | Actual |
|---|---|---|
| Update D loads only the test file | 1 file, 10 rows | _pending_ |
| Test rows joined to the original rows on the natural key, all 36 values equal (`<=>`) | 10 of 10 | _pending_ |
| Test rows with rescued data | 0 | _pending_ |
| Cleanup: test file deleted, update E (full refresh) rebuilds from 31 files, test rows in table | 0 | _pending_ |
| Chunk folder after the demo (no held files left) | 31 files | _pending_ |

### 3. Expectation metrics per update (event log)

From `rain_forecast_chunk2_csv_event_log` via `expectation_metrics_sql` – passed / failed records per
update and expectation. _pending_

### 4. Checks job (`[dev] Bronze Declarative Pipeline`)

| Check | Expected | Actual |
|---|---|---|
| Files / rows per file and `sum(label)` = chunk files | 31 files, 14,107,500 rows | _pending_ |
| Rescued / malformed rows | 0 / 0 | _pending_ |
| Expectations failing in the table; failed records in latest update | 0 / 0 | _pending_ |
| Duplicate natural keys | 0 | _pending_ |
| Audit columns, `ingestion_method = DECLARATIVE_PIPELINE` | 0 problems | _pending_ |
| Column names, order and types = `rain_schema` | equal | _pending_ |
| Second job run: rows loaded | 0 | _pending_ |

### 5. To record after the first run

- Which `load_run_id` the pipeline writes: the update ID, or `pipeline-<pipeline id>` (decision 36).
- Table and column descriptions and `CLUSTER BY (day, hour)` visible in `DESCRIBE TABLE EXTENDED`.
- `pipelines.reset.allowed = true` in dev (table properties).
