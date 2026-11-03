# Bronze chunk 2 (CSV, Lakeflow Declarative Pipeline) – verification (dev)

> **Assumption (Q2):** the spec's "4th part" for the declarative pipeline load is taken to be **chunk 2**
> (Oct 2019 CSV). Built on this assumption while waiting for the trainer's confirmation.

| | |
|---|---|
| **Status** | Deployed to dev and verified on 2026-11-03: demo (updates A–E) and checks job run twice, all passing |
| **Table** | `workspace.vstone_dev_bronze.rain_forecast_chunk2_csv` (streaming table owned by the pipeline) |
| **Pipeline** | `[dev] Bronze Declarative Pipeline (chunk 2)` (id `6157baeb-2e5a-4022-8a49-d28c99c91b61`) – `resources/bronze_chunk2_pipeline.yml`, `src/pipelines/bronze_chunk2_csv.py` |
| **Jobs** | `[dev] Bronze Declarative Pipeline` (update + checks), `[dev] Bronze chunk 2 incremental demo` (dev only) |
| **Source** | `/Volumes/workspace/vstone_dev_bronze/raw_files/chunks/chunk2_csv/` – 31 files, 14,107,500 rows |
| **Decisions** | 31–38 in `docs/requirements_and_assumptions.md` |

## Runs

| Run | Job run ID | Start (local) | Duration | Result |
|---|---|---|---|---|
| Demo (updates A–E) | 328244185864076 | 2026-11-03 15:49:31 | 6.6 min | SUCCESS |
| Checks job run 1 | 607784582755047 | 2026-11-03 15:56:48 | 1.2 min | SUCCESS |
| Checks job run 2 | 845553048620481 | 2026-11-03 15:58:05 | 1.6 min | SUCCESS |

An earlier demo attempt (run 887633549376929) stopped at its first check because Databricks could not read
Unity Catalog storage at that time (`UNAVAILABLE … service=key-manager`, a platform-side outage). No file was
moved and no update was started. The preflight now checks that the chunk folder can be read
(`os.path.isdir`), so a storage failure stops with a clear message instead of looking like "0 files".

Full demo output (every update, the column-order proof, the expectation metrics, the restore step):
[`chunk2_demo_evidence.json`](chunk2_demo_evidence.json).

## 1. Incremental load in two drops (demo updates A–C)

Before A, the notebook moved days 16–31 to `raw_files/_hold/chunk2_csv/`; before B it moved them back.

| Update | Update ID | Files available | Type | Expected | Files / rows loaded | Micro-batches | Table after |
|---|---|---|---|---|---|---|---|
| A | `16e36645…` | days 01–15 | full refresh | 15 files loaded | **15 / 7,425,000** (01 → 15) | 2 | 15 files, 7,425,000 rows |
| B | `49ed2c5e…` | days 01–31 | normal | only days 16–31 | **16 / 6,682,500** (16 → 31) | 2 | 31 files, 14,107,500 rows |
| C | `a88478bd…` | days 01–31 | normal | 0 rows | **0 / 0** | 0 | 31 files, 14,107,500 rows |

**What this shows:** B loaded only the 16 files the pipeline had not seen. The pipeline's own checkpoint
remembers days 01–15. C found nothing new and loaded 0 rows. The micro-batches follow
`maxFilesPerTrigger = 10` (15 files → 10 + 5, 16 files → 10 + 6).

## 2. Column-order proof through the real pipeline (update D)

Test file `column_order_test/combined_test_column_order.csv` contains the first 10 data rows of
`combined_2019_10_01.csv` with the **36 columns in reversed order** (`label, WVB, …, col, row`).
If Auto Loader matched columns by position, `row` would receive `label` and `col` would receive `WVB` (not an
integer → rescued), so the rows would not equal the original.

| Check | Expected | Actual |
|---|---|---|
| Update D (`cc63c47d…`) loads only the test file | 1 file, 10 rows | **1 file, 10 rows** (table 14,107,510 rows, 32 files) |
| Test rows joined to the original rows on the natural key, all 36 values equal (`<=>`) | 10 of 10 | **10 matched, 10 equal** |
| Test rows with rescued data | 0 | **0** |
| Cleanup: test file deleted, update E (`36eb7749…`, full refresh) rebuilds from 31 files, test rows in table | 0 | **31 files, 14,107,500 rows, 0 test rows** (4 micro-batches) |
| Chunk folder after the demo (no held files left) | 31 files | **31 files** (also confirmed with `LIST` after the run) |

**Conclusion:** CSV columns are matched **by header name**, not by position, in the real pipeline as well.

The restore step in `finally` reported `moved_back = 0` and `test_file_removed = false`. This is correct: the
held files were already moved back before B, and the test file was already deleted before E, so there was
nothing left to restore. The empty folder `raw_files/_hold/chunk2_csv/` remains. It holds no files and is
harmless.

Because E was a full refresh, the table now has only E's rows. Its four `load_dt` values are the four
micro-batches of E ([`bronze_chunk2_batches_run1.csv`](bronze_chunk2_batches_run1.csv)).

## 3. Expectation metrics per update (event log)

From `rain_forecast_chunk2_csv_event_log` via `expectation_metrics_sql`
([`bronze_chunk2_expectations_run1.csv`](bronze_chunk2_expectations_run1.csv), also in the demo JSON).
All 9 expectations (7 warn, 2 fail) were evaluated on every row:

| Update | Passed records (each of the 9 expectations) | Failed records |
|---|---|---|
| A | 7,425,000 | 0 |
| B | 6,682,500 | 0 |
| C | – (no rows, no metrics) | – |
| D | 10 | 0 |
| E | 14,107,500 | 0 |

Expectations: `no_rescued_data`, `not_malformed`, `label_is_0_or_1`, `hour_in_range`, `row_in_range`,
`col_in_range`, `month_in_chunk` (warn) and `source_file_present`, `ingestion_method_set` (fail).

The pipeline updates started by the two checks-job runs (`70e5cc59…`, `c99a4bec…`) appear in the event log with
**0 output rows** and no expectation metrics.

## 4. Checks job (`[dev] Bronze Declarative Pipeline`)

Evidence: [`bronze_chunk2_summary_run1.csv`](bronze_chunk2_summary_run1.csv) / [`_run2`](bronze_chunk2_summary_run2.csv),
[`bronze_chunk2_checks_run1.csv`](bronze_chunk2_checks_run1.csv) / [`_run2`](bronze_chunk2_checks_run2.csv) (per file),
[`bronze_chunk2_table_rules_run1.csv`](bronze_chunk2_table_rules_run1.csv) / [`_run2`](bronze_chunk2_table_rules_run2.csv)
(expectations re-checked on the whole table). All run 1 and run 2 files are byte-identical.

| Check | Expected | Run 1 | Run 2 |
|---|---|---|---|
| Files in chunk folder / in table | 31 / 31 | 31 / 31 | 31 / 31 |
| Rows in chunk files / in table | 14,107,500 | 14,107,500 / 14,107,500 | 14,107,500 / 14,107,500 |
| Per-file rows and `sum(label)` equal to the chunk file | 31 OK | 31 / 31 OK (label sum 13,435 = 13,435) | 31 / 31 OK |
| Rescued rows | 0 | 0 | 0 |
| Expectations failing in the table; failed records in latest update | 0 / 0 | 0 / 0 | 0 / 0 |
| Duplicate natural keys | 0 | 0 | 0 |
| Null audit columns; `ingestion_method = DECLARATIVE_PIPELINE` | 0 problems | 0 | 0 |
| Column names, order and types = `rain_schema` | equal | equal (check passed) | equal |
| Rows loaded by the job's pipeline update | – | **0** | **0** |
| Result | PASS | **PASS** | **PASS** |

**Note:** run 1 also loaded 0 rows, because the demo's update E had already loaded all 31 files. So both
runs are reruns. Together with update C, they show three times that a normal update loads only new files.

## 5. Recorded after the first run

| Item | Result |
|---|---|
| `load_run_id` written by the pipeline (decision 36) | `pipeline-6157baeb-2e5a-4022-8a49-d28c99c91b61` – the **pipeline ID fallback**; the update ID is not exposed to the pipeline code. Every row has this one value, so updates are told apart by `load_dt` and the event log's `origin.update_id`. |
| Table description | Set from code (starts "Bronze: Rain Forecast chunk2_csv loaded as-is by a Lakeflow Declarative Pipeline…", mentions Q2) |
| Column descriptions | 43 columns, **0 without a comment** (`information_schema.columns`) |
| Liquid Clustering | `clusteringColumns = [day, hour]` |
| `pipelines.reset.allowed` | `true` (dev). Prod uses the variable default `false`. |
| Other table properties | `pipelines.pipelineId = 6157baeb-…` (the table is owned by the pipeline), change data feed, deletion vectors and row tracking on |
