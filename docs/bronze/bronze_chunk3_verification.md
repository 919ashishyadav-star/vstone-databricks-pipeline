# Bronze chunk 3 (Auto Loader) – verification (dev)

Verification of the `[dev] Bronze Auto Loader` job (`src/notebooks/04_bronze_auto_loader.py`), which
loads `chunk3_json` (Apr 2020, 30 gzip JSON files) from
`/Volumes/workspace/vstone_dev_bronze/raw_files/chunks/chunk3_json/` into
**`workspace.vstone_dev_bronze.rain_forecast_chunk3_json`** with Auto Loader.

## Setup

| Item | Value |
|---|---|
| Table | Managed Delta, 36 data + 6 audit columns + `_rescued_data` = **43 columns, all described** |
| Liquid Clustering | `day, hour` |
| Schema | Explicit and typed (`rain_schema.spark_schema()`); `cloudFiles.schemaEvolutionMode = none`; no inference |
| Bad records | `rescuedDataColumn = _rescued_data` |
| Checkpoint | `/Volumes/workspace/vstone_dev_bronze/checkpoints/rain_forecast_chunk3_json/` (new `checkpoints` volume) |
| Schema location | `…/checkpoints/rain_forecast_chunk3_json/_schema` |
| Trigger | `availableNow=True` (serverless), `cloudFiles.maxFilesPerTrigger = 10` |
| `ingestion_method` | `AUTO_LOADER` |

## Runs

| Run | Job run ID | Start (local) | Duration | Result | Files / rows loaded |
|---|---|---|---|---|---|
| first attempt | 877938057802640 | 2026-10-21 13:27 | – | FAILED after the load (see below) | 30 / 16,155,000 – then reset |
| **1** | 820660574067794 | 2026-10-21 13:37:21 | 2.4 min | SUCCESS, attempt 0 | **30 / 16,155,000** |
| **2** | 707850635412311 | 2026-10-21 13:40:25 | 1.7 min | SUCCESS, attempt 0 | **0 / 0** |

**First attempt:** the stream loaded all 30 files correctly (3 micro-batches, checkpoint commits 0–2), then the
notebook crashed while building its run report, because `numInputRows` in the streaming progress report was
null on serverless. The checks never ran. Fix (commit `ec00c06`): the per-batch report now comes from the
table itself – each micro-batch stamps its own `load_dt`. With approval, the table was emptied
(`TRUNCATE TABLE`) **and** its checkpoint folder deleted together, then runs 1 and 2 were done cleanly.

### Run 1 – rows and files per micro-batch
([`bronze_chunk3_batches_run1.csv`](bronze_chunk3_batches_run1.csv))

| Micro-batch (`load_dt`, UTC) | Files | Rows | First → last file |
|---|---|---|---|
| 08:08:23.124 | 10 | 5,377,500 | 2020-04-01 → 04-10 |
| 08:08:23.653 | 10 | 5,377,500 | 2020-04-11 → 04-20 |
| 08:08:23.973 | 10 | 5,400,000 | 2020-04-21 → 04-30 |
| **Total** | **30** | **16,155,000** | |

The first two batches are 22,500 rows short of 10 × 540,000 because 2020-04-06 and 2020-04-13 each miss one
hour (finding F1).

### Run 2
- **0 micro-batches, 0 files, 0 rows** ([`bronze_chunk3_batches_run2.csv`](bronze_chunk3_batches_run2.csv) is empty).
- The checkpoint still has only commits 0, 1, 2 – no new batch was started.
- Per-file checks identical to run 1; the table keeps one `load_run_id`.

## Checks (run 1 → run 2)

Evidence: [`bronze_chunk3_checks_run1.csv`](bronze_chunk3_checks_run1.csv) / [`_run2`](bronze_chunk3_checks_run2.csv)
(30 rows, all `OK`, identical), [`bronze_chunk3_summary_run1.csv`](bronze_chunk3_summary_run1.csv) / [`_run2`](bronze_chunk3_summary_run2.csv).

| # | Check | Run 1 | Run 2 |
|---|---|---|---|
| 1 | Files in chunk folder / in table | 30 / 30 | 30 / 30 |
| 1 | Rows in chunk files / in table | 16,155,000 / 16,155,000 | 16,155,000 / 16,155,000 |
| 1 | Per-file rows and `sum(label)` equal to the chunk file | 30 / 30 OK (label sum 15,131 = 15,131) | 30 / 30 OK |
| 2 | Null satellite bands (all 30 files have bands) | 0 | 0 |
| 3 | Rows with rescued data / malformed rows | 0 / 0 | 0 / 0 |
| 4 | Duplicate natural keys | 0 | 0 |
| 5 | Null audit columns / other `ingestion_method` | 0 / 0 | 0 / 0 |
| – | Rows inserted by the run | 16,155,000 | **0** |

## Shared-check refactor (chunk 1)

The per-file comparison moved from `03_bronze_copy_into.py` into `src/common/bronze_checks.py` (used by both
notebooks). `[dev] Bronze COPY INTO` was rerun once (job run 147996199973667, 1.7 min, SUCCESS):

- COPY INTO inserted **0 rows** ([`bronze_chunk1_copy_into_result_run3.csv`](bronze_chunk1_copy_into_result_run3.csv)).
- [`bronze_chunk1_checks_run3.csv`](bronze_chunk1_checks_run3.csv) is **byte-identical** to `bronze_chunk1_checks_run1.csv`
  (written before the refactor), and [`bronze_chunk1_summary_run3.csv`](bronze_chunk1_summary_run3.csv) is
  byte-identical to `bronze_chunk1_summary_run2.csv`.

## Rescued-data test

Evidence: [`rescued_data_test/`](rescued_data_test/) – the input file, the rows loaded, and the job's failure message.

The same job and notebook were run with overrides `chunk_name=_test_rescued_json`,
`table_name=zz_test_rescued_chunk3_json`, `run_optimize=false` on a 5-line test file
([`combined_test_rescued.json`](rescued_data_test/combined_test_rescued.json)):

| Line | Problem | Result in the table ([`rescued_rows.csv`](rescued_data_test/rescued_rows.csv)) |
|---|---|---|
| 1 | none | loaded normally, `_rescued_data` null |
| 2 | `CAPE` = `"not_a_number"` (wrong type) | `CAPE` null; `_rescued_data` = `{"CAPE":"not_a_number","_file_path":…}` |
| 3 | extra field `EXTRA_FIELD` | row loaded; `_rescued_data` = `{"EXTRA_FIELD":123,"_file_path":…}` |
| 4 | `kx` instead of `KX` (case) | `KX` null; `_rescued_data` = `{"kx":7.25,"_file_path":…}` |
| 5 | truncated JSON line | row with **every column null and `_rescued_data` null** |

The job **failed as designed** ([`job_failure_message.txt`](rescued_data_test/job_failure_message.txt)):
3 rows with rescued data, 1 malformed row (every key and label null), plus that row's null label and bands.

**Lesson:** rescued data catches values that do not fit the schema, but a completely broken line is **not**
rescued – it arrives as an all-null row. That is why the separate "every key and label null" check exists.

**Cleanup:** the test folder, the scratch table `zz_test_rescued_chunk3_json` and its checkpoint folder were
deleted after the evidence was saved. (An earlier attempt failed before the test because the test folder had
not been created yet; it loaded nothing.)

## Why reruns load nothing
The checkpoint records every file already processed, and `cloudFiles.allowOverwrites` stays false, so a file
rewritten at the same path is not reloaded. The notebook refuses to run if the table has rows but the
checkpoint is missing, so a lost checkpoint cannot silently create duplicates.
