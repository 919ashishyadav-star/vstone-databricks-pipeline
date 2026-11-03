# Bronze chunk 1 (COPY INTO) – verification (dev)

Verification of the `[dev] Bronze COPY INTO` job (`src/notebooks/03_bronze_copy_into.py`), which
loads `chunk1_csv` (Apr 2019, 30 CSV files) from
`/Volumes/workspace/vstone_dev_bronze/raw_files/chunks/chunk1_csv/` into
**`workspace.vstone_dev_bronze.rain_forecast_chunk1_csv`**.

## Runs

| Run | Job run ID | Start (local) | Duration | Result | Rows inserted |
|---|---|---|---|---|---|
| 1 | 808990251945056 | 2026-10-18 00:31:36 | 3.6 min | SUCCESS, attempt 0 | **16,200,000** |
| 2 | 104310479396699 | 2026-10-18 00:36:48 | 1.7 min | SUCCESS, attempt 0 | **0** |

Evidence exported from each run's own notebook output:

| File | Content |
|---|---|
| [`bronze_chunk1_copy_into_result_run1.csv`](bronze_chunk1_copy_into_result_run1.csv) / [`_run2`](bronze_chunk1_copy_into_result_run2.csv) | COPY INTO result: `num_inserted_rows` 16,200,000 / 0 |
| [`bronze_chunk1_checks_run1.csv`](bronze_chunk1_checks_run1.csv) / [`_run2`](bronze_chunk1_checks_run2.csv) | Per-file checks (30 rows, all `OK`; identical in both runs) |
| [`bronze_chunk1_summary_run1.csv`](bronze_chunk1_summary_run1.csv) / [`_run2`](bronze_chunk1_summary_run2.csv) | Summary checks (`Result = PASS` in both runs) |

## Table

| Property | Value |
|---|---|
| Type / format | Managed Delta table |
| Columns | 36 data columns + 6 audit columns = **42, every one with a description** |
| Table description | Set (source path, natural key, sentinel meaning) |
| Liquid Clustering | **`day, hour`** (`clusteringColumns = [["day"],["hour"]]`) |
| Audit columns | `source_file`, `source_file_path`, `source_file_modified_dt`, `load_dt`, `load_run_id`, `ingestion_method` |

## Checks (run 1 → run 2)

| # | Check | Run 1 | Run 2 |
|---|---|---|---|
| 1 | Files in chunk folder / in table | 30 / 30 | 30 / 30 |
| 1 | Rows in chunk files / in table | 16,200,000 / 16,200,000 | 16,200,000 / 16,200,000 |
| 1 | Per-file rows and `sum(label)` equal to the chunk file | 30 / 30 OK | 30 / 30 OK |
| 2 | 27-column files: all 9 bands null on every row, valid `label`, `sum(label)` equal | 4 / 4 OK | 4 / 4 OK |
| 2b | 36-column files: no null bands | 26 / 26 OK | 26 / 26 OK |
| 3 | Duplicate natural keys | 0 | 0 |
| 4 | Rows with a null audit column | 0 | 0 |
| 4 | `ingestion_method` other than `COPY_INTO` | 0 | 0 |
| – | Rows inserted by the run | 16,200,000 | **0** |
| – | Distinct `load_run_id` in the table | 1 (`808990251945056`) | 1 (`808990251945056`) |

### The 27-column proof (2019-04-27 to 2019-04-30)

These source files have no satellite columns, so `label` is their 27th column instead of the 36th. If
columns were matched by position, label values would land in `B09B` and `label` would be wrong.

| Source file | Rows (source = table) | `sum(label)` source | `sum(label)` table | Rows with all 9 bands null |
|---|---|---|---|---|
| combined_2019_04_27.csv | 540,000 | 90 | 90 | 540,000 |
| combined_2019_04_28.csv | 540,000 | 273 | 273 | 540,000 |
| combined_2019_04_29.csv | 540,000 | 325 | 325 | 540,000 |
| combined_2019_04_30.csv | 540,000 | 1,051 | 1,051 | 540,000 |

Every row of these files has all bands null, and the label totals match the source exactly, so the
columns were matched **by name**. The source sums are computed from the chunk files with the shared
by-name reader (`src/common/source_reader.py`).

## Independent check (SQL warehouse, read-only, after each run)

| Measure | Value |
|---|---|
| Total rows / files | 16,200,000 / 30 |
| `load_run_id` values | `808990251945056` only (run 1's job run ID) |
| `ingestion_method` values | `COPY_INTO` only |
| `load_dt` | a single timestamp (2026-10-17 19:03:18 UTC) – unchanged after run 2 |
| Rows with null `B09B` in the 4 short files | 2,160,000 (= 4 × 540,000) |
| Rows with `CIN = 9999` (kept as-is in Bronze) | 3,321,766 |
| Rain rate in chunk 1 | 0.0506% |

## Why the rerun loads nothing
COPY INTO records every file it has loaded into the table and skips those files on later runs. The
job never uses `force`, and the chunk files keep fixed names (Day 1, decision 11), so a rerun of
either the chunking job or this job cannot load the same day twice.
