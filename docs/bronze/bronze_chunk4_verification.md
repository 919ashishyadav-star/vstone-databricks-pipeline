# Bronze chunk 4 (PySpark XML) – verification (dev)

Verification of the `[dev] Bronze XML` job (`src/notebooks/05_bronze_xml.py`), which loads `chunk4_xml`
(Oct 2020, 31 gzip XML files) from `/Volumes/workspace/vstone_dev_bronze/raw_files/chunks/chunk4_xml/` into
**`workspace.vstone_dev_bronze.rain_forecast_chunk4_xml`** with a PySpark batch read/write.

## Setup

| Item | Value |
|---|---|
| Read | `spark.read.format("xml")`, `rowTag = record` (from `src/config/chunk_plan.json`), explicit typed schema |
| Parse mode | `PERMISSIVE`, `rescuedDataColumn = _rescued_data`, `columnNameOfCorruptRecord = _corrupt_record` |
| Write | One Delta commit per run: `append` (default) or `overwrite` + `replaceWhere` (mode `replace`) |
| Table | 36 data + 6 audit + `_rescued_data` + `_corrupt_record` = **44 columns, all described** |
| Liquid Clustering | `day, hour` |
| `ingestion_method` | `PYSPARK_XML` |
| Rerun safety | No checkpoint: files already in the table (same name and modification time) are skipped; a changed file stops the run |
| Compared with | The **original landing CSVs** (`/Volumes/workspace/vstone_landing/source_files/`), read by column name |
| Runtime | Serverless Spark 4.2.0; CI tests on PySpark 4.2.0 (same version) |

## Runs

| Run | Job run ID | Start (local) | Duration | Result | Files loaded / skipped | Rows written |
|---|---|---|---|---|---|---|
| 1 | 243339843059660 | 2026-10-21 16:28:32 | 6.2 min | SUCCESS, attempt 0 | **31 / 0** | **16,695,000** |
| 2 | 960804117523419 | 2026-10-21 16:35:21 | 1.6 min | SUCCESS, attempt 0 | **0 / 31** | **0** |

Evidence: [`bronze_chunk4_load_report_run1.csv`](bronze_chunk4_load_report_run1.csv) / [`_run2`](bronze_chunk4_load_report_run2.csv),
[`bronze_chunk4_checks_run1.csv`](bronze_chunk4_checks_run1.csv) / [`_run2`](bronze_chunk4_checks_run2.csv) (31 rows, all `OK`, identical),
[`bronze_chunk4_value_sums_run1.csv`](bronze_chunk4_value_sums_run1.csv) / [`_run2`](bronze_chunk4_value_sums_run2.csv) (identical),
[`bronze_chunk4_summary_run1.csv`](bronze_chunk4_summary_run1.csv) / [`_run2`](bronze_chunk4_summary_run2.csv).

## Checks (run 1 → run 2)

| # | Check | Run 1 | Run 2 |
|---|---|---|---|
| 1 | Files in chunk folder / in table | 31 / 31 | 31 / 31 |
| 1 | Rows: original CSVs / table | 16,695,000 / 16,695,000 | 16,695,000 / 16,695,000 |
| 1 | Per-file rows and `sum(label)` equal to the original CSV | 31 / 31 OK (48,994 = 48,994) | 31 / 31 OK |
| 2 | `sum(CAPE)` per file vs original CSV | equal in **31 / 31** files | identical to run 1 |
| 2 | `sum(B14B)` per file vs original CSV | max relative difference **3.4 × 10⁻¹³** (tolerance 10⁻⁹) | identical to run 1 |
| 3 | Null satellite bands | 0 | 0 |
| 4 | Rows with `_rescued_data` / `_corrupt_record` / all keys and label null | 0 / 0 / 0 | 0 / 0 / 0 |
| 5 | Table columns, order and types = `rain_schema` | OK | OK |
| 6 | Duplicate natural keys / null audit columns / other `ingestion_method` | 0 / 0 / 0 | 0 / 0 / 0 |
| – | Distinct `load_run_id` in the table | 1 | 1 |

The `B14B` differences come only from adding doubles in a different order (rounding); `CAPE` sums are exactly equal.

## XML lessons applied

| Lesson (XML reference demo) | What was done | Evidence |
|---|---|---|
| XML has no types | Explicit typed schema; nothing inferred; misfits go to `_rescued_data` | 0 rescued rows; schema check OK |
| Schema inference sorts columns alphabetically | Elements matched **by name**; columns selected by name in table order | Schema check (names, order, types) OK; unit test reads alphabetically ordered elements into the right columns |
| Values can be lost in round trips | Per-file value sums compared with the original CSVs | `CAPE` exact, `B14B` ≤ 3.4 × 10⁻¹³ |
| Column names must be XML-safe; avoid tag clashes | All names are simple identifiers; row tag `record` (dataset has a `row` column) | Load succeeded with `rowTag = record` |

## Bad-record test

Evidence: [`xml_bad_record_test/`](xml_bad_record_test/) – the input file, the rows loaded, the job's failure message.

The same job and notebook were run with overrides `chunk_name=_test_bad_xml`, `config_chunk=chunk4_xml`,
`table_name=zz_test_bad_chunk4_xml`, `run_optimize=false` on a 5-record file
([`combined_test_bad.xml`](xml_bad_record_test/combined_test_bad.xml)):

| Record | Problem | Result ([`loaded_rows.csv`](xml_bad_record_test/loaded_rows.csv)) |
|---|---|---|
| 1 | none | loaded normally |
| 2 | `<CAPE>not_a_number</CAPE>` (wrong type) | `CAPE` null; `_rescued_data` = `{"CAPE":"not_a_number","_file_path":…}` |
| 3 | extra element `<EXTRA_FIELD>` | row loaded; `_rescued_data` = `{"EXTRA_FIELD":123,…}` |
| 4 | `<kx>` instead of `<KX>` | `KX` null; `_rescued_data` = `{"kx":7.25,…}` |
| 5 | malformed record (`<CAPE>1.5</record>`) | data columns null; **`_corrupt_record` = the raw XML of the record** |

The job **failed as designed** ([`job_failure_message.txt`](xml_bad_record_test/job_failure_message.txt)):
3 rows with rescued data, 1 corrupt record, 1 row with every key and label null. The first four messages in that
file are expected for a test file that has no original CSV to compare with (the wording "in table but not in chunk
folder" means "no source counts" here).

**Compared with chunk 3 (JSON):** there the malformed line arrived as an all-null row with nothing kept. For XML the
corrupt-record column keeps the raw text, so the bad input can be inspected and fixed.

**Cleanup:** the test folder and the scratch table `zz_test_bad_chunk4_xml` were deleted after saving the evidence
(XML loads have no checkpoint).

## Why reruns load nothing
`files_to_load` compares the chunk folder with the `source_file` / `source_file_modified_dt` audit columns: run 2
found all 31 files already loaded with unchanged modification times and wrote nothing. Each load is one Delta
commit, so a failed run leaves no partial files. A changed file stops the run; `mode = replace` reloads chosen files
with `replaceWhere`, without duplicates.
