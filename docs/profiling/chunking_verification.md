# Data Chunking – verification (dev)

Verification of the `[dev] Data Chunking` job (`src/notebooks/02_data_chunking.py`), which
splits the 122 source CSVs in `/Volumes/workspace/vstone_landing/source_files/` into four
chunks in `/Volumes/workspace/vstone_dev_bronze/raw_files/chunks/`.

## Runs

| Run | Run ID | Start (local) | Duration | Result | What it did |
|---|---|---|---|---|---|
| 1 | 262581471753754 | 2026-10-17 23:38:31 | 14.7 min | SUCCESS, attempt 0 | 61 CSVs copied, 61 days converted (30 JSON, 31 XML) |
| 2 | 851989939016428 | 2026-10-18 00:06:31 | 5.6 min | SUCCESS, attempt 0 | all 122 outputs **skipped (exists)**; sanity check only |

Per-file sanity-check tables, exported from each run's own notebook output:
[`chunking_sanity_check_run1.csv`](chunking_sanity_check_run1.csv),
[`chunking_sanity_check_run2.csv`](chunking_sanity_check_run2.csv)
(122 rows each, all `OK`; the two files are identical).

## Checks

### 1. File counts and sizes per chunk folder

| Chunk | Files (expected) | Output size | Source size | Notes |
|---|---|---|---|---|
| chunk1_csv | **30** (30) | 4,827,197,774 bytes (4.50 GB) | same | plain copies |
| chunk2_csv | **31** (31) | 4,385,066,220 bytes (4.08 GB) | same | plain copies |
| chunk3_json | **30** (30) | 915,318,692 bytes (0.85 GB) | 4.67 GB CSV | gzip; estimate was ~0.7 GB |
| chunk4_xml | **31** (31) | 1,114,569,097 bytes (1.04 GB) | 4.86 GB CSV | gzip; estimate was ~0.8 GB |
| **Total** | **122** | **≈ 10.5 GB** | | |

- Every output has a fixed per-day name (`combined_YYYY_MM_DD.csv` / `.json.gz` / `.xml.gz`); no
  other files are in the chunk folders. The temporary `_staging` folder was left empty.

### 2. Row counts: output vs source

| Chunk | Files | Source rows | Output rows | Mismatched files |
|---|---|---|---|---|
| chunk1_csv | 30 | 16,200,000 | 16,200,000 | 0 |
| chunk2_csv | 31 | 14,107,500 | 14,107,500 | 0 |
| chunk3_json | 30 | 16,155,000 | 16,155,000 | 0 |
| chunk4_xml | 31 | 16,695,000 | 16,695,000 | 0 |
| **Total** | **122** | **63,157,500** | **63,157,500** | **0** |

Checked twice, independently:
- **By the job itself** (Step 3 of the notebook, PySpark with explicit schemas) – in both runs.
- **By a separate SQL query** on the Serverless Starter Warehouse after run 1, using
  `read_files()` over the source volume and each chunk folder, counting rows per file and joining on
  the day. Same totals, 0 mismatches.

### 3. CSV chunks are byte-identical copies

- All 61 files in `chunk1_csv` and `chunk2_csv` have **exactly the same size** as their source file.
- `combined_2019_04_27.csv` (one of the 27-column files) was downloaded from `chunk1_csv` and
  hashed: **SHA256 identical** to the original in `C:\FILES\FINAL`
  (`50BDC3C3A9F9F0C8167B187C4C4527EF34249BF157CEAE6E5548B81064CD677B`).

### 4. The 27-column files are preserved

- Reading `chunk1_csv/combined_2019_04_27.csv` returns **27 columns** (no satellite bands), exactly as
  in the source. Bronze (COPY INTO) must therefore match columns by name (decision 12).

### 5. JSON and XML have the 36 data columns and no extra columns

- `chunk3_json/combined_2020_04_01.json.gz` and `chunk4_xml/combined_2020_10_01.xml.gz` each read back
  with **36 columns** – the dataset columns only. There is **no `source_file` column** and no
  temporary column from the chunking job (decision 4); Bronze takes the file name from
  `_metadata.file_name`.
- XML uses `records` / `record` tags (decision 14).

### 6. A rerun skips everything and creates no duplicates

- Run 2 (default mode `skip_existing`) reported **skipped (exists)** for all 122 outputs and wrote
  nothing.
- File modification times: all 122 outputs were written **during run 1** (23:39:39–23:49:15 local);
  **none** were written at or after run 2's start (00:06:31).
- After run 2 the file counts (30 / 31 / 30 / 31), folder sizes and row counts are unchanged, and
  run 2's own sanity check is identical to run 1's.
- Because every output keeps the same path, COPY INTO / Auto Loader will not see new files after a
  rerun, so Bronze cannot load the same day twice (decision 11).

## Notes

- `databricks fs ls --long` on this machine prints **local** time with a `Z` suffix; times above are
  local (IST).
- Estimated vs actual compressed sizes: JSON 0.85 GB (est. ~0.7), XML 1.04 GB (est. ~0.8) – within
  the ±15–30% expected from the 20,000-row sample.
