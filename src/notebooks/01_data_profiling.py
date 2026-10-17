# Databricks notebook source
# MAGIC %md
# MAGIC # 01 – Data profiling: Rain Forecast source files
# MAGIC
# MAGIC **Purpose:** profile the original Kaggle Rain Forecast CSVs in the shared landing volume
# MAGIC **before** any Bronze ingestion, so data issues are known up front and the Bronze/Silver
# MAGIC rules are based on evidence.
# MAGIC
# MAGIC **What it checks**
# MAGIC
# MAGIC | # | Check | Why |
# MAGIC |---|---|---|
# MAGIC | 1 | File inventory and header variants | Are all files present? Do they all have the same columns? |
# MAGIC | 2 | Rows, columns and hours per file | Find incomplete days (a full day = grid cells × 24 hours) |
# MAGIC | 3 | Null counts per column | Find missing or unparseable values |
# MAGIC | 4 | Duplicates on the natural key | Is `row, col, year, month, day, hour` really unique? |
# MAGIC | 5 | Sentinel values: `CIN = 9999`, satellite band = 0 | "Hidden" missing values that are not nulls |
# MAGIC | 6 | Label distribution | How rare is rain? |
# MAGIC
# MAGIC **Inputs:** the widgets below (catalog, schema, volume, file pattern). Nothing is hard-coded,
# MAGIC so the same notebook works for any environment or a subset of files.
# MAGIC
# MAGIC **Side effects:** none – the notebook only **reads** files. It creates no tables and writes no files,
# MAGIC so it is safe to re-run any number of times.
# MAGIC
# MAGIC **Compute:** serverless. Each check scans the CSVs (~18 GB); a full run takes several minutes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC
# MAGIC Widgets make the notebook configurable. When run from a job, the job passes these values
# MAGIC (from bundle variables); when run by hand, edit them at the top of the notebook.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("landing_schema", "vstone_landing", "Landing schema")
dbutils.widgets.text("landing_volume", "source_files", "Landing volume")
dbutils.widgets.text("file_pattern", "combined_*.csv", "File name pattern")

params = {
    name: dbutils.widgets.get(name).strip()
    for name in ["catalog", "landing_schema", "landing_volume", "file_pattern"]
}
empty = [name for name, value in params.items() if not value]
if empty:
    raise ValueError(f"These widgets must not be empty: {empty}")

catalog = params["catalog"]
landing_schema = params["landing_schema"]
landing_volume = params["landing_volume"]
file_pattern = params["file_pattern"]

# Volume paths always have the form /Volumes/<catalog>/<schema>/<volume>/
source_dir = f"/Volumes/{catalog}/{landing_schema}/{landing_volume}"
print(f"Profiling files matching '{file_pattern}' in {source_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataset constants
# MAGIC
# MAGIC These describe the **dataset's structure** (not the environment), so they live here rather than in
# MAGIC widgets. See `docs/requirements_and_assumptions.md` for where they come from.

# COMMAND ----------

import fnmatch
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql import types as T

# One reading per grid cell per hour.
NATURAL_KEY = ["row", "col", "year", "month", "day", "hour"]
# Whole-number columns; every other column is a decimal measurement.
INT_COLUMNS = set(NATURAL_KEY + ["label"])
# Satellite brightness temperatures (Kelvin). 0 K is physically impossible -> means "missing".
SATELLITE_BANDS = ["B09B", "B10B", "B11B", "B12B", "B14B", "B16B", "I2B", "IRB", "WVB"]
# CIN uses 9999 to mean "no value".
CIN_SENTINEL = 9999
HOURS_PER_DAY = 24
# File names look like combined_2019_04_01.csv; this pulls out the date part.
FILE_DATE_REGEX = r"combined_(\d{4}_\d{2}_\d{2})\.csv"


def pct(part, whole):
    """Percentage rounded to 4 decimals; 0 when the whole is 0."""
    return round(100.0 * part / whole, 4) if whole else 0.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 1 – File inventory and header variants
# MAGIC
# MAGIC **What:** list the files in the volume, and read just the **first line (header)** of each file.
# MAGIC
# MAGIC **Why the header matters:** Spark's CSV reader takes the column names from the *first* file and
# MAGIC then matches columns in the other files **by position**. If some files have fewer columns, their
# MAGIC values would silently land in the wrong columns. So we group files by header first and read
# MAGIC each group with its own schema.
# MAGIC
# MAGIC **Expected:** 122 files, no other files. Known finding: 4 files (2019-04-27 to 2019-04-30) have
# MAGIC 27 columns – the 9 satellite bands are missing.

# COMMAND ----------

all_entries = dbutils.fs.ls(source_dir)
files = sorted(
    (f for f in all_entries if not f.name.endswith("/") and fnmatch.fnmatch(f.name, file_pattern)),
    key=lambda f: f.name,
)
other_entries = sorted(f.name for f in all_entries if f not in files)
if not files:
    raise ValueError(f"No files matching '{file_pattern}' found in {source_dir}")


def read_header(path):
    """Return the column names from the first line of a CSV file (reads only the first 4 KB)."""
    first_line = dbutils.fs.head(path, 4096).splitlines()[0]
    return [c.strip() for c in first_line.split(",")]


inventory_rows = []
for f in files:
    header = read_header(f.path)
    inventory_rows.append((f.name, f.path, f.size, len(header), ",".join(header)))

inventory_df = spark.createDataFrame(
    inventory_rows,
    "file_name string, path string, size_bytes long, column_count int, header string",
)

print(f"Files matching pattern : {len(files)}")
print(f"Total size             : {sum(f.size for f in files):,} bytes ({sum(f.size for f in files) / 1024**3:.2f} GB)")
print(f"Other entries in volume: {other_entries if other_entries else 'none'}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Files per month**

# COMMAND ----------

display(
    inventory_df
    .withColumn("month", F.regexp_extract("file_name", r"combined_(\d{4}_\d{2})_", 1))
    .groupBy("month")
    .agg(F.count("*").alias("files"), F.round(F.sum("size_bytes") / 1024**3, 2).alias("size_gb"))
    .orderBy("month")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Header variants** – one row per distinct header. More than one row means the files do not all
# MAGIC have the same columns.

# COMMAND ----------

display(
    inventory_df
    .groupBy("column_count", "header")
    .agg(
        F.count("*").alias("files"),
        F.min("file_name").alias("first_file"),
        F.max("file_name").alias("last_file"),
    )
    .orderBy(F.desc("files"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the data
# MAGIC
# MAGIC Each header variant is read with an **explicit schema** (no `inferSchema`, which would scan
# MAGIC 18 GB just to guess types and can guess wrong). `enforceSchema=false` makes Spark check that
# MAGIC each file's header really matches the schema. The groups are then combined with
# MAGIC `unionByName(allowMissingColumns=True)`, which matches columns **by name** and fills
# MAGIC columns a file does not have with null.
# MAGIC
# MAGIC `source_file` comes from Spark's hidden `_metadata` column, so every row knows which file it
# MAGIC came from.
# MAGIC
# MAGIC Nothing is computed yet – Spark only runs the work when a check below needs a result.

# COMMAND ----------

def schema_for(columns):
    return T.StructType(
        [T.StructField(c, T.IntegerType() if c in INT_COLUMNS else T.DoubleType(), True) for c in columns]
    )


paths_by_header = {}
for file_name, path, _size, _count, header in inventory_rows:
    paths_by_header.setdefault(header, []).append(path)

frames = []
for header, paths in paths_by_header.items():
    frames.append(
        spark.read.format("csv")
        .option("header", "true")
        .option("enforceSchema", "false")
        .option("mode", "PERMISSIVE")  # unparseable values become null (counted in check 3)
        .schema(schema_for(header.split(",")))
        .load(paths)
        .withColumn("source_file", F.col("_metadata.file_name"))
    )

raw_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)

# Column order from the widest header, so the 36-column layout is used.
data_columns = max((h.split(",") for h in paths_by_header), key=len)
raw_df = raw_df.select(*data_columns, "source_file")
bands_present = [b for b in SATELLITE_BANDS if b in data_columns]

print(f"Header variants: {len(paths_by_header)}; columns after union: {len(data_columns)}")
raw_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 2 – Rows, columns and hours per file
# MAGIC
# MAGIC **What:** for every file, count data rows, distinct hours and distinct grid rows/cols, and check
# MAGIC that the `year/month/day` columns match the date in the file name.
# MAGIC
# MAGIC **Expected rows per file** are not hard-coded: they are worked out from the data as
# MAGIC *grid rows × grid cols × 24 hours* (90 × 250 × 24 = 540,000 for this dataset).
# MAGIC
# MAGIC **Known finding:** 19 files are missing whole hours (mostly Oct 2019); `combined_2019_10_20.csv`
# MAGIC has only 1 hour.

# COMMAND ----------

file_date = F.to_date(F.regexp_extract("source_file", FILE_DATE_REGEX, 1), "yyyy_MM_dd")

per_file_stats = (
    raw_df.groupBy("source_file")
    .agg(
        F.count(F.lit(1)).alias("data_rows"),
        F.countDistinct("hour").alias("distinct_hours"),
        F.countDistinct("row").alias("grid_rows"),
        F.countDistinct("col").alias("grid_cols"),
        F.sum((F.make_date("year", "month", "day") != file_date).cast("long")).alias("rows_date_mismatch"),
    )
    .collect()  # only 122 small rows
)

grid_rows = max(r["grid_rows"] for r in per_file_stats)
grid_cols = max(r["grid_cols"] for r in per_file_stats)
expected_rows = grid_rows * grid_cols * HOURS_PER_DAY
print(f"Grid: {grid_rows} rows x {grid_cols} cols -> expected rows per complete file = {expected_rows:,}")

per_file_df = (
    spark.createDataFrame([r.asDict() for r in per_file_stats])
    .withColumnRenamed("source_file", "file_name")
    .join(inventory_df.select("file_name", "size_bytes", "column_count"), "file_name")
    .withColumn("expected_rows", F.lit(expected_rows))
    .withColumn("missing_rows", F.col("expected_rows") - F.col("data_rows"))
    .withColumn(
        "is_complete",
        (F.col("data_rows") == F.col("expected_rows"))
        & (F.col("distinct_hours") == HOURS_PER_DAY)
        & (F.coalesce(F.col("rows_date_mismatch"), F.lit(0)) == 0),
    )
    .select(
        "file_name", "size_bytes", "column_count", "data_rows", "expected_rows", "missing_rows",
        "distinct_hours", "grid_rows", "grid_cols", "rows_date_mismatch", "is_complete",
    )
    .orderBy("file_name")
)

total_rows = sum(r["data_rows"] for r in per_file_stats)
incomplete = [r for r in per_file_df.collect() if not r["is_complete"]]
print(f"Total rows: {total_rows:,} (expected if all complete: {expected_rows * len(files):,})")
print(f"Complete files: {len(files) - len(incomplete)}; incomplete files: {len(incomplete)}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Incomplete files only** (rows or hours below expected, or dates not matching the file name)

# COMMAND ----------

display(per_file_df.filter(~F.col("is_complete")))

# COMMAND ----------

# MAGIC %md
# MAGIC **All files**

# COMMAND ----------

display(per_file_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks 3, 5 and 6 – Nulls, sentinel values and label distribution
# MAGIC
# MAGIC These three checks are computed in **one pass** over the data (one aggregation with many
# MAGIC counters), instead of scanning 18 GB three times. Serverless compute does not support
# MAGIC `cache()`, so combining work like this is how we keep the run fast.

# COMMAND ----------

def any_of(conditions):
    return reduce(lambda a, b: a | b, conditions)


def all_of(conditions):
    return reduce(lambda a, b: a & b, conditions)


def count_where(condition, alias):
    """Count rows where the condition is true (null counts as false)."""
    return F.sum(F.when(condition, 1).otherwise(0)).cast("long").alias(alias)


agg_exprs = [F.count(F.lit(1)).alias("total_rows")]
agg_exprs += [count_where(F.col(c).isNull(), f"null__{c}") for c in data_columns]
agg_exprs += [
    count_where(F.col("CIN") == CIN_SENTINEL, "cin_sentinel"),
    count_where(F.col("CIN").isNotNull(), "cin_not_null"),
]
if bands_present:
    agg_exprs += [count_where(F.col(b) == 0, f"zero__{b}") for b in bands_present]
    agg_exprs += [
        count_where(any_of([F.col(b).isNotNull() for b in bands_present]), "rows_with_bands"),
        count_where(any_of([F.col(b) == 0 for b in bands_present]), "rows_any_band_zero"),
        count_where(all_of([F.col(b) == 0 for b in bands_present]), "rows_all_bands_zero"),
    ]
agg_exprs += [
    count_where(F.col("label") == 1, "label_1"),
    count_where(F.col("label") == 0, "label_0"),
    count_where(F.col("label").isNull(), "label_null"),
    count_where(~F.col("label").isin(0, 1), "label_other"),
]

stats = raw_df.agg(*agg_exprs).first().asDict()
print(f"Rows scanned: {stats['total_rows']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 3 – Null counts per column
# MAGIC
# MAGIC **What:** how many values are null in each column. A null here means either the file has no
# MAGIC such column, or the value could not be parsed as a number.
# MAGIC
# MAGIC **Expected nulls** are the rows from files whose header does not include that column (the 4
# MAGIC files without satellite bands). **Unexpected nulls** should be 0 for every column – anything else
# MAGIC means empty or malformed values in the source.

# COMMAND ----------

rows_by_file = {r["source_file"]: r["data_rows"] for r in per_file_stats}
headers_by_file = {name: header.split(",") for name, _p, _s, _c, header in inventory_rows}

null_rows = []
for c in data_columns:
    nulls = stats[f"null__{c}"]
    expected_nulls = sum(rows_by_file.get(name, 0) for name, cols in headers_by_file.items() if c not in cols)
    null_rows.append((c, nulls, pct(nulls, stats["total_rows"]), expected_nulls, nulls - expected_nulls))

null_df = spark.createDataFrame(
    null_rows, "column string, null_count long, null_pct double, expected_nulls long, unexpected_nulls long"
)
display(null_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check 4 – Duplicates on the natural key
# MAGIC
# MAGIC **What:** group all rows by `row, col, year, month, day, hour` across the **whole dataset** and
# MAGIC look for groups with more than one row.
# MAGIC
# MAGIC **Why:** the natural key must be unique for Bronze/Silver deduplication rules, MERGE INTO and
# MAGIC the Gold fact table's primary key. Duplicates could also come from two files containing the same
# MAGIC date.
# MAGIC
# MAGIC **Expected:** 0 duplicate keys (none were found in the sampled files).

# COMMAND ----------

dup_keys_df = (
    raw_df.groupBy(*NATURAL_KEY)
    .agg(
        F.count(F.lit(1)).alias("occurrences"),
        F.min("source_file").alias("first_file"),
        F.max("source_file").alias("last_file"),
    )
    .filter(F.col("occurrences") > 1)
)

dup_summary = dup_keys_df.agg(
    F.count(F.lit(1)).alias("duplicate_keys"),
    F.coalesce(F.sum(F.col("occurrences") - 1), F.lit(0)).alias("extra_rows"),
).first()

print(f"Duplicate natural keys: {dup_summary['duplicate_keys']:,}")
print(f"Extra (duplicate) rows: {dup_summary['extra_rows']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Examples of duplicate keys** (empty table = no duplicates)

# COMMAND ----------

display(dup_keys_df.orderBy(F.desc("occurrences"), *NATURAL_KEY).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 5 – Sentinel values (hidden missing values)
# MAGIC
# MAGIC **What:** values that are not null but mean "missing":
# MAGIC - **`CIN = 9999`** – the dataset's "no value" marker for CIN.
# MAGIC - **Satellite band = 0** – a brightness temperature of 0 Kelvin is physically impossible.
# MAGIC
# MAGIC Band percentages are measured only over rows that **have** satellite columns (the 4 files without
# MAGIC bands are excluded from the denominator).
# MAGIC
# MAGIC **Known finding:** `CIN = 9999` in ~47% of rows; satellite bands = 0 in ~2.2% of rows, in
# MAGIC whole-hour blocks. Silver will turn these into nulls / quarantine (decision 5).

# COMMAND ----------

sentinel_rows = [
    ("CIN = 9999", "CIN", stats["cin_sentinel"], stats["cin_not_null"], pct(stats["cin_sentinel"], stats["cin_not_null"])),
]
if bands_present:
    with_bands = stats["rows_with_bands"]
    sentinel_rows += [
        (f"{b} = 0", b, stats[f"zero__{b}"], with_bands, pct(stats[f"zero__{b}"], with_bands)) for b in bands_present
    ]
    sentinel_rows += [
        ("any satellite band = 0", "all bands", stats["rows_any_band_zero"], with_bands, pct(stats["rows_any_band_zero"], with_bands)),
        ("all satellite bands = 0", "all bands", stats["rows_all_bands_zero"], with_bands, pct(stats["rows_all_bands_zero"], with_bands)),
    ]

sentinel_df = spark.createDataFrame(
    sentinel_rows, "check string, column string, matching_rows long, rows_checked long, pct double"
)
display(sentinel_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 6 – Label distribution
# MAGIC
# MAGIC **What:** how many rows are rain (`label = 1`) vs no rain (`label = 0`), plus any null or
# MAGIC unexpected label values.
# MAGIC
# MAGIC **Why:** rain is expected to be very rare (~0.2–0.3%). This matters for Gold: rain frequency
# MAGIC must be reported as a **rate** (decision 8), and any later model would face a heavily
# MAGIC imbalanced target.

# COMMAND ----------

label_rows = [
    ("1 (rain)", stats["label_1"], pct(stats["label_1"], stats["total_rows"])),
    ("0 (no rain)", stats["label_0"], pct(stats["label_0"], stats["total_rows"])),
    ("null", stats["label_null"], pct(stats["label_null"], stats["total_rows"])),
    ("other value", stats["label_other"], pct(stats["label_other"], stats["total_rows"])),
]
display(spark.createDataFrame(label_rows, "label string, rows long, pct double"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC One row per check. `status` is **OK** when the result matches expectations and **REVIEW** when it
# MAGIC needs a decision (known findings are listed in `docs/requirements_and_assumptions.md`).

# COMMAND ----------

unexpected_null_total = sum(r[4] for r in null_rows)
summary_rows = [
    ("1. Files found", f"{len(files)} files, {len(other_entries)} other entries", "OK" if not other_entries else "REVIEW"),
    ("1. Header variants", f"{len(paths_by_header)} variant(s)", "OK" if len(paths_by_header) == 1 else "REVIEW"),
    ("2. Total rows", f"{total_rows:,} of {expected_rows * len(files):,} expected", "OK" if total_rows == expected_rows * len(files) else "REVIEW"),
    ("2. Incomplete files", f"{len(incomplete)} file(s)", "OK" if not incomplete else "REVIEW"),
    ("3. Unexpected nulls", f"{unexpected_null_total:,}", "OK" if unexpected_null_total == 0 else "REVIEW"),
    ("4. Duplicate natural keys", f"{dup_summary['duplicate_keys']:,} keys, {dup_summary['extra_rows']:,} extra rows", "OK" if dup_summary["duplicate_keys"] == 0 else "REVIEW"),
    ("5. CIN = 9999", f"{pct(stats['cin_sentinel'], stats['cin_not_null'])}% of rows", "REVIEW" if stats["cin_sentinel"] else "OK"),
]
if bands_present:
    summary_rows.append(
        ("5. Any satellite band = 0", f"{pct(stats['rows_any_band_zero'], stats['rows_with_bands'])}% of rows with bands", "REVIEW" if stats["rows_any_band_zero"] else "OK")
    )
summary_rows.append(
    ("6. Rain rate (label = 1)", f"{pct(stats['label_1'], stats['total_rows'])}%", "OK" if stats["label_null"] == 0 and stats["label_other"] == 0 else "REVIEW")
)

display(spark.createDataFrame(summary_rows, "check string, result string, status string"))
