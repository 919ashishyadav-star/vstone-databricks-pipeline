# Databricks notebook source
# MAGIC %md
# MAGIC # 04 – Bronze: chunk 3 JSON via Auto Loader
# MAGIC
# MAGIC **Purpose:** load the chunk 3 files (Apr 2020, gzip JSON) from the Bronze `raw_files` volume into the
# MAGIC Delta table `rain_forecast_chunk3_json` with **Auto Loader**, as-is, with audit columns.
# MAGIC
# MAGIC **Steps**
# MAGIC 1. Create the table if needed (explicit schema, description on the table and every column,
# MAGIC    Liquid Clustering) and re-apply the descriptions.
# MAGIC 2. **Checkpoint guard** – stop if the table has data but its checkpoint is missing (that would reload
# MAGIC    everything and create duplicates).
# MAGIC 3. **Auto Loader stream** with `trigger(availableNow=True)`: processes every new file in micro-batches,
# MAGIC    then stops – the only kind of trigger serverless supports.
# MAGIC 4. `OPTIMIZE` to cluster the new data (optional).
# MAGIC 5. Sanity checks – the notebook **fails** if any check fails.
# MAGIC
# MAGIC **Explicit schema:** the JSON values are already typed, so the schema uses the table's real types. A value
# MAGIC that does not fit (wrong type, unexpected field) becomes null and is kept in `_rescued_data` – the row is
# MAGIC never dropped, and the checks fail if any row has rescued data.
# MAGIC
# MAGIC **Rerunnable:** the checkpoint records every file already loaded, so a second run loads 0 rows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("bronze_schema", "vstone_dev_bronze", "Bronze schema")
dbutils.widgets.text("raw_volume", "raw_files", "Raw (chunk) volume")
dbutils.widgets.text("chunk_name", "chunk3_json", "Chunk folder")
dbutils.widgets.text("table_name", "rain_forecast_chunk3_json", "Bronze table")
dbutils.widgets.text("checkpoint_volume", "checkpoints", "Checkpoint volume")
dbutils.widgets.text("cluster_by", "day,hour", "Liquid Clustering columns")
dbutils.widgets.text("max_files_per_trigger", "10", "Files per micro-batch")
dbutils.widgets.text("load_run_id", "", "Load run ID (job run ID)")
dbutils.widgets.dropdown("run_optimize", "true", ["true", "false"], "Run OPTIMIZE")

names = ["catalog", "bronze_schema", "raw_volume", "chunk_name", "table_name", "checkpoint_volume",
         "cluster_by", "max_files_per_trigger", "load_run_id", "run_optimize"]
params = {name: dbutils.widgets.get(name).strip() for name in names}
empty = [n for n, v in params.items() if not v and n != "load_run_id"]
if empty:
    raise ValueError(f"These widgets must not be empty: {empty}")

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F

src_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from common.auto_loader import INGESTION_METHOD, auto_loader_options, checkpoint_paths, select_expressions, volume_path
from common.bronze_audit import resolve_run_id
from common.bronze_checks import (
    CHECK_SCHEMA,
    audit_check_sql,
    compare_file_stats,
    duplicate_keys_sql,
    load_batches_sql,
    table_file_stats_sql,
)
from common.rain_schema import (
    SATELLITE_BANDS,
    bronze_columns,
    comment_sql,
    create_table_sql,
    full_table_name,
    quote_identifier,
    spark_schema,
)

catalog, bronze_schema = params["catalog"], params["bronze_schema"]
table = full_table_name(catalog, bronze_schema, params["table_name"])
source_dir = volume_path(catalog, bronze_schema, params["raw_volume"], "chunks", params["chunk_name"])
checkpoint, schema_location = checkpoint_paths(catalog, bronze_schema, params["checkpoint_volume"], params["table_name"])
cluster_by = [c.strip() for c in params["cluster_by"].split(",") if c.strip()]
run_id = resolve_run_id(params["load_run_id"])
columns = bronze_columns(include_rescued=True)
table_comment = (
    f"Bronze: Rain Forecast {params['chunk_name']} loaded as-is with Auto Loader from {source_dir}. "
    "One row per grid cell per hour; natural key row, col, year, month, day, hour. "
    "Values kept exactly as in the source (CIN = 9999 and satellite band = 0 mean missing). "
    "Values that did not fit the schema are kept in _rescued_data."
)

print(f"Table           : {table}")
print(f"Source          : {source_dir}")
print(f"Checkpoint      : {checkpoint}")
print(f"Schema location : {schema_location}")
print(f"Clustering      : {cluster_by}")
print(f"Run ID          : {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 – Create the table and apply descriptions

# COMMAND ----------

spark.sql(create_table_sql(table, table_comment, cluster_by, columns))
spark.sql(f"ALTER TABLE {table} CLUSTER BY ({', '.join(quote_identifier(c) for c in cluster_by)})")
for statement in comment_sql(table, table_comment, columns):
    spark.sql(statement)

display(spark.sql(f"DESCRIBE TABLE {table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 – Checkpoint guard
# MAGIC
# MAGIC The checkpoint is the only record of which files are already in the table. If the table has rows but the
# MAGIC checkpoint is gone, Auto Loader would load every file again and create duplicates – so stop instead.
# MAGIC A deliberate reset means emptying the table **and** deleting its checkpoint folder together.

# COMMAND ----------

existing_rows = spark.sql(f"SELECT count(*) AS n FROM {table}").first()["n"]
checkpoint_has_state = os.path.isdir(checkpoint) and any(n != "_schema" for n in os.listdir(checkpoint))
print(f"Rows already in table: {existing_rows:,}; checkpoint has state: {checkpoint_has_state}")
if existing_rows and not checkpoint_has_state:
    raise RuntimeError(
        f"{table} has {existing_rows:,} rows but checkpoint {checkpoint} is missing or empty. "
        "Running would reload every file and create duplicates. To reset deliberately, empty the table "
        "and delete the checkpoint folder together."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 – Auto Loader stream (`availableNow`)
# MAGIC
# MAGIC Options come from `src/common/auto_loader.py` (unit tested). `maxFilesPerTrigger` splits the load into
# MAGIC micro-batches; the stream stops by itself when every new file is processed. A rerun processes 0 rows.

# COMMAND ----------

options = auto_loader_options("json", params["max_files_per_trigger"], schema_location)
for key, value in options.items():
    print(f"  {key} = {value}")

run_started = str(spark.sql("SELECT current_timestamp() AS t").first()["t"])
query = (
    spark.readStream.format("cloudFiles")
    .options(**options)
    .schema(spark_schema())
    .load(source_dir)
    .selectExpr(*select_expressions(run_id))
    .writeStream.option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable(table)
)
query.awaitTermination()

# What this run wrote, per micro-batch (each micro-batch stamps its own load_dt).
batches_df = spark.sql(load_batches_sql(table, run_id, run_started))
batch_rows = batches_df.collect()
inserted_rows = sum(r["rows"] for r in batch_rows)
inserted_files = sum(r["files"] for r in batch_rows)
display(batches_df)
print(f"Micro-batches with data: {len(batch_rows)}; files: {inserted_files}; rows inserted by this run: {inserted_rows:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 – OPTIMIZE (Liquid Clustering)

# COMMAND ----------

if params["run_optimize"] == "true":
    display(spark.sql(f"OPTIMIZE {table}"))
else:
    print("OPTIMIZE skipped (run_optimize = false)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 – Sanity checks
# MAGIC
# MAGIC | # | Check | Expected |
# MAGIC |---|---|---|
# MAGIC | 1 | Rows and `sum(label)` per file in the table = per chunk file (read in batch with the same explicit schema); same set of files | equal |
# MAGIC | 2 | Satellite bands: no nulls for files that have them | 0 |
# MAGIC | 3 | Rows with rescued data; rows with every key and label null (malformed) | 0 / 0 |
# MAGIC | 4 | Duplicate natural keys | 0 |
# MAGIC | 5 | Audit columns never null; `ingestion_method` = AUTO_LOADER | 0 problems |

# COMMAND ----------

chunk_paths = sorted(
    f"{source_dir}/{n}" for n in os.listdir(source_dir) if os.path.isfile(f"{source_dir}/{n}") and not n.startswith((".", "_"))
)
bands_all_null = F.lit(True)
for b in SATELLITE_BANDS:
    bands_all_null = bands_all_null & F.col(b).isNull()
source = {
    r["file_name"]: r
    for r in spark.read.format("json").schema(spark_schema()).load(chunk_paths)
    .groupBy(F.col("_metadata.file_name").alias("file_name"))
    .agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum("label").alias("label_sum"),
        F.sum(F.when(bands_all_null, 1).otherwise(0)).alias("rows_all_bands_null"),
    )
    .collect()
}
source_stats = {name: (r["rows"], r["label_sum"]) for name, r in source.items()}
has_bands = {name: r["rows_all_bands_null"] < r["rows"] for name, r in source.items()}

table_stats = {
    r["source_file"]: r.asDict()
    for r in spark.sql(table_file_stats_sql(table, include_rescued=True)).collect()
}
check_rows, problems = compare_file_stats(source_stats, table_stats, has_bands)
check_df = spark.createDataFrame(check_rows, CHECK_SCHEMA)
display(check_df.orderBy(F.col("status") == "OK", "source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Rows with rescued data** (empty = every value fitted the schema)

# COMMAND ----------

display(spark.sql(f"SELECT source_file, _rescued_data, * EXCEPT (source_file, _rescued_data) FROM {table} WHERE _rescued_data IS NOT NULL LIMIT 20"))

# COMMAND ----------

duplicate_keys = spark.sql(duplicate_keys_sql(table)).first()["n"]
audit = spark.sql(audit_check_sql(table, INGESTION_METHOD)).first().asDict()
rescued_total = sum(t.get("rows_rescued", 0) for t in table_stats.values())

if duplicate_keys:
    problems.append(f"{duplicate_keys:,} duplicate natural keys")
if audit["rows_null_audit"]:
    problems.append(f"{audit['rows_null_audit']:,} rows with a null audit column")
if audit["rows_other_method"]:
    problems.append(f"{audit['rows_other_method']:,} rows with ingestion_method other than {INGESTION_METHOD}")

summary = [
    ("Files in chunk folder", str(len(source_stats))),
    ("Files in table", str(len(table_stats))),
    ("Rows in chunk files", f"{sum(v[0] for v in source_stats.values()):,}"),
    ("Rows in table", f"{audit['total_rows']:,}"),
    ("Rows inserted by this run", f"{inserted_rows:,}"),
    ("Files loaded by this run", str(inserted_files)),
    ("Micro-batches with data in this run", str(len(batch_rows))),
    ("Rows with rescued data", f"{rescued_total:,}"),
    ("Duplicate natural keys", f"{duplicate_keys:,}"),
    ("Rows with null audit columns", f"{audit['rows_null_audit']:,}"),
    ("Distinct load_run_id values", str(audit["load_runs"])),
    ("Result", "PASS" if not problems else "FAIL"),
]
display(spark.createDataFrame(summary, "check string, value string"))

# COMMAND ----------

if problems:
    raise AssertionError("Bronze Auto Loader sanity check failed:\n- " + "\n- ".join(problems))
print(f"All checks passed: {audit['total_rows']:,} rows from {len(table_stats)} files in {table}.")
