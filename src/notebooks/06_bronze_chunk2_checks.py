# Databricks notebook source
# MAGIC %md
# MAGIC # 06 – Bronze: chunk 2 CSV checks (after the Declarative Pipeline)
# MAGIC
# MAGIC **Purpose:** sanity checks for `rain_forecast_chunk2_csv`, which is loaded by the Lakeflow Declarative
# MAGIC Pipeline in `src/pipelines/bronze_chunk2_csv.py`. The job runs this notebook **after** the pipeline update.
# MAGIC
# MAGIC Why a separate notebook: the pipeline owns its table and only declares it, so the checks the other Bronze
# MAGIC notebooks run at the end live here. The pipeline's expectations only **warn** (Bronze keeps raw rows);
# MAGIC this notebook turns any warning into a **job failure** (and the failure email).
# MAGIC
# MAGIC **Steps**
# MAGIC 1. Rows per load (`load_dt`) – shows which update loaded which files.
# MAGIC 2. Expectation metrics per update from the pipeline's event log.
# MAGIC 3. Sanity checks against the chunk files – the notebook **fails** if any check fails.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("bronze_schema", "vstone_dev_bronze", "Bronze schema")
dbutils.widgets.text("raw_volume", "raw_files", "Raw (chunk) volume")
dbutils.widgets.text("chunk_name", "chunk2_csv", "Chunk folder")
dbutils.widgets.text("table_name", "rain_forecast_chunk2_csv", "Bronze table")
dbutils.widgets.text("event_log_table", "rain_forecast_chunk2_csv_event_log", "Pipeline event log table")
dbutils.widgets.text("chunk_plan_path", "", "Path of src/config/chunk_plan.json (empty = from this repo)")

# COMMAND ----------

names = ["catalog", "bronze_schema", "raw_volume", "chunk_name", "table_name", "event_log_table", "chunk_plan_path"]
params = {name: dbutils.widgets.get(name).strip() for name in names}
empty = [n for n, v in params.items() if not v and n != "chunk_plan_path"]
if empty:
    raise ValueError(f"These widgets must not be empty: {empty}")

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F

src_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from common.auto_loader import volume_path
from common.bronze_checks import (
    CHECK_SCHEMA,
    audit_check_sql,
    compare_file_stats,
    duplicate_keys_sql,
    schema_problems,
    table_file_stats_sql,
)
from common.chunk_plan import find_chunk, load_chunk_plan
from common.copy_into import cast_expressions
from common.declarative_pipeline import (
    FAIL_EXPECTATIONS,
    INGESTION_METHOD,
    expectation_metrics_sql,
    table_columns,
    violation_counts_sql,
    warn_expectations,
)
from common.rain_schema import SATELLITE_BANDS, full_table_name

catalog, bronze_schema = params["catalog"], params["bronze_schema"]
table = full_table_name(catalog, bronze_schema, params["table_name"])
event_log_table = full_table_name(catalog, bronze_schema, params["event_log_table"])
source_dir = volume_path(catalog, bronze_schema, params["raw_volume"], "chunks", params["chunk_name"])
chunk_plan_path = params["chunk_plan_path"] or os.path.join(src_root, "config", "chunk_plan.json")
months = find_chunk(load_chunk_plan(chunk_plan_path), params["chunk_name"])["months"]
columns = table_columns()

print(f"Table     : {table}")
print(f"Event log : {event_log_table}")
print(f"Source    : {source_dir}")
print(f"Months    : {months}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 – Rows per load
# MAGIC
# MAGIC Each micro-batch stamps its own `load_dt`, so this shows every batch that is still in the table
# MAGIC (a full refresh replaces all of them).

# COMMAND ----------

display(spark.sql(
    f"SELECT load_dt, load_run_id, count(*) AS rows, count(DISTINCT source_file) AS files,\n"
    f"       min(source_file) AS first_file, max(source_file) AS last_file\n"
    f"FROM {table} GROUP BY load_dt, load_run_id ORDER BY load_dt"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 – Expectation metrics per update (event log)
# MAGIC
# MAGIC Passed / failed records for each expectation, summed over the micro-batches of each pipeline update.
# MAGIC Failures in the **latest** update fail this notebook.

# COMMAND ----------

metrics_df = spark.sql(expectation_metrics_sql(event_log_table))
metrics = metrics_df.collect()
display(metrics_df)

latest_update = max(metrics, key=lambda r: r["first_event"])["update_id"] if metrics else None
latest_failures = {
    r["expectation"]: r["failed_records"] for r in metrics if r["update_id"] == latest_update and r["failed_records"]
}
print(f"Latest update with data: {latest_update}; expectations with failed records: {latest_failures or 'none'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 – Sanity checks
# MAGIC
# MAGIC | # | Check | Expected |
# MAGIC |---|---|---|
# MAGIC | 1 | Rows and `sum(label)` per file in the table = per chunk file (read separately, columns **by header name**, cast to the table types); same set of files | equal |
# MAGIC | 2 | Satellite bands: no nulls for files that have them | 0 |
# MAGIC | 3 | Rows with rescued data; rows with every key and label null (malformed) | 0 / 0 |
# MAGIC | 4 | Every expectation re-checked on the whole table (same rules as the pipeline) | 0 failing rows |
# MAGIC | 5 | No failed records in the latest update's event log | 0 |
# MAGIC | 6 | Duplicate natural keys | 0 |
# MAGIC | 7 | Audit columns never null; `ingestion_method` = DECLARATIVE_PIPELINE | 0 problems |
# MAGIC | 8 | Table columns, order and types = `rain_schema` (data, audit, `_rescued_data`) | equal |

# COMMAND ----------

chunk_paths = sorted(
    f"{source_dir}/{n}" for n in os.listdir(source_dir) if os.path.isfile(f"{source_dir}/{n}") and not n.startswith((".", "_"))
)
bands_all_null = F.lit(True)
for b in SATELLITE_BANDS:
    bands_all_null = bands_all_null & F.col(b).isNull()
# Read as text by header name (no schema, no inference) and cast, so this comparison does not depend on the
# pipeline's reader. All chunk 2 files have the full 36-column header.
source = {
    r["file_name"]: r
    for r in spark.read.format("csv").option("header", "true").option("inferSchema", "false").load(chunk_paths)
    .selectExpr(*cast_expressions(), "_metadata.file_name AS file_name")
    .groupBy("file_name")
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

rules = {**warn_expectations(months), **FAIL_EXPECTATIONS}
violations = spark.sql(violation_counts_sql(table, rules)).first().asDict()
display(spark.createDataFrame(sorted(violations.items()), "expectation string, failing_rows long"))
problems += [f"expectation {name}: {n:,} failing rows in the table" for name, n in violations.items() if n]
problems += [f"event log: expectation {name} has {n:,} failed records in update {latest_update}"
             for name, n in sorted(latest_failures.items())]

actual_fields = [(f.name, f.dataType.simpleString()) for f in spark.table(table).schema]
problems += [f"schema: {p}" for p in schema_problems(actual_fields, columns)]

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
    ("Rows with rescued data", f"{rescued_total:,}"),
    ("Expectations failing in the table", str(sum(1 for n in violations.values() if n))),
    ("Expectations with failed records in latest update", str(len(latest_failures))),
    ("Duplicate natural keys", f"{duplicate_keys:,}"),
    ("Rows with null audit columns", f"{audit['rows_null_audit']:,}"),
    ("Distinct load_run_id values", str(audit["load_runs"])),
    ("Result", "PASS" if not problems else "FAIL"),
]
display(spark.createDataFrame(summary, "check string, value string"))

# COMMAND ----------

if problems:
    raise AssertionError("Bronze chunk 2 (Declarative Pipeline) sanity check failed:\n- " + "\n- ".join(problems))
print(f"All checks passed: {audit['total_rows']:,} rows from {len(table_stats)} files in {table}.")
