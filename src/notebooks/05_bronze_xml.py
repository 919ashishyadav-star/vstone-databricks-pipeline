# Databricks notebook source
# MAGIC %md
# MAGIC # 05 – Bronze: chunk 4 XML via PySpark
# MAGIC
# MAGIC **Purpose:** load the chunk 4 files (Oct 2020, gzip XML) from the Bronze `raw_files` volume into the Delta
# MAGIC table `rain_forecast_chunk4_xml` with a **PySpark batch read/write** (native `format("xml")` with `rowTag`).
# MAGIC
# MAGIC **Steps**
# MAGIC 1. Create the table if needed (explicit schema, descriptions, Liquid Clustering) and re-apply descriptions.
# MAGIC 2. **Choose the files** – there is no checkpoint, so the table itself says what is loaded:
# MAGIC    new files are loaded, files already in the table (same name and modification time) are skipped, and a
# MAGIC    file that changed since it was loaded **stops the run**. `mode = replace` reloads chosen files instead.
# MAGIC 3. Read the XML with an explicit typed schema and write it in **one atomic commit** (append, or
# MAGIC    `replaceWhere` for replace mode).
# MAGIC 4. `OPTIMIZE` (optional).
# MAGIC 5. Sanity checks against the **original landing CSVs** – the notebook **fails** if any check fails.
# MAGIC
# MAGIC **XML lessons applied:** XML has no types and schema inference sorts columns alphabetically, so the schema
# MAGIC is explicit (real types), elements are matched **by name**, columns are selected by name in table order, and a
# MAGIC check confirms the table's columns, order and types. Value sums are compared with the original CSVs to prove
# MAGIC the CSV → XML → Delta round trip kept the numbers.
# MAGIC
# MAGIC **Bad records:** parse mode `PERMISSIVE` keeps every record. A value that does not fit goes to
# MAGIC `_rescued_data`; a record that cannot be parsed at all goes to `_corrupt_record`. Any such row fails the checks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("bronze_schema", "vstone_dev_bronze", "Bronze schema")
dbutils.widgets.text("raw_volume", "raw_files", "Raw (chunk) volume")
dbutils.widgets.text("chunk_name", "chunk4_xml", "Chunk folder")
dbutils.widgets.text("config_chunk", "chunk4_xml", "Chunk entry in the chunk plan (row tag)")
dbutils.widgets.text("table_name", "rain_forecast_chunk4_xml", "Bronze table")
dbutils.widgets.text("landing_schema", "vstone_landing", "Landing schema (original CSVs)")
dbutils.widgets.text("landing_volume", "source_files", "Landing volume (original CSVs)")
dbutils.widgets.text("config_path", "../config/chunk_plan.json", "Chunk plan config path")
dbutils.widgets.dropdown("mode", "skip_loaded", ["skip_loaded", "replace"], "Mode")
dbutils.widgets.text("replace_files", "", "Files to replace (comma-separated; empty = all)")
dbutils.widgets.text("cluster_by", "day,hour", "Liquid Clustering columns")
dbutils.widgets.text("load_run_id", "", "Load run ID (job run ID)")
dbutils.widgets.dropdown("run_optimize", "true", ["true", "false"], "Run OPTIMIZE")

names = ["catalog", "bronze_schema", "raw_volume", "chunk_name", "config_chunk", "table_name", "landing_schema",
         "landing_volume", "config_path", "mode", "replace_files", "cluster_by", "load_run_id", "run_optimize"]
params = {name: dbutils.widgets.get(name).strip() for name in names}
empty = [n for n, v in params.items() if not v and n not in ("load_run_id", "replace_files")]
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
from common.bronze_audit import resolve_run_id
from common.bronze_checks import (
    CHECK_SCHEMA,
    audit_check_sql,
    compare_file_stats,
    duplicate_keys_sql,
    load_batches_sql,
    schema_problems,
    table_file_stats_sql,
)
from common.chunk_plan import find_chunk, load_chunk_plan, source_csv_for
from common.rain_schema import (
    SATELLITE_BANDS,
    bronze_columns,
    comment_sql,
    create_table_sql,
    full_table_name,
    quote_identifier,
)
from common.source_reader import FILE_NAME_COLUMN, parse_header, read_csv_by_name
from common.xml_loader import (
    INGESTION_METHOD,
    files_to_load,
    read_schema,
    replace_where_predicate,
    select_expressions,
    xml_read_options,
)

SUM_COLUMNS = ["CAPE", "B14B"]  # value checks against the original CSVs

catalog, bronze_schema = params["catalog"], params["bronze_schema"]
table = full_table_name(catalog, bronze_schema, params["table_name"])
source_dir = volume_path(catalog, bronze_schema, params["raw_volume"], "chunks", params["chunk_name"])
landing_dir = volume_path(catalog, params["landing_schema"], params["landing_volume"])
config_path = params["config_path"] if os.path.isabs(params["config_path"]) else os.path.abspath(params["config_path"])
chunk_config = find_chunk(load_chunk_plan(config_path), params["config_chunk"])
if chunk_config.get("format") != "xml":
    raise ValueError(f"Chunk {params['config_chunk']} is not an XML chunk")
row_tag = chunk_config["xml_row_tag"]
cluster_by = [c.strip() for c in params["cluster_by"].split(",") if c.strip()]
run_id = resolve_run_id(params["load_run_id"])
columns = bronze_columns(include_rescued=True, include_corrupt=True)
table_comment = (
    f"Bronze: Rain Forecast {params['chunk_name']} loaded as-is with a PySpark XML batch read (rowTag '{row_tag}') "
    f"from {source_dir}. One row per grid cell per hour; natural key row, col, year, month, day, hour. "
    "Values kept exactly as in the source (CIN = 9999 and satellite band = 0 mean missing). Values that did not fit "
    "the schema are in _rescued_data; records that could not be parsed are in _corrupt_record."
)

print(f"Table   : {table}")
print(f"Source  : {source_dir} (rowTag = {row_tag})")
print(f"Compare : {landing_dir}")
print(f"Mode    : {params['mode']}")
print(f"Run ID  : {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 – Create the table and apply descriptions

# COMMAND ----------

spark.sql(create_table_sql(table, table_comment, cluster_by, columns))
spark.sql(f"ALTER TABLE {table} CLUSTER BY ({', '.join(quote_identifier(c) for c in cluster_by)})")
for statement in comment_sql(table, table_comment, columns):
    spark.sql(statement)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 – Choose the files to load
# MAGIC
# MAGIC `files_to_load` (in `src/common/xml_loader.py`, unit tested) compares the chunk folder with the
# MAGIC `source_file` / `source_file_modified_dt` audit columns already in the table.

# COMMAND ----------

chunk_files = {
    f.name: f.modificationTime
    for f in dbutils.fs.ls(source_dir)
    if not f.name.endswith("/") and not f.name.startswith(("_", "."))
}
loaded_rows = spark.sql(f"""
    SELECT source_file, count(DISTINCT source_file_modified_dt) AS versions,
           max(unix_millis(source_file_modified_dt)) AS modified_ms
    FROM {table} GROUP BY source_file
""").collect()
multi_version = [r["source_file"] for r in loaded_rows if r["versions"] > 1]
if multi_version:
    raise RuntimeError(f"Files loaded from more than one version: {multi_version}")
loaded = {r["source_file"]: r["modified_ms"] for r in loaded_rows}

replace_files = [n.strip() for n in params["replace_files"].split(",") if n.strip()]
choice = files_to_load(chunk_files, loaded, params["mode"], replace_files)
print(f"Files in chunk folder: {len(chunk_files)}; to load: {len(choice['load'])}; skipped: {len(choice['skipped'])}; "
      f"replace: {choice['replace']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 – Read the XML and write it (one commit)

# COMMAND ----------

run_started = str(spark.sql("SELECT current_timestamp() AS t").first()["t"])
if choice["load"]:
    options = xml_read_options(row_tag)
    for key, value in options.items():
        print(f"  {key} = {value}")
    df = (
        spark.read.format("xml")
        .options(**options)
        .schema(read_schema())
        .load([f"{source_dir}/{n}" for n in choice["load"]])
        .selectExpr(*select_expressions(run_id))
    )
    if choice["replace"]:
        predicate = replace_where_predicate(choice["load"])
        print(f"replaceWhere: {predicate}")
        df.write.format("delta").mode("overwrite").option("replaceWhere", predicate).saveAsTable(table)
    else:
        df.write.format("delta").mode("append").saveAsTable(table)
else:
    print("Nothing to load: every file in the chunk folder is already in the table.")

load_report = spark.sql(load_batches_sql(table, run_id, run_started)).collect()
inserted_rows = sum(r["rows"] for r in load_report)
inserted_files = sum(r["files"] for r in load_report)
display(spark.createDataFrame(
    [("new or replaced", len(choice["load"])), ("skipped (already loaded)", len(choice["skipped"])),
     ("files written by this run", inserted_files), ("rows written by this run", inserted_rows)],
    "item string, value long",
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 – OPTIMIZE (Liquid Clustering)

# COMMAND ----------

if params["run_optimize"] == "true" and choice["load"]:
    display(spark.sql(f"OPTIMIZE {table}"))
else:
    print("OPTIMIZE skipped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 – Sanity checks
# MAGIC
# MAGIC Compared with the **original landing CSV** for each day (read by column name) – independent of the XML reader.
# MAGIC
# MAGIC | # | Check | Expected |
# MAGIC |---|---|---|
# MAGIC | 1 | Rows and `sum(label)` per file = the original CSV; same set of files | equal |
# MAGIC | 2 | `sum(CAPE)`, `sum(B14B)` per file = the original CSV (floating-point rounding only) | equal |
# MAGIC | 3 | Null satellite bands in files that have them | 0 |
# MAGIC | 4 | Rows with `_rescued_data` / `_corrupt_record` / every key and label null | 0 / 0 / 0 |
# MAGIC | 5 | Table columns, order and types = `rain_schema` | identical |
# MAGIC | 6 | Duplicate natural keys; null audit columns; `ingestion_method` = PYSPARK_XML | 0 / 0 / OK |

# COMMAND ----------

problems = []
csv_for = {name: source_csv_for(name) for name in chunk_files}
no_csv = sorted(n for n, c in csv_for.items() if c is None or not os.path.isfile(f"{landing_dir}/{c}"))
problems += [f"{n}: no matching original CSV in {landing_dir}" for n in no_csv]

headers = {}
for name, csv_name in csv_for.items():
    if name in no_csv:
        continue
    with open(f"{landing_dir}/{csv_name}", encoding="utf-8") as f:
        headers[f"{landing_dir}/{csv_name}"] = parse_header(f.readline())
by_csv = {}
if headers:
    by_csv = {
        r[FILE_NAME_COLUMN]: r.asDict()
        for r in read_csv_by_name(spark, headers, with_file_name=True)
        .groupBy(FILE_NAME_COLUMN)
        .agg(F.count(F.lit(1)).alias("rows"), F.sum("label").alias("label_sum"),
             *[F.sum(c).alias(f"sum_{c}") for c in SUM_COLUMNS])
        .collect()
    }
xml_name_for_csv = {c: n for n, c in csv_for.items() if c}
source_stats = {xml_name_for_csv[c]: (r["rows"], r["label_sum"]) for c, r in by_csv.items()}
source_sums = {xml_name_for_csv[c]: {col: r[f"sum_{col}"] for col in SUM_COLUMNS} for c, r in by_csv.items()}
has_bands = {xml_name_for_csv[os.path.basename(p)]: all(b in h for b in SATELLITE_BANDS) for p, h in headers.items()}

table_stats = {
    r["source_file"]: r.asDict()
    for r in spark.sql(table_file_stats_sql(table, include_rescued=True, include_corrupt=True,
                                            sum_columns=SUM_COLUMNS)).collect()
}
check_rows, file_problems = compare_file_stats(source_stats, table_stats, has_bands, source_sums)
problems += file_problems
check_df = spark.createDataFrame(check_rows, CHECK_SCHEMA)
display(check_df.orderBy(F.col("status") == "OK", "source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Value sums per file: table vs original CSV** (check 2)

# COMMAND ----------

display(spark.createDataFrame(
    [(n, c, float(source_sums.get(n, {}).get(c) or 0), float(table_stats.get(n, {}).get(f"sum_{c}") or 0))
     for n in sorted(table_stats) for c in SUM_COLUMNS],
    "source_file string, column string, source_sum double, table_sum double",
))

# COMMAND ----------

# MAGIC %md
# MAGIC **Rows with rescued data or corrupt records** (empty = every record parsed and fitted the schema)

# COMMAND ----------

display(spark.sql(
    f"SELECT source_file, _rescued_data, _corrupt_record, * EXCEPT (source_file, _rescued_data, _corrupt_record) "
    f"FROM {table} WHERE _rescued_data IS NOT NULL OR _corrupt_record IS NOT NULL LIMIT 20"
))

# COMMAND ----------

actual_fields = [(f.name, f.dataType.simpleString()) for f in spark.table(table).schema]
schema_issues = schema_problems(actual_fields, columns)
problems += [f"schema: {p}" for p in schema_issues]

duplicate_keys = spark.sql(duplicate_keys_sql(table)).first()["n"]
audit = spark.sql(audit_check_sql(table, INGESTION_METHOD)).first().asDict()
if duplicate_keys:
    problems.append(f"{duplicate_keys:,} duplicate natural keys")
if audit["rows_null_audit"]:
    problems.append(f"{audit['rows_null_audit']:,} rows with a null audit column")
if audit["rows_other_method"]:
    problems.append(f"{audit['rows_other_method']:,} rows with ingestion_method other than {INGESTION_METHOD}")

summary = [
    ("Files in chunk folder", str(len(chunk_files))),
    ("Files in table", str(len(table_stats))),
    ("Rows in original CSVs", f"{sum(v[0] for v in source_stats.values()):,}"),
    ("Rows in table", f"{audit['total_rows']:,}"),
    ("Files loaded / skipped by this run", f"{len(choice['load'])} / {len(choice['skipped'])}"),
    ("Rows written by this run", f"{inserted_rows:,}"),
    ("Rows with rescued data", f"{sum(t.get('rows_rescued', 0) for t in table_stats.values()):,}"),
    ("Corrupt records", f"{sum(t.get('rows_corrupt', 0) for t in table_stats.values()):,}"),
    ("Schema (names, order, types)", "OK" if not schema_issues else "; ".join(schema_issues)),
    ("Duplicate natural keys", f"{duplicate_keys:,}"),
    ("Rows with null audit columns", f"{audit['rows_null_audit']:,}"),
    ("Distinct load_run_id values", str(audit["load_runs"])),
    ("Result", "PASS" if not problems else "FAIL"),
]
display(spark.createDataFrame(summary, "check string, value string"))

# COMMAND ----------

if problems:
    raise AssertionError("Bronze XML sanity check failed:\n- " + "\n- ".join(problems))
print(f"All checks passed: {audit['total_rows']:,} rows from {len(table_stats)} files in {table}.")
