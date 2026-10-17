# Databricks notebook source
# MAGIC %md
# MAGIC # 02 – Data Chunking
# MAGIC
# MAGIC **Purpose:** split the 122 original Rain Forecast CSVs (shared landing volume) into four chunks
# MAGIC in this environment's Bronze `raw_files` volume, one chunk per Bronze ingestion method:
# MAGIC
# MAGIC | Chunk | Months | Output | How |
# MAGIC |---|---|---|---|
# MAGIC | chunk1_csv | Apr 2019 | CSV | **copied as-is** |
# MAGIC | chunk2_csv | Oct 2019 | CSV | **copied as-is** |
# MAGIC | chunk3_json | Apr 2020 | JSON, gzip | converted with Spark |
# MAGIC | chunk4_xml | Oct 2020 | XML, gzip | converted with Spark |
# MAGIC
# MAGIC The mapping above is **not** in this notebook: it comes from `src/config/chunk_plan.json`.
# MAGIC Paths come from the widgets / job parameters.
# MAGIC
# MAGIC **Design rules** (see decisions 11–15 in `docs/requirements_and_assumptions.md`)
# MAGIC - **One output file per source day, with a fixed name** (e.g. `chunk3_json/combined_2020_04_01.json.gz`).
# MAGIC   COPY INTO and Auto Loader remember files by path, so fixed names make reruns safe.
# MAGIC - **Safe to rerun:** mode `skip_existing` (default) leaves existing outputs alone; `overwrite`
# MAGIC   regenerates them at the same paths. Nothing is ever appended, so no duplicates.
# MAGIC - **No `source_file` column** is added: Bronze reads the file name from file metadata.
# MAGIC - CSV chunks are byte-for-byte copies, so the 4 files with 27 columns reach Bronze unchanged.
# MAGIC - A **row-count sanity check** compares every output file with its source file and **fails the
# MAGIC   job** if anything does not match.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("landing_schema", "vstone_landing", "Landing schema")
dbutils.widgets.text("landing_volume", "source_files", "Landing volume")
dbutils.widgets.text("bronze_schema", "vstone_dev_bronze", "Bronze schema")
dbutils.widgets.text("raw_volume", "raw_files", "Raw (chunk output) volume")
dbutils.widgets.text("config_path", "../config/chunk_plan.json", "Chunk plan config path")
dbutils.widgets.dropdown("mode", "skip_existing", ["skip_existing", "overwrite"], "Mode")

params = {
    name: dbutils.widgets.get(name).strip()
    for name in ["catalog", "landing_schema", "landing_volume", "bronze_schema", "raw_volume", "config_path", "mode"]
}
empty = [name for name, value in params.items() if not value]
if empty:
    raise ValueError(f"These widgets must not be empty: {empty}")

mode = params["mode"]
source_dir = f"/Volumes/{params['catalog']}/{params['landing_schema']}/{params['landing_volume']}"
raw_dir = f"/Volumes/{params['catalog']}/{params['bronze_schema']}/{params['raw_volume']}"
chunks_dir = f"{raw_dir}/chunks"
staging_root = f"{raw_dir}/_staging"  # temporary Spark output; outside the chunk folders Bronze reads

print(f"Source : {source_dir}")
print(f"Output : {chunks_dir}")
print(f"Mode   : {mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared code and config
# MAGIC
# MAGIC `src/common/` holds code shared with other notebooks and covered by unit tests in `tests/`.
# MAGIC The notebook runs from `src/notebooks/`, so `src/` is added to the Python path.

# COMMAND ----------

import fnmatch
import os
import sys
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F

src_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from common.chunk_plan import load_chunk_plan, output_file_name, validate_chunk_plan
from common.source_reader import FILE_NAME_COLUMN, parse_header, read_csv_by_name, schema_for

config_path = params["config_path"]
if not os.path.isabs(config_path):
    config_path = os.path.abspath(config_path)
plan = load_chunk_plan(config_path)
print(f"Config : {config_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 – List source files and validate the plan
# MAGIC
# MAGIC Every source month must belong to exactly one chunk (all files must reach Gold), and every month
# MAGIC in the config must have source files. A bad config stops here, **before** anything is written.

# COMMAND ----------

def list_files(directory):
    """File names in a volume folder (empty list if the folder does not exist yet)."""
    if not os.path.isdir(directory):
        return []
    return sorted(n for n in os.listdir(directory) if os.path.isfile(os.path.join(directory, n)))


def read_header(path):
    with open(path, encoding="utf-8") as f:
        return parse_header(f.readline())


source_names = [n for n in list_files(source_dir) if fnmatch.fnmatch(n, plan["source_file_pattern"])]
if not source_names:
    raise ValueError(f"No files matching {plan['source_file_pattern']!r} in {source_dir}")

assignment = validate_chunk_plan(plan, source_names)
source_headers = {n: read_header(f"{source_dir}/{n}") for n in source_names}

display(
    spark.createDataFrame(
        [(c["name"], ", ".join(c["months"]), c["format"], c["method"], c["compression"], len(assignment[c["name"]]))
         for c in plan["chunks"]],
        "chunk string, months string, format string, method string, compression string, source_files int",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 – Build the chunks
# MAGIC
# MAGIC - **copy** (CSV): each source file is copied to a staging folder, then moved to its fixed name.
# MAGIC   Staging first means a half-finished copy never appears in the chunk folder.
# MAGIC - **convert** (JSON / XML): one Spark write per chunk. Rows are grouped by source day
# MAGIC   (`repartition` + `partitionBy`), so each day becomes exactly one file in staging; each file is then
# MAGIC   moved to its fixed name. Columns are matched **by name** (shared reader), and the temporary day
# MAGIC   column is not written into the data.
# MAGIC - XML uses `records` / `record` tags because the data has a column called `row`.

# COMMAND ----------

run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
DAY_COLUMN = "_output_file"
actions = []  # (chunk, source file, output file, action)


def move_into_place(src_path, dst_path):
    """Replace dst with src (same path every run, so Bronze never sees a new file name)."""
    if os.path.exists(dst_path):
        dbutils.fs.rm(dst_path)
    dbutils.fs.mv(src_path, dst_path)


for chunk in plan["chunks"]:
    name, fmt, method = chunk["name"], chunk["format"], chunk["method"]
    compression = chunk.get("compression", "none")
    out_dir = f"{chunks_dir}/{name}"
    dbutils.fs.mkdirs(out_dir)
    existing = set(list_files(out_dir))

    todo = []
    for src in assignment[name]:
        out = output_file_name(src, fmt, compression)
        if mode == "skip_existing" and out in existing:
            actions.append((name, src, out, "skipped (exists)"))
        else:
            todo.append((src, out))
    if not todo:
        print(f"{name}: nothing to do ({len(assignment[name])} outputs already exist)")
        continue

    staging = f"{staging_root}/{run_id}/{name}"
    try:
        if method == "copy":
            for src, out in todo:
                dbutils.fs.cp(f"{source_dir}/{src}", f"{staging}/{out}")
                move_into_place(f"{staging}/{out}", f"{out_dir}/{out}")
                actions.append((name, src, out, "copied"))
        else:
            headers = {f"{source_dir}/{src}": source_headers[src] for src, _ in todo}
            out_by_src = {src: out for src, out in todo}
            mapping = F.create_map(*[F.lit(x) for pair in out_by_src.items() for x in pair])
            df = (
                read_csv_by_name(spark, headers, with_file_name=True)
                .withColumn(DAY_COLUMN, mapping[F.col(FILE_NAME_COLUMN)])
                .drop(FILE_NAME_COLUMN)
            )
            writer = (
                df.repartition(DAY_COLUMN)
                .write.mode("overwrite")
                .partitionBy(DAY_COLUMN)
                .option("compression", compression)
            )
            if fmt == "json":
                writer.format("json").save(staging)
            elif fmt == "xml":
                (writer.format("xml")
                 .option("rootTag", chunk["xml_root_tag"])
                 .option("rowTag", chunk["xml_row_tag"])
                 .save(staging))
            else:
                raise ValueError(f"{name}: cannot convert to {fmt}")

            for src, out in todo:
                day_dir = f"{staging}/{DAY_COLUMN}={out}"
                parts = [n for n in list_files(day_dir) if n.startswith("part-")]
                if len(parts) != 1:
                    raise RuntimeError(f"{name}: expected 1 output file for {src}, found {len(parts)} in {day_dir}")
                move_into_place(f"{day_dir}/{parts[0]}", f"{out_dir}/{out}")
                actions.append((name, src, out, "converted"))
        print(f"{name}: wrote {len(todo)} file(s)")
    finally:
        dbutils.fs.rm(staging, recurse=True)

dbutils.fs.rm(f"{staging_root}/{run_id}", recurse=True)

# COMMAND ----------

display(
    spark.createDataFrame(actions, "chunk string, source_file string, output_file string, action string")
    .groupBy("chunk", "action").count().orderBy("chunk", "action")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 – Sanity check: row counts per chunk
# MAGIC
# MAGIC For every chunk:
# MAGIC 1. The output folder must contain **exactly** the expected files – nothing missing, nothing extra
# MAGIC    (an extra file would be loaded into Bronze).
# MAGIC 2. **Rows in each output file = rows in its source file.** Source rows are counted from the landing
# MAGIC    volume; output rows are counted by reading the written files back (CSV copies, JSON, XML) with
# MAGIC    explicit schemas.
# MAGIC
# MAGIC If anything does not match, the notebook raises an error, so the job fails and sends the failure
# MAGIC email. Tip: a file left half-written by an interrupted run is fixed by re-running with
# MAGIC `mode = overwrite`.

# COMMAND ----------

def count_rows_by_file(df):
    return {r[0]: r[1] for r in df.groupBy(F.col("_metadata.file_name")).count().collect()}


all_source_paths = {f"{source_dir}/{n}": h for n, h in source_headers.items()}
source_rows = {
    r[FILE_NAME_COLUMN]: r["count"]
    for r in read_csv_by_name(spark, all_source_paths, with_file_name=True).groupBy(FILE_NAME_COLUMN).count().collect()
}

check_rows, problems = [], []
for chunk in plan["chunks"]:
    name, fmt = chunk["name"], chunk["format"]
    compression = chunk.get("compression", "none")
    out_dir = f"{chunks_dir}/{name}"
    expected = {output_file_name(src, fmt, compression): src for src in assignment[name]}
    present = set(list_files(out_dir))

    for out in sorted(set(expected) - present):
        problems.append(f"{name}: missing output {out}")
    for out in sorted(present - set(expected)):
        problems.append(f"{name}: unexpected file {out}")

    paths = [f"{out_dir}/{out}" for out in sorted(set(expected) & present)]
    output_rows = {}
    if paths:
        if fmt == "csv":
            headers = {p: read_header(p) for p in paths}
            output_rows = {
                r[FILE_NAME_COLUMN]: r["count"]
                for r in read_csv_by_name(spark, headers, with_file_name=True).groupBy(FILE_NAME_COLUMN).count().collect()
            }
        elif fmt == "json":
            full_schema = schema_for(max(source_headers.values(), key=len))
            output_rows = count_rows_by_file(spark.read.format("json").schema(full_schema).load(paths))
        elif fmt == "xml":
            full_schema = schema_for(max(source_headers.values(), key=len))
            output_rows = count_rows_by_file(
                spark.read.format("xml").option("rowTag", chunk["xml_row_tag"]).schema(full_schema).load(paths)
            )

    for out, src in sorted(expected.items()):
        s, o = source_rows.get(src, 0), output_rows.get(out, 0)
        status = "OK" if out in present and s == o else "MISMATCH"
        if status != "OK" and out in present:
            problems.append(f"{name}: {out} has {o:,} rows, source {src} has {s:,}")
        check_rows.append((name, src, out, s, o, status))

check_df = spark.createDataFrame(
    check_rows, "chunk string, source_file string, output_file string, source_rows long, output_rows long, status string"
)
display(
    check_df.groupBy("chunk")
    .agg(
        F.count("*").alias("files"),
        F.sum("source_rows").alias("source_rows"),
        F.sum("output_rows").alias("output_rows"),
        F.sum(F.when(F.col("status") != "OK", 1).otherwise(0)).alias("mismatched_files"),
    )
    .orderBy("chunk")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Per-file detail** (mismatches first)

# COMMAND ----------

display(check_df.orderBy(F.col("status") == "OK", "chunk", "output_file"))

# COMMAND ----------

if problems:
    raise AssertionError("Data Chunking sanity check failed:\n- " + "\n- ".join(problems))

total = sum(r[3] for r in check_rows)
print(f"Sanity check passed: {len(check_rows)} files, {total:,} rows, all chunks match the source.")
