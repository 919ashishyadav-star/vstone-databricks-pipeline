# Databricks notebook source
# MAGIC %md
# MAGIC # 06b – Bronze chunk 2: incremental-load demo and column-order proof (dev only)
# MAGIC
# MAGIC **Purpose:** evidence that the Declarative Pipeline for chunk 2 loads **incrementally** and matches CSV
# MAGIC columns **by header name**. The notebook starts pipeline updates itself (Databricks SDK) and checks each one.
# MAGIC
# MAGIC | Update | Files available | Type | Expected |
# MAGIC |---|---|---|---|
# MAGIC | A | first `first_drop_files` days (the rest moved to a holding folder) | full refresh | loads drop 1 only |
# MAGIC | B | all 31 days (held files moved back) | normal | loads **only** the held files |
# MAGIC | C | all 31 days | normal | loads 0 rows |
# MAGIC | D | 31 days + a test file with the columns in a different order | normal | loads only the test file; values equal the original rows |
# MAGIC | E | 31 days (test file deleted) | full refresh | table rebuilt from the 31 files – test rows gone |
# MAGIC
# MAGIC **Restore-safe:** moving the held files back and deleting the test file happen in a `finally` block, so they
# MAGIC run even if a step fails; the notebook then confirms the chunk folder has exactly the expected files.
# MAGIC If the run itself is cancelled, run it again with `mode = restore`.
# MAGIC Do **not** run the Data Chunking job or the Bronze Declarative Pipeline job while this runs.
# MAGIC
# MAGIC > **Assumption (Q2):** the declarative pipeline chunk is chunk 2 (Oct 2019 CSV), pending trainer confirmation.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("bronze_schema", "vstone_dev_bronze", "Bronze schema")
dbutils.widgets.text("raw_volume", "raw_files", "Raw (chunk) volume")
dbutils.widgets.text("chunk_name", "chunk2_csv", "Chunk folder")
dbutils.widgets.text("table_name", "rain_forecast_chunk2_csv", "Bronze table")
dbutils.widgets.text("event_log_table", "rain_forecast_chunk2_csv_event_log", "Pipeline event log table")
dbutils.widgets.text("pipeline_id", "", "Pipeline ID")
dbutils.widgets.text("hold_folder", "_hold", "Holding folder (in the raw volume)")
dbutils.widgets.text("first_drop_files", "15", "Files in drop 1")
dbutils.widgets.text("expected_files", "31", "Files expected in the chunk folder")
dbutils.widgets.text("column_order_test_file", "", "Workspace path of the column-order test CSV")
dbutils.widgets.text("original_file", "combined_2019_10_01.csv", "Chunk file the test rows were taken from")
dbutils.widgets.dropdown("mode", "demo", ["demo", "restore"], "Mode")

# COMMAND ----------

names = ["catalog", "bronze_schema", "raw_volume", "chunk_name", "table_name", "event_log_table", "pipeline_id",
         "hold_folder", "first_drop_files", "expected_files", "column_order_test_file", "original_file", "mode"]
params = {name: dbutils.widgets.get(name).strip() for name in names}
empty = [n for n, v in params.items() if not v]
if empty:
    raise ValueError(f"These widgets must not be empty: {empty}")

# COMMAND ----------

import json
import os
import shutil
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import UpdateInfoState

src_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if src_root not in sys.path:
    sys.path.insert(0, src_root)

from common.auto_loader import volume_path
from common.declarative_pipeline import expectation_metrics_sql
from common.rain_schema import DATA_COLUMNS, NATURAL_KEY, full_table_name, quote_identifier, sql_string

catalog, bronze_schema, chunk_name = params["catalog"], params["bronze_schema"], params["chunk_name"]
table = full_table_name(catalog, bronze_schema, params["table_name"])
event_log_table = full_table_name(catalog, bronze_schema, params["event_log_table"])
source_dir = volume_path(catalog, bronze_schema, params["raw_volume"], "chunks", chunk_name)
hold_dir = volume_path(catalog, bronze_schema, params["raw_volume"], params["hold_folder"], chunk_name)
pipeline_id = params["pipeline_id"]
first_drop, expected_files = int(params["first_drop_files"]), int(params["expected_files"])
test_source = params["column_order_test_file"]
test_name = os.path.basename(test_source)
test_target = f"{source_dir}/{test_name}"
if not 0 < first_drop < expected_files:
    raise ValueError("first_drop_files must be between 1 and expected_files - 1")

w = WorkspaceClient()
print(f"Table: {table}\nSource: {source_dir}\nHolding folder: {hold_dir}\nPipeline: {pipeline_id}")

# COMMAND ----------


def data_files(folder: str) -> list:
    if not os.path.isdir(folder):
        return []
    return sorted(n for n in os.listdir(folder) if os.path.isfile(f"{folder}/{n}") and not n.startswith((".", "_")))


def restore_files() -> dict:
    """Move every held file back, delete the test file, and confirm the chunk folder is complete."""
    moved_back = []
    for name in data_files(hold_dir):
        if os.path.exists(f"{source_dir}/{name}"):
            raise RuntimeError(f"{name} is in both {hold_dir} and {source_dir}; check by hand")
        dbutils.fs.mv(f"{hold_dir}/{name}", f"{source_dir}/{name}")
        moved_back.append(name)
    test_removed = os.path.exists(test_target)
    if test_removed:
        os.remove(test_target)
    files = data_files(source_dir)
    if len(files) != expected_files or test_name in files or data_files(hold_dir):
        raise RuntimeError(
            f"Chunk folder not restored: {len(files)} files (expected {expected_files}), "
            f"held files left: {data_files(hold_dir)}"
        )
    print(f"Restored: moved back {len(moved_back)} files, test file removed: {test_removed}; "
          f"{source_dir} has {len(files)} files")
    return {"moved_back": len(moved_back), "test_file_removed": test_removed, "files_in_chunk_folder": len(files)}


def run_update(label: str, full_refresh: bool, timeout_s: int = 3600) -> dict:
    """Start a pipeline update, wait for it, and report what it loaded (rows stamped after it started)."""
    started = spark.sql("SELECT current_timestamp() AS t").first()["t"]
    update_id = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=full_refresh).update_id
    print(f"Update {label}: {update_id} (full_refresh={full_refresh}) started")
    deadline = time.time() + timeout_s
    while True:
        state = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id).update.state
        if state in (UpdateInfoState.COMPLETED, UpdateInfoState.FAILED, UpdateInfoState.CANCELED):
            break
        if time.time() > deadline:
            raise TimeoutError(f"Update {label} ({update_id}) still {state} after {timeout_s}s")
        time.sleep(20)
    if state != UpdateInfoState.COMPLETED:
        raise RuntimeError(f"Update {label} ({update_id}) ended {state}; see the pipeline event log")
    loaded = spark.sql(
        f"SELECT count(*) AS rows, count(DISTINCT source_file) AS files, count(DISTINCT load_dt) AS batches,\n"
        f"       min(source_file) AS first_file, max(source_file) AS last_file\n"
        f"FROM {table} WHERE load_dt >= CAST({sql_string(str(started))} AS TIMESTAMP)"
    ).first().asDict()
    total = spark.sql(f"SELECT count(*) AS rows, count(DISTINCT source_file) AS files FROM {table}").first()
    result = {"update": label, "update_id": update_id, "full_refresh": full_refresh, "rows_loaded": loaded["rows"],
              "files_loaded": loaded["files"], "micro_batches": loaded["batches"], "first_file": loaded["first_file"],
              "last_file": loaded["last_file"], "table_rows_after": total["rows"], "table_files_after": total["files"]}
    print(result)
    return result


def expect(condition: bool, message: str):
    if not condition:
        raise AssertionError(message)


def test_rows_in_table() -> int:
    if not spark.catalog.tableExists(table):
        return 0
    return spark.sql(f"SELECT count(*) AS n FROM {table} WHERE source_file = {sql_string(test_name)}").first()["n"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restore mode
# MAGIC
# MAGIC Puts the chunk folder back and, if the test rows are still in the table, rebuilds it with a full refresh.

# COMMAND ----------

if params["mode"] == "restore":
    restored = restore_files()
    if test_rows_in_table():
        restored["full_refresh"] = run_update("restore", full_refresh=True)
        expect(test_rows_in_table() == 0, "Test rows still in the table after the full refresh")
    dbutils.notebook.exit(json.dumps({"mode": "restore", **restored}, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preflight

# COMMAND ----------

all_files = data_files(source_dir)
expect(len(all_files) == expected_files, f"{source_dir} has {len(all_files)} files, expected {expected_files}")
expect(not data_files(hold_dir), f"{hold_dir} is not empty; run with mode = restore first")
expect(not os.path.exists(test_target), f"{test_target} already exists; run with mode = restore first")
expect(os.path.isfile(test_source), f"Column-order test file not found: {test_source}")
drop_1, drop_2 = all_files[:first_drop], all_files[first_drop:]
print(f"Drop 1: {len(drop_1)} files ({drop_1[0]} .. {drop_1[-1]}); drop 2: {len(drop_2)} files ({drop_2[0]} .. {drop_2[-1]})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo (updates A–E) – restore always runs

# COMMAND ----------

updates, column_order = [], {}
try:
    # Drop 1: hold back the later days, then rebuild the table from the first days only.
    os.makedirs(hold_dir, exist_ok=True)
    for name in drop_2:
        dbutils.fs.mv(f"{source_dir}/{name}", f"{hold_dir}/{name}")
    expect(data_files(source_dir) == drop_1, "Chunk folder does not hold exactly drop 1")
    a = run_update("A", full_refresh=True)
    expect(a["files_loaded"] == len(drop_1) and a["table_files_after"] == len(drop_1), "Update A did not load exactly drop 1")
    updates.append(a)

    # Drop 2: the held days arrive; a normal update must load only those.
    restore_files()
    b = run_update("B", full_refresh=False)
    expect(b["files_loaded"] == len(drop_2) and b["first_file"] == drop_2[0] and b["last_file"] == drop_2[-1],
           "Update B did not load exactly drop 2")
    expect(b["table_rows_after"] == a["rows_loaded"] + b["rows_loaded"], "Update B changed rows of drop 1")
    updates.append(b)

    c = run_update("C", full_refresh=False)
    expect(c["rows_loaded"] == 0 and c["table_rows_after"] == b["table_rows_after"], "Update C loaded rows again")
    updates.append(c)

    # Column-order proof: same rows as the original file, columns in a different order.
    with open(test_source, encoding="utf-8") as f:
        test_header = f.readline().strip().split(",")
    original_header = [n for n, _t, _d in DATA_COLUMNS]
    expect(sorted(test_header) == sorted(original_header) and test_header != original_header,
           "Test file must have the same columns in a different order")
    shutil.copyfile(test_source, test_target)
    d = run_update("D", full_refresh=False)
    expect(d["files_loaded"] == 1 and d["first_file"] == test_name, "Update D did not load only the test file")
    updates.append(d)

    keys = " AND ".join(f"t.{quote_identifier(k)} = o.{quote_identifier(k)}" for k in NATURAL_KEY)
    same = " AND ".join(f"t.{quote_identifier(n)} <=> o.{quote_identifier(n)}" for n, _t, _d in DATA_COLUMNS)
    proof = spark.sql(
        f"SELECT count(*) AS test_rows, count(o.`row`) AS matched_rows, count_if({same}) AS equal_rows,\n"
        f"       count_if(t._rescued_data IS NOT NULL) AS rescued_rows\n"
        f"FROM (SELECT * FROM {table} WHERE source_file = {sql_string(test_name)}) t\n"
        f"LEFT JOIN (SELECT * FROM {table} WHERE source_file = {sql_string(params['original_file'])}) o ON {keys}"
    ).first().asDict()
    column_order = {"test_file": test_name, "test_header": test_header, **proof}
    print(column_order)
    expect(proof["test_rows"] == d["rows_loaded"] and proof["test_rows"] > 0, "No test rows loaded")
    expect(proof["equal_rows"] == proof["test_rows"] and not proof["rescued_rows"],
           "Columns were not matched by name: test rows differ from the original rows")

    # Clean up: delete the test file and rebuild the table from the 31 chunk files (dev allows a full refresh).
    os.remove(test_target)
    e = run_update("E", full_refresh=True)
    expect(e["table_files_after"] == expected_files and test_rows_in_table() == 0,
           "Update E did not rebuild the table from the chunk files only")
    expect(e["table_rows_after"] == b["table_rows_after"], "Full refresh row count differs from drops 1 + 2")
    updates.append(e)
finally:
    restored = restore_files()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evidence
# MAGIC
# MAGIC Per update: what it loaded, and the expectation metrics from the pipeline event log.

# COMMAND ----------

display(spark.createDataFrame(updates))
metrics_df = spark.sql(expectation_metrics_sql(event_log_table, [u["update_id"] for u in updates]))
display(metrics_df)
evidence = {
    "updates": updates,
    "column_order_proof": column_order,
    "expectation_metrics": [r.asDict() for r in metrics_df.collect()],
    "restore": restored,
}
dbutils.notebook.exit(json.dumps(evidence, default=str))
