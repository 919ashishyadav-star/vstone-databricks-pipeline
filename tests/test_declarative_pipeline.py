import ast
import datetime
import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.testing import assertSchemaEqual

from common.chunk_plan import find_chunk, load_chunk_plan
from common.declarative_pipeline import (
    FAIL_EXPECTATIONS,
    INGESTION_METHOD,
    WARN_EXPECTATIONS,
    expectation_metrics_sql,
    month_expectation,
    pipeline_run_id,
    reader_options,
    select_expressions,
    table_columns,
    table_schema_ddl,
    violation_counts_sql,
    warn_expectations,
)
from common.rain_schema import AUDIT_COLUMNS, DATA_COLUMNS, RESCUED_COLUMN, column_names, spark_schema

REPO_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_SOURCE = REPO_ROOT / "src" / "pipelines" / "bronze_chunk2_csv.py"


def test_ingestion_method():
    assert INGESTION_METHOD == "DECLARATIVE_PIPELINE"


def test_reader_options():
    assert reader_options("10") == {
        "cloudFiles.format": "csv",
        "cloudFiles.schemaEvolutionMode": "none",
        "cloudFiles.includeExistingFiles": "true",
        "cloudFiles.maxFilesPerTrigger": "10",
        "header": "true",
        "rescuedDataColumn": "_rescued_data",
    }
    # Never re-read changed files or infer types.
    assert not any("allowOverwrites" in k or "inferColumnTypes" in k for k in reader_options(1))


@pytest.mark.parametrize("max_files", [0, "-1", "ten"])
def test_reader_options_reject_bad_values(max_files):
    with pytest.raises(ValueError):
        reader_options(max_files)


def test_table_columns_are_data_audit_rescued():
    assert table_columns() == DATA_COLUMNS + AUDIT_COLUMNS + [RESCUED_COLUMN]


def test_table_schema_ddl_types_and_descriptions(spark):
    schema = spark.createDataFrame([], table_schema_ddl()).schema
    assertSchemaEqual(T.StructType([T.StructField(f.name, f.dataType, f.nullable) for f in schema]),
                      spark_schema(table_columns()))
    for field, (_name, _type, description) in zip(schema.fields, table_columns()):
        assert field.metadata.get("comment") == description


def test_select_expressions_match_declared_schema(spark, tmp_path):
    """The pipeline refuses to start if its query does not match the declared schema, so check it here."""
    names = column_names(DATA_COLUMNS)
    values = ["1", "2", "2019", "10", "3", "4"] + ["1.5"] * (len(names) - 7) + ["0"]
    (tmp_path / "combined_2019_10_03.csv").write_text(",".join(names) + "\n" + ",".join(values) + "\n")

    df = (
        spark.read.format("csv").option("header", "true").schema(spark_schema()).load(str(tmp_path))
        .withColumn("_rescued_data", F.lit(None).cast("string"))  # Auto Loader adds this column
        .selectExpr(*select_expressions("update-1"))
    )
    declared = spark.createDataFrame([], table_schema_ddl()).schema
    assert [(f.name, f.dataType.simpleString()) for f in df.schema] == [
        (f.name, f.dataType.simpleString()) for f in declared
    ]
    row = df.first()
    assert row["ingestion_method"] == "DECLARATIVE_PIPELINE"
    assert row["load_run_id"] == "update-1"
    assert row["source_file"] == "combined_2019_10_03.csv"
    assert (row["row"], row["col"], row["day"], row["hour"], row["label"]) == (1, 2, 3, 4, 0)


def test_month_expectation():
    assert month_expectation(["2019-10"]) == {
        "month_in_chunk": "year IS NOT NULL AND month IS NOT NULL AND ((year = 2019 AND month = 10))"
    }
    assert "OR (year = 2020 AND month = 4)" in month_expectation(["2019-10", "2020-04"])["month_in_chunk"]
    for bad in ([], ["2019-13"], ["2019/10"], ["10-2019"], [""]):
        with pytest.raises(ValueError):
            month_expectation(bad)


def test_chunk2_months_come_from_chunk_plan():
    plan = load_chunk_plan(str(REPO_ROOT / "src" / "config" / "chunk_plan.json"))
    assert find_chunk(plan, "chunk2_csv")["months"] == ["2019-10"]


def test_expectation_split_warn_vs_fail():
    # Data problems only warn (Bronze keeps raw rows); only a broken load fails the update.
    assert set(warn_expectations(["2019-10"])) == {
        "no_rescued_data", "not_malformed", "label_is_0_or_1", "hour_in_range", "row_in_range", "col_in_range",
        "month_in_chunk",
    }
    assert set(FAIL_EXPECTATIONS) == {"source_file_present", "ingestion_method_set"}
    assert not set(WARN_EXPECTATIONS) & set(FAIL_EXPECTATIONS)


def _bronze_row(**overrides):
    values = {n: 1.0 if t == "DOUBLE" else None for n, t, _d in DATA_COLUMNS}
    values.update({"row": 0, "col": 0, "year": 2019, "month": 10, "day": 1, "hour": 0, "label": 0})
    values.update({
        "source_file": "combined_2019_10_01.csv", "source_file_path": "/Volumes/c/s/v/chunks/chunk2_csv/f.csv",
        "source_file_modified_dt": datetime.datetime(2026, 10, 1), "load_dt": datetime.datetime(2026, 11, 3),
        "load_run_id": "u1", "ingestion_method": "DECLARATIVE_PIPELINE", "_rescued_data": None,
    })
    values.update(overrides)
    return tuple(values[n] for n, _t, _d in table_columns())


def test_violation_counts_null_safe(spark):
    all_null = {c: None for c in ["row", "col", "year", "month", "day", "hour", "label"]}
    rows = [
        _bronze_row(),                                 # clean
        _bronze_row(_rescued_data='{"CAPE":"x"}'),     # rescued value
        _bronze_row(**all_null),                       # malformed line
        _bronze_row(label=2),
        _bronze_row(label=None),                       # null must fail, not be ignored
        _bronze_row(hour=24),
        _bronze_row(row=90, col=250),
        _bronze_row(month=11),
        _bronze_row(source_file=None),
        _bronze_row(ingestion_method="AUTO_LOADER"),
    ]
    spark.createDataFrame(rows, spark_schema(table_columns())).createOrReplaceTempView("bronze_chunk2_test")
    rules = {**warn_expectations(["2019-10"]), **FAIL_EXPECTATIONS}
    counts = spark.sql(violation_counts_sql("bronze_chunk2_test", rules)).first().asDict()
    assert counts == {
        "no_rescued_data": 1,
        "not_malformed": 1,
        "label_is_0_or_1": 3,     # label 2, label null, malformed row
        "hour_in_range": 2,       # hour 24, malformed row
        "row_in_range": 2,        # row 90, malformed row
        "col_in_range": 2,        # col 250, malformed row
        "month_in_chunk": 2,      # month 11, malformed row
        "source_file_present": 1,
        "ingestion_method_set": 1,
    }


def test_violation_counts_sql_needs_rules():
    with pytest.raises(ValueError):
        violation_counts_sql("t", {})


def test_pipeline_run_id():
    assert pipeline_run_id("0a1b-2c3d", "p-1") == "0a1b-2c3d"
    assert pipeline_run_id("", "1234abcd-0000-1111-2222-333344445555") == "pipeline-1234abcd-0000-1111-2222-333344445555"
    assert pipeline_run_id(None, None) == "manual"
    for bad in [("x'; drop", ""), ("", "a b")]:
        with pytest.raises(ValueError):
            pipeline_run_id(*bad)


def test_expectation_metrics_sql_on_event_log(spark):
    def event(update, event_type, expectations):
        details = {"flow_progress": {"data_quality": {"expectations": expectations}}} if expectations else {}
        return (update, datetime.datetime(2026, 11, 3, 10, int(update[-1])), event_type, json.dumps(details))

    exp = lambda name, passed, failed: {  # noqa: E731
        "name": name, "dataset": "rain_forecast_chunk2_csv", "passed_records": passed, "failed_records": failed,
    }
    events = [
        event("u1", "flow_progress", [exp("no_rescued_data", 100, 0), exp("label_is_0_or_1", 99, 1)]),
        event("u1", "flow_progress", [exp("no_rescued_data", 50, 0), exp("label_is_0_or_1", 50, 0)]),
        event("u2", "flow_progress", [exp("no_rescued_data", 10, 2)]),
        event("u2", "update_progress", None),
    ]
    schema = "update_id string, timestamp timestamp, event_type string, details string"
    (spark.createDataFrame(events, schema)
     .selectExpr("named_struct('update_id', update_id) AS origin", "timestamp", "event_type", "details")
     .createOrReplaceTempView("event_log_test"))

    rows = [r.asDict() for r in spark.sql(expectation_metrics_sql("event_log_test")).collect()]
    got = {(r["update_id"], r["expectation"]): (r["passed_records"], r["failed_records"]) for r in rows}
    assert got == {
        ("u1", "no_rescued_data"): (150, 0),
        ("u1", "label_is_0_or_1"): (149, 1),
        ("u2", "no_rescued_data"): (10, 2),
    }
    only_u2 = spark.sql(expectation_metrics_sql("event_log_test", ["u2"])).collect()
    assert {r["update_id"] for r in only_u2} == {"u2"}
    with pytest.raises(ValueError):
        expectation_metrics_sql("event_log_test", ["u1' OR 1=1 --"])


def test_pipeline_source_uses_settings_not_hard_coded_names():
    """The pipeline file is parsed (not run: it needs the pipeline runtime) to check it reads every setting."""
    source = PIPELINE_SOURCE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    settings = next(
        node.value for node in ast.walk(tree)
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", None) == "_SETTINGS"
    )
    assert {elt.value for elt in settings.elts} == {
        "catalog", "bronze_schema", "raw_volume", "chunk_name", "table_name", "cluster_by",
        "max_files_per_trigger", "reset_allowed", "chunk_plan_path",
    }
    for hard_coded in ("vstone_dev", "rain_forecast_chunk2_csv", "/Volumes/", "workspace."):
        assert hard_coded not in source.split('"""', 2)[2]  # outside the module docstring
