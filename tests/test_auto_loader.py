import gzip
import json

import pytest
from pyspark.sql import types as T
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

from common.auto_loader import (
    INGESTION_METHOD,
    RESCUED_DATA_COLUMN,
    auto_loader_options,
    checkpoint_paths,
    select_expressions,
    volume_path,
)
from common.rain_schema import AUDIT_COLUMNS, DATA_COLUMNS, column_names, spark_schema


def test_ingestion_method_is_auto_loader():
    assert INGESTION_METHOD == "AUTO_LOADER"


def test_volume_path_builds_and_validates():
    assert volume_path("workspace", "vstone_dev_bronze", "raw_files", "chunks", "chunk3_json") == (
        "/Volumes/workspace/vstone_dev_bronze/raw_files/chunks/chunk3_json"
    )
    for bad in [("work space", "s", "v"), ("c", "s", "v", "../x"), ("c", "s", "v", "a/b"), ("c", "s", "v", "")]:
        with pytest.raises(ValueError):
            volume_path(*bad)


def test_checkpoint_paths_one_folder_per_table_with_schema_inside():
    checkpoint, schema_location = checkpoint_paths("workspace", "vstone_dev_bronze", "checkpoints", "rain_forecast_chunk3_json")
    assert checkpoint == "/Volumes/workspace/vstone_dev_bronze/checkpoints/rain_forecast_chunk3_json"
    assert schema_location == checkpoint + "/_schema"


def test_auto_loader_options():
    options = auto_loader_options("json", "10", "/Volumes/c/s/checkpoints/t/_schema")
    assert options == {
        "cloudFiles.format": "json",
        "cloudFiles.schemaLocation": "/Volumes/c/s/checkpoints/t/_schema",
        "cloudFiles.schemaEvolutionMode": "none",
        "cloudFiles.includeExistingFiles": "true",
        "cloudFiles.maxFilesPerTrigger": "10",
        "rescuedDataColumn": "_rescued_data",
    }
    assert not any("allowOverwrites" in k or "inferColumnTypes" in k for k in options)


@pytest.mark.parametrize("fmt, max_files", [("avro", 10), ("json", 0), ("json", "-1"), ("json", "ten")])
def test_auto_loader_options_reject_bad_values(fmt, max_files):
    with pytest.raises(ValueError):
        auto_loader_options(fmt, max_files, "/Volumes/c/s/v/t/_schema")


def test_select_expressions_order_and_audit_values():
    exprs = select_expressions("12345")
    data = column_names(DATA_COLUMNS)
    assert exprs[: len(data)] == [f"`{c}`" for c in data]
    audit = exprs[len(data): len(data) + len(AUDIT_COLUMNS)]
    assert [a.rsplit(" AS ", 1)[1] for a in audit] == [f"`{c}`" for c in column_names(AUDIT_COLUMNS)]
    assert "'AUTO_LOADER' AS `ingestion_method`" in audit
    assert "'12345' AS `load_run_id`" in audit
    assert exprs[-1] == f"`{RESCUED_DATA_COLUMN}`"


def test_select_expressions_reject_unsafe_run_id():
    with pytest.raises(ValueError):
        select_expressions("1'; DROP TABLE t; --")


def write_gzip_json(path, records):
    with gzip.open(path, "wt", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def record(row, hour, label):
    values = {n: (1.5 if t == "DOUBLE" else 0) for n, t, _ in DATA_COLUMNS}
    values.update({"row": row, "col": 2, "year": 2020, "month": 4, "day": 1, "hour": hour, "label": label, "CIN": 9999.0})
    return values


def test_gzip_json_reads_with_the_explicit_schema(spark, tmp_path):
    write_gzip_json(tmp_path / "combined_2020_04_01.json.gz", [record(0, 0, 1), record(1, 5, 0)])

    df = spark.read.format("json").schema(spark_schema()).load(str(tmp_path))

    assertSchemaEqual(df.schema, spark_schema())
    got = df.select("row", "hour", "label", "CIN", "B09B")
    expected = spark.createDataFrame(
        [(0, 0, 1, 9999.0, 1.5), (1, 5, 0, 9999.0, 1.5)],
        "row int, hour int, label int, CIN double, B09B double",
    )
    assertDataFrameEqual(got, expected)


def _rows_processed(query):
    progress = [p if isinstance(p, dict) else json.loads(p.json) for p in query.recentProgress]
    return sum(int(p.get("numInputRows", 0)) for p in progress)


def test_available_now_with_checkpoint_loads_each_file_once(spark, tmp_path):
    """Same checkpoint behaviour as Auto Loader: the second run processes 0 rows."""
    source, sink, checkpoint = tmp_path / "source", tmp_path / "sink", tmp_path / "checkpoint"
    source.mkdir()
    write_gzip_json(source / "combined_2020_04_01.json.gz", [record(0, 0, 1), record(1, 1, 0)])
    write_gzip_json(source / "combined_2020_04_02.json.gz", [record(2, 2, 0)])

    def run():
        query = (
            spark.readStream.format("json").schema(spark_schema()).option("maxFilesPerTrigger", 1)
            .load(str(source))
            .selectExpr(*[f"`{c}`" for c in column_names(DATA_COLUMNS)])
            .writeStream.format("parquet").option("checkpointLocation", str(checkpoint))
            .trigger(availableNow=True)
            .start(str(sink))
        )
        query.awaitTermination()
        return _rows_processed(query)

    assert run() == 3
    assert run() == 0
    assert spark.read.parquet(str(sink)).count() == 3


def test_spark_schema_matches_table_types():
    schema = spark_schema(DATA_COLUMNS)
    assert schema["row"].dataType == T.IntegerType()
    assert schema["label"].dataType == T.IntegerType()
    assert schema["CAPE"].dataType == T.DoubleType()
    assert [f.name for f in schema] == column_names(DATA_COLUMNS)
