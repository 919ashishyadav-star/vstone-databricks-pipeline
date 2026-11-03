import pytest
from pyspark.sql import types as T
from pyspark.testing import assertDataFrameEqual

from common.copy_into import (
    INGESTION_METHOD,
    MANUAL_RUN_ID,
    build_copy_into_sql,
    cast_expressions,
    resolve_run_id,
)
from common.rain_schema import DATA_COLUMNS, SATELLITE_BANDS, column_names

TABLE = "`workspace`.`vstone_dev_bronze`.`rain_forecast_chunk1_csv`"
SOURCE_DIR = "/Volumes/workspace/vstone_dev_bronze/raw_files/chunks/chunk1_csv"


@pytest.mark.parametrize("value", [None, "", "   ", "{{job.run_id}}"])
def test_resolve_run_id_defaults_to_manual(value):
    assert resolve_run_id(value) == MANUAL_RUN_ID


def test_resolve_run_id_keeps_a_job_run_id():
    assert resolve_run_id(" 262581471753754 ") == "262581471753754"


def test_resolve_run_id_rejects_unsafe_values():
    with pytest.raises(ValueError):
        resolve_run_id("1'; DROP TABLE x; --")


def test_copy_into_sql_has_casts_audit_columns_and_options():
    sql = build_copy_into_sql(TABLE, SOURCE_DIR, "12345")
    assert sql.startswith(f"COPY INTO {TABLE}\n")
    assert f"FROM '{SOURCE_DIR}'" in sql
    for expr in cast_expressions():
        assert expr in sql
    for audit in [
        "_metadata.file_name AS `source_file`",
        "_metadata.file_path AS `source_file_path`",
        "_metadata.file_modification_time AS `source_file_modified_dt`",
        "current_timestamp() AS `load_dt`",
        "'12345' AS `load_run_id`",
        f"'{INGESTION_METHOD}' AS `ingestion_method`",
    ]:
        assert audit in sql
    assert "FILEFORMAT = CSV" in sql
    assert "'header' = 'true'" in sql and "'inferSchema' = 'false'" in sql and "'mergeSchema' = 'true'" in sql
    assert "COPY_OPTIONS ('mergeSchema' = 'false')" in sql


def test_copy_into_sql_never_forces_a_reload():
    assert "force" not in build_copy_into_sql(TABLE, SOURCE_DIR, "manual").lower()


def test_ingestion_method_is_copy_into():
    assert INGESTION_METHOD == "COPY_INTO"


@pytest.mark.parametrize("bad_dir", ["/tmp/chunk1", "/Volumes/a/b", "/Volumes/a/b/c/x'y", "dbfs:/Volumes/a/b/c"])
def test_copy_into_sql_rejects_non_volume_paths(bad_dir):
    with pytest.raises(ValueError):
        build_copy_into_sql(TABLE, bad_dir, "manual")


def test_cast_expressions_load_short_files_by_name(spark):
    """Emulates COPY INTO: text columns from a 36-column and a 27-column file merged by name, then cast.

    The short file's bands must be null and its label must stay in `label`.
    """
    full_header = column_names(DATA_COLUMNS)
    short_header = [c for c in full_header if c not in SATELLITE_BANDS]
    full_values = ["0", "0", "2019", "4", "26", "0"] + ["1.5"] * 20 + ["250.25"] * 9 + ["1"]
    short_values = ["0", "1", "2019", "4", "27", "5"] + ["2.5"] * 20 + ["0"]

    as_text = lambda cols: T.StructType([T.StructField(c, T.StringType()) for c in cols])
    full_df = spark.createDataFrame([full_values], as_text(full_header))
    short_df = spark.createDataFrame([short_values], as_text(short_header))
    merged = full_df.unionByName(short_df, allowMissingColumns=True)  # what mergeSchema does

    loaded = merged.selectExpr(*cast_expressions())

    expected_schema = T.StructType(
        [T.StructField(n, T.IntegerType() if t == "INT" else T.DoubleType(), True) for n, t, _ in DATA_COLUMNS]
    )
    expected = spark.createDataFrame(
        [
            tuple([0, 0, 2019, 4, 26, 0] + [1.5] * 20 + [250.25] * 9 + [1]),
            tuple([0, 1, 2019, 4, 27, 5] + [2.5] * 20 + [None] * 9 + [0]),
        ],
        expected_schema,
    )
    assertDataFrameEqual(loaded, expected)
