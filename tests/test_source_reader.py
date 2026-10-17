import pytest
from pyspark.sql import types as T
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

from common.source_reader import (
    FILE_NAME_COLUMN,
    file_month,
    full_column_order,
    group_by_header,
    parse_header,
    read_csv_by_name,
    schema_for,
)

# A cut-down version of the real layout: keys, one model variable, two satellite bands, label.
FULL_HEADER = ["row", "col", "year", "month", "day", "hour", "CIN", "B09B", "B10B", "label"]
SHORT_HEADER = ["row", "col", "year", "month", "day", "hour", "CIN", "label"]  # no satellite bands


def write_csv(path, header, rows):
    lines = [",".join(header)] + [",".join(str(v) for v in row) for row in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


def test_parse_header_strips_whitespace_and_bom():
    assert parse_header("﻿row, col ,label\n") == ["row", "col", "label"]


def test_parse_header_rejects_empty_column():
    with pytest.raises(ValueError):
        parse_header("row,,label")


@pytest.mark.parametrize(
    "name, expected",
    [
        ("combined_2019_04_01.csv", "2019-04"),
        ("combined_2020_10_31.csv", "2020-10"),
        ("combined_2020_10_31.json.gz", None),
        ("metadata.csv", None),
    ],
)
def test_file_month(name, expected):
    assert file_month(name) == expected


def test_schema_for_uses_int_for_keys_and_label_and_double_otherwise():
    expected = T.StructType(
        [
            T.StructField("row", T.IntegerType(), True),
            T.StructField("hour", T.IntegerType(), True),
            T.StructField("CIN", T.DoubleType(), True),
            T.StructField("label", T.IntegerType(), True),
        ]
    )
    assertSchemaEqual(schema_for(["row", "hour", "CIN", "label"]), expected)


def test_group_by_header_and_full_column_order():
    headers = {"b.csv": SHORT_HEADER, "a.csv": FULL_HEADER, "c.csv": FULL_HEADER}
    groups = group_by_header(headers)
    assert groups == {tuple(FULL_HEADER): ["a.csv", "c.csv"], tuple(SHORT_HEADER): ["b.csv"]}
    assert full_column_order(headers) == FULL_HEADER


def test_read_csv_by_name_aligns_files_with_missing_columns(spark, tmp_path):
    """The short file's label must stay in `label`, and its missing bands must be null."""
    full = write_csv(tmp_path / "combined_2019_04_26.csv", FULL_HEADER, [[0, 0, 2019, 4, 26, 0, 9999.0, 250.5, 260.25, 1]])
    short = write_csv(tmp_path / "combined_2019_04_27.csv", SHORT_HEADER, [[0, 1, 2019, 4, 27, 5, 12.5, 0]])

    df = read_csv_by_name(spark, {full: FULL_HEADER, short: SHORT_HEADER})

    assertSchemaEqual(df.schema, schema_for(FULL_HEADER))
    expected = spark.createDataFrame(
        [
            (0, 0, 2019, 4, 26, 0, 9999.0, 250.5, 260.25, 1),
            (0, 1, 2019, 4, 27, 5, 12.5, None, None, 0),
        ],
        schema_for(FULL_HEADER),
    )
    assertDataFrameEqual(df, expected)


def test_read_csv_by_name_adds_source_file_name(spark, tmp_path):
    path = write_csv(tmp_path / "combined_2020_04_01.csv", SHORT_HEADER, [[1, 2, 2020, 4, 1, 3, 1.0, 0]])

    df = read_csv_by_name(spark, {path: SHORT_HEADER}, with_file_name=True)

    assert df.columns == SHORT_HEADER + [FILE_NAME_COLUMN]
    assert [r[FILE_NAME_COLUMN] for r in df.collect()] == ["combined_2020_04_01.csv"]


def test_read_csv_by_name_requires_files(spark):
    with pytest.raises(ValueError):
        read_csv_by_name(spark, {})
