"""Read the Rain Forecast source CSVs safely, matching columns by name.

Why this exists: Spark's CSV reader takes the column names from the first file and
then matches the other files' columns by *position*. Four source files have 27
columns instead of 36 (no satellite bands), so a plain multi-file read would put
their values in the wrong columns. Here files are grouped by header, each group is
read with an explicit schema, and the groups are combined by column *name*.
"""

import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# One reading per grid cell per hour.
NATURAL_KEY = ["row", "col", "year", "month", "day", "hour"]
# Whole-number columns; every other column is a decimal measurement.
INT_COLUMNS = frozenset(NATURAL_KEY + ["label"])
# Satellite brightness temperatures (Kelvin).
SATELLITE_BANDS = ["B09B", "B10B", "B11B", "B12B", "B14B", "B16B", "I2B", "IRB", "WVB"]
# Source file names look like combined_2019_04_01.csv.
SOURCE_FILE_REGEX = re.compile(r"^combined_(\d{4})_(\d{2})_(\d{2})\.csv$")
# Name of the optional column holding the source file name.
FILE_NAME_COLUMN = "_source_file_name"


def parse_header(first_line: str) -> list:
    """Split a CSV header line into column names (strips whitespace and a UTF-8 BOM)."""
    names = [c.strip() for c in first_line.lstrip("﻿").strip().split(",")]
    if not names or any(not n for n in names):
        raise ValueError(f"Invalid CSV header: {first_line!r}")
    return names


def file_month(file_name: str):
    """Return 'YYYY-MM' for a source file name, or None if the name does not match."""
    match = SOURCE_FILE_REGEX.match(file_name)
    return f"{match.group(1)}-{match.group(2)}" if match else None


def schema_for(columns) -> T.StructType:
    """Explicit schema: integer for key/label columns, double for measurements."""
    return T.StructType(
        [T.StructField(c, T.IntegerType() if c in INT_COLUMNS else T.DoubleType(), True) for c in columns]
    )


def group_by_header(headers: dict) -> dict:
    """Group file paths by their header: {tuple(columns): [paths, ...]} (paths sorted)."""
    groups = {}
    for path in sorted(headers):
        groups.setdefault(tuple(headers[path]), []).append(path)
    return groups


def full_column_order(headers: dict) -> list:
    """All columns across files: the widest header first, then any extra columns in first-seen order."""
    widest = max(headers.values(), key=len) if headers else []
    order = list(widest)
    for path in sorted(headers):
        for c in headers[path]:
            if c not in order:
                order.append(c)
    return order


def read_csv_by_name(spark: SparkSession, headers: dict, with_file_name: bool = False) -> DataFrame:
    """Read CSV files with possibly different headers, matching columns by name.

    Args:
        spark: active Spark session.
        headers: {path: [column names from that file's header]}.
        with_file_name: add FILE_NAME_COLUMN with each row's source file name.

    Columns a file does not have are null for that file's rows.
    """
    if not headers:
        raise ValueError("No files to read")

    frames = []
    for columns, paths in group_by_header(headers).items():
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("enforceSchema", "false")  # fail if a header does not match the schema
            .option("mode", "PERMISSIVE")
            .schema(schema_for(columns))
            .load(paths)
        )
        if with_file_name:
            df = df.withColumn(FILE_NAME_COLUMN, F.col("_metadata.file_name"))
        frames.append(df)

    combined = frames[0]
    for df in frames[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    columns = full_column_order(headers) + ([FILE_NAME_COLUMN] if with_file_name else [])
    return combined.select(*columns)
