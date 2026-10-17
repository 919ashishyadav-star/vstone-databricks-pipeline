"""Build the COPY INTO statement that loads a CSV chunk into a Bronze table.

Every source value is read as text (no type inference) and cast explicitly to the table's type.
The CSV reader matches columns by header name and `mergeSchema` combines the different headers,
so files without satellite columns load with those columns as null.
"""

import re

from common.rain_schema import DATA_COLUMNS, quote_identifier, sql_string

INGESTION_METHOD = "COPY_INTO"
MANUAL_RUN_ID = "manual"
_RUN_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_VOLUME_PATH = re.compile(r"^/Volumes/[A-Za-z0-9_]+/[A-Za-z0-9_]+/[A-Za-z0-9_]+(/[A-Za-z0-9_.-]+)*/?$")


def resolve_run_id(value) -> str:
    """Job run ID for the audit column; 'manual' when empty or not filled in by a job."""
    value = (value or "").strip()
    if not value or value.startswith("{{"):
        return MANUAL_RUN_ID
    if not _RUN_ID.match(value):
        raise ValueError(f"Invalid load_run_id: {value!r}")
    return value


def cast_expressions(columns=DATA_COLUMNS) -> list:
    """CAST(`col` AS TYPE) AS `col` for every data column, in table order."""
    return [f"CAST({quote_identifier(n)} AS {t}) AS {quote_identifier(n)}" for n, t, _d in columns]


def audit_expressions(run_id: str, ingestion_method: str = INGESTION_METHOD) -> list:
    """Audit columns, in the same order as rain_schema.AUDIT_COLUMNS."""
    return [
        "_metadata.file_name AS `source_file`",
        "_metadata.file_path AS `source_file_path`",
        "_metadata.file_modification_time AS `source_file_modified_dt`",
        "current_timestamp() AS `load_dt`",
        f"{sql_string(run_id)} AS `load_run_id`",
        f"{sql_string(ingestion_method)} AS `ingestion_method`",
    ]


def build_copy_into_sql(table: str, source_dir: str, run_id: str, columns=DATA_COLUMNS,
                        ingestion_method: str = INGESTION_METHOD) -> str:
    """COPY INTO statement for a folder of CSV files with headers.

    COPY INTO remembers which files it has loaded into the table, so running this again loads
    only new files. `force` is never used.
    """
    if not _VOLUME_PATH.match(source_dir):
        raise ValueError(f"source_dir must be a Unity Catalog volume path, got {source_dir!r}")
    if not _RUN_ID.match(run_id):
        raise ValueError(f"Invalid run_id: {run_id!r}")
    select_list = ",\n    ".join(cast_expressions(columns) + audit_expressions(run_id, ingestion_method))
    return (
        f"COPY INTO {table}\n"
        f"FROM (\n  SELECT\n    {select_list}\n  FROM {sql_string(source_dir)}\n)\n"
        "FILEFORMAT = CSV\n"
        "FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'false', 'mergeSchema' = 'true')\n"
        "COPY_OPTIONS ('mergeSchema' = 'false')"
    )
