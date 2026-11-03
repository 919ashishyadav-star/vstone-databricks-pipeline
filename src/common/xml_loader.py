"""Load XML chunk files into a Bronze table with a PySpark batch read/write.

There is no checkpoint, so rerun safety comes from the table itself: files already in the table
(same name and modification time) are skipped, a changed file stops the run, and `replace` mode
rewrites only the chosen files' rows in one commit.
"""

import re

from common.bronze_audit import audit_expressions
from common.rain_schema import CORRUPT_COLUMN, DATA_COLUMNS, RESCUED_COLUMN, quote_identifier, spark_schema, sql_string

INGESTION_METHOD = "PYSPARK_XML"
RESCUED_DATA_COLUMN = RESCUED_COLUMN[0]
CORRUPT_RECORD_COLUMN = CORRUPT_COLUMN[0]
MODES = ("skip_loaded", "replace")
_TAG = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*$")
_FILE_NAME = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$")


class ChangedFileError(ValueError):
    """A file already in the table was rewritten in the chunk folder."""


def xml_read_options(row_tag: str) -> dict:
    """Reader options: PERMISSIVE keeps every record; bad values and records go to the data-quality columns."""
    if not _TAG.match(row_tag or ""):
        raise ValueError(f"Invalid rowTag: {row_tag!r}")
    return {
        "rowTag": row_tag,
        "mode": "PERMISSIVE",  # never DROPMALFORMED (silent loss) or FAILFAST (record not kept)
        "rescuedDataColumn": RESCUED_DATA_COLUMN,
        "columnNameOfCorruptRecord": CORRUPT_RECORD_COLUMN,
    }


def read_schema():
    """Explicit typed schema for the XML read; the corrupt-record column must be part of it."""
    return spark_schema(DATA_COLUMNS + [CORRUPT_COLUMN])


def select_expressions(run_id: str, ingestion_method: str = INGESTION_METHOD, columns=DATA_COLUMNS) -> list:
    """Data columns by name in table order, audit columns, then the two data-quality columns."""
    return (
        [quote_identifier(n) for n, _t, _d in columns]
        + audit_expressions(run_id, ingestion_method)
        + [quote_identifier(RESCUED_DATA_COLUMN), quote_identifier(CORRUPT_RECORD_COLUMN)]
    )


def _seconds(ms) -> int:
    # Compare modification times to the second: listings and file metadata may differ in sub-second precision.
    return int(ms) // 1000


def files_to_load(chunk_files: dict, loaded: dict, mode: str = "skip_loaded", replace_files=None) -> dict:
    """Decide which chunk files to load.

    Args:
        chunk_files: {file name: modification time in ms} in the chunk folder.
        loaded: {file name: modification time in ms} already in the table (from source_file_modified_dt).
        mode: 'skip_loaded' (default) or 'replace'.
        replace_files: files to reload in 'replace' mode; empty means every chunk file.

    Returns:
        {"load": [...], "skipped": [...], "replace": bool}
    """
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, got {mode!r}")
    for name in list(chunk_files) + list(replace_files or []):
        if not _FILE_NAME.match(name):
            raise ValueError(f"Invalid file name: {name!r}")

    if mode == "replace":
        chosen = sorted(replace_files) if replace_files else sorted(chunk_files)
        missing = [n for n in chosen if n not in chunk_files]
        if missing:
            raise ValueError(f"Files to replace are not in the chunk folder: {missing}")
        return {"load": chosen, "skipped": sorted(set(chunk_files) - set(chosen)), "replace": True}

    changed = sorted(
        n for n in chunk_files if n in loaded and _seconds(chunk_files[n]) != _seconds(loaded[n])
    )
    if changed:
        raise ChangedFileError(
            f"These files were already loaded but have changed since: {changed}. "
            "Reload them deliberately with mode = replace."
        )
    return {
        "load": sorted(n for n in chunk_files if n not in loaded),
        "skipped": sorted(n for n in chunk_files if n in loaded),
        "replace": False,
    }


def replace_where_predicate(files) -> str:
    """`replaceWhere` condition limiting an overwrite to the given source files."""
    if not files:
        raise ValueError("No files to replace")
    for name in files:
        if not _FILE_NAME.match(name):
            raise ValueError(f"Invalid file name: {name!r}")
    return "source_file IN (" + ", ".join(sql_string(n) for n in sorted(files)) + ")"
