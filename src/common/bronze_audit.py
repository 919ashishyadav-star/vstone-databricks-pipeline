"""Audit columns shared by every Bronze load (COPY INTO, Auto Loader, ...)."""

import re

from common.rain_schema import sql_string

MANUAL_RUN_ID = "manual"
_RUN_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def resolve_run_id(value) -> str:
    """Job run ID for the audit column; 'manual' when empty or not filled in by a job."""
    value = (value or "").strip()
    if not value or value.startswith("{{"):
        return MANUAL_RUN_ID
    validate_run_id(value)
    return value


def validate_run_id(run_id: str) -> str:
    if not _RUN_ID.match(run_id or ""):
        raise ValueError(f"Invalid load_run_id: {run_id!r}")
    return run_id


def audit_expressions(run_id: str, ingestion_method: str) -> list:
    """SQL expressions for the audit columns, in the same order as rain_schema.AUDIT_COLUMNS.

    `_metadata` is the hidden file-metadata column available to COPY INTO and Auto Loader.
    """
    validate_run_id(run_id)
    return [
        "_metadata.file_name AS `source_file`",
        "_metadata.file_path AS `source_file_path`",
        "_metadata.file_modification_time AS `source_file_modified_dt`",
        "current_timestamp() AS `load_dt`",
        f"{sql_string(run_id)} AS `load_run_id`",
        f"{sql_string(ingestion_method)} AS `ingestion_method`",
    ]
