"""Settings for the Lakeflow Declarative Pipeline that loads a CSV chunk into a Bronze streaming table.

Plain Python so it can be unit tested: the pipeline source in `src/pipelines/` only passes these to
`@dp.table`, the expectations and Auto Loader. The pipeline owns the checkpoint and the schema location,
so (unlike `auto_loader.py`) no checkpoint paths are needed here.
"""

import re

from common.auto_loader import RESCUED_DATA_COLUMN, select_expressions as _auto_loader_select
from common.bronze_audit import MANUAL_RUN_ID, validate_run_id
from common.rain_schema import NATURAL_KEY, bronze_columns, quote_identifier, sql_string

INGESTION_METHOD = "DECLARATIVE_PIPELINE"
_MONTH = re.compile(r"^(\d{4})-(\d{2})$")
_PIPELINE_ID = re.compile(r"^[A-Za-z0-9-]{1,55}$")

# Expectations that only WARN: the row is kept (Bronze keeps raw data) and the failure is counted in the
# event log. The checks task after the pipeline fails the job if any of them failed.
# Every rule is null-safe (a null value fails the rule instead of being ignored).
_ALL_DATA_NULL = " AND ".join(f"{quote_identifier(c)} IS NULL" for c in NATURAL_KEY + ["label"])
WARN_EXPECTATIONS = {
    "no_rescued_data": f"{RESCUED_DATA_COLUMN} IS NULL",
    "not_malformed": f"NOT ({_ALL_DATA_NULL})",
    "label_is_0_or_1": "label IS NOT NULL AND label IN (0, 1)",
    "hour_in_range": "hour IS NOT NULL AND hour BETWEEN 0 AND 23",
    "row_in_range": "`row` IS NOT NULL AND `row` BETWEEN 0 AND 89",
    "col_in_range": "col IS NOT NULL AND col BETWEEN 0 AND 249",
}

# Expectations that FAIL the update: if these break, the load itself is broken (not the data),
# so nothing from the micro-batch is written.
FAIL_EXPECTATIONS = {
    "source_file_present": "source_file IS NOT NULL AND source_file_path IS NOT NULL",
    "ingestion_method_set": f"ingestion_method IS NOT NULL AND ingestion_method = {sql_string(INGESTION_METHOD)}",
}


def reader_options(max_files_per_trigger) -> dict:
    """Auto Loader options for a CSV chunk inside the pipeline (typed schema, rescued data, header by name)."""
    max_files = int(max_files_per_trigger)
    if max_files < 1:
        raise ValueError("max_files_per_trigger must be at least 1")
    return {
        "cloudFiles.format": "csv",
        "cloudFiles.schemaEvolutionMode": "none",  # the schema is fixed; never add columns
        "cloudFiles.includeExistingFiles": "true",  # the first update loads the files already there
        "cloudFiles.maxFilesPerTrigger": str(max_files),
        "header": "true",  # columns are matched by header name, not position
        "rescuedDataColumn": RESCUED_DATA_COLUMN,
    }


def select_expressions(run_id: str) -> list:
    """Data columns, audit columns with ingestion_method = DECLARATIVE_PIPELINE, then `_rescued_data`."""
    return _auto_loader_select(run_id, INGESTION_METHOD)


def table_columns() -> list:
    """Columns of the streaming table, in order: data, audit, `_rescued_data`."""
    return bronze_columns(include_rescued=True)


def table_schema_ddl(columns=None) -> str:
    """Schema for `@dp.table(schema=...)`: every column with its type and description."""
    columns = table_columns() if columns is None else columns
    return ",\n".join(f"{quote_identifier(n)} {t} COMMENT {sql_string(d)}" for n, t, d in columns)


def month_expectation(months) -> dict:
    """WARN expectation that every row belongs to the chunk's months, e.g. ["2019-10"]."""
    parts = []
    for month in months:
        match = _MONTH.match(month or "")
        if not match or not 1 <= int(match.group(2)) <= 12:
            raise ValueError(f"Invalid month {month!r}, expected YYYY-MM")
        parts.append(f"(year = {int(match.group(1))} AND month = {int(match.group(2))})")
    if not parts:
        raise ValueError("At least one month is required")
    return {"month_in_chunk": "year IS NOT NULL AND month IS NOT NULL AND (" + " OR ".join(parts) + ")"}


def warn_expectations(months) -> dict:
    return {**WARN_EXPECTATIONS, **month_expectation(months)}


def pipeline_run_id(update_id, pipeline_id) -> str:
    """Value for `load_run_id`: the pipeline update ID if known, else `pipeline-<pipeline id>`, else 'manual'.

    A pipeline does not receive the job's `{{job.run_id}}`, so the update is the closest equivalent.
    """
    update_id, pipeline_id = (update_id or "").strip(), (pipeline_id or "").strip()
    if update_id:
        return validate_run_id(update_id)
    if pipeline_id:
        if not _PIPELINE_ID.match(pipeline_id):
            raise ValueError(f"Invalid pipeline id: {pipeline_id!r}")
        return validate_run_id(f"pipeline-{pipeline_id}")
    return MANUAL_RUN_ID


def violation_counts_sql(table: str, expectations: dict) -> str:
    """Rows failing each expectation (same null-safe rules the pipeline uses), one column per rule."""
    if not expectations:
        raise ValueError("No expectations given")
    counts = ",\n       ".join(
        f"count_if(NOT coalesce(({rule}), false)) AS {quote_identifier(name)}" for name, rule in expectations.items()
    )
    return f"SELECT {counts}\nFROM {table}"


def expectation_metrics_sql(event_log_table: str, update_ids=None) -> str:
    """Passed / failed records per update and expectation, from the pipeline's published event log.

    `details` is the event's JSON text; each `flow_progress` event of a micro-batch lists its expectations.
    """
    where = ""
    if update_ids:
        ids = ", ".join(sql_string(validate_run_id(u)) for u in update_ids)
        where = f"\n  AND origin.update_id IN ({ids})"
    return (
        "SELECT origin.update_id AS update_id,\n"
        "       min(timestamp) AS first_event,\n"
        "       e.dataset AS dataset,\n"
        "       e.name AS expectation,\n"
        "       sum(e.passed_records) AS passed_records,\n"
        "       sum(e.failed_records) AS failed_records\n"
        f"FROM {event_log_table}\n"
        "LATERAL VIEW explode(from_json(\n"
        "    get_json_object(details, '$.flow_progress.data_quality.expectations'),\n"
        "    'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>'\n"
        ")) x AS e\n"
        "WHERE event_type = 'flow_progress'"
        f"{where}\n"
        "GROUP BY origin.update_id, e.dataset, e.name\n"
        "ORDER BY first_event, expectation"
    )
