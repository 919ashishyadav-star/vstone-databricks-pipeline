"""Sanity checks shared by the Bronze loads: compare each loaded file with its chunk file."""

import math

from common.rain_schema import NATURAL_KEY, SATELLITE_BANDS, SPARK_TYPE_NAMES, quote_identifier, sql_string

# Spark DDL for the per-file check table shown in the notebooks.
CHECK_SCHEMA = (
    "source_file string, has_satellite_columns string, source_rows long, table_rows long, "
    "source_label_sum long, table_label_sum long, rows_all_bands_null long, status string"
)


def table_file_stats_sql(table: str, include_rescued: bool = False, include_corrupt: bool = False,
                         sum_columns=()) -> str:
    """Per-source-file statistics of a Bronze table.

    `sum_columns` adds `sum_<column>` for value checks against the source (e.g. CAPE, B14B).
    """
    bands_all_null = " AND ".join(f"{quote_identifier(b)} IS NULL" for b in SATELLITE_BANDS)
    bands_any_null = " OR ".join(f"{quote_identifier(b)} IS NULL" for b in SATELLITE_BANDS)
    extra = ""
    if include_rescued:
        extra += (
            ",\n       count_if(_rescued_data IS NOT NULL) AS rows_rescued"
            ",\n       count_if(" + " AND ".join(f"{quote_identifier(c)} IS NULL" for c in NATURAL_KEY + ["label"])
            + ") AS rows_all_data_null"
        )
    if include_corrupt:
        extra += ",\n       count_if(_corrupt_record IS NOT NULL) AS rows_corrupt"
    for c in sum_columns:
        extra += f",\n       sum({quote_identifier(c)}) AS {quote_identifier('sum_' + c)}"
    return (
        "SELECT source_file,\n"
        "       count(*) AS rows,\n"
        "       sum(label) AS label_sum,\n"
        f"       count_if({bands_all_null}) AS rows_all_bands_null,\n"
        f"       count_if({bands_any_null}) AS rows_any_band_null,\n"
        "       count_if(label IS NULL OR label NOT IN (0, 1)) AS rows_bad_label"
        f"{extra}\n"
        f"FROM {table}\n"
        "GROUP BY source_file"
    )


def load_batches_sql(table: str, run_id: str, since: str) -> str:
    """Rows and files written per micro-batch by one run.

    Every micro-batch stamps its own `load_dt`, so grouping this run's rows by `load_dt` gives one row per
    micro-batch. `since` is the run's start time; it separates repeated manual runs that share the id 'manual'.
    """
    return (
        "SELECT load_dt, count(*) AS rows, count(DISTINCT source_file) AS files,\n"
        "       min(source_file) AS first_file, max(source_file) AS last_file\n"
        f"FROM {table}\n"
        f"WHERE load_run_id = {sql_string(run_id)} AND load_dt >= CAST({sql_string(since)} AS TIMESTAMP)\n"
        "GROUP BY load_dt\n"
        "ORDER BY load_dt"
    )


def duplicate_keys_sql(table: str) -> str:
    keys = ", ".join(quote_identifier(c) for c in NATURAL_KEY)
    return f"SELECT count(*) AS n FROM (SELECT {keys} FROM {table} GROUP BY {keys} HAVING count(*) > 1)"


def audit_check_sql(table: str, ingestion_method: str) -> str:
    return (
        "SELECT count_if(source_file IS NULL OR source_file_path IS NULL OR source_file_modified_dt IS NULL\n"
        "                OR load_dt IS NULL OR load_run_id IS NULL OR ingestion_method IS NULL) AS rows_null_audit,\n"
        f"       count_if(ingestion_method <> '{ingestion_method}') AS rows_other_method,\n"
        "       count(DISTINCT load_run_id) AS load_runs,\n"
        "       count(*) AS total_rows\n"
        f"FROM {table}"
    )


def values_match(a, b, rel_tol: float = 1e-9) -> bool:
    """Equal sums, allowing only floating-point rounding (sums are added in a different order)."""
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(float(a), float(b), rel_tol=rel_tol, abs_tol=1e-6)


def schema_problems(actual_fields, expected_columns) -> list:
    """Compare a table's columns (name, Spark type string) with the expected columns, in order."""
    expected = [(n, SPARK_TYPE_NAMES[t]) for n, t, _d in expected_columns]
    actual = [(n, t) for n, t in actual_fields]
    if actual == expected:
        return []
    problems = []
    if [n for n, _ in actual] != [n for n, _ in expected]:
        problems.append(f"column order/names differ: table {[n for n, _ in actual]} vs expected {[n for n, _ in expected]}")
    for (an, at), (en, et) in zip(actual, expected):
        if an == en and at != et:
            problems.append(f"column {an}: type {at} != expected {et}")
    return problems or ["schema differs from rain_schema"]


def compare_file_stats(source_stats: dict, table_stats: dict, has_bands: dict, source_sums=None,
                       rel_tol: float = 1e-9) -> tuple:
    """Compare a Bronze table with its chunk files, file by file.

    Args:
        source_stats: {file name: (rows, sum of label)} counted from the chunk (or original source) files.
        table_stats: {file name: row dict from table_file_stats_sql()}.
        has_bands: {file name: True if the source file has satellite columns}.
        source_sums: optional {file name: {column: sum}} checked against `sum_<column>` in table_stats.

    Returns:
        (check rows matching CHECK_SCHEMA, list of problem messages). No problems = pass.
    """
    rows, problems = [], []
    for name in sorted(set(source_stats) | set(table_stats)):
        src_rows, src_label = source_stats.get(name, (0, None))
        t = table_stats.get(name, {})
        tbl_rows = t.get("rows", 0)
        issues = []
        if name not in source_stats:
            issues.append("in table but not in chunk folder")
        if name not in table_stats:
            issues.append("not loaded")
        if tbl_rows != src_rows:
            issues.append(f"rows {tbl_rows:,} != source {src_rows:,}")
        if t and t["label_sum"] != src_label:
            issues.append(f"sum(label) {t['label_sum']} != source {src_label}")
        if t and t["rows_bad_label"]:
            issues.append(f"{t['rows_bad_label']:,} rows with null/invalid label")
        if t and has_bands.get(name) is False and t["rows_all_bands_null"] != tbl_rows:
            issues.append("satellite bands not null for a file without satellite columns")
        if t and has_bands.get(name) is True and t["rows_any_band_null"]:
            issues.append(f"{t['rows_any_band_null']:,} rows with null bands in a file that has them")
        if t.get("rows_rescued"):
            issues.append(f"{t['rows_rescued']:,} rows with rescued data")
        if t.get("rows_all_data_null"):
            issues.append(f"{t['rows_all_data_null']:,} rows with every key and label null (malformed)")
        if t.get("rows_corrupt"):
            issues.append(f"{t['rows_corrupt']:,} corrupt records")
        for column, src_sum in sorted(((source_sums or {}).get(name) or {}).items()):
            if t and not values_match(t.get(f"sum_{column}"), src_sum, rel_tol):
                issues.append(f"sum({column}) {t.get(f'sum_{column}')} != source {src_sum}")
        problems += [f"{name}: {i}" for i in issues]
        rows.append((
            name, "yes" if has_bands.get(name) else "no", src_rows, tbl_rows, src_label, t.get("label_sum"),
            t.get("rows_all_bands_null", 0), "OK" if not issues else "; ".join(issues),
        ))
    return rows, problems
